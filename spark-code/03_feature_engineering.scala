import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val spark = SparkSession.builder().appName("03-feature-engineering-v2").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.shuffle.partitions", "64")

// ====================== CONFIG (edit these) ======================
val base        = "hdfs://namenode-cloud:9000/telementary"
val metricsPath = s"$base/processed/unified_machine_5min.parquet"
val aliLogsPath = s"$base/raw/logs/alibaba_syslogs"
val gooLogsPath = s"$base/raw/logs/google_syslogs"
val outPath     = s"$base/processed/features_5min.parquet"
val STEP        = 300L                 // window size in seconds
val USE_DAY     = true                 // add "same time yesterday" features (costs the first 24 h of each trace)
val WARMUP      = if (USE_DAY) 288 else 72
val HORIZONS    = Seq(1, 6, 12)        // predict 5 min, 30 min, 1 h ahead
val GAP         = HORIZONS.max * STEP  // buffer at split boundaries so labels never cross into the next split
// =================================================================

println("===== Feature engineering v2 =====")
def clip5(c: Column): Column = greatest(lit(-5.0), least(lit(5.0), c))

// -------------------- 1. Metrics --------------------
val metrics = spark.read.parquet(metricsPath)

// -------------------- 2. Parse synthetic logs --------------------
val tsFmt = "yyyy-MM-dd HH:mm:ss"
val t0    = unix_timestamp(lit("2026-01-01 00:00:00"), tsFmt)
val ts    = "(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})"
val aliRe = s"^$ts \\| (\\w+) \\| alibaba-node-(\\S+) \\| .*$$"
val gooRe = s"^$ts \\[(\\w+)\\] gcp-host-(\\S+) : .*$$"

def parseLogs(path: String, re: String, cloud: String) =
  spark.read.text(path).select(
      regexp_extract(col("value"), re, 1).as("ts"),
      regexp_extract(col("value"), re, 2).as("level"),
      regexp_extract(col("value"), re, 3).as("mid"))
    .filter(col("ts") =!= "")
    .select(
      concat_ws("_", lit(cloud), col("mid")).as("node_id"),
      (unix_timestamp(col("ts"), tsFmt) - t0).as("window_ts"),
      when(col("level").isin("ERROR", "CRITICAL"), 2)
        .when(col("level").isin("WARN", "WARNING"), 1)
        .otherwise(0).as("sev"))
    .filter(col("sev") > 0)

val logs = parseLogs(aliLogsPath, aliRe, "alibaba")
  .unionByName(parseLogs(gooLogsPath, gooRe, "google"))
  .groupBy("node_id", "window_ts").agg(max("sev").as("sev"))

// -------------------- 3. Cluster context (per cloud, per window) --------------------
val clAgg = metrics.groupBy("cloud_provider", "window_ts").agg(
  avg("cpu_usage_norm").as("cl_cpu_mean"),
  coalesce(stddev("cpu_usage_norm"), lit(0.0)).as("cl_cpu_std"),
  avg("mem_usage_norm").as("cl_mem_mean"),
  coalesce(stddev("mem_usage_norm"), lit(0.0)).as("cl_mem_std"))
val wcl = Window.partitionBy("cloud_provider").orderBy("window_ts")
val cl = clAgg
  .withColumn("gap_ok", (col("window_ts") - lag("window_ts", 1).over(wcl)) === lit(STEP))
  .withColumn("cl_cpu_d1", when(col("gap_ok"), col("cl_cpu_mean") - lag("cl_cpu_mean", 1).over(wcl)).otherwise(0.0))
  .withColumn("cl_mem_d1", when(col("gap_ok"), col("cl_mem_mean") - lag("cl_mem_mean", 1).over(wcl)).otherwise(0.0))
  .drop("gap_ok")

// -------------------- 4. Dense time grid per node --------------------
val bounds = metrics.groupBy("cloud_provider", "node_id")
  .agg(min("window_ts").as("t_lo"), max("window_ts").as("t_hi"))

val grid = bounds
  .withColumn("window_ts", explode(sequence(col("t_lo"), col("t_hi"), lit(STEP))))
  .drop("t_lo", "t_hi")

val dense = grid
  .join(metrics.select("node_id", "window_ts", "cpu_usage_norm", "mem_usage_norm", "n_records"),
        Seq("node_id", "window_ts"), "left")
  .join(logs, Seq("node_id", "window_ts"), "left")
  .withColumn("sev", coalesce(col("sev"), lit(0)))
  .join(broadcast(cl), Seq("cloud_provider", "window_ts"), "left")

val wf = Window.partitionBy("node_id").orderBy("window_ts")
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)

val filled = dense
  .withColumn("observed", col("cpu_usage_norm").isNotNull)
  .withColumn("cpu",   last(col("cpu_usage_norm"), true).over(wf))
  .withColumn("mem",   last(col("mem_usage_norm"), true).over(wf))
  .withColumn("n_rec", last(col("n_records"), true).over(wf))

// -------------------- 5. Stage A: window features (past + current only) --------------------
val w = Window.partitionBy("node_id").orderBy("window_ts")
def roll(n: Int) = w.rowsBetween(-(n - 1), 0)

val lagKs  = Seq(1, 2, 3, 6, 12) ++ (if (USE_DAY) Seq(288) else Nil)
val meanNs = Seq(6, 12, 72) ++ (if (USE_DAY) Seq(288) else Nil)

val rawFeats: Seq[Column] = Seq("cpu", "mem").flatMap { c =>
  val x = col(c)
  lagKs.map(k  => lag(x, k).over(w).as(s"${c}_lag$k")) ++
  meanNs.map(n => avg(x).over(roll(n)).as(s"${c}_mean$n")) ++
  Seq(6, 12, 72).map(n => stddev_pop(x).over(roll(n)).as(s"${c}_std$n")) ++
  Seq(6, 12, 72).map(n => max(x).over(roll(n)).as(s"${c}_max$n")) ++
  Seq(min(x).over(roll(12)).as(s"${c}_min12"))
}

val nrecFeats = Seq(lag(col("n_rec"), 1).over(w).as("n_rec_lag1"),
                    avg(col("n_rec")).over(roll(12)).as("n_rec_mean12"))

val warn = (col("sev") === 1).cast("int")
val err  = (col("sev") === 2).cast("int")
val logFeats = Seq(
  sum(warn).over(roll(6)).as("warn_cnt6"),   sum(err).over(roll(6)).as("err_cnt6"),
  sum(warn).over(roll(12)).as("warn_cnt12"), sum(err).over(roll(12)).as("err_cnt12"))

val tod = (col("window_ts") % 86400L) / 86400.0 * 2 * math.Pi
val timeFeats = Seq(sin(tod).as("tod_sin"), cos(tod).as("tod_cos"))

val labels = for (c <- Seq("cpu", "mem"); h <- HORIZONS)
  yield lead(col(s"${c}_usage_norm"), h).over(w).as(s"y_${c}_h$h")

val baseCols = Seq(col("cloud_provider"), col("node_id"), col("window_ts"), col("observed"),
  col("cpu"), col("mem"), col("n_rec"),
  col("cl_cpu_mean"), col("cl_cpu_std"), col("cl_cpu_d1"),
  col("cl_mem_mean"), col("cl_mem_std"), col("cl_mem_d1"),
  (col("cpu") - col("cl_cpu_mean")).as("cpu_vs_cl"),
  (col("mem") - col("cl_mem_mean")).as("mem_vs_cl"),
  row_number().over(w).as("rn"))

val stageA = filled.select(baseCols ++ timeFeats ++ rawFeats ++ nrecFeats ++ logFeats ++ labels: _*)

// -------------------- 6. Stage B: derived features (deltas, deviations, slopes, z-score) --------------------
val derived: Seq[Column] = Seq("cpu", "mem").flatMap { c =>
  val x = col(c)
  val core = Seq(
    (x - col(s"${c}_lag1")).as(s"${c}_d1"),                                        // 5-min change
    (x - col(s"${c}_lag3")).as(s"${c}_d3"),                                        // 15-min change
    (x - col(s"${c}_lag12")).as(s"${c}_d12"),                                      // 1-h change
    ((x - col(s"${c}_lag1")) - (col(s"${c}_lag1") - col(s"${c}_lag2"))).as(s"${c}_accel"),
    (x - col(s"${c}_mean6")).as(s"${c}_dev6"),                                     // deviation from 30-min mean
    (x - col(s"${c}_mean12")).as(s"${c}_dev12"),
    (x - col(s"${c}_mean72")).as(s"${c}_dev72"),
    (col(s"${c}_mean6") - col(s"${c}_mean12")).as(s"${c}_slope_short"),            // short mean vs medium mean
    (col(s"${c}_mean12") - col(s"${c}_mean72")).as(s"${c}_slope_long"),
    (col(s"${c}_max12") - col(s"${c}_min12")).as(s"${c}_range12"),
    clip5((x - col(s"${c}_mean72")) / (col(s"${c}_std72") + 1e-6)).as(s"${c}_z72"))
  val day = if (USE_DAY) Seq(
    (x - col(s"${c}_lag288")).as(s"${c}_dday"),                                    // vs same time yesterday
    (x - col(s"${c}_mean288")).as(s"${c}_dev288")) else Nil
  core ++ day
} ++ Seq((col("n_rec") - col("n_rec_lag1")).as("n_rec_d1"))                        // instance churn on the node

val all = stageA.select(col("*") +: derived: _*)

// -------------------- 7. Filter + time-based split per cloud --------------------
val ready = all.filter(col("observed") && col("rn") > WARMUP)

val cb = metrics.groupBy("cloud_provider")
  .agg(min("window_ts").as("lo"), max("window_ts").as("hi"))

val result = ready.join(cb, "cloud_provider")
  .withColumn("t70", col("lo") + (col("hi") - col("lo")) * 0.70)
  .withColumn("t85", col("lo") + (col("hi") - col("lo")) * 0.85)
  .withColumn("split",
    when(col("window_ts") < col("t70") - GAP, "train")
    .when(col("window_ts") >= col("t70") && col("window_ts") < col("t85") - GAP, "val")
    .when(col("window_ts") >= col("t85"), "test"))
  .filter(col("split").isNotNull)
  .drop("lo", "hi", "t70", "t85", "observed", "rn")

result.write.mode("overwrite").partitionBy("cloud_provider").parquet(outPath)

// -------------------- 8. Summary --------------------
val chk = spark.read.parquet(outPath)
val idCols   = Set("node_id", "window_ts", "split", "cloud_provider")
val featCols = chk.columns.filterNot(c => idCols(c) || c.startsWith("y_"))
println("Feature count: " + featCols.length)
println("Features: " + featCols.mkString(", "))

val rowsWithNullFeat = chk.filter(featCols.map(c => col(c).isNull).reduce(_ || _)).count()
println("Rows with any null feature (should be 0): " + rowsWithNullFeat)

println("Rows per cloud / split, label null rate, and last-value baseline RMSE (h1):")
chk.groupBy("cloud_provider", "split").agg(
  count(lit(1)).as("rows"),
  countDistinct("node_id").as("nodes"),
  round(avg(col("y_cpu_h1").isNull.cast("int")), 3).as("y_cpu_null"),
  round(sqrt(avg(pow(col("y_cpu_h1") - col("cpu"), 2))), 4).as("base_rmse_cpu"),
  round(sqrt(avg(pow(col("y_mem_h1") - col("mem"), 2))), 4).as("base_rmse_mem")
).orderBy("cloud_provider", "split").show(false)

println("===== Done. Output: " + outPath + " =====")
sys.exit(0)
