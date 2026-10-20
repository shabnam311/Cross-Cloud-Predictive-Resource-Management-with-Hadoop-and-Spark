// =====================================================================
//  05_stream.scala : Structured Streaming job (file source -> foreachBatch -> FastAPI)
//
//  Source : /telementary/stream_<cloud>/incoming  (1 parquet file per 5-min window, written by 05a_replayer.scala)
//  Per micro-batch (= one window, maxFilesPerTrigger=1):
//    1. history (last 290 windows, kept in executor memory via localCheckpoint) + new window
//    2. recompute the 80 features with the SAME code as 03_feature_engineering (computeFeatures below)
//    3. scale with the saved Google statistics (_meta_gbt_delta.txt), score gbt_delta for cpu and mem,
//       prediction = current + model_output * sigma, clipped to 0..1   (exactly as in 04_gbt.scala)
//    4. append predictions to Parquet, POST them to FastAPI, compare the previous predictions with the
//       actuals that just arrived (live RMSE vs persistence)
//
//  Env: API_URL=http://host.docker.internal:8000/api/recommend/batch  FEATURE_LOG_WINDOWS=20
//       SHUFFLE_PARTS=8  HDFS_BASE=hdfs://namenode-cloud:9000
//  Start 05a_replayer.scala FIRST (it builds the seed history and cleans /telementary/stream).
// =====================================================================
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.GBTRegressionModel
import org.apache.hadoop.fs.Path
import java.net.{HttpURLConnection, URL}

val spark = SparkSession.builder().appName("05-stream-gbt-delta").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.shuffle.partitions", sys.env.getOrElse("SHUFFLE_PARTS", "8"))   // ~140k rows per batch: 64 partitions would be mostly scheduling overhead

val HDFS     = sys.env.getOrElse("HDFS_BASE", "hdfs://namenode-cloud:9000")
val base     = s"$HDFS/telementary"
val CLOUD    = sys.env.getOrElse("CLOUD", "google")      // google | alibaba
val SDIR     = s"$base/stream_$CLOUD"
val STEP     = 300L
val HIST_WIN = 290
val H_MIN    = 5
val RUN      = "gbt_delta"
val API_URL  = sys.env.getOrElse("API_URL", "http://host.docker.internal:8000/api/recommend/batch")
val FEATURE_LOG_WINDOWS = sys.env.getOrElse("FEATURE_LOG_WINDOWS", "20").toInt
def modelDir(t: String) = s"$base/models/$CLOUD/$t/h${H_MIN}min"

val fs = new Path(base).getFileSystem(spark.sparkContext.hadoopConfiguration)
require(fs.exists(new Path(s"$SDIR/seed")), "seed history missing: run 05a_replayer.scala first")

// ------------------------- model metadata + models -------------------------
def readText(p: String): String = {
  val path = new Path(p)
  val in = fs.open(path)
  try scala.io.Source.fromInputStream(in, "UTF-8").mkString finally in.close()
}
case class Meta(stats: Map[String, Double], feats: Seq[String], sigma: Double)
def parseMeta(t: String): Meta = {
  val lines = readText(s"${modelDir(t)}/_meta_$RUN.txt").split("\n").map(_.trim)
  require(lines.exists(_.startsWith("scale_mode=delta")), s"$t model was not trained with SCALE_MODE=delta")
  val stats = lines.find(_.startsWith("train stats:")).get.stripPrefix("train stats:").split(",").map(_.trim).filter(_.nonEmpty)
    .map { kv => val a = kv.split("="); a(0).trim -> a(1).trim.toDouble }.toMap
  val feats = lines.find(_.startsWith("features(in order)=")).get.stripPrefix("features(in order)=").split(",").map(_.trim).toSeq
  Meta(stats, feats, stats(s"dsd_$t"))
}
val targets = Seq("cpu", "mem")
val metas  = targets.map(t => t -> parseMeta(t)).toMap
val models = targets.map(t => t -> GBTRegressionModel.load(s"${modelDir(t)}/$RUN")).toMap
require(metas("cpu").feats == metas("mem").feats, "cpu and mem models use different feature lists")
val featNames = metas("cpu").feats
println(s"loaded ${models.size} models, ${featNames.length} features, sigma cpu=${metas("cpu").sigma} mem=${metas("mem").sigma}")

// scaling: copied from 04_gbt.scala (SCALE_MODE=delta -> sigma of a change = dsd_<metric>)
def sigOf(st: Map[String, Double], m: String): Double = st(s"dsd_$m")
def scaleOf(f: String, st: Map[String, Double]): Column = {
  val x = col(f)
  val isLevel = f.matches("^(cpu|mem)(_(lag|mean|max|min)\\d+)?$") || f.matches("^cl_(cpu|mem)_mean$")
  if (f.endsWith("_z72") || f.startsWith("tod_") || f.startsWith("warn_") || f.startsWith("err_")) x
  else if (f.startsWith("n_rec")) {
    if (Set("n_rec", "n_rec_lag1", "n_rec_mean12").contains(f)) (x - lit(st("mu_n"))) / lit(st("sd_n"))
    else x / lit(st("sd_n"))
  } else {
    val m = if (f.startsWith("cl_")) f.split("_")(1) else if (f.startsWith("cpu")) "cpu" else "mem"
    if (isLevel) (x - lit(st(s"mu_$m"))) / lit(st(s"sd_$m")) else x / lit(sigOf(st, m))
  }
}

// ------------------------- feature code: copy of 03_feature_engineering (sections 3-6) -------------------------
// Input: raw rows (node_id, window_ts, cpu_usage_norm, mem_usage_norm, n_records, sev) = retained history + new window.
// Output: one row per observed (node, window) with all feature columns. Labels and the split are not needed here.
def clip5(c: Column): Column = greatest(lit(-5.0), least(lit(5.0), c))
def computeFeatures(raw: DataFrame): DataFrame = {
  val metrics = raw.filter(col("cpu_usage_norm").isNotNull).withColumn("cloud_provider", lit(CLOUD))
  val logs = raw.filter(col("sev") > 0).groupBy("node_id", "window_ts").agg(max("sev").as("sev"))
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
  val bounds = metrics.groupBy("cloud_provider", "node_id").agg(min("window_ts").as("t_lo"), max("window_ts").as("t_hi"))
  val grid = bounds.withColumn("window_ts", explode(sequence(col("t_lo"), col("t_hi"), lit(STEP)))).drop("t_lo", "t_hi")
  val dense = grid
    .join(metrics.select("node_id", "window_ts", "cpu_usage_norm", "mem_usage_norm", "n_records"), Seq("node_id", "window_ts"), "left")
    .join(logs, Seq("node_id", "window_ts"), "left")
    .withColumn("sev", coalesce(col("sev"), lit(0)))
    .join(broadcast(cl), Seq("cloud_provider", "window_ts"), "left")
  val wf = Window.partitionBy("node_id").orderBy("window_ts").rowsBetween(Window.unboundedPreceding, Window.currentRow)
  val filled = dense
    .withColumn("observed", col("cpu_usage_norm").isNotNull)
    .withColumn("cpu",   last(col("cpu_usage_norm"), true).over(wf))
    .withColumn("mem",   last(col("mem_usage_norm"), true).over(wf))
    .withColumn("n_rec", last(col("n_records"), true).over(wf))
  val w = Window.partitionBy("node_id").orderBy("window_ts")
  def roll(n: Int) = w.rowsBetween(-(n - 1), 0)
  val lagKs  = Seq(1, 2, 3, 6, 12, 288)
  val meanNs = Seq(6, 12, 72, 288)
  val rawFeats: Seq[Column] = Seq("cpu", "mem").flatMap { c =>
    val x = col(c)
    lagKs.map(k  => lag(x, k).over(w).as(s"${c}_lag$k")) ++
    meanNs.map(n => avg(x).over(roll(n)).as(s"${c}_mean$n")) ++
    Seq(6, 12, 72).map(n => stddev_pop(x).over(roll(n)).as(s"${c}_std$n")) ++
    Seq(6, 12, 72).map(n => max(x).over(roll(n)).as(s"${c}_max$n")) ++
    Seq(min(x).over(roll(12)).as(s"${c}_min12"))
  }
  val nrecFeats = Seq(lag(col("n_rec"), 1).over(w).as("n_rec_lag1"), avg(col("n_rec")).over(roll(12)).as("n_rec_mean12"))
  val warn = (col("sev") === 1).cast("int")
  val err  = (col("sev") === 2).cast("int")
  val logFeats = Seq(
    sum(warn).over(roll(6)).as("warn_cnt6"),   sum(err).over(roll(6)).as("err_cnt6"),
    sum(warn).over(roll(12)).as("warn_cnt12"), sum(err).over(roll(12)).as("err_cnt12"))
  val tod = (col("window_ts") % 86400L) / 86400.0 * 2 * math.Pi
  val timeFeats = Seq(sin(tod).as("tod_sin"), cos(tod).as("tod_cos"))
  val baseCols = Seq(col("cloud_provider"), col("node_id"), col("window_ts"), col("observed"),
    col("cpu"), col("mem"), col("n_rec"),
    col("cl_cpu_mean"), col("cl_cpu_std"), col("cl_cpu_d1"),
    col("cl_mem_mean"), col("cl_mem_std"), col("cl_mem_d1"),
    (col("cpu") - col("cl_cpu_mean")).as("cpu_vs_cl"),
    (col("mem") - col("cl_mem_mean")).as("mem_vs_cl"))
  val stageA = filled.select(baseCols ++ timeFeats ++ rawFeats ++ nrecFeats ++ logFeats: _*)
  val derived: Seq[Column] = Seq("cpu", "mem").flatMap { c =>
    val x = col(c)
    val core = Seq(
      (x - col(s"${c}_lag1")).as(s"${c}_d1"),
      (x - col(s"${c}_lag3")).as(s"${c}_d3"),
      (x - col(s"${c}_lag12")).as(s"${c}_d12"),
      ((x - col(s"${c}_lag1")) - (col(s"${c}_lag1") - col(s"${c}_lag2"))).as(s"${c}_accel"),
      (x - col(s"${c}_mean6")).as(s"${c}_dev6"),
      (x - col(s"${c}_mean12")).as(s"${c}_dev12"),
      (x - col(s"${c}_mean72")).as(s"${c}_dev72"),
      (col(s"${c}_mean6") - col(s"${c}_mean12")).as(s"${c}_slope_short"),
      (col(s"${c}_mean12") - col(s"${c}_mean72")).as(s"${c}_slope_long"),
      (col(s"${c}_max12") - col(s"${c}_min12")).as(s"${c}_range12"),
      clip5((x - col(s"${c}_mean72")) / (col(s"${c}_std72") + 1e-6)).as(s"${c}_z72"))
    val day = Seq((x - col(s"${c}_lag288")).as(s"${c}_dday"), (x - col(s"${c}_mean288")).as(s"${c}_dev288"))
    core ++ day
  } ++ Seq((col("n_rec") - col("n_rec_lag1")).as("n_rec_d1"))
  stageA.select(col("*") +: derived: _*)
}

// ------------------------- scoring -------------------------
def predict(fRows: DataFrame, t: String): DataFrame = {
  val m = metas(t)
  val scaled = m.feats.map(f => scaleOf(f, m.stats).as(f))
  val d = fRows.select(scaled ++ Seq(col("node_id"), col("cpu").as("cur_cpu"), col("mem").as("cur_mem")): _*)
  val a = new VectorAssembler().setInputCols(m.feats.toArray).setOutputCol("features").setHandleInvalid("skip").transform(d)
  models(t).transform(a).select(col("node_id"),
    greatest(lit(0.0), least(lit(1.0), col(s"cur_$t") + col("prediction") * lit(m.sigma))).as(s"pred_$t"),
    col(s"cur_$t"))
}

def postBatch(ts: Long, rows: Array[org.apache.spark.sql.Row]): Unit = {
  try {
    val items = rows.map(r => s"""{"node_id":"${r.getString(0)}","pred_cpu_util":${r.getDouble(1)},"pred_mem_util":${r.getDouble(2)},"cur_cpu_util":${r.getDouble(3)},"cur_mem_util":${r.getDouble(4)}}""").mkString(",")
    val body = s"""{"cloud":"$CLOUD","timestamp":${ts + STEP},"observed_timestamp":$ts,"predictions":[$items],"task_arrival_rate":null}"""
    val c = new URL(API_URL).openConnection().asInstanceOf[HttpURLConnection]
    c.setRequestMethod("POST"); c.setRequestProperty("Content-Type", "application/json"); c.setDoOutput(true)
    c.setConnectTimeout(3000); c.setReadTimeout(10000)
    val os = c.getOutputStream; os.write(body.getBytes("UTF-8")); os.close()
    val code = c.getResponseCode
    c.disconnect()
    if (code != 200) println(s"  [api] HTTP $code")
  } catch { case e: Exception => println(s"  [api] post failed (stream continues): ${e.getMessage}") }
}

// ------------------------- per-window state (driver side) -------------------------
val rawCols = Seq("node_id", "window_ts", "cpu_usage_norm", "mem_usage_norm", "n_records", "sev")
var hist: DataFrame = null
var prevPreds: Array[(String, Double, Double, Double, Double)] = Array()   // node, pred_cpu, pred_mem, cur_cpu, cur_mem
var prevTargetTs = -1L
var nDone = 0
var sseCm, sseCp, sseMm, sseMp, nTot = 0.0

def processWindow(win: DataFrame, ts: Long): Unit = {
  val tStart = System.nanoTime()
  if (hist == null) hist = spark.read.parquet(s"$SDIR/seed").select(rawCols.map(col): _*).localCheckpoint()
  val cur = hist.unionByName(win.select(rawCols.map(col): _*)).localCheckpoint()
  val feat = computeFeatures(cur)
  val missing = featNames.filterNot(feat.columns.contains)
  require(missing.isEmpty, "features missing in stream: " + missing.mkString(","))
  val fRows = feat.filter(col("window_ts") === ts && col("observed")).na.drop(featNames).persist()
  val nReady = fRows.count()
  val out = predict(fRows, "cpu").join(predict(fRows, "mem"), "node_id")
    .select("node_id", "pred_cpu", "pred_mem", "cur_cpu", "cur_mem")
    .withColumn("window_ts", lit(ts)).withColumn("target_ts", lit(ts + STEP)).persist()
  val rows = out.select("node_id", "pred_cpu", "pred_mem", "cur_cpu", "cur_mem").collect()
  out.coalesce(1).write.mode("append").parquet(s"$SDIR/predictions")
  if (nDone < FEATURE_LOG_WINDOWS) fRows.coalesce(1).write.mode("append").parquet(s"$SDIR/features_log")
  postBatch(ts, rows)
  // live error: predictions made one window ago vs the actuals that just arrived
  var liveMsg = "live error: n/a (first window)"
  if (prevTargetTs == ts && prevPreds.nonEmpty) {
    val actual = win.filter(col("cpu_usage_norm").isNotNull).select("node_id", "cpu_usage_norm", "mem_usage_norm")
      .collect().map(r => r.getString(0) -> ((r.getDouble(1), r.getDouble(2)))).toMap
    val pairs = prevPreds.flatMap { case (n, pc, pm, cc, cm) => actual.get(n).map { case (ac, am) => (pc, pm, cc, cm, ac, am) } }
    if (pairs.nonEmpty) {
      val n = pairs.length.toDouble
      val eCm = pairs.map(p => math.pow(p._1 - p._5, 2)).sum; val eCp = pairs.map(p => math.pow(p._3 - p._5, 2)).sum
      val eMm = pairs.map(p => math.pow(p._2 - p._6, 2)).sum; val eMp = pairs.map(p => math.pow(p._4 - p._6, 2)).sum
      sseCm += eCm; sseCp += eCp; sseMm += eMm; sseMp += eMp; nTot += n
      liveMsg = f"cpu RMSE model=${math.sqrt(eCm / n)}%.4f persist=${math.sqrt(eCp / n)}%.4f | mem model=${math.sqrt(eMm / n)}%.4f persist=${math.sqrt(eMp / n)}%.4f" +
        f" | running cpu ${math.sqrt(sseCm / nTot)}%.4f vs ${math.sqrt(sseCp / nTot)}%.4f, mem ${math.sqrt(sseMm / nTot)}%.4f vs ${math.sqrt(sseMp / nTot)}%.4f"
      spark.createDataFrame(Seq((ts, pairs.length, math.sqrt(eCm / n), math.sqrt(eCp / n), math.sqrt(eMm / n), math.sqrt(eMp / n))))
        .toDF("window_ts", "n", "rmse_cpu_model", "rmse_cpu_persist", "rmse_mem_model", "rmse_mem_persist")
        .coalesce(1).write.mode("append").parquet(s"$SDIR/live_error")
    }
  }
  prevPreds = rows.map(r => (r.getString(0), r.getDouble(1), r.getDouble(2), r.getDouble(3), r.getDouble(4)))
  prevTargetTs = ts + STEP
  hist = cur.filter(col("window_ts") >= ts - (HIST_WIN - 1) * STEP)
  fRows.unpersist(); out.unpersist()
  nDone += 1
  val secs = (System.nanoTime() - tStart) / 1e9
  println(f"[window $ts] scored ${rows.length}/$nReady machines in $secs%.1fs | $liveMsg")
}

// ------------------------- the stream -------------------------
val rawSchema = StructType(Seq(
  StructField("node_id", StringType), StructField("window_ts", LongType),
  StructField("cpu_usage_norm", DoubleType), StructField("mem_usage_norm", DoubleType),
  StructField("n_records", LongType), StructField("sev", IntegerType)))

val query = spark.readStream.schema(rawSchema).option("maxFilesPerTrigger", 1)
  .parquet(s"$SDIR/incoming")
  .writeStream.queryName("resource-predictions")
  .option("checkpointLocation", s"$SDIR/checkpoint")
  .trigger(Trigger.ProcessingTime("5 seconds"))
  .foreachBatch((batch: DataFrame, batchId: Long) => {
    batch.persist()
    val tss = batch.select("window_ts").distinct().collect().map(_.getLong(0)).sorted
    tss.foreach(t => processWindow(batch.filter(col("window_ts") === t), t))
    batch.unpersist()
    ()
  }).start()
println("stream started; waiting for windows...")
query.awaitTermination()
