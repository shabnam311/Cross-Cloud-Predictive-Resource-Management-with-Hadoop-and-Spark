import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{GBTRegressor, GBTRegressionModel, LinearRegression}
import org.apache.spark.storage.StorageLevel

val spark = SparkSession.builder().appName("Phase4-TransferGBT-2Stage-cpu").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

// ------------------------------------------------------------------ CONFIG
val LABEL = "target_cpu_future"
val HORIZON_SEC = 300.0            // MUST match target horizon, in timestamp_sec units
val ALIBABA_SHARE = 0.25
val USE_PROVIDER_SCALING = true
val MAX_TRAIN_ROWS = 3000000L      // per source (Google / Alibaba fit). Lower to 1.5M if executors still die
val MAX_EVAL_ROWS  = 600000L       // cap for validation / calibration sets
val RUN_ZERO_SHOT_ABLATION = true
val OUT = "hdfs://namenode:9000/telemetry/models"

// ------------------------------------------------------------------ LOAD + FEATURES
val baseCols = Array(
  "cpu_usage_norm", "mem_usage_norm", "time_delta_sec", "error_count",
  "task_arrival_count", "instance_arrival_count", "cpu_lag_1", "cpu_lag_2",
  "cpu_avg_5m", "cpu_avg_15m", "mem_lag_1", "mem_lag_2", "mem_avg_5m", "mem_avg_15m",
  "task_lag_1", "instance_lag_1", "task_avg_5m", "instance_avg_5m", "task_avg_15m")

val raw = spark.read.parquet("hdfs://namenode:9000/telemetry/processed/ml_modeling_ready.parquet")
  .na.fill(0.0, baseCols)
  .filter(col(LABEL).isNotNull)

val df = raw
  .withColumn("is_google", when(col("cloud_provider") === "google", 1.0).otherwise(0.0))
  .withColumn("cpu_d1", col("cpu_usage_norm") - col("cpu_lag_1"))
  .withColumn("cpu_d2", col("cpu_lag_1") - col("cpu_lag_2"))
  .withColumn("cpu_accel", col("cpu_d1") - col("cpu_d2"))
  .withColumn("cpu_dev5", col("cpu_usage_norm") - col("cpu_avg_5m"))
  .withColumn("cpu_trend", col("cpu_avg_5m") - col("cpu_avg_15m"))
  .withColumn("mem_d1", col("mem_usage_norm") - col("mem_lag_1"))
  .withColumn("mem_d2", col("mem_lag_1") - col("mem_lag_2"))
  .withColumn("mem_dev5", col("mem_usage_norm") - col("mem_avg_5m"))
  .withColumn("mem_trend", col("mem_avg_5m") - col("mem_avg_15m"))
  .withColumn("task_d1", col("task_arrival_count") - col("task_lag_1"))
  .withColumn("task_trend", col("task_avg_5m") - col("task_avg_15m"))
  .withColumn("inst_d1", col("instance_arrival_count") - col("instance_lag_1"))
  .withColumn("cpu_mem_gap", col("cpu_usage_norm") - col("mem_usage_norm"))
  .withColumn("err_per_task", col("error_count") / (col("task_arrival_count") + 1.0))
  .withColumn("resid_label", col(LABEL) - col("cpu_usage_norm"))

val derivedCols = Array("cpu_d1", "cpu_d2", "cpu_accel", "cpu_dev5", "cpu_trend", "mem_d1", "mem_d2",
  "mem_dev5", "mem_trend", "task_d1", "task_trend", "inst_d1", "cpu_mem_gap", "err_per_task")
val statCols = baseCols ++ derivedCols

// ------------------------------------------------------------------ SPLITS (chronological + embargo)
val G = df.filter(col("cloud_provider") === "google")
val A = df.filter(col("cloud_provider") === "alibaba")
val ts = col("timestamp_sec")

val g80 = G.stat.approxQuantile("timestamp_sec", Array(0.8), 0.001)(0)
val aq  = A.stat.approxQuantile("timestamp_sec", Array(0.06, 0.08, 0.10), 0.001)

// Row cap via random sampling (features are precomputed, so this doesn't break temporal structure)
def cap(d: DataFrame, n: Long): (DataFrame, Double) = {
  val c = d.count()
  if (c > n) (d.sample(false, n.toDouble / c, 42L), n.toDouble) else (d, c.toDouble)
}

val (gTrain, nG) = cap(G.filter(ts <= g80 - HORIZON_SEC), MAX_TRAIN_ROWS)
val (aFit,   nA) = cap(A.filter(ts <= aq(0) - HORIZON_SEC), MAX_TRAIN_ROWS)
val (gValid, _)  = cap(G.filter(ts > g80), MAX_EVAL_ROWS)
val (aCal,   _)  = cap(A.filter(ts > aq(0) && ts <= aq(1) - HORIZON_SEC), MAX_EVAL_ROWS)
val (aVal,   _)  = cap(A.filter(ts > aq(1) && ts <= aq(2) - HORIZON_SEC), MAX_EVAL_ROWS)
val aTest = A.filter(ts > aq(2) + HORIZON_SEC)     // full test set, streamed (never cached)

// ------------------------------------------------------------------ PER-PROVIDER SCALING (train rows only)
val aggs = statCols.flatMap(c => Seq(mean(c).as(s"${c}__m"), stddev(c).as(s"${c}__s")))
val stats: Map[String, Map[String, (Double, Double)]] =
  gTrain.unionByName(aFit).groupBy("cloud_provider").agg(aggs.head, aggs.tail: _*).collect().map { r =>
    r.getAs[String]("cloud_provider") -> statCols.map { c =>
      val sd = r.getAs[Double](s"${c}__s")
      c -> (r.getAs[Double](s"${c}__m"), if (sd.isNaN || sd < 1e-9) 1.0 else sd)
    }.toMap
  }.toMap

def standardize(d: DataFrame): DataFrame = {
  val exprs = statCols.map { c =>
    stats.foldLeft(lit(0.0)) { case (e, (p, m)) =>
      val (mu, sd) = m(c)
      when(col("cloud_provider") === p, (col(c) - mu) / sd).otherwise(e)
    }.as(s"${c}_z")
  }
  d.select(col("*") +: exprs: _*)
}

val featNames = if (USE_PROVIDER_SCALING) statCols.map(_ + "_z") :+ "is_google" else statCols :+ "is_google"
val assembler = new VectorAssembler().setInputCols(featNames).setOutputCol("features")
val keep = Seq("features", "resid_label", LABEL, "cpu_usage_norm", "cpu_d1", "cpu_avg_5m",
  "mem_usage_norm", "is_google").map(col) ++ Seq(col("w"))

def prep(d: DataFrame, withW: Boolean = false): DataFrame = {
  val base = if (withW) d else d.withColumn("w", lit(1.0))
  val s = if (USE_PROVIDER_SCALING) standardize(base) else base
  assembler.transform(s).select(keep: _*)
}

// ------------------------------------------------------------------ INSTANCE WEIGHTING
val wA = math.min(100.0, (ALIBABA_SHARE / (1.0 - ALIBABA_SHARE)) * nG / nA)
println(f"Rows: googleTrain~$nG%.0f alibabaFit~$nA%.0f -> alibaba weight=$wA%.2f")

// The ONLY persisted dataset. Everything else is recomputed from parquet on demand.
val trainPool = prep(gTrain.withColumn("w", lit(1.0)).unionByName(aFit.withColumn("w", lit(wA))), withW = true)
  .persist(StorageLevel.MEMORY_AND_DISK)
trainPool.count()

val gValidF = prep(gValid)
val aCalF   = prep(aCal)
val aValF   = prep(aVal)
val aTestF  = prep(aTest)

// ------------------------------------------------------------------ METRICS (single pass)
def metrics(d: DataFrame, pred: org.apache.spark.sql.Column): (Double, Double, Double) = {
  val r = d.select(pred.as("p"), col(LABEL).as("y"))
    .agg(count(lit(1)).as("n"), sum(pow(col("y") - col("p"), 2)).as("sse"),
         sum(abs(col("y") - col("p"))).as("sae"), var_pop("y").as("vy")).head
  val n = r.getLong(0).toDouble
  val sse = r.getDouble(1)
  (math.sqrt(sse / n), r.getDouble(2) / n, 1.0 - sse / n / r.getDouble(3))
}
val persist = col("cpu_usage_norm")
val gBase = metrics(gValidF, persist)
val aBase = metrics(aValF, persist)

// ------------------------------------------------------------------ STAGE A
def fitGbt(train: DataFrame, depth: Int, iters: Int, weighted: Boolean): GBTRegressionModel = {
  val g = new GBTRegressor()
    .setLabelCol("resid_label").setFeaturesCol("features")
    .setMaxDepth(depth).setMaxIter(iters).setStepSize(0.1)
    .setSubsamplingRate(0.5).setFeatureSubsetStrategy("0.8")
    .setMaxBins(32).setMinInstancesPerNode(50).setSeed(42)
  (if (weighted) g.setWeightCol("w") else g).fit(train)
}

val grid = Seq((4, 100), (6, 100), (7, 150))     // shallow first: cheap and fails last
var bestA: GBTRegressionModel = null
var bestScore = Double.MaxValue
var bestCfg = (0, 0)

for ((d, it) <- grid) {
  val m = fitGbt(trainPool, d, it, weighted = true)
  val g = metrics(m.transform(gValidF), persist + col("prediction"))
  val a = metrics(m.transform(aValF), persist + col("prediction"))
  val score = 0.5 * (g._1 / gBase._1) + 0.5 * (a._1 / aBase._1)
  println(f"depth=$d iters=$it -> Google RMSE ${g._1}%.5f (base ${gBase._1}%.5f) | Alibaba-val RMSE ${a._1}%.5f (base ${aBase._1}%.5f) | score $score%.4f")
  if (score < bestScore) { bestScore = score; bestA = m; bestCfg = (d, it) }
}
println(s"\nWinning Stage-A config: depth=${bestCfg._1}, iters=${bestCfg._2}")

// ------------------------------------------------------------------ STAGE B
val bFeats = Array("stageA_res", "cpu_usage_norm", "cpu_d1", "cpu_avg_5m", "mem_usage_norm")
val bAsm = new VectorAssembler().setInputCols(bFeats).setOutputCol("bfeatures")

def withA(d: DataFrame): DataFrame = bestA.transform(d)
  .withColumnRenamed("prediction", "stageA_res")
  .withColumn("corr_label", col("resid_label") - col("stageA_res"))

val stageB = new LinearRegression()
  .setLabelCol("corr_label").setFeaturesCol("bfeatures")
  .setRegParam(0.1).setElasticNetParam(0.0).setStandardization(true)
  .fit(bAsm.transform(withA(aCalF)))

def evalAlibaba(d: DataFrame) = {
  val da = withA(d)
  val mA  = metrics(da, persist + col("stageA_res"))
  val mAB = metrics(stageB.transform(bAsm.transform(da)), persist + col("stageA_res") + col("prediction"))
  (mA, mAB)
}
val (aValA, aValAB) = evalAlibaba(aValF)
val useB = aValAB._1 < aValA._1
println(f"Stage B on Alibaba-val: A-only RMSE ${aValA._1}%.5f -> A+B RMSE ${aValAB._1}%.5f | using Stage B: $useB")

// ------------------------------------------------------------------ FINAL TEST + ABLATION
val gFinal = metrics(bestA.transform(gValidF), persist + col("prediction"))
val (tA, tAB) = evalAlibaba(aTestF)
val tFinal = if (useB) tAB else tA
val tBase  = metrics(aTestF, persist)
val tZero =
  if (RUN_ZERO_SHOT_ABLATION) {
    val z = fitGbt(trainPool.filter(col("is_google") === 1.0), bestCfg._1, bestCfg._2, weighted = false)
    Some(metrics(z.transform(aTestF), persist + col("prediction")))
  } else None

def row(name: String, m: (Double, Double, Double), base: Double) =
  println(f"$name%-34s RMSE ${m._1}%.5f | MAE ${m._2}%.5f | R2 ${m._3}%.4f | vs persistence ${(1 - m._1 / base) * 100}%+.1f%%")

println("\n=== RESULTS (GBT, residual target, 2-stage transfer) ===")
row("Google valid: persistence", gBase, gBase._1)
row("Google valid: Stage A", gFinal, gBase._1)
row("Alibaba test: persistence", tBase, tBase._1)
tZero.foreach(z => row("Alibaba test: zero-shot (Google)", z, tBase._1))
row("Alibaba test: pooled+weighted (A)", tA, tBase._1)
row("Alibaba test: A + Stage B", tAB, tBase._1)
println(s"Reported final Alibaba model: ${if (useB) "A+B" else "A only"}")

// ------------------------------------------------------------------ SAVE
bestA.write.overwrite().save(s"$OUT/transfer_gbt_stageA_cpu")
stageB.write.overwrite().save(s"$OUT/transfer_gbt_stageB_cpu")
import spark.implicits._
stats.toSeq.flatMap { case (p, m) => m.map { case (c, (mu, sd)) => (p, c, mu, sd) } }
  .toDF("cloud_provider", "col", "mean", "std")
  .write.mode("overwrite").parquet(s"$OUT/transfer_gbt_scaling_stats")

trainPool.unpersist()
sys.exit(0)