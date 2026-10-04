import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{VectorAssembler, UnivariateFeatureSelector, StringIndexer, OneHotEncoder}
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.functions._

val spark = SparkSession.builder()
  .appName("Phase5-GBTTaskArrivalRegressor")
  .config("spark.eventLog.enabled", "true")
  .config("spark.eventLog.dir", "hdfs://namenode-cloud:9000/spark-events")
  .config("spark.executor.memoryOverhead", "1024m")
  .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

val df = spark.read.parquet("hdfs://namenode-cloud:9000/telemetry/processed/task_arrival_features_v2/all_clouds.parquet")

val googleDF = df.filter("cloud_provider = 'google'")
val alibabaDF = df.filter("cloud_provider = 'alibaba'")

// 1. Google Chronological Split (80% Train / 20% Valid)
val googleSplitTime = googleDF.stat.approxQuantile("timestamp_sec", Array(0.8), 0.01)(0)
val googleTrain = googleDF.filter(col("timestamp_sec") <= googleSplitTime)
val googleValid = googleDF.filter(col("timestamp_sec") > googleSplitTime).cache()

// 2. Alibaba Chronological Split (10% Warm-up / 90% Test)
val alibabaSplitTime = alibabaDF.stat.approxQuantile("timestamp_sec", Array(0.1), 0.01)(0)
val alibabaWarmup = alibabaDF.filter(col("timestamp_sec") <= alibabaSplitTime)
val alibabaTest = alibabaDF.filter(col("timestamp_sec") > alibabaSplitTime).cache()

// 3. Pool the Training Data — CRITICAL: filter to active-only AFTER pooling, not before,
//    so the pool ratio of Google:Alibaba active rows still matches the intended 80:10 mix.
val pooledTrainData = googleTrain.unionByName(alibabaWarmup)
  .filter(col("is_active_future") === 1.0)
  .cache()

println(s"Transfer Learning (Hurdle Regressor): Google Train + 10% Alibaba Warmup (<= $alibabaSplitTime), active-only")

// 4. Feature Engineering — identical feature universe to the classifier (Part 3)
val indexer = new StringIndexer().setInputCol("cloud_provider").setOutputCol("cloud_idx")
val encoder = new OneHotEncoder().setInputCol("cloud_idx").setOutputCol("cloud_vec")

val featureCols = Array(
  "cloud_vec", "cpu_usage_norm", "mem_usage_norm", "error_count",
  "task_arrival_count", "instance_arrival_count", "cpu_lag_1", "cpu_lag_2",
  "cpu_avg_5m", "cpu_avg_15m", "mem_lag_1", "mem_lag_2", "mem_avg_5m", "mem_avg_15m",
  "task_lag_1", "instance_lag_1", "task_avg_5m", "instance_avg_5m", "task_avg_15m",
  "arrival_lag_1", "arrival_rolling_mean_5", "arrival_rolling_std_5", "hour_sin", "hour_cos"
)

val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("raw_features")

val selector = new UnivariateFeatureSelector()
  .setFeatureType("continuous").setLabelType("continuous").setSelectionMode("numTopFeatures")
  .setSelectionThreshold(12)
  .setFeaturesCol("raw_features").setLabelCol("log1p_target_arrival_future").setOutputCol("features")

// 5. Grid Search (identical shape to gradientcpu.scala)
val maxIterParams = Array(20, 30, 50)
val maxDepthParams = Array(3, 4, 5)

val evaluator = new RegressionEvaluator().setLabelCol("log1p_target_arrival_future").setPredictionCol("prediction")

var bestModel: org.apache.spark.ml.PipelineModel = null
var bestRmse = Double.MaxValue
var bestParams = ""

println("Starting Transfer Learning Grid Search for Task Arrival Hurdle Regressor...")

for (mi <- maxIterParams; md <- maxDepthParams) {
  val gbt = new GBTRegressor()
    .setLabelCol("log1p_target_arrival_future")
    .setFeaturesCol("features")
    .setMaxIter(mi)
    .setMaxDepth(md)
    .setSeed(42)

  val pipeline = new Pipeline().setStages(Array(indexer, encoder, assembler, selector, gbt))
  val model = pipeline.fit(pooledTrainData)

  val preds = model.transform(googleValid.filter(col("is_active_future") === 1.0))
  val rmse = evaluator.setMetricName("rmse").evaluate(preds)

  println(s"Tested maxIter=$mi, maxDepth=$md -> Valid RMSE (log1p space): $rmse")

  if (rmse < bestRmse) {
    bestRmse = rmse
    bestModel = model
    bestParams = s"maxIter=$mi, maxDepth=$md"
  }
}

println(s"\nWinning Regressor Hyperparameters: $bestParams")

// 6. Final Evaluation — Experiment A (Google future, active-only) vs Experiment B (Alibaba 90%, active-only)
val googleActiveValid = googleValid.filter(col("is_active_future") === 1.0)
val alibabaActiveTest = alibabaTest.filter(col("is_active_future") === 1.0)

val googlePreds = bestModel.transform(googleActiveValid)
val alibabaPreds = bestModel.transform(alibabaActiveTest)

val googleRmse = evaluator.setMetricName("rmse").evaluate(googlePreds)
val googleR2 = evaluator.setMetricName("r2").evaluate(googlePreds)
val alibabaRmse = evaluator.setMetricName("rmse").evaluate(alibabaPreds)
val alibabaR2 = evaluator.setMetricName("r2").evaluate(alibabaPreds)

println("\n=== TRANSFER LEARNING RESULTS (HURDLE REGRESSOR, active-only, log1p space) ===")
println(s"Experiment A (Google Future) -> RMSE: $googleRmse | R2: $googleR2")
println(s"Experiment B (Alibaba 90%)   -> RMSE: $alibabaRmse | R2: $alibabaR2")

// 7. Save
println("Saving winning hurdle regressor PipelineModel to HDFS...")
bestModel.write.overwrite().save("hdfs://namenode-cloud:9000/telemetry/models/transfer_gbt_task_arrival_regressor")
println("Task Arrival Hurdle Regressor successfully saved.")

pooledTrainData.unpersist()
googleValid.unpersist()
alibabaTest.unpersist()

sys.exit(0)
