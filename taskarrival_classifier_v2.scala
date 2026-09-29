import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{VectorAssembler, UnivariateFeatureSelector, StringIndexer, OneHotEncoder}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.functions._

val spark = SparkSession.builder()
  .appName("Phase5-RFTaskArrivalClassifier")
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

// 3. Pool the Training Data (Transfer Learning) — same mechanism as your working CPU scripts
val pooledTrainData = googleTrain.unionByName(alibabaWarmup).cache()

println(s"Transfer Learning (Hurdle Classifier): Google Train + 10% Alibaba Warmup (<= $alibabaSplitTime)")

// 4. Class weighting on the POOLED set (idle-majority bias correction)
val counts = pooledTrainData.groupBy("is_active_future").count().collect()
  .map(r => r.getDouble(0) -> r.getLong(1)).toMap
val total = counts.values.sum.toDouble
val weightFor0 = total / (2.0 * counts.getOrElse(0.0, 1L).toDouble)
val weightFor1 = total / (2.0 * counts.getOrElse(1.0, 1L).toDouble)
println(s"Pooled class weight (idle)=$weightFor0   class weight (active)=$weightFor1")

val weightedPooled = pooledTrainData.withColumn("class_weight",
  when(col("is_active_future") === 1.0, lit(weightFor1)).otherwise(lit(weightFor0)))

// 5. Feature Engineering — cross-domain (cpu/mem/error/instance) + arrival-specific + cloud flag
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
  .setFeatureType("continuous").setLabelType("categorical").setSelectionMode("numTopFeatures")
  .setSelectionThreshold(12)
  .setFeaturesCol("raw_features").setLabelCol("is_active_future").setOutputCol("features")

// 6. Grid Search (mirrors randomcpu.scala's search pattern, selecting by AUC not RMSE)
val numTreesParams = Array(30, 40, 50)
val maxDepthParams = Array(6, 7, 8)

val aucEvaluator = new BinaryClassificationEvaluator().setLabelCol("is_active_future").setRawPredictionCol("rawPrediction")

var bestModel: org.apache.spark.ml.PipelineModel = null
var bestAuc = Double.MinValue
var bestParams = ""

println("Starting Transfer Learning Grid Search for Task Arrival Hurdle Classifier...")

for (nt <- numTreesParams; md <- maxDepthParams) {
  val rf = new RandomForestClassifier()
    .setLabelCol("is_active_future")
    .setFeaturesCol("features")
    .setWeightCol("class_weight")
    .setNumTrees(nt)
    .setMaxDepth(md)
    .setSeed(42)

  val pipeline = new Pipeline().setStages(Array(indexer, encoder, assembler, selector, rf))
  val model = pipeline.fit(weightedPooled)

  val preds = model.transform(googleValid)
  val auc = aucEvaluator.evaluate(preds)

  println(s"Tested numTrees=$nt, maxDepth=$md -> Valid AUC: $auc")

  if (auc > bestAuc) {
    bestAuc = auc
    bestModel = model
    bestParams = s"numTrees=$nt, maxDepth=$md"
  }
}

println(s"\nWinning Classifier Hyperparameters: $bestParams")

// 7. Final Evaluation — Experiment A (Google future) vs Experiment B (Alibaba 90%)
val googleAuc = aucEvaluator.evaluate(bestModel.transform(googleValid))
val alibabaAuc = aucEvaluator.evaluate(bestModel.transform(alibabaTest))

println("\n=== TRANSFER LEARNING RESULTS (HURDLE CLASSIFIER) ===")
println(s"Experiment A (Google Future) -> AUC: $googleAuc")
println(s"Experiment B (Alibaba 90%)   -> AUC: $alibabaAuc")

// 8. Save
println("Saving winning hurdle classifier PipelineModel to HDFS...")
bestModel.write.overwrite().save("hdfs://namenode-cloud:9000/telemetry/models/transfer_rf_task_arrival_classifier")
println("Task Arrival Hurdle Classifier successfully saved.")

pooledTrainData.unpersist()
googleValid.unpersist()
alibabaTest.unpersist()

sys.exit(0)
