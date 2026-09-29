import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.{RandomForestClassifier, RandomForestClassificationModel}
import org.apache.spark.ml.regression.{GBTRegressor, GBTRegressionModel}
import org.apache.spark.ml.linalg.Vector

val spark = SparkSession.builder()
  .appName("TaskArrivalTransferFinetune_Alibaba")
  .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

println("=====================================================")
println(" Starting TARGET model transfer + fine-tune (Alibaba)")
println("=====================================================")

val FEATURE_COLS = Array(
  "arrival_lag_1", "arrival_lag_2", "arrival_lag_3",
  "rolling_mean_5", "rolling_std_5", "rolling_max_5",
  "hour_sin", "hour_cos", "day_of_week"
)

// 1. Load Alibaba partition (the smaller / target-domain data)
println("[1/6] Loading Alibaba feature partition...")
val alibabaDF = spark.read.parquet("hdfs://namenode-cloud:9000/telemetry/processed/task_arrival_features/all_clouds.parquet")
  .filter(col("cloud_provider") === "alibaba")

val Array(trainDF, testDF) = alibabaDF.randomSplit(Array(0.8, 0.2), seed = 42)

// 2. Load the FROZEN source models trained on Google (Part 4a)
println("[2/6] Loading frozen source models pretrained on Google...")
val assembler = new VectorAssembler().setInputCols(FEATURE_COLS).setOutputCol("features")

val sourceClassifier = RandomForestClassificationModel.load(
  "hdfs://namenode-cloud:9000/telemetry/models/task_arrival_source_classifier_google")
val sourceRegressor = GBTRegressionModel.load(
  "hdfs://namenode-cloud:9000/telemetry/models/task_arrival_source_regressor_google")

// 3. Generate "teacher signal" columns from the frozen source models on Alibaba data
println("[3/6] Computing transferred source-model predictions as extra features...")

def extractProb1 = udf((v: Vector) => v(1))

def addTransferFeatures(df: org.apache.spark.sql.DataFrame) = {
  val withFeatures = assembler.transform(df)
  val withSourceProb = sourceClassifier.transform(withFeatures)
    .withColumn("source_active_prob", extractProb1(col("probability")))
    .drop("rawPrediction", "probability", "prediction")
  val withSourceMag = sourceRegressor.transform(withSourceProb)
    .withColumnRenamed("prediction", "source_log1p_magnitude")
    .drop("features")
  withSourceMag
}

val trainTransfer = addTransferFeatures(trainDF).cache()
val testTransfer = addTransferFeatures(testDF).cache()

// 4. Reassemble features to INCLUDE the two transferred columns
val TRANSFER_FEATURE_COLS = FEATURE_COLS ++ Array("source_active_prob", "source_log1p_magnitude")
val transferAssembler = new VectorAssembler().setInputCols(TRANSFER_FEATURE_COLS).setOutputCol("features_transfer")

// 5. Class weighting
println("[4/6] Computing Alibaba-specific class weights...")
val counts = trainDF.groupBy("is_active").count().collect()
  .map(r => r.getDouble(0) -> r.getLong(1)).toMap
val total = counts.values.sum.toDouble
val weightFor0 = total / (2.0 * counts.getOrElse(0.0, 1L).toDouble)
val weightFor1 = total / (2.0 * counts.getOrElse(1.0, 1L).toDouble)
println(s"   Alibaba class weight (idle)=$weightFor0   class weight (active)=$weightFor1")

val weightedTrainTransfer = transferAssembler.transform(
  trainTransfer.withColumn("class_weight",
    when(col("is_active") === 1.0, lit(weightFor1)).otherwise(lit(weightFor0))))

// 6. Fine-tune Part A on Alibaba, WITH the transferred source signal included
println("[5/6] Fine-tuning target classifier + regressor on Alibaba with transferred features...")

val rfTarget = new RandomForestClassifier()
  .setLabelCol("is_active")
  .setFeaturesCol("features_transfer")
  .setWeightCol("class_weight")
  .setNumTrees(50)
  .setMaxDepth(6)
  .setSeed(42)

val targetClassifierModel = rfTarget.fit(weightedTrainTransfer)

val activeTrainTransfer = transferAssembler.transform(trainTransfer.filter(col("is_active") === 1.0))

val gbtTarget = new GBTRegressor()
  .setLabelCol("log1p_arrival_count")
  .setFeaturesCol("features_transfer")
  .setMaxIter(50)
  .setMaxDepth(5)
  .setStepSize(0.05)
  .setSeed(42)

val targetRegressorModel = gbtTarget.fit(activeTrainTransfer)

// 7. Persist target models + the transformed test set
println("[6/6] Saving target models and transformed test data...")
targetClassifierModel.write.overwrite().save("hdfs://namenode-cloud:9000/telemetry/models/task_arrival_target_classifier_alibaba")
targetRegressorModel.write.overwrite().save("hdfs://namenode-cloud:9000/telemetry/models/task_arrival_target_regressor_alibaba")

testTransfer.write.mode("overwrite")
  .parquet("hdfs://namenode-cloud:9000/telemetry/processed/task_arrival_features/alibaba_test_transfer.parquet")

println("=====================================================")
println(" TARGET transfer + fine-tune complete!")
println("=====================================================")

sys.exit(0)
