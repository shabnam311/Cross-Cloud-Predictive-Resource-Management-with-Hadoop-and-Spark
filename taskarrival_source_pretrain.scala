import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.{RandomForestClassifier, RandomForestClassificationModel}
import org.apache.spark.ml.regression.{GBTRegressor, GBTRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

val spark = SparkSession.builder()
  .appName("TaskArrivalSourcePretrain_Google")
  .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

println("=====================================================")
println(" Starting SOURCE model pretraining (Google trace)")
println("=====================================================")

val FEATURE_COLS = Array(
  "arrival_lag_1", "arrival_lag_2", "arrival_lag_3",
  "rolling_mean_5", "rolling_std_5", "rolling_max_5",
  "hour_sin", "hour_cos", "day_of_week"
)

// 1. Load Google partition only
println("[1/5] Loading Google feature partition...")
val googleDF = spark.read.parquet("hdfs://namenode-cloud:9000/telemetry/processed/task_arrival_features/all_clouds.parquet")
  .filter(col("cloud_provider") === "google")

val Array(trainDF, testDF) = googleDF.randomSplit(Array(0.8, 0.2), seed = 42)
trainDF.cache()

// 2. Class weighting for the hurdle classifier — counteracts the idle-majority bias
println("[2/5] Computing class weights from observed imbalance...")
val counts = trainDF.groupBy("is_active").count().collect()
  .map(r => r.getDouble(0) -> r.getLong(1)).toMap
val total = counts.values.sum.toDouble
val weightFor0 = total / (2.0 * counts.getOrElse(0.0, 1L).toDouble)
val weightFor1 = total / (2.0 * counts.getOrElse(1.0, 1L).toDouble)
println(s"   class weight (idle)=$weightFor0   class weight (active)=$weightFor1")

val weightedTrain = trainDF.withColumn("class_weight",
  when(col("is_active") === 1.0, lit(weightFor1)).otherwise(lit(weightFor0)))

// 3. Assemble features
val assembler = new VectorAssembler().setInputCols(FEATURE_COLS).setOutputCol("features")
val assembledTrain = assembler.transform(weightedTrain).cache()
val assembledTest = assembler.transform(testDF).cache()

// 4. PART A — hurdle classifier (idle vs active)
println("[3/5] Training Part A: RandomForestClassifier (idle vs active)...")
val rf = new RandomForestClassifier()
  .setLabelCol("is_active")
  .setFeaturesCol("features")
  .setWeightCol("class_weight")
  .setNumTrees(50)
  .setMaxDepth(6)
  .setSeed(42)

val classifierModel = rf.fit(assembledTrain)

// 5. PART B — magnitude regressor, trained ONLY on active (non-idle) rows, on the log1p target
println("[4/5] Training Part B: GBTRegressor on active-only subset (log1p target)...")
val activeTrain = assembledTrain.filter(col("is_active") === 1.0)

val gbt = new GBTRegressor()
  .setLabelCol("log1p_arrival_count")
  .setFeaturesCol("features")
  .setMaxIter(50)
  .setMaxDepth(5)
  .setStepSize(0.05)
  .setSeed(42)

val regressorModel = gbt.fit(activeTrain)

// 6. Persist both source models
println("[5/5] Saving source models to HDFS...")
classifierModel.write.overwrite().save("hdfs://namenode-cloud:9000/telemetry/models/task_arrival_source_classifier_google")
regressorModel.write.overwrite().save("hdfs://namenode-cloud:9000/telemetry/models/task_arrival_source_regressor_google")

// Quick source-domain sanity metrics
val classPreds = classifierModel.transform(assembledTest)
val aucEval = new BinaryClassificationEvaluator().setLabelCol("is_active").setRawPredictionCol("rawPrediction")
println(s"   [Source] Classifier AUC (test, Google): ${aucEval.evaluate(classPreds)}")

println("=====================================================")
println(" SOURCE model pretraining complete!")
println("=====================================================")

sys.exit(0)
