import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.regression.GBTRegressionModel
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.linalg.Vector

val spark = SparkSession.builder()
  .appName("TaskArrivalEvaluation")
  .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

println("=====================================================")
println(" Evaluating Task Arrival Rate models (Target: Alibaba)")
println("=====================================================")

// 1. Load the pre-saved, already-transferred test set
println("[1/4] Loading transformed Alibaba test set...")
val testTransfer = spark.read.parquet(
  "hdfs://namenode-cloud:9000/telemetry/processed/task_arrival_features/alibaba_test_transfer.parquet")

val FEATURE_COLS = Array(
  "arrival_lag_1", "arrival_lag_2", "arrival_lag_3",
  "rolling_mean_5", "rolling_std_5", "rolling_max_5",
  "hour_sin", "hour_cos", "day_of_week",
  "source_active_prob", "source_log1p_magnitude"
)

import org.apache.spark.ml.feature.VectorAssembler
val transferAssembler = new VectorAssembler().setInputCols(FEATURE_COLS).setOutputCol("features_transfer")
val assembled = transferAssembler.transform(testTransfer)

// 2. Load target models and predict
println("[2/4] Loading target (transfer-learned) models and predicting...")
val targetClassifier = RandomForestClassificationModel.load(
  "hdfs://namenode-cloud:9000/telemetry/models/task_arrival_target_classifier_alibaba")
val targetRegressor = GBTRegressionModel.load(
  "hdfs://namenode-cloud:9000/telemetry/models/task_arrival_target_regressor_alibaba")

val classPreds = targetClassifier.transform(assembled)

// 3. PART A metrics: hurdle / idle-vs-active classification
println("[3/4] Part A metrics — idle vs active classification")
val aucEval = new BinaryClassificationEvaluator().setLabelCol("is_active").setRawPredictionCol("rawPrediction")
val f1Eval = new MulticlassClassificationEvaluator().setLabelCol("is_active").setPredictionCol("prediction").setMetricName("f1")
val precEval = new MulticlassClassificationEvaluator().setLabelCol("is_active").setPredictionCol("prediction").setMetricName("weightedPrecision")
val recEval = new MulticlassClassificationEvaluator().setLabelCol("is_active").setPredictionCol("prediction").setMetricName("weightedRecall")

println(s"   AUC-ROC            : ${aucEval.evaluate(classPreds)}")
println(s"   Weighted F1         : ${f1Eval.evaluate(classPreds)}")
println(s"   Weighted Precision  : ${precEval.evaluate(classPreds)}")
println(s"   Weighted Recall     : ${recEval.evaluate(classPreds)}")

println("   Confusion matrix (rows=actual, cols=predicted):")
classPreds.groupBy("is_active").pivot("prediction").count().orderBy("is_active").show(false)

// 4. PART B metrics: magnitude regression, on ACTIVE rows only, un-logged back to real units
println("[4/4] Part B metrics — arrival-count regression (active rows only, real units)")

val activeOnly = assembled.filter(col("is_active") === 1.0)
val regPreds = targetRegressor.transform(activeOnly)
  .withColumn("predicted_arrival_count", expm1(col("prediction")))
  .withColumn("actual_arrival_count", col("arrival_count"))

val rmseEval = new RegressionEvaluator().setLabelCol("log1p_arrival_count").setPredictionCol("prediction").setMetricName("rmse")
val maeEval = new RegressionEvaluator().setLabelCol("log1p_arrival_count").setPredictionCol("prediction").setMetricName("mae")
val r2Eval = new RegressionEvaluator().setLabelCol("log1p_arrival_count").setPredictionCol("prediction").setMetricName("r2")

println(s"   RMSE (log1p space)         : ${rmseEval.evaluate(regPreds)}")
println(s"   MAE  (log1p space)         : ${maeEval.evaluate(regPreds)}")
println(s"   R^2  (log1p space)         : ${r2Eval.evaluate(regPreds)}")

// Real-unit RMSE/MAE
val realUnitStats = regPreds.select(
  sqrt(avg(pow(col("actual_arrival_count") - col("predicted_arrival_count"), 2))).alias("rmse_real"),
  avg(abs(col("actual_arrival_count") - col("predicted_arrival_count"))).alias("mae_real")
).first()

println(s"   RMSE (real arrival units)  : ${realUnitStats.getAs[Double]("rmse_real")}")
println(s"   MAE  (real arrival units)  : ${realUnitStats.getAs[Double]("mae_real")}")

// Poisson deviance
val poissonDev = regPreds.select(
  avg(
    lit(2.0) * (
      when(col("actual_arrival_count") === 0, col("predicted_arrival_count"))
        .otherwise(col("actual_arrival_count") * log(col("actual_arrival_count") / col("predicted_arrival_count"))
          - (col("actual_arrival_count") - col("predicted_arrival_count")))
    )
  ).alias("mean_poisson_deviance")
).first()

println(s"   Mean Poisson Deviance      : ${poissonDev.getAs[Double]("mean_poisson_deviance")}")

regPreds.select("machine_id", "bucket_ts", "actual_arrival_count", "predicted_arrival_count").show(20, false)

println("=====================================================")
println(" Evaluation Complete!")
println("=====================================================")

sys.exit(0)
