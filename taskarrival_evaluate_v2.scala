import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}

val spark = SparkSession.builder()
  .appName("Phase5-TaskArrivalHurdleEvaluation")
  .config("spark.eventLog.enabled", "true")
  .config("spark.eventLog.dir", "hdfs://namenode-cloud:9000/spark-events")
  .config("spark.sql.shuffle.partitions", "32")
  .config("spark.sql.parquet.enableVectorizedReader", "false")
  .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

println("=====================================================")
println(" Evaluating Combined Hurdle Model (Classifier + Regressor)")
println("=====================================================")

// 1. Load feature data and both saved PipelineModels
println("[1/4] Loading feature data and both hurdle models...")
val df = spark.read.parquet("hdfs://namenode-cloud:9000/telemetry/processed/task_arrival_features_v2/all_clouds.parquet")
val alibabaDF = df.filter("cloud_provider = 'alibaba'")

val alibabaSplitTime = alibabaDF.stat.approxQuantile("timestamp_sec", Array(0.1), 0.01)(0)
val alibabaTest = alibabaDF.filter(col("timestamp_sec") > alibabaSplitTime)

val classifierModel = PipelineModel.load("hdfs://namenode-cloud:9000/telemetry/models/transfer_rf_task_arrival_classifier")
val regressorModel = PipelineModel.load("hdfs://namenode-cloud:9000/telemetry/models/transfer_gbt_task_arrival_regressor")

// 2. Part A — Classification on Alibaba Test Set
println("\n[2/4] Part A: Evaluating Classifier (Idle vs Active)...")
val dropCols = Seq("cloud_idx", "cloud_vec", "raw_features", "features")
val classRaw = classifierModel.transform(alibabaTest)

val aucEval = new BinaryClassificationEvaluator().setLabelCol("is_active_future").setRawPredictionCol("rawPrediction")
val f1Eval = new MulticlassClassificationEvaluator().setLabelCol("is_active_future").setPredictionCol("prediction").setMetricName("f1")

val aucVal = aucEval.evaluate(classRaw)
val f1Val = f1Eval.evaluate(classRaw)

println(s"   AUC-ROC     : $aucVal")
println(s"   Weighted F1 : $f1Val")

val fullClassPreds = classRaw.withColumnRenamed("prediction", "class_pred")
val cleanClassPreds = dropCols.foldLeft(fullClassPreds)((d, c) => if (d.columns.contains(c)) d.drop(c) else d)

// 3. Part B & Part C — Regressor & Combined Hurdle Pipeline
println("\n[3/4] Part B & C: Generating Regressor Predictions & Combining Hurdle Pipeline...")
val activeOnlyToRegress = cleanClassPreds.filter(col("class_pred") === 1.0)
val activePreds = regressorModel.transform(activeOnlyToRegress)
  .withColumn("final_predicted_arrival", expm1(col("prediction")))
  .select("machine_id", "timestamp_sec", "is_active_future", "target_arrival_future", "class_pred", "final_predicted_arrival")

val idlePreds = cleanClassPreds.filter(col("class_pred") === 0.0)
  .withColumn("final_predicted_arrival", lit(0.0))
  .select("machine_id", "timestamp_sec", "is_active_future", "target_arrival_future", "class_pred", "final_predicted_arrival")

val combined = activePreds.unionByName(idlePreds).cache()

// 4. Compute all metrics in consolidated aggregations
println("\n[4/4] Computing Consolidated Metrics...")

// Confusion Matrix
println("\n--- CONFUSION MATRIX (Alibaba 90% Test Set) ---")
combined.groupBy("is_active_future").pivot("class_pred").count().orderBy("is_active_future").show(false)

// Active-Only Regression Metrics (Real Arrival Units)
val activeStats = combined.filter("is_active_future = 1.0").select(
  sqrt(avg(pow(col("target_arrival_future") - col("final_predicted_arrival"), 2))).alias("rmse_active"),
  avg(abs(col("target_arrival_future") - col("final_predicted_arrival"))).alias("mae_active")
).first()

val rmseActive = activeStats.getAs[Double]("rmse_active")
val maeActive = activeStats.getAs[Double]("mae_active")

// End-to-End Hurdle Metrics (All Rows)
val combinedStats = combined.select(
  sqrt(avg(pow(col("target_arrival_future") - col("final_predicted_arrival"), 2))).alias("rmse_combined"),
  avg(abs(col("target_arrival_future") - col("final_predicted_arrival"))).alias("mae_combined")
).first()

val rmseComb = combinedStats.getAs[Double]("rmse_combined")
val maeComb = combinedStats.getAs[Double]("mae_combined")

println("=========================================================================")
println("          TASK ARRIVAL RATE — REVISED V2 TRANSFER LEARNING RESULTS       ")
println("=========================================================================")
println(s"  [Part A] Hurdle Classifier AUC-ROC      : $aucVal")
println(s"  [Part A] Hurdle Classifier Weighted F1  : $f1Val")
println(s"  [Part B] Active-Only Regressor RMSE     : $rmseActive (real arrival units)")
println(s"  [Part B] Active-Only Regressor MAE      : $maeActive (real arrival units)")
println(s"  [Part C] Combined End-to-End RMSE       : $rmseComb (all test rows)")
println(s"  [Part C] Combined End-to-End MAE        : $maeComb (all test rows)")
println("=========================================================================\n")

println("Sample Predictions (Alibaba Target Test Workloads):")
combined.filter("target_arrival_future > 0").select("machine_id", "timestamp_sec", "target_arrival_future", "final_predicted_arrival").show(20, false)

println("=====================================================")
println(" Evaluation Completed Successfully!")
println("=====================================================")

combined.unpersist()
sys.exit(0)
