import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("VerifyTaskArrivalFeaturesV2")
  .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

println("\n=====================================================")
println(" VERIFYING TASK ARRIVAL FEATURES V2 (GOOGLE & ALIBABA)")
println("=====================================================")

try {
  val df = spark.read.parquet("hdfs://namenode-cloud:9000/telemetry/processed/task_arrival_features_v2/all_clouds.parquet")

  println("\n[1/2] GOOGLE: Task Arrival Feature Matrix (v2)")
  df.filter("cloud_provider = 'google'")
    .select("machine_id", "timestamp_sec", "task_arrival_count", "target_arrival_future",
      "is_active_future", "cpu_usage_norm", "mem_usage_norm", "arrival_lag_1", "hour_sin")
    .show(5, false)

  println("\n[2/2] ALIBABA: Task Arrival Feature Matrix (v2)")
  df.filter("cloud_provider = 'alibaba'")
    .select("machine_id", "timestamp_sec", "task_arrival_count", "target_arrival_future",
      "is_active_future", "cpu_usage_norm", "mem_usage_norm", "arrival_lag_1", "hour_sin")
    .show(5, false)

} catch {
  case e: Exception => println(" Error reading task_arrival_features_v2 Parquet file: " + e.getMessage)
}

println("\n=====================================================")
println(" Verification Complete! Exiting...")
println("=====================================================")

sys.exit(0)
