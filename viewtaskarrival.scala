import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("VerifyTaskArrivalFeatures")
  .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

println("\n=====================================================")
println(" VERIFYING TASK ARRIVAL FEATURES (GOOGLE & ALIBABA)")
println("=====================================================")

try {
  val df = spark.read.parquet("hdfs://namenode-cloud:9000/telemetry/processed/task_arrival_features/all_clouds.parquet")

  println("\n[1/2] GOOGLE: Task Arrival Feature Matrix")
  df.filter("cloud_provider = 'google'")
    .select("machine_id", "bucket_ts", "arrival_count", "is_active",
      "arrival_lag_1", "rolling_mean_5", "hour_sin", "hour_cos")
    .show(5, false)

  println("\n[2/2] ALIBABA: Task Arrival Feature Matrix")
  df.filter("cloud_provider = 'alibaba'")
    .select("machine_id", "bucket_ts", "arrival_count", "is_active",
      "arrival_lag_1", "rolling_mean_5", "hour_sin", "hour_cos")
    .show(5, false)

} catch {
  case e: Exception => println(" Error reading the task_arrival_features Parquet file: " + e.getMessage)
}

println("\n=====================================================")
println(" Verification Complete! Exiting...")
println("=====================================================")

sys.exit(0)
