import org.apache.spark.sql.SparkSession

// Initialize Spark Session
val spark = SparkSession.builder()
  .appName("VerifyAllFeatures")
  .getOrCreate()

// Hide background logs
spark.sparkContext.setLogLevel("WARN")

println("\n=====================================================")
println(" VERIFYING ALL ENGINEERED FEATURES (CPU & MEMORY)")
println("=====================================================")

try {
  // Load the final ML features Parquet file
  val mlDF = spark.read.parquet("hdfs://namenode-cloud:9000/telemetry/processed/ml_features.parquet")

  println("\n[1/2] ALIBABA: Complete Feature Matrix")
  mlDF.filter("cloud_provider = 'alibaba'")
      .select(
        "machine_id", 
        "timestamp_sec", 
        "cpu_usage_norm", "cpu_lag_1", "cpu_lag_2", 
        "mem_usage_norm", "mem_lag_1", "mem_lag_2", 
        "error_count"
      )
      .show(5, false)

  println("\n[2/2] GOOGLE: Complete Feature Matrix")
  mlDF.filter("cloud_provider = 'google'")
      .select(
        "machine_id", 
        "timestamp_sec", 
        "cpu_usage_norm", "cpu_lag_1", "cpu_lag_2", 
        "mem_usage_norm", "mem_lag_1", "mem_lag_2", 
        "error_count"
      )
      .show(5, false)

} catch {
  case e: Exception => println(" Error reading the ml_features Parquet file: " + e.getMessage)
}

println("\n=====================================================")
println(" Verification Complete! Exiting...")
println("=====================================================")

sys.exit(0)