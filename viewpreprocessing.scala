import org.apache.spark.sql.SparkSession

// Initialize Spark Session
val spark = SparkSession.builder()
  .appName("VerifyPreprocessing")
  .getOrCreate()

// Hide background logs
spark.sparkContext.setLogLevel("WARN")

println("\n=====================================================")
println(" VERIFYING PREPROCESSED UNIFIED DATA")
println("=====================================================")

try {
  // Load the unified Parquet file we generated in the preprocessing step
  val unifiedDF = spark.read.parquet("hdfs://namenode-cloud:9000/telemetry/processed/unified_telemetry.parquet")

  println("\n[1/2] Top 5 Rows: ALIBABA (Should have 'alibaba' tag and usage between 0.0 and 1.0)")
  unifiedDF.filter("cloud_provider = 'alibaba'").show(5, false)

  println("\n[2/2] Top 5 Rows: GOOGLE (Should have 'google' tag and usage between 0.0 and 1.0)")
  unifiedDF.filter("cloud_provider = 'google'").show(5, false)

} catch {
  case e: Exception => println(" Error reading the processed Parquet file: " + e.getMessage)
}

println("\n=====================================================")
println(" Verification Complete! Exiting...")
println("=====================================================")

sys.exit(0)