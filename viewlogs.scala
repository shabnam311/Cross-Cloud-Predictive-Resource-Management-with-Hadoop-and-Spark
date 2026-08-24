import org.apache.spark.sql.SparkSession

// Initialize Spark Session
val spark = SparkSession.builder()
  .appName("LogViewer")
  .getOrCreate()

// Hide background info logs
spark.sparkContext.setLogLevel("WARN")

println("\n=====================================================")
println(" 1. ALIBABA UNSTRUCTURED LOGS (Top 5)")
println("=====================================================")
try {
  val aliLogs = spark.read.text("hdfs://namenode-cloud:9000/telemetry/raw/logs/alibaba_syslogs")
  aliLogs.show(5, false) // 'false' prevents Spark from truncating long strings with "..."
} catch {
  case e: Exception => println("Error reading Alibaba logs: " + e.getMessage)
}

println("\n=====================================================")
println(" 2. GOOGLE UNSTRUCTURED LOGS (Top 5)")
println("=====================================================")
try {
  val googLogs = spark.read.text("hdfs://namenode-cloud:9000/telemetry/raw/logs/google_syslogs")
  googLogs.show(5, false)
} catch {
  case e: Exception => println("Error reading Google logs: " + e.getMessage)
}

println("\n=====================================================")
println(" Log Inspection Complete! Exiting...")
println("=====================================================")

sys.exit(0)