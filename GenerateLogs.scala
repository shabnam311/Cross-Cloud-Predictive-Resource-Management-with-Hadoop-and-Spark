import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Initialize Spark Session
val spark = SparkSession.builder()
  .appName("SyntheticLogGeneratorScala")
  .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

println("=====================================================")
println(" Starting Scala Log Generation for Cross-Cloud Data")
println("=====================================================")

// ====================================================================
// 1. ALIBABA LOG GENERATION (CSV)
// ====================================================================
println("\n[1/2] Processing Alibaba traces (CSV)...")

// TARGET SPECIFIC FILE: Replace 'machine_usage.csv' with your actual filename
// Avoid using *.csv to prevent schema clashes
val alibabaDF = spark.read.option("header", "true").csv("hdfs://namenode-cloud:9000/telemetry/raw/alibaba/machine_usage_bigger.csv")

// Alibaba Schema Variables
val aliCpuCol = "cpu_util_percent"
val aliMemCol = "mem_util_percent"
val aliMachineCol = "machine_id"
val aliTimeCol = "time_stamp"

val aliLogs = alibabaDF.withColumn("log_line",
  when(col(aliCpuCol).cast("float") > 85,
    concat(col(aliTimeCol), lit(" | WARNING | alibaba-node-"), col(aliMachineCol), lit(" | CPU throttling triggered! Usage exceeds 85%")))
  .when(col(aliMemCol).cast("float") > 90,
    concat(col(aliTimeCol), lit(" | ERROR | alibaba-node-"), col(aliMachineCol), lit(" | Out of Memory risk. Container eviction imminent!")))
  .when(rand() < 0.001, 
    concat(col(aliTimeCol), lit(" | INFO | alibaba-node-"), col(aliMachineCol), lit(" | System heartbeat normal.")))
  .otherwise(lit(null))
)

// Filter out empty rows and write to HDFS
val finalAliLogs = aliLogs.filter(col("log_line").isNotNull).select("log_line")
finalAliLogs.write.mode("overwrite").text("hdfs://namenode-cloud:9000/telemetry/raw/logs/alibaba_syslogs")
println(" Alibaba logs generated and saved to HDFS!")


// ====================================================================
// 2. GOOGLE LOG GENERATION (JSON.GZ)
// ====================================================================
println("\n[2/2] Processing Google traces (JSON.GZ)...")

// Target the exact Google files
val googleDF = spark.read.json(
  "hdfs://namenode-cloud:9000/telemetry/raw/google/instance_usage-000000000000.json.gz",
  "hdfs://namenode-cloud:9000/telemetry/raw/google/instance_usage-000000000001.json.gz"
)

// Google Schema Variables using dot-notation for nested structs
val googCpuCol = "average_usage.cpus"
val googMemCol = "average_usage.memory"
val googMachineCol = "machine_id"
val googTimeCol = "end_time"

val googLogs = googleDF.withColumn("log_line",
  when(col(googCpuCol).cast("float") > 0.85, 
    concat(col(googTimeCol), lit(" [WARN] gcp-host-"), col(googMachineCol), lit(" : CPU saturation detected.")))
  .when(col(googMemCol).cast("float") > 0.90,
    concat(col(googTimeCol), lit(" [CRITICAL] gcp-host-"), col(googMachineCol), lit(" : OOM Killer invoked.")))
  .when(rand() < 0.001,
    concat(col(googTimeCol), lit(" [INFO] gcp-host-"), col(googMachineCol), lit(" : active_tasks=42")))
  .otherwise(lit(null))
)

val finalGoogLogs = googLogs.filter(col("log_line").isNotNull).select("log_line")
finalGoogLogs.write.mode("overwrite").text("hdfs://namenode-cloud:9000/telemetry/raw/logs/google_syslogs")
println(" Google logs generated and saved to HDFS!")

println("\n=====================================================")
println(" Log Generation Complete! Exiting...")
println("=====================================================")
sys.exit(0)