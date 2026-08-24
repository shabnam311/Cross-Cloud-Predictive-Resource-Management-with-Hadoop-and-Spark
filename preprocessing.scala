import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Initialize Spark Session
val spark = SparkSession.builder()
  .appName("CrossCloudPreprocessor")
  .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

println("=====================================================")
println(" Starting Cross-Cloud Data Preprocessing Pipeline")
println("=====================================================")

// ====================================================================
// 1. PROCESS ALIBABA DATA
// ====================================================================
println("[1/3] Standardizing Alibaba Data (CSV)...")

val alibabaDF = spark.read.option("header", "true")
  .csv("hdfs://namenode-cloud:9000/telemetry/raw/alibaba/container_usage.csv")

// Normalize 0-100 scale to 0-1 scale and rename columns
val cleanAlibaba = alibabaDF.select(
  col("machine_id").cast(StringType).alias("machine_id"),
  col("time_stamp").cast(LongType).alias("timestamp_sec"),
  (col("cpu_util_percent").cast(DoubleType) / 100.0).alias("cpu_usage_norm"),
  (col("mem_util_percent").cast(DoubleType) / 100.0).alias("mem_usage_norm"),
  lit("alibaba").alias("cloud_provider")
).na.drop() // ✅ FIX: Scala uses .na.drop() instead of .dropna()


// ====================================================================
// 2. PROCESS GOOGLE DATA
// ====================================================================
println("[2/3] Standardizing Google Data (JSON.GZ)...")

val googleDF = spark.read.json(
  "hdfs://namenode-cloud:9000/telemetry/raw/google/instance_usage-000000000000.json.gz",
  "hdfs://namenode-cloud:9000/telemetry/raw/google/instance_usage-000000000001.json.gz"
)

// Convert Microseconds to Seconds and map nested structs
val cleanGoogle = googleDF.select(
  col("machine_id").cast(StringType).alias("machine_id"),
  (col("end_time").cast(LongType) / 1000000L).alias("timestamp_sec"),
  col("average_usage.cpus").cast(DoubleType).alias("cpu_usage_norm"),
  col("average_usage.memory").cast(DoubleType).alias("mem_usage_norm"),
  lit("google").alias("cloud_provider")
).na.drop() // ✅ FIX: Scala uses .na.drop() instead of .dropna()


// ====================================================================
// 3. UNION AND SAVE AS PARQUET
// ====================================================================
println("[3/3] Unioning datasets and saving as optimized Parquet...")

// Combine both standard datasets into one massive DataFrame
val unifiedDF = cleanAlibaba.unionByName(cleanGoogle)

// Save to HDFS in Parquet format (Best practice for Spark MLlib)
unifiedDF.write.mode("overwrite")
  .parquet("hdfs://namenode-cloud:9000/telemetry/processed/unified_telemetry.parquet")

println("=====================================================")
println(" Preprocessing Complete! Unified data saved to HDFS.")
println("=====================================================")

sys.exit(0)