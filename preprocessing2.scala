import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val spark = SparkSession.builder()
  .appName("FeatureEngineeringPipeline")
  .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

println("=====================================================")
println(" Starting Phase 2: ML Feature Engineering")
println("=====================================================")

// 1. Load the Unified Parquet Data
println("[1/3] Loading standardized telemetry data...")
val unifiedDF = spark.read.parquet("hdfs://namenode-cloud:9000/telemetry/processed/unified_telemetry.parquet")

// ====================================================================
// STEP A: REGEX PARSING ON UNSTRUCTURED LOGS
// ====================================================================
println("[2/3] Extracting features from unstructured logs via Regex...")

// Read both Google and Alibaba unstructured logs into a single text column
val rawLogs = spark.read.text(
  "hdfs://namenode-cloud:9000/telemetry/raw/logs/alibaba_syslogs",
  "hdfs://namenode-cloud:9000/telemetry/raw/logs/google_syslogs"
)

// Use Regex to extract the Timestamp, Machine ID, and Error Level from the raw text
// Google format: 123456 [WARN] gcp-host-M123 : ...
// Alibaba format: 123456 | WARNING | alibaba-node-M123 | ...
val parsedLogs = rawLogs.select(
  regexp_extract(col("value"), "^(\\d+)", 1).cast(LongType).alias("log_timestamp"),
  regexp_extract(col("value"), "(WARN|WARNING|ERROR|CRITICAL)", 1).alias("error_level"),
  regexp_extract(col("value"), "(?:node-|host-)([^\\s|:]+)", 1).alias("log_machine_id")
).filter(col("error_level").isNotNull && col("error_level") =!= "")

// Count how many errors each machine had at that specific timestamp
val errorCounts = parsedLogs.groupBy("log_machine_id", "log_timestamp")
  .agg(count("*").alias("error_count"))

// Join the Error Counts back into our main structured DataFrame
val dfWithLogs = unifiedDF.join(
  errorCounts,
  unifiedDF("machine_id") === errorCounts("log_machine_id") && unifiedDF("timestamp_sec") === errorCounts("log_timestamp"),
  "left_outer"
).drop("log_machine_id", "log_timestamp")
 .na.fill(0, Seq("error_count")) // If no logs exist for that second, the error count is 0


// ====================================================================
// STEP B: TIME-SERIES LAG FEATURES
// ====================================================================
println("[3/3] Generating Time-Series Lag Features...")

// Define a Window that groups by machine and orders chronologically
val windowSpec = Window.partitionBy("machine_id").orderBy("timestamp_sec")

// Create "Lag" features so the ML model can see the CPU/Mem usage from the previous 1 and 2 timestamps
val finalFeatureDF = dfWithLogs

  .withColumn("mem_lag_1", lag("mem_usage_norm", 1).over(windowSpec))
  .withColumn("mem_lag_2", lag("mem_usage_norm", 2).over(windowSpec))
  .na.drop() // Drop the first two rows for each machine since they won't have past lag data

// Save the completely engineered dataset ready for ML Training
finalFeatureDF.write.mode("overwrite")
  .parquet("hdfs://namenode-cloud:9000/telemetry/processed/ml_features.parquet")

println("=====================================================")
println(" Feature Engineering Complete! Ready for ML Model Training.")
println("=====================================================")

sys.exit(0)  .withColumn("cpu_lag_1", lag("cpu_usage_norm", 1).over(windowSpec))
  .withColumn("cpu_lag_2", lag("cpu_usage_norm", 2).over(windowSpec))