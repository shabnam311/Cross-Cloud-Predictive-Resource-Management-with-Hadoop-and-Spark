import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val spark = SparkSession.builder().appName("01-preprocess-v2").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

// ====================== CONFIG (edit these) ======================
val base      = "hdfs://namenode-cloud:9000/telementary"            // same spelling as your existing HDFS folder
val aliPath   = s"$base/raw/data/alibaba-trace/alibaba-trace/machine_usage_bigger.csv"
val googUsage = s"$base/raw/google_bq/usage_5min*.csv*"             // from BigQuery export (.csv or .csv.gz)
val googCap   = s"$base/raw/google_bq/machine_capacity*.csv*"
val outPath   = s"$base/processed/unified_machine_5min.parquet"
val WIN       = 300L                                                // 5-minute window in seconds
// =================================================================

println("===== Preprocessing v2: per-machine, 5-min windows =====")

// -------------------- 1. ALIBABA (unchanged logic) --------------------
println("[1/3] Alibaba machine_usage ...")

val aliSchema = StructType(Seq(
  "machine_id","time_stamp","cpu_util_percent","mem_util_percent",
  "mem_gps","mkpi","net_in","net_out","disk_io_percent"
).map(StructField(_, StringType)))

val aliProbe = spark.read.option("header", "true").csv(aliPath)
val aliRaw   = if (aliProbe.columns.contains("machine_id")) aliProbe
               else spark.read.schema(aliSchema).csv(aliPath)

val alibaba = aliRaw.select(
    col("machine_id").cast(StringType).as("machine_id"),
    col("time_stamp").cast(LongType).as("ts"),
    col("cpu_util_percent").cast(DoubleType).as("cpu"),
    col("mem_util_percent").cast(DoubleType).as("mem"))
  .na.drop()
  .filter(col("cpu").between(0, 100) && col("mem").between(0, 100))   // drops -1 / 101 sentinels
  .withColumn("window_ts", (floor(col("ts") / WIN) * WIN).cast(LongType))
  .groupBy("machine_id", "window_ts")
  .agg((avg("cpu") / 100.0).as("cpu_usage_norm"),
       (avg("mem") / 100.0).as("mem_usage_norm"),
       count(lit(1)).as("n_records"))
  .withColumn("cloud_provider", lit("alibaba"))

// -------------------- 2. GOOGLE (pre-aggregated in BigQuery) --------------------
println("[2/3] Google BigQuery export ...")

val capacity = spark.read.option("header", "true").csv(googCap)
  .select(col("machine_id").cast(StringType).as("machine_id"),
          col("cap_cpu").cast(DoubleType).as("cap_cpu"),
          col("cap_mem").cast(DoubleType).as("cap_mem"))
  .na.drop()
  .filter(col("cap_cpu") > 0 && col("cap_mem") > 0)
  .groupBy("machine_id")
  .agg(max("cap_cpu").as("cap_cpu"), max("cap_mem").as("cap_mem"))

val google = spark.read.option("header", "true").csv(googUsage)
  .select(col("machine_id").cast(StringType).as("machine_id"),
          col("window_ts").cast(LongType).as("window_ts"),
          col("cpu_sum").cast(DoubleType).as("cpu_sum"),
          col("mem_sum").cast(DoubleType).as("mem_sum"),
          col("n_records").cast(LongType).as("n_records"))
  .na.drop()
  .filter(col("cpu_sum") >= 0 && col("mem_sum") >= 0)
  .join(capacity, "machine_id")
  .withColumn("cpu_usage_norm", least(lit(1.0), col("cpu_sum") / col("cap_cpu")))
  .withColumn("mem_usage_norm", least(lit(1.0), col("mem_sum") / col("cap_mem")))
  .select("machine_id", "window_ts", "cpu_usage_norm", "mem_usage_norm", "n_records")
  .withColumn("cloud_provider", lit("google"))

// -------------------- 3. UNION + SAVE --------------------
println("[3/3] Union and save Parquet ...")

val unified = alibaba.unionByName(google)
  .withColumn("node_id", concat_ws("_", col("cloud_provider"), col("machine_id")))
  .select("cloud_provider", "node_id", "machine_id", "window_ts",
          "cpu_usage_norm", "mem_usage_norm", "n_records")

unified.write.mode("overwrite").partitionBy("cloud_provider").parquet(outPath)

// -------------------- Sanity checks --------------------
val check = spark.read.parquet(outPath)
println("Per-cloud summary:")
check.groupBy("cloud_provider").agg(
  count(lit(1)).as("rows"),
  countDistinct("node_id").as("machines"),
  round(count(lit(1)) / countDistinct("node_id"), 0).as("windows_per_machine"),
  min("window_ts").as("min_ts"),
  max("window_ts").as("max_ts"),
  round(avg("cpu_usage_norm"), 3).as("avg_cpu"),
  round(avg("mem_usage_norm"), 3).as("avg_mem")
).show(false)

println("===== Done. Output: " + outPath + " =====")
sys.exit(0)
