import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val spark = SparkSession.builder().appName("01-preprocess").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

// ====================== CONFIG (edit these) ======================
val base       = "hdfs://namenode-cloud:9000/telementary"
val aliPath    = s"$base/raw/data/alibaba-trace/alibaba-trace/machine_usage_bigger.csv"   // machine-level file, NOT container_usage
val googUsage  = s"$base/raw/data/google-trace/google-trace/instance_usage-*.json*"      // matches .json and .json.gz
val googEvents = s"$base/raw/data/google-trace/google-trace/machine_events-*.json*"
val outPath    = s"$base/processed/unified_machine_5min.parquet"
val WIN        = 300L                                            // 5-minute window in seconds
// =================================================================

println("===== Preprocessing: per-machine, 5-min windows =====")

// -------------------- 1. ALIBABA --------------------
println("[1/3] Alibaba machine_usage ...")

// Official machine_usage files have no header. Handle both cases automatically.
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

// -------------------- 2. GOOGLE --------------------
println("[2/3] Google instance_usage + machine_events ...")

// Machine capacity (normalized units) from machine_events
val capacity = spark.read.json(googEvents)
  .filter(col("capacity").isNotNull)
  .groupBy(col("machine_id").cast(StringType).as("machine_id"))
  .agg(max("capacity.cpus").cast(DoubleType).as("cap_cpu"),
       max("capacity.memory").cast(DoubleType).as("cap_mem"))
  .filter(col("cap_cpu") > 0 && col("cap_mem") > 0)

val googRaw = spark.read.json(googUsage)

// Sum usage of all instances on a machine in each window = machine load
val google = googRaw.select(
    col("machine_id").cast(StringType).as("machine_id"),
    (col("start_time").cast(LongType) / 1000000L).as("ts"),          // microseconds -> seconds
    col("average_usage.cpus").cast(DoubleType).as("cpu"),
    col("average_usage.memory").cast(DoubleType).as("mem"))
  .na.drop()
  .filter(col("cpu") >= 0 && col("mem") >= 0)
  .withColumn("window_ts", (floor(col("ts") / WIN) * WIN).cast(LongType))
  .groupBy("machine_id", "window_ts")
  .agg(sum("cpu").as("cpu_sum"),
       sum("mem").as("mem_sum"),
       count(lit(1)).as("n_records"))                                  // = number of instances
  .join(capacity, "machine_id")                                        // inner join drops machines with no capacity
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
println("Rows / machines / time range per cloud:")
check.groupBy("cloud_provider").agg(
  count(lit(1)).as("rows"),
  countDistinct("node_id").as("machines"),
  min("window_ts").as("min_ts"),
  max("window_ts").as("max_ts"),
  round(avg("cpu_usage_norm"), 3).as("avg_cpu"),
  round(avg("mem_usage_norm"), 3).as("avg_mem")
).show(false)

println("===== Done. Output: " + outPath + " =====")
sys.exit(0)