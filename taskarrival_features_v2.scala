import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.Try

val spark = SparkSession.builder()
  .appName("Phase5-TaskArrivalFeatureEngineering")
  .config("spark.eventLog.enabled", "true")
  .config("spark.eventLog.dir", "hdfs://namenode-cloud:9000/spark-events")
  .config("spark.sql.parquet.enableVectorizedReader", "false")
  .config("spark.sql.shuffle.partitions", "64")
  .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

println("=====================================================")
println(" Starting Task Arrival Feature Engineering (v2)")
println("=====================================================")

// 1. Load the unified dataset
println("[1/4] Loading ml_modeling_ready.parquet (unified, full resolution)...")

val path1 = "hdfs://namenode-cloud:9000/telemetry/processed/ml_modeling_ready.parquet"
val path2 = "hdfs://namenode-cloud:9000/telemetry/raw/ml_modeling_ready.parquet"

val df = Try(spark.read.parquet(path1)).getOrElse(spark.read.parquet(path2))

// 2. Build the forward-looking arrival target (lead) on full-resolution data
println("[2/4] Building forward-looking arrival target (lead), on full-resolution data...")

val machineWindow = Window.partitionBy("cloud_provider", "machine_id").orderBy("timestamp_sec")

val withTarget = df
  .withColumn("target_arrival_future", lead("task_arrival_count", 1).over(machineWindow))
  .na.drop(Seq("target_arrival_future")) // drops only the last row per machine (no future value exists)

// 3. Arrival-specific extra features (lag/rolling/cyclical features)
println("[3/4] Adding arrival-specific lag/rolling/cyclical features...")

val rollingSpec = machineWindow.rowsBetween(-5, -1)

val featured = withTarget
  .withColumn("arrival_lag_1", lag("task_arrival_count", 1).over(machineWindow))
  .withColumn("arrival_rolling_mean_5", avg("task_arrival_count").over(rollingSpec))
  .withColumn("arrival_rolling_std_5", stddev("task_arrival_count").over(rollingSpec))
  .withColumn("hour_of_day", ((col("timestamp_sec") / 3600) % 24).cast(DoubleType))
  .withColumn("hour_sin", sin(col("hour_of_day") * (2 * math.Pi / 24)))
  .withColumn("hour_cos", cos(col("hour_of_day") * (2 * math.Pi / 24)))
  .withColumn("is_active_future", when(col("target_arrival_future") > 0, 1.0).otherwise(0.0))
  .withColumn("log1p_target_arrival_future", log1p(col("target_arrival_future")))
  .na.fill(0.0, Seq("arrival_rolling_std_5", "arrival_rolling_mean_5"))
  .na.drop(Seq("arrival_lag_1"))

// 4. Persist to HDFS directly, partitioned by cloud_provider
println("[4/4] Writing engineered features to HDFS (partitioned by cloud_provider)...")

val outPath = "hdfs://namenode-cloud:9000/telemetry/processed/task_arrival_features_v2/all_clouds.parquet"

featured
  .write.mode("overwrite")
  .partitionBy("cloud_provider")
  .parquet(outPath)

println("\n--- OBSERVED CLASS BALANCE (Evidence of Zero-Inflation) ---")
val savedDF = spark.read.parquet(outPath)
savedDF.groupBy("cloud_provider", "is_active_future").count().show(false)

println("=====================================================")
println(" Task Arrival Feature Engineering (v2) Complete!")
println("=====================================================")

sys.exit(0)
