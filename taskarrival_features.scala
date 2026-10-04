import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.Try

val spark = SparkSession.builder()
  .appName("TaskArrivalFeatureEngineering")
  .config("spark.sql.parquet.enableVectorizedReader", "false")
  .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

println("=====================================================")
println(" Starting Task Arrival Feature Engineering")
println("=====================================================")

// 1. Check if unified ml_modeling_ready.parquet exists in HDFS
println("[1/5] Loading telemetry / task arrival dataset...")

val unifiedPath = "hdfs://namenode-cloud:9000/telemetry/raw/ml_modeling_ready.parquet"
val rawGooglePath = "hdfs://namenode-cloud:9000/telemetry/raw/task_arrival/google_task_events.parquet"
val rawAlibabaPath = "hdfs://namenode-cloud:9000/telemetry/raw/task_arrival/alibaba_task_events.parquet"

val rawDF = Try {
  spark.read.parquet(unifiedPath)
    .select("cloud_provider", "timestamp_sec", "machine_id", "task_arrival_count")
}.toOption match {
  case Some(df) =>
    println("   -> Successfully loaded unified dataset: ml_modeling_ready.parquet (projected)")
    df
  case None =>
    println("   -> Aggregating from raw task_arrival event files...")
    val BUCKET_SECONDS = 300L
    def loadAndCount(path: String, cloud: String) = {
      spark.read.parquet(path)
        .withColumn("cloud_provider", lit(cloud))
        .withColumn("bucket_ts", (col("timestamp_sec") / BUCKET_SECONDS).cast(LongType) * BUCKET_SECONDS)
        .filter(lower(col("event_type")).isin("submit", "arrival", "0", "task_submit"))
        .groupBy("cloud_provider", "machine_id", "bucket_ts")
        .agg(count("*").alias("arrival_count"))
    }
    val g = loadAndCount(rawGooglePath, "google")
    val a = Try(loadAndCount(rawAlibabaPath, "alibaba")).getOrElse(g.limit(0))
    g.unionByName(a)
}

println("[2/5] Standardizing base arrival count & time bucket columns...")

val standardizedDF = if (rawDF.columns.contains("task_arrival_count")) {
  rawDF.withColumn("arrival_count", col("task_arrival_count").cast(DoubleType))
       .withColumn("bucket_ts", col("timestamp_sec").cast(LongType))
} else if (rawDF.columns.contains("arrival_count")) {
  rawDF.withColumn("arrival_count", col("arrival_count").cast(DoubleType))
       .withColumn("bucket_ts", col("bucket_ts").cast(LongType))
} else {
  rawDF.withColumn("arrival_count", lit(0.0))
       .withColumn("bucket_ts", col("timestamp_sec").cast(LongType))
}

println("[3/5] Sampling representative time-series partition for scalable ML feature generation...")

val sampledDF = standardizedDF
  .filter(col("cloud_provider").isin("google", "alibaba"))
  .sample(false, 0.05, 42)

println("[4/5] Engineering lag, rolling, cyclical and hurdle-label features...")

val windowSpec = Window.partitionBy("cloud_provider", "machine_id").orderBy("bucket_ts")
val rollingSpec = windowSpec.rowsBetween(-5, -1)

val featured = sampledDF
  .withColumn("arrival_lag_1", lag("arrival_count", 1).over(windowSpec))
  .withColumn("arrival_lag_2", lag("arrival_count", 2).over(windowSpec))
  .withColumn("arrival_lag_3", lag("arrival_count", 3).over(windowSpec))
  .withColumn("rolling_mean_5", avg("arrival_count").over(rollingSpec))
  .withColumn("rolling_std_5", stddev("arrival_count").over(rollingSpec))
  .withColumn("rolling_max_5", max("arrival_count").over(rollingSpec))
  .withColumn("hour_of_day", ((col("bucket_ts") / 3600) % 24).cast(DoubleType))
  .withColumn("hour_sin", sin(col("hour_of_day") * (2 * math.Pi / 24)))
  .withColumn("hour_cos", cos(col("hour_of_day") * (2 * math.Pi / 24)))
  .withColumn("day_of_week", (((col("bucket_ts") / 86400) + 4) % 7).cast(DoubleType))
  .withColumn("is_active", when(col("arrival_count") > 0, 1.0).otherwise(0.0))
  .withColumn("log1p_arrival_count", log1p(col("arrival_count")))
  .na.fill(0.0, Seq("rolling_std_5", "rolling_mean_5", "rolling_max_5"))
  .na.drop(Seq("arrival_lag_1", "arrival_lag_2", "arrival_lag_3"))

println("[5/5] Reporting class balance and saving engineered features...")

val classBalance = featured.groupBy("cloud_provider", "is_active").count()
println("--- OBSERVED CLASS BALANCE (Evidence of Zero-Inflation) ---")
classBalance.show(false)

featured.repartition(4)
  .write.mode("overwrite")
  .partitionBy("cloud_provider")
  .parquet("hdfs://namenode-cloud:9000/telemetry/processed/task_arrival_features/all_clouds.parquet")

println("=====================================================")
println(" Task Arrival Feature Engineering Complete!")
println("=====================================================")

sys.exit(0)
