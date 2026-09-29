import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("CheckSchema")
  .master("local[2]")
  .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

val df = spark.read.parquet("hdfs://namenode-cloud:9000/telemetry/raw/ml_modeling_ready.parquet")

println("==================================================")
println(" SCHEMA OF ml_modeling_ready.parquet:")
println("==================================================")
df.printSchema()

println("==================================================")
println(" SAMPLE ROWS (5):")
println("==================================================")
df.show(5, false)

println("==================================================")
println(" ROW COUNT & CLOUD PROVIDERS:")
println("==================================================")
println(s"Total count: ${df.count()}")
if (df.columns.contains("cloud_provider")) {
  df.groupBy("cloud_provider").count().show(false)
}

sys.exit(0)
