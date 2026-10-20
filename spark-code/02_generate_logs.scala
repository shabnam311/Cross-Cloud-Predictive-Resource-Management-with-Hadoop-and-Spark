import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder().appName("02-generate-logs").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "UTC")

// ====================== CONFIG (edit these) ======================
val base       = "hdfs://namenode-cloud:9000/telementary"
val inPath     = s"$base/processed/unified_machine_5min.parquet"
val outAli     = s"$base/raw/logs/alibaba_syslogs"
val outGoo     = s"$base/raw/logs/google_syslogs"
val BASE_EPOCH = 1767225600L        // 2026-01-01 00:00:00 UTC: synthetic wall-clock start for both traces
val HEARTBEAT  = 0.002              // share of normal windows that also emit an INFO heartbeat
// =================================================================

println("===== Generating synthetic logs from clean per-machine data =====")

val df = spark.read.parquet(inPath)

def buildLogs(cloud: String) = {
  val d = df.filter(col("cloud_provider") === cloud)

  // Thresholds are percentiles of THIS cloud's own data, so both clouds get realistic WARN/ERROR rates
  val Array(cpu95, cpu99) = d.stat.approxQuantile("cpu_usage_norm", Array(0.95, 0.99), 0.001)
  val Array(mem95, mem99) = d.stat.approxQuantile("mem_usage_norm", Array(0.95, 0.99), 0.001)
  println(f"[$cloud] cpu p95=$cpu95%.3f p99=$cpu99%.3f | mem p95=$mem95%.3f p99=$mem99%.3f")

  val cpu = col("cpu_usage_norm")
  val mem = col("mem_usage_norm")

  val reason = when(mem >= mem99, "MEM_CRIT")
    .when(cpu >= cpu99, "CPU_CRIT")
    .when(cpu >= cpu95, "CPU_HIGH")
    .when(mem >= mem95, "MEM_HIGH")
    .when(rand(42) < HEARTBEAT, "HEARTBEAT")

  val ts = date_format(from_unixtime(lit(BASE_EPOCH) + col("window_ts")), "yyyy-MM-dd HH:mm:ss")
  val withReason = d.withColumn("reason", reason).filter(col("reason").isNotNull)

  val logLine =
    if (cloud == "alibaba") {
      val level = when(col("reason").isin("MEM_CRIT", "CPU_CRIT"), "ERROR")
        .when(col("reason").isin("CPU_HIGH", "MEM_HIGH"), "WARNING")
        .otherwise("INFO")
      val msg = when(col("reason") === "MEM_CRIT", "Out of Memory risk. Container eviction imminent!")
        .when(col("reason") === "CPU_CRIT", "CPU throttling triggered! Usage at saturation")
        .when(col("reason") === "CPU_HIGH", "High CPU load on node")
        .when(col("reason") === "MEM_HIGH", "High memory pressure on node")
        .otherwise("System heartbeat normal.")
      concat_ws(" | ", ts, level, concat(lit("alibaba-node-"), col("machine_id")), msg)
    } else {
      val level = when(col("reason").isin("MEM_CRIT", "CPU_CRIT"), "CRITICAL")
        .when(col("reason").isin("CPU_HIGH", "MEM_HIGH"), "WARN")
        .otherwise("INFO")
      val msg = when(col("reason") === "MEM_CRIT", "OOM Killer invoked.")
        .when(col("reason") === "CPU_CRIT", "CPU saturation detected.")
        .when(col("reason") === "CPU_HIGH", "CPU load above normal.")
        .when(col("reason") === "MEM_HIGH", "Memory usage above normal.")
        .otherwise(concat(lit("active_tasks="), col("n_records").cast("string")))
      concat(ts, lit(" ["), level, lit("] gcp-host-"), col("machine_id"), lit(" : "), msg)
    }

  withReason.select(col("reason"), logLine.as("log_line")).cache()
}

val aliLogs = buildLogs("alibaba")
aliLogs.select("log_line").coalesce(8).write.mode("overwrite").text(outAli)
println("Alibaba log lines by reason:")
aliLogs.groupBy("reason").count().orderBy(desc("count")).show(false)

val gooLogs = buildLogs("google")
gooLogs.select("log_line").coalesce(8).write.mode("overwrite").text(outGoo)
println("Google log lines by reason:")
gooLogs.groupBy("reason").count().orderBy(desc("count")).show(false)

println("===== Done. Logs written to " + base + "/raw/logs/ =====")
sys.exit(0)
