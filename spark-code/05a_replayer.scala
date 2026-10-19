// =====================================================================
//  05a_replayer.scala : turns the historical Google test period into a "live" feed.
//
//  1. Builds the raw table (metrics FULL OUTER JOIN parsed log severity) for the seed + test period.
//  2. Writes the seed history (the 290 windows BEFORE the test period) to  /telementary/stream/seed
//  3. Waits until the stream job (05_stream.scala) has started, then writes ONE parquet file per 5-minute
//     window (all machines) into /telementary/stream/incoming, one every INTERVAL_S seconds.
//
//  The test period is computed exactly like 03_feature_engineering (window_ts >= lo + 0.85 * (hi - lo)).
//  FRESH START: this script deletes /telementary/stream_<cloud> (incoming, seed, checkpoint, outputs).
//  CLOUD=google (default) or CLOUD=alibaba. Run one cloud at a time on a 3-core cluster.
//  Stop the stream job before re-running it.
//
//  Env: INTERVAL_S=30  MAX_WINDOWS=0 (0 = all)  HDFS_BASE=hdfs://namenode-cloud:9000
//  Run with 1 core:  --total-executor-cores 1 --executor-memory 1G --driver-memory 1G
// =====================================================================
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.Path

val spark = SparkSession.builder().appName("05a-replayer").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.shuffle.partitions", "8")

val HDFS       = sys.env.getOrElse("HDFS_BASE", "hdfs://namenode-cloud:9000")
val base       = s"$HDFS/telementary"
val CLOUD      = sys.env.getOrElse("CLOUD", "google")      // google | alibaba
val streamDir  = s"$base/stream_$CLOUD"
val STEP       = 300L
val HIST_WIN   = 290                                   // windows of history the stream needs (289 + slack)
val INTERVAL_S = sys.env.getOrElse("INTERVAL_S", "30").toInt
val MAX_WIN    = sys.env.getOrElse("MAX_WINDOWS", "0").toInt
val metricsPath = s"$base/processed/unified_machine_5min.parquet"
val logsPath    = s"$base/raw/logs/${CLOUD}_syslogs"

val fs = new Path(base).getFileSystem(spark.sparkContext.hadoopConfiguration)
if (fs.exists(new Path(streamDir))) fs.delete(new Path(streamDir), true)
Seq("incoming", "staging", "seed").foreach(d => fs.mkdirs(new Path(s"$streamDir/$d")))

// ---------------- test period (same rule as step 03) ----------------
val metrics = spark.read.parquet(metricsPath).filter(col("cloud_provider") === CLOUD)
val bnd = metrics.agg(min("window_ts"), max("window_ts")).first()
val lo = bnd.getLong(0); val hi = bnd.getLong(1)
val t85 = lo + (hi - lo) * 0.85
val ts0Row = metrics.filter(col("window_ts") >= t85).agg(min("window_ts")).first()
val ts0 = ts0Row.getLong(0)
val seedFrom = ts0 - HIST_WIN * STEP
println(s"trace: $lo .. $hi | test starts at window_ts >= $t85 | first test window = $ts0 | seed from $seedFrom")

// ---------------- parse Google logs exactly like step 03 ----------------
val tsFmt = "yyyy-MM-dd HH:mm:ss"
val t0    = unix_timestamp(lit("2026-01-01 00:00:00"), tsFmt)
val tsRe  = "(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})"
val googleRe = s"^$tsRe \\[(\\w+)\\] gcp-host-(\\S+) : .*$$"
val aliRe    = s"^$tsRe \\| (\\w+) \\| alibaba-node-(\\S+) \\| .*$$"
val gooRe    = if (CLOUD == "alibaba") aliRe else googleRe          // name kept: used below as the log pattern
val logsG = spark.read.text(logsPath).select(
    regexp_extract(col("value"), gooRe, 1).as("ts"),
    regexp_extract(col("value"), gooRe, 2).as("level"),
    regexp_extract(col("value"), gooRe, 3).as("mid"))
  .filter(col("ts") =!= "")
  .select(
    concat_ws("_", lit(CLOUD), col("mid")).as("node_id"),
    (unix_timestamp(col("ts"), tsFmt) - t0).as("window_ts"),
    when(col("level").isin("ERROR", "CRITICAL"), 2)
      .when(col("level").isin("WARN", "WARNING"), 1)
      .otherwise(0).as("sev"))
  .filter(col("sev") > 0 && col("window_ts") >= seedFrom)
  .groupBy("node_id", "window_ts").agg(max("sev").as("sev"))

// ---------------- raw table: metrics + log severity (log-only rows kept, like the batch join) ----------------
val raw = metrics.select("node_id", "window_ts", "cpu_usage_norm", "mem_usage_norm", "n_records")
  .filter(col("window_ts") >= seedFrom)
  .join(logsG, Seq("node_id", "window_ts"), "full_outer")
  .select(col("node_id").cast("string").as("node_id"), col("window_ts").cast("long").as("window_ts"),
    col("cpu_usage_norm").cast("double").as("cpu_usage_norm"), col("mem_usage_norm").cast("double").as("mem_usage_norm"),
    col("n_records").cast("long").as("n_records"), coalesce(col("sev"), lit(0)).cast("int").as("sev"))
  .persist()

raw.filter(col("window_ts") < ts0).coalesce(1).write.mode("overwrite").parquet(s"$streamDir/seed")
val allTs = raw.filter(col("window_ts") >= ts0).select("window_ts").distinct().collect().map(_.getLong(0)).sorted
val replayTs = if (MAX_WIN > 0) allTs.take(MAX_WIN) else allTs
println(s"seed written. ${replayTs.length} windows to replay every ${INTERVAL_S}s (~${replayTs.length * INTERVAL_S / 60} min).")
println("Start 05_stream.scala now; replay begins as soon as the stream's checkpoint appears...")

val ckpt = new Path(s"$streamDir/checkpoint/metadata")
while (!fs.exists(ckpt)) Thread.sleep(2000)

// ---------------- drip one file per window ----------------
replayTs.zipWithIndex.foreach { case (ts, i) =>
  val tmp = new Path(s"$streamDir/staging/w_$ts")
  raw.filter(col("window_ts") === ts).coalesce(1).write.mode("overwrite").parquet(tmp.toString)
  val part = fs.listStatus(tmp).map(_.getPath).find(p => p.getName.startsWith("part-") && p.getName.endsWith(".parquet")).get
  fs.rename(part, new Path(s"$streamDir/incoming/w_$ts.parquet"))   // atomic move: the stream never sees a half-written file
  fs.delete(tmp, true)
  println(s"  replayed window ${i + 1}/${replayTs.length}  window_ts=$ts")
  Thread.sleep(INTERVAL_S * 1000L)
}
println("replay finished.")
sys.exit(0)
