// =====================================================================
//  05b_parity.scala : do the features computed in the stream match the batch features table (step 03)?
//
//  Run AFTER the stream has processed a few windows (the stream saves its feature rows for the first
//  FEATURE_LOG_WINDOWS windows to /telementary/stream/features_log).
//  Compares, for every (node_id, window_ts) present in both, every model feature.
//  Light job: 1 core is enough.
// =====================================================================
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder().appName("05b-parity").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.shuffle.partitions", "8")
val HDFS = sys.env.getOrElse("HDFS_BASE", "hdfs://namenode-cloud:9000")
val base = s"$HDFS/telementary"
val CLOUD = sys.env.getOrElse("CLOUD", "google")
val TOL = sys.env.getOrElse("TOL", "1e-9").toDouble

val sf = spark.read.parquet(s"$base/stream_$CLOUD/features_log")
val bfAll = spark.read.parquet(s"$base/processed/features_5min.parquet").filter(col("cloud_provider") === CLOUD)
val idCols = Set("node_id", "window_ts", "split", "cloud_provider")
val feats = bfAll.columns.toSeq.filterNot(c => idCols(c) || c.startsWith("y_"))
val tsList = sf.select("window_ts").distinct().collect().map(_.getLong(0)).sorted
println(s"stream windows checked: ${tsList.length} (${tsList.head} .. ${tsList.last}) | ${feats.length} features | tolerance $TOL")

val s = sf.select((Seq(col("node_id"), col("window_ts")) ++ feats.map(f => col(f).as(s"s_$f"))): _*)
val b = bfAll.filter(col("window_ts").isin(tsList: _*))
  .select((Seq(col("node_id"), col("window_ts")) ++ feats.map(f => col(f).as(s"b_$f"))): _*)
val both = s.join(b, Seq("node_id", "window_ts")).persist()
val nS = s.count(); val nB = b.count(); val nBoth = both.count()
println(s"rows: stream=$nS batch=$nB matched=$nBoth | only in stream=${nS - nBoth} | only in batch=${nB - nBoth}")

val diffs = both.agg(feats.map(f => max(abs(col(s"s_$f") - col(s"b_$f"))).as(f)).head,
                     feats.map(f => max(abs(col(s"s_$f") - col(s"b_$f"))).as(f)).tail: _*).first()
val res = feats.zipWithIndex.map { case (f, i) => (f, if (diffs.isNullAt(i)) Double.NaN else diffs.get(i).asInstanceOf[Number].doubleValue) }
val bad = res.filter { case (_, d) => d.isNaN || d > TOL }
if (bad.isEmpty) println(s"PARITY OK: all ${feats.length} features match within $TOL on $nBoth rows")
else {
  println(s"PARITY FAILED on ${bad.length} features (max abs difference):")
  bad.sortBy(-_._2).foreach { case (f, d) => println(f"  $f%-18s $d%.3e") }
}
sys.exit(0)
