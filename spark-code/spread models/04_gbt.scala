// =====================================================================
//  04_gbt.scala : GBT (gradient boosted trees) - 4 experiments x 2 targets, with grid search
//
//  Input : processed/features_5min.parquet (from 03_feature_engineering_v2.scala)
//  Targets: CPU and MEMORY of every machine, HORIZON_STEPS windows ahead (1 = 5 min, 6 = 30 min, 12 = 1 h)
//
//  4 EXPERIMENTS (x 2 targets): google->google, alibaba->alibaba (same cloud, chronological test split)
//                               google->alibaba, alibaba->google (cross-cloud: the model is never fitted on the
//                               target cloud; only that cloud's mean/std are used to normalise the features)
//
//  HYPER-PARAMETER TUNING: grid search on a chronological validation set (train < val < test in time).
//  Spark's CrossValidator is NOT used on purpose: its random folds would let the model see the future.
//  Each (target, cloud) is tuned on a TUNE_FRAC sample of train rows, the best config is refitted on TRAIN_FRAC.
//
//  Model learns the CHANGE from the current value in units of the cloud's own std:
//      label = (target - current) / sigma ;  prediction = current + output * sigma  (clipped to 0..1)
//  Metrics (real 0..1 scale, test split): RMSE, MAE, R2, RMSE of two baselines (persistence: next = current,
//  MA6: next = mean of last 30 min), skill = 1 - RMSE/RMSE_baseline. Views: per_machine and cluster_mean.
//
//  Env settings (optional): TARGETS=cpu,mem HORIZON_STEPS=1 TRAIN_FRAC=0.10 TUNE_FRAC=0.04 VAL_FRAC=0.25
//      QUICK=1 (only 2 configs, for a smoke test)  USE_NREC=1  MAX_ITER=100 (max trees tried per config) MIN_INST=100
//  Outputs (HDFS, under /telementary): models/<cloud>/<target>/h<min>min/gbt, results/gbt_*
// =====================================================================
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{GBTRegressor, GBTRegressionModel, RandomForestRegressor, RandomForestRegressionModel, LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.storage.StorageLevel

val T0 = System.nanoTime()
val spark = SparkSession.builder().appName("04-gbt-5min").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", sys.env.getOrElse("SHUFFLE_PARTS", "64"))

// ------------------------------ CONFIG -------------------------------
val MODEL      = "gbt"
val HDFS       = sys.env.getOrElse("HDFS_BASE", "hdfs://namenode-cloud:9000")
val BASE       = s"$HDFS/telementary"
val FEAT_PATH  = sys.env.getOrElse("FEAT_PATH", s"$BASE/processed/features_5min.parquet")
val TARGETS    = sys.env.getOrElse("TARGETS", "cpu,mem").split(",").map(_.trim).filter(_.nonEmpty).toSeq
val HSTEPS     = sys.env.getOrElse("HORIZON_STEPS", "1").toInt
val H_MIN      = HSTEPS * 5
val USE_NREC   = sys.env.getOrElse("USE_NREC", "1") == "1"      // n_rec means instances (Google) vs samples (Alibaba)
val QUICK      = sys.env.getOrElse("QUICK", "0") == "1"
val TRAIN_FRAC = sys.env.getOrElse("TRAIN_FRAC", "0.10").toDouble
val TUNE_FRAC  = sys.env.getOrElse("TUNE_FRAC", "0.04").toDouble
val VAL_FRAC   = sys.env.getOrElse("VAL_FRAC", "0.25").toDouble
val MAX_ITER   = sys.env.getOrElse("MAX_ITER", "100").toInt
val MIN_INST   = sys.env.getOrElse("MIN_INST", "100").toInt
val CKPT_EVERY = sys.env.getOrElse("CKPT_INTERVAL", "10").toInt
val SER        = StorageLevel.MEMORY_AND_DISK_SER

require(Seq(1, 6, 12).contains(HSTEPS), "HORIZON_STEPS must be 1, 6 or 12 (the horizons stored in the feature table)")
require(TARGETS.forall(Seq("cpu", "mem").contains), "TARGETS must be cpu and/or mem")
require(TUNE_FRAC <= TRAIN_FRAC, "TUNE_FRAC must not exceed TRAIN_FRAC")
spark.sparkContext.setCheckpointDir(s"$HDFS/tmp/spark_ckpt")

val fsys = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(HDFS), spark.sparkContext.hadoopConfiguration)
def writeHdfsText(path: String, text: String): Unit = {
  val p = new org.apache.hadoop.fs.Path(path)
  if (fsys.exists(p)) fsys.delete(p, true)
  val out = fsys.create(p)
  try out.write(text.getBytes("UTF-8")) finally out.close()
}

println(s"=== $MODEL | targets=${TARGETS.mkString(",")} | horizon=${H_MIN}min | trainFrac=$TRAIN_FRAC tuneFrac=$TUNE_FRAC quick=$QUICK ===")

// ------------------------------ LOAD ---------------------------------
println("[1/5] Loading feature table and computing per-cloud scaling statistics (train rows only)...")
val all = spark.read.parquet(FEAT_PATH)
val idCols = Set("node_id", "window_ts", "split", "cloud_provider")
val feats: Seq[String] = all.columns.toSeq
  .filterNot(c => idCols(c) || c.startsWith("y_"))
  .filter(c => USE_NREC || !c.startsWith("n_rec"))
val clouds = Seq("alibaba", "google")
println(s"  ${feats.length} features")

def statsFor(d: DataFrame): Map[String, Double] = {
  val r = d.filter(col("split") === "train").agg(
    avg("cpu").as("mu_cpu"), stddev("cpu").as("sd_cpu"),
    avg("mem").as("mu_mem"), stddev("mem").as("sd_mem"),
    avg("n_rec").as("mu_n"), stddev("n_rec").as("sd_n")).first()
  def g(n: String): Double = if (r.isNullAt(r.fieldIndex(n))) 0.0 else r.getDouble(r.fieldIndex(n))
  Map("mu_cpu" -> g("mu_cpu"), "sd_cpu" -> math.max(g("sd_cpu"), 1e-3),
      "mu_mem" -> g("mu_mem"), "sd_mem" -> math.max(g("sd_mem"), 1e-3),
      "mu_n"   -> g("mu_n"),   "sd_n"   -> math.max(g("sd_n"), 1.0))
}
val stats: Map[String, Map[String, Double]] =
  clouds.map(c => c -> statsFor(all.filter(col("cloud_provider") === c))).toMap
stats.foreach { case (c, m) => println(s"  $c: " + m.map { case (k, v) => f"$k=$v%.4f" }.mkString(", ")) }

// LEVEL features -> (x - mu) / sd of their own metric ; delta / spread features -> x / sd ;
// z-scores, time-of-day and log counts stay unscaled (already comparable across clouds)
def scaleOf(f: String, st: Map[String, Double]): Column = {
  val x = col(f)
  val isLevel = f.matches("^(cpu|mem)(_(lag|mean|max|min)\\d+)?$") || f.matches("^cl_(cpu|mem)_mean$")
  if (f.endsWith("_z72") || f.startsWith("tod_") || f.startsWith("warn_") || f.startsWith("err_")) x
  else if (f.startsWith("n_rec")) {
    if (Set("n_rec", "n_rec_lag1", "n_rec_mean12").contains(f)) (x - lit(st("mu_n"))) / lit(st("sd_n"))
    else x / lit(st("sd_n"))
  } else {
    val m = if (f.startsWith("cl_")) f.split("_")(1) else if (f.startsWith("cpu")) "cpu" else "mem"
    if (isLevel) (x - lit(st(s"mu_$m"))) / lit(st(s"sd_$m")) else x / lit(st(s"sd_$m"))
  }
}

// ------------------------------ BUILD SPLITS -------------------------
println("[2/5] Building scaled train / val / test sets...")
val assembler = new VectorAssembler().setInputCols(feats.toArray).setOutputCol("features").setHandleInvalid("skip")
val labelNames = TARGETS.map(t => s"y_${t}_h$HSTEPS")

def build(c: String, split: String, frac: Double, seed: Long): DataFrame = {
  val st = stats(c)
  val d0 = all.filter(col("cloud_provider") === c && col("split") === split)
  val d1 = if (frac < 1.0) d0.sample(false, frac, seed) else d0
  val scaled = feats.map(f => scaleOf(f, st).as(f))
  val extra = Seq(col("window_ts"), col("node_id"),
    col("cpu").as("cur_cpu"), col("mem").as("cur_mem"),
    col("cpu_mean6").as("ma6_cpu"), col("mem_mean6").as("ma6_mem")) ++ labelNames.map(col)
  val keep = (Seq("window_ts", "node_id", "cur_cpu", "cur_mem", "ma6_cpu", "ma6_mem") ++ labelNames) :+ "features"
  assembler.transform(d1.select(scaled ++ extra: _*)).select(keep.map(col): _*)
}
val trainDF = clouds.map(c => c -> build(c, "train", TRAIN_FRAC, 42L).persist(SER)).toMap
val valDF   = clouds.map(c => c -> build(c, "val",   VAL_FRAC,   43L).persist(SER)).toMap
val testDF  = clouds.map(c => c -> build(c, "test",  1.0,        44L).persist(SER)).toMap
clouds.foreach { c =>
  println(s"  $c rows: train=${trainDF(c).count()} val=${valDF(c).count()} test=${testDF(c).count()}")
}

// target-specific columns (rows whose future window was not observed are dropped)
def forTarget(df: DataFrame, c: String, t: String): DataFrame =
  df.filter(col(s"y_${t}_h$HSTEPS").isNotNull)
    .withColumn("cur_raw", col(s"cur_$t"))
    .withColumn("tgt_raw", col(s"y_${t}_h$HSTEPS"))
    .withColumn("ma6_raw", col(s"ma6_$t"))
    .withColumn("sigma_m", lit(stats(c)(s"sd_$t")))
    .withColumn("label", (col("tgt_raw") - col("cur_raw")) / col("sigma_m"))

// ------------------------------ HELPERS ------------------------------
type M = org.apache.spark.ml.Model[_] with MLWritable
case class Cfg(desc: String, p: Map[String, Double], str: String = "")
case class Tuned(cfg: Cfg, valMse: Double, iters: Int)

def withPred(m: M, df: DataFrame): DataFrame =
  m.transform(df).withColumn("pred",
    greatest(lit(0.0), least(lit(1.0), col("cur_raw") + col("prediction") * col("sigma_m"))))

def labelMse(m: M, va: DataFrame): Double =
  new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("mse").evaluate(m.transform(va))

case class Sc(rmse: Double, mae: Double, r2: Double, rmseP: Double, rmseM: Double) {
  def skillP: Double = 1.0 - rmse / math.max(rmseP, 1e-12)
  def skillM: Double = 1.0 - rmse / math.max(rmseM, 1e-12)
}
def score(t: DataFrame): Sc = {
  val r = t.agg(
    avg(pow(col("pred") - col("tgt_raw"), 2)).as("mse"),
    avg(abs(col("pred") - col("tgt_raw"))).as("mae"),
    var_pop("tgt_raw").as("v"),
    avg(pow(col("cur_raw") - col("tgt_raw"), 2)).as("mseP"),
    avg(pow(col("ma6_raw") - col("tgt_raw"), 2)).as("mseM")).first()
  val mse = r.getDouble(0)
  Sc(math.sqrt(mse), r.getDouble(1), 1.0 - mse / math.max(r.getDouble(2), 1e-12),
     math.sqrt(r.getDouble(3)), math.sqrt(r.getDouble(4)))
}

// ------------------------- MODEL: GBT (gradient boosted trees) ---------
// Grid: tree depth x learning rate. The number of trees is chosen per config from the validation curve
// (evaluateEachIteration), so the grid does not need to include it.
val GRID: Seq[Cfg] = for (d <- Seq(3, 5, 7); s <- Seq(0.05, 0.1)) yield
  Cfg(s"depth=$d,step=$s", Map("depth" -> d.toDouble, "step" -> s))

def gbt(cfg: Cfg, iters: Int) = new GBTRegressor()
  .setLabelCol("label").setFeaturesCol("features")
  .setMaxDepth(cfg.p("depth").toInt).setStepSize(cfg.p("step")).setMinInstancesPerNode(MIN_INST).setMaxBins(32)
  .setMaxIter(iters).setSubsamplingRate(0.8).setSeed(42).setCheckpointInterval(CKPT_EVERY)

def tuneOne(cfg: Cfg, tr: DataFrame, va: DataFrame): Tuned = {
  val m     = gbt(cfg, MAX_ITER).fit(tr)
  val curve = m.evaluateEachIteration(va, "squared")
  val bi    = curve.indices.minBy(i => curve(i))
  Tuned(cfg, curve(bi), bi + 1)                                  // loss at the best tree count, and that count
}
def fitFinal(t: Tuned, tr: DataFrame): M = gbt(t.cfg, math.max(t.iters, 10)).fit(tr)
def importance(m: M, n: Int): Array[Double] = m.asInstanceOf[GBTRegressionModel].featureImportances.toArray
def sizeOf(m: M): Int = m.asInstanceOf[GBTRegressionModel].getNumTrees

// ------------------- TUNE + FIT ONE MODEL PER (TARGET, CLOUD) ---------
println("[3/5] Grid search on the chronological validation set, then final fit...")
case class Fit(model: M, size: Int, cfg: Cfg)
val fits = scala.collection.mutable.Map[(String, String), Fit]()
val impRows  = scala.collection.mutable.ArrayBuffer[(String, String, String, Int, String, Double)]()
val tuneRows = scala.collection.mutable.ArrayBuffer[(String, String, String, String, Double, Int, Boolean)]()
val grid = if (QUICK && GRID.length > 2) Seq(GRID.head, GRID.last) else GRID
println(s"  ${grid.length} configs per (target, cloud): " + grid.map(_.desc).mkString(" | "))

TARGETS.foreach { t =>
  clouds.foreach { c =>
    val tStart = System.nanoTime()
    val trFull = forTarget(trainDF(c), c, t)
    val tuneTr = trFull.sample(false, math.min(1.0, TUNE_FRAC / TRAIN_FRAC), 7L).persist(SER)
    val va     = forTarget(valDF(c), c, t)
    println(s"  [$t][$c] tuning on ${tuneTr.count()} rows, validating on ${va.count()} rows")
    val res = grid.map { cfg =>
      val r = tuneOne(cfg, tuneTr, va)
      println(f"      ${cfg.desc}%-34s val_loss=${r.valMse}%.6f" + (if (r.iters > 0) s"  best_iters=${r.iters}" else ""))
      r
    }
    tuneTr.unpersist()
    val best  = res.minBy(_.valMse)
    res.foreach(r => tuneRows += ((MODEL, c, t, r.cfg.desc, r.valMse, r.iters, r.cfg == best.cfg)))
    val model = fitFinal(best, trFull)
    val st = score(withPred(model, trFull)); val sv = score(withPred(model, va))
    val mins = (System.nanoTime() - tStart) / 6.0e10
    println(f"  [$t%-3s][$c%-7s] BEST ${best.cfg.desc} | skill vs persistence: train=${st.skillP}%.4f val=${sv.skillP}%.4f | $mins%.1f min")
    fits((c, t)) = Fit(model, sizeOf(model), best.cfg)

    val imp = feats.zip(importance(model, feats.length)).sortBy(-_._2)
    println("     top features: " + imp.take(8).map { case (n, v) => f"$n (${v * 100}%.1f%%)" }.mkString(", "))
    imp.foreach { case (n, v) => impRows += ((MODEL, c, t, H_MIN, n, v)) }

    // save model + metadata (scaling + feature order are required to reproduce predictions in streaming)
    val dir = s"$BASE/models/$c/$t/h${H_MIN}min"
    model.write.overwrite().save(s"$dir/$MODEL")
    writeHdfsText(s"$dir/_meta_$MODEL.txt", Seq(
      s"model=$MODEL", s"target=$t", s"horizon_steps=$HSTEPS", s"horizon_min=$H_MIN", s"source_cloud=$c",
      s"best_config=${best.cfg.desc} size=${fits((c, t)).size} trainFrac=$TRAIN_FRAC tuneFrac=$TUNE_FRAC",
      "train stats: " + stats(c).map { case (k, v) => s"$k=$v" }.mkString(", "),
      "scaling: level features (x-mu)/sd of own metric; delta/spread features x/sd; n_rec like level (mu_n, sd_n); z72, tod_*, warn_*, err_* unscaled",
      s"label=(target-current)/sd_$t ; prediction = current + model_output * sd_$t, clipped to 0..1",
      "features(in order)=" + feats.mkString(",")).mkString("\n") + "\n")
  }
}

// ----------------------------- 4 EXPERIMENTS ---------------------------
println("[4/5] Testing the 4 experiments...")
val scenarios = Seq(("google", "google"), ("alibaba", "alibaba"), ("google", "alibaba"), ("alibaba", "google"))
val rows = scala.collection.mutable.ArrayBuffer[(String, String, String, Int, String, Long, Int, Double, Double, Double, Double, Double, Double, Double)]()
println("\n%-4s %-18s %-13s %9s %5s %9s %9s %7s %10s %9s %10s %9s".format(
  "tgt", "experiment", "view", "n", "size", "RMSE", "MAE", "R2", "RMSE_pers", "RMSE_ma6", "skill_pers", "skill_ma6"))
TARGETS.foreach { t =>
  scenarios.foreach { case (src, tg) =>
    val fi = fits((src, t))
    val p = withPred(fi.model, forTarget(testDF(tg), tg, t))
      .select("window_ts", "cur_raw", "tgt_raw", "ma6_raw", "pred").persist(SER)
    val agg = p.groupBy("window_ts").agg(avg("cur_raw").as("cur_raw"), avg("tgt_raw").as("tgt_raw"),
      avg("ma6_raw").as("ma6_raw"), avg("pred").as("pred")).persist(SER)
    Seq(("per_machine", p), ("cluster_mean", agg)).foreach { case (view, df) =>
      val n = df.count(); val s = score(df); val name = s"$src->$tg"
      println("%-4s %-18s %-13s %9d %5d %9.5f %9.5f %7.4f %10.5f %9.5f %10.4f %9.4f".format(
        t, name, view, n, fi.size, s.rmse, s.mae, s.r2, s.rmseP, s.rmseM, s.skillP, s.skillM))
      rows += ((MODEL, name, t, H_MIN, view, n, fi.size, s.rmse, s.mae, s.r2, s.rmseP, s.rmseM, s.skillP, s.skillM))
    }
    p.unpersist(); agg.unpersist()
  }
}
println("\nskill = 1 - RMSE/RMSE_baseline (persistence: next = current ; ma6: next = mean of last 30 min). Cross-cloud rows use only the target cloud's mean/std for normalisation.")
println("Best configs: " + fits.toSeq.sortBy(_._1.toString).map { case ((c, t), f) => s"$t/$c -> ${f.cfg.desc}" }.mkString(" ; "))

// ------------------------------- SAVE --------------------------------
println("[5/5] Saving results...")
spark.createDataFrame(rows.toSeq).toDF("model", "experiment", "target", "h_min", "view", "n", "size", "rmse", "mae", "r2",
    "rmse_persist", "rmse_ma6", "skill_persist", "skill_ma6")
  .coalesce(1).write.mode("overwrite").option("header", "true").csv(s"$BASE/results/${MODEL}_h${H_MIN}min")
spark.createDataFrame(tuneRows.toSeq).toDF("model", "source_cloud", "target", "config", "val_loss", "best_iters", "is_best")
  .coalesce(1).write.mode("overwrite").option("header", "true").csv(s"$BASE/results/${MODEL}_tuning_h${H_MIN}min")
spark.createDataFrame(impRows.toSeq).toDF("model", "source_cloud", "target", "h_min", "feature", "importance")
  .coalesce(1).write.mode("overwrite").option("header", "true").csv(s"$BASE/results/${MODEL}_importance_h${H_MIN}min")

val totalMin = (System.nanoTime() - T0) / 6.0e10
println(s"Results : $BASE/results/${MODEL}_h${H_MIN}min  (+ _tuning_ and _importance_ folders)")
println(f"Total run time: $totalMin%.1f minutes")
sys.exit(0)
