# run_exp.ps1 : one memory/partition experiment on the feature-engineering script (nothing is saved to HDFS)
#
# Usage (PowerShell, from E:\bdlab):
#   .\run_exp.ps1 -Mem 2G -Parts 64            # run A (baseline)
#   .\run_exp.ps1 -Mem 1G -Parts 64            # run B
#   .\run_exp.ps1 -Mem 1G -Parts 200           # run C
#   .\run_exp.ps1 -Mem 1G -Parts 200 -Run 2    # repeat of run C (use -Run to number repeats)
#
# The Spark application name is built from the settings, e.g. "03-exp-mem1G-parts200-run1",
# so every experiment is easy to find in the History Server.
param(
  [Parameter(Mandatory = $true)][string]$Mem,     # executor memory, e.g. 1G or 2G
  [Parameter(Mandatory = $true)][int]$Parts,      # number of shuffle partitions, e.g. 64 or 200
  [int]$Run = 1,                                  # repeat number
  [string]$Script = "/tmp/03_feature_engineering_exp.scala"
)

$name = "03-exp-mem$Mem-parts$Parts-run$Run"
Write-Host "Starting $name (executor memory $Mem, $Parts shuffle partitions, nothing will be saved)"

docker exec -it `
  -e EXPERIMENT=1 -e FIXED_PARTS=1 -e SHUFFLE_PARTS=$Parts `
  spark-master-cloud /opt/spark/bin/spark-shell `
  --master spark://spark-master-cloud:7077 `
  --total-executor-cores 3 --executor-cores 1 `
  --executor-memory $Mem --driver-memory 2G `
  --name $name `
  --conf spark.eventLog.enabled=true `
  --conf spark.eventLog.dir=hdfs://namenode:9000/spark-events `
  -i $Script
