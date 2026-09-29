@echo off
echo =========================================
echo 1. Checking HDFS status on namenode-cloud...
echo =========================================

docker exec namenode-cloud hdfs dfsadmin -safemode wait

echo =========================================
echo 2. Creating HDFS directories...
echo =========================================

docker exec namenode-cloud hdfs dfs -mkdir -p /telemetry/raw/google
docker exec namenode-cloud hdfs dfs -mkdir -p /telemetry/raw/alibaba
docker exec namenode-cloud hdfs dfs -mkdir -p /telemetry/raw/logs
docker exec namenode-cloud hdfs dfs -mkdir -p /telemetry/raw/task_arrival
docker exec namenode-cloud hdfs dfs -mkdir -p /telemetry/processed/task_arrival_features
docker exec namenode-cloud hdfs dfs -mkdir -p /telemetry/models

echo =========================================
echo 3. Checking local Parquet files in ./data/ ...
echo =========================================

if exist ".\data\google_task_events.parquet" (
    echo Copying google_task_events.parquet into container and HDFS...
    docker cp .\data\google_task_events.parquet namenode-cloud:/tmp/google_task_events.parquet
    docker exec namenode-cloud hdfs dfs -put -f /tmp/google_task_events.parquet /telemetry/raw/task_arrival/google_task_events.parquet
    docker exec namenode-cloud rm -f /tmp/google_task_events.parquet
) else (
    echo Note: ./data/google_task_events.parquet not found locally.
)

if exist ".\data\alibaba_task_events.parquet" (
    echo Copying alibaba_task_events.parquet into container and HDFS...
    docker cp .\data\alibaba_task_events.parquet namenode-cloud:/tmp/alibaba_task_events.parquet
    docker exec namenode-cloud hdfs dfs -put -f /tmp/alibaba_task_events.parquet /telemetry/raw/task_arrival/alibaba_task_events.parquet
    docker exec namenode-cloud rm -f /tmp/alibaba_task_events.parquet
) else (
    echo Note: ./data/alibaba_task_events.parquet not found locally.
)

echo =========================================
echo 4. Current HDFS state:
echo =========================================
docker exec namenode-cloud hdfs dfs -ls -R /telemetry/
echo =========================================
echo Done!
echo =========================================
