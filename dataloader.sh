#!/bin/bash
set -e

echo "========================================="
echo "1. Checking HDFS status..."
echo "========================================="

# Wait until NameNode leaves safe mode
hdfs dfsadmin -safemode wait

echo "========================================="
echo "2. Creating HDFS directories..."
echo "========================================="

hdfs dfs -mkdir -p /telemetry/raw/google
hdfs dfs -mkdir -p /telemetry/raw/alibaba
hdfs dfs -mkdir -p /telemetry/raw/logs

echo "========================================="
echo "3. Ingesting Google Trace (.gz files)..."
echo "========================================="

if ls /opt/google-trace/google-trace/*.gz > /dev/null 2>&1; then

    hdfs dfs -put -f \
        /opt/google-trace/google-trace/*.gz \
        /telemetry/raw/google/

    echo "Google data ingested successfully."

else

    echo "WARNING: No Google .gz files found in:"
    echo "/opt/data/google-trace/google-trace/"

fi

echo "========================================="
echo "4. Ingesting Alibaba Trace (CSV files)..."
echo "========================================="

if ls /opt/alibaba-trace/*.csv >/dev/null 2>&1; then

    hdfs dfs -mkdir -p /telemetry/raw/alibaba

    hdfs dfs -put -f /opt/alibaba-trace/*.csv /telemetry/raw/alibaba/

    echo "Alibaba CSV files ingested successfully."

else

    echo "WARNING: No CSV files found in /opt/alibaba-trace"

fi

echo "========================================="
echo "5. Ingestion Complete! Current HDFS state:"
echo "========================================="

hdfs dfs -ls -R /telemetry/raw/

