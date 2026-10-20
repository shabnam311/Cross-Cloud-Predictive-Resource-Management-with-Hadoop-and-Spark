#  Cross-Cloud Predictive Resource Management with Hadoop & Spark

PS E:\bdlab> docker exec -it -e CLOUD=alibaba -e INTERVAL_S=10 -e MAX_WINDOWS=150 spark-master-cloud /opt/spark/bin/spark-shell `
>>   --master spark://spark-master-cloud:7077 `
>>   --total-executor-cores 1 --executor-cores 1 `
>>   --executor-memory 1G --driver-memory 1G `
>>   -i /opt/spark-apps/05a_replayer.scala

PS E:\bdlab> docker exec -it -e CLOUD=alibaba spark-master-cloud /opt/spark/bin/spark-shell `
>>   --master spark://spark-master-cloud:7077 `
>>   --total-executor-cores 2 --executor-cores 1 `
>>   --executor-memory 2G --driver-memory 2G `
>>   --conf spark.eventLog.enabled=true `
>>   --conf spark.eventLog.dir=hdfs://namenode-cloud:9000/spark-events `
>>   --conf spark.eventLog.rolling.enabled=true `
>>   --conf spark.eventLog.rolling.maxFileSize=16m `
>>   -i /opt/spark-apps/05_stream.scala
>> 
##  Project Overview
Modern cloud infrastructures traditionally rely on reactive resource allocation, which can lead to latency spikes and inefficient resource utilization. This project shifts the paradigm from **reactive to proactive resource allocation** by building an end-to-end Big Data pipeline. 

By leveraging **Google Cluster Trace 2019** and **Alibaba Cluster Trace 2018**, the system performs robust cross-cloud generalization testing. It ingests massive cluster logs, standardizes the schema, and utilizes machine learning to forecast CPU usage, memory consumption, and task arrival rates. 

##  Architecture & Pipeline
1. **Storage & Ingestion:** Massive unstructured and structured trace logs are ingested into **HDFS**. Synthetic unstructured logs are also generated to stress-test pipeline robustness.
2. **Data Preprocessing:** A **Scala-based schema standardization pipeline** runs via **Hadoop MapReduce** to unify the Google and Alibaba trace formats.
3. **Predictive Modeling:** **Spark MLlib** trains three Gradient Boosted Tree (GBT) forecasting models to predict:
   - CPU Usage
   - Memory Consumption
   - Task Arrival 
4. **Recommendation Engine:** Core logic translates ML forecasts into actionable, proactive resource allocation decisions.
5. **Visualization:** A **React-based frontend dashboard** visualizes cluster health, predictive insights, and cross-cloud metric comparisons.

##  Tech Stack
*   **Big Data Ecosystem:** Apache Hadoop (HDFS, MapReduce), Apache Spark
*   **Machine Learning:** Spark MLlib (Gradient Boosted Trees)
*   **Programming Languages:** Scala, Python, JavaScript
*   **Frontend:** React.js
*   **Infrastructure & Deployment:** Docker (ARM-based architecture)

## Task Arrival Rate Model (Transfer Learning) — v2

Task arrival rate is modeled as a two-part "hurdle" model (RandomForestClassifier
for idle-vs-active, GBTRegressor for magnitude on active buckets), trained via
**pooled instance-based transfer learning**: Google's chronological 80% training
split is pooled with a 10% chronological warm-up slice of Alibaba, and one model
is fit on the combined pool — the same strategy used by the CPU/Memory models
(`randomcpu.scala`, `linearcpu.scala`, `gradientcpu.scala`). Both stages use the
full cross-domain feature set already present in `ml_modeling_ready.parquet`
(CPU, memory, error-count and instance-arrival signals) plus a handful of
arrival-specific lag/rolling/time-of-day features, selected down to the top 12
via `UnivariateFeatureSelector`, with a small grid search per stage.

Run order and full code: see `TASK_ARRIVAL_RATE_TRANSFER_LEARNING_V2.md`.

##  Datasets
*   [Google Cluster Trace 2019](https://github.com/google/cluster-data)
*   [Alibaba Cluster Trace 2018](https://github.com/alibaba/clusterdata)

##  Project Status & Roadmap

### Completed
- [x] Dockerized infrastructure setup on ARM architecture.
- [x] Ingestion of Google and Alibaba trace datasets into HDFS.
- [x] Development of the Scala schema standardization pipeline (MapReduce).
- [x] Generation and verification of synthetic unstructured logs for robustness testing.

###  In Progress / Upcoming
- [x] Finalize Spark MLlib GBT/RF models for CPU and Memory.
- [x] Finalize Task Arrival Rate model (hurdle classifier + regressor, Google→Alibaba transfer learning).
- [ ] Develop the resource recommendation engine logic.
- [ ] Build and integrate the React frontend dashboard.

##  Getting Started (Local Development)
*Prerequisites: Docker Desktop installed on your machine.*

1. Clone the repository:
   ```bash
   git clone [https://github.com/your-username/cross-cloud-resource-management.git](https://github.com/your-username/cross-cloud-resource-management.git)
   cd cross-cloud-resource-management
