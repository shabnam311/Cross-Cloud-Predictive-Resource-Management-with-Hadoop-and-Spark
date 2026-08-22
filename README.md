#  Cross-Cloud Predictive Resource Management with Hadoop & Spark

![Status: Work in Progress](https://img.shields.io/badge/Status-Work_in_Progress-yellow)
![Tech: Hadoop](https://img.shields.io/badge/Tech-Hadoop-blue)
![Tech: Spark](https://img.shields.io/badge/Tech-Apache_Spark-orange)
![Tech: React](https://img.shields.io/badge/Tech-React-61DAFB)

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
*   **Infrastructure & Deployment:** Docker (ARM-based architecture), Kubernetes (Planned)

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
- [ ] Finalize Spark MLlib Gradient Boosted Tree models (CPU, Memory, Task Arrival).
- [ ] Develop the resource recommendation engine logic.
- [ ] Build and integrate the React frontend dashboard.
- [ ] (Optional) Kubernetes-based deployment and validation.

##  Getting Started (Local Development)
*Prerequisites: Docker Desktop installed on your machine.*

1. Clone the repository:
   ```bash
   git clone [https://github.com/your-username/cross-cloud-resource-management.git](https://github.com/your-username/cross-cloud-resource-management.git)
   cd cross-cloud-resource-management
