import React from 'react';
import { PageHeader } from '../components/PageHeader';

export function About() {
  return (
    <div className="page-section">
      <PageHeader
        title="About the Project"
        description="Background, predictive methodology, datasets and technical boundaries."
      />

      <div className="page-section" style={{ gap: '20px' }}>
        <h2 style={{ fontSize: 'var(--font-size-title-sm)' }}>The problem</h2>
        <p>
          Cloud infrastructure management commonly relies on reactive threshold autoscaling. By the time a cluster
          surges past 85% CPU or memory utilization, response latency has already degraded, worker queues have backed
          up, and task scheduling delays occur. Predictive resource management moves the allocation decision ahead of
          time by forecasting workload demand 30 to 60 minutes into the future.
        </p>

        <h2 style={{ fontSize: 'var(--font-size-title-sm)' }}>The method</h2>
        <p>
          This project implements a big data telemetry pipeline built on Apache Hadoop HDFS and Apache Spark MLlib.
          Telemetry traces are standardized through MapReduce jobs that align machine IDs, normalized resource usages,
          and temporal lag features. Gradient Boosted Tree (GBT) regressors forecast continuous CPU and memory demand,
          while a two-stage hurdle model addresses zero-inflated task arrival spikes. A rule-based engine translates these
          forecasts into proactive scaling actions.
        </p>

        <h2 style={{ fontSize: 'var(--font-size-title-sm)' }}>The datasets</h2>
        <p>
          Experiments evaluate real production traces from two hyperscale providers: the{' '}
          <a
            href="https://github.com/google/cluster-data"
            target="_blank"
            rel="noopener noreferrer"
          >
            Google Cluster Trace 2019
          </a>{' '}
          and the{' '}
          <a
            href="https://github.com/alibaba/clusterdata"
            target="_blank"
            rel="noopener noreferrer"
          >
            Alibaba Cluster Trace 2018
          </a>
          . In total, over 38 million machine-level telemetry rows are processed to benchmark single-cloud forecasting
          accuracy against cross-cloud domain transferability.
        </p>

        <h2 style={{ fontSize: 'var(--font-size-title-sm)' }}>Limitations & source code</h2>
        <p>
          The pipeline currently operates in batch mode on historical traces. Forecast accuracy degrades when
          workload distributions shift abruptly due to unobserved external events. Source code and deployment scripts
          are open source and available on the{' '}
          <a
            href="https://github.com/shabnamparveen-7/Cross-Cloud-Predictive-Resource-Management-with-Hadoop-and-Spark"
            target="_blank"
            rel="noopener noreferrer"
          >
            GitHub repository
          </a>
          .
        </p>
      </div>
    </div>
  );
}
