import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const dataDir = path.resolve(__dirname, '../public/data');

if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

// Seeded pseudorandom number generator (mulberry32)
function mulberry32(a) {
  return function () {
    let t = (a += 0x6d2b79f5);
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const rng = mulberry32(20261004);

// Current timestamp rounded to previous 30-minute boundary (Item 17)
const nowMs = Math.floor(Date.now() / (30 * 60 * 1000)) * (30 * 60 * 1000);
const generatedAt = new Date(nowMs).toISOString();
const isSample = true;

// -------------------------------------------------------------
// 1. forecasts.json
// 30 minute steps for 48 hours = 96 points. The last 6 points are the forecast (3 hours).
// Index 90 is "now" (generatedAt). Index 92 is 1 hour ahead (Item 17, 20, 21).
// -------------------------------------------------------------
const totalSteps = 96;
const horizonSteps = 6;
const startTime = nowMs - 90 * 30 * 60 * 1000;

function generateTimeSeries(metric, clusterId) {
  const series = [];
  for (let i = 0; i < totalSteps; i++) {
    const timeMs = startTime + i * 30 * 60 * 1000;
    const date = new Date(timeMs);
    const hour = date.getUTCHours() + date.getUTCMinutes() / 60;

    // Daily diurnal cycle: peaks at hour 14-16, troughs at hour 3-5
    const cycle = Math.sin(((hour - 8) / 24) * 2 * Math.PI) * 0.15;
    const noise = (rng() - 0.5) * 0.06;
    const isHorizon = i >= totalSteps - horizonSteps;

    let baseActual;
    let predNoise = (rng() - 0.5) * 0.03;

    if (metric === 'cpu') {
      const clusterBase = clusterId === 'google' ? 0.58 : 0.54;
      baseActual = Math.min(0.85, Math.max(0.35, clusterBase + cycle + noise));
    } else if (metric === 'memory') {
      const clusterBase = clusterId === 'google' ? 0.68 : 0.72;
      baseActual = Math.min(0.90, Math.max(0.45, clusterBase + cycle * 0.8 + noise));
    } else {
      // taskArrival (tasks per minute)
      const clusterBase = clusterId === 'google' ? 400 : 480;
      const arrivalCycle = cycle * 300;
      const arrivalNoise = (rng() - 0.5) * 60;
      baseActual = Math.round(Math.max(50, clusterBase + arrivalCycle + arrivalNoise));
      predNoise = (rng() - 0.5) * 25;
    }

    let predicted;
    if (metric === 'taskArrival') {
      predicted = Math.round(baseActual + predNoise);
    } else {
      predicted = parseFloat((baseActual + predNoise).toFixed(4));
      baseActual = parseFloat(baseActual.toFixed(4));
    }

    series.push({
      t: date.toISOString(),
      actual: isHorizon ? null : baseActual,
      predicted: predicted
    });
  }
  return series;
}

const forecastsData = {
  generatedAt,
  isSample,
  series: {
    cpu: {
      google: generateTimeSeries('cpu', 'google'),
      alibaba: generateTimeSeries('cpu', 'alibaba')
    },
    memory: {
      google: generateTimeSeries('memory', 'google'),
      alibaba: generateTimeSeries('memory', 'alibaba')
    },
    taskArrival: {
      google: generateTimeSeries('taskArrival', 'google'),
      alibaba: generateTimeSeries('taskArrival', 'alibaba')
    }
  }
};

// -------------------------------------------------------------
// 2. overview.json
// Derives 1-hour-ahead forecasts directly from index 92 of forecastsData (Item 20)
// -------------------------------------------------------------
const nextHourGoogleCpu = forecastsData.series.cpu.google[92].predicted;
const nextHourGoogleMem = forecastsData.series.memory.google[92].predicted;
const nextHourGoogleTask = forecastsData.series.taskArrival.google[92].predicted;
const nextHourAlibabaCpu = forecastsData.series.cpu.alibaba[92].predicted;
const nextHourAlibabaMem = forecastsData.series.memory.alibaba[92].predicted;
const nextHourAlibabaTask = forecastsData.series.taskArrival.alibaba[92].predicted;

const overviewData = {
  generatedAt,
  isSample,
  window: '1h',
  nextHour: {
    google: {
      cpu: nextHourGoogleCpu,
      memory: nextHourGoogleMem,
      taskArrival: nextHourGoogleTask
    },
    alibaba: {
      cpu: nextHourAlibabaCpu,
      memory: nextHourAlibabaMem,
      taskArrival: nextHourAlibabaTask
    }
  },
  clusters: [
    {
      id: 'google',
      label: 'Google Cluster 2019',
      cpuUtilization: nextHourGoogleCpu,
      memoryUtilization: nextHourGoogleMem,
      tasksPerMinute: nextHourGoogleTask
    },
    {
      id: 'alibaba',
      label: 'Alibaba Cluster 2018',
      cpuUtilization: nextHourAlibabaCpu,
      memoryUtilization: nextHourAlibabaMem,
      tasksPerMinute: nextHourAlibabaTask
    }
  ]
};

// -------------------------------------------------------------
// 3. comparison.json
// Includes relativeError (RMSE / target mean) and unit per Item 9
// -------------------------------------------------------------
const rawComparisonRows = [
  { metric: 'cpu', trainOn: 'Google 2019', testOn: 'Google 2019', rmse: 0.052, mae: 0.039, r2: 0.884, targetMean: 0.56, unit: 'fraction' },
  { metric: 'cpu', trainOn: 'Google 2019', testOn: 'Alibaba 2018', rmse: 0.089, mae: 0.067, r2: 0.718, targetMean: 0.56, unit: 'fraction' },
  { metric: 'cpu', trainOn: 'Alibaba 2018', testOn: 'Alibaba 2018', rmse: 0.048, mae: 0.036, r2: 0.899, targetMean: 0.56, unit: 'fraction' },
  { metric: 'cpu', trainOn: 'Alibaba 2018', testOn: 'Google 2019', rmse: 0.094, mae: 0.071, r2: 0.692, targetMean: 0.56, unit: 'fraction' },
  { metric: 'memory', trainOn: 'Google 2019', testOn: 'Google 2019', rmse: 0.041, mae: 0.031, r2: 0.912, targetMean: 0.70, unit: 'fraction' },
  { metric: 'memory', trainOn: 'Google 2019', testOn: 'Alibaba 2018', rmse: 0.076, mae: 0.058, r2: 0.764, targetMean: 0.70, unit: 'fraction' },
  { metric: 'memory', trainOn: 'Alibaba 2018', testOn: 'Alibaba 2018', rmse: 0.039, mae: 0.029, r2: 0.925, targetMean: 0.70, unit: 'fraction' },
  { metric: 'memory', trainOn: 'Alibaba 2018', testOn: 'Google 2019', rmse: 0.081, mae: 0.062, r2: 0.739, targetMean: 0.70, unit: 'fraction' },
  { metric: 'taskArrival', trainOn: 'Google 2019', testOn: 'Google 2019', rmse: 28.4, mae: 19.8, r2: 0.841, targetMean: 440, unit: 'tasks/min' },
  { metric: 'taskArrival', trainOn: 'Google 2019', testOn: 'Alibaba 2018', rmse: 54.2, mae: 38.6, r2: 0.612, targetMean: 440, unit: 'tasks/min' },
  { metric: 'taskArrival', trainOn: 'Alibaba 2018', testOn: 'Alibaba 2018', rmse: 26.1, mae: 18.2, r2: 0.865, targetMean: 440, unit: 'tasks/min' },
  { metric: 'taskArrival', trainOn: 'Alibaba 2018', testOn: 'Google 2019', rmse: 58.7, mae: 41.5, r2: 0.583, targetMean: 440, unit: 'tasks/min' }
];

const comparisonData = {
  generatedAt,
  isSample,
  rows: rawComparisonRows.map((r) => ({
    metric: r.metric,
    trainOn: r.trainOn,
    testOn: r.testOn,
    rmse: r.rmse,
    mae: r.mae,
    r2: r.r2,
    relativeError: parseFloat((r.rmse / r.targetMean).toFixed(3)),
    unit: r.unit
  }))
};

// -------------------------------------------------------------
// 4. recommendations.json
// Window timestamps aligned with generatedAt
// -------------------------------------------------------------
const recommendationsData = {
  generatedAt,
  isSample,
  items: [
    {
      id: 'REC-001',
      cluster: 'Google 2019',
      resource: 'cpu',
      action: 'scale_up',
      magnitudePercent: 15,
      windowStart: new Date(nowMs).toISOString(),
      windowEnd: new Date(nowMs + 60 * 60 * 1000).toISOString(),
      confidence: 0.92,
      reason: 'Forecast indicates sustained CPU utilization above 75% for 45 consecutive minutes during afternoon peak.'
    },
    {
      id: 'REC-002',
      cluster: 'Alibaba 2018',
      resource: 'memory',
      action: 'scale_up',
      magnitudePercent: 12,
      windowStart: new Date(nowMs + 15 * 60 * 1000).toISOString(),
      windowEnd: new Date(nowMs + 75 * 60 * 1000).toISOString(),
      confidence: 0.88,
      reason: 'Memory pressure predicted to cross 85% threshold driven by incoming batch queue accumulation.'
    },
    {
      id: 'REC-003',
      cluster: 'Google 2019',
      resource: 'memory',
      action: 'hold',
      magnitudePercent: 0,
      windowStart: new Date(nowMs).toISOString(),
      windowEnd: new Date(nowMs + 60 * 60 * 1000).toISOString(),
      confidence: 0.95,
      reason: 'Memory consumption is stable at 74% with sufficient headroom for short bursts.'
    },
    {
      id: 'REC-004',
      cluster: 'Alibaba 2018',
      resource: 'cpu',
      action: 'scale_down',
      magnitudePercent: 8,
      windowStart: new Date(nowMs + 90 * 60 * 1000).toISOString(),
      windowEnd: new Date(nowMs + 150 * 60 * 1000).toISOString(),
      confidence: 0.84,
      reason: 'Workload completion expected to reduce CPU demand below 40% after ongoing map tasks finish.'
    },
    {
      id: 'REC-005',
      cluster: 'Google 2019',
      resource: 'taskArrival',
      action: 'scale_up',
      magnitudePercent: 20,
      windowStart: new Date(nowMs + 120 * 60 * 1000).toISOString(),
      windowEnd: new Date(nowMs + 180 * 60 * 1000).toISOString(),
      confidence: 0.89,
      reason: 'Task arrival rate is projected to climb past 550 tasks per minute, requiring extra worker nodes.'
    },
    {
      id: 'REC-006',
      cluster: 'Alibaba 2018',
      resource: 'taskArrival',
      action: 'hold',
      magnitudePercent: 0,
      windowStart: new Date(nowMs).toISOString(),
      windowEnd: new Date(nowMs + 60 * 60 * 1000).toISOString(),
      confidence: 0.91,
      reason: 'Task arrival velocity aligns with current node capacity without risk of scheduling delays.'
    }
  ]
};

// -------------------------------------------------------------
// 5. models.json - derive summary stats from same-cloud comparison rows (Item 10)
// -------------------------------------------------------------
function sameCloudAvg(metric, field) {
  const rows = comparisonData.rows.filter(
    (r) => r.metric === metric && r.trainOn === r.testOn
  );
  return rows.reduce((t, r) => t + r[field], 0) / rows.length;
}

const modelsData = {
  generatedAt,
  isSample,
  models: [
    {
      metric: 'cpu',
      rmse: parseFloat(sameCloudAvg('cpu', 'rmse').toFixed(3)),
      mae: parseFloat(sameCloudAvg('cpu', 'mae').toFixed(3)),
      r2: parseFloat(sameCloudAvg('cpu', 'r2').toFixed(3)),
      unit: 'fraction',
      trainRows: 38400000,
      features: [
        { name: 'cpu_lag_1', importance: 0.342 },
        { name: 'cpu_avg_5m', importance: 0.228 },
        { name: 'cpu_lag_2', importance: 0.145 },
        { name: 'mem_usage_norm', importance: 0.089 },
        { name: 'hour_sin', importance: 0.065 },
        { name: 'task_arrival_count', importance: 0.052 },
        { name: 'cpu_avg_15m', importance: 0.044 },
        { name: 'error_count', importance: 0.035 }
      ]
    },
    {
      metric: 'memory',
      rmse: parseFloat(sameCloudAvg('memory', 'rmse').toFixed(3)),
      mae: parseFloat(sameCloudAvg('memory', 'mae').toFixed(3)),
      r2: parseFloat(sameCloudAvg('memory', 'r2').toFixed(3)),
      unit: 'fraction',
      trainRows: 38400000,
      features: [
        { name: 'mem_lag_1', importance: 0.381 },
        { name: 'mem_avg_5m', importance: 0.246 },
        { name: 'mem_lag_2', importance: 0.132 },
        { name: 'cpu_usage_norm', importance: 0.078 },
        { name: 'instance_arrival_count', importance: 0.058 },
        { name: 'hour_cos', importance: 0.041 },
        { name: 'mem_avg_15m', importance: 0.039 },
        { name: 'error_count', importance: 0.025 }
      ]
    },
    {
      metric: 'taskArrival',
      rmse: parseFloat(sameCloudAvg('taskArrival', 'rmse').toFixed(1)),
      mae: parseFloat(sameCloudAvg('taskArrival', 'mae').toFixed(1)),
      r2: parseFloat(sameCloudAvg('taskArrival', 'r2').toFixed(3)),
      unit: 'tasks/min',
      trainRows: 38400000,
      features: [
        { name: 'arrival_lag_1', importance: 0.315 },
        { name: 'arrival_rolling_mean_5', importance: 0.218 },
        { name: 'task_lag_1', importance: 0.154 },
        { name: 'hour_sin', importance: 0.102 },
        { name: 'hour_cos', importance: 0.076 },
        { name: 'arrival_rolling_std_5', importance: 0.055 },
        { name: 'cpu_avg_5m', importance: 0.045 },
        { name: 'instance_avg_5m', importance: 0.035 }
      ]
    }
  ]
};

// -------------------------------------------------------------
// 6. pipeline.json
// -------------------------------------------------------------
const pipelineData = {
  generatedAt,
  isSample,
  stages: [
    {
      id: 1,
      name: 'Docker infrastructure setup',
      tool: 'Docker Compose',
      status: 'done',
      description: 'Deploys Hadoop NameNode, DataNodes, Spark Master, Workers and History Server in a bridged network.'
    },
    {
      id: 2,
      name: 'HDFS cluster trace ingestion',
      tool: 'Hadoop HDFS',
      status: 'done',
      description: 'Ingests raw Google Cluster Trace 2019 and Alibaba Cluster Trace 2018 datasets into distributed storage.'
    },
    {
      id: 3,
      name: 'Schema standardisation MapReduce',
      tool: 'Apache Spark',
      status: 'done',
      description: 'Unifies disparate column schemas, timestamp units and resource normalisation across both cloud providers.'
    },
    {
      id: 4,
      name: 'Synthetic syslog generation and regex extraction',
      tool: 'Scala / Spark',
      status: 'done',
      description: 'Generates unstructured cluster log messages and extracts error counts using regular expressions.'
    },
    {
      id: 5,
      name: 'Gradient Boosted Tree predictive models',
      tool: 'Spark MLlib',
      status: 'in_progress',
      description: 'Trains GBT regressors and random forest hurdle classifiers for CPU, memory and task arrival rate forecasting.'
    },
    {
      id: 6,
      name: 'Recommendation engine',
      tool: 'Scala / Rules Engine',
      status: 'in_progress',
      description: 'Translates workload forecasts into automated proactive actions such as scale up, scale down or hold.'
    },
    {
      id: 7,
      name: 'Static reporting dashboard',
      tool: 'React / Vite',
      status: 'in_progress',
      description: 'Renders proactive resource forecasts, cross cloud comparison metrics and recommendations in a static interface.'
    },
    {
      id: 8,
      name: 'Kubernetes production deployment',
      tool: 'Kubernetes / Helm',
      status: 'planned',
      description: 'Optional container orchestration deployment for real time cluster autoscaling controllers.'
    }
  ]
};

fs.writeFileSync(path.join(dataDir, 'overview.json'), JSON.stringify(overviewData, null, 2));
fs.writeFileSync(path.join(dataDir, 'forecasts.json'), JSON.stringify(forecastsData, null, 2));
fs.writeFileSync(path.join(dataDir, 'comparison.json'), JSON.stringify(comparisonData, null, 2));
fs.writeFileSync(path.join(dataDir, 'recommendations.json'), JSON.stringify(recommendationsData, null, 2));
fs.writeFileSync(path.join(dataDir, 'models.json'), JSON.stringify(modelsData, null, 2));
fs.writeFileSync(path.join(dataDir, 'pipeline.json'), JSON.stringify(pipelineData, null, 2));

console.log('Mock data generation complete. 6 files written to public/data.');
