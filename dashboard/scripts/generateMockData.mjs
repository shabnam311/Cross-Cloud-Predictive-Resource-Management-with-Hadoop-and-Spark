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
// Cross-cloud RMSE comparison for the two project models
// -------------------------------------------------------------
const rawComparisonRows = [
  { metric: 'cpu', cloud: 'Google 2019', gbtRmse: 0.0826, randomForestRmse: 0.0825, unit: 'fraction' },
  { metric: 'cpu', cloud: 'Alibaba 2018', gbtRmse: 0.0556, randomForestRmse: 0.0547, unit: 'fraction' },
  { metric: 'memory', cloud: 'Google 2019', gbtRmse: 0.0481, randomForestRmse: 0.0482, unit: 'fraction' },
  { metric: 'memory', cloud: 'Alibaba 2018', gbtRmse: 0.0171, randomForestRmse: 0.0166, unit: 'fraction' }
];

const comparisonData = {
  generatedAt,
  isSample,
  rows: rawComparisonRows,
  transferRows: [
    { direction: 'Google → Alibaba', cpuGbtChange: 4, cpuRandomForestChange: 5, memoryGbtChange: -3, memoryRandomForestChange: 4 },
    { direction: 'Alibaba → Google', cpuGbtChange: 10, cpuRandomForestChange: 11, memoryGbtChange: 6, memoryRandomForestChange: 10 }
  ]
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
// 5. models.json - model summary stats
// -------------------------------------------------------------
const modelsData = {
  generatedAt,
  isSample,
  models: [
    {
      metric: 'cpu',
      rmse: 0.050,
      mae: 0.038,
      r2: 0.892,
      unit: 'fraction',
      trainRows: 38400000,
      features: [
        { name: 'cpu_lag1', importance: 0.342 },
        { name: 'cpu_mean6', importance: 0.228 },
        { name: 'cpu_lag2', importance: 0.145 },
        { name: 'mem', importance: 0.089 },
        { name: 'tod_sin', importance: 0.065 },
        { name: 'n_rec', importance: 0.052 },
        { name: 'cpu_mean12', importance: 0.044 },
        { name: 'err_cnt6', importance: 0.035 }
      ]
    },
    {
      metric: 'memory',
      rmse: 0.040,
      mae: 0.030,
      r2: 0.918,
      unit: 'fraction',
      trainRows: 38400000,
      features: [
        { name: 'mem_lag1', importance: 0.381 },
        { name: 'mem_mean6', importance: 0.246 },
        { name: 'mem_lag2', importance: 0.132 },
        { name: 'cpu', importance: 0.078 },
        { name: 'n_rec_mean12', importance: 0.058 },
        { name: 'tod_cos', importance: 0.041 },
        { name: 'mem_mean12', importance: 0.039 },
        { name: 'err_cnt6', importance: 0.025 }
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
      name: 'Schema standardisation and feature preparation',
      tool: 'Scala / Apache Spark SQL',
      status: 'done',
      description: 'Implemented in spark-code/01_preprocessing_v2.scala: reads Alibaba and Google usage exports, converts timestamps to 5-minute windows, normalises CPU and memory, unions both clouds, and writes partitioned Parquet with sanity checks.'
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
      status: 'done',
      description: 'Trains GBT regressors and random forest hurdle classifiers for CPU, memory and task arrival rate forecasting.'
    },
    {
      id: 6,
      name: 'Recommendation engine',
      tool: 'Scala / Rules Engine',
      status: 'done',
      description: 'Translates workload forecasts into automated proactive actions such as scale up, scale down or hold.'
    },
    {
      id: 7,
      name: 'Static reporting dashboard',
      tool: 'React / Vite',
      status: 'done',
      description: 'Renders proactive resource forecasts, cross cloud comparison metrics and recommendations in a static interface.'
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
