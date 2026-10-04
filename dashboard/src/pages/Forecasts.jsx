import React, { useState } from 'react';
import { useData } from '../hooks/useData';
import { DataState } from '../components/DataState';
import { PageHeader } from '../components/PageHeader';
import { Figure } from '../components/Figure';
import { ForecastChart } from '../components/ForecastChart';
import { ChartTableToggle } from '../components/ChartTableToggle';
import { formatPercent, formatNumber, formatDecimal, formatDate } from '../lib/format';

export function Forecasts() {
  const { data: forecasts, loading: loadingForecasts, error: errorForecasts } = useData('forecasts.json');
  const { data: comparison, loading: loadingComparison, error: errorComparison } = useData('comparison.json');

  const [metric, setMetric] = useState('cpu'); // 'cpu' | 'memory' | 'taskArrival'
  const [cluster, setCluster] = useState('google'); // 'google' | 'alibaba'
  const [viewMode, setViewMode] = useState('chart');

  const loading = loadingForecasts || loadingComparison;
  const error = errorForecasts || errorComparison;

  // Retrieve metrics from comparison.json for the active selection
  const clusterLabel = cluster === 'google' ? 'Google 2019' : 'Alibaba 2018';
  const metricRow = comparison?.rows?.find(
    (r) => r.metric === metric && r.trainOn === clusterLabel && r.testOn === clusterLabel
  );

  const seriesData = forecasts?.series?.[metric]?.[cluster] || [];

  return (
    <DataState loading={loading} error={error}>
      {forecasts && (
        <div className="page-section">
          <PageHeader
            title="Workload Forecasts"
            description="Proactive multi-horizon resource forecasting across cluster compute, memory and task arrivals."
            isSample={forecasts.isSample}
          />

          {/* Metric Tabs & Cluster Controls */}
          <div className="controls-row">
            <div className="segmented-group" role="tablist" aria-label="Metric selector">
              <button
                type="button"
                role="tab"
                aria-selected={metric === 'cpu'}
                className={`segmented-btn ${metric === 'cpu' ? 'active' : ''}`}
                onClick={() => setMetric('cpu')}
              >
                CPU usage
              </button>
              <button
                type="button"
                role="tab"
                aria-selected={metric === 'memory'}
                className={`segmented-btn ${metric === 'memory' ? 'active' : ''}`}
                onClick={() => setMetric('memory')}
              >
                Memory consumption
              </button>
              <button
                type="button"
                role="tab"
                aria-selected={metric === 'taskArrival'}
                className={`segmented-btn ${metric === 'taskArrival' ? 'active' : ''}`}
                onClick={() => setMetric('taskArrival')}
              >
                Task arrival rate
              </button>
            </div>

            <div style={{ display: 'flex', gap: '12px' }}>
              <div className="segmented-group" role="group" aria-label="Cluster selector">
                <button
                  type="button"
                  className={`segmented-btn ${cluster === 'google' ? 'active' : ''}`}
                  onClick={() => setCluster('google')}
                  aria-pressed={cluster === 'google'}
                >
                  Google 2019
                </button>
                <button
                  type="button"
                  className={`segmented-btn ${cluster === 'alibaba' ? 'active' : ''}`}
                  onClick={() => setCluster('alibaba')}
                  aria-pressed={cluster === 'alibaba'}
                >
                  Alibaba 2018
                </button>
              </div>

              <ChartTableToggle view={viewMode} onToggle={setViewMode} />
            </div>
          </div>

          {/* Forecast Chart / Table */}
          <Figure
            caption={`48-hour time series and 30-minute proactive forecast horizon for ${clusterLabel} (${
              metric === 'cpu'
                ? 'CPU utilization'
                : metric === 'memory'
                ? 'memory consumption'
                : 'task arrivals per minute'
            }).`}
            ariaLabel="Workload forecast chart showing solid actual values and dashed predicted values."
          >
            {viewMode === 'chart' ? (
              <ForecastChart data={seriesData} metric={metric} cluster={cluster} />
            ) : (
              <div className="table-container">
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>Timestamp</th>
                      <th>Actual value</th>
                      <th>Predicted value</th>
                    </tr>
                  </thead>
                  <tbody>
                    {seriesData.slice(-36).map((row) => (
                      <tr key={row.t}>
                        <td>{formatDate(row.t)}</td>
                        <td>
                          {row.actual !== null
                            ? metric === 'taskArrival'
                              ? formatNumber(row.actual)
                              : formatPercent(row.actual, 2)
                            : '—'}
                        </td>
                        <td>
                          {metric === 'taskArrival'
                            ? formatNumber(row.predicted)
                            : formatPercent(row.predicted, 2)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </Figure>

          {/* Two-column RMSE / MAE Metric Summary */}
          {metricRow && (
            <div className="stat-row" style={{ marginTop: '8px' }}>
              <div className="stat-item">
                <span className="stat-label">Root mean square error (RMSE)</span>
                <span className="stat-value">
                  {metric === 'taskArrival'
                    ? formatDecimal(metricRow.rmse, 1)
                    : formatDecimal(metricRow.rmse, 3)}
                </span>
              </div>
              <div className="stat-item">
                <span className="stat-label">Mean absolute error (MAE)</span>
                <span className="stat-value">
                  {metric === 'taskArrival'
                    ? formatDecimal(metricRow.mae, 1)
                    : formatDecimal(metricRow.mae, 3)}
                </span>
              </div>
              <div className="stat-item">
                <span className="stat-label">Coefficient of determination (R²)</span>
                <span className="stat-value">{formatDecimal(metricRow.r2, 3)}</span>
              </div>
            </div>
          )}
        </div>
      )}
    </DataState>
  );
}
