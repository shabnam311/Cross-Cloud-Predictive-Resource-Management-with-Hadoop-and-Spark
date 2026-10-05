import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { useData } from '../hooks/useData';
import { DataState } from '../components/DataState';
import { PageHeader } from '../components/PageHeader';
import { Figure } from '../components/Figure';
import { ForecastChart } from '../components/ForecastChart';
import { ChartTableToggle } from '../components/ChartTableToggle';
import { formatPercent, formatNumber, formatDate } from '../lib/format';

export function Overview() {
  const { data: overview, loading: loadingOverview, error: errorOverview } = useData('overview.json');
  const { data: forecasts, loading: loadingForecasts, error: errorForecasts } = useData('forecasts.json');
  const { data: recs, loading: loadingRecs, error: errorRecs } = useData('recommendations.json');

  const [selectedCluster, setSelectedCluster] = useState('google');
  const [viewMode, setViewMode] = useState('chart');

  const loading = loadingOverview || loadingForecasts || loadingRecs;
  const error = errorOverview || errorForecasts || errorRecs;

  return (
    <DataState loading={loading} error={error}>
      {overview && (
        <div className="page-section">
          <PageHeader
            title="System Overview"
            description="High-level cluster telemetry forecasts and automated scaling recommendations."
            isSample={overview.isSample}
          />

          {/* Large Editorial Sentence */}
          <div className="overview-headline">
            Over the next hour, CPU on{' '}
            {selectedCluster === 'google' ? 'Google 2019' : 'Alibaba 2018'} is forecast to reach{' '}
            {formatPercent(overview.nextHour?.[selectedCluster]?.cpu, 0)}.
          </div>

          {/* Inline Stat Row */}
          <div className="stat-row">
            <div className="stat-item">
              <span className="stat-label">Forecast CPU</span>
              <span className="stat-value">
                {formatPercent(overview.nextHour?.[selectedCluster]?.cpu, 1)}
              </span>
            </div>
            <div className="stat-item">
              <span className="stat-label">Forecast memory</span>
              <span className="stat-value">
                {formatPercent(overview.nextHour?.[selectedCluster]?.memory, 1)}
              </span>
            </div>
            <div className="stat-item">
              <span className="stat-label">Forecast task arrival</span>
              <span className="stat-value">
                {formatNumber(overview.nextHour?.[selectedCluster]?.taskArrival)} / min
              </span>
            </div>
          </div>

          {/* Cluster Selector & Chart/Table Toggle */}
          <div className="controls-row">
            <div className="segmented-group" role="group" aria-label="Cluster selector">
              <button
                type="button"
                className={`segmented-btn ${selectedCluster === 'google' ? 'active' : ''}`}
                onClick={() => setSelectedCluster('google')}
                aria-pressed={selectedCluster === 'google'}
              >
                Google 2019
              </button>
              <button
                type="button"
                className={`segmented-btn ${selectedCluster === 'alibaba' ? 'active' : ''}`}
                onClick={() => setSelectedCluster('alibaba')}
                aria-pressed={selectedCluster === 'alibaba'}
              >
                Alibaba 2018
              </button>
            </div>

            <ChartTableToggle view={viewMode} onToggle={setViewMode} />
          </div>

          {/* Forecast Chart / Table */}
          {forecasts?.series?.cpu?.[selectedCluster] && (
            <Figure
              caption={`48-hour CPU telemetry and 30-minute proactive forecast horizon for ${
                selectedCluster === 'google' ? 'Google 2019' : 'Alibaba 2018'
              }.`}
              ariaLabel="48 hour CPU utilization time series chart with forecast horizon."
            >
              {viewMode === 'chart' ? (
                <ForecastChart
                  data={forecasts.series.cpu[selectedCluster]}
                  metric="cpu"
                  cluster={selectedCluster}
                />
              ) : (
                <div className="table-container">
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Timestamp</th>
                        <th>Actual CPU</th>
                        <th>Predicted CPU</th>
                      </tr>
                    </thead>
                    <tbody>
                      {forecasts.series.cpu[selectedCluster].slice(-24).map((row) => (
                        <tr key={row.t}>
                          <td>{formatDate(row.t)}</td>
                          <td>{row.actual !== null ? formatPercent(row.actual, 2) : '-'}</td>
                          <td>{formatPercent(row.predicted, 2)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </Figure>
          )}

          <hr className="hairline" />

          {/* Top 3 Recommendations */}
          <div className="page-section">
            <div className="controls-row">
              <h2 style={{ fontSize: 'var(--font-size-title-sm)' }}>Active recommendations</h2>
              <Link to="/recommendations" style={{ fontSize: 'var(--font-size-sm)' }}>
                View all recommendations
              </Link>
            </div>

            {recs?.items && (
              <div className="table-container">
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>Cluster</th>
                      <th>Resource</th>
                      <th>Action</th>
                      <th>Change</th>
                      <th>Window</th>
                      <th>Confidence</th>
                    </tr>
                  </thead>
                  <tbody>
                    {recs.items.slice(0, 3).map((item) => (
                      <tr key={item.id}>
                        <td>{item.cluster}</td>
                        <td>{item.resource.toUpperCase()}</td>
                        <td className={`action-${item.action.replace('_', '-')}`}>
                          {item.action.replace('_', ' ')}
                        </td>
                        <td>{item.magnitudePercent > 0 ? `+${item.magnitudePercent}%` : '0%'}</td>
                        <td>{formatDate(item.windowStart)}</td>
                        <td>{formatPercent(item.confidence, 0)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      )}
    </DataState>
  );
}
