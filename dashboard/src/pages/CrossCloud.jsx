import React from 'react';
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
} from 'recharts';
import { useData } from '../hooks/useData';
import { DataState } from '../components/DataState';
import { PageHeader } from '../components/PageHeader';
import { Figure } from '../components/Figure';
import { colors } from '../lib/colors';
import { formatDecimal, formatPercent } from '../lib/format';

export function CrossCloud() {
  const { data, loading, error } = useData('comparison.json');

  // Compute grouped bar chart data using relative error (RMSE / target mean) per Item 9
  const metricKeys = ['cpu', 'memory', 'taskArrival'];
  const metricLabels = { cpu: 'CPU usage', memory: 'Memory', taskArrival: 'Task arrival' };

  const avgRelativeError = (rows, metric, same) => {
    const filtered = rows.filter(
      (r) => r.metric === metric && (r.trainOn === r.testOn) === same
    );
    if (filtered.length === 0) return 0;
    return (
      filtered.reduce((t, r) => t + (r.relativeError !== undefined ? r.relativeError : r.rmse), 0) /
      filtered.length
    );
  };

  const chartData = data?.rows
    ? metricKeys.map((m) => ({
        name: metricLabels[m],
        sameCloud: avgRelativeError(data.rows, m, true),
        crossCloud: avgRelativeError(data.rows, m, false),
      }))
    : [];

  // Group rows by metric to find the lowest RMSE for each metric
  const bestRmseByMetric = {};
  if (data?.rows) {
    data.rows.forEach((row) => {
      if (!bestRmseByMetric[row.metric] || row.rmse < bestRmseByMetric[row.metric]) {
        bestRmseByMetric[row.metric] = row.rmse;
      }
    });
  }

  return (
    <DataState loading={loading} error={error}>
      {data && (
        <div className="page-section">
          <PageHeader
            title="Cross-Cloud Generalisation"
            description="Evaluates whether predictive models trained on Google Cluster traces generalise effectively to Alibaba workloads and vice versa."
            isSample={data.isSample}
          />

          <p>
            This comparison tests domain transferability by training Gradient Boosted Tree models on one
            cloud provider and evaluating forecast error directly against the other provider without retraining.
          </p>

          {/* Grouped Bar Chart of Relative Error */}
          <Figure
            caption="Average relative error (RMSE divided by target mean) for same-cloud baseline evaluation versus zero-shot cross-cloud transfer."
            ariaLabel="Grouped bar chart comparing same cloud vs cross cloud relative error."
          >
            <div className="chart-wrapper">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={chartData} margin={{ top: 16, right: 16, bottom: 8, left: 0 }}>
                  <CartesianGrid stroke={colors.line} vertical={false} />
                  <XAxis
                    dataKey="name"
                    stroke={colors.muted}
                    fontSize={12}
                    tickLine={false}
                    axisLine={{ stroke: colors.line }}
                  />
                  <YAxis
                    stroke={colors.muted}
                    fontSize={12}
                    tickLine={false}
                    axisLine={{ stroke: colors.line }}
                    tickFormatter={(v) => formatPercent(v, 0)}
                    domain={[0, 0.25]}
                  />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: colors.surface,
                      borderColor: colors.line,
                      borderRadius: 6,
                      fontSize: 13,
                    }}
                    formatter={(val) => [formatPercent(val, 1), 'Relative error']}
                  />
                  <Legend
                    verticalAlign="top"
                    align="right"
                    iconType="rect"
                    wrapperStyle={{ fontSize: 13, paddingBottom: 12 }}
                  />
                  <Bar dataKey="sameCloud" name="Same cloud (baseline)" fill={colors.pine} />
                  <Bar dataKey="crossCloud" name="Cross cloud (transfer)" fill={colors.muted} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </Figure>

          {/* Detailed Evaluation Metrics Table with Unit and Relative Error */}
          <div className="page-section">
            <h2 style={{ fontSize: 'var(--font-size-title-sm)' }}>Detailed evaluation metrics</h2>
            <div className="table-container">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Metric</th>
                    <th>Train on</th>
                    <th>Test on</th>
                    <th>RMSE</th>
                    <th>MAE</th>
                    <th>Unit</th>
                    <th>Relative error</th>
                    <th>R² score</th>
                  </tr>
                </thead>
                <tbody>
                  {data.rows?.map((row, idx) => {
                    const isBest = row.rmse === bestRmseByMetric[row.metric];
                    const isTrainAlibaba = row.trainOn.includes('Alibaba');
                    const isTestAlibaba = row.testOn.includes('Alibaba');
                    return (
                      <tr key={idx} className={isBest ? 'highlight-best' : ''}>
                        <td>{row.metric === 'taskArrival' ? 'Task arrival' : row.metric.toUpperCase()}</td>
                        <td>
                          <span style={{ color: isTrainAlibaba ? colors.alibaba : colors.google, fontWeight: 500 }}>
                            {row.trainOn}
                          </span>
                        </td>
                        <td>
                          <span style={{ color: isTestAlibaba ? colors.alibaba : colors.google, fontWeight: 500 }}>
                            {row.testOn}
                          </span>
                        </td>
                        <td>{formatDecimal(row.rmse, row.metric === 'taskArrival' ? 1 : 3)}</td>
                        <td>{formatDecimal(row.mae, row.metric === 'taskArrival' ? 1 : 3)}</td>
                        <td style={{ color: colors.muted, fontSize: 'var(--font-size-sm)' }}>
                          {row.unit || (row.metric === 'taskArrival' ? 'tasks/min' : 'fraction')}
                        </td>
                        <td>
                          {row.relativeError !== undefined
                            ? formatPercent(row.relativeError, 1)
                            : formatPercent(
                                row.rmse / (row.metric === 'taskArrival' ? 440 : row.metric === 'memory' ? 0.70 : 0.56),
                                1
                              )}
                        </td>
                        <td>{formatDecimal(row.r2, 3)}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}
    </DataState>
  );
}
