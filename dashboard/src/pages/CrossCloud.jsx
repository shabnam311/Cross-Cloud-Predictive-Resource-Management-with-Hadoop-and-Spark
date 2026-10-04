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
import { formatDecimal } from '../lib/format';

export function CrossCloud() {
  const { data, loading, error } = useData('comparison.json');

  // Prepare grouped bar chart data for normalized error comparison
  // Same cloud (Google->Google, Alibaba->Alibaba) vs Cross cloud (Google->Alibaba, Alibaba->Google)
  const chartData = [
    {
      name: 'CPU usage',
      sameCloud: 0.05,
      crossCloud: 0.091,
    },
    {
      name: 'Memory',
      sameCloud: 0.04,
      crossCloud: 0.078,
    },
    {
      name: 'Task arrival (/100)',
      sameCloud: 0.272,
      crossCloud: 0.564,
    },
  ];

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

          {/* Grouped Bar Chart */}
          <Figure
            caption="Average root mean square error (RMSE) for same-cloud baseline evaluation versus zero-shot cross-cloud transfer."
            ariaLabel="Grouped bar chart comparing same cloud vs cross cloud RMSE error."
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
                  />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: colors.surface,
                      borderColor: colors.line,
                      borderRadius: 6,
                      fontSize: 13,
                    }}
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

          {/* Comparison Table */}
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
                    <th>R² score</th>
                  </tr>
                </thead>
                <tbody>
                  {data.rows?.map((row, idx) => {
                    const isBest = row.rmse === bestRmseByMetric[row.metric];
                    return (
                      <tr key={idx} className={isBest ? 'highlight-best' : ''}>
                        <td>{row.metric === 'taskArrival' ? 'Task arrival' : row.metric.toUpperCase()}</td>
                        <td>{row.trainOn}</td>
                        <td>{row.testOn}</td>
                        <td>{formatDecimal(row.rmse, row.metric === 'taskArrival' ? 1 : 3)}</td>
                        <td>{formatDecimal(row.mae, row.metric === 'taskArrival' ? 1 : 3)}</td>
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
