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

  const visibleRows = data?.rows || [];
  const chartData = visibleRows.map((row) => ({
    name: `${row.metric === 'cpu' ? 'CPU' : 'Memory'} · ${row.cloud}`,
    gbtRmse: row.gbtRmse,
    randomForestRmse: row.randomForestRmse,
  }));

  return (
    <DataState loading={loading} error={error}>
      {data && (
        <div className="page-section">
          <PageHeader
            title="Cross-Cloud Generalisation"
            description="Compares RMSE for Gradient Boosted Tree and Random Forest models across the Google and Alibaba workloads."
            isSample={data.isSample}
          />

          <p>
            This comparison reports the root mean square error (RMSE) of the two models used by the project.
          </p>

          {/* Grouped Bar Chart of RMSE */}
          <Figure
            caption="RMSE comparison for GBT and Random Forest across CPU and memory workloads. Lower values indicate lower prediction error."
            ariaLabel="Grouped bar chart comparing GBT and Random Forest RMSE."
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
                  <YAxis stroke={colors.muted} fontSize={12} tickLine={false} axisLine={{ stroke: colors.line }} />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: colors.surface,
                      borderColor: colors.line,
                      borderRadius: 6,
                      fontSize: 13,
                    }}
                    formatter={(val) => [formatDecimal(val, 4), 'RMSE']}
                  />
                  <Legend
                    verticalAlign="top"
                    align="right"
                    iconType="rect"
                    wrapperStyle={{ fontSize: 13, paddingBottom: 12 }}
                  />
                  <Bar dataKey="gbtRmse" name="GBT RMSE" fill={colors.pine} />
                  <Bar dataKey="randomForestRmse" name="Random Forest RMSE" fill={colors.muted} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </Figure>

          {/* RMSE comparison table */}
          <div className="page-section">
            <h2 style={{ fontSize: 'var(--font-size-title-sm)' }}>RMSE by model</h2>
            <div className="table-container">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Metric</th>
                    <th>Cloud</th>
                    <th>GBT RMSE</th>
                    <th>Random Forest RMSE</th>
                  </tr>
                </thead>
                <tbody>
                  {visibleRows.map((row) => (
                    <tr key={`${row.metric}-${row.cloud}`}>
                      <td>{row.metric === 'cpu' ? 'CPU' : 'Memory'}</td>
                      <td style={{ color: row.cloud.includes('Alibaba') ? colors.alibaba : colors.google, fontWeight: 500 }}>
                        {row.cloud}
                      </td>
                      <td>{formatDecimal(row.gbtRmse, 4)}</td>
                      <td>{formatDecimal(row.randomForestRmse, 4)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}
    </DataState>
  );
}
