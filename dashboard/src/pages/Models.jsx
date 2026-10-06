import React from 'react';
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
} from 'recharts';
import { useData } from '../hooks/useData';
import { DataState } from '../components/DataState';
import { PageHeader } from '../components/PageHeader';
import { Figure } from '../components/Figure';
import { colors } from '../lib/colors';
import { formatDecimal, formatNumber, formatPercent } from '../lib/format';

export function Models() {
  const { data, loading, error } = useData('models.json');

  const modelLabels = {
    cpu: 'CPU utilization model (GBT Regressor)',
    memory: 'Memory consumption model (GBT Regressor)',
    taskArrival: 'Task arrival rate model (Hurdle Classifier + GBT)',
  };

  return (
    <DataState loading={loading} error={error}>
      {data && (
        <div className="page-section">
          <PageHeader
            title="Model Architecture & Feature Importance"
            description="Spark MLlib Gradient Boosted Tree regressors and top predictive feature contributions."
            isSample={data.isSample}
          />

          {data.models?.map((model) => (
            <div key={model.metric} className="page-section" style={{ gap: '16px' }}>
              <h2 style={{ fontSize: 'var(--font-size-title-sm)' }}>
                {modelLabels[model.metric] || model.metric}
              </h2>

              {/* Inline metric stats */}
              <div className="stat-row" style={{ padding: '16px 0' }}>
                <div className="stat-item">
                  <span className="stat-label">
                    {model.metric === 'taskArrival' ? 'RMSE (tasks/min)' : 'RMSE (normalised)'}
                  </span>
                  <span className="stat-value">
                    {model.metric === 'taskArrival'
                      ? `${formatDecimal(model.rmse, 1)} / min`
                      : formatDecimal(model.rmse, 3)}
                  </span>
                </div>
                <div className="stat-item">
                  <span className="stat-label">
                    {model.metric === 'taskArrival' ? 'MAE (tasks/min)' : 'MAE (normalised)'}
                  </span>
                  <span className="stat-value">
                    {model.metric === 'taskArrival'
                      ? `${formatDecimal(model.mae, 1)} / min`
                      : formatDecimal(model.mae, 3)}
                  </span>
                </div>
                <div className="stat-item">
                  <span className="stat-label">R² score</span>
                  <span className="stat-value">{formatDecimal(model.r2, 3)}</span>
                </div>
                <div className="stat-item">
                  <span className="stat-label">Training records</span>
                  <span className="stat-value">{formatNumber(model.trainRows)}</span>
                </div>
              </div>

              {/* Horizontal Bar Chart for Top 8 Features */}
              <Figure
                caption={`Top predictive features ranked by Gini split importance for the ${model.metric} model.`}
                ariaLabel={`Feature importance chart for ${model.metric}`}
              >
                <div style={{ width: '100%', height: 260 }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart
                      layout="vertical"
                      data={model.features?.slice(0, 8)}
                      margin={{ top: 8, right: 24, bottom: 8, left: 60 }}
                    >
                      <CartesianGrid stroke={colors.line} horizontal={false} />
                      <XAxis
                        type="number"
                        stroke={colors.muted}
                        fontSize={12}
                        tickLine={false}
                        axisLine={{ stroke: colors.line }}
                        tickFormatter={(v) => formatPercent(v, 0)}
                      />
                      <YAxis
                        type="category"
                        dataKey="name"
                        stroke={colors.ink}
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
                        formatter={(val) => [formatPercent(val, 1), 'Importance']}
                      />
                      <Bar dataKey="importance" fill={colors.pine} radius={[0, 4, 4, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </Figure>

              <hr className="hairline" style={{ marginTop: '16px' }} />
            </div>
          ))}
        </div>
      )}
    </DataState>
  );
}
