import React from 'react';
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ReferenceArea,
} from 'recharts';
import { colors } from '../lib/colors';
import { formatTime, formatDate, formatPercent, formatNumber } from '../lib/format';

function CustomTooltip({ active, payload, label, metric }) {
  if (!active || !payload || !payload.length) return null;

  const isPercent = metric === 'cpu' || metric === 'memory';

  return (
    <div className="chart-tooltip">
      <div className="chart-tooltip-time">{formatDate(label)}</div>
      {payload.map((entry) => {
        const valStr = isPercent
          ? formatPercent(entry.value, 1)
          : formatNumber(entry.value);
        return (
          <div key={entry.name} style={{ color: entry.color || colors.ink }}>
            {entry.name}: {entry.value !== null && entry.value !== undefined ? valStr : '—'}
          </div>
        );
      })}
    </div>
  );
}

export function ForecastChart({ data, metric = 'cpu', cluster = 'google', showHorizon = true }) {
  if (!data || data.length === 0) return null;

  // Identify forecast horizon (last 6 data points where actual is null)
  const horizonIndex = data.findIndex((d) => d.actual === null);
  const horizonStart = horizonIndex >= 0 ? data[horizonIndex]?.t : null;
  const horizonEnd = data[data.length - 1]?.t;

  const isPercent = metric === 'cpu' || metric === 'memory';
  const strokeColor = cluster === 'alibaba' ? colors.alibaba : colors.google;

  return (
    <div className="chart-wrapper">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ top: 12, right: 16, bottom: 8, left: 0 }}>
          <CartesianGrid stroke={colors.line} vertical={false} />
          <XAxis
            dataKey="t"
            tickFormatter={formatTime}
            stroke={colors.muted}
            fontSize={12}
            tickLine={false}
            axisLine={{ stroke: colors.line }}
            minTickGap={40}
          />
          <YAxis
            stroke={colors.muted}
            fontSize={12}
            tickLine={false}
            axisLine={{ stroke: colors.line }}
            tickFormatter={(val) => (isPercent ? `${Math.round(val * 100)}%` : val)}
            domain={isPercent ? [0, 1] : ['auto', 'auto']}
          />
          <Tooltip content={<CustomTooltip metric={metric} />} />

          {showHorizon && horizonStart && (
            <ReferenceArea
              x1={horizonStart}
              x2={horizonEnd}
              fill={colors.pineFaint}
              strokeOpacity={0}
              label={{
                value: 'Forecast horizon',
                position: 'insideTopLeft',
                fill: colors.pine,
                fontSize: 11,
              }}
            />
          )}

          <Line
            type="monotone"
            dataKey="actual"
            name="Actual"
            stroke={strokeColor}
            strokeWidth={1.5}
            dot={false}
            isAnimationActive={true}
            animationDuration={700}
            connectNulls={false}
          />

          <Line
            type="monotone"
            dataKey="predicted"
            name="Predicted"
            stroke={strokeColor}
            strokeWidth={1.5}
            strokeDasharray="4 4"
            dot={false}
            isAnimationActive={true}
            animationDuration={700}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
