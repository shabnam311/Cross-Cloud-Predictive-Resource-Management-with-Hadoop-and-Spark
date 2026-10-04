import React from 'react';

export function ChartTableToggle({ view, onToggle }) {
  return (
    <div className="segmented-group" role="group" aria-label="View toggle">
      <button
        type="button"
        className={`segmented-btn ${view === 'chart' ? 'active' : ''}`}
        onClick={() => onToggle('chart')}
        aria-pressed={view === 'chart'}
      >
        Chart
      </button>
      <button
        type="button"
        className={`segmented-btn ${view === 'table' ? 'active' : ''}`}
        onClick={() => onToggle('table')}
        aria-pressed={view === 'table'}
      >
        Table
      </button>
    </div>
  );
}
