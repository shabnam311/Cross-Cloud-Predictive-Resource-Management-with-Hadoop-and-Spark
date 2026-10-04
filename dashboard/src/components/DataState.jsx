import React from 'react';

export function DataState({ loading, error, empty, emptyMessage, children }) {
  if (loading) {
    return (
      <div className="state-container" role="status" aria-live="polite">
        <div className="state-title">Loading data</div>
        <div className="state-message">Fetching exported dataset files...</div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="state-container" role="alert">
        <div className="state-title">Unable to load data</div>
        <div className="state-message">{error}</div>
      </div>
    );
  }

  if (empty) {
    return (
      <div className="state-container" role="status">
        <div className="state-title">No items found</div>
        <div className="state-message">
          {emptyMessage || 'No records match the selected criteria. Adjust filters to display data.'}
        </div>
      </div>
    );
  }

  return children;
}
