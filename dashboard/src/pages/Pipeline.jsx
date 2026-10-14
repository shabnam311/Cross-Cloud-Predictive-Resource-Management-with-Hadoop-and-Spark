import React from 'react';
import { useData } from '../hooks/useData';
import { DataState } from '../components/DataState';
import { PageHeader } from '../components/PageHeader';

export function Pipeline() {
  const { data, loading, error } = useData('pipeline.json');

  const formatStatus = (status) => {
    if (status === 'done') return 'Completed';
    if (status === 'in_progress') return 'In progress';
    if (status === 'planned') return 'Planned';
    return status;
  };

  return (
    <DataState loading={loading} error={error}>
      {data && (
        <div className="page-section">
          <PageHeader
            title="Data & ML Pipeline Architecture"
            description="Sequential stages from raw cluster trace ingestion to proactive scaling recommendations."
            isSample={data.isSample}
          />

          <div className="pipeline-list">
            {data.stages?.map((stage) => (
              <div key={stage.id} className="pipeline-item">
                <div className="pipeline-num">{stage.id}.</div>
                <div className="pipeline-content">
                  <div className="pipeline-name-row">
                    <div className="pipeline-name">
                      {stage.name}
                      <span className="pipeline-tool">({stage.tool})</span>
                    </div>
                    <span className={`pipeline-status ${stage.status}`}>
                      {formatStatus(stage.status)}
                    </span>
                  </div>
                  <p className="pipeline-desc">{stage.description}</p>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </DataState>
  );
}
