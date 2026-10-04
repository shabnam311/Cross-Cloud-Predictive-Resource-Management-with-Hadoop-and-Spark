import React, { useState } from 'react';
import { useData } from '../hooks/useData';
import { DataState } from '../components/DataState';
import { PageHeader } from '../components/PageHeader';
import { formatPercent, formatDate } from '../lib/format';

export function Recommendations() {
  const { data, loading, error } = useData('recommendations.json');

  const [clusterFilter, setClusterFilter] = useState('all');
  const [actionFilter, setActionFilter] = useState('all');
  const [expandedId, setExpandedId] = useState(null);

  const filteredItems = (data?.items || []).filter((item) => {
    const matchCluster = clusterFilter === 'all' || item.cluster === clusterFilter;
    const matchAction = actionFilter === 'all' || item.action === actionFilter;
    return matchCluster && matchAction;
  });

  const toggleExpand = (id) => {
    setExpandedId((prev) => (prev === id ? null : id));
  };

  return (
    <DataState loading={loading} error={error}>
      {data && (
        <div className="page-section">
          <PageHeader
            title="Proactive Recommendations"
            description="Rule-based resource scaling actions generated from forward-looking workload predictions."
            isSample={data.isSample}
          />

          {/* Filters */}
          <div className="controls-row">
            <div style={{ display: 'flex', gap: '12px', flexWrap: 'wrap' }}>
              <select
                className="filter-select"
                value={clusterFilter}
                onChange={(e) => setClusterFilter(e.target.value)}
                aria-label="Filter by cluster"
              >
                <option value="all">All clusters</option>
                <option value="Google 2019">Google 2019</option>
                <option value="Alibaba 2018">Alibaba 2018</option>
              </select>

              <select
                className="filter-select"
                value={actionFilter}
                onChange={(e) => setActionFilter(e.target.value)}
                aria-label="Filter by action"
              >
                <option value="all">All actions</option>
                <option value="scale_up">Scale up</option>
                <option value="scale_down">Scale down</option>
                <option value="hold">Hold</option>
              </select>
            </div>

            <span style={{ fontSize: 'var(--font-size-sm)', color: 'var(--color-muted)' }}>
              Showing {filteredItems.length} of {data.items?.length || 0} actions
            </span>
          </div>

          {/* Recommendations Table */}
          {filteredItems.length === 0 ? (
            <div className="state-container" role="status">
              <div className="state-title">No recommendations match these filters</div>
              <div className="state-message">Clear a filter to see all items.</div>
            </div>
          ) : (
            <div className="table-container">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Cluster</th>
                    <th>Resource</th>
                    <th>Action</th>
                    <th>Change</th>
                    <th>Window start</th>
                    <th>Confidence</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredItems.map((item) => {
                    const isExpanded = expandedId === item.id;
                    const actionClass = `action-${item.action.replace('_', '-')}`;
                    return (
                      <React.Fragment key={item.id}>
                        <tr
                          className="row-expandable"
                          onClick={() => toggleExpand(item.id)}
                          tabIndex={0}
                          onKeyDown={(e) => {
                            if (e.key === 'Enter' || e.key === ' ') {
                              e.preventDefault();
                              toggleExpand(item.id);
                            }
                          }}
                          aria-expanded={isExpanded}
                        >
                          <td>{item.cluster}</td>
                          <td>
                            {item.resource === 'taskArrival' ? 'Task arrival' : item.resource.toUpperCase()}
                          </td>
                          <td className={actionClass}>
                            {item.action.replace('_', ' ')}
                          </td>
                          <td>{item.magnitudePercent > 0 ? `+${item.magnitudePercent}%` : '0%'}</td>
                          <td>{formatDate(item.windowStart)}</td>
                          <td>{formatPercent(item.confidence, 0)}</td>
                        </tr>
                        {isExpanded && (
                          <tr>
                            <td colSpan={6} className="table-expanded-reason">
                              <strong>Decision reason:</strong> {item.reason}
                            </td>
                          </tr>
                        )}
                      </React.Fragment>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </DataState>
  );
}
