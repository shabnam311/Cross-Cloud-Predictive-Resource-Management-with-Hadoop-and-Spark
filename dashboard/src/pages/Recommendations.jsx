import React, { useState } from 'react';
import { useRecommendations } from '../hooks/useRecommendations';
import { PageHeader } from '../components/PageHeader';
import { formatPercent } from '../lib/format';

export function Recommendations() {
  const [cloudFilter, setCloudFilter] = useState('all');
  const [actionFilter, setActionFilter] = useState('all');
  const [alertFilter, setAlertFilter] = useState('all');
  const [expandedId, setExpandedId] = useState(null);

  // Pass undefined when 'all' is selected so useRecommendations fetches all clouds
  const selectedCloud = cloudFilter === 'all' ? undefined : cloudFilter;
  const { machines, summary, connected } = useRecommendations(selectedCloud);

  const filteredMachines = machines.filter((m) => {
    const matchCloud = cloudFilter === 'all' || m.cloud === cloudFilter;
    const matchAction = actionFilter === 'all' || m.action === actionFilter;
    const matchAlert = alertFilter === 'all' || m.alert_level === alertFilter;
    return matchCloud && matchAction && matchAlert;
  });

  const toggleExpand = (nodeId) => {
    setExpandedId((prev) => (prev === nodeId ? null : nodeId));
  };

  // Map backend action ("SCALE_UP", "SCALE_DOWN", "MAINTAIN") to existing CSS classes
  const getActionClass = (action) => {
    if (action === 'SCALE_UP') return 'action-scale-up';
    if (action === 'SCALE_DOWN') return 'action-scale-down';
    return 'action-hold';
  };

  const formatChange = (cpuPct, memPct) => {
    const parts = [];
    if (cpuPct !== 0) parts.push(`CPU ${cpuPct > 0 ? `+${cpuPct}%` : `${cpuPct}%`}`);
    if (memPct !== 0) parts.push(`MEM ${memPct > 0 ? `+${memPct}%` : `${memPct}%`}`);
    return parts.length > 0 ? parts.join(', ') : '0%';
  };

  return (
    <div className="page-section">
      <PageHeader
        title="Proactive Recommendations"
        description="Real-time resource scaling decisions streamed from Spark Structured Streaming via FastAPI."
        isSample={false}
      />

      {/* Live Stream Status & Window Summary Bar */}
      <div className="simulation-bar">
        <div className="simulation-header">
          <div className="simulation-title-group">
            <span className="simulation-title">
              Spark Structured Streaming &rarr; FastAPI WebSocket
            </span>
            {connected && <span className="pulse-indicator" title="WebSocket connected" />}
          </div>
          <div className="simulation-actions">
            <span
              style={{
                fontSize: 'var(--font-size-sm)',
                fontWeight: 600,
                color: connected ? '#16a34a' : '#dc2626',
              }}
            >
              {connected ? '● HISTORY STREAM CONNECTED' : '○ SERVER CONNECTION OFFLINE (No data > 15s)'}
            </span>
          </div>
        </div>

        {summary ? (
          <div className="simulation-log" role="log" aria-live="polite">
            [Window t={summary.timestamp}s | Cloud: {summary.cloud.toUpperCase()}] Machines:{' '}
            {summary.total} | Critical: {summary.critical} | Warning: {summary.warning} | Scale Up:{' '}
            {summary.scale_up} | Scale Down: {summary.scale_down}
            {summary.arrival_spike ? ' | ⚠️ TASK ARRIVAL SPIKE' : ''}
          </div>
        ) : (
          <div className="simulation-log" role="log">
            Waiting for next micro-batch window from Spark... (Showing {machines.length} cached machines)
          </div>
        )}
      </div>

      {/* Filters */}
      <div className="controls-row">
        <div style={{ display: 'flex', gap: '12px', flexWrap: 'wrap' }}>
          <select
            className="filter-select"
            value={cloudFilter}
            onChange={(e) => setCloudFilter(e.target.value)}
            aria-label="Filter by cloud"
          >
            <option value="all">All clouds</option>
            <option value="google">Google 2019</option>
            <option value="alibaba">Alibaba 2018</option>
          </select>

          <select
            className="filter-select"
            value={actionFilter}
            onChange={(e) => setActionFilter(e.target.value)}
            aria-label="Filter by action"
          >
            <option value="all">All actions</option>
            <option value="SCALE_UP">Scale up</option>
            <option value="SCALE_DOWN">Scale down</option>
            <option value="MAINTAIN">Maintain</option>
          </select>

          <select
            className="filter-select"
            value={alertFilter}
            onChange={(e) => setAlertFilter(e.target.value)}
            aria-label="Filter by alert level"
          >
            <option value="all">All alert levels</option>
            <option value="CRITICAL">Critical</option>
            <option value="WARNING">Warning</option>
            <option value="NORMAL">Normal</option>
          </select>
        </div>

        <span style={{ fontSize: 'var(--font-size-sm)', color: 'var(--color-muted)' }}>
          Showing {filteredMachines.length} of {machines.length} machines
        </span>
      </div>

      {/* Recommendations Table */}
      {filteredMachines.length === 0 ? (
        <div className="state-container" role="status">
          <div className="state-title">
            {machines.length === 0
              ? 'No stream data received yet'
              : 'No machines match these filters'}
          </div>
          <div className="state-message">
            {machines.length === 0
              ? 'Ensure uvicorn main:app and your Spark stream / replayer are running.'
              : 'Clear a filter to see all machines.'}
          </div>
        </div>
      ) : (
        <div className="table-container">
          <table className="data-table">
            <thead>
              <tr>
                <th>Node ID</th>
                <th>Cloud</th>
                <th>Cur CPU / Mem</th>
                <th>Pred CPU / Mem</th>
                <th>Alert</th>
                <th>Action</th>
                <th>Change</th>
                <th>Window (s)</th>
              </tr>
            </thead>
            <tbody>
              {filteredMachines.map((m) => {
                const isExpanded = expandedId === m.node_id;
                const actionClass = getActionClass(m.action);
                return (
                  <React.Fragment key={m.node_id}>
                    <tr
                      className="row-expandable"
                      onClick={() => toggleExpand(m.node_id)}
                      tabIndex={0}
                      onKeyDown={(e) => {
                        if (e.key === 'Enter' || e.key === ' ') {
                          e.preventDefault();
                          toggleExpand(m.node_id);
                        }
                      }}
                      aria-expanded={isExpanded}
                    >
                      <td>
                        <strong>{m.node_id}</strong>
                        <span className="live-badge">Live</span>
                      </td>
                      <td style={{ textTransform: 'capitalize' }}>{m.cloud}</td>
                      <td>
                        {m.cur_cpu_util != null ? formatPercent(m.cur_cpu_util, 1) : '—'} /{' '}
                        {m.cur_mem_util != null ? formatPercent(m.cur_mem_util, 1) : '—'}
                      </td>
                      <td>
                        <strong>{formatPercent(m.pred_cpu_util, 1)}</strong> /{' '}
                        <strong>{formatPercent(m.pred_mem_util, 1)}</strong>
                      </td>
                      <td>{m.alert_level}</td>
                      <td className={actionClass}>{m.action.replace('_', ' ')}</td>
                      <td>{formatChange(m.cpu_change_pct, m.mem_change_pct)}</td>
                      <td>{m.timestamp}</td>
                    </tr>
                    {isExpanded && (
                      <tr>
                        <td colSpan={8} className="table-expanded-reason">
                          <div>
                            <strong>Decision reason:</strong> {m.reason}
                            {m.last_action && (
                              <span>
                                {' '}
                                | <strong>Last action:</strong> {m.last_action} at t=
                                {m.last_action_timestamp}s
                              </span>
                            )}
                          </div>
                          {m.previous && (
                            <div style={{ marginTop: '6px', color: 'var(--color-muted)' }}>
                              <strong>Previous Window Verification (t={m.previous.timestamp}s):</strong>{' '}
                              CPU Pred {formatPercent(m.previous.pred_cpu_util, 1)} vs Actual{' '}
                              {m.previous.actual_cpu_util != null
                                ? formatPercent(m.previous.actual_cpu_util, 1)
                                : '—'}{' '}
                              (Error:{' '}
                              {m.previous.error_cpu != null
                                ? `${(m.previous.error_cpu * 100).toFixed(2)}%`
                                : '—'}
                              ) | MEM Pred {formatPercent(m.previous.pred_mem_util, 1)} vs Actual{' '}
                              {m.previous.actual_mem_util != null
                                ? formatPercent(m.previous.actual_mem_util, 1)
                                : '—'}{' '}
                              (Error:{' '}
                              {m.previous.error_mem != null
                                ? `${(m.previous.error_mem * 100).toFixed(2)}%`
                                : '—'}
                              )
                            </div>
                          )}
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
  );
}
