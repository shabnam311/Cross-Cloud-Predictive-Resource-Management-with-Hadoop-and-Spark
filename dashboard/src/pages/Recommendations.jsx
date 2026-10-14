import React, { useState, useEffect, useRef } from 'react';
import { useData } from '../hooks/useData';
import { DataState } from '../components/DataState';
import { PageHeader } from '../components/PageHeader';
import { formatPercent, formatDate } from '../lib/format';

export function Recommendations() {
  const { data, loading, error } = useData('recommendations.json');

  const [clusterFilter, setClusterFilter] = useState('all');
  const [actionFilter, setActionFilter] = useState('all');
  const [expandedId, setExpandedId] = useState(null);

  // Live simulation states (matches recommend/backend/mock.py and main.py)
  const [liveItems, setLiveItems] = useState([]);
  const [isSimulating, setIsSimulating] = useState(false);
  const [simLog, setSimLog] = useState(null);
  const timerRef = useRef(null);
  const wsRef = useRef(null);

  // Initialize liveItems when base data loads
  useEffect(() => {
    if (data?.items && liveItems.length === 0) {
      setLiveItems(data.items);
    }
  }, [data]);

  // Connect to optional local FastAPI WebSocket if available
  useEffect(() => {
    try {
      const ws = new WebSocket('ws://localhost:8000/ws');
      ws.onmessage = (event) => {
        try {
          const rec = JSON.parse(event.data);
          const timeStr = new Date().toLocaleTimeString('en-GB');
          const newItem = {
            id: `LIVE-${Date.now().toString().slice(-4)}`,
            cluster: 'Active Stream',
            resource: 'cpu',
            action: rec.action.toLowerCase(),
            magnitudePercent: rec.action === 'SCALE_UP' ? 15 : rec.action === 'SCALE_DOWN' ? 10 : 0,
            windowStart: new Date().toISOString(),
            confidence: 0.94,
            reason: `[${rec.node_id}] ${rec.reason} (Alert: ${rec.alert_level})`,
            isLive: true,
          };
          setLiveItems((prev) => [newItem, ...prev]);
          setSimLog(`[${timeStr}] WS BROADCAST: ${rec.node_id} -> [${rec.action}] ${rec.reason}`);
        } catch (e) {
          // ignore parse error
        }
      };
      wsRef.current = ws;
      return () => ws.close();
    } catch (e) {
      // ws unavailable in pure static deployment
    }
  }, []);

  // Single simulation step matching mock.py and main.py
  const simulateStep = () => {
    const nodes = ['Node-01', 'Node-02', 'Node-03', 'Node-14'];
    const node = nodes[Math.floor(Math.random() * nodes.length)];

    // Weighted randomizer from mock.py (30% chance of critical spike >= 70%)
    const cpuUtil = Math.random() < 0.3
      ? 0.70 + Math.random() * 0.28
      : 0.15 + Math.random() * 0.55;
    const memUtil = Math.random() < 0.3
      ? 0.70 + Math.random() * 0.25
      : 0.15 + Math.random() * 0.55;
    const taskArrival = Math.floor(100 + Math.random() * 1400);

    // Scaling rules from recommend/backend/main.py
    let action = 'hold';
    let magnitude = 0;
    let reason = 'Utilization within optimal bounds.';
    let resource = 'cpu';

    if (cpuUtil >= 0.85 || memUtil >= 0.85) {
      action = 'scale_up';
      magnitude = taskArrival > 1000 ? 25 : 15;
      resource = cpuUtil >= memUtil ? 'cpu' : 'memory';
      reason = `Critical utilization predicted. CPU: ${(cpuUtil * 100).toFixed(0)}%, RAM: ${(memUtil * 100).toFixed(0)}%${
        taskArrival > 1000 ? ' + High task arrival rate.' : ''
      }`;
    } else if (cpuUtil <= 0.30 && memUtil <= 0.30) {
      action = 'scale_down';
      magnitude = 10;
      resource = 'cpu';
      reason = 'Resources underutilized. Safely scaling down.';
    }

    const timeStr = new Date().toLocaleTimeString('en-GB');
    const newItem = {
      id: `SIM-${Date.now().toString().slice(-4)}`,
      cluster: Math.random() > 0.5 ? 'Google 2019' : 'Alibaba 2018',
      resource,
      action,
      magnitudePercent: magnitude,
      windowStart: new Date().toISOString(),
      confidence: parseFloat((0.85 + Math.random() * 0.12).toFixed(2)),
      reason: `[${node}] ${reason}`,
      isLive: true,
    };

    setLiveItems((prev) => [newItem, ...prev.slice(0, 24)]);
    setSimLog(
      `[${timeStr}] SENT: ${node} (CPU: ${(cpuUtil * 100).toFixed(0)}%, RAM: ${(memUtil * 100).toFixed(0)}%, Tasks: ${taskArrival}/m) -> RECEIVED: [${action.toUpperCase()}] | ${reason}`
    );
  };

  // Toggle continuous simulation loop (2.5s intervals like mock.py)
  const toggleSimulation = () => {
    if (isSimulating) {
      clearInterval(timerRef.current);
      timerRef.current = null;
      setIsSimulating(false);
    } else {
      setIsSimulating(true);
      simulateStep();
      timerRef.current = setInterval(simulateStep, 2500);
    }
  };

  useEffect(() => {
    return () => {
      if (timerRef.current) clearInterval(timerRef.current);
    };
  }, []);

  const itemsToDisplay = liveItems.length > 0 ? liveItems : (data?.items || []);

  const filteredItems = itemsToDisplay.filter((item) => {
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

          {/* Simulation Control Bar */}
          <div className="simulation-bar">
            <div className="simulation-header">
              <div className="simulation-title-group">
                <span className="simulation-title">Live Recommendation Simulator (mock.py)</span>
                {isSimulating && <span className="pulse-indicator" title="Live stream active" />}
              </div>
              <div className="simulation-actions">
                <button
                  type="button"
                  className={`btn-simulate ${isSimulating ? 'active' : ''}`}
                  onClick={toggleSimulation}
                >
                  {isSimulating ? 'Stop Simulation' : 'Start Simulation'}
                </button>
                <button
                  type="button"
                  className="btn-simulate-step"
                  onClick={simulateStep}
                  title="Simulate a single micro-batch"
                >
                  Simulate Step
                </button>
              </div>
            </div>

            {simLog && (
              <div className="simulation-log" role="log" aria-live="polite">
                {simLog}
              </div>
            )}
          </div>

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
              Showing {filteredItems.length} of {itemsToDisplay.length} actions
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
                          <td>
                            {item.cluster}
                            {item.isLive && <span className="live-badge">Live</span>}
                          </td>
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
