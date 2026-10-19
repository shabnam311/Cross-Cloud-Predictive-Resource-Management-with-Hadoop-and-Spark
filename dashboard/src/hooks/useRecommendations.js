import { useEffect, useState } from "react";

// Backend address. CORS is already enabled in main.py.
const HTTP = "http://localhost:8000";
const WS = "ws://localhost:8000/ws";
const STREAM_TIMEOUT_MS = 15000; // 15 seconds without incoming batch -> mark offline

/**
 * Live recommendations from the FastAPI service.
 *  machines  : newest record per machine (same shape as GET /api/latest)
 *  summary   : last per-window summary {cloud, timestamp, total, critical, warning, scale_up, ...}
 *  alerts    : rolling feed (newest first) of machines that were not NORMAL/MAINTAIN
 *  connected : true ONLY when WS is connected AND data arrived within the last 15s
 * cloud: "google" | "alibaba" | undefined (all)
 */
export function useRecommendations(cloud) {
  const [machines, setMachines] = useState([]);
  const [summary, setSummary] = useState(null);
  const [alerts, setAlerts] = useState([]);
  const [connected, setConnected] = useState(false);

  useEffect(() => {
    let ws;
    let retry;
    let inactivityTimer;
    let stopped = false;

    const resetInactivityTimer = () => {
      clearTimeout(inactivityTimer);
      setConnected(true);
      inactivityTimer = setTimeout(() => {
        // No window batch received for 15 seconds -> mark stream offline
        setConnected(false);
      }, STREAM_TIMEOUT_MS);
    };

    const load = async () => {
      try {
        const q = cloud ? `?cloud=${cloud}` : "";
        const res = await fetch(`${HTTP}/api/latest${q}`);
        setMachines(await res.json());
      } catch (e) {
        /* API not up yet: next window or reconnect retries */
      }
    };

    const open = () => {
      ws = new WebSocket(WS);
      ws.onopen = () => {
        load();
        resetInactivityTimer();
      };
      ws.onmessage = (e) => {
        const msg = JSON.parse(e.data);
        if (msg.type !== "window") return;
        
        // Data arrived from Spark! Reset the 15-second offline countdown
        resetInactivityTimer();

        if (cloud && msg.summary.cloud !== cloud) return;
        setSummary(msg.summary);
        setAlerts((old) => [...msg.alerts, ...old].slice(0, 200));
        load(); 
      };
      ws.onclose = () => {
        clearTimeout(inactivityTimer);
        setConnected(false);
        if (!stopped) retry = setTimeout(open, 2000);
      };
    };

    load();
    open();
    return () => {
      stopped = true;
      clearTimeout(retry);
      clearTimeout(inactivityTimer);
      if (ws) ws.close();
    };
  }, [cloud]);

  return { machines, summary, alerts, connected };
}