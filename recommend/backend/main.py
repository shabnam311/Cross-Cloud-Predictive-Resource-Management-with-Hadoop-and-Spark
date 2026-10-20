"""Resource Recommendation Engine (v2 - RMSE Per-Cloud Thresholds).

Run:  uvicorn main:app --host 0.0.0.0 --port 8000
(0.0.0.0 is required so the Spark container can reach it at host.docker.internal:8000)
"""
from collections import deque
from statistics import mean
from typing import Deque, Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="Resource Recommendation Engine")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

# ---- Policy (per-cloud RMSE thresholds) ----
# Critical = 1.0 - 2 * RMSE | Warning = 1.0 - 3 * RMSE
MODEL_RMSE: Dict[str, Tuple[float, float]] = {
    "google": (0.083, 0.048),   # Crit: CPU 83.4%, Mem 90.4% | Warn: CPU 75.1%, Mem 85.6%
    "alibaba": (0.056, 0.017),  # Crit: CPU 88.8%, Mem 96.6% | Warn: CPU 83.2%, Mem 94.9%
}
DEFAULT_RMSE = (0.075, 0.075)   # Fallback -> Crit: 85.0%, Warn: 77.5%

LOW = 0.30           # both CPU and memory must be below this to scale down
UP_STREAK = 2        # consecutive windows at/above CRITICAL before SCALE_UP
DOWN_STREAK = 6      # consecutive low windows (30 min) before SCALE_DOWN
COOLDOWN = 3         # windows to wait after any scaling action
SPIKE_FACTOR = 1.5   # arrivals vs. recent average that counts as a spike
UP_PCT, DOWN_PCT = 25, 10     # change as % of current capacity
HISTORY = 288        # windows kept per machine (24 h)


def get_thresholds(cloud: str) -> Tuple[float, float, float, float]:
    rmse_cpu, rmse_mem = MODEL_RMSE.get(cloud.lower(), DEFAULT_RMSE)
    crit_cpu = round(1.0 - 2.0 * rmse_cpu, 4)
    crit_mem = round(1.0 - 2.0 * rmse_mem, 4)
    warn_cpu = round(1.0 - 3.0 * rmse_cpu, 4)
    warn_mem = round(1.0 - 3.0 * rmse_mem, 4)
    return crit_cpu, crit_mem, warn_cpu, warn_mem


# ---- Schemas ----
class MachinePrediction(BaseModel):
    node_id: str
    pred_cpu_util: float            # prediction for `timestamp` (next window)
    pred_mem_util: float
    cur_cpu_util: Optional[float] = None   # actual value in the window that just arrived
    cur_mem_util: Optional[float] = None


class PredictionBatch(BaseModel):
    cloud: str
    timestamp: int                          # window_ts being predicted
    observed_timestamp: Optional[int] = None    # window_ts whose actuals are in cur_*
    predictions: List[MachinePrediction]
    task_arrival_rate: Optional[float] = None   # cluster level, optional


# ---- State ----
class NodeState:
    def __init__(self) -> None:
        self.hi = 0
        self.lo = 0
        self.cooldown = 0
        self.last_action: Optional[str] = None
        self.last_action_ts: Optional[int] = None
        self.history: Deque[dict] = deque(maxlen=HISTORY)


states: Dict[str, NodeState] = {}
latest: Dict[str, dict] = {}
arrivals: Deque[float] = deque(maxlen=12)


class ConnectionManager:
    def __init__(self) -> None:
        self.active: List[WebSocket] = []

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket) -> None:
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, message: dict) -> None:
        for ws in list(self.active):
            try:
                await ws.send_json(message)
            except Exception:
                self.disconnect(ws)


manager = ConnectionManager()


def clamp(x: float) -> float:
    return min(max(x, 0.0), 1.0)


def decide(node_id: str, cloud: str, cpu: float, mem: float, spike: bool, st: NodeState) -> dict:
    cpu, mem = clamp(cpu), clamp(mem)
    crit_cpu, crit_mem, warn_cpu, warn_mem = get_thresholds(cloud)

    cpu_is_crit = cpu >= crit_cpu
    mem_is_crit = mem >= crit_mem
    is_crit = cpu_is_crit or mem_is_crit
    is_warn = (cpu >= warn_cpu) or (mem >= warn_mem)

    st.hi = st.hi + 1 if is_crit else 0
    st.lo = st.lo + 1 if (cpu <= LOW and mem <= LOW) else 0

    action, alert, cpu_pct, mem_pct = "MAINTAIN", "NORMAL", 0, 0
    reason = "Utilization within bounds."

    if is_crit:
        alert = "CRITICAL"
        reason = (
            f"Predicted CPU {cpu:.1%} (crit >= {crit_cpu:.1%}), "
            f"memory {mem:.1%} (crit >= {crit_mem:.1%})."
        )
    elif is_warn:
        alert = "WARNING"
        reason = (
            f"Predicted CPU {cpu:.1%} (warn >= {warn_cpu:.1%}), "
            f"memory {mem:.1%} (warn >= {warn_mem:.1%}) approaching limit."
        )

    if st.cooldown > 0:
        st.cooldown -= 1
        if is_crit:
            reason += " Cooling down after a recent scaling action."
    elif st.hi >= UP_STREAK:
        action = "SCALE_UP"
        factor = 2 if spike else 1
        cpu_pct = UP_PCT * factor if cpu_is_crit else 0
        mem_pct = UP_PCT * factor if mem_is_crit else 0
        reason += f" Sustained for {st.hi} windows."
        if spike:
            reason += " Task arrivals spiking."
        st.cooldown, st.hi = COOLDOWN, 0
    elif st.lo >= DOWN_STREAK:
        action = "SCALE_DOWN"
        cpu_pct = mem_pct = -DOWN_PCT
        reason = f"CPU and memory <= {LOW:.0%} for {st.lo} windows."
        st.cooldown, st.lo = COOLDOWN, 0

    return {
        "node_id": node_id,
        "action": action,
        "cpu_change_pct": cpu_pct,
        "mem_change_pct": mem_pct,
        "alert_level": alert,
        "reason": reason,
        "pred_cpu_util": round(cpu, 4),
        "pred_mem_util": round(mem, 4),
    }


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "machines": len(latest), "ws_clients": len(manager.active)}


@app.post("/api/recommend/batch")
async def recommend_batch(batch: PredictionBatch) -> dict:
    spike = False
    if batch.task_arrival_rate is not None:
        if len(arrivals) >= 3:
            spike = batch.task_arrival_rate > SPIKE_FACTOR * mean(arrivals)
        arrivals.append(batch.task_arrival_rate)

    recs = []
    for p in batch.predictions:
        st = states.setdefault(p.node_id, NodeState())

        # 1. close the loop: the previous prediction was for the window that just arrived
        prev = st.history[-1] if st.history else None
        if (
            prev is not None
            and batch.observed_timestamp is not None
            and prev["timestamp"] == batch.observed_timestamp
            and p.cur_cpu_util is not None
            and p.cur_mem_util is not None
        ):
            prev["actual_cpu_util"] = round(p.cur_cpu_util, 4)
            prev["actual_mem_util"] = round(p.cur_mem_util, 4)
            prev["error_cpu"] = round(prev["pred_cpu_util"] - p.cur_cpu_util, 4)
            prev["error_mem"] = round(prev["pred_mem_util"] - p.cur_mem_util, 4)

        # 2. new recommendation for the next window
        rec = decide(p.node_id, batch.cloud, p.pred_cpu_util, p.pred_mem_util, spike, st)
        if rec["action"] != "MAINTAIN":
            st.last_action, st.last_action_ts = rec["action"], batch.timestamp
        rec.update(
            cloud=batch.cloud,
            timestamp=batch.timestamp,
            cur_cpu_util=None if p.cur_cpu_util is None else round(p.cur_cpu_util, 4),
            cur_mem_util=None if p.cur_mem_util is None else round(p.cur_mem_util, 4),
            last_action=st.last_action,
            last_action_timestamp=st.last_action_ts,
            previous=None
            if prev is None
            else {
                k: prev.get(k)
                for k in (
                    "timestamp", "pred_cpu_util", "actual_cpu_util", "error_cpu",
                    "pred_mem_util", "actual_mem_util", "error_mem",
                )
            },
        )
        st.history.append(rec)
        latest[p.node_id] = rec
        recs.append(rec)

    summary = {
        "cloud": batch.cloud,
        "timestamp": batch.timestamp,
        "total": len(recs),
        "critical": sum(r["alert_level"] == "CRITICAL" for r in recs),
        "warning": sum(r["alert_level"] == "WARNING" for r in recs),
        "scale_up": sum(r["action"] == "SCALE_UP" for r in recs),
        "scale_down": sum(r["action"] == "SCALE_DOWN" for r in recs),
        "arrival_spike": spike,
    }
    await manager.broadcast(
        {
            "type": "window",
            "summary": summary,
            "alerts": [r for r in recs if r["alert_level"] != "NORMAL" or r["action"] != "MAINTAIN"],
        }
    )
    return summary


@app.get("/api/latest")
async def get_latest(cloud: Optional[str] = None) -> List[dict]:
    rows = list(latest.values())
    return [r for r in rows if cloud is None or r["cloud"] == cloud]


@app.get("/api/machine/{node_id}")
async def get_machine(node_id: str) -> dict:
    st = states.get(node_id)
    if st is None:
        raise HTTPException(404, "unknown node")
    return {"node_id": node_id, "latest": latest[node_id], "history": list(st.history)}


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    await manager.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws)