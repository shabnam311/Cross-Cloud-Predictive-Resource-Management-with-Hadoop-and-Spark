from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List

app = FastAPI(title="Resource Recommendation Engine")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            await connection.send_json(message)

manager = ConnectionManager()

# 1. Define the incoming prediction schema (from Spark)
class MLPrediction(BaseModel):
    node_id: str
    timestamp: int
    pred_cpu_util: float  # e.g., 0.88 for 88%
    pred_mem_util: float  # e.g., 0.85 for 85%
    task_arrival_rate: int # tasks per minute
    
# 2. Define the outgoing recommendation schema (to React/K8s)
class ScalingRecommendation(BaseModel):
    node_id: str
    action: str            # "SCALE_UP", "SCALE_DOWN", "MAINTAIN"
    cpu_cores_change: int
    mem_gb_change: int
    alert_level: str       # "NORMAL", "WARNING", "CRITICAL"
    reason: str

# 3. Define your scaling thresholds
THRESHOLD_CRITICAL_CPU = 0.85
THRESHOLD_CRITICAL_MEM = 0.85
THRESHOLD_LOW_CPU = 0.30

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # Keeps connection open
            await websocket.receive_text() 
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.post("/api/recommend", response_model=ScalingRecommendation)
async def generate_recommendation(data: MLPrediction):
    
    # Default state
    action = "MAINTAIN"
    cpu_change = 0
    mem_change = 0
    alert = "NORMAL"
    reason = "Utilization within optimal bounds."

    # Rule 1: Impending Bottleneck (Scale Up)
    if data.pred_cpu_util >= THRESHOLD_CRITICAL_CPU or data.pred_mem_util >= THRESHOLD_CRITICAL_MEM:
        action = "SCALE_UP"
        alert = "CRITICAL"
        cpu_change = 2  # Add 2 cores
        mem_change = 4  # Add 4 GB RAM
        reason = f"Critical utilization predicted. CPU: {data.pred_cpu_util*100}%, RAM: {data.pred_mem_util*100}%"
        
        # Aggressive scaling if task arrival is also spiking
        if data.task_arrival_rate > 1000:
            cpu_change = 4
            mem_change = 8
            reason += " + High task arrival rate."

    # Rule 2: Underutilization (Scale Down to save costs)
    elif data.pred_cpu_util <= THRESHOLD_LOW_CPU and data.pred_mem_util <= THRESHOLD_LOW_CPU:
        action = "SCALE_DOWN"
        alert = "NORMAL"
        cpu_change = -1 # Remove 1 core
        mem_change = -2 # Remove 2 GB RAM
        reason = "Resources underutilized. Safely scaling down."

    recommendation = {
        "node_id": data.node_id,
        "action": action,
        "cpu_cores_change": cpu_change,
        "mem_gb_change": mem_change,
        "alert_level": alert,
        "reason": reason
    }
    
    # NEW: Broadcast the decision to React instantly
    await manager.broadcast(recommendation)
    
    return recommendation

import random
import time

NODES = ["Node-01", "Node-02", "Node-03", "Node-14"]

def generate_mock_prediction():
    """Generates a random ML prediction payload matching mock.py."""
    cpu_util = random.choices([random.uniform(0.1, 0.7), random.uniform(0.7, 0.99)], weights=[70, 30])[0]
    mem_util = random.choices([random.uniform(0.1, 0.7), random.uniform(0.7, 0.95)], weights=[70, 30])[0]
    return MLPrediction(
        node_id=random.choice(NODES),
        timestamp=int(time.time()),
        pred_cpu_util=round(cpu_util, 2),
        pred_mem_util=round(mem_util, 2),
        task_arrival_rate=random.randint(100, 1500)
    )

@app.post("/api/simulate", response_model=ScalingRecommendation)
async def simulate_recommendation_step():
    """Triggers one mock prediction matching mock.py and broadcasts recommendation."""
    prediction = generate_mock_prediction()
    return await generate_recommendation(prediction)