import time
import random
import requests
from datetime import datetime

# Point this to your active FastAPI Resource Engine endpoint
URL = "http://localhost:8000/api/recommend"

# Simulating a subset of worker nodes from the cloud traces
NODES = ["Node-01", "Node-02", "Node-03", "Node-14"]

def generate_mock_prediction():
    """Generates a random ML prediction payload matching the API contract."""
    # Weight the randomizer to occasionally spike above 85% to trigger SCALE_UP
    cpu_util = random.choices([random.uniform(0.1, 0.7), random.uniform(0.7, 0.99)], weights=[70, 30])[0]
    mem_util = random.choices([random.uniform(0.1, 0.7), random.uniform(0.7, 0.95)], weights=[70, 30])[0]
    
    return {
        "node_id": random.choice(NODES),
        "timestamp": int(time.time()),
        "pred_cpu_util": round(cpu_util, 2),
        "pred_mem_util": round(mem_util, 2),
        "task_arrival_rate": random.randint(100, 1500)
    }

print(f"Starting mock Spark stream to {URL}...")
print("-" * 60)

try:
    while True:
        payload = generate_mock_prediction()
        try:
            # Send the simulated Spark output to the Recommendation Engine
            response = requests.post(URL, json=payload)
            result = response.json()
            
            # Print the transaction to the terminal
            timestamp_str = datetime.now().strftime('%H:%M:%S')
            print(f"[{timestamp_str}] SENT: {payload['node_id']} (CPU: {payload['pred_cpu_util']*100}%, RAM: {payload['pred_mem_util']*100}%)")
            print(f"         RECEIVED: [{result['action']}] | {result['reason']}\n")
            
        except requests.exceptions.ConnectionError:
            print("Connection failed. Is the FastAPI server running on port 8000?")
            break
            
        # Wait 2 seconds before the next micro-batch
        time.sleep(2)
        
except KeyboardInterrupt:
    print("\nMock stream stopped.")