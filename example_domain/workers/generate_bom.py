# ============================================================
#  generate_bom.py — Stage 3: BOM Generation
# ============================================================

import time
import json
from engine.tasks.base_task import TaskResult


def run(task, update_queue):
    """
    Generate structured Bill of Materials from verified room measurements.
    In a real domain: load areas.json, apply domain rules, output final BOM.
    """
    comments = []

    def log(msg):
        comments.append(msg)
        update_queue.put({'type': 'log', 'message': msg, 'revision_id': task.revision_id})

    log(f"[BOM Generation] Starting for revision {task.revision_id}")
    log("[BOM Generation] Generating structured BOM from verified measurements...")

    # --- Your actual BOM logic goes here ---
    bom = {
        "revision_id": task.revision_id,
        "total_area_sqft": 840,
        "rooms": [
            {"name": "bedroom_1",   "area_sqft": 180, "type": "bedroom"},
            {"name": "bedroom_2",   "area_sqft": 160, "type": "bedroom"},
            {"name": "living_room", "area_sqft": 320, "type": "living"},
            {"name": "kitchen",     "area_sqft": 120, "type": "kitchen"},
            {"name": "bathroom",    "area_sqft":  60, "type": "bathroom"},
        ]
    }

    time.sleep(0.1)
    log(f"[BOM Generation] Output: {json.dumps(bom, indent=2)}")
    log("[BOM Generation] Completed successfully")

    return TaskResult(
        status='completed',
        comments=comments,
        file_paths=[task.payload.get('output_path', '') + '/bom.json'],
        time_taken=0.1,
    )
