# ============================================================
#  measure_areas.py — Stage 2: Area Measurement
#  Pauses for human review before running (see workflow_stages.json)
# ============================================================

import time
from engine.tasks.base_task import TaskResult


def run(task, update_queue):
    """
    Compute area of each detected room using geometric computation.
    In a real domain: load rooms.json from stage 1, use Shapely polygons, calculate sq ft.
    """
    comments = []

    def log(msg):
        comments.append(msg)
        update_queue.put({'type': 'log', 'message': msg, 'revision_id': task.revision_id})

    log(f"[Area Measurement] Starting for revision {task.revision_id}")
    log("[Area Measurement] Loading room boundaries from stage 1 output...")

    # --- Your actual geometry logic goes here ---
    # Example: shapely.geometry.Polygon(coords).area * scale_factor
    time.sleep(0.1)

    log("[Area Measurement] bedroom_1: 180 sq ft")
    log("[Area Measurement] bedroom_2: 160 sq ft")
    log("[Area Measurement] living_room: 320 sq ft")
    log("[Area Measurement] kitchen: 120 sq ft")
    log("[Area Measurement] bathroom: 60 sq ft")
    log("[Area Measurement] Total: 840 sq ft")
    log("[Area Measurement] Completed successfully")

    return TaskResult(
        status='completed',
        comments=comments,
        file_paths=[task.payload.get('output_path', '') + '/areas.json'],
        time_taken=0.1,
    )
