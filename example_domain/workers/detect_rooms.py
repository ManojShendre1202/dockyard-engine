# ============================================================
#  detect_rooms.py — Stage 1: Room Detection
#
#  Every worker file must expose a single function:
#      run(task, update_queue) -> TaskResult
#
#  - task         : Task dataclass (see engine/tasks/base_task.py)
#  - update_queue : multiprocessing.Queue to push log messages
#                   (engine broadcasts these via WebSocket in real time)
# ============================================================

import time
from engine.tasks.base_task import TaskResult


def run(task, update_queue):
    """
    Detect rooms from a floor plan drawing.
    In a real domain: load DXF/PDF, run OpenCV contour detection, return results.
    """
    comments = []

    def log(msg):
        comments.append(msg)
        update_queue.put({'type': 'log', 'message': msg, 'revision_id': task.revision_id})

    log(f"[Room Detection] Starting for revision {task.revision_id}")
    log(f"[Room Detection] Input file: {task.payload.get('file_path', 'N/A')}")

    # --- Your actual CV logic goes here ---
    # Example: cv2.imread() → grayscale → threshold → findContours → classify rooms
    time.sleep(0.1)  # placeholder for actual processing

    log("[Room Detection] Detected 5 rooms: bedroom x2, living room, kitchen, bathroom")
    log("[Room Detection] Completed successfully")

    return TaskResult(
        status='completed',
        comments=comments,
        file_paths=[task.payload.get('output_path', '') + '/rooms.json'],
        time_taken=0.1,
    )
