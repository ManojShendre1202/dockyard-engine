# ============================================================
#  pipeline_config.py — Example Domain: Floor Plan Analyser
#  This is a DEMO domain showing how to onboard any new domain
#  into the Dockyard Workflow Engine in under 30 minutes.
#
#  Replace this file with your own domain config.
#  Engine files (engine/) are NEVER touched.
# ============================================================

from example_domain.workers.detect_rooms   import detect_rooms
from example_domain.workers.measure_areas  import measure_areas
from example_domain.workers.generate_bom   import generate_bom

# ---- Step 1: Define queues (one per pipeline stage) ----
QUEUES = [
    'ROOM_DETECTION_QUEUE',
    'AREA_MEASUREMENT_QUEUE',
    'BOM_GENERATION_QUEUE',
]

# ---- Step 2: Define the pipeline (stage_name -> executor function) ----
#  Each executor must have signature: run(task, update_queue) -> TaskResult
PIPELINE = {
    'ROOM_DETECTION':   (detect_rooms,   'ROOM_DETECTION_QUEUE'),
    'AREA_MEASUREMENT': (measure_areas,  'AREA_MEASUREMENT_QUEUE'),
    'BOM_GENERATION':   (generate_bom,   'BOM_GENERATION_QUEUE'),
}

# ---- Step 3: Set the entry stage (first stage in the pipeline) ----
ENTRY_STAGE = 'ROOM_DETECTION'

# ---- Step 4: Set concurrency per queue ----
#  How many workers run in parallel for each queue.
WORKER_CONCURRENCY = {
    'ROOM_DETECTION_QUEUE':   2,
    'AREA_MEASUREMENT_QUEUE': 2,
    'BOM_GENERATION_QUEUE':   1,
}
