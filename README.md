# Dockyard — Distributed ML Workflow Orchestration Engine

A domain-agnostic backend engine for building multi-stage ML processing pipelines with human-review gates, real-time WebSocket log streaming, and crash recovery — built entirely from scratch in Python.

> Built to process complex engineering drawings across railways, shipbuilding, aerospace, and automotive domains. Powers 10+ production deployments. New domain onboarded in under 30 minutes.

---

## The Problem It Solves

Most ML pipelines are one-off scripts tied to a specific domain. Every new client means rewriting the same orchestration logic: job queuing, worker management, stage chaining, error handling, real-time feedback, crash recovery.

Dockyard solves this once. The engine is written once and never touched again. You plug in a new domain by writing 3 things — a config, a JSON, and your worker functions — and the engine handles everything else.

---

## Architecture

```
Django View  ──TCP signal──▶  Signal Listener (Thread A)
                                      │
                               notify_queue
                                      │
                              Dispatcher (Thread B)
                           ┌──────────┴──────────┐
                           │                     │
                      Queue Registry         Worker Pool
                  (per-stage queues)    (ProcessPoolExecutor)
                           │                     │
                    Tasks sit here    Workers execute here
                           └──────────┬──────────┘
                                      │
                              WebSocket Broadcaster
                                      │
                              Browser (real-time logs)
```

### Thread Model

| Thread | Role |
|--------|------|
| **Thread A — Signal Listener** | TCP server on `127.0.0.1:9000`. Django sends `"revision_id:stage_name"` when a job is submitted. Puts it on `notify_queue`. |
| **Thread B — Dispatcher** | Drains `notify_queue`, creates `Task` objects, submits to worker pool, chains stages on completion, handles human-review pauses. |
| **Worker Processes** | `ProcessPoolExecutor` — true parallelism, bypasses GIL. Each worker runs your executor function and returns a `TaskResult`. |
| **WebSocket Server** | Broadcasts real-time log lines to the browser as each stage runs. GhostRoom buffers replayed to late-connecting clients. |

---

## Key Features

- **Domain-agnostic** — engine files are never modified when onboarding a new domain
- **Custom worker pool** — `ProcessPoolExecutor` with configurable concurrency per queue (not Celery, not Redis, no external dependencies)
- **Multi-stage pipelines** — stages chain automatically; each stage's output is passed to the next
- **Human-review gates** — pipeline pauses between stages and waits for user confirmation before continuing
- **Real-time WebSocket streaming** — every log line from every worker is streamed to the browser live during processing
- **TCP signal dispatch** — Django sends a lightweight TCP signal to trigger the engine; no polling, no message broker
- **Crash recovery** — on startup, `recovery.py` queries the DB for stuck `processing` revisions and re-enqueues them from their last incomplete stage
- **GhostRoom** — in-memory log buffer that replays full stage history to clients who connect after processing has already started

---

## Onboarding a New Domain (6 Steps, ~30 Minutes)

The engine never changes. For every new domain you only write:

### Step 1 — `pipeline_config.py`

```python
from my_domain.workers.step_one import step_one
from my_domain.workers.step_two import step_two

QUEUES = ['STEP_ONE_QUEUE', 'STEP_TWO_QUEUE']

PIPELINE = {
    'STEP_ONE': (step_one, 'STEP_ONE_QUEUE'),
    'STEP_TWO': (step_two, 'STEP_TWO_QUEUE'),
}

ENTRY_STAGE = 'STEP_ONE'

WORKER_CONCURRENCY = {
    'STEP_ONE_QUEUE': 2,
    'STEP_TWO_QUEUE': 1,
}
```

### Step 2 — `workflow_stages.json`

Stage names must match `PIPELINE` keys exactly.

```json
{
  "stages": [
    {
      "name": "STEP_ONE",
      "display_name": "Step One",
      "description": "What this stage does.",
      "requires_human_review": false
    },
    {
      "name": "STEP_TWO",
      "display_name": "Step Two",
      "description": "What this stage does.",
      "requires_human_review": true,
      "review_prompt": "Please verify output before continuing."
    }
  ]
}
```

### Step 3 — Worker files

Every worker exposes one function with this exact signature:

```python
from engine.tasks.base_task import TaskResult

def run(task, update_queue) -> TaskResult:
    comments = []

    def log(msg):
        comments.append(msg)
        update_queue.put({'type': 'log', 'message': msg, 'revision_id': task.revision_id})

    log("Starting stage...")

    # --- your actual logic here ---

    log("Done.")

    return TaskResult(
        status='completed',       # 'completed' | 'failed'
        comments=comments,        # all log lines
        file_paths=['/path/to/output.json'],
        time_taken=1.23,
        error=None,               # set this string if status='failed'
    )
```

### Steps 4-6 — Django wiring

4. In your upload view, send a TCP signal after saving the revision:
   ```python
   import socket
   with socket.create_connection(('127.0.0.1', 9000)) as s:
       s.sendall(f"{revision_id}:STEP_ONE".encode())
   ```

5. Add review API endpoints that call `db_sync.set_revision_awaiting_review()` / `complete_revision()` as appropriate

6. Ensure every new Python package folder has an `__init__.py`

**That's it. The engine picks it up automatically.**

---

## Project Structure

```
engine/
├── dispatcher.py          # Pipeline orchestrator — chains stages, handles results
├── signal_listener.py     # Thread A — TCP receiver, puts signals on notify_queue
├── worker_pool.py         # ProcessPoolExecutor wrapper — true multiprocessing
├── recovery.py            # Startup crash recovery — re-enqueues stuck revisions
├── db/
│   ├── db_sync.py         # All synchronous DB operations (Django ORM)
│   └── __init__.py
├── queues/
│   ├── base_queue.py      # Thread-safe queue with put/get/empty
│   ├── registry.py        # QueueRegistry singleton — maps names to queues
│   └── __init__.py
├── tasks/
│   ├── base_task.py       # Task + TaskResult dataclasses
│   └── __init__.py
├── ws/
│   ├── ws_server.py       # WebSocket server — handles connections + auth
│   ├── ws_broadcaster.py  # Broadcaster + GhostRoom buffer
│   ├── ws_auth.py         # Cookie-based session auth for WebSocket connections
│   └── __init__.py
└── example_domain/        # Full working demo — Floor Plan Analyser
    ├── pipeline_config.py
    ├── workflow_stages.json
    └── workers/
        ├── detect_rooms.py
        ├── measure_areas.py
        └── generate_bom.py
```

---

## Data Flow — What Happens When a Job Is Submitted

```
1. Django saves the revision to DB
2. Django sends TCP signal → "42:ROOM_DETECTION"
3. Signal Listener receives it → puts (42, "ROOM_DETECTION") on notify_queue
4. Dispatcher picks it up → creates Task → puts in ROOM_DETECTION_QUEUE
5. Worker pool picks Task → runs detect_rooms.run(task, update_queue)
6. Worker streams log lines via update_queue → WebSocket → browser (live)
7. Worker returns TaskResult(status='completed', ...)
8. Dispatcher chains to next stage → AREA_MEASUREMENT_QUEUE
9. If pause_after=True → pipeline pauses, sends 'awaiting_review' WS event
10. User reviews on screen → clicks confirm → Django sends next TCP signal
11. Dispatcher resumes from AREA_MEASUREMENT → BOM_GENERATION → done
```

---

## Crash Recovery

If the engine process dies mid-pipeline (server restart, OOM, etc):

```
Startup → recovery.py queries DB for status='processing' revisions
        → finds revision 42 stuck at AREA_MEASUREMENT
        → re-enqueues Task at AREA_MEASUREMENT
        → Dispatcher picks it up and continues from where it left off
```

No jobs are lost across restarts.

---

## Example Domain — Floor Plan Analyser

The `example_domain/` folder contains a fully working demo domain:

| Stage | What it does |
|-------|-------------|
| `ROOM_DETECTION` | Detects and classifies rooms from a floor plan drawing |
| `AREA_MEASUREMENT` | Computes area of each room — pauses for human review |
| `BOM_GENERATION` | Generates structured BOM JSON from verified measurements |

Run it to see the full pipeline in action including the human-review gate.

---

## Production Deployments

Dockyard powers production processing pipelines across:

- **Railways** — automated Bill of Materials extraction from engineering drawings at 3km scale (95% reduction in processing time)
- **Shipbuilding** — cable routing across lakhs of cables using graph-based spatial reasoning
- **Aerospace** — BOM generation from scanned Airbus/Boeing aircraft drawings
- **Automotive** — wiring harness BOM from complex schematic drawings

---

## Tech Stack

- Python 3.11+
- Django (DB layer + views)
- `concurrent.futures.ProcessPoolExecutor` (worker pool)
- `websockets` (WebSocket server)
- TCP sockets (signal dispatch)
- No Celery. No Redis. No external message broker.

---

## License

See [LICENSE](LICENSE) file.
