from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Task:
    """
    A single unit of work handed to the worker pool.

    Created by the Dispatcher when a revision enters the pipeline.
    One Task is created per stage per revision.
    """
    revision_id: int        # FK to RevisionWorkflow.id
    stage_name:  str        # must match a key in PIPELINE and in workflow_stages.json
    queue_name:  str        # which queue this task was pulled from
    payload:     dict       # file_path, output_path, and any other data the executor needs

    task_id:    str      = field(default_factory=lambda: str(uuid.uuid4()))
    status:     str      = 'pending'       # pending | processing | completed | failed
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class TaskResult:
    """
    Returned by every executor function when it finishes (success or failure).

    Dispatcher reads this to update the local store, flush to DB,
    push the WebSocket update, and decide whether to chain the next stage.
    """
    status:     str               # 'completed' | 'failed'
    comments:   list[str]         # all log lines accumulated during execution
    file_paths: list[str]          # output files produced by this stage
    time_taken: float             # wall-clock seconds the executor ran for
    error:      str | None = None # human-readable error message when status='failed'
