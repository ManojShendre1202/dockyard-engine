"""
dispatcher.py — Thread B: the pipeline orchestrator.

Receives (revision_id, stage_name) signals from notify_queue (TCP listener).
Creates Tasks, submits them to the worker pool, and chains stages in order
until the pipeline is complete.

The stage_name is explicit in the TCP signal — the Dispatcher never guesses
which stage to run next from DB state. DB state is only read to fetch the
section_data payload needed by the executor.

The Dispatcher is injected with the PIPELINE dict at startup and has zero
knowledge of the actual business logic — it only knows stage names, queue
names, and which stage follows which.
"""

import logging
import queue
import threading

from workflow.engine.db import db_sync
from workflow.engine.queues.registry import registry
from workflow.engine.signal_listener import notify_queue
from workflow.engine.tasks.base_task import Task, TaskResult
from workflow.engine.worker_pool import pool
from workflow.engine.ws.ws_broadcaster import broadcaster

logger = logging.getLogger(__name__)


class Dispatcher:

    def __init__(self, pipeline: dict, entry_stage: str, queue_concurrency: dict[str, int] | None = None, update_queue=None) -> None:
        self._pipeline          = pipeline
        self._entry_stage       = entry_stage
        self._queue_concurrency = queue_concurrency or {}
        self._update_queue      = update_queue
        # Track revision IDs currently in-flight to prevent duplicate enqueues
        self._active:    set[int]       = set()
        self._in_flight: dict[str, int] = {}   # queue_name → running count
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public — called from main.py
    # ------------------------------------------------------------------

    def start(self) -> None:
        """
        Run the dispatcher loop in the calling thread (Thread B).

        Tight loop: drain queues every 0.5 s so stage-chained tasks are
        picked up immediately after a worker finishes.
        """
        logger.info('Dispatcher started')

        while True:
            # 1. Submit anything already sitting in a queue
            #    (recovery tasks on startup, stage-chained tasks mid-pipeline)
            self._process_queued_tasks()

            # 2. Check for a new signal — short timeout keeps loop responsive
            #    Each signal is a (revision_id, stage_name) tuple sent by Django
            #    with the explicit stage to run next — no DB detection needed
            try:
                revision_id, stage_name = notify_queue.get(timeout=0.5)
                self._enqueue_revision(revision_id, stage_name)
            except queue.Empty:
                pass

    def _process_queued_tasks(self) -> None:
        """
        Submit waiting Tasks to the worker pool, respecting per-queue concurrency cap.
        Tasks beyond the cap stay in BaseQueue (status remains 'queued').
        """
        for name, q in registry.all().items():
            cap = self._queue_concurrency.get(name, 1)
            while not q.empty():
                with self._lock:
                    if self._in_flight.get(name, 0) >= cap:
                        break  # at capacity — leave remaining tasks in queue
                    self._in_flight[name] = self._in_flight.get(name, 0) + 1
                try:
                    task = q.get(block=False)
                except queue.Empty:
                    with self._lock:
                        self._in_flight[name] -= 1
                    break
                self._submit(task)

    def _enqueue_revision(self, revision_id: int, stage_name: str) -> None:
        """
        Create a Task for a revision and put it in the correct stage queue.

        stage_name comes directly from the TCP signal — Django tells us exactly
        which stage to run next. No DB stage status detection needed here at all.

        DB is only read to fetch section_data payload for the executor.
        """
        with self._lock:
            if revision_id in self._active:
                logger.debug('Revision %d already active — skipping duplicate signal', revision_id)
                return
            self._active.add(revision_id)

        # Validate stage_name is known to this pipeline
        if stage_name not in self._pipeline:
            logger.error(
                'Dispatcher: unknown stage "%s" for revision %d — ignoring signal',
                stage_name, revision_id,
            )
            with self._lock:
                self._active.discard(revision_id)
            return

        # Fetch section_data from DB — needed by executor functions
        section_data = {}
        try:
            section_data = db_sync.get_section_data(revision_id)
        except Exception as exc:
            logger.error('Dispatcher: failed to fetch payload for revision %d — %s', revision_id, exc)

        queue_name = self._pipeline[stage_name]['queue']

        task = Task(
            revision_id = revision_id,
            stage_name  = stage_name,
            queue_name  = queue_name,
            payload     = {'section_data': section_data},
        )

        registry.get(queue_name).put(task)

        # Mark stage as honestly queued — worker hasn't started yet
        try:
            db_sync.set_stage_status(revision_id, stage_name, 'queued')
        except Exception as exc:
            logger.error('Dispatcher: failed to set queued status for revision %d stage "%s" — %s', revision_id, stage_name, exc)

        broadcaster.send(revision_id, {
            'type':  'stage_update',
            'stage': stage_name,
            'status': 'queued',
        })
        broadcaster.send(revision_id, {
            'type':            'revision_status',
            'revision_status': 'queued',
        })

        # For fresh revisions (entry stage) also update revision-level status
        if stage_name == self._entry_stage:
            try:
                db_sync.set_revision_queued(revision_id)
            except Exception as exc:
                logger.error('Dispatcher: failed to set revision queued for revision %d — %s', revision_id, exc)

        logger.info('Dispatcher: enqueued revision %d at stage "%s"', revision_id, stage_name)

    def _submit(self, task: Task) -> None:
        """Submit a Task to the worker pool and attach the done callback."""
        # Mark active here — covers both signal-triggered and recovery-enqueued tasks
        with self._lock:
            self._active.add(task.revision_id)

        stage_cfg  = self._pipeline[task.stage_name]
        stage_fn   = stage_cfg['fn']

        if stage_fn is None:
            # Human-review passthrough stage — mark it completed and chain to next
            logger.info(
                'Dispatcher: stage "%s" has no executor (fn=None) for revision %d — marking completed and chaining',
                task.stage_name, task.revision_id,
            )
            with self._lock:
                self._in_flight[task.queue_name] = max(0, self._in_flight.get(task.queue_name, 1) - 1)

            # Mark this stage as completed in DB + WS — human already confirmed it
            try:
                db_sync.set_stage_status(task.revision_id, task.stage_name, 'completed')
            except Exception as exc:
                logger.error('Dispatcher: failed to set completed for revision %d stage "%s" — %s', task.revision_id, task.stage_name, exc)
            broadcaster.send(task.revision_id, {
                'type':   'stage_update',
                'stage':  task.stage_name,
                'status': 'completed',
            })

            next_stage = self._pipeline[task.stage_name].get('next')
            if next_stage:
                next_queue = self._pipeline[next_stage]['queue']
                next_task  = Task(
                    revision_id = task.revision_id,
                    stage_name  = next_stage,
                    queue_name  = next_queue,
                    payload     = task.payload,
                )
                registry.get(next_queue).put(next_task)
            else:
                with self._lock:
                    self._active.discard(task.revision_id)
            return

        # Clear DB comments for this stage before the worker runs so the page-load
        # DB fetch never returns stale comments from a previous run.
        try:
            db_sync.reset_stage_comments(task.revision_id, task.stage_name)
        except Exception as exc:
            logger.error('Dispatcher: failed to reset stage comments for revision %d stage "%s" — %s', task.revision_id, task.stage_name, exc)

        # Mark revision as actively processing
        try:
            db_sync.set_revision_processing(task.revision_id)
        except Exception as exc:
            logger.error('Dispatcher: failed to set processing status for revision %d — %s', task.revision_id, exc)

        # Announce stage is starting — update DB and WS together so both stay in sync
        try:
            db_sync.set_stage_status(task.revision_id, task.stage_name, 'processing')
        except Exception as exc:
            logger.error('Dispatcher: failed to set stage status for revision %d stage "%s" — %s', task.revision_id, task.stage_name, exc)

        # Find the previous stage (the one that has next == current stage) and
        # broadcast its completed status so the frontend shows it correctly.
        # This covers the pause_after resume case (e.g. Cabin Validation → Node Identification).
        prev_stage = next(
            (name for name, cfg in self._pipeline.items() if cfg.get('next') == task.stage_name),
            None,
        )

        # Clear GhostRoom then broadcast a clean snapshot of current state:
        # 1. previous stage completed, 2. revision processing, 3. current stage processing
        broadcaster.clear_GhostRoom(task.revision_id)
        if prev_stage:
            broadcaster.send(task.revision_id, {
                'type':   'stage_update',
                'stage':  prev_stage,
                'status': 'completed',
            })
        broadcaster.send(task.revision_id, {
            'type':            'revision_status',
            'revision_status': 'processing',
        })
        broadcaster.send(task.revision_id, {
            'type':  'stage_update',
            'stage': task.stage_name,
            'status': 'processing',
        })

        future = pool.submit(stage_fn, task, self._update_queue)
        future.add_done_callback(lambda f: self._on_task_done(f, task))

    def _on_task_done(self, future, task: Task) -> None:
        """
        Called by the ThreadPoolExecutor when a worker finishes.
        Reads the TaskResult, flushes to DB, pushes WS update, and chains the next stage.
        """
        revision_id = task.revision_id
        stage_name  = task.stage_name

        try:
            result: TaskResult = future.result()
        except Exception as exc:
            logger.error(
                'Executor raised an exception for revision %d stage "%s": %s',
                revision_id, stage_name, exc,
            )
            result = None

        if result is None:
            logger.error(
                'Executor returned None for revision %d stage "%s" — treating as failed',
                revision_id, stage_name,
            )
            result = TaskResult(
                status='failed',
                comments=[],
                file_paths=[],
                time_taken=0.0,
                error='Executor returned None (no TaskResult)',
            )

        try:
            # Flush stage result to DB
            try:
                db_sync.flush_stage(revision_id, stage_name, result)
            except Exception as exc:
                logger.error('flush_stage failed for revision %d stage "%s": %s', revision_id, stage_name, exc)

            # Push stage completion to browser — DB already written by flush_stage above,
            # but also call set_stage_status to ensure status field is in sync if flush_stage partial-failed
            try:
                db_sync.set_stage_status(revision_id, stage_name, result.status)
            except Exception as exc:
                logger.error('Dispatcher: failed to set stage status for revision %d stage "%s" — %s', revision_id, stage_name, exc)
            broadcaster.send(revision_id, {
                'type':       'stage_update',
                'stage':      stage_name,
                'status':     result.status,
                'time_taken': result.time_taken,
            })

            if result.status == 'failed':
                broadcaster.send(revision_id, {
                    'type':  'error',
                    'stage': stage_name,
                    'text':  result.error or 'Unknown error',
                })
                try:
                    db_sync.set_revision_failed(revision_id)
                except Exception as exc:
                    logger.error('set_revision_failed failed for revision %d: %s', revision_id, exc)
                broadcaster.send(revision_id, {
                    'type':            'done',
                    'revision_status': 'failed',
                })
                broadcaster.clear_GhostRoom(revision_id)
                return

            next_stage  = self._pipeline[stage_name].get('next')
            pause_after = self._pipeline[stage_name].get('pause_after', False)

            # If pause_after=True, stop pipeline and wait for user to confirm on screen
            if next_stage and pause_after:
                try:
                    db_sync.set_revision_awaiting_review(revision_id)
                except Exception as exc:
                    logger.error('set_revision_awaiting_review failed for revision %d: %s', revision_id, exc)
                try:
                    db_sync.set_stage_status(revision_id, next_stage, 'awaiting_review')
                except Exception as exc:
                    logger.error('set_stage_status awaiting_review failed for revision %d stage "%s": %s', revision_id, next_stage, exc)
                broadcaster.send(revision_id, {
                    'type':       'awaiting_review',
                    'next_stage': next_stage,
                })
                logger.info('Revision %d paused at "%s" — awaiting user review', revision_id, next_stage)
                return

            if next_stage:
                next_queue = self._pipeline[next_stage]['queue']

                # Keep section_data in payload fresh so the next stage can read
                # any previous stage's file_paths via get_stage_paths()
                updated_section_data = task.payload.get('section_data', {})
                updated_section_data.setdefault('stages', {})[stage_name] = {
                    'status':     result.status,
                    'comments':   result.comments,
                    'file_paths': result.file_paths,
                    'time_taken': result.time_taken,
                    'error':      result.error,
                }

                next_task  = Task(
                    revision_id = revision_id,
                    stage_name  = next_stage,
                    queue_name  = next_queue,
                    payload     = {**task.payload, 'section_data': updated_section_data},
                )
                registry.get(next_queue).put(next_task)
            else:
                # All stages done
                try:
                    db_sync.complete_revision(revision_id)
                except Exception as exc:
                    logger.error('complete_revision failed for revision %d: %s', revision_id, exc)

                broadcaster.send(revision_id, {
                    'type':            'done',
                    'revision_status': 'completed',
                })
                broadcaster.clear_GhostRoom(revision_id)
                logger.info('Revision %d completed all stages', revision_id)

        finally:
            # Always runs — even if anything above raises — so the slot is never permanently blocked
            with self._lock:
                self._in_flight[task.queue_name] = max(0, self._in_flight.get(task.queue_name, 1) - 1)
                if (result is None
                        or result.status == 'failed'
                        or not self._pipeline[stage_name].get('next')
                        or self._pipeline[stage_name].get('pause_after', False)):
                    self._active.discard(revision_id)
