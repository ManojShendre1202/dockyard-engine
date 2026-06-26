"""
Startup recovery for stuck 'processing' revisions.

Called once at startup before the dispatcher loop begins.
Queries the DB for any RevisionWorkflow rows still in status='processing'
and re-enqueues them from their last incomplete stage so nothing is lost
across Dockyard restarts.
"""

import logging

from workflow.engine.db.db_sync import get_pending_revisions
from workflow.engine.queues.registry import registry
from workflow.engine.tasks.base_task import Task

logger = logging.getLogger(__name__)


def _resume_stage(pipeline: dict, entry_stage: str, section_data: dict) -> str:
    """
    Walk the pipeline in execution order and return the first stage
    that has not yet completed. Falls back to entry_stage if section_data
    has no stage info at all.
    """
    stages_data = section_data.get("stages", {})

    stage = entry_stage
    while stage:
        status = stages_data.get(stage, {}).get("status", "")
        if status != "completed":
            return stage
        stage = pipeline[stage].get("next")

    # All stages show completed but revision is still marked as processing.
    # Re-run the entry stage to recover safely.
    return entry_stage


def run_recovery(pipeline: dict, entry_stage: str) -> None:
    """
    Re-enqueue all pending revisions from the DB.

    Creates a Task for each revision at its resume stage and puts it
    into the correct queue. The dispatcher will pick these up immediately
    when it starts.
    """
    try:
        pending = get_pending_revisions()
    except Exception as exc:
        logger.error("Recovery: failed to query pending revisions - %s", exc)
        return

    if not pending:
        logger.info("Recovery: no pending revisions found")
        return

    logger.info("Recovery: found %d pending revision(s)", len(pending))

    for row in pending:
        revision_id = row["id"]
        section_data = row["section_data"]

        stage_name = _resume_stage(pipeline, entry_stage, section_data)
        queue_name = pipeline[stage_name]["queue"]

        task = Task(
            revision_id=revision_id,
            stage_name=stage_name,
            queue_name=queue_name,
            payload={
                "file_path": section_data.get("file_path", ""),
                "output_path": section_data.get("output_path", ""),
            },
        )

        try:
            registry.get(queue_name).put(task)
            logger.info(
                'Recovery: re-enqueued revision %d at stage "%s"',
                revision_id,
                stage_name,
            )
        except KeyError as exc:
            logger.error("Recovery: queue not found for revision %d - %s", revision_id, exc)
