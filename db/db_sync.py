import logging
import time
import functools

from django.contrib.sessions.models import Session
from django.db import OperationalError, InterfaceError
from django.db import close_old_connections
from django.utils import timezone

from core.models import ProcessingJob, User
from workflow.engine.tasks.base_task import TaskResult

logger = logging.getLogger(__name__)

_RETRY_ATTEMPTS = 3
_RETRY_BACKOFF  = [1, 2, 4]  # seconds between attempts


def _with_retry(fn):
    """
    Decorator that retries a DB operation up to _RETRY_ATTEMPTS times
    on transient connection failures (OperationalError, InterfaceError).
    Closes stale connections before each retry so Django opens a fresh one.
    """
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        last_exc = None
        for attempt in range(_RETRY_ATTEMPTS):
            try:
                return fn(*args, **kwargs)
            except (OperationalError, InterfaceError) as exc:
                last_exc = exc
                logger.warning(
                    '%s failed (attempt %d/%d): %s — retrying in %ds',
                    fn.__name__, attempt + 1, _RETRY_ATTEMPTS, exc, _RETRY_BACKOFF[attempt],
                )
                close_old_connections()
                time.sleep(_RETRY_BACKOFF[attempt])
        logger.error('%s failed after %d attempts: %s', fn.__name__, _RETRY_ATTEMPTS, last_exc)
        raise last_exc
    return wrapper


@_with_retry
def get_pending_revisions() -> list[dict]:
    """
    Return all ProcessingJob rows where status IN ('processing', 'queued').

    Each dict has: id (int), section_data (dict).
    """
    try:
        rows = ProcessingJob.objects.filter(
            status__in=['processing', 'queued', 'awaiting_review']
        ).values('id', 'section_data')
        return [
            {
                'id':           r['id'],
                'section_data': r['section_data'] or {},
            }
            for r in rows
        ]
    except Exception as exc:
        logger.error('get_pending_revisions failed: %s', exc)
        raise


@_with_retry
def get_section_data(revision_id: int) -> dict:
    """Fetch section_data for a revision regardless of its status."""
    try:
        job = ProcessingJob.objects.filter(pk=revision_id).values('section_data').first()
        return job['section_data'] or {} if job else {}
    except Exception as exc:
        logger.error('get_section_data failed for job %s: %s', revision_id, exc)
        raise


@_with_retry
def flush_stage(revision_id: int, stage_name: str, result: TaskResult) -> None:
    """
    Update section_data for one completed stage and write it back to DB.
    """
    try:
        job = ProcessingJob.objects.get(pk=revision_id)
        section_data = job.section_data if isinstance(job.section_data, dict) else {}
        section_data.setdefault('stages', {})[stage_name] = {
            'status':     result.status,
            'comments':   result.comments,
            'file_paths': result.file_paths,
            'time_taken': result.time_taken,
            'error':      result.error,
        }
        job.section_data = section_data
        job.save(update_fields=['section_data'])
    except ProcessingJob.DoesNotExist:
        logger.error('flush_stage: job %s not found', revision_id)
    except Exception as exc:
        logger.error('flush_stage failed for job %s stage %s: %s', revision_id, stage_name, exc)
        raise


@_with_retry
def set_stage_status(revision_id: int, stage_name: str, status: str) -> None:
    """
    Update a single stage's status inside section_data.
    """
    try:
        job = ProcessingJob.objects.get(pk=revision_id)
        section_data = job.section_data if isinstance(job.section_data, dict) else {}
        section_data.setdefault('stages', {}).setdefault(stage_name, {})['status'] = status
        job.section_data = section_data
        job.save(update_fields=['section_data'])
    except ProcessingJob.DoesNotExist:
        logger.error('set_stage_status: job %s not found', revision_id)
    except Exception as exc:
        logger.error('set_stage_status failed for job %s stage %s: %s', revision_id, stage_name, exc)
        raise


@_with_retry
def reset_stage_comments(revision_id: int, stage_name: str) -> None:
    """
    Clear the comments list for a stage before a fresh run begins.
    """
    try:
        job = ProcessingJob.objects.get(pk=revision_id)
        section_data = job.section_data if isinstance(job.section_data, dict) else {}
        section_data.setdefault('stages', {}).setdefault(stage_name, {})['comments'] = []
        job.section_data = section_data
        job.save(update_fields=['section_data'])
    except ProcessingJob.DoesNotExist:
        logger.error('reset_stage_comments: job %s not found', revision_id)
    except Exception as exc:
        logger.error('reset_stage_comments failed for job %s stage %s: %s', revision_id, stage_name, exc)


def set_revision_queued(revision_id: int) -> None:
    """Set RevisionWorkflow.status = 'queued'."""
    _set_status(revision_id, 'queued')


def set_revision_awaiting_review(revision_id: int) -> None:
    """Set RevisionWorkflow.status = 'awaiting_review'."""
    _set_status(revision_id, 'awaiting_review')


def set_revision_processing(revision_id: int) -> None:
    """Set RevisionWorkflow.status = 'processing'."""
    _set_status(revision_id, 'processing')


def complete_revision(revision_id: int) -> None:
    """Set RevisionWorkflow.status = 'completed'."""
    _set_status(revision_id, 'completed')


def set_revision_failed(revision_id: int) -> None:
    """Set RevisionWorkflow.status = 'failed'."""
    _set_status(revision_id, 'failed')


@_with_retry
def _set_status(revision_id: int, status: str) -> None:
    try:
        ProcessingJob.objects.filter(pk=revision_id).update(status=status)
    except Exception as exc:
        logger.error('_set_status(%s) failed for job %s: %s', status, revision_id, exc)
        raise


@_with_retry
def validate_session_token(session_key: str) -> str | None:
    """
    Validate a Django sessionid cookie value.
    Returns the user email if the session is valid and UserSession record exists.
    """
    try:
        from core.models import UserSession
        session = Session.objects.filter(
            session_key=session_key,
            expire_date__gt=timezone.now(),
        ).first()
        if session is None:
            return None
        user_id = session.get_decoded().get('_auth_user_id')
        if not user_id:
            return None
        user = User.objects.filter(pk=user_id, is_active=True).first()
        if not user:
            return None
        if not UserSession.objects.filter(user=user, session_key=session_key).exists():
            return None
        return user.email
    except Exception as exc:
        logger.error('validate_session_token failed: %s', exc)
        raise


@_with_retry
def get_revision_owner(revision_id: int) -> str | None:
    """
    Return the uploaded_by for the given job.
    Returns None if the job does not exist.
    """
    try:
        job = ProcessingJob.objects.filter(pk=revision_id).values('uploaded_by').first()
        return job['uploaded_by'] if job else None
    except Exception as exc:
        logger.error('get_revision_owner failed for job %s: %s', revision_id, exc)
        raise
