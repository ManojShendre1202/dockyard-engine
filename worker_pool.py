import logging
import os
from concurrent.futures import Future, ProcessPoolExecutor

logger = logging.getLogger(__name__)


class WorkerPool:
    """
    Thin wrapper around ProcessPoolExecutor.

    Each worker is a separate process — bypasses GIL completely for true
    parallelism even with pure Python CPU-heavy computation.
    Sized at cpu_count - 2 to leave headroom for OS, Django, and engine threads.
    Minimum of 1 worker regardless of core count.
    """

    def __init__(self) -> None:
        self._max_workers = max(1, os.cpu_count() - 2)
        self._executor = ProcessPoolExecutor(max_workers=self._max_workers)
        logger.info('WorkerPool started with %d workers', self._max_workers)

    def submit(self, fn, *args, **kwargs) -> Future:
        """Submit a callable to the pool. Returns a Future immediately."""
        return self._executor.submit(fn, *args, **kwargs)

    def shutdown(self, wait: bool = True) -> None:
        """Graceful shutdown — waits for running tasks to finish by default."""
        self._executor.shutdown(wait=wait)
        logger.info('WorkerPool shut down')


# Module-level singleton — imported directly by the Dispatcher
pool = WorkerPool()
