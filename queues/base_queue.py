from __future__ import annotations

import queue

from workflow.engine.tasks.base_task import Task


class BaseQueue:
    """
    Thin wrapper around Python's thread-safe queue.Queue.

    Gives each queue a name so the Dispatcher and registry
    can refer to queues by string name rather than by object reference.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self._q: queue.Queue[Task] = queue.Queue()

    def put(self, task: Task) -> None:
        self._q.put(task)

    def get(self, block: bool = True, timeout: float | None = None) -> Task:
        return self._q.get(block=block, timeout=timeout)

    def task_done(self) -> None:
        self._q.task_done()

    def empty(self) -> bool:
        return self._q.empty()

    def size(self) -> int:
        return self._q.qsize()

    def __repr__(self) -> str:
        return f"<BaseQueue name={self.name!r} size={self.size()}>"
