from __future__ import annotations

from workflow.engine.queues.base_queue import BaseQueue


class QueueRegistry:
    """
    Central registry of all named queues.

    The engine never hardcodes queue names. At startup, main.py calls
    registry.setup(QUEUES) with the list that comes from
    scripts/pipeline_config.py — so the engine stays completely
    unaware of which queues a particular client needs.

    Usage:
        from dockyard.engine.queues.registry import registry

        registry.setup(['CABIN_DETECTION_QUEUE'])
        q = registry.get('CABIN_DETECTION_QUEUE')
        q.put(task)
    """

    def __init__(self) -> None:
        self._queues: dict[str, BaseQueue] = {}

    def setup(self, queue_names: list[str]) -> None:
        """
        Called once at startup with the QUEUES list from dockyard/scripts/pipeline_config.py.
        Creates a BaseQueue for every name and registers it.
        """
        for name in queue_names:
            self._queues[name] = BaseQueue(name)

    def get(self, name: str) -> BaseQueue:
        if name not in self._queues:
            raise KeyError(
                f"Queue '{name}' is not registered. "
                f"Available queues: {list(self._queues.keys())}"
            )
        return self._queues[name]

    def all(self) -> dict[str, BaseQueue]:
        return dict(self._queues)

    def names(self) -> list[str]:
        return list(self._queues.keys())

    def __repr__(self) -> str:
        return f"<QueueRegistry queues={self.names()}>"


# module-level singleton — imported everywhere in the engine
registry = QueueRegistry()
