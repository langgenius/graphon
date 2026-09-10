<!-- knowledge
last_checked: "2026-09-10T01:03:07Z"
-->
# Layers

Pluggable middleware for engine extensions.

## Lifecycle integration

The [Layer API](base.py) defines hooks and runtime access. Use task context
managers to activate tracing context; keep logical spans alive across container
suspension without carrying context tokens between workers. The
[worker](../worker/worker.py) owns task execution, and
[context tests](../../../../tests/engine/test_layer_node_run_context.py) enforce
hook ordering and failure isolation.

## Usage

```python
from graphon.engine.layer import Layer
from graphon.engine_events import EngineEvent, NodeRunSucceededEvent


class MetricsLayer(Layer):
    def __init__(self):
        """Create storage for elapsed time collected during one engine run."""
        super().__init__()
        self.metrics: dict[str, float] = {}

    def on_graph_start(self) -> None:
        """Reset collected metrics before the engine starts a new graph run."""
        self.metrics.clear()

    def on_event(self, event: EngineEvent) -> None:
        """Record elapsed time when a node run succeeds."""
        if isinstance(event, NodeRunSucceededEvent) and event.finished_at is not None:
            duration = event.finished_at - event.start_at
            self.metrics[event.node_id] = duration.total_seconds()
```

For snapshot persistence from hooks, follow the
[snapshot contract](../../../../ARCHITECTURE.md#state-and-execution-invariants).
