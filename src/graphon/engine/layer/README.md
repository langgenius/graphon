# Layers

Pluggable middleware for engine extensions.

## Components

### Layer (base)

Base class with optional lifecycle hooks for layers.

- `initialize()` - Receive runtime context (runtime state is bound here and always available to hooks)
- `on_graph_start()` - Execution start hook
- `on_event()` - Process all events
- `on_graph_end()` - Execution end hook

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

`engine.add_layer()` binds the read-only runtime state before execution, so
`graph_runtime_state` is always available inside layer hooks.
