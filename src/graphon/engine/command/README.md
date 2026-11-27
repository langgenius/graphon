<!-- knowledge
last_checked: "2026-11-26T03:34:51Z"
-->
# Commands

Use commands to pause or abort a graph execution or update its variables.

Use the [command definitions](entities.py) for payloads and deserialization
literals, [CommandProcessor](processor.py) for dispatch, and the channel
implementations for [in-memory](builtin/in_memory.py) or
[Redis](builtin/redis.py) delivery. The
[dispatch tests](../../../../tests/engine/test_dispatch_patterns.py) cover
command handling. With frame observations enabled, successful variable
writes produce
[frame commit observations](../../../../ARCHITECTURE.md#raw-frame-observations),
including writes completed before a later command update fails.

## Usage

Pass one `command_channel` per execution to [Engine](../engine.py). For a shared
`Graph` definition, pass the host's `node_factory` and select `root_node_id` if
the graph has multiple entries. For Redis, use a unique `channel_key` for each
execution. Consume `engine.run()` and watch for
[`GraphRunPausedEvent` or `GraphRunAbortedEvent`](../../engine_events/graph.py)
to see when a queued pause or abort takes effect.

```python
from graphon.engine import Engine
from graphon.engine.command import AbortCommand, InMemoryChannel, RedisChannel
from graphon.engine_events import GraphRunAbortedEvent

# Local execution
channel = InMemoryChannel()
engine = Engine(
    graph=graph,
    runtime_state=runtime_state,
    node_factory=node_factory,
    command_channel=channel,
)
channel.send_command(AbortCommand(reason="stop"))
for event in engine.run():
    if isinstance(event, GraphRunAbortedEvent):
        print(event.reason)

# Distributed execution
redis_channel = RedisChannel(
    redis_client=redis_client, channel_key=f"graph:{execution_id}:commands"
)
```
