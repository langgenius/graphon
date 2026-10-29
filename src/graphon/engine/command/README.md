<!-- knowledge
last_checked: "2026-10-29T01:02:46Z"
-->
# Commands

Command processing and channels for external workflow control.

Use the [command definitions](entities.py) for payloads and deserialization
literals, [CommandProcessor](processor.py) for dispatch, and the channel
implementations for [in-memory](builtin/in_memory.py) or
[Redis](builtin/redis.py) delivery. The
[dispatch tests](../../../../tests/engine/test_dispatch_patterns.py) cover
workflow control behavior. Successful variable writes also produce
[frame commit observations](../../../../ARCHITECTURE.md#raw-frame-observations),
including writes completed before a later command update fails.

## Usage

Pass the channel to [Engine](../engine.py) through `command_channel`; each
channel belongs to one workflow execution.

```python
from graphon.engine import Engine
from graphon.engine.command import AbortCommand, InMemoryChannel, RedisChannel

# Local execution
channel = InMemoryChannel()
engine = Engine(graph=graph, runtime_state=runtime_state, command_channel=channel)
channel.send_command(AbortCommand(reason="stop"))

# Distributed execution
redis_channel = RedisChannel(
    redis_client=redis_client, channel_key="workflow:123:commands"
)
```
