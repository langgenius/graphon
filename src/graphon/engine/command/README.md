<!-- knowledge
last_checked: "2026-09-09T21:56:06Z"
-->
# Commands

Command processing and channels for external workflow control.

Use the [command definitions](entities.py) for payloads and deserialization
literals, [CommandProcessor](processor.py) for dispatch, and the channel
implementations for [in-memory](builtin/in_memory.py) or
[Redis](builtin/redis.py) delivery. The
[dispatch tests](../../../../tests/engine/test_dispatch_patterns.py) cover
workflow control behavior.

## Usage

```python
from graphon.engine.command import AbortCommand, InMemoryChannel, RedisChannel

# Local execution
channel = InMemoryChannel()
channel.send_command(AbortCommand(reason="stop"))

# Distributed execution
redis_channel = RedisChannel(
    redis_client=redis_client, channel_key="workflow:123:commands"
)
```
