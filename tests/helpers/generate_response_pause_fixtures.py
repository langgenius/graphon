"""Capture paired snapshots with historical engines, never relabel current JSON.

Run with Python 3.12 and PYTHONPATH=<git-archive-root>/src from that archive root.
Pass --revision, --dsl and --output as absolute paths/commit identities. Supported
writers are c3870df (runtime v2/filter v1) and 069cd55 (runtime v3/filter v2).
The historical test helpers supply each revision's real node factory and engine.
"""

import argparse
import importlib
import json
from pathlib import Path
from typing import Any

from graphon.entities.pause_reason import SchedulingPause
from graphon.nodes.human_input.entities import HITLContext, PauseRequested


def _serialize_stream_chunks(events: list[Any]) -> list[dict[str, Any]]:
    return [
        {
            "id": event.id,
            "node_id": event.node_id,
            "selector": event.selector,
            "chunk": event.chunk,
            "is_final": event.is_final,
        }
        for event in events
        if type(event).__name__ == "NodeRunStreamChunkEvent"
    ]


def _request_hitl_pause(context: HITLContext) -> PauseRequested:
    return PauseRequested(session_id=f"session-{context.node_id}")


def _create_pause_command_engine(
    helpers: Any, uses_graph_engine: bool, dsl: str
) -> Any:
    channel_module = importlib.import_module(
        "graphon.graph_engine.command_channels.in_memory_channel"
        if uses_graph_engine
        else "graphon.engine.command.builtin.in_memory"
    )
    commands = importlib.import_module(
        "graphon.graph_engine.entities.commands"
        if uses_graph_engine
        else "graphon.engine.command"
    )
    channel = channel_module.InMemoryChannel()
    channel.send_command(commands.PauseCommand(reason="host checkpoint"))
    runtime = helpers._new_runtime_state({})
    hitl_engine = helpers._hitl_engine(
        dsl, runtime_state=runtime, callback=_request_hitl_pause
    )
    if uses_graph_engine:
        config = importlib.import_module("graphon.graph_engine.config")
        command_engine = type(hitl_engine)(
            workflow_id="workflow",
            graph=hitl_engine.graph,
            graph_runtime_state=runtime,
            command_channel=channel,
            config=config.GraphEngineConfig(min_workers=1, max_workers=1),
        )
    else:
        command_engine = type(hitl_engine)(
            graph=hitl_engine.graph,
            runtime_state=runtime,
            command_channel=channel,
            workers=1,
        )
    return command_engine


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--dsl", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not args.revision.startswith(("c3870df", "069cd55")):
        parser.error("Only the pinned c3870df and 069cd55 writers are supported")
    uses_graph_engine = args.revision.startswith("c3870df")
    # Names and constructor signatures changed between these historical APIs.
    # Keep that dynamic adaptation confined to this fixture-generation boundary.
    helpers = importlib.import_module(
        "tests.graph_engine.test_serializable_graph_runtime"
        if uses_graph_engine
        else "tests.engine.test_runtime_state_serialization"
    )
    filters = importlib.import_module(
        "graphon.graph_engine.filters" if uses_graph_engine else "graphon.engine.filter"
    )
    filter_events = (
        filters.filter_graph_events
        if uses_graph_engine
        else filters.filter_engine_events
    )
    context_type = (
        filters.GraphEventFilterContext
        if uses_graph_engine
        else filters.EngineEventFilterContext
    )
    dsl = args.dsl.read_text()

    def complete_first(context: HITLContext) -> Any:
        if context.node_id == "first":
            return helpers._completed_hitl("alpha")
        return _request_hitl_pause(context)

    def capture_pause(engine: Any, stream_filter: Any) -> dict[str, Any]:
        events = list(
            filter_events(
                engine.run(),
                filters=[stream_filter],
                context=context_type.from_engine(engine),
            )
        )
        assert type(events[-1]).__name__ == "GraphRunPausedEvent"
        state = (
            engine.graph_runtime_state if uses_graph_engine else engine.runtime_state
        )
        return {
            "runtime": state.dumps(),
            "response_filter": stream_filter.dumps(),
            "chunks": _serialize_stream_chunks(events),
            "pause": events[-1].model_dump(mode="json"),
            "completed_nodes": [
                event.node_id
                for event in events
                if type(event).__name__ == "NodeRunSucceededEvent"
            ],
            "started_nodes": {
                event.node_id: event.id
                for event in events
                if type(event).__name__ == "NodeRunStartedEvent"
            },
        }

    engine = helpers._hitl_engine(
        dsl, runtime_state=helpers._new_runtime_state({}), callback=_request_hitl_pause
    )
    first = capture_pause(engine, filters.ResponseStreamFilter())
    state_type = type(
        engine.graph_runtime_state if uses_graph_engine else engine.runtime_state
    )
    resumed_engine = helpers._hitl_engine(
        dsl,
        runtime_state=state_type.from_snapshot(first["runtime"]),
        callback=complete_first,
    )
    resumed_filter = filters.ResponseStreamFilter()
    resumed_filter.loads(first["response_filter"])
    second = capture_pause(resumed_engine, resumed_filter)

    command = capture_pause(
        _create_pause_command_engine(helpers, uses_graph_engine, dsl),
        filters.ResponseStreamFilter(),
    )
    for checkpoint in (first, second, command):
        assert json.loads(checkpoint["runtime"])["version"] == (
            "2.0" if uses_graph_engine else "3.0"
        )
        assert json.loads(checkpoint["response_filter"])["version"] == (
            "1.0" if uses_graph_engine else "2.0"
        )
    assert command["pause"]["reasons"] == [
        SchedulingPause(message="host checkpoint").model_dump(mode="json")
    ]
    args.output.write_text(
        json.dumps(
            {
                "writer_commit": args.revision,
                "first": first,
                "second": second,
                "command": command,
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
