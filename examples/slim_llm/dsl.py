from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from examples.slim_llm.settings import (
    DEFAULT_QUERY,
    GRAPH_FILE,
    load_credentials,
    use_local_slim_binary,
)
from graphon.dsl import loads
from graphon.graph_engine import (
    GraphEventFilterContext,
    ResponseStreamFilter,
    filter_graph_events,
)
from graphon.graph_events.graph import GraphRunSucceededEvent
from graphon.graph_events.node import NodeRunStreamChunkEvent


def run(
    query: str,
    *,
    on_stream_chunk: Callable[[str], None] | None = None,
) -> str:
    use_local_slim_binary()
    engine = loads(
        GRAPH_FILE.read_text(encoding="utf-8"),
        credentials=load_credentials(),
        workflow_id="slim-llm-dsl-example",
        start_inputs={"query": query},
    )

    events = filter_graph_events(
        engine.run(),
        context=GraphEventFilterContext.from_engine(engine),
        filters=[ResponseStreamFilter()],
    )
    final_event: GraphRunSucceededEvent | None = None
    for event in events:
        if isinstance(event, NodeRunStreamChunkEvent):
            if on_stream_chunk is not None:
                on_stream_chunk(event.chunk)
        elif isinstance(event, GraphRunSucceededEvent):
            final_event = event

    if not isinstance(final_event, GraphRunSucceededEvent):
        msg = f"Workflow did not succeed: {type(final_event).__name__}"
        raise TypeError(msg)

    answer = final_event.outputs.get("answer")
    if not isinstance(answer, str):
        msg = "Workflow finished without a string answer."
        raise TypeError(msg)
    return answer


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("query", nargs="?", default=DEFAULT_QUERY)
    args = parser.parse_args()

    sys.stdout.write("stream_text:\n")
    sys.stdout.flush()

    saw_stream = False

    def write_stream_chunk(chunk: str) -> None:
        nonlocal saw_stream
        saw_stream = True
        sys.stdout.write(chunk)
        sys.stdout.flush()

    answer = run(args.query, on_stream_chunk=write_stream_chunk)
    if saw_stream:
        if not answer.endswith("\n"):
            sys.stdout.write("\n")
    else:
        sys.stdout.write("(no stream chunks)\n")

    sys.stdout.write("final_answer:\n")
    sys.stdout.write(f"{answer}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
