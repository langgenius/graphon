from __future__ import annotations

import argparse
import sys
from collections.abc import Iterable
from dataclasses import dataclass
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
    GraphEngine,
    GraphEventFilterContext,
    ResponseStreamFilter,
    filter_graph_events,
)
from graphon.graph_events.base import GraphEngineEvent
from graphon.graph_events.graph import GraphRunSucceededEvent
from graphon.graph_events.node import NodeRunStreamChunkEvent
from graphon.graph_events.traversal import GraphEdgeTakenEvent


@dataclass(frozen=True)
class EventFilterExampleResult:
    answer: str
    stream_text: str
    stream_chunk_count: int
    edge_taken_count: int


def build_engine(query: str) -> GraphEngine:
    use_local_slim_binary()
    return loads(
        GRAPH_FILE.read_text(encoding="utf-8"),
        credentials=load_credentials(),
        workflow_id="slim-llm-event-filter-example",
        start_inputs={"query": query},
    )


def iter_events(
    engine: GraphEngine,
    *,
    response_stream_filter: bool,
) -> Iterable[GraphEngineEvent]:
    events = engine.run()
    if not response_stream_filter:
        return events

    return filter_graph_events(
        events,
        context=GraphEventFilterContext.from_engine(engine),
        filters=[ResponseStreamFilter()],
    )


def run(query: str, *, response_stream_filter: bool = True) -> EventFilterExampleResult:
    engine = build_engine(query)
    chunks: list[str] = []
    edge_taken_count = 0
    final_event: GraphRunSucceededEvent | None = None

    for event in iter_events(
        engine,
        response_stream_filter=response_stream_filter,
    ):
        if isinstance(event, NodeRunStreamChunkEvent):
            chunks.append(event.chunk)
        elif isinstance(event, GraphEdgeTakenEvent):
            edge_taken_count += 1
        elif isinstance(event, GraphRunSucceededEvent):
            final_event = event

    if final_event is None:
        msg = "Workflow did not emit GraphRunSucceededEvent."
        raise RuntimeError(msg)

    answer = final_event.outputs.get("answer")
    if not isinstance(answer, str):
        msg = "Workflow finished without a string answer."
        raise TypeError(msg)

    return EventFilterExampleResult(
        answer=answer,
        stream_text="".join(chunks),
        stream_chunk_count=len(chunks),
        edge_taken_count=edge_taken_count,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("query", nargs="?", default=DEFAULT_QUERY)
    parser.add_argument(
        "--raw",
        action="store_true",
        help=(
            "consume GraphEngine.run() directly instead of applying "
            "ResponseStreamFilter"
        ),
    )
    args = parser.parse_args()

    result = run(args.query, response_stream_filter=not args.raw)
    mode = "raw" if args.raw else "response-stream-filter"

    sys.stdout.write(f"mode: {mode}\n")
    sys.stdout.write(f"stream_chunk_count: {result.stream_chunk_count}\n")
    sys.stdout.write(f"edge_taken_count: {result.edge_taken_count}\n")
    if result.stream_text:
        sys.stdout.write("stream_text:\n")
        sys.stdout.write(f"{result.stream_text}\n")
    sys.stdout.write("final_answer:\n")
    sys.stdout.write(f"{result.answer}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
