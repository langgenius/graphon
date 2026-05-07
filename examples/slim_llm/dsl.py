from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from examples.slim_llm.settings import (
    DEFAULT_QUERY,
    GRAPH_FILE,
    load_credentials,
    use_local_slim_binary,
)
from graphon.dsl import loads
from graphon.graph_events.graph import GraphRunSucceededEvent


def run(query: str) -> str:
    use_local_slim_binary()
    engine = loads(
        GRAPH_FILE.read_text(encoding="utf-8"),
        credentials=load_credentials(),
        workflow_id="slim-llm-dsl-example",
        start_inputs={"query": query},
    )

    events = list(engine.run())
    final_event: Any = events[-1] if events else None
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

    sys.stdout.write(f"{run(args.query)}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
