from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from examples.slim_llm.settings import (
    DEFAULT_QUERY,
    OPENAI_MODEL,
    OPENAI_PLUGIN_ID,
    OPENAI_PROVIDER,
    load_credentials,
    use_local_slim_binary,
)
from graphon.dsl import DslDependency, PluginDependencyType, load_graph
from graphon.graph import Graph, NodeDefinition


def build_graph() -> Graph:
    start = NodeDefinition("start", "start")
    llm = NodeDefinition(
        "llm",
        "llm",
        config={
            "title": "LLM",
            "model": {
                "provider": OPENAI_PROVIDER,
                "name": OPENAI_MODEL,
                "mode": "chat",
            },
            "prompt_template": [{"role": "user", "text": "{{#start.query#}}"}],
            "context": {"enabled": False},
        },
    )
    answer = NodeDefinition("answer", "answer", config={"answer": "{{#llm.text#}}"})
    return Graph.new().add_root(start).add_node(llm).add_node(answer).build()


def run(query: str) -> str:
    use_local_slim_binary()
    engine = load_graph(
        build_graph(),
        graph_id="slim-llm-code-example",
        start_inputs={"query": query},
        credentials=load_credentials(),
        dependencies=[
            DslDependency(
                type=PluginDependencyType.MARKETPLACE,
                plugin_unique_identifier=OPENAI_PLUGIN_ID,
            )
        ],
    )
    list(engine.run())
    answer = engine.runtime_state.get_output("answer")
    if not isinstance(answer, str):
        msg = "Graph finished without a string answer."
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
