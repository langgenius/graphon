"""Fast end-to-end benchmark for Graphon's engine execution.

Run this file explicitly with ``pytest --benchmark-enable -n 0`` so the
repository's default disabled benchmark mode and xdist configuration do not
disable or distort the measurements.
"""

from __future__ import annotations

import json
from itertools import pairwise
from typing import cast

import pytest
from pytest_benchmark.fixture import BenchmarkFixture

from graphon.dsl import loads
from graphon.engine import Engine
from graphon.engine_events import (
    EngineEvent,
    GraphRunSucceededEvent,
    NodeRunSucceededEvent,
)

_WORKERS = 5
_ROUNDS = 2
_STEP_NODE_IDS = tuple(f"step-{index}" for index in range(3))
_EXPECTED_NODE_IDS = ("start", *_STEP_NODE_IDS, "end")
_EXPECTED_OUTPUTS = {"result": "ok"}
_WORKFLOW_DSL = json.dumps({
    "kind": "graph",
    "dependencies": [],
    "graph": {
        "nodes": [
            {"id": "start", "data": {"type": "start", "variables": []}},
            *[
                {
                    "id": node_id,
                    "data": {
                        "type": "template-transform",
                        "variables": [],
                        "template": "ok",
                    },
                }
                for node_id in _STEP_NODE_IDS
            ],
            {
                "id": "end",
                "data": {
                    "type": "end",
                    "outputs": [
                        {
                            "variable": "result",
                            "value_selector": [_STEP_NODE_IDS[-1], "output"],
                        },
                    ],
                },
            },
        ],
        "edges": [
            {"source": source, "target": target}
            for source, target in pairwise(_EXPECTED_NODE_IDS)
        ],
    },
})


def _setup_engine() -> tuple[tuple[Engine], dict[str, object]]:
    """Create the one-shot Engine passed to one benchmark round.

    ``pytest-benchmark`` invokes this setup before starting the timer. Building
    the DSL, graph, runtime state, and worker objects is therefore excluded from
    the measurement, while returning a new Engine every time prevents completed
    runtime state from leaking between measured rounds.

    Returns:
        Positional and keyword arguments containing one fresh Engine instance.

    """
    return (loads(_WORKFLOW_DSL, workers=_WORKERS),), {}


def _run_engine(engine: Engine) -> list[EngineEvent]:
    """Run one Engine instance to completion and return every emitted event.

    ``Engine.run()`` is a lazy generator, so fully materializing it is part of
    this function's definition: the measured call includes worker startup,
    scheduling, all sequential node handoffs, event dispatch, and shutdown.

    Returns:
        The complete ordered event stream produced by the engine execution.

    """
    return list(engine.run())


def test_engine_run_benchmark(benchmark: BenchmarkFixture) -> None:
    """Measure a complete local workflow without third-party service nodes.

    The workload is a deterministic single-worker chain containing Start, three
    constant Template Transform nodes, and End. Fixed pedantic rounds keep the
    benchmark quick and comparable, while the terminal event, output, and
    node-order checks ensure an implementation cannot appear faster by skipping
    scheduled work.
    """
    events = cast(
        list[EngineEvent],
        benchmark.pedantic(
            _run_engine,
            setup=_setup_engine,
            rounds=_ROUNDS,
        ),
    )

    if not events or not isinstance(events[-1], GraphRunSucceededEvent):
        pytest.fail("engine did not emit a terminal success event")
    if events[-1].outputs != _EXPECTED_OUTPUTS:
        pytest.fail(f"unexpected workflow outputs: {events[-1].outputs!r}")

    succeeded_node_ids = tuple(
        event.node_id for event in events if isinstance(event, NodeRunSucceededEvent)
    )
    if succeeded_node_ids != _EXPECTED_NODE_IDS:
        pytest.fail(f"unexpected successful node order: {succeeded_node_ids!r}")
