import subprocess  # ruff: ignore[suspicious-subprocess-import]
import sys
from textwrap import dedent


def test_current_snapshots_do_not_import_legacy_version_modules() -> None:
    """Verify current public APIs work when legacy snapshot modules are absent.

    The subprocess starts with a clean import graph and marks each removable legacy
    module as unavailable before importing Graphon. A current RuntimeState v3
    round-trip and ResponseStreamFilter v4 round-trip must still succeed. This
    protects the file-level retirement contract without asserting private models or
    migration implementation details.
    """
    script = dedent(
        """
        import json
        import sys

        sys.modules["graphon.runtime.runtime_state.v1"] = None
        sys.modules["graphon.runtime.runtime_state.v2"] = None
        sys.modules["graphon.engine.filter.builtin.response_stream.v1"] = None
        sys.modules["graphon.engine.filter.builtin.response_stream.v2"] = None
        sys.modules["graphon.engine.filter.builtin.response_stream.v3"] = None

        from graphon.dsl import loads
        from graphon.engine.filter import ResponseStreamFilter, EngineEventFilterContext
        from graphon.runtime import RuntimeState, VariablePool

        runtime = RuntimeState(
            variable_pool=VariablePool(),
            start_at=0,
            workflow_id="workflow",
        )
        restored_runtime = RuntimeState.from_snapshot(runtime.dumps())
        assert json.loads(restored_runtime.dumps())["version"] == "3.0"

        engine = loads(json.dumps({"kind": "graph", "graph": {"nodes": [
            {"id": "start", "data": {"type": "start", "variables": []}},
            {"id": "answer", "data": {"type": "answer", "answer": "hello"}},
        ], "edges": [{"source": "start", "target": "answer"}]}}))
        response_filter = ResponseStreamFilter()
        context = EngineEventFilterContext.from_engine(engine)
        response_filter.initialize(context)
        snapshot = response_filter.dumps()
        restored_filter = ResponseStreamFilter()
        restored_filter.loads(snapshot)
        restored_filter.initialize(context)
        assert json.loads(restored_filter.dumps())["version"] == "4.0"

        try:
            RuntimeState.from_snapshot(json.dumps({"version": "1.0"}))
        except ValueError as error:
            assert "Unsupported RuntimeState snapshot version" in str(error)
        else:
            raise AssertionError("Removed RuntimeState v1 module was loaded")

        try:
            ResponseStreamFilter().loads(
                json.dumps({"type": "ResponseStreamFilter", "version": "1.0"})
            )
        except ValueError as error:
            assert "Unsupported ResponseStreamFilter snapshot version" in str(error)
        else:
            raise AssertionError("Removed ResponseStreamFilter v1 module was loaded")
        """
    )

    subprocess.run([sys.executable, "-c", script], check=True)  # ruff: ignore[subprocess-without-shell-equals-true]
