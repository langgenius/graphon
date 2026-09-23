import subprocess  # ruff: ignore[suspicious-subprocess-import]
import sys
from textwrap import dedent


def test_current_snapshots_do_not_import_legacy_version_modules() -> None:
    """Verify current public APIs work when legacy snapshot modules are absent.

    The subprocess starts with a clean import graph and marks each removable legacy
    module as unavailable before importing Graphon. A current RuntimeState v3
    round-trip and ResponseStreamFilter v2 round-trip must still succeed. This
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

        from graphon.engine.filter import ResponseStreamFilter
        from graphon.runtime import RuntimeState, VariablePool

        runtime = RuntimeState(
            variable_pool=VariablePool(),
            start_at=0,
            workflow_id="workflow",
        )
        restored_runtime = RuntimeState.from_snapshot(runtime.dumps())
        assert json.loads(restored_runtime.dumps())["version"] == "3.0"

        response_filter = ResponseStreamFilter()
        response_filter.loads(
            json.dumps({"type": "ResponseStreamFilter", "version": "2.0"})
        )
        assert json.loads(response_filter.dumps())["version"] == "2.0"

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
