from unittest.mock import MagicMock

import pytest

from graphon.engine import Engine
from graphon.engine.filter import (
    EngineEventFilterContext,
    ResponseStreamFilter,
    filter_engine_events,
)
from graphon.engine_events import (
    NodeRunStartedEvent,
    NodeRunStreamChunkEvent,
    NodeRunSucceededEvent,
)
from graphon.enums import WorkflowNodeExecutionStatus
from graphon.file import File, FileTransferMethod, FileType
from graphon.graph import Graph
from graphon.nodes.answer.answer_node import AnswerNode
from graphon.nodes.answer.entities import AnswerNodeData
from graphon.nodes.base.template import TextSegment, VariableSegment
from graphon.nodes.start.entities import StartNodeData
from graphon.nodes.start.start_node import StartNode
from graphon.runtime import RuntimeState
from graphon.variables.segments import ArrayFileSegment
from tests.helpers import build_init_params, build_variable_pool
from tests.helpers.workflow_events import final_outputs


@pytest.mark.parametrize(
    ("template", "expected"),
    [("", ""), ("Hello", "Hello"), ("Hello {{#start.name#}}!", "Hello Graphon!")],
)
def test_answer_returns_completed_outputs(template: str, expected: str) -> None:
    node = AnswerNode(
        "answer",
        AnswerNodeData(answer=template),
        init_params=build_init_params(),
        runtime_state=RuntimeState(
            workflow_id="workflow",
            variable_pool=build_variable_pool(
                variables=[(["start", "name"], "Graphon")]
            ),
            start_at=1,
        ),
    )
    node.bind_execution_id("answer-run")

    events = list(node.run())

    assert [type(event) for event in events] == [
        NodeRunStartedEvent,
        NodeRunSucceededEvent,
    ]
    succeeded = events[-1]
    assert isinstance(succeeded, NodeRunSucceededEvent)
    assert succeeded.id == "answer-run"
    assert succeeded.node_run_result.status == WorkflowNodeExecutionStatus.SUCCEEDED
    assert succeeded.node_run_result.outputs == {
        "answer": expected,
        "files": ArrayFileSegment(value=[]),
    }
    assert node.get_streaming_text_selector() == ["answer", "answer"]
    assert str(node.get_streaming_template()) == template


@pytest.mark.parametrize("filtered", [False, True])
def test_answer_preserves_file_outputs_and_rendering(filtered: bool) -> None:
    document = File(
        file_type=FileType.DOCUMENT,
        transfer_method=FileTransferMethod.LOCAL_FILE,
        reference="report-upload",
        filename="report.txt",
    )
    image = File(
        file_type=FileType.IMAGE,
        transfer_method=FileTransferMethod.REMOTE_URL,
        remote_url="https://example.com/image.png",
        filename="image.png",
    )
    unused = File(
        file_type=FileType.DOCUMENT,
        transfer_method=FileTransferMethod.TOOL_FILE,
        reference="unused-tool-file",
        filename="unused.txt",
    )
    state = RuntimeState(
        workflow_id="workflow",
        variable_pool=build_variable_pool(
            variables=[
                (["start", "document"], document),
                (["start", "files"], [image, document]),
                (["start", "unused"], [unused]),
            ]
        ),
        start_at=1,
    )
    params = build_init_params()
    answer = AnswerNode(
        "answer",
        AnswerNodeData(
            answer=(
                "Files: {{#start.document#}}\n{{#start.files#}}\n{{#start.document#}}"
            )
        ),
        init_params=params,
        runtime_state=state,
    )
    engine = Engine(
        Graph
        .new()
        .add_root(
            StartNode("start", StartNodeData(), init_params=params, runtime_state=state)
        )
        .add_node(answer)
        .build(),
        state,
        workers=1,
        file_runtime=MagicMock(
            resolve_file_url=lambda *, file, **_kwargs: (
                file.remote_url or "https://example.com/report.txt"
            )
        ),
    )
    raw_events = list(engine.run())
    assert not any(isinstance(event, NodeRunStreamChunkEvent) for event in raw_events)
    events = (
        list(
            filter_engine_events(
                raw_events,
                context=EngineEventFilterContext.from_engine(engine),
                filters=[ResponseStreamFilter()],
            )
        )
        if filtered
        else raw_events
    )
    result = next(
        event.node_run_result
        for event in events
        if isinstance(event, NodeRunSucceededEvent) and event.node_id == "answer"
    )
    expected = (
        "Files: [report.txt](https://example.com/report.txt)\n"
        "![image.png](https://example.com/image.png)\n"
        "[report.txt](https://example.com/report.txt)\n"
        "[report.txt](https://example.com/report.txt)"
    )
    files = ArrayFileSegment(value=[document, image, document, document])
    assert result.outputs == {"answer": expected, "files": files}
    assert final_outputs(events) == {"answer": expected, "files": files}
    value = state.variable_pool.get(["answer", "answer"])
    assert value is not None
    assert value.to_object() == expected
    file_output = state.variable_pool.get(["answer", "files"])
    assert file_output is not None
    assert file_output.to_object() == files.value
    assert answer.get_streaming_template().segments == [
        TextSegment("Files: "),
        VariableSegment(["start", "document"]),
        TextSegment("\n"),
        VariableSegment(["start", "files"]),
        TextSegment("\n"),
        VariableSegment(["start", "document"]),
    ]
    if filtered:
        chunks = [
            event for event in events if isinstance(event, NodeRunStreamChunkEvent)
        ]
        assert "".join(event.chunk for event in chunks) == expected
