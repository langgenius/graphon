from __future__ import annotations

import base64
import json
from collections.abc import Iterator
from contextlib import contextmanager
from itertools import zip_longest
from unittest.mock import MagicMock

import pytest

from graphon.dsl import loads
from graphon.engine import Engine
from graphon.engine.filter import EngineEventFilterContext, ResponseStreamFilter
from graphon.engine.layer import Layer
from graphon.engine_events.base import EngineEvent, NodeEvent
from graphon.engine_events.graph import (
    GraphRunFailedEvent,
    GraphRunStartedEvent,
    GraphRunSucceededEvent,
)
from graphon.engine_events.node import NodeRunStreamChunkEvent
from graphon.file import File, FileTransferMethod, FileType
from graphon.file.file_manager import to_prompt_message_content
from graphon.file.protocols import WorkflowFileRuntimeProtocol
from graphon.file.runtime import use_workflow_file_runtime
from graphon.graph.graph import Graph
from graphon.model_runtime.entities.message_entities import DocumentPromptMessageContent
from graphon.nodes.base.entities import OutputVariableEntity
from graphon.nodes.base.node import Node
from graphon.nodes.base.template import Template, TextSegment
from graphon.nodes.document_extractor.entities import DocumentExtractorNodeData
from graphon.nodes.document_extractor.node import DocumentExtractorNode
from graphon.nodes.end.end_node import EndNode
from graphon.nodes.end.entities import EndNodeData
from graphon.nodes.human_input.entities import Completed, HITLContext, PauseRequested
from graphon.nodes.start.entities import StartNodeData
from graphon.nodes.start.start_node import StartNode
from graphon.runtime.init_params import InitParams
from graphon.runtime.runtime_state import RuntimeState
from tests.engine.test_runtime_state_serialization import (
    _completed_hitl,
    _hitl_engine,
    _loop_dsl,
    _new_runtime_state,
    _snapshot_after_hitl_pause,
)
from tests.helpers.workflow_events import final_outputs


def _make_file_adapter(name: str, *, send_format: str = "url") -> MagicMock:
    return MagicMock(
        multimodal_send_format=send_format,
        resolve_file_url=MagicMock(return_value=f"https://{name}.example/report.txt"),
        load_file_bytes=MagicMock(return_value=f"Document from {name}".encode()),
    )


def _make_file() -> File:
    return File(
        file_type=FileType.DOCUMENT,
        transfer_method=FileTransferMethod.LOCAL_FILE,
        reference="shared-upload",
        filename="report.txt",
        extension=".txt",
        mime_type="text/plain",
    )


def _make_file_engine(
    file: File, *, file_runtime: WorkflowFileRuntimeProtocol | None = None
) -> Engine:
    state = _new_runtime_state({"file": file})
    params = InitParams(
        workflow_id="workflow", graph_config={}, run_context={}, call_depth=0
    )
    graph = (
        Graph
        .new()
        .add_root(
            StartNode("start", StartNodeData(), init_params=params, runtime_state=state)
        )
        .add_node(
            DocumentExtractorNode(
                "extract",
                DocumentExtractorNodeData(variable_selector=["sys", "file"]),
                init_params=params,
                runtime_state=state,
            )
        )
        .add_node(
            EndNode(
                "end",
                EndNodeData(
                    outputs=[
                        OutputVariableEntity(
                            variable="text", value_selector=["extract", "text"]
                        ),
                        OutputVariableEntity(
                            variable="url", value_selector=["sys", "file", "url"]
                        ),
                    ]
                ),
                init_params=params,
                runtime_state=state,
            )
        )
        .build()
    )
    return Engine(graph, state, workers=1, file_runtime=file_runtime)


class _FileLayer(Layer):
    def __init__(self, file: File) -> None:
        super().__init__()
        self.file = file
        self.reads: list[tuple[str, str | None]] = []
        self.prompts: list[DocumentPromptMessageContent] = []

    def on_graph_start(self) -> None:
        self.reads.append(("graph-start", self.file.generate_url()))

    def on_event(self, event: EngineEvent) -> None:
        self.reads.append((type(event).__name__, self.file.generate_url()))

    def on_graph_end(self, error: Exception | None) -> None:
        _ = error
        self.reads.append(("graph-end", self.file.generate_url()))

    @contextmanager
    def node_run_context(
        self, node: Node, *, parent_execution_id: str | None = None
    ) -> Iterator[None]:
        _ = parent_execution_id
        self.reads.append((f"enter:{node.id}", self.file.generate_url()))
        try:
            yield
        finally:
            self.reads.append((f"exit:{node.id}", self.file.generate_url()))

    def on_node_run_start(self, node: Node) -> None:
        _ = node
        content = to_prompt_message_content(self.file)
        assert isinstance(content, DocumentPromptMessageContent)
        self.prompts.append(content)

    def on_node_run_end(
        self, node: Node, error: Exception | None, result_event: NodeEvent | None = None
    ) -> None:
        _ = error, result_event
        self.reads.append((f"end:{node.id}", self.file.generate_url()))


def test_interleaved_engines_retain_file_adapters_without_changing_shared_files() -> (
    None
):
    file = _make_file()
    persisted_file = file.model_dump(mode="json")
    adapters = [_make_file_adapter("a"), _make_file_adapter("b", send_format="base64")]
    engines = []
    layers = []
    for adapter in adapters:
        engine = _make_file_engine(file, file_runtime=adapter)
        layer = _FileLayer(file)
        engine.add_layer(layer)
        engines.append(engine)
        layers.append(layer)
    caller = _make_file_adapter("caller")
    with use_workflow_file_runtime(caller):
        streams = [engine.run() for engine in engines]
        events: list[list[EngineEvent]] = [[], []]
        try:
            for pair in zip_longest(*streams):
                for index, event in enumerate(pair):
                    if event is not None:
                        events[index].append(event)
                assert (
                    file.markdown == "[report.txt](https://caller.example/report.txt)"
                )
        finally:
            for stream in streams:
                stream.close()

        for name, engine_events in zip(("a", "b"), events, strict=True):
            assert final_outputs(engine_events) == {
                "text": f"Document from {name}",
                "url": f"https://{name}.example/report.txt",
            }
        for name, layer in zip(("a", "b"), layers, strict=True):
            assert {url for _, url in layer.reads} == {
                f"https://{name}.example/report.txt"
            }
            assert {kind for kind, _ in layer.reads} >= {
                "graph-start",
                "graph-end",
                "NodeRunSucceededEvent",
                "enter:extract",
                "exit:extract",
                "end:extract",
            }
        assert layers[0].prompts
        assert all(
            content.url == "https://a.example/report.txt" and not content.base64_data
            for content in layers[0].prompts
        )
        assert layers[1].prompts
        assert all(
            content.base64_data == base64.b64encode(b"Document from b").decode()
            and not content.url
            for content in layers[1].prompts
        )
        assert [engine.file_runtime for engine in engines] == adapters
        assert file.model_dump(mode="json") == persisted_file
        assert file.generate_url() == "https://caller.example/report.txt"


@pytest.mark.parametrize("configured", [False, True])
def test_engine_captures_scoped_or_unconfigured_file_runtime(configured: bool) -> None:
    file = _make_file()
    adapter = _make_file_adapter("scoped")
    if configured:
        with use_workflow_file_runtime(adapter):
            engine = _make_file_engine(file)
            engine = Engine(
                engine.graph, engine.runtime_state, workers=1, file_runtime=None
            )
    else:
        engine = _make_file_engine(file)
    with use_workflow_file_runtime(_make_file_adapter("caller")):
        if configured:
            assert final_outputs(list(engine.run())) == {
                "text": "Document from scoped",
                "url": "https://scoped.example/report.txt",
            }
        else:
            with pytest.raises(
                Exception, match="workflow file runtime is not configured"
            ):
                list(engine.run())
        assert file.generate_url() == "https://caller.example/report.txt"


@pytest.mark.parametrize("finish", ["close", "failure"])
def test_engine_restores_caller_file_runtime_after_close_or_failure(
    finish: str,
) -> None:
    file = _make_file()
    adapter = _make_file_adapter("engine")
    engine = _make_file_engine(file, file_runtime=adapter)
    layer = _FileLayer(file)
    engine.add_layer(layer)
    caller = _make_file_adapter("caller")
    with use_workflow_file_runtime(caller):
        stream = engine.run()
        try:
            assert isinstance(next(stream), GraphRunStartedEvent)
            assert file.generate_url() == "https://caller.example/report.txt"
            if finish == "failure":
                assert isinstance(
                    stream.throw(RuntimeError("file read failed")), GraphRunFailedEvent
                )
                assert file.generate_url() == "https://caller.example/report.txt"
                with pytest.raises(RuntimeError, match="file read failed"):
                    next(stream)
        finally:
            stream.close()

        assert file.generate_url() == "https://caller.example/report.txt"
        assert ("graph-end", "https://engine.example/report.txt") in layer.reads
        assert {url for _, url in layer.reads} == {"https://engine.example/report.txt"}


def test_paused_child_files_restore_with_an_explicit_engine_adapter() -> None:
    file = _make_file()
    observed_urls: list[str] = []

    def pause(context: HITLContext) -> PauseRequested:
        value = context.variable_pool.get(("sys", "file", "url"))
        assert value is not None
        observed_urls.append(value.text)
        return PauseRequested(session_id="file-approval")

    with use_workflow_file_runtime(_make_file_adapter("original")):
        engine = _hitl_engine(
            _loop_dsl(),
            runtime_state=_new_runtime_state({"file": file}),
            callback=pause,
        )
    with use_workflow_file_runtime(_make_file_adapter("caller")):
        snapshot, _ = _snapshot_after_hitl_pause(engine)
        assert observed_urls == ["https://original.example/report.txt"]
        assert "original.example" not in snapshot
        restored = RuntimeState.from_snapshot(snapshot)
        restored_file = restored.variable_pool.get_file(("sys", "file"))
        assert restored_file is not None
        assert restored_file.value.model_dump(mode="json") == file.model_dump(
            mode="json"
        )

        def complete(context: HITLContext) -> Completed:
            value = context.variable_pool.get(("sys", "file", "url"))
            assert value is not None
            return _completed_hitl(value.text)

        rebuilt = _hitl_engine(_loop_dsl(), runtime_state=restored, callback=complete)
        adapter = _make_file_adapter("restored")
        resumed = Engine(rebuilt.graph, restored, workers=1, file_runtime=adapter)
        layer = _FileLayer(restored_file.value)
        resumed.add_layer(layer)
        events = list(resumed.run())
        assert isinstance(events[-1], GraphRunSucceededEvent)
        assert restored.variable_pool.get_file(("sys", "file")) == restored_file
        assert {url for _, url in layer.reads} == {
            "https://restored.example/report.txt"
        }
        assert sum(kind == "enter:loop" for kind, _ in layer.reads) >= 2
        assert resumed.file_runtime is adapter
        assert file.generate_url() == "https://caller.example/report.txt"


@pytest.mark.parametrize("restore_before_flush", [False, True])
def test_response_filter_renders_files_with_its_engine_adapter(
    restore_before_flush: bool,
) -> None:
    file = _make_file()
    adapter = _make_file_adapter("response")
    with use_workflow_file_runtime(adapter):
        engine = loads(
            json.dumps({
                "kind": "graph",
                "graph": {
                    "nodes": [
                        {"id": "start", "data": {"type": "start", "variables": []}},
                        {
                            "id": "answer",
                            "data": {"type": "answer", "answer": "{{#sys.file#}}"},
                        },
                    ],
                    "edges": [{"source": "start", "target": "answer"}],
                },
            }),
            workers=1,
        )
        context = EngineEventFilterContext.from_engine(engine)
    with use_workflow_file_runtime(_make_file_adapter("caller")):
        stream_filter = ResponseStreamFilter()
        stream_filter.initialize(context)
        if restore_before_flush:
            list(stream_filter.on_event(GraphRunStartedEvent()))
            snapshot = stream_filter.dumps()
            stream_filter = ResponseStreamFilter()
            stream_filter.loads(snapshot)
            stream_filter.initialize(context)
        engine.runtime_state.variable_pool.add(("sys", "file"), file)
        output = list(
            stream_filter.flush()
            if restore_before_flush
            else stream_filter.on_event(GraphRunStartedEvent())
        )
        assert (
            "".join(
                event.chunk
                for event in output
                if isinstance(event, NodeRunStreamChunkEvent)
            )
            == "[report.txt](https://response.example/report.txt)"
        )
        assert context.file_runtime is adapter
        assert file.generate_url() == "https://caller.example/report.txt"


@pytest.mark.parametrize("operation", ["initialize", "loads"])
@pytest.mark.parametrize("fail", [False, True])
def test_response_template_callbacks_use_the_filter_adapter(
    monkeypatch: pytest.MonkeyPatch, operation: str, fail: bool
) -> None:
    file = _make_file()
    adapter = _make_file_adapter("response")
    engine = _make_file_engine(file, file_runtime=adapter)
    context = EngineEventFilterContext.from_engine(engine)
    stream_filter = ResponseStreamFilter()
    stream_filter.initialize(context)
    snapshot = stream_filter.dumps()
    urls: list[str | None] = []

    def get_template() -> Template:
        urls.append(file.generate_url())
        if fail:
            message = "template failed"
            raise RuntimeError(message)
        return Template(segments=[TextSegment(text=file.markdown)])

    monkeypatch.setattr(
        engine.graph.nodes["end"], "get_streaming_template", get_template
    )
    with use_workflow_file_runtime(_make_file_adapter("caller")):

        def apply_template() -> None:
            if operation == "initialize":
                stream_filter.initialize(context)
            else:
                stream_filter.loads(snapshot)

        if fail:
            with pytest.raises(RuntimeError, match="template failed"):
                apply_template()
        else:
            apply_template()
        assert urls
        assert set(urls) == {"https://response.example/report.txt"}
        assert file.generate_url() == "https://caller.example/report.txt"
