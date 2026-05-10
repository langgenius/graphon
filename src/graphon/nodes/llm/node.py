from __future__ import annotations

import base64
import io
import json
import logging
import re
import time
from collections.abc import Generator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, assert_never, override

from graphon.entities.graph_init_params import GraphInitParams
from graphon.enums import (
    BuiltinNodeTypes,
    WorkflowNodeExecutionMetadataKey,
    WorkflowNodeExecutionStatus,
)
from graphon.file import file_manager
from graphon.file.enums import FileType
from graphon.file.models import File
from graphon.http import HttpClientProtocol
from graphon.model_runtime.entities.llm_entities import (
    LLMResult,
    LLMResultChunk,
    LLMResultChunkWithStructuredOutput,
    LLMResultWithStructuredOutput,
    LLMStructuredOutput,
    LLMUsage,
)
from graphon.model_runtime.entities.message_entities import (
    AssistantPromptMessage,
    ImagePromptMessageContent,
    PromptMessage,
    PromptMessageContentType,
    PromptMessageContentUnionTypes,
    PromptMessageRole,
    SystemPromptMessage,
    TextPromptMessageContent,
    UserPromptMessage,
)
from graphon.model_runtime.entities.model_entities import ModelPropertyKey
from graphon.model_runtime.memory.prompt_message_memory import PromptMessageMemory
from graphon.model_runtime.utils.encoders import jsonable_encoder
from graphon.node_events.base import (
    NodeEventBase,
    NodeRunResult,
)
from graphon.node_events.node import (
    ModelInvokeCompletedEvent,
    RunRetrieverResourceEvent,
    StreamChunkEvent,
    StreamCompletedEvent,
)
from graphon.nodes.base.entities import VariableSelector
from graphon.nodes.base.node import Node
from graphon.nodes.base.variable_template_parser import VariableTemplateParser
from graphon.nodes.llm.runtime_protocols import (
    LLMProtocol,
    PromptMessageSerializerProtocol,
    RetrieverAttachmentLoaderProtocol,
)
from graphon.prompt_entities import MemoryConfig
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from graphon.runtime.variable_pool import VariablePool
from graphon.template_rendering import Jinja2TemplateRenderer, TemplateRenderError
from graphon.variables.segments import (
    ArrayFileSegment,
    ArraySegment,
    FileSegment,
    NoneSegment,
    ObjectSegment,
    StringSegment,
)

from . import llm_utils
from .entities import (
    LLMNodeChatModelMessage,
    LLMNodeCompletionModelPromptTemplate,
    LLMNodeData,
)
from .exc import (
    InvalidContextStructureError,
    InvalidVariableTypeError,
    LLMNodeError,
    MemoryRolePrefixRequiredError,
    NoPromptFoundError,
    TemplateTypeNotSupportError,
    VariableNotFoundError,
)
from .file_saver import LLMFileSaver

logger = logging.getLogger(__name__)


@dataclass
class _CollectedRunContext:
    context: str | None = None
    context_files: list[File] = field(default_factory=list)


@dataclass
class _PreparedRunPrompt:
    prompt_messages: Sequence[PromptMessage] = field(default_factory=tuple)
    stop: Sequence[str] | None = None
    model_instance: LLMProtocol | None = None


class LLMNode(Node[LLMNodeData]):
    node_type = BuiltinNodeTypes.LLM

    # Compiled regex for extracting <think> blocks (with compatibility for attributes)
    _THINK_PATTERN = re.compile(r"<think[^>]*>(.*?)</think>", re.IGNORECASE | re.DOTALL)

    # Instance attributes specific to LLMNode.
    # Output variable for file
    _file_outputs: list[File]

    _llm_file_saver: LLMFileSaver
    _retriever_attachment_loader: RetrieverAttachmentLoaderProtocol | None
    _prompt_message_serializer: PromptMessageSerializerProtocol
    _jinja2_template_renderer: Jinja2TemplateRenderer | None
    _model_instance: LLMProtocol
    _memory: PromptMessageMemory | None
    _default_query_selector: tuple[str, ...] | None

    @override
    def __init__(
        self,
        node_id: str,
        data: LLMNodeData,
        *,
        graph_init_params: GraphInitParams,
        graph_runtime_state: GraphRuntimeState,
        credentials_provider: object | None = None,
        model_factory: object | None = None,
        model_instance: LLMProtocol,
        http_client: HttpClientProtocol | None = None,
        memory: PromptMessageMemory | None = None,
        llm_file_saver: LLMFileSaver,
        prompt_message_serializer: PromptMessageSerializerProtocol,
        retriever_attachment_loader: RetrieverAttachmentLoaderProtocol | None = None,
        jinja2_template_renderer: Jinja2TemplateRenderer | None = None,
        default_query_selector: Sequence[str] | None = None,
    ) -> None:
        super().__init__(
            node_id=node_id,
            data=data,
            graph_init_params=graph_init_params,
            graph_runtime_state=graph_runtime_state,
        )
        # LLM file outputs, used for MultiModal outputs.
        self._file_outputs = []

        _ = credentials_provider, model_factory, http_client
        self._model_instance = model_instance
        self._memory = memory

        self._llm_file_saver = llm_file_saver
        self._prompt_message_serializer = prompt_message_serializer
        self._retriever_attachment_loader = retriever_attachment_loader
        self._jinja2_template_renderer = jinja2_template_renderer
        self._default_query_selector = (
            tuple(default_query_selector)
            if default_query_selector is not None
            else None
        )

    @classmethod
    @override
    def version(cls) -> str:
        return "1"

    @override
    def _run(self) -> Generator:
        node_inputs: dict[str, Any] = {}
        process_data: dict[str, Any] = {}
        usage_holder = {"value": LLMUsage.empty_usage()}

        try:
            prepared_prompt = _PreparedRunPrompt()
            yield from self._prepare_run_prompt(
                node_inputs=node_inputs,
                prepared_prompt=prepared_prompt,
            )
            model_instance = self._require_model_instance(
                prepared_prompt=prepared_prompt,
            )

            yield from self._yield_run_completion(
                node_inputs=node_inputs,
                process_data=process_data,
                usage_holder=usage_holder,
                prompt_messages=prepared_prompt.prompt_messages,
                stop=prepared_prompt.stop,
                model_provider=model_instance.provider,
                model_name=model_instance.model_name,
            )
        except ValueError as exc:
            yield StreamCompletedEvent(
                node_run_result=NodeRunResult(
                    status=WorkflowNodeExecutionStatus.FAILED,
                    error=str(exc),
                    inputs=node_inputs,
                    process_data=process_data,
                    error_type=type(exc).__name__,
                    llm_usage=usage_holder["value"],
                ),
            )
        except Exception as exc:
            logger.exception("error while executing llm node")
            yield StreamCompletedEvent(
                node_run_result=NodeRunResult(
                    status=WorkflowNodeExecutionStatus.FAILED,
                    error=str(exc),
                    inputs=node_inputs,
                    process_data=process_data,
                    error_type=type(exc).__name__,
                    llm_usage=usage_holder["value"],
                ),
            )

    def _prepare_run_prompt(
        self,
        *,
        node_inputs: dict[str, Any],
        prepared_prompt: _PreparedRunPrompt,
    ) -> Generator[
        NodeEventBase,
        None,
        None,
    ]:
        self.node_data.prompt_template = self._transform_chat_messages(
            self.node_data.prompt_template,
        )
        inputs = self._fetch_inputs(node_data=self.node_data)
        inputs.update(self._fetch_jinja_inputs(node_data=self.node_data))

        files = (
            llm_utils.fetch_files(
                variable_pool=self.graph_runtime_state.variable_pool,
                selector=self.node_data.vision.configs.variable_selector,
            )
            if self.node_data.vision.enabled
            else []
        )
        if files:
            node_inputs["#files#"] = [file.to_dict() for file in files]

        collected_context = _CollectedRunContext()
        yield from self._collect_run_context(
            node_inputs=node_inputs,
            collected_context=collected_context,
        )
        model_instance = self._prepare_model_instance()
        node_inputs.update(
            llm_utils.build_model_identity_inputs(model_instance=model_instance),
        )
        prompt_messages, stop = LLMNode.fetch_prompt_messages(
            sys_query=self._resolve_memory_query(),
            sys_files=files,
            context=collected_context.context or "",
            memory=self._memory,
            model_instance=model_instance,
            stop=model_instance.stop,
            prompt_template=self.node_data.prompt_template,
            memory_config=self.node_data.memory,
            vision_enabled=self.node_data.vision.enabled,
            vision_detail=self.node_data.vision.configs.detail,
            variable_pool=self.graph_runtime_state.variable_pool,
            jinja2_variables=self.node_data.prompt_config.jinja2_variables,
            context_files=collected_context.context_files,
            jinja2_template_renderer=self._jinja2_template_renderer,
        )
        prepared_prompt.prompt_messages = prompt_messages
        prepared_prompt.stop = stop
        prepared_prompt.model_instance = model_instance

    @staticmethod
    def _require_model_instance(
        *,
        prepared_prompt: _PreparedRunPrompt,
    ) -> LLMProtocol:
        if prepared_prompt.model_instance is None:
            msg = "model instance was not prepared"
            raise AssertionError(msg)
        return prepared_prompt.model_instance

    def _collect_run_context(
        self,
        *,
        node_inputs: dict[str, Any],
        collected_context: _CollectedRunContext,
    ) -> Generator[NodeEventBase, None, None]:
        context_generator = self._fetch_context(node_data=self.node_data)
        if context_generator is not None:
            for event in context_generator:
                collected_context.context = event.context
                collected_context.context_files = event.context_files or []
                yield event

        if collected_context.context:
            node_inputs["#context#"] = collected_context.context
        if collected_context.context_files:
            node_inputs["#context_files#"] = [
                file.model_dump() for file in collected_context.context_files
            ]

    def _prepare_model_instance(self) -> LLMProtocol:
        model_instance = self._model_instance
        model_instance.parameters = llm_utils.resolve_completion_params_variables(
            model_instance.parameters,
            self.graph_runtime_state.variable_pool,
        )
        return model_instance

    def _resolve_memory_query(self) -> str | None:
        if not self.node_data.memory:
            return None

        query = self.node_data.memory.query_prompt_template
        if query:
            return query
        if not self._default_query_selector:
            return None

        query_variable = self.graph_runtime_state.variable_pool.get(
            self._default_query_selector,
        )
        return query_variable.text if query_variable else None

    def _yield_run_completion(
        self,
        *,
        node_inputs: dict[str, Any],
        process_data: dict[str, Any],
        usage_holder: dict[str, LLMUsage],
        prompt_messages: Sequence[PromptMessage],
        stop: Sequence[str] | None,
        model_provider: Any,
        model_name: str,
    ) -> Generator[NodeEventBase, None, None]:
        generator = LLMNode.invoke_llm(
            model_instance=self._model_instance,
            prompt_messages=prompt_messages,
            stop=stop,
            structured_output_enabled=self.node_data.structured_output_enabled,
            structured_output=self.node_data.structured_output,
            file_saver=self._llm_file_saver,
            file_outputs=self._file_outputs,
            node_id=self._node_id,
            reasoning_format=self.node_data.reasoning_format,
        )
        usage = LLMUsage.empty_usage()
        finish_reason = None
        reasoning_content = ""
        clean_text = ""
        structured_output: LLMStructuredOutput | None = None

        for event in generator:
            if isinstance(event, StreamChunkEvent):
                yield event
                continue

            if isinstance(event, LLMStructuredOutput):
                structured_output = event
                continue

            if not isinstance(event, ModelInvokeCompletedEvent):
                continue

            usage = event.usage
            usage_holder["value"] = usage
            finish_reason = event.finish_reason
            reasoning_content = event.reasoning_content or ""
            clean_text = self._extract_clean_text(event.text)
            if event.structured_output:
                structured_output = LLMStructuredOutput(
                    structured_output=event.structured_output,
                )
            break

        node_inputs.update(
            llm_utils.build_model_identity_inputs(model_instance=self._model_instance),
        )
        process_data.update(
            self._build_process_data(
                prompt_messages=prompt_messages,
                usage=usage,
                finish_reason=finish_reason,
                model_provider=model_provider,
                model_name=model_name,
            ),
        )
        outputs = self._build_run_outputs(
            clean_text=clean_text,
            usage=usage,
            finish_reason=finish_reason,
            reasoning_content=reasoning_content,
            structured_output=structured_output,
        )
        yield StreamChunkEvent(
            selector=[self._node_id, "text"],
            chunk="",
            is_final=True,
        )
        yield StreamCompletedEvent(
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.SUCCEEDED,
                inputs=node_inputs,
                process_data=process_data,
                outputs=outputs,
                metadata={
                    WorkflowNodeExecutionMetadataKey.TOTAL_TOKENS: usage.total_tokens,
                    WorkflowNodeExecutionMetadataKey.TOTAL_PRICE: usage.total_price,
                    WorkflowNodeExecutionMetadataKey.CURRENCY: usage.currency,
                },
                llm_usage=usage,
            ),
        )

    def _extract_clean_text(self, text: str) -> str:
        if self.node_data.reasoning_format == "tagged":
            return text

        clean_text, _ = LLMNode._split_reasoning(
            text,
            self.node_data.reasoning_format,
        )
        return clean_text

    def _build_process_data(
        self,
        *,
        prompt_messages: Sequence[PromptMessage],
        usage: LLMUsage,
        finish_reason: str | None,
        model_provider: Any,
        model_name: str,
    ) -> dict[str, Any]:
        return {
            "model_mode": self.node_data.model.mode,
            "prompts": self._prompt_message_serializer.serialize(
                model_mode=self.node_data.model.mode,
                prompt_messages=prompt_messages,
            ),
            "usage": jsonable_encoder(usage),
            "finish_reason": finish_reason,
            "model_provider": model_provider,
            "model_name": model_name,
        }

    def _build_run_outputs(
        self,
        *,
        clean_text: str,
        usage: LLMUsage,
        finish_reason: str | None,
        reasoning_content: str,
        structured_output: LLMStructuredOutput | None,
    ) -> dict[str, Any]:
        outputs = {
            "text": clean_text,
            "reasoning_content": reasoning_content,
            "usage": jsonable_encoder(usage),
            "finish_reason": finish_reason,
        }
        if structured_output:
            outputs["structured_output"] = structured_output.structured_output
        if self._file_outputs:
            outputs["files"] = ArrayFileSegment(value=self._file_outputs)
        return outputs

    @staticmethod
    def invoke_llm(
        *,
        model_instance: LLMProtocol,
        prompt_messages: Sequence[PromptMessage],
        stop: Sequence[str] | None = None,
        structured_output_enabled: bool,
        structured_output: Mapping[str, Any] | None = None,
        file_saver: LLMFileSaver,
        file_outputs: list[File],
        node_id: str,
        reasoning_format: Literal["separated", "tagged"] = "tagged",
    ) -> Generator[NodeEventBase | LLMStructuredOutput, None, None]:
        model_parameters = model_instance.parameters
        invoke_model_parameters = dict(model_parameters)
        invoke_result: LLMResult | Generator[LLMResultChunk, None, None]
        if structured_output_enabled:
            output_schema = LLMNode.fetch_structured_output_schema(
                structured_output=structured_output or {},
            )
            request_start_time = time.perf_counter()

            invoke_result = model_instance.invoke_llm_with_structured_output(
                prompt_messages=prompt_messages,
                json_schema=output_schema,
                model_parameters=invoke_model_parameters,
                stop=stop,
                stream=True,
            )
        else:
            request_start_time = time.perf_counter()

            invoke_result = model_instance.invoke_llm(
                prompt_messages=prompt_messages,
                model_parameters=invoke_model_parameters,
                tools=None,
                stop=stop,
                stream=True,
            )

        return LLMNode.handle_invoke_result(
            invoke_result=invoke_result,
            file_saver=file_saver,
            file_outputs=file_outputs,
            node_id=node_id,
            model_instance=model_instance,
            reasoning_format=reasoning_format,
            request_start_time=request_start_time,
        )

    @staticmethod
    def handle_invoke_result(
        *,
        invoke_result: LLMResult
        | Generator[LLMResultChunk | LLMStructuredOutput, None, None],
        file_saver: LLMFileSaver,
        file_outputs: list[File],
        node_id: str,
        model_instance: LLMProtocol,
        reasoning_format: Literal["separated", "tagged"] = "tagged",
        request_start_time: float | None = None,
    ) -> Generator[NodeEventBase | LLMStructuredOutput, None, None]:
        if isinstance(invoke_result, LLMResult):
            yield from LLMNode._yield_blocking_invoke_result(
                invoke_result=invoke_result,
                file_saver=file_saver,
                file_outputs=file_outputs,
                reasoning_format=reasoning_format,
                request_start_time=request_start_time,
            )
            return

        yield from LLMNode._yield_streaming_invoke_result(
            invoke_result=invoke_result,
            file_saver=file_saver,
            file_outputs=file_outputs,
            node_id=node_id,
            model_instance=model_instance,
            reasoning_format=reasoning_format,
            request_start_time=request_start_time,
        )

    @staticmethod
    def _yield_blocking_invoke_result(
        *,
        invoke_result: LLMResult,
        file_saver: LLMFileSaver,
        file_outputs: list[File],
        reasoning_format: Literal["separated", "tagged"] = "tagged",
        request_start_time: float | None = None,
    ) -> Generator[ModelInvokeCompletedEvent, None, None]:
        duration = None
        if request_start_time is not None:
            duration = time.perf_counter() - request_start_time
            invoke_result.usage.latency = round(duration, 3)

        yield LLMNode.handle_blocking_result(
            invoke_result=invoke_result,
            saver=file_saver,
            file_outputs=file_outputs,
            reasoning_format=reasoning_format,
            request_latency=duration,
        )

    @staticmethod
    def _yield_streaming_invoke_result(
        *,
        invoke_result: Generator[LLMResultChunk | LLMStructuredOutput, None, None],
        file_saver: LLMFileSaver,
        file_outputs: list[File],
        node_id: str,
        model_instance: LLMProtocol,
        reasoning_format: Literal["separated", "tagged"] = "tagged",
        request_start_time: float | None = None,
    ) -> Generator[NodeEventBase | LLMStructuredOutput, None, None]:
        model = ""
        usage = LLMUsage.empty_usage()
        finish_reason = None
        full_text_buffer = io.StringIO()

        # Initialize streaming metrics tracking
        start_time = (
            request_start_time
            if request_start_time is not None
            else time.perf_counter()
        )
        first_token_time = None
        has_content = False

        collected_structured_output = (
            None  # Collect structured_output from streaming chunks
        )
        # Consume the invoke result and handle generator exception
        try:
            for result in invoke_result:
                if isinstance(result, LLMResultChunkWithStructuredOutput):
                    # Collect structured_output from the chunk
                    if result.structured_output is not None:
                        collected_structured_output = dict(result.structured_output)
                    yield result
                if isinstance(result, LLMResultChunk):
                    contents = result.delta.message.content
                    for (
                        text_part
                    ) in LLMNode._save_multimodal_output_and_convert_result_to_markdown(
                        contents=contents,
                        file_saver=file_saver,
                        file_outputs=file_outputs,
                    ):
                        # Detect first token for TTFT calculation
                        if text_part and not has_content:
                            first_token_time = time.perf_counter()
                            has_content = True

                        full_text_buffer.write(text_part)
                        yield StreamChunkEvent(
                            selector=[node_id, "text"],
                            chunk=text_part,
                            is_final=False,
                        )

                    model, usage, finish_reason = LLMNode._update_streaming_metadata(
                        result=result,
                        model=model,
                        usage=usage,
                        finish_reason=finish_reason,
                    )
        except Exception as e:
            is_structured_output_parse_error = getattr(
                model_instance,
                "is_structured_output_parse_error",
                None,
            )
            if callable(is_structured_output_parse_error) and (
                is_structured_output_parse_error(e)
            ):
                msg = f"Failed to parse structured output: {e}"
                raise LLMNodeError(msg) from e
            if type(e).__name__ == "OutputParserError":
                msg = f"Failed to parse structured output: {e}"
                raise LLMNodeError(msg) from e
            raise

        # Extract reasoning content from <think> tags in the main text
        full_text = full_text_buffer.getvalue()
        clean_text, reasoning_content = LLMNode._extract_stream_reasoning(
            full_text=full_text,
            reasoning_format=reasoning_format,
        )
        LLMNode._finalize_streaming_usage(
            usage=usage,
            has_content=has_content,
            first_token_time=first_token_time,
            start_time=start_time,
        )

        yield ModelInvokeCompletedEvent(
            # Use clean_text for separated mode, full_text for tagged mode
            text=clean_text if reasoning_format == "separated" else full_text,
            usage=usage,
            finish_reason=finish_reason,
            # Reasoning content for workflow variables and downstream nodes
            reasoning_content=reasoning_content,
            # Pass structured output if collected from streaming chunks
            structured_output=collected_structured_output,
        )

    @staticmethod
    def _update_streaming_metadata(
        *,
        result: LLMResultChunk,
        model: str,
        usage: LLMUsage,
        finish_reason: str | None,
    ) -> tuple[str, LLMUsage, str | None]:
        if not model and result.model:
            model = result.model
        if usage.prompt_tokens == 0 and result.delta.usage:
            usage = result.delta.usage
        if finish_reason is None and result.delta.finish_reason:
            finish_reason = result.delta.finish_reason
        return model, usage, finish_reason

    @staticmethod
    def _extract_stream_reasoning(
        *,
        full_text: str,
        reasoning_format: Literal["separated", "tagged"],
    ) -> tuple[str, str]:
        if reasoning_format == "tagged":
            return full_text, ""
        return LLMNode._split_reasoning(full_text, reasoning_format)

    @staticmethod
    def _finalize_streaming_usage(
        *,
        usage: LLMUsage,
        has_content: bool,
        first_token_time: float | None,
        start_time: float,
    ) -> None:
        end_time = time.perf_counter()
        total_duration = end_time - start_time
        usage.latency = round(total_duration, 3)
        if not has_content or first_token_time is None:
            return

        gen_ai_server_time_to_first_token = first_token_time - start_time
        llm_streaming_time_to_generate = end_time - first_token_time
        usage.time_to_first_token = round(gen_ai_server_time_to_first_token, 3)
        usage.time_to_generate = round(llm_streaming_time_to_generate, 3)

    @staticmethod
    def _image_file_to_markdown(file: File, /) -> str:
        return f"![]({file.generate_url()})"

    @classmethod
    def _split_reasoning(
        cls,
        text: str,
        reasoning_format: Literal["separated", "tagged"] = "tagged",
    ) -> tuple[str, str]:
        """Split reasoning content from text based on reasoning_format strategy.

        Args:
            text: Full text that may contain <think> blocks
            reasoning_format: Strategy for handling reasoning content
                - "separated": Remove <think> tags and return clean text
                plus reasoning_content field
                - "tagged": Keep <think> tags in text, return empty reasoning_content

        Returns:
            tuple of (clean_text, reasoning_content)

        """
        if reasoning_format == "tagged":
            return text, ""

        # Find all <think>...</think> blocks (case-insensitive)
        matches = cls._THINK_PATTERN.findall(text)

        # Extract reasoning content from all <think> blocks
        reasoning_content = (
            "\n".join(match.strip() for match in matches) if matches else ""
        )

        # Remove all <think>...</think> blocks from original text
        clean_text = cls._THINK_PATTERN.sub("", text)

        # Clean up extra whitespace
        clean_text = re.sub(r"\n\s*\n", "\n\n", clean_text).strip()

        # Separated mode: always return clean text and reasoning_content
        return clean_text, reasoning_content or ""

    def _transform_chat_messages(
        self,
        messages: Sequence[LLMNodeChatModelMessage]
        | LLMNodeCompletionModelPromptTemplate,
        /,
    ) -> Sequence[LLMNodeChatModelMessage] | LLMNodeCompletionModelPromptTemplate:
        if isinstance(messages, LLMNodeCompletionModelPromptTemplate):
            if messages.edition_type == "jinja2" and messages.jinja2_text:
                messages.text = messages.jinja2_text

            return messages

        for message in messages:
            if message.edition_type == "jinja2" and message.jinja2_text:
                message.text = message.jinja2_text

        return messages

    def _fetch_jinja_inputs(self, node_data: LLMNodeData) -> dict[str, str]:
        if not node_data.prompt_config:
            return {}

        variables: dict[str, str] = {}
        for variable_selector in node_data.prompt_config.jinja2_variables or []:
            variable = self._get_required_variable(variable_selector)
            variables[variable_selector.variable] = self._stringify_jinja_variable(
                variable,
            )
        return variables

    def _fetch_inputs(self, node_data: LLMNodeData) -> dict[str, Any]:
        inputs: dict[str, Any] = {}
        self._collect_input_variables(
            inputs=inputs,
            variable_selectors=self._extract_prompt_input_variable_selectors(
                prompt_template=node_data.prompt_template,
            ),
        )
        self._collect_input_variables(
            inputs=inputs,
            variable_selectors=self._extract_memory_query_variable_selectors(
                node_data.memory,
            ),
            skip_none=True,
        )
        return inputs

    def _fetch_context(
        self,
        node_data: LLMNodeData,
    ) -> Generator[RunRetrieverResourceEvent, None, None]:
        context_value_variable = self._get_context_value_variable(node_data)
        if context_value_variable is None:
            return

        if isinstance(context_value_variable, StringSegment):
            yield RunRetrieverResourceEvent(
                retriever_resources=[],
                context=context_value_variable.value,
                context_files=[],
            )
            return

        if not isinstance(context_value_variable, ArraySegment):
            return

        yield self._build_array_context_event(context_value_variable)

    def _get_required_variable(self, variable_selector: VariableSelector) -> Any:
        variable = self.graph_runtime_state.variable_pool.get(
            variable_selector.value_selector,
        )
        if variable is None:
            msg = f"Variable {variable_selector.variable} not found"
            raise VariableNotFoundError(msg)
        return variable

    @staticmethod
    def _stringify_context_mapping(input_dict: Mapping[str, Any]) -> str:
        if (
            "metadata" in input_dict
            and "_source" in input_dict["metadata"]
            and "content" in input_dict
        ):
            return str(input_dict["content"])
        try:
            return json.dumps(input_dict, ensure_ascii=False)
        except (TypeError, ValueError, OverflowError):
            return str(input_dict)

    @classmethod
    def _stringify_jinja_variable(cls, variable: Any) -> str:
        if isinstance(variable, ArraySegment):
            result = ""
            for item in variable.value:
                result += (
                    cls._stringify_context_mapping(item)
                    if isinstance(item, dict)
                    else str(item)
                )
                result += "\n"
            return result.strip()
        if isinstance(variable, ObjectSegment):
            return cls._stringify_context_mapping(variable.value)
        return variable.text

    def _collect_input_variables(
        self,
        *,
        inputs: dict[str, Any],
        variable_selectors: Sequence[VariableSelector],
        skip_none: bool = False,
    ) -> None:
        for variable_selector in variable_selectors:
            variable = self._get_required_variable(variable_selector)
            if isinstance(variable, NoneSegment):
                if skip_none:
                    continue
                inputs[variable_selector.variable] = ""
            inputs[variable_selector.variable] = variable.to_object()

    @staticmethod
    def _extract_memory_query_variable_selectors(
        memory: MemoryConfig | None,
    ) -> list[VariableSelector]:
        if not memory or not memory.query_prompt_template:
            return []
        return VariableTemplateParser(
            template=memory.query_prompt_template,
        ).extract_variable_selectors()

    @staticmethod
    def _extract_prompt_input_variable_selectors(
        *,
        prompt_template: Sequence[LLMNodeChatModelMessage]
        | LLMNodeCompletionModelPromptTemplate,
    ) -> list[VariableSelector]:
        if isinstance(prompt_template, list):
            return [
                variable_selector
                for prompt in prompt_template
                for variable_selector in VariableTemplateParser(
                    template=prompt.text,
                ).extract_variable_selectors()
            ]
        if isinstance(prompt_template, LLMNodeCompletionModelPromptTemplate):
            return VariableTemplateParser(
                template=prompt_template.text,
            ).extract_variable_selectors()

        msg = f"Invalid prompt template type: {type(prompt_template)}"
        raise InvalidVariableTypeError(msg)

    def _get_context_value_variable(self, node_data: LLMNodeData) -> Any | None:
        if not node_data.context.enabled or not node_data.context.variable_selector:
            return None
        return self.graph_runtime_state.variable_pool.get(
            node_data.context.variable_selector,
        )

    def _build_array_context_event(
        self,
        context_value_variable: ArraySegment,
    ) -> RunRetrieverResourceEvent:
        context = ""
        retriever_resources: list[dict[str, Any]] = []
        context_files: list[File] = []
        for item in context_value_variable.value:
            text_part, retriever_resource = self._parse_context_item(item)
            context += text_part
            if retriever_resource is None:
                continue
            retriever_resources.append(retriever_resource)
            context_files.extend(self._load_context_files(retriever_resource))
        return RunRetrieverResourceEvent(
            retriever_resources=retriever_resources,
            context=context.strip(),
            context_files=context_files,
        )

    def _parse_context_item(
        self,
        item: Any,
    ) -> tuple[str, dict[str, Any] | None]:
        if isinstance(item, str):
            return f"{item}\n", None
        if "content" not in item:
            msg = f"Invalid context structure: {item}"
            raise InvalidContextStructureError(msg)

        context = ""
        if item.get("summary"):
            context += f"{item['summary']}\n"
        context += f"{item['content']}\n"
        retriever_resource = self._convert_to_original_retriever_resource(item)
        return context, retriever_resource

    def _load_context_files(
        self,
        retriever_resource: dict[str, Any],
    ) -> Sequence[File]:
        segment_id = retriever_resource.get("segment_id")
        if not segment_id or self._retriever_attachment_loader is None:
            return []
        return self._retriever_attachment_loader.load(segment_id=segment_id)

    def _convert_to_original_retriever_resource(
        self,
        context_dict: dict,
    ) -> dict[str, Any] | None:
        if (
            "metadata" in context_dict
            and "_source" in context_dict["metadata"]
            and context_dict["metadata"]["_source"] == "knowledge"
        ):
            metadata = context_dict.get("metadata", {})

            return {
                "position": metadata.get("position"),
                "dataset_id": metadata.get("dataset_id"),
                "dataset_name": metadata.get("dataset_name"),
                "document_id": metadata.get("document_id"),
                "document_name": metadata.get("document_name"),
                "data_source_type": metadata.get("data_source_type"),
                "segment_id": metadata.get("segment_id"),
                "retriever_from": metadata.get("retriever_from"),
                "score": metadata.get("score"),
                "hit_count": metadata.get("segment_hit_count"),
                "word_count": metadata.get("segment_word_count"),
                "segment_position": metadata.get("segment_position"),
                "index_node_hash": metadata.get("segment_index_node_hash"),
                "content": context_dict.get("content"),
                "page": metadata.get("page"),
                "doc_metadata": metadata.get("doc_metadata"),
                "files": context_dict.get("files"),
                "summary": context_dict.get("summary"),
            }

        return None

    @staticmethod
    def fetch_prompt_messages(
        *,
        sys_query: str | None = None,
        sys_files: Sequence[File],
        context: str = "",
        memory: PromptMessageMemory | None = None,
        model_instance: LLMProtocol,
        prompt_template: Sequence[LLMNodeChatModelMessage]
        | LLMNodeCompletionModelPromptTemplate,
        stop: Sequence[str] | None = None,
        memory_config: MemoryConfig | None = None,
        vision_enabled: bool = False,
        vision_detail: ImagePromptMessageContent.DETAIL,
        variable_pool: VariablePool,
        jinja2_variables: Sequence[VariableSelector],
        context_files: list[File] | None = None,
        jinja2_template_renderer: Jinja2TemplateRenderer | None = None,
    ) -> tuple[Sequence[PromptMessage], Sequence[str] | None]:
        model_schema = llm_utils.fetch_model_schema(model_instance=model_instance)
        prompt_messages = LLMNode._build_prompt_messages_from_template(
            sys_query=sys_query,
            context=context,
            memory=memory,
            model_instance=model_instance,
            prompt_template=prompt_template,
            memory_config=memory_config,
            vision_detail=vision_detail,
            variable_pool=variable_pool,
            jinja2_variables=jinja2_variables,
            jinja2_template_renderer=jinja2_template_renderer,
        )
        LLMNode._append_prompt_files(
            prompt_messages=prompt_messages,
            files=sys_files,
            vision_enabled=vision_enabled,
            vision_detail=vision_detail,
        )
        LLMNode._append_prompt_files(
            prompt_messages=prompt_messages,
            files=context_files,
            vision_enabled=vision_enabled,
            vision_detail=vision_detail,
        )
        filtered_prompt_messages = LLMNode._filter_prompt_messages(
            prompt_messages=prompt_messages,
            model_schema=model_schema,
        )

        if len(filtered_prompt_messages) == 0:
            msg = (
                "No prompt found in the LLM configuration. "
                "Please ensure a prompt is properly configured before proceeding."
            )
            raise NoPromptFoundError(msg)

        return filtered_prompt_messages, stop

    @staticmethod
    def _build_prompt_messages_from_template(
        *,
        sys_query: str | None,
        context: str,
        memory: PromptMessageMemory | None,
        model_instance: LLMProtocol,
        prompt_template: Sequence[LLMNodeChatModelMessage]
        | LLMNodeCompletionModelPromptTemplate,
        memory_config: MemoryConfig | None,
        vision_detail: ImagePromptMessageContent.DETAIL,
        variable_pool: VariablePool,
        jinja2_variables: Sequence[VariableSelector],
        jinja2_template_renderer: Jinja2TemplateRenderer | None = None,
    ) -> list[PromptMessage]:
        if isinstance(prompt_template, list):
            return LLMNode._build_chat_prompt_messages(
                messages=prompt_template,
                sys_query=sys_query,
                context=context,
                memory=memory,
                memory_config=memory_config,
                model_instance=model_instance,
                vision_detail=vision_detail,
                variable_pool=variable_pool,
                jinja2_variables=jinja2_variables,
                jinja2_template_renderer=jinja2_template_renderer,
            )

        if isinstance(prompt_template, LLMNodeCompletionModelPromptTemplate):
            return LLMNode._build_completion_prompt_messages(
                sys_query=sys_query,
                context=context,
                memory=memory,
                model_instance=model_instance,
                prompt_template=prompt_template,
                memory_config=memory_config,
                variable_pool=variable_pool,
                jinja2_variables=jinja2_variables,
                jinja2_template_renderer=jinja2_template_renderer,
            )

        raise TemplateTypeNotSupportError(type_name=str(type(prompt_template)))

    @staticmethod
    def _build_chat_prompt_messages(
        *,
        messages: Sequence[LLMNodeChatModelMessage],
        sys_query: str | None,
        context: str,
        memory: PromptMessageMemory | None,
        memory_config: MemoryConfig | None,
        model_instance: LLMProtocol,
        vision_detail: ImagePromptMessageContent.DETAIL,
        variable_pool: VariablePool,
        jinja2_variables: Sequence[VariableSelector],
        jinja2_template_renderer: Jinja2TemplateRenderer | None = None,
    ) -> list[PromptMessage]:
        prompt_messages = list(
            LLMNode.handle_list_messages(
                messages=messages,
                context=context,
                jinja2_variables=jinja2_variables,
                variable_pool=variable_pool,
                vision_detail_config=vision_detail,
                jinja2_template_renderer=jinja2_template_renderer,
            ),
        )
        prompt_messages.extend(
            _handle_memory_chat_mode(
                memory=memory,
                memory_config=memory_config,
                model_instance=model_instance,
            ),
        )
        if not sys_query:
            return prompt_messages

        query_message = LLMNodeChatModelMessage(
            text=sys_query,
            role=PromptMessageRole.USER,
            edition_type="basic",
        )
        prompt_messages.extend(
            LLMNode.handle_list_messages(
                messages=[query_message],
                context="",
                jinja2_variables=[],
                variable_pool=variable_pool,
                vision_detail_config=vision_detail,
                jinja2_template_renderer=jinja2_template_renderer,
            ),
        )
        return prompt_messages

    @staticmethod
    def _build_completion_prompt_messages(
        *,
        sys_query: str | None,
        context: str,
        memory: PromptMessageMemory | None,
        model_instance: LLMProtocol,
        prompt_template: LLMNodeCompletionModelPromptTemplate,
        memory_config: MemoryConfig | None,
        variable_pool: VariablePool,
        jinja2_variables: Sequence[VariableSelector],
        jinja2_template_renderer: Jinja2TemplateRenderer | None = None,
    ) -> list[PromptMessage]:
        prompt_messages = list(
            _handle_completion_template(
                template=prompt_template,
                context=context,
                jinja2_variables=jinja2_variables,
                variable_pool=variable_pool,
                jinja2_template_renderer=jinja2_template_renderer,
            ),
        )
        memory_text = _handle_memory_completion_mode(
            memory=memory,
            memory_config=memory_config,
            model_instance=model_instance,
        )
        LLMNode._merge_completion_memory(prompt_messages[0], memory_text)
        if sys_query:
            LLMNode._merge_completion_query(prompt_messages[0], sys_query)
        return prompt_messages

    @staticmethod
    def _merge_completion_memory(
        prompt_message: PromptMessage,
        memory_text: str,
    ) -> None:
        prompt_content = prompt_message.content
        if isinstance(prompt_content, str):
            prompt_content = str(prompt_content)
            if "#histories#" in prompt_content:
                prompt_content = prompt_content.replace("#histories#", memory_text)
            else:
                prompt_content = memory_text + "\n" + prompt_content
            prompt_message.content = prompt_content
            return

        if isinstance(prompt_content, list):
            for content_item in prompt_content:
                if not isinstance(content_item, TextPromptMessageContent):
                    continue
                if "#histories#" in content_item.data:
                    content_item.data = content_item.data.replace(
                        "#histories#",
                        memory_text,
                    )
                else:
                    content_item.data = memory_text + "\n" + content_item.data
            return

        msg = "Invalid prompt content type"
        raise TypeError(msg)

    @staticmethod
    def _merge_completion_query(prompt_message: PromptMessage, sys_query: str) -> None:
        prompt_content = prompt_message.content
        if isinstance(prompt_content, str):
            prompt_message.content = str(prompt_content).replace(
                "#sys.query#",
                sys_query,
            )
            return

        if isinstance(prompt_content, list):
            for content_item in prompt_content:
                if isinstance(content_item, TextPromptMessageContent):
                    content_item.data = sys_query + "\n" + content_item.data
            return

        msg = "Invalid prompt content type"
        raise TypeError(msg)

    @staticmethod
    def _append_prompt_files(
        *,
        prompt_messages: list[PromptMessage],
        files: Sequence[File] | None,
        vision_enabled: bool,
        vision_detail: ImagePromptMessageContent.DETAIL,
    ) -> None:
        if not vision_enabled or not files:
            return

        file_prompts = [
            file_manager.to_prompt_message_content(
                file,
                image_detail_config=vision_detail,
            )
            for file in files
        ]
        if (
            prompt_messages
            and isinstance(prompt_messages[-1], UserPromptMessage)
            and isinstance(prompt_messages[-1].content, list)
        ):
            prompt_messages[-1] = UserPromptMessage(
                content=file_prompts + prompt_messages[-1].content,
            )
            return

        prompt_messages.append(UserPromptMessage(content=file_prompts))

    @staticmethod
    def _filter_prompt_messages(
        *,
        prompt_messages: Sequence[PromptMessage],
        model_schema: Any,
    ) -> list[PromptMessage]:
        filtered_prompt_messages = []
        for prompt_message in prompt_messages:
            if isinstance(prompt_message.content, list):
                prompt_message_content = [
                    content_item
                    for content_item in prompt_message.content
                    if model_schema.supports_prompt_content_type(content_item.type)
                ]
                if (
                    len(prompt_message_content) == 1
                    and prompt_message_content[0].type == PromptMessageContentType.TEXT
                ):
                    prompt_message.content = prompt_message_content[0].data
                else:
                    prompt_message.content = prompt_message_content
            if prompt_message.is_empty():
                continue
            filtered_prompt_messages.append(prompt_message)
        return filtered_prompt_messages

    @classmethod
    @override
    def _extract_variable_selector_to_variable_mapping(
        cls,
        *,
        graph_config: Mapping[str, Any],
        node_id: str,
        node_data: LLMNodeData,
    ) -> Mapping[str, Sequence[str]]:
        # graph_config is not used in this node type
        _ = graph_config  # Explicitly mark as unused
        variable_mapping = cls._extract_prompt_variable_mapping(
            node_data.prompt_template
        )
        cls._add_memory_query_mapping(
            variable_mapping=variable_mapping,
            node_data=node_data,
        )
        cls._add_special_variable_mappings(
            variable_mapping=variable_mapping,
            node_data=node_data,
        )
        cls._add_jinja_variable_mappings(
            variable_mapping=variable_mapping,
            prompt_template=node_data.prompt_template,
            node_data=node_data,
        )
        return {node_id + "." + key: value for key, value in variable_mapping.items()}

    @classmethod
    def _extract_prompt_variable_mapping(
        cls,
        prompt_template: Sequence[LLMNodeChatModelMessage]
        | LLMNodeCompletionModelPromptTemplate,
    ) -> dict[str, Sequence[str]]:
        return {
            variable_selector.variable: variable_selector.value_selector
            for variable_selector in cls._extract_prompt_variable_selectors(
                prompt_template=prompt_template,
            )
        }

    @staticmethod
    def _extract_prompt_variable_selectors(
        *,
        prompt_template: Sequence[LLMNodeChatModelMessage]
        | LLMNodeCompletionModelPromptTemplate,
    ) -> list[VariableSelector]:
        if isinstance(prompt_template, list):
            return [
                variable_selector
                for prompt in prompt_template
                if prompt.edition_type != "jinja2"
                for variable_selector in VariableTemplateParser(
                    template=prompt.text,
                ).extract_variable_selectors()
            ]
        if isinstance(prompt_template, LLMNodeCompletionModelPromptTemplate):
            if prompt_template.edition_type == "jinja2":
                return []
            return VariableTemplateParser(
                template=prompt_template.text,
            ).extract_variable_selectors()

        msg = f"Invalid prompt template type: {type(prompt_template)}"
        raise InvalidVariableTypeError(msg)

    @staticmethod
    def _add_memory_query_mapping(
        *,
        variable_mapping: dict[str, Sequence[str]],
        node_data: LLMNodeData,
    ) -> None:
        memory = node_data.memory
        if not memory or not memory.query_prompt_template:
            return

        for variable_selector in VariableTemplateParser(
            template=memory.query_prompt_template,
        ).extract_variable_selectors():
            variable_mapping[variable_selector.variable] = (
                variable_selector.value_selector
            )

    @staticmethod
    def _add_special_variable_mappings(
        *,
        variable_mapping: dict[str, Sequence[str]],
        node_data: LLMNodeData,
    ) -> None:
        if (
            node_data.context.enabled
            and node_data.context.variable_selector is not None
        ):
            variable_mapping["#context#"] = node_data.context.variable_selector
        if node_data.vision.enabled:
            variable_mapping["#files#"] = node_data.vision.configs.variable_selector

    @classmethod
    def _add_jinja_variable_mappings(
        cls,
        *,
        variable_mapping: dict[str, Sequence[str]],
        prompt_template: Sequence[LLMNodeChatModelMessage]
        | LLMNodeCompletionModelPromptTemplate,
        node_data: LLMNodeData,
    ) -> None:
        if not node_data.prompt_config or not cls._prompt_template_uses_jinja(
            prompt_template=prompt_template,
        ):
            return

        for variable_selector in node_data.prompt_config.jinja2_variables or []:
            variable_mapping[variable_selector.variable] = (
                variable_selector.value_selector
            )

    @staticmethod
    def _prompt_template_uses_jinja(
        *,
        prompt_template: Sequence[LLMNodeChatModelMessage]
        | LLMNodeCompletionModelPromptTemplate,
    ) -> bool:
        if isinstance(prompt_template, LLMNodeCompletionModelPromptTemplate):
            return prompt_template.edition_type == "jinja2"
        return any(prompt.edition_type == "jinja2" for prompt in prompt_template)

    @classmethod
    @override
    def get_default_config(
        cls,
        filters: Mapping[str, object] | None = None,
    ) -> Mapping[str, object]:
        _ = filters
        return {
            "type": "llm",
            "config": {
                "prompt_templates": {
                    "chat_model": {
                        "prompts": [
                            {
                                "role": "system",
                                "text": "You are a helpful AI assistant.",
                                "edition_type": "basic",
                            },
                        ],
                    },
                    "completion_model": {
                        "conversation_histories_role": {
                            "user_prefix": "Human",
                            "assistant_prefix": "Assistant",
                        },
                        "prompt": {
                            "text": (
                                "Here are the chat histories between human and "
                                "assistant, inside <histories></histories> XML "
                                "tags.\n\n<histories>\n{{#histories#}}\n"
                                "</histories>\n\n\nHuman: {{#sys.query#}}\n\n"
                                "Assistant:"
                            ),
                            "edition_type": "basic",
                        },
                        "stop": ["Human:"],
                    },
                },
            },
        }

    @staticmethod
    def handle_list_messages(
        *,
        messages: Sequence[LLMNodeChatModelMessage],
        context: str,
        jinja2_variables: Sequence[VariableSelector],
        variable_pool: VariablePool,
        vision_detail_config: ImagePromptMessageContent.DETAIL,
        jinja2_template_renderer: Jinja2TemplateRenderer | None = None,
    ) -> Sequence[PromptMessage]:
        prompt_messages: list[PromptMessage] = []
        for message in messages:
            prompt_messages.extend(
                LLMNode._build_prompt_messages_for_message(
                    message=message,
                    context=context,
                    jinja2_variables=jinja2_variables,
                    variable_pool=variable_pool,
                    vision_detail_config=vision_detail_config,
                    jinja2_template_renderer=jinja2_template_renderer,
                ),
            )
        return prompt_messages

    @staticmethod
    def _build_prompt_messages_for_message(
        *,
        message: LLMNodeChatModelMessage,
        context: str,
        jinja2_variables: Sequence[VariableSelector],
        variable_pool: VariablePool,
        vision_detail_config: ImagePromptMessageContent.DETAIL,
        jinja2_template_renderer: Jinja2TemplateRenderer | None = None,
    ) -> list[PromptMessage]:
        if message.edition_type == "jinja2":
            return [
                LLMNode._build_jinja_prompt_message(
                    message=message,
                    jinja2_variables=jinja2_variables,
                    variable_pool=variable_pool,
                    jinja2_template_renderer=jinja2_template_renderer,
                ),
            ]
        return LLMNode._build_basic_prompt_messages(
            message=message,
            context=context,
            variable_pool=variable_pool,
            vision_detail_config=vision_detail_config,
        )

    @staticmethod
    def _build_jinja_prompt_message(
        *,
        message: LLMNodeChatModelMessage,
        jinja2_variables: Sequence[VariableSelector],
        variable_pool: VariablePool,
        jinja2_template_renderer: Jinja2TemplateRenderer | None = None,
    ) -> PromptMessage:
        result_text = _render_jinja2_message(
            template=message.jinja2_text or "",
            jinja2_variables=jinja2_variables,
            variable_pool=variable_pool,
            jinja2_template_renderer=jinja2_template_renderer,
        )
        return _combine_message_content_with_role(
            contents=[TextPromptMessageContent(data=result_text)],
            role=message.role,
        )

    @staticmethod
    def _build_basic_prompt_messages(
        *,
        message: LLMNodeChatModelMessage,
        context: str,
        variable_pool: VariablePool,
        vision_detail_config: ImagePromptMessageContent.DETAIL,
    ) -> list[PromptMessage]:
        template = message.text.replace(llm_utils.CONTEXT_PLACEHOLDER, context)
        segment_group = variable_pool.convert_template(template)
        prompt_messages: list[PromptMessage] = []
        plain_text = segment_group.text
        if plain_text:
            prompt_messages.append(
                _combine_message_content_with_role(
                    contents=[TextPromptMessageContent(data=plain_text)],
                    role=message.role,
                ),
            )

        file_contents = LLMNode._collect_multimodal_file_contents(
            segment_group.value,
            vision_detail_config=vision_detail_config,
        )
        if file_contents:
            prompt_messages.append(
                _combine_message_content_with_role(
                    contents=file_contents,
                    role=message.role,
                ),
            )
        return prompt_messages

    @staticmethod
    def _collect_multimodal_file_contents(
        segments: Sequence[Any],
        *,
        vision_detail_config: ImagePromptMessageContent.DETAIL,
    ) -> list[PromptMessageContentUnionTypes]:
        file_contents: list[PromptMessageContentUnionTypes] = []
        for segment in segments:
            file_contents.extend(
                LLMNode._segment_to_prompt_message_contents(
                    segment,
                    vision_detail_config=vision_detail_config,
                ),
            )
        return file_contents

    @staticmethod
    def _segment_to_prompt_message_contents(
        segment: Any,
        *,
        vision_detail_config: ImagePromptMessageContent.DETAIL,
    ) -> list[PromptMessageContentUnionTypes]:
        if isinstance(segment, ArrayFileSegment):
            return [
                file_manager.to_prompt_message_content(
                    file,
                    image_detail_config=vision_detail_config,
                )
                for file in segment.value
                if file.type
                in frozenset((
                    FileType.IMAGE,
                    FileType.VIDEO,
                    FileType.AUDIO,
                    FileType.DOCUMENT,
                ))
            ]
        if isinstance(segment, FileSegment) and segment.value.type in frozenset((
            FileType.IMAGE,
            FileType.VIDEO,
            FileType.AUDIO,
            FileType.DOCUMENT,
        )):
            return [
                file_manager.to_prompt_message_content(
                    segment.value,
                    image_detail_config=vision_detail_config,
                ),
            ]
        return []

    @staticmethod
    def handle_blocking_result(
        *,
        invoke_result: LLMResult | LLMResultWithStructuredOutput,
        saver: LLMFileSaver,
        file_outputs: list[File],
        reasoning_format: Literal["separated", "tagged"] = "tagged",
        request_latency: float | None = None,
    ) -> ModelInvokeCompletedEvent:
        buffer = io.StringIO()
        for text_part in LLMNode._save_multimodal_output_and_convert_result_to_markdown(
            contents=invoke_result.message.content,
            file_saver=saver,
            file_outputs=file_outputs,
        ):
            buffer.write(text_part)

        # Extract reasoning content from <think> tags in the main text
        full_text = buffer.getvalue()

        if reasoning_format == "tagged":
            # Keep <think> tags in text for backward compatibility
            clean_text = full_text
            reasoning_content = ""
        else:
            # Extract clean text and reasoning from <think> tags
            clean_text, reasoning_content = LLMNode._split_reasoning(
                full_text,
                reasoning_format,
            )

        event = ModelInvokeCompletedEvent(
            # Use clean_text for separated mode, full_text for tagged mode
            text=clean_text if reasoning_format == "separated" else full_text,
            usage=invoke_result.usage,
            finish_reason=None,
            # Reasoning content for workflow variables and downstream nodes
            reasoning_content=reasoning_content,
            # Pass structured output if enabled
            structured_output=getattr(invoke_result, "structured_output", None),
        )
        if request_latency is not None:
            event.usage.latency = round(request_latency, 3)
        return event

    @staticmethod
    def save_multimodal_image_output(
        *,
        content: ImagePromptMessageContent,
        file_saver: LLMFileSaver,
    ) -> File:
        """_save_multimodal_output saves multi-modal contents generated by LLM plugins.

        There are two kinds of multimodal outputs:

          - Inlined data encoded in base64, which would be saved to storage directly.
          - Remote files referenced by an url, which would be downloaded and
            then saved to storage.

        Currently, only image files are supported.

        Returns:
            The persisted graph-owned `File` describing the saved image output.

        """
        if content.url:
            saved_file = file_saver.save_remote_url(content.url, FileType.IMAGE)
        else:
            saved_file = file_saver.save_binary_string(
                data=base64.b64decode(content.base64_data),
                mime_type=content.mime_type,
                file_type=FileType.IMAGE,
            )
        return saved_file

    @staticmethod
    def fetch_structured_output_schema(
        *,
        structured_output: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Fetch the structured output schema from the node data.

        Returns:
            dict[str, Any]: The structured output schema

        Raises:
            LLMNodeError: If the schema payload is missing, invalid JSON,
                or not a JSON object.

        """
        if not structured_output:
            msg = "Please provide a valid structured output schema"
            raise LLMNodeError(msg)
        structured_output_schema = json.dumps(
            structured_output.get("schema", {}),
            ensure_ascii=False,
        )
        if not structured_output_schema:
            msg = "Please provide a valid structured output schema"
            raise LLMNodeError(msg)

        try:
            schema = json.loads(structured_output_schema)
            if not isinstance(schema, dict):
                msg = "structured_output_schema must be a JSON object"
                raise LLMNodeError(msg)
        except json.JSONDecodeError as error:
            msg = "structured_output_schema is not valid JSON format"
            raise LLMNodeError(msg) from error
        else:
            return schema

    @staticmethod
    def _save_multimodal_output_and_convert_result_to_markdown(
        *,
        contents: str | list[PromptMessageContentUnionTypes] | None,
        file_saver: LLMFileSaver,
        file_outputs: list[File],
    ) -> Generator[str, None, None]:
        """Convert intermediate prompt messages into strings and yield
        them to the caller.

        If the messages contain non-textual content (e.g., multimedia like
        images or videos), it will be saved separately, and the
        corresponding Markdown representation will be yielded to the caller.

        Yields:
            Text or Markdown fragments as soon as each intermediate content
                item is ready.

        """
        # NOTE(QuantumGhost): This function should yield results to the
        # caller immediately whenever new content or partial content is
        # available. Avoid any intermediate buffering of results.
        # Additionally, do not yield empty strings; instead, yield from an
        # empty list.
        # if necessary.
        if contents is None:
            yield from []
            return
        if isinstance(contents, str):
            yield contents
        else:
            for item in contents:
                if isinstance(item, TextPromptMessageContent):
                    yield item.data
                elif isinstance(item, ImagePromptMessageContent):
                    file = LLMNode.save_multimodal_image_output(
                        content=item,
                        file_saver=file_saver,
                    )
                    file_outputs.append(file)
                    yield LLMNode._image_file_to_markdown(file)
                else:
                    logger.warning("unknown item type encountered, type=%s", type(item))
                    yield str(item)

    @property
    def retry(self) -> bool:
        return self.node_data.retry_config.retry_enabled

    @property
    def model_instance(self) -> LLMProtocol:
        return self._model_instance


def _combine_message_content_with_role(
    *,
    contents: str | list[PromptMessageContentUnionTypes] | None = None,
    role: PromptMessageRole,
) -> PromptMessage:
    match role:
        case PromptMessageRole.USER:
            return UserPromptMessage(content=contents)
        case PromptMessageRole.ASSISTANT:
            return AssistantPromptMessage(content=contents)
        case PromptMessageRole.SYSTEM:
            return SystemPromptMessage(content=contents)
        case PromptMessageRole.TOOL:
            msg = f"Role {role} is not supported"
            raise NotImplementedError(msg)
        case _:
            assert_never(role)


def _render_jinja2_message(
    *,
    template: str,
    jinja2_variables: Sequence[VariableSelector],
    variable_pool: VariablePool,
    jinja2_template_renderer: Jinja2TemplateRenderer | None,
) -> str:
    if not template:
        return ""

    jinja2_inputs = {}
    for jinja2_variable in jinja2_variables:
        variable = variable_pool.get(jinja2_variable.value_selector)
        jinja2_inputs[jinja2_variable.variable] = (
            variable.to_object() if variable else ""
        )
    if jinja2_template_renderer is None:
        msg = (
            "LLMNode requires an injected jinja2_template_renderer for jinja2 prompts."
        )
        raise TemplateRenderError(msg)
    return jinja2_template_renderer.render_template(template, jinja2_inputs)


def _calculate_rest_token(
    *,
    prompt_messages: list[PromptMessage],
    model_instance: LLMProtocol,
) -> int:
    rest_tokens = 2000
    runtime_model_schema = llm_utils.fetch_model_schema(model_instance=model_instance)
    runtime_model_parameters = model_instance.parameters

    model_context_tokens = runtime_model_schema.model_properties.get(
        ModelPropertyKey.CONTEXT_SIZE,
    )
    if model_context_tokens:
        curr_message_tokens = model_instance.get_llm_num_tokens(prompt_messages)

        max_tokens = 0
        for parameter_rule in runtime_model_schema.parameter_rules:
            if parameter_rule.name == "max_tokens" or (
                parameter_rule.use_template
                and parameter_rule.use_template == "max_tokens"
            ):
                max_tokens = (
                    runtime_model_parameters.get(parameter_rule.name)
                    or runtime_model_parameters.get(str(parameter_rule.use_template))
                    or 0
                )

        rest_tokens = model_context_tokens - max_tokens - curr_message_tokens
        rest_tokens = max(rest_tokens, 0)

    return rest_tokens


def _handle_memory_chat_mode(
    *,
    memory: PromptMessageMemory | None,
    memory_config: MemoryConfig | None,
    model_instance: LLMProtocol,
) -> Sequence[PromptMessage]:
    memory_messages: Sequence[PromptMessage] = []
    # Get messages from memory for chat model
    if memory and memory_config:
        rest_tokens = _calculate_rest_token(
            prompt_messages=[],
            model_instance=model_instance,
        )
        memory_messages = memory.get_history_prompt_messages(
            max_token_limit=rest_tokens,
            message_limit=memory_config.window.size
            if memory_config.window.enabled
            else None,
        )
    return memory_messages


def _handle_memory_completion_mode(
    *,
    memory: PromptMessageMemory | None,
    memory_config: MemoryConfig | None,
    model_instance: LLMProtocol,
) -> str:
    memory_text = ""
    # Get history text from memory for completion model
    if memory and memory_config:
        rest_tokens = _calculate_rest_token(
            prompt_messages=[],
            model_instance=model_instance,
        )
        if not memory_config.role_prefix:
            msg = "Memory role prefix is required for completion model."
            raise MemoryRolePrefixRequiredError(msg)
        memory_text = llm_utils.fetch_memory_text(
            memory=memory,
            max_token_limit=rest_tokens,
            message_limit=memory_config.window.size
            if memory_config.window.enabled
            else None,
            human_prefix=memory_config.role_prefix.user,
            ai_prefix=memory_config.role_prefix.assistant,
        )
    return memory_text


def _handle_completion_template(
    *,
    template: LLMNodeCompletionModelPromptTemplate,
    context: str,
    jinja2_variables: Sequence[VariableSelector],
    variable_pool: VariablePool,
    jinja2_template_renderer: Jinja2TemplateRenderer | None = None,
) -> Sequence[PromptMessage]:
    """Handle completion template processing outside of LLMNode class.

    Args:
        template: The completion model prompt template
        context: Context string
        jinja2_variables: Variables for jinja2 template rendering
        variable_pool: Variable pool for template conversion
        jinja2_template_renderer: Optional renderer for jinja2 templates

    Returns:
        Sequence of prompt messages

    """
    prompt_messages = []
    if template.edition_type == "jinja2":
        result_text = _render_jinja2_message(
            template=template.jinja2_text or "",
            jinja2_variables=jinja2_variables,
            variable_pool=variable_pool,
            jinja2_template_renderer=jinja2_template_renderer,
        )
    else:
        template_text = template.text.replace(llm_utils.CONTEXT_PLACEHOLDER, context)
        result_text = variable_pool.convert_template(template_text).text
    prompt_message = _combine_message_content_with_role(
        contents=[TextPromptMessageContent(data=result_text)],
        role=PromptMessageRole.USER,
    )
    prompt_messages.append(prompt_message)
    return prompt_messages
