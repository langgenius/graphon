import logging
import time
import uuid
from collections.abc import Callable, Generator, Iterator, Mapping, Sequence

from graphon.model_runtime.callbacks.base_callback import Callback
from graphon.model_runtime.callbacks.logging_callback import LoggingCallback
from graphon.model_runtime.entities.llm_entities import (
    LLMResult,
    LLMResultChunk,
    LLMUsage,
)
from graphon.model_runtime.entities.message_entities import (
    AssistantPromptMessage,
    PromptMessage,
    PromptMessageContentUnionTypes,
    PromptMessageTool,
    TextPromptMessageContent,
)
from graphon.model_runtime.entities.model_entities import (
    ModelType,
    PriceType,
)
from graphon.model_runtime.model_providers.base.ai_model import AIModel

logger = logging.getLogger(__name__)


def _run_callbacks(
    callbacks: Sequence[Callback] | None,
    *,
    event: str,
    invoke: Callable[[Callback], None],
) -> None:
    if not callbacks:
        return

    for callback in callbacks:
        try:
            invoke(callback)
        except Exception as e:
            if callback.raise_error:
                raise
            logger.warning(
                "Callback %s %s failed with error %s",
                callback.__class__.__name__,
                event,
                e,
            )


def generate_tool_call_id() -> str:
    return f"chatcmpl-tool-{uuid.uuid4().hex!s}"


def _get_or_create_tool_call(
    existing_tools_calls: list[AssistantPromptMessage.ToolCall],
    tool_call_id: str,
) -> AssistantPromptMessage.ToolCall:
    """Get or create a tool call by ID.

    If `tool_call_id` is empty, returns the most recently created tool call.

    Returns:
        The existing or newly created tool call that should receive the delta.

    Raises:
        ValueError: If `tool_call_id` is empty and there is no prior tool call to reuse.

    """
    if not tool_call_id:
        if not existing_tools_calls:
            msg = (
                "tool_call_id is empty but no existing tool call is "
                "available to apply the delta"
            )
            raise ValueError(msg)
        return existing_tools_calls[-1]

    tool_call = next(
        (
            tool_call
            for tool_call in existing_tools_calls
            if tool_call.id == tool_call_id
        ),
        None,
    )
    if tool_call is None:
        tool_call = AssistantPromptMessage.ToolCall(
            id=tool_call_id,
            type="function",
            function=AssistantPromptMessage.ToolCall.ToolCallFunction(
                name="",
                arguments="",
            ),
        )
        existing_tools_calls.append(tool_call)

    return tool_call


def _merge_tool_call_delta(
    tool_call: AssistantPromptMessage.ToolCall,
    delta: AssistantPromptMessage.ToolCall,
) -> None:
    if delta.id:
        tool_call.id = delta.id
    if delta.type:
        tool_call.type = delta.type
    if delta.function.name:
        tool_call.function.name = delta.function.name
    if delta.function.arguments:
        tool_call.function.arguments += delta.function.arguments


def _build_llm_result_from_chunks(
    model: str,
    prompt_messages: Sequence[PromptMessage],
    chunks: Iterator[LLMResultChunk],
) -> LLMResult:
    """Build a single `LLMResult` by accumulating all returned chunks.

    Some models only support streaming output and the runtime may still return
    an iterator in non-stream mode, so all chunks must be consumed and merged.

    Returns:
        A normalized `LLMResult` assembled from the consumed chunks.

    """
    content = ""
    content_list: list[PromptMessageContentUnionTypes] = []
    usage = LLMUsage.empty_usage()
    system_fingerprint: str | None = None
    tools_calls: list[AssistantPromptMessage.ToolCall] = []

    try:
        for chunk in chunks:
            if isinstance(chunk.delta.message.content, str):
                content += chunk.delta.message.content
            elif isinstance(chunk.delta.message.content, list):
                content_list.extend(chunk.delta.message.content)

            if chunk.delta.message.tool_calls:
                merge_tool_call_deltas(chunk.delta.message.tool_calls, tools_calls)

            if chunk.delta.usage:
                usage = chunk.delta.usage
            if chunk.system_fingerprint:
                system_fingerprint = chunk.system_fingerprint
    except Exception:
        logger.exception("Error while consuming non-stream plugin chunk iterator.")
        raise
    finally:
        close = getattr(chunks, "close", None)
        if callable(close):
            close()

    return LLMResult(
        model=model,
        prompt_messages=prompt_messages,
        message=AssistantPromptMessage(
            content=content or content_list,
            tool_calls=tools_calls,
        ),
        usage=usage,
        system_fingerprint=system_fingerprint,
    )


def normalize_non_stream_runtime_result(
    model: str,
    prompt_messages: Sequence[PromptMessage],
    result: LLMResult | Iterator[LLMResultChunk],
) -> LLMResult:
    if isinstance(result, LLMResult):
        return result
    return _build_llm_result_from_chunks(
        model=model,
        prompt_messages=prompt_messages,
        chunks=result,
    )


def merge_tool_call_deltas(
    new_tool_calls: list[AssistantPromptMessage.ToolCall],
    existing_tools_calls: list[AssistantPromptMessage.ToolCall],
    *,
    id_generator: Callable[[], str] | None = None,
) -> None:
    """Merge incremental tool call deltas into existing tool calls.

    :param new_tool_calls: List of new tool call deltas to be merged.
    :param existing_tools_calls: List of existing tool calls modified in place.
    :param id_generator: Optional callable for generating IDs in tests/callers.
    """
    generator = generate_tool_call_id if id_generator is None else id_generator

    for new_tool_call in new_tool_calls:
        if new_tool_call.function.name and not new_tool_call.id:
            new_tool_call.id = generator()

        tool_call = _get_or_create_tool_call(existing_tools_calls, new_tool_call.id)
        _merge_tool_call_delta(tool_call, new_tool_call)


def _invoke_llm_via_runtime(
    *,
    llm_model: "LargeLanguageModel",
    provider: str,
    model: str,
    credentials: dict,
    model_parameters: dict,
    prompt_messages: Sequence[PromptMessage],
    tools: list[PromptMessageTool] | None,
    stop: Sequence[str] | None,
    stream: bool,
) -> LLMResult | Generator[LLMResultChunk, None, None]:
    return llm_model.model_runtime.invoke_llm(
        provider=provider,
        model=model,
        credentials=credentials,
        model_parameters=model_parameters,
        prompt_messages=list(prompt_messages),
        tools=tools,
        stop=stop,
        stream=stream,
    )


class LargeLanguageModel(AIModel):
    """Model class for large language model."""

    model_type: ModelType = ModelType.LLM

    def invoke(
        self,
        model: str,
        credentials: dict,
        prompt_messages: list[PromptMessage],
        model_parameters: dict | None = None,
        tools: list[PromptMessageTool] | None = None,
        stop: list[str] | None = None,
        stream: bool = True,
        callbacks: list[Callback] | None = None,
    ) -> LLMResult | Generator[LLMResultChunk, None, None]:
        """Invoke the large language model and optionally stream result chunks."""
        # validate and filter model parameters
        if model_parameters is None:
            model_parameters = {}

        self.started_at = time.perf_counter()

        callbacks = callbacks or []

        if logger.isEnabledFor(logging.DEBUG):
            callbacks.append(LoggingCallback())

        # trigger before invoke callbacks
        self._trigger_before_invoke_callbacks(
            model=model,
            credentials=credentials,
            prompt_messages=prompt_messages,
            model_parameters=model_parameters,
            tools=tools,
            stop=stop,
            stream=stream,
            callbacks=callbacks,
        )

        result: LLMResult | Generator[LLMResultChunk, None, None]

        try:
            result = _invoke_llm_via_runtime(
                llm_model=self,
                provider=self.provider,
                model=model,
                credentials=credentials,
                model_parameters=model_parameters,
                prompt_messages=prompt_messages,
                tools=tools,
                stop=stop,
                stream=stream,
            )

            if not stream:
                result = normalize_non_stream_runtime_result(
                    model=model,
                    prompt_messages=prompt_messages,
                    result=result,
                )
        except Exception as e:
            self._trigger_invoke_error_callbacks(
                model=model,
                ex=e,
                credentials=credentials,
                prompt_messages=prompt_messages,
                model_parameters=model_parameters,
                tools=tools,
                stop=stop,
                stream=stream,
                callbacks=callbacks,
            )

            # TODO
            raise self._transform_invoke_error(e) from e

        if stream and not isinstance(result, LLMResult):
            return self._invoke_result_generator(
                model=model,
                result=result,
                credentials=credentials,
                prompt_messages=prompt_messages,
                model_parameters=model_parameters,
                tools=tools,
                stop=stop,
                stream=stream,
                callbacks=callbacks,
            )
        if isinstance(result, LLMResult):
            self._trigger_after_invoke_callbacks(
                model=model,
                result=result,
                credentials=credentials,
                prompt_messages=prompt_messages,
                model_parameters=model_parameters,
                tools=tools,
                stop=stop,
                stream=stream,
                callbacks=callbacks,
            )
            # Following https://github.com/langgenius/dify/issues/17799,
            # we removed the prompt_messages from the chunk on the plugin daemon side.
            # To ensure compatibility, we add the prompt_messages back here.
            result.prompt_messages = prompt_messages
            return result
        msg = "unsupported invoke result type"
        raise NotImplementedError(msg, type(result))

    def _invoke_result_generator(
        self,
        model: str,
        result: Generator[LLMResultChunk, None, None],
        credentials: dict,
        prompt_messages: Sequence[PromptMessage],
        model_parameters: dict,
        tools: list[PromptMessageTool] | None = None,
        stop: Sequence[str] | None = None,
        stream: bool = True,
        invocation_context: Mapping[str, object] | None = None,
        callbacks: list[Callback] | None = None,
    ) -> Generator[LLMResultChunk, None, None]:
        """Stream runtime result chunks through callbacks and bookkeeping hooks."""
        callbacks = callbacks or []
        message_content: list[PromptMessageContentUnionTypes] = []
        usage = None
        system_fingerprint = None
        real_model = model

        def _update_message_content(
            content: str | list[PromptMessageContentUnionTypes] | None,
        ) -> None:
            if not content:
                return
            if isinstance(content, list):
                message_content.extend(content)
                return
            if isinstance(content, str):
                message_content.append(TextPromptMessageContent(data=content))
                return

        try:
            for chunk in result:
                # Following https://github.com/langgenius/dify/issues/17799,
                # We removed prompt_messages from the chunk on the plugin
                # daemon side.
                # To ensure compatibility, we add the prompt_messages back here.
                chunk.prompt_messages = prompt_messages
                yield chunk

                self._trigger_new_chunk_callbacks(
                    chunk=chunk,
                    model=model,
                    credentials=credentials,
                    prompt_messages=prompt_messages,
                    model_parameters=model_parameters,
                    tools=tools,
                    stop=stop,
                    stream=stream,
                    invocation_context=invocation_context,
                    callbacks=callbacks,
                )

                _update_message_content(chunk.delta.message.content)

                real_model = chunk.model
                if chunk.delta.usage:
                    usage = chunk.delta.usage

                if chunk.system_fingerprint:
                    system_fingerprint = chunk.system_fingerprint
        except Exception as e:
            raise self._transform_invoke_error(e) from e

        assistant_message = AssistantPromptMessage(content=message_content)
        self._trigger_after_invoke_callbacks(
            model=model,
            result=LLMResult(
                model=real_model,
                prompt_messages=prompt_messages,
                message=assistant_message,
                usage=usage or LLMUsage.empty_usage(),
                system_fingerprint=system_fingerprint,
            ),
            credentials=credentials,
            prompt_messages=prompt_messages,
            model_parameters=model_parameters,
            tools=tools,
            stop=stop,
            stream=stream,
            invocation_context=invocation_context,
            callbacks=callbacks,
        )

    def get_num_tokens(
        self,
        model: str,
        credentials: dict,
        prompt_messages: list[PromptMessage],
        tools: list[PromptMessageTool] | None = None,
    ) -> int:
        """Count prompt tokens for the given messages and optional tools."""
        return self.model_runtime.get_llm_num_tokens(
            provider=self.provider,
            model_type=self.model_type,
            model=model,
            credentials=credentials,
            prompt_messages=prompt_messages,
            tools=tools,
        )

    def calc_response_usage(
        self,
        model: str,
        credentials: dict,
        prompt_tokens: int,
        completion_tokens: int,
    ) -> LLMUsage:
        """Calculate unified usage and pricing metadata for a response."""
        # get prompt price info
        prompt_price_info = self.get_price(
            model=model,
            credentials=credentials,
            price_type=PriceType.INPUT,
            tokens=prompt_tokens,
        )

        # get completion price info
        completion_price_info = self.get_price(
            model=model,
            credentials=credentials,
            price_type=PriceType.OUTPUT,
            tokens=completion_tokens,
        )

        # transform usage
        return LLMUsage(
            prompt_tokens=prompt_tokens,
            prompt_unit_price=prompt_price_info.unit_price,
            prompt_price_unit=prompt_price_info.unit,
            prompt_price=prompt_price_info.total_amount,
            completion_tokens=completion_tokens,
            completion_unit_price=completion_price_info.unit_price,
            completion_price_unit=completion_price_info.unit,
            completion_price=completion_price_info.total_amount,
            total_tokens=prompt_tokens + completion_tokens,
            total_price=prompt_price_info.total_amount
            + completion_price_info.total_amount,
            currency=prompt_price_info.currency,
            latency=time.perf_counter() - self.started_at,
        )

    def _trigger_before_invoke_callbacks(
        self,
        model: str,
        credentials: dict,
        prompt_messages: list[PromptMessage],
        model_parameters: dict,
        tools: list[PromptMessageTool] | None = None,
        stop: Sequence[str] | None = None,
        stream: bool = True,
        invocation_context: Mapping[str, object] | None = None,
        callbacks: list[Callback] | None = None,
    ) -> None:
        """Trigger before invoke callbacks

        :param model: model name
        :param credentials: model credentials
        :param prompt_messages: prompt messages
        :param model_parameters: model parameters
        :param tools: tools for tool calling
        :param stop: stop words
        :param stream: is stream response
        :param invocation_context: opaque request metadata for the current invocation
        :param callbacks: callbacks
        """
        _run_callbacks(
            callbacks,
            event="on_before_invoke",
            invoke=lambda callback: callback.on_before_invoke(
                llm_instance=self,
                model=model,
                credentials=credentials,
                prompt_messages=prompt_messages,
                model_parameters=model_parameters,
                tools=tools,
                stop=stop,
                stream=stream,
                invocation_context=invocation_context,
            ),
        )

    def _trigger_new_chunk_callbacks(
        self,
        chunk: LLMResultChunk,
        model: str,
        credentials: dict,
        prompt_messages: Sequence[PromptMessage],
        model_parameters: dict,
        tools: list[PromptMessageTool] | None = None,
        stop: Sequence[str] | None = None,
        stream: bool = True,
        invocation_context: Mapping[str, object] | None = None,
        callbacks: list[Callback] | None = None,
    ) -> None:
        """Trigger new chunk callbacks

        :param chunk: chunk
        :param model: model name
        :param credentials: model credentials
        :param prompt_messages: prompt messages
        :param model_parameters: model parameters
        :param tools: tools for tool calling
        :param stop: stop words
        :param stream: is stream response
        :param invocation_context: opaque request metadata for the current invocation
        """
        _run_callbacks(
            callbacks,
            event="on_new_chunk",
            invoke=lambda callback: callback.on_new_chunk(
                llm_instance=self,
                chunk=chunk,
                model=model,
                credentials=credentials,
                prompt_messages=prompt_messages,
                model_parameters=model_parameters,
                tools=tools,
                stop=stop,
                stream=stream,
                invocation_context=invocation_context,
            ),
        )

    def _trigger_after_invoke_callbacks(
        self,
        model: str,
        result: LLMResult,
        credentials: dict,
        prompt_messages: Sequence[PromptMessage],
        model_parameters: dict,
        tools: list[PromptMessageTool] | None = None,
        stop: Sequence[str] | None = None,
        stream: bool = True,
        invocation_context: Mapping[str, object] | None = None,
        callbacks: list[Callback] | None = None,
    ) -> None:
        """Trigger after invoke callbacks

        :param model: model name
        :param result: result
        :param credentials: model credentials
        :param prompt_messages: prompt messages
        :param model_parameters: model parameters
        :param tools: tools for tool calling
        :param stop: stop words
        :param stream: is stream response
        :param invocation_context: opaque request metadata for the current invocation
        :param callbacks: callbacks
        """
        _run_callbacks(
            callbacks,
            event="on_after_invoke",
            invoke=lambda callback: callback.on_after_invoke(
                llm_instance=self,
                result=result,
                model=model,
                credentials=credentials,
                prompt_messages=prompt_messages,
                model_parameters=model_parameters,
                tools=tools,
                stop=stop,
                stream=stream,
                invocation_context=invocation_context,
            ),
        )

    def _trigger_invoke_error_callbacks(
        self,
        model: str,
        ex: Exception,
        credentials: dict,
        prompt_messages: list[PromptMessage],
        model_parameters: dict,
        tools: list[PromptMessageTool] | None = None,
        stop: Sequence[str] | None = None,
        stream: bool = True,
        invocation_context: Mapping[str, object] | None = None,
        callbacks: list[Callback] | None = None,
    ) -> None:
        """Trigger invoke error callbacks

        :param model: model name
        :param ex: exception
        :param credentials: model credentials
        :param prompt_messages: prompt messages
        :param model_parameters: model parameters
        :param tools: tools for tool calling
        :param stop: stop words
        :param stream: is stream response
        :param invocation_context: opaque request metadata for the current invocation
        :param callbacks: callbacks
        """
        _run_callbacks(
            callbacks,
            event="on_invoke_error",
            invoke=lambda callback: callback.on_invoke_error(
                llm_instance=self,
                ex=ex,
                model=model,
                credentials=credentials,
                prompt_messages=prompt_messages,
                model_parameters=model_parameters,
                tools=tools,
                stop=stop,
                stream=stream,
                invocation_context=invocation_context,
            ),
        )
