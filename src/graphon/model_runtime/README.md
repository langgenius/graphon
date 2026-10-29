<!-- knowledge
last_checked: "2026-10-29T01:02:46Z"
-->
# Model Runtime

This package defines provider schemas, credential validation, capability-specific
runtime contracts, and model wrappers. Host applications supply concrete runtime
adapters; any provider-selection or credential UI belongs to the host.

For graph-facing nodes, use the
[host integration guide](../../../docs/development.md#integrate-host-services).

## Choose a runtime surface

Implement only the [capability protocols](protocols/) your adapter needs. The
[aggregate runtime protocol](protocols/runtime.py) is for adapters that
intentionally provide the complete surface.

Use [ModelProviderFactory](model_providers/model_provider_factory.py) for provider
discovery and credential validation; use [capability wrappers](model_providers/)
for invocation. [Entities](entities/) describe model parameters and credential
forms for host UIs. The
[model dispatch tests](../../../tests/model_runtime/test_model_dispatch.py)
verify that provider discovery and individual capabilities remain independent.

## Preserve provider message state

Use `opaque_body` on [assistant messages and content items](entities/message_entities.py)
for JSON-serializable provider state, such as signatures or encrypted reasoning.
The adapter owns its meaning; `None` means no state, and any other value keeps
an assistant message nonempty. See the
[message tests](../../../tests/model_runtime/test_message_entities.py).

[LargeLanguageModel](model_providers/base/large_language_model.py) keeps the last
non-`None` assistant state per invocation for blocking results and `on_after_invoke`.
Emit complete snapshots: each replaces the previous state. Mixed text and content
blocks retain their order and block state; see the
[dispatch tests](../../../tests/model_runtime/test_model_dispatch.py).
[SlimLLM](../dsl/slim/llm.py) shares this aggregation for blocking ordinary and
structured output calls, covered by the [Slim tests](../../../tests/dsl/test_slim_llm.py).

Hosts can display chunks while calling `consume(chunk)` on one [LLMResultAccumulator](model_providers/base/large_language_model.py) per invocation.
After consuming the stream, call its `to_result(model=..., prompt_messages=...)` and use the returned `message` for replay or [JSON persistence](../../../tests/model_runtime/test_llm_result_accumulator.py).

[Prompt filtering](../nodes/llm/llm_utils.py) preserves state on supported blocks
and assistant messages with state but no content; see the
[prompt tests](../../../tests/nodes/llm/test_llm_utils.py).
