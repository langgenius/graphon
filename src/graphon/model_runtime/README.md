<!-- knowledge
last_checked: "2026-09-17T01:26:49Z"
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

Use `opaque_body` on [assistant messages and content items](entities/message_entities.py) for provider state that must survive JSON serialization, such as signatures or encrypted reasoning.
The field accepts `JsonValue` and defaults to `None`; the adapter owns its meaning.
An assistant message with any non-`None` state is nonempty even without visible content or tool calls.
See the [message entity tests](../../../tests/model_runtime/test_message_entities.py) for serialization and empty-message behavior.

When consuming chunks, [LargeLanguageModel](model_providers/base/large_language_model.py) retains the last non-`None` assistant snapshot within each invocation for both the non-stream result and the result passed to `on_after_invoke`.
Adapters should emit complete snapshots because each replaces the previous value, including empty collections, empty strings, zero, and `False`.
Aggregation preserves the order of mixed strings and content blocks, including each block's state; see the [model dispatch tests](../../../tests/model_runtime/test_model_dispatch.py).
