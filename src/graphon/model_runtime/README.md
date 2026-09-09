<!-- knowledge
last_checked: "2026-09-09T21:56:06Z"
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
