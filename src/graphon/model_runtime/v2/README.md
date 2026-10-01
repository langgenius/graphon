<!-- knowledge
last_checked: "2026-10-01T11:32:21Z"
-->
# Model Runtime V2

An additive contract package for plugin implementations. Import its public values
from `graphon.model_runtime.v2`. It does not select providers or execute calls.

[Identity records](domain/identity.py) separate `ModelRef` (plugin, provider,
model) from `ContractRef` (contract ID, revision). Contract revisions are opaque
identifiers for effective input/output contracts, not model-weight versions.
Connection selection is separate from model identity.
[Identity checks](../../../../tests/model_runtime/v2/test_identity.py) define
validation, immutability, and exact-string identity semantics.

The public package re-exports the application surface; application declarations
depend on domain records. [Dependency checks](../../../../tests/model_runtime/v2/test_imports.py)
enforce inward imports and standard-library-only dependencies. Importing the
package still executes its existing parent initializer; isolation applies to V2
source dependencies, not the absence of ancestor imports from `sys.modules`.

Existing model consumers require explicit future adapters; introducing these
declarations does not switch any caller to V2.

[DataFormat](domain/formats.py) carries an owned JSON object schema, a
`mime_types` tuple of strings, and an optional profile string. MIME types describe
supported payload formats, such as `text/plain`, `image/png`, or `application/json`;
profiles identify separately defined semantics. No shared profile is standardized
here. [JSON values](domain/json_values.py)
preserve scalar types and require finite numbers, string object keys, and acyclic
lists and objects. The `schema` property returns a defensive copy; callers cannot
change a description by mutating the supplied schema or a returned dictionary.
[Format checks](../../../../tests/model_runtime/v2/test_formats.py) cover these
ownership and JSON boundaries.

MIME types describe content, excluding invocation parameters and the JSON
transport envelope. Plugins advertise concrete media types with MIME parameters
when needed, not wildcard media ranges. Values are preserved unchanged, including
parameter case. An empty tuple means unspecified, not support for every format.
MIME labels do not define tool semantics, score meaning, or valid input/output
combinations; schemas and profile definitions carry those constraints.
Declarations check only that MIME metadata is a tuple of strings. Plugins
validate [media-type syntax and parameter semantics](https://www.rfc-editor.org/rfc/rfc9110.html#section-8.3.1)
and actual format support.

The plugin boundary is responsible for interpreting self-contained JSON Schema
Draft 2020-12 with document-local references. Request schemas describe the joint
`{input, parameters}` object; output schemas describe native JSON values. The
declarations check JSON value categories only; schema keywords, validity,
reference policy, and semantic constraints belong to the plugin boundary.

[Immutable descriptors](domain/descriptors.py) associate each model with explicit
input/output contracts. Operation tags support discovery; they neither select
nor authorize an invocation. A model may offer distinct text-to-score and
audio-to-text contracts without promising other combinations of those formats.
Request MIME types summarize input content, output MIME types summarize
returned content, and stream MIME types summarize preview content.
[Descriptor checks](../../../../tests/model_runtime/v2/test_descriptors.py)
define contract-ID uniqueness and delivery requirements. Matching the selected
revision to the effective connection configuration and rejecting stale revisions
before provider inference are plugin responsibilities, outside these declarations.

[CallContext](application/context.py) is an immutable record that identifies an
already configured connection and correlates one call without carrying
credentials. An identifier grants neither authorization nor an
idempotency guarantee. Plugins bind discovery and invocation to the same
effective connection and interpret the optional timeout at the call boundary.
[Context checks](../../../../tests/model_runtime/v2/test_context.py) cover its
identity and timeout invariants.

[ProviderState](domain/provider_state.py) carries opaque JSON continuation with its
model, contract, connection scope, and state version. Scope and version are
nonempty strings preserved exactly. Its envelope is frozen and incoming JSON is
copied deeply; nested containers remain owned, mutable snapshots. Unlike discovery
schemas, reading `value` does not return a defensive copy. The potentially
sensitive payload is omitted from the record's repr.

Plugins are responsible for checking ownership, expiry, and supported versions;
hosts retain and return the state unchanged. The record provides no session
management. [State checks](../../../../tests/model_runtime/v2/test_provider_state.py)
cover provenance, envelope immutability, and JSON payload ownership and validation.

[ModelError](domain/errors.py) carries an open, nonempty failure code, safe message,
optional provider code, and optional finite, nonnegative retry delay in seconds.
Plugins must map failures and remove sensitive provider details; the record cannot
determine whether a message contains secrets.
[ModelCallError](application/errors.py) is the Python failure envelope. Retry
hints are advisory and grant no idempotency guarantee; these declarations never
retry calls. Treat unknown error codes as generic failures. Plugins should use
these common meanings without closing the set of representable codes:

| Code | Meaning |
| --- | --- |
| `invalid_request`, `invalid_result` | Input or provider output violates the selected contract. |
| `not_found`, `access_denied` | The target is missing or the caller cannot access it. |
| `unsupported_delivery` | The requested interface, including cancellation, is unavailable. |
| `contract_changed` | The effective revision changed; reject before provider inference. |
| `rate_limited`, `unavailable`, `timeout` | The call exceeded a limit, was unavailable, or timed out. |
| `cancelled`, `provider_error` | The call was cancelled or failed at the provider. |

[Failure checks](../../../../tests/model_runtime/v2/test_errors.py) cover error
fields and raw usage. Usage is opaque provider JSON: preserve supplied falsey
values and nested nulls; only missing or top-level null becomes integer `0`.
`ModelCallError` owns a deep copy of supplied usage; its nested containers remain
mutable. There is no token counting, pricing, unit conversion, or accumulation,
and fallback zero does not imply a free call.

[ModelRequest and ModelResult](domain/exchange.py) carry native JSON exchanges,
including scalar output such as a relevance score. Their envelopes are frozen;
incoming JSON containers are copied deeply and remain owned, mutable snapshots.
Continuation retains the supplied `ProviderState` reference. Parameters, optional
output schema, and result metadata are JSON objects; metadata carries provider
information, not permissions or application state.

The plugin matches result model/contract to the request and `request_id` to the
call context. A requested output schema requires contract support; output must
satisfy both the declared and requested schemas. These records neither execute
schemas nor transform provider output.
[Exchange checks](../../../../tests/model_runtime/v2/test_exchange.py) cover native
values, continuation, per-record defaults, ownership, and raw usage preservation.

[ModelCatalog](application/catalog.py) lists models and describes their effective
contracts in a supplied `CallContext`. Its `describe` method permits explicitly
configured models absent from `list_models`. Discovery grants no invocation
authorization.
[Catalog example](../../../../tests/model_runtime/v2/test_catalog.py) checks a
structural implementation that needs no invocation, streaming, or job methods.

[ModelInvoker](application/invoker.py) declares a complete call using the request,
context, and result records. It is independent of discovery and other delivery
modes. Plugins check the selected contract and connection, perform the call, and
return native output or raise `ModelCallError`. The optional context timeout
bounds this operation, including plugin validation and provider interaction.
Omission uses the connection/plugin default; a timeout does not guarantee remote
execution has stopped.
[Complete-call example](../../../../tests/model_runtime/v2/test_invoker.py) uses a
score-only implementation with native numeric output and unchanged falsey usage.

[Stream events](domain/stream_events.py) distinguish content previews, usage-only
updates, completed results, and failures. Nonterminal records carry a nonnegative
integer sequence; plugins number content and usage events together from zero,
increasing by one. Plugins emit exactly one terminal event and nothing afterward;
consumers treat iteration ending without one as interruption.
`StreamCompleted.result` is the authoritative complete `ModelResult` assembled by
the plugin. After `StreamFailed`, consumers must not treat earlier previews as
successful output. These records neither assemble output nor enforce stream
lifecycle.

Usage belongs to each event independently, with the same raw-value and null-zero
rules as complete results. Record envelopes are frozen and incoming JSON is copied
deeply into owned, mutable snapshots. Terminal records retain their result/error
references. [Event checks](../../../../tests/model_runtime/v2/test_stream_events.py)
cover the record invariants and illustrate plugin lifecycle responsibilities.

[ModelStreamer](application/streamer.py) returns a context-managed event iterator.
The implementation releases local resources on completion, exception, or early
exit; closing the context does not guarantee remote cancellation. Its timeout
runs from context entry through the terminal event. Expected failures before any
output may raise `ModelCallError`; after output begins they become `StreamFailed`,
including timeouts. Transport loss can still interrupt without a terminal event.
Consumers may ignore previews whose profile they cannot render and wait for the
complete output. [Streaming example](../../../../tests/model_runtime/v2/test_streamer.py)
checks a stream-only implementation and resource cleanup across all three exits.
