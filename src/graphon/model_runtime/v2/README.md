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

[DataFormat](domain/formats.py) carries an owned JSON object schema, a tuple of
kind strings, and an optional profile string. Kind and profile values are open
metadata; the [built-in LLM contract](#built-in-llm-contract) defines shared content
profiles. [JSON values](domain/json_values.py)
preserve scalar types and require finite numbers, string object keys, and acyclic
lists and objects. The `schema` property returns a defensive copy; callers cannot
change a description by mutating the supplied schema or a returned dictionary.
[Format checks](../../../../tests/model_runtime/v2/test_formats.py) cover these
ownership and JSON boundaries.

The plugin boundary is responsible for interpreting self-contained JSON Schema
Draft 2020-12 with document-local references. Request schemas describe the joint
`{input, parameters}` object; output schemas describe native JSON values. The
declarations check JSON value categories only; schema keywords, validity,
reference policy, and semantic constraints belong to the plugin boundary.

[Immutable descriptors](domain/descriptors.py) associate each model with explicit
input/output contracts. Operation tags support discovery; they neither select
nor authorize an invocation. A model may offer distinct text-to-score and
audio-to-text contracts without promising other combinations of those formats.
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

[JobRef and JobStatus](domain/job_status.py) represent opaque remote handles and
exclusive queued, running, succeeded, failed, or cancelled outcomes. Success
requires a result matching the handle's model, contract, and submission request
ID. Failure requires an error; other states carry neither. Terminal states cannot
be cancellable. Positive finite scheduling hints are advisory; expiry is an aware
UTC timestamp for access to the handle/result, not proof that provider work ended.
Handle tokens are omitted from repr and copied as owned JSON snapshots. Plugins
retain the submission's validation context in the token or their own state, check
scope, and pin the original contract revision. Hosts keep the latest returned
handle unchanged on the same connection. Status usage belongs to that response,
independently of any nested result usage. Terminal outcomes must stay stable
across reads; fresh handles and usage can still be returned. These cross-call
obligations belong to the plugin, not a V2 store or scheduler.
[Job-record checks](../../../../tests/model_runtime/v2/test_job_status.py) cover
exclusive outcomes, original submission identity, hints, expiry, and raw JSON.

[ModelJobs](application/jobs.py) declares submission, one status read, and
best-effort cancellation. Submission may finish immediately. Failed status reads
or expired handles raise `ModelCallError`, not an invented failed job state.
Cancelling a terminal job returns that outcome. For active jobs, unsupported
cancellation raises `ModelCallError` with code `unsupported_delivery`; a response
may remain running while cancellation is pending or succeed if completion wins.
Only `cancelled` confirms cancellation. Each method has its own context timeout.
The host owns subsequent reads and the overall waiting deadline. The interface
adds no polling, sleep, retry, or persistence. Restart-safe resume requires
plugin/daemon support.
[Job-control examples](../../../../tests/model_runtime/v2/test_jobs.py) illustrate
these outcomes with a job-only implementation and refreshed opaque handles.

## Built-in LLM contract

This declaration provides one common content contract for new LLM nodes to adopt.
Existing nodes remain unchanged; this package supplies no reader or fallback
runtime. These profiles standardize text, media, tool calls/results, native JSON,
provider-visible reasoning, and refusals.

Import `LLM_CONTRACT` from `graphon.model_runtime.v2`. Its
[schemas](domain/llm.py) define these values:

| Format | Value | Versioned profile |
| --- | --- | --- |
| Request projection | `{input: {messages: [{role, content}]}, parameters: {}}` | `graphon.llm.request/1` |
| `ModelResult.output` | `{content: ContentPart[]}` or `{text: string}` shorthand | `graphon.llm.output/1` |
| `StreamChunk.value` preview | Indexed content previews; `{text_delta: string}` shorthand for text-only output | `graphon.llm.stream/1` |

Messages preserve their order and carry a string `role`; `content` is a string
shorthand or an ordered array of typed parts. Plugins define their supported
roles, such as `system`, `user`, and `assistant`, in the effective schema.
Preserve text exactly, including whitespace and empty strings.

| Content part | Shape and meaning |
| --- | --- |
| Text | `{type: "text", text: string}` |
| Media | `{type: "image" or "audio" or "video" or "document", mime_type, source}` |
| Media source | Exactly one of `{type: "uri", uri}`, `{type: "file", id}`, or `{type: "inline", encoding: "base64", data}` |
| Tool call | `{type: "tool_call", call_id, name, arguments: object}` |
| Tool result | `{type: "tool_result", call_id, content, is_error?: boolean}`; content is text or an ordered array of text/media/JSON parts |
| JSON | `{type: "json", value: any JSON value}`; preserve objects, arrays, scalars, null, and falsey values |
| Reasoning | `{type: "reasoning", text: string}`; only reasoning or summaries the provider exposes |
| Refusal | `{type: "refusal", text: string}`; distinct from answer text |

Media URI/file identifiers and MIME labels are nonempty strings. Source objects
have only the fields belonging to their selected source kind. Plugins check
actual MIME types, base64 decoding, size limits, reference access and lifetime;
the declarations do not fetch, decode, or store media. Input, message, part,
output, and preview objects permit extra fields with no standardized meaning.
The outer request projection contains only `input` and `parameters`.

`input.tools` optionally declares tools as `{name, description?, input_schema}`,
where `input_schema` is a JSON Schema object describing the final argument object.
Tool names and call IDs are nonempty strings.
`parameters.tool_choice` is `auto` (model may call tools),
`none` (no calls), `required` (at least one call), or `{name}` (only that named
tool must be called). `parameters.parallel_tool_calls: false` permits at most one
call per reply; `true` permits multiple calls. Omitted options use the
plugin/provider default. Other generation settings remain model-specific.

Plugins check unique tool names and call IDs, selected tool membership, argument
schema validity and conformance, and result correspondence to prior calls. The
declarations check only that `input_schema` is an object. Calls and results
keep their IDs and order across turns, including multimodal results; a result's
`is_error` describes tool execution, not a model invocation failure. A call is a
request for host execution, not execution authority. The host authorizes and runs
tools; this package adds no tool loop or dispatcher. Provider-hosted tools with
different semantics require an explicitly understood custom contract.

Output content is authoritative when present. An optional `text` convenience
field must equal the concatenation of its text parts; plugins enforce this
correspondence. Media-only, tool-only, and empty content do not require invented text.
Optional `finish_reason` is an open string describing why generation stopped;
preserve provider-specific values. Refusals and truncation do not become fabricated
answers or completed JSON. Return available content and the stop reason only when
they satisfy the selected contract and any requested output schema; otherwise use
the existing model failure envelope.

Native JSON parts describe returned values; they do not enable schema-directed
generation automatically. `accepts_output_schema` remains false by default.
Models supporting it opt in explicitly. `ModelRequest.output_schema` constrains
the entire `ModelResult.output` object, including its content wrapper, rather
than only a JSON part's `value`. Plugins enforce both the declared and requested
schemas. This adds no second response-format parameter or schema interpretation.

Opaque signatures, redacted/encrypted reasoning, and provider replay items belong
in `ProviderState`, not visible reasoning text. Plugins preserve their association
with content parts and their replay order; hosts retain and return that state
unchanged. Visible reasoning text alone is not a substitute for continuation.

The declaration describes representable formats, not capabilities every model
supports. Plugins narrow schemas and kinds to actual input/output combinations,
MIME/source kinds, parameters, and limits, rejecting unsupported requests before
provider inference.

The constant defaults to complete delivery. Its stream format is available for
models that support streaming; it does not advertise that all LLMs stream.
Plugins can use `dataclasses.replace(LLM_CONTRACT, ref=..., delivery=...)` to
declare their model-local identity, effective revision, and actual delivery.
Keep the shared profile meanings when narrowing schemas, and do not add mandatory
provider-only fields. Shared profile versions are independent of opaque
model-local contract revisions.
Custom contracts remain available; usage and continuation retain their existing
outer-envelope semantics.

Text, reasoning, and refusal previews use
`{type: "text_delta" or "reasoning_delta" or "refusal_delta", index, text}`;
whole-part previews use
`{type: "content_part", index, part}`. Their nonnegative `index` identifies the
position in final output content, independently of the outer event sequence.
Each text-bearing delta appends to its matching part type at that position;
a whole part replaces its preview there. JSON previews use complete parts, not
partially parsed values.
Plugins preserve part indices throughout a stream. The `{text_delta}`
shorthand appends only to text-only output and is not mixed with indexed events.
The completed result is authoritative; these declarations add no assembler.

Tool argument previews use
`{type: "tool_call_delta", index, call_id, name, arguments_delta: string}`.
Fragments append in event order at that index; they can be incomplete JSON and
are neither final arguments nor executable calls. Plugins preserve each call's
ID/name at its index, parse and check final arguments, and put complete call
objects in the terminal result.

Future consumers read ordered content first, or the `text` shorthand when
content is absent. A consumer supporting only text must retain the entire output
when it contains non-text parts, instead of selecting only the `text` convenience
field. Ignore unreadable previews and wait for completion.
If output interpretation fails,
pass through the original entire `ModelResult.output` JSON value, including extra
fields, arrays, scalars, or null. Other nodes can then parse that value. Do not
stringify it or substitute the `ModelResult` envelope. This fallback does not turn
provider errors, contract validation failures, failed streams, or interrupted
streams into successful output. Implementing these node behaviors belongs to a
later adoption change.

[Contract checks](../../../../tests/model_runtime/v2/test_llm_contract.py)
exercise the built-in schemas and reuse with model-local identity and delivery.
The source choices reflect
[OpenAI's image inputs](https://developers.openai.com/api/docs/guides/images-vision),
and ordered parts follow the message structure described by
[Gemini Content](https://ai.google.dev/api/generate-content#Content). Plugins own
conversion to provider formats and their support limits.
[Tool checks](../../../../tests/model_runtime/v2/test_llm_tools.py) cover
tool declarations, payload shapes, and incomplete argument previews; they do not
exercise provider integration or tool execution.
The tool/result forms reflect
[Claude tool exchanges](https://platform.claude.com/docs/en/agents-and-tools/tool-use/handle-tool-calls)
and [Gemini function calling](https://ai.google.dev/gemini-api/docs/function-calling).
[Response checks](../../../../tests/model_runtime/v2/test_llm_responses.py) cover
native JSON, reasoning/refusal parts, finish reasons, and their previews.
The distinction between structured output and refusals follows
[OpenAI structured outputs](https://developers.openai.com/api/docs/guides/structured-outputs).
Continuation obligations reflect
[OpenAI reasoning](https://developers.openai.com/api/docs/guides/reasoning) and
[Gemini thought signatures](https://ai.google.dev/gemini-api/docs/thinking#thought-signatures).
