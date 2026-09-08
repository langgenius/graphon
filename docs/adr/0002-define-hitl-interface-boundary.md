# ADR 0002: Define HITL Interface Boundary

- Status: Accepted
- Date: 2026-06-29
- Related PRs: #185
- Supersedes: N/A
- Superseded by: N/A

## Context

Graphon was extracted from Dify, but `HumanInputNode` still carried host-facing
semantics that did not belong in the library boundary.

In the old shape, Graphon knew about form schemas, resolved default values,
action definitions, session-oriented payloads, and host-side restoration
behavior. That made the HITL boundary too large:

- Graphon had to change when the host product changed its HITL semantics.
- Pause payloads leaked host-facing form content and delivery-oriented data.
- Host applications and Graphon had to evolve in lockstep.
- Too many ordinary HITL feature changes required synchronized work in both
  Graphon and Dify.

Graphon needs a smaller HITL contract: one that preserves control-flow
semantics while leaving product semantics to the embedding application.

## Decision

Graphon owns only the HITL control-flow state machine.

The supported Graphon-owned boundary is:

- `HumanInputNode` builds `HITLContext` and calls a host-provided
  `HITLCallback`.
- The callback returns one of three control-flow decisions:
  `PauseRequested`, `Completed`, or `Expired`.
- Graphon translates those decisions into its own node events and node run
  results.
- `Completed.inputs` and `Completed.outputs` carry runtime `Segment` mappings,
  not serialized host payloads.
- HITL pause reasons are reduced to the minimal host lookup key:
  `session_id`, `node_id`, and `node_title`.

This is intentionally a decision callback boundary, not an event stream or
generic result boundary. Graphon only needs to know:

- whether this execution should pause
- which branch handle should be selected when it continues
- which runtime inputs and outputs should be handed to downstream nodes

The host application owns everything else, including:

- form schema and rendering semantics
- action schema and output schema semantics
- default value resolution and submission validation
- session storage and persistence
- delivery and recipient semantics
- submission restoration and replay enrichment
- product-specific action semantics

Historical or Dify-specific payload shape is not part of the supported Graphon
interface. Graphon may keep compatibility shims when decoding legacy payloads,
but those legacy payloads are not the intended contract.

## Consequences

- Graphon HITL behavior is now defined in terms of callback-driven
  control-flow, not form persistence semantics.
- Dify or another embedding host is responsible for reconstructing persisted
  values into runtime `Segment` values before resuming Graphon execution.
- Embedding applications must own session lookup, form reconstruction, and
  submission semantics outside Graphon.
- Most future HITL product changes should happen in the embedding application,
  not in Graphon.
- Future HITL evolution in Graphon should focus on control-flow semantics and
  node result translation, not host UX or transport details.

## Alternatives Considered

- Keep Graphon-owned form schema and runtime binding abstractions:
  rejected because host product semantics leaked into the library and forced
  synchronized changes across host and Graphon.
- Use an event-style callback boundary:
  rejected because HITL here is a single control-flow decision, not a stream of
  intermediate events.
- Return generic `NodeRunResult` values from the callback:
  rejected because `Completed` and `Expired` make the HITL boundary narrower
  and more self-descriptive than a generic node result type.
- Return serialized JSON-like payloads instead of runtime `Segment` values:
  rejected because value restoration belongs to the host, and Graphon should
  consume runtime values rather than host serialization semantics.
- Keep rich pause payloads with form content and resolved defaults:
  rejected because that makes Graphon responsible for host-side storage,
  delivery, and replay semantics.
- Move all HITL behavior into the host and make Graphon unaware of HITL:
  rejected because Graphon still needs to own the workflow control-flow
  semantics of pause, completion, expiration, and selected edge handling.

## Rollout Notes

- PR #185 introduced the callback boundary and removed Dify-specific form
  entities and runtime bindings from the supported Graphon interface.
- Legacy `human_input_required` pause reasons are still normalized when
  deserializing persisted payloads, but that compatibility path is not the
  canonical contract.
