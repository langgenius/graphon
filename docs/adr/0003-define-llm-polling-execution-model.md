# ADR 0003: Define LLM Polling Execution Model

- Status: Accepted
- Date: 2026-06-29
- Related PRs: #151, #184
- Supersedes: N/A
- Superseded by: N/A

## Context

Some LLM runtimes cannot return a terminal result from a single invoke call.
Instead, they return a pending state that must be checked until the job
finishes or fails.

Graphon therefore needed a supported execution model for polling-capable LLM
runtimes.

An early direction was to model polling through Graphon's suspension and
resumption path, similar to the HITL flow. That was attractive when Dify Cloud
was believed to enforce a ten-minute workflow runtime limit, because a
continuously running workflow seemed impossible there.

That assumption later changed:

- Dify Cloud workflow runtime had already been relaxed to one hour, so keeping
  the workflow running was no longer blocked by the old ten-minute limit.
- Plugin execution on Dify Cloud is still limited to ten minutes per run, so
  the polling loop cannot live inside the plugin itself.

With that constraint set, the simpler question became: should polling be
modeled as suspension, or as continued execution of a running Graphon LLM node?

## Decision

Graphon models LLM polling as continued execution of a running LLM node.

The supported polling contract is:

- polling is an optional capability layered on top of the normal `LLMProtocol`
  path via `LLMPollingCapableProtocol`
- the runtime starts polling through `start_llm_polling(...)`
- while the job is still running, the runtime returns `LLMPollingResult` with
  `status=RUNNING` plus `plugin_state`
- Graphon remains in control of the polling loop, waits between checks, and
  continues polling through `check_llm_polling(plugin_state=...)`
- polling progress is surfaced through lightweight Graphon events
- non-polling LLM runtimes stay on the existing invoke path

Polling is not modeled as suspension or resumption, and the supported polling
protocol does not carry workflow or node identity fields such as
`workflow_run_id` or `node_id`.

## Consequences

- Graphon and the embedding host remain responsible for keeping the workflow
  alive during polling.
- Polling cannot be delegated to a long-running plugin loop on Dify Cloud,
  because plugin execution time remains separately constrained.
- Polling-capable runtimes only need to persist and round-trip `plugin_state`
  across checks.
- Graphon owns timeout, retry cadence, and progress visibility around the
  polling loop.

## Alternatives Considered

- Use Graphon suspension and resumption, similar to HITL:
  rejected because it introduces a more complex protocol and lifecycle than the
  problem requires, while the original ten-minute Dify Cloud workflow limit is
  no longer the blocking constraint it was once thought to be.
- Keep polling inside the plugin runtime:
  rejected because plugin execution on Dify Cloud is still limited to ten
  minutes per run, so the plugin cannot safely own the long-running polling
  loop.
- Replace the normal invoke path with polling for all LLM runtimes:
  rejected because polling is only needed for a subset of runtimes, and
  non-polling models should keep the simpler existing execution path.

## Rollout Notes

- PR #151 introduced the initial polling capability, polling result/config
  entities, and progress events.
- PR #184 removed `workflow_run_id` and `node_id` from the public polling
  protocol after the suspension-based route was rejected.
