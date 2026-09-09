<!-- knowledge
last_checked: "2026-09-09T21:14:48Z"
-->
# Technical debt

Unresolved work only. These proposals are not accepted architecture policy or
assigned tasks. Current behavior and boundaries live in
[Architecture](../ARCHITECTURE.md); contributor workflow lives in
[CONTRIBUTING.md](../CONTRIBUTING.md). Follow the
[completion and archive rules](maintenance.md#archive-completed-work) when an
item is resolved, retaining the IDs of remaining items.

| ID | Unresolved work | Priority | Area |
| --- | --- | --- | --- |
| TD-06 | [Ignored LLM integration arguments](#td-06--ignored-llm-integration-arguments) | P2 | Model/node API |
| TD-07 | [Optional capability dependencies](#td-07--optional-capability-dependencies) | P2 | Packaging/document extraction |
| TD-08 | [Offline execution example](#td-08--offline-execution-example) | P2 | Examples |
| TD-09 | [Performance feedback](#td-09--performance-feedback) | P2 | Filters/performance |
| TD-10 | [Decision records](#td-10--decision-records) | P3 | Maintainers/docs |

P2 denotes targeted API, architecture, or developer-feedback work; P3 can follow
more consequential changes. Priorities are suggestions, not delivery commitments.
Before starting, repeat the issue/PR search required by
[Contributing](../CONTRIBUTING.md#issues) and record the selected work's owner and
tracking link. TD-06 needs an API transition decision; TD-08 can support TD-07's
minimal-install validation. TD-09 and TD-10 already have open work to continue.

## TD-06 — Ignored LLM integration arguments

[LLMNode](../src/graphon/nodes/llm/node.py),
[QuestionClassifierNode](../src/graphon/nodes/question_classifier/question_classifier_node.py),
and [ParameterExtractorNode](../src/graphon/nodes/parameter_extractor/parameter_extractor_node.py)
accept and discard `credentials_provider` and `model_factory`; the first two also
ignore `http_client`. [ModelFactory and CredentialsProvider](../src/graphon/nodes/llm/protocols.py)
remain publicly exported. Supplied configuration can therefore have no effect.

Document the effective replacements and choose a compatibility window before
rejecting or removing arguments. Prepared `model_instance` injection is the
current graph-facing model seam; do not build an unused factory just to justify
an existing parameter. See the [host integration guide](development.md#integrate-host-services).

**Completion:** tests cover supplied ignored arguments on all three nodes, the
transition is documented, and examples use effective dependencies. Preserve
[LLM](../tests/nodes/llm/test_node.py),
[classifier](../tests/nodes/question_classifier/test_question_classifier_node.py),
and [extractor](../tests/nodes/parameter_extractor/test_prompts.py) behavior.

## TD-07 — Optional capability dependencies

Document-extraction packages are mandatory in [pyproject.toml](../pyproject.toml),
and [the extractor](../src/graphon/nodes/document_extractor/node.py) imports several
eagerly. Minimal engine consumers inherit these dependencies, including the Python
upper bound tied to `unstructured`. No size or startup benefit has been measured.

Consider one document-extraction extra within the existing distribution. Isolate
its imports, provide an actionable missing-extra error, retain a fully equipped
development environment, and document the installation transition. Evaluate the
[tokenizer fallback](../src/graphon/model_runtime/model_providers/base/tokenizers/gpt2_tokenizer.py)
separately if needed.

**Completion:** a clean base-only installation executes a minimal graph; the full
extra passes [extractor tests](../tests/nodes/document_extractor/) and package/lockfile
checks. Moving `unstructured` alone does not establish Python 3.14 support; verify
the remaining dependencies and version matrix before changing that claim.

## TD-08 — Offline execution example

The public [Slim examples](../examples/slim_llm/README.md) need credentials and a
daemon. [Example tests](../tests/examples/test_slim_llm_example.py) exercise setup
helpers, so contributors lack a documented standalone workflow that separates
engine behavior from external setup.

Publish one small credential-free Start → Template → End example using public
APIs and the workload pattern in [the benchmark](../benchmarks/test_engine_performance.py).
Show final output and raw node events; link existing
[layer guidance](../src/graphon/engine/layer/README.md) for deeper diagnosis.

**Completion:** the documented command runs after setup with network access
disabled and no credentials or daemon. One smoke check verifies final output and
successful node order. Keep the example small enough to serve as a reproduction.

## TD-09 — Performance feedback

[The benchmark](../benchmarks/test_engine_performance.py) measures two rounds of a
five-node sequential execution. Graph construction happens outside the timer;
response-filter initialization is absent. Those timings cannot establish the
cost of construction, filtering, branching, or larger graphs.

Continue [PR #271](https://github.com/langgenius/graphon/pull/271) for the known
response-filter initialization path and
[PR #266](https://github.com/langgenius/graphon/pull/266) for benchmark instructions.
Prefer deterministic complexity checks for that path and identify which phase a
measurement covers. Add timing workloads only for a specific performance question.

**Completion:** a runnable filter regression preserves output correctness, the
command is discoverable, and results distinguish construction, initialization,
and execution. Preserve existing benchmarks and
[response-filter tests](../tests/engine/test_response_stream_filter.py).

## TD-10 — Decision records

[Issue #199](https://github.com/langgenius/graphon/issues/199) and
[PR #200](https://github.com/langgenius/graphon/pull/200) cover decision records,
including HITL and model polling boundaries. Current behavior guides do not
substitute for accepted decisions.

Continue that work and reconcile it with [maintenance guidance](maintenance.md).
Promote approved lasting rules to stable guides and executable checks.

**Completion:** accepted decisions have repository-local links and explicit
status, and the knowledge index and maintenance guidance agree with merged policy.
