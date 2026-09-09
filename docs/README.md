<!-- knowledge
last_checked: "2026-09-08T17:06:45Z"
-->
# Repository knowledge

Start with the question you need to answer, then follow the links to code and
tests. These documents describe this checkout; they do not substitute for the
implementation or establish new public contracts.

At the start of every development workflow, read and follow
[Development Style](../CONTRIBUTING.md#development-style) for tests first,
independent test and knowledge reviews, and the final naming pass.

## Map

| Question | Start here |
| --- | --- |
| What is Graphon and how do I run it? | [Project README](../README.md), [Slim example](../examples/slim_llm/README.md) |
| Where does execution happen, and who owns state? | [Architecture](../ARCHITECTURE.md) |
| How should runtime dependencies and mutable state be owned? | [Explicit state ownership](../CONTRIBUTING.md#explicit-state-ownership) |
| Which files and tests should I change? | [Development guide](development.md) |
| How do I set up, check, and contribute changes? | [Contributing](../CONTRIBUTING.md) |
| How do I keep knowledge current or record a longer task? | [Knowledge maintenance](maintenance.md) |
| What breaks when upgrading? | [Migration guide](../MIGRATION.md), [changelog](../CHANGELOG.md) |
| What structural debt has been identified, and what should improve next? | [Architecture review and technical debt](technical-debt.md) |
| What is the progress and evidence for TD-02? | [Graph validation plan](plans/graph-validation.md) |
| What is the progress and evidence for TD-03? | [Snapshot eligibility plan](plans/snapshot-eligibility.md) |
| What is the progress and evidence for TD-04? | [Execution file runtime plan](plans/execution-file-runtime.md) |
| What is the progress and evidence for TD-01 and TD-05? | [Runtime import boundaries plan](plans/runtime-import-boundaries.md) |

## Component references

Keep detailed component knowledge next to its implementation and link it here.

| Topic | Reference | Implementation / executable evidence |
| --- | --- | --- |
| Engine lifecycle extensions | [Layers](../src/graphon/engine/layer/README.md) | [Layer base](../src/graphon/engine/layer/base.py), [worker context tests](../tests/engine/test_layer_node_run_context.py) |
| Pause, abort, variable updates | [Commands](../src/graphon/engine/command/README.md) | [Command processor](../src/graphon/engine/command/processor.py), [dispatch tests](../tests/engine/test_dispatch_patterns.py) |
| Execution and host file rendering | [File runtime isolation](../MIGRATION.md#file-runtime-isolation) | [File runtime](../src/graphon/file/runtime.py), [execution file tests](../tests/engine/test_file_runtime.py) |
| Model capabilities and providers | [Model runtime](../src/graphon/model_runtime/README.md) | [Public protocols](../src/graphon/protocols/__init__.py), [model dispatch tests](../tests/model_runtime/test_model_dispatch.py) |
| DSL and direct Python construction | [Slim example setup](../examples/slim_llm/README.md) | [DSL example](../examples/slim_llm/dsl.py), [Python example](../examples/slim_llm/code.py), [example tests](../tests/examples/test_slim_llm_example.py) |

## Evidence and review

Each page records its review timestamp using the shared
[document metadata format](maintenance.md#document-metadata-and-review-deadline).
This is not a claim that all behavior or external integrations have been
exercised. Linked tests show where to verify a change;
[Contributing](../CONTRIBUTING.md) defines the validation workflow.

The [documentation test](../tests/test_repository_docs.py) checks local file
links, whether this knowledge base is reachable from [AGENTS.md](../AGENTS.md),
and whether every maintained page has been checked within seven days. It runs
as part of the normal pytest suite in CI. Recorded timestamps do not
prove that prose matches behavior; review source and tests when updating a page.

## Related work

As checked on 2026-09-08, design-decision capture is being discussed in
[issue #199](https://github.com/langgenius/graphon/issues/199) and implemented in
[PR #200](https://github.com/langgenius/graphon/pull/200). Those proposals are not
accepted policy in this checkout. When they land, link the decision index here
and reconcile the maintenance guidance with it.

This layout applies the small entry point, linked repository knowledge, and
mechanical upkeep described in OpenAI's
[harness engineering article](https://openai.com/index/harness-engineering/).
