<!-- knowledge
last_checked: "2026-09-09T21:56:06Z"
-->
# Repository knowledge

Start with the question you need to answer, then follow the links to code and
tests. These documents describe this checkout; they do not substitute for the
implementation or establish new public contracts.

## Map

| Question | Start here |
| --- | --- |
| What is Graphon and how do I run it? | [Project README](../README.md), [Slim example](../examples/slim_llm/README.md) |
| Where does execution happen, and who owns state? | [Architecture](../ARCHITECTURE.md) |
| How should runtime dependencies and mutable state be owned? | [Explicit state ownership](../CONTRIBUTING.md#explicit-state-ownership) |
| Which files and tests should I change? | [Development guide](development.md) |
| How do I set up, check, and contribute changes? | [Contributing](../CONTRIBUTING.md) |
| How do I keep knowledge current or record a longer task? | [Knowledge maintenance](maintenance.md) |
| What unresolved debt should improve next? | [Technical debt](technical-debt.md) |

## Component references

Keep detailed component knowledge next to its implementation and link it here.

| Topic | Reference |
| --- | --- |
| Engine lifecycle extensions | [Layers](../src/graphon/engine/layer/README.md) |
| Pause, abort, variable updates | [Commands](../src/graphon/engine/command/README.md) |
| Execution and host file rendering | [State ownership](../ARCHITECTURE.md#state-and-execution-invariants) |
| Model capabilities and providers | [Model runtime](../src/graphon/model_runtime/README.md) |
| DSL and direct Python construction | [Slim example setup](../examples/slim_llm/README.md) |

## Evidence and review

Follow [maintenance guidance](maintenance.md) for review evidence, validation,
and the lifecycle of active and archived knowledge.

## Related work

See [decision-record work](technical-debt.md#td-10--decision-records) before
introducing decision conventions.

This layout applies the small entry point, linked repository knowledge, and
mechanical upkeep described in OpenAI's
[harness engineering article](https://openai.com/index/harness-engineering/).
