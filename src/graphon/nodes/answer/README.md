<!-- knowledge
last_checked: "2026-10-26T16:51:24Z"
-->
# Answer output and attachments

[AnswerNode](answer_node.py) returns exactly one completed output:
`outputs={"answer": rendered_value}`. The ordinary node lifecycle publishes it
on `NodeRunSucceededEvent`; `[answer_id, "answer"]` remains a variable selector.
`get_streaming_template()` exposes the declarative template. Answer and the engine
do not synthesize response chunks; the external
[ResponseStreamFilter](../../engine/filter/builtin/response_stream/filter.py)
owns incremental presentation.

Files still render as Markdown within `answer`, using the engine's bound
[file runtime](../../file/runtime.py). The same selected files are available as
`event.node_run_result.attachments`, a sequence of typed
[File](../../file/models.py) references outside the output-variable mapping.
Single-file and file-array segments are flattened in template order, preserving
repeated references. Unreferenced files are excluded; text-only Answers have an
empty attachment list. Other nodes default to empty attachments and retain their
existing outputs.

The existing success event carries attachments through raw and filtered streams.
A host can serialize the event with `model_dump_json()` and restore typed files
with `NodeRunSucceededEvent.model_validate_json()`. Bind the host's file adapter
when resolving URLs or calling `File.to_dict()` outside engine execution:

```python
from graphon.engine_events import NodeRunSucceededEvent
from graphon.file.runtime import use_workflow_file_runtime

for event in events:  # Engine.run() or filter_engine_events(...)
    if isinstance(event, NodeRunSucceededEvent):
        with use_workflow_file_runtime(engine.file_runtime):
            attachments = [file.to_dict() for file in event.node_run_result.attachments]
        # Deliver/persist attachments with event.id in the host's node-success handler.
```

The [event processor](../../engine/event/processor.py) stores only `outputs` in
variables and workflow outputs. Attachments are not accumulated into
`GraphRunSucceededEvent.outputs` or
[runtime snapshots](../../runtime/runtime_state/snapshot.py).
Persist attachment delivery in the host as success events arrive, using execution
identity rather than node ID for repeated container runs. Retain that host state
across pause/resume; a runtime snapshot cannot replay completed attachment events.
The [Answer tests](../../../../tests/nodes/answer/test_answer_node.py) cover the
output boundary, lifecycle, rendering, filtered delivery, and event serialization.

## Migration from the `files` output

Removing `outputs["files"]` is a breaking contract change. There is no second
output or `[answer_id, "attachments"]` selector.

| Consumer | Required change |
| --- | --- |
| Node-success attachment delivery | Read `node_run_result.attachments` instead of `outputs["files"].value` |
| Workflow-final or blocking response files | Collect and persist attachments from node-success events; do not derive Answer files from workflow outputs |
| Workflow selectors such as `[answer_id, "files"]` | Reference the original file-producing node; combine source files explicitly when the old Answer selected multiple sources |
| Stored node results / older event payloads | Read historical `outputs["files"]` for old records; payloads missing `attachments` load with an empty sequence, without automatic conversion |

Finish existing runs on the old Graphon pin before adopting the new contract.
Restored old snapshots may still contain historical `files` variables or workflow
outputs; they are not rewritten or converted into attachment deliveries. A host
choosing to migrate in-flight runs must preserve its already delivered attachments
and migrate those saved values explicitly.

### Dify integration prerequisites

The downstream audit at Dify
[`8845095`](https://github.com/langgenius/dify/tree/88450956962dc4ea0e9add82f6d0b8989bd1df5e)
found these consumers. Its
[dependency pin](https://github.com/langgenius/dify/blob/88450956962dc4ea0e9add82f6d0b8989bd1df5e/api/pyproject.toml)
does not yet include this contract. Before upgrading it:

1. Forward `node_run_result.attachments` from
   [WorkflowAppRunner](https://github.com/langgenius/dify/blob/88450956962dc4ea0e9add82f6d0b8989bd1df5e/api/core/app/apps/workflow_app_runner.py)
   through a dedicated field on
   [QueueNodeSucceededEvent](https://github.com/langgenius/dify/blob/88450956962dc4ea0e9add82f6d0b8989bd1df5e/api/core/app/entities/queue_entities.py).
   Preserve typed references or serialize them with the host file adapter; do not
   insert them back into node outputs.
2. Change the Answer branch of
   [AdvancedChatAppGenerateTaskPipeline](https://github.com/langgenius/dify/blob/88450956962dc4ea0e9add82f6d0b8989bd1df5e/api/core/app/apps/advanced_chat/generate_task_pipeline.py)
   to populate `_recorded_files` from that field. This feeds assistant message-file
   persistence and final message files. Keep the existing End/LLM output scanning.
3. Update
   [WorkflowResponseConverter](https://github.com/langgenius/dify/blob/88450956962dc4ea0e9add82f6d0b8989bd1df5e/api/core/app/apps/common/workflow_response_converter.py)
   node-finish `files` and both live and stored workflow-finish responses. Its current
   `fetch_files_from_node_outputs` cannot discover files inside rendered Markdown.
   Preserve output scanning for other node types and legacy stored records.
4. Add attachment persistence to
   [WorkflowPersistenceLayer](https://github.com/langgenius/dify/blob/88450956962dc4ea0e9add82f6d0b8989bd1df5e/api/services/workflow_persistence.py)
   and the single-step result path in
   [WorkflowService](https://github.com/langgenius/dify/blob/88450956962dc4ea0e9add82f6d0b8989bd1df5e/api/services/workflow_service.py).
   Both currently copy outputs into execution records without an attachment field.
   Carry attachments through the host's storage and response schemas separately
   from node/workflow outputs, and restore the host collection on resume. Updating
   the queue event alone does not preserve attachments in stored executions.
5. Audit saved workflows for Answer `files` selectors and migrate them as above.
   The checked Graphon fixtures contain no such selector; this is not evidence
   about user-authored workflows. Update the downstream Answer, runner, converter,
   message persistence, and resume coverage together with the pin upgrade.

The later Dify integration stage (H) for
[graphon#356](https://github.com/langgenius/graphon/issues/356)
upgrades the dependency pin to the published container-streaming implementation,
saves/restores paired engine/filter snapshots, and removes Dify's nested-Answer
success-time text fallback. Remove that fallback only when filter-produced child
replies are available. This attachment migration alone does not implement child
streaming or make the full dependency upgrade ready.
