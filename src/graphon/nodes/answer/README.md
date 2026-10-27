<!-- knowledge
last_checked: "2026-10-27T05:33:21Z"
-->
# Answer outputs

[AnswerNode](answer_node.py) computes completed values through the ordinary node
lifecycle. Its `NodeRunSucceededEvent.node_run_result.outputs` contains:

| Key | Value |
| --- | --- |
| `answer` | The rendered Markdown string |
| `files` | An [ArrayFileSegment](../../variables/segments.py) containing the selected files |

Single-file and file-array template segments are flattened into `files` in
template order, preserving repeated references and excluding unreferenced files.
Text-only Answers have an empty file array. Files also render as Markdown within
`answer`, using the engine's bound [file runtime](../../file/runtime.py).
Consumers read the selected files from `node_run_result.outputs["files"].value`.

The [event processor](../../engine/event/processor.py) stores both outputs in the
variable pool, retaining selectors `[answer_id, "answer"]` and
`[answer_id, "files"]`, and merges response outputs into the frame's workflow
outputs. [Response-output merging](../../runtime/runtime_state/state.py)
concatenates `answer` and replaces other keys; workflow-final `files` is therefore
not an aggregate of every Answer. Hosts needing files from each Answer consume
the individual node-success events, which `ResponseStreamFilter` preserves.

`get_streaming_template()` exposes the declarative template and
`get_streaming_text_selector()` returns `[answer_id, "answer"]`. Answer and the
engine do not synthesize response chunks. The external
[ResponseStreamFilter](../../engine/filter/builtin/response_stream/filter.py)
owns incremental presentation.

The [Answer tests](../../../../tests/nodes/answer/test_answer_node.py) cover
ordinary lifecycle, template access, text/file rendering, file output selection,
and delivery through raw and filtered events.
