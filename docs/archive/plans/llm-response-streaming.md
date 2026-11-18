<!-- knowledge
last_checked: "2026-11-18T15:56:37Z"
-->
# Present complete LLM text in Graphon replies

The local implementation and validation finished on 2026-11-18.
PR #395 was still open for issue #389 on that date. [Issue #389](https://github.com/langgenius/graphon/issues/389) · [PR #395](https://github.com/langgenius/graphon/pull/395)

## Decisions and evidence

- Sending the complete text in the LLM's final chunk fixed a simple polling reply, but a later Assigner could clear that value before Answer ran.
  The engine now reports each explicit `[node_id, "text"]` lookup by a successful Answer or End, and the filter uses those reads for whole LLM results in root and child frames. [Raw frame observations](../../../ARCHITECTURE.md#raw-frame-observations) · [Queued Answer and End tests](../../../tests/engine/test_llm_reply_values.py)
- In a mixed reply, the filter displayed the token `X` before an Assigner overwrote the source and a command changed another selector between lookups.
  The saved Answer was `edited|first/next`, and the required stream was `X|first/next`.
  The two occurrences of the whole LLM result needed separate reads. [Token and repeated read tests](../../../tests/engine/test_llm_answer_streaming.py)
- The version 3.1 root checkpoint saves those unread values.
  Earlier writers saved identical runtime and filter state when one run read `A` then `BC` and another read `AB` then `C`.
  Both Answers saved `ABC`.
  Restoring that pair preserves the original reader's stream cursor, including its empty final marker. [Response sessions](../../../ARCHITECTURE.md#response-sessions) · [Paired resume tests](../../../tests/engine/test_llm_reply_resume.py)

## Validation and follow-up

`just test` and `just check` passed locally.
The suite passed all 1,455 tests on each of Python 3.12, 3.13, and 3.14, including the existing graph traversal and retained memory checks. [Complexity tests](../../../tests/engine/test_response_stream_filter_complexity.py)

Draft #412 tracks child checkpoints for stage F of issue #356.
At `538a0af`, its reader accepted a 2.0 checkpoint and rejected both 3.0 and 3.1.
Migration of those root checkpoints into the draft's version 4 format remained open. [Stage F](https://github.com/langgenius/graphon/issues/356) · [Draft #412](https://github.com/langgenius/graphon/pull/412) · [Draft codecs at 538a0af](https://github.com/langgenius/graphon/tree/538a0af1687a5cc448b6c4301525ed48d8b95d59/src/graphon/engine/filter/builtin/response_stream)

Issue #414 tracks queued Answers that read a Template Transform output before a later edit. [Issue #414](https://github.com/langgenius/graphon/issues/414)
