# ADR Backlog

This backlog lists historical pull request groups that should be backfilled as
ADRs.

The unit of work is one ADR per durable decision, not one ADR per pull request.

Many older boundaries in this repository were extracted from Dify. For now,
this backlog intentionally keeps only the Graphon-specific decisions that are
most worth recording first. Other historical changes can be reviewed later.

## Current Priority

### HITL interface

- Related PRs:
  [feat(human-input)!: extract Dify logic from HITL node](https://github.com/langgenius/graphon/pull/185)
- Why it needs an ADR:
  this change is a clear Graphon-owned boundary decision. It removes
  Dify-specific form entities and logic from core Graphon and redefines what
  the human-input node interface is responsible for.

### LLM Polling

- Related PRs:
  [feat: add LLM polling runtime support](https://github.com/langgenius/graphon/pull/151)
- Why it needs an ADR:
  this introduces a new execution model for asynchronous LLM completion and is
  one of the clearest Graphon-native runtime decisions worth documenting early.
