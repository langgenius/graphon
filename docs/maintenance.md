<!-- knowledge
last_checked: "2026-09-09T21:14:48Z"
-->
# Maintaining repository knowledge

The [knowledge index](README.md) is the entry point for durable context.
[CONTRIBUTING.md](../CONTRIBUTING.md) remains the authority for contributor
workflow. The author of a change updates affected knowledge; its reviewer checks
the claims against the linked implementation and evidence.

## Put information where it belongs

| Information | Home |
| --- | --- |
| Setup, commands, CI, contribution policy | [CONTRIBUTING.md](../CONTRIBUTING.md) |
| Package boundaries and execution flow | [ARCHITECTURE.md](../ARCHITECTURE.md) |
| How to locate code, extend it, and exercise it | [Development guide](development.md) |
| Component-specific details | The component's existing README, linked from the index |
| A complex task's progress and unresolved decisions | A linked plan under `docs/plans/`, created when needed |
| Completed plans retained as history | [Archived plans](archive/plans/), excluded from routine knowledge checks |

Prefer a link to a maintained definition over another copied command, API list,
or code example. Record only context that helps someone make a change: purpose,
ownership, constraints, failure modes, and how to verify it. Link to source files
and focused tests so the reader can check your claim.

Separate observed implementation from intentional contracts and proposals.
Do not infer design rationale from a code shape, backfill an approval that did
not happen, or describe an open issue as shipped. Decision-record work already
exists in [PR #200](https://github.com/langgenius/graphon/pull/200); consult it
before starting overlapping work.

## Keep a change self-contained

1. Read the relevant guide and linked code before editing.
2. Update the guide when changing its behavior, boundary, public name, or path.
3. Link new knowledge from the index or an already indexed guide. Remove obsolete
   text rather than accumulating contradictory instructions.
4. Keep any review date honest: advance it only after checking that page's claims.
5. Run the documentation check and the behavior checks appropriate to the change.

Use relative inline Markdown links for repository files, including evidence in
tables. Keep examples inside fenced code blocks. The existing test suite checks
links in root Markdown files and Markdown files under `docs/`, `src/`, and
`examples/`, excluding `docs/archive/`:

```bash
uv run pytest -n 0 tests/test_repository_docs.py
```

The check catches missing local paths and enforces reachability from `AGENTS.md`
for `ARCHITECTURE.md` and active pages under `docs/`, plus the review metadata below.
It does not validate heading anchors, external URLs, reference-style links, or
prose accuracy. When it fails, repair the link, add a route from an indexed page,
or review the overdue document; do not weaken the check to hide obsolete knowledge.

## Document metadata and review deadline

Every maintained knowledge document starts with this YAML metadata in an HTML
comment, before its title. The comment stays hidden in rendered Markdown,
including the package README:

```markdown
<!-- knowledge
last_checked: "2026-09-08T00:00:00Z"
-->
# Document title
```

`last_checked` is required: a UTC timestamp in `YYYY-MM-DDTHH:MM:SSZ` format,
stored as a YAML string. Use quotes as shown above to prevent YAML from converting
it to a timestamp value. It records when the page's claims were last checked
against their source and evidence, not the file modification time or proof that
every linked test was executed. Advance it only after reviewing the page,
correcting stale claims, and checking its links. A partial edit does not renew
an entire page's review. Other descriptive YAML fields may be added when needed;
CI requires only `last_checked`.

The same format applies to all root Markdown files except `CLA.md` (the legal
agreement), and Markdown files under `docs/`, `src/`, and `examples/`, except
`docs/archive/`. New active nested knowledge bases and non-README guides are
included automatically. Archived plans and templates under `.github/` are outside
the maintained knowledge scope.

The documentation check rejects missing, malformed, or future timestamps. It
compares the earliest `last_checked` across the entire scope with the current UTC
time and fails when its age exceeds seven 24-hour days; exactly seven days passes.
The failure identifies the oldest file and its timestamp.

The check runs in the normal pytest suite, including the existing CI jobs for
pull requests targeting `main` and release tags, and with the focused command
above. It scans the full maintained scope without changed-file filters. Archives
are excluded from review metadata, freshness, outgoing-link, and reachability
checks. Links from active documents must still point to existing paths, including
links into the archive.

## Plans for work that needs durable context

Small changes can keep their plan in the working conversation. For work spanning
sessions, multiple dependent changes, or unresolved tradeoffs, create
`docs/plans/<topic>.md` and link it from the knowledge index. Use ordinary Markdown;
no generator or separate planning tool is required.

Include these fields and enough detail for a new contributor to resume:

- **Status and last update:** active, blocked, or completed, with a date.
- **Objective and scope:** the concrete outcome and constraints.
- **Context:** relevant source, tests, issue/PR links, and accepted decisions.
- **Steps:** a short checklist with completed work marked accurately.
- **Decision log:** alternatives, chosen direction, rationale, and open questions.
- **Validation and outcome:** commands, observed results, remaining risks, and
  follow-ups with links.

Update the plan as work progresses. Never record credentials or private
operational data in it. On completion, follow the archive rules below.

## Archive completed work

Keep active knowledge focused on current behavior and unfinished work.

1. Before removing a completed task or resolved debt item, extract reusable
   guidance into its stable home: architecture for invariants, the development
   guide for workflows, a component README for local behavior, or CONTRIBUTING.md
   for contributor rules. Reuse existing explanations and link source/tests;
   do not copy task history into general guidance or promote an unaccepted proposal
   into policy.
2. Move completed plans from `docs/plans/` to `docs/archive/plans/`, preserving
   their original directory under the archive. Retain useful decisions, completion
   evidence, and original review timestamps as history.
   Repair links affected by the move and remove links to deleted debt sections.
   Record any unfinished follow-up in an active issue or separate plan.
3. Remove completed task entries, resolved debt sections, obsolete status reports,
   and their individual links from the active knowledge index and guides. Keep
   `technical-debt.md` limited to unresolved items; do not renumber surviving IDs.
   Use archived plans and Git history when completion details are needed.
4. Exclude `docs/archive/` from scheduled reviews and routine knowledge update
   checks. Do not refresh archive timestamps or rewrite historical results to
   match current APIs. If archived work becomes active again, move it back under
   `docs/plans/`, review the whole page, restore its index link, and record a fresh
   timestamp before continuing.

## Review for drift

At the end of each development cycle, complete the independent knowledge review
required by [Development Style](../CONTRIBUTING.md#context-free-reviews) before
the final naming pass. Give the subagent the revision's requirements and diff so
it can identify the affected knowledge through the index and relevant links.

Limit both review and edits exclusively to information relevant to this revision:
changed behavior, APIs, workflows, affected guides, and their navigation or
source/test references. A document being touched does not make every section
in scope. Do not turn this step into a repository-wide audit or unrelated cleanup.

The subagent must compare relevant claims against current source, tests, and
requirements, then apply necessary corrections. Remove redundant explanations,
prefer links to maintained definitions, and shorten detail that does not help
with the revised behavior or workflow. Preserve required rules, compatibility
guidance, useful decisions, and supporting evidence. Do not make edits merely to
produce a cleanup diff.

After editing, repair affected links and run the documentation check above.
Report the changes made, or that no relevant cleanup was needed. Keep review
dates limited to claims actually checked. Broader audits remain outside this step.
