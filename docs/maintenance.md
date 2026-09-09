<!-- knowledge
last_checked: "2026-09-09T21:27:27Z"
-->
# Maintaining repository knowledge

The [knowledge index](README.md) is the entry point for durable context.
[CONTRIBUTING.md](../CONTRIBUTING.md) remains the authority for contributor
workflow. The author of a change updates affected knowledge; its reviewer checks
the claims against the linked implementation and evidence.

## Repository knowledge maintenance

Apply these principles to both everyday documentation changes and full inspections:

- **Verify claims against evidence.** Read the relevant implementation, callers,
  tests, and tooling before correcting a claim. Investigate disagreements between
  code and prose; do not infer design intent from implementation shape.
- **Separate current behavior, proposals, and history.** Confirm issue/PR status
  before calling work complete. A local implementation is not a merged change,
  and a merge is not proof of a release. Label proposals and uncertainty explicitly;
  consult [related work](README.md#related-work) before introducing new conventions.
- **Keep guidance useful beyond one task.** Retain purpose, ownership, constraints,
  failure modes, compatibility requirements, and verification paths. Remove
  conversation history, temporary branch ownership, one-off progress reports,
  and unsupported assertions from active guides.
- **Give each fact a stable home.** Use the placement table below and link to
  maintained definitions instead of copying commands, API lists, or examples.
  Keep AGENTS.md and the knowledge index small and navigable. Link active pages
  from the index or an already indexed guide, and repair references when files move.
- **Retire completed work.** Extract reusable guidance before removing resolved
  debt and completed tasks from active documents. Archive completed plans under
  `docs/archive/plans/` using the [archive rules](#archive-completed-work); keep
  historical material out of routine reviews and freshness checks.
- **Keep review evidence honest.** Advance `last_checked` only after reviewing the
  whole page. Distinguish reading source or tests from running them. Record actual
  check results and limitations; never relabel historical test counts as current
  results or refresh timestamps just to pass CI.
- **Make focused corrections and validate them.** Remove stale, invalid, redundant,
  or overly specific content while preserving useful decisions and constraints.
  Avoid edits made solely to produce a cleanup diff. Mechanical checks do not
  establish prose accuracy; verify claims as well as navigation and examples.

### Repository-wide inspections

A scheduled or explicitly requested full inspection has a broader scope than a
normal change's [knowledge review](#review-for-drift):

1. Check existing issues and PRs before starting. For a new inspection, fetch the
   latest default branch and create a fresh worktree from that fetched revision.
   Continue follow-up revisions on the existing review branch instead of opening
   a duplicate PR.
2. Start at the [knowledge index](README.md), follow its links, and reconcile it
   with the [maintained document scope](#document-metadata-and-review-deadline)
   to catch unindexed pages. Exclude `docs/archive/`.
3. Review every active page against the checked revision, applying the principles
   above. Verify external tracking claims when relevant, correct the content, and
   only then update that page's review timestamp.
4. Run [validation](#validation) and complete the independent knowledge review
   followed by the final naming pass required by
   [CONTRIBUTING.md](../CONTRIBUTING.md#context-free-reviews).
5. Submit the revisions in a focused PR linked to its tracking issue. Report the
   checked revision, reviewed scope, actual check results, and unresolved limits
   there; keep run-specific reports out of permanent guidance. Follow the existing
   [PR requirements](../CONTRIBUTING.md#pull-requests).

## Put information where it belongs

| Information | Home |
| --- | --- |
| Setup, commands, CI, contribution policy | [CONTRIBUTING.md](../CONTRIBUTING.md) |
| Package boundaries and execution flow | [ARCHITECTURE.md](../ARCHITECTURE.md) |
| How to locate code, extend it, and exercise it | [Development guide](development.md) |
| Component-specific details | The component's existing README, linked from the index |
| Unresolved technical debt | [Technical debt](technical-debt.md) |
| A complex task's progress and unresolved decisions | A linked plan under `docs/plans/`, created when needed |
| Completed plans retained as history | [Archived plans](archive/plans/), excluded from routine knowledge checks |

## Validation

Use relative inline Markdown links for repository files, including evidence in
tables. Keep examples inside fenced code blocks. The existing test suite checks
links in root Markdown files and Markdown files under `docs/`, `src/`, and
`examples/`, excluding `docs/archive/`:

```bash
uv run pytest -n 0 tests/test_repository_docs.py
```

The [documentation check](../tests/test_repository_docs.py) catches missing local
paths and enforces reachability from `AGENTS.md` for `ARCHITECTURE.md` and active
pages under `docs/`, plus the review metadata below.
It does not validate heading anchors, external URLs, reference-style links, or
prose accuracy. When it fails, repair the link, add a route from an indexed page,
or review the overdue document; do not weaken the check to hide obsolete knowledge.
Manually verify affected heading anchors, external references, and examples that
the checker cannot validate. Run the relevant behavior checks and broader
[repository validation](../CONTRIBUTING.md#testing-and-validation) for the change.

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

For a normal development change, limit this review and its edits to the revision's
changed behavior, APIs, workflows, guides, and references. Touching a document
does not bring every section into scope. A full inspection is a separate
[explicitly requested or scheduled workflow](#repository-wide-inspections), not
an automatic addition to every code change.

The independent reviewer applies the [maintenance principles](#repository-knowledge-maintenance)
to the relevant claims and makes necessary corrections itself. After edits, repair
affected links and run the documentation check. Report the changes made or that
no relevant cleanup was needed; keep review dates limited to whole pages actually
checked.
