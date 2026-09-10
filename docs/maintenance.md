<!-- knowledge
last_checked: "2026-09-10T01:03:07Z"
-->
# Maintaining repository knowledge

The [knowledge index](README.md) is the entry point for durable context.
[CONTRIBUTING.md](../CONTRIBUTING.md) remains the authority for contributor
workflow. The author of a change updates affected knowledge; its reviewer checks
the claims against the linked implementation and evidence.

## Repository knowledge maintenance

Apply these principles to both everyday documentation changes and full inspections:

- **Enforce automatable constraints in CI.** Any constraint that a script can
  verify belongs in an automated CI check, not in repository knowledge. Link to
  the enforcing check or configuration when readers need to find the rule;
  keep only rationale and guidance that require human judgment in prose.
- **Keep one source of truth.** Every piece of knowledge has one authoritative
  home. References from other knowledge pages, code, or workflows must link to
  that home instead of repeating its content. For executable rules, the check
  or configuration is the source of truth.
- **Verify claims against evidence.** Read the relevant implementation, callers,
  tests, and tooling before correcting a claim. Investigate disagreements between
  code and prose; do not infer design intent from implementation shape.
- **Separate current behavior, proposals, and history.** Confirm issue/PR status
  before calling work complete. A local implementation is not a merged change,
  and a merge is not proof of a release. Label proposals and uncertainty explicitly;
  consult [related work](README.md#related-work) before introducing new conventions.
- **Keep guidance useful beyond one task.** Retain purpose, ownership, design rationale,
  failure modes, and verification paths. Remove
  conversation history, temporary branch ownership, one-off progress reports,
  and unsupported assertions from active guides.
- **Retire completed work.** Extract reusable guidance before removing resolved
  debt and completed tasks, following the
  [completion and archive workflow](#archive-completed-work).
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

1. Follow the [existing-work search](../CONTRIBUTING.md#issues). For a new
   inspection, fetch the latest default branch and create a fresh worktree from
   that fetched revision. Continue follow-up revisions on the existing review
   branch.
2. Start at the [knowledge index](README.md), follow its links, and reconcile it
   with the [maintained document scope](#document-metadata-and-review-deadline)
   to catch unindexed pages. Exclude `docs/archive/`.
3. Review every active page against the checked revision, applying the principles
   above. Verify external tracking claims when relevant, correct the content, and
   only then update that page's review timestamp.
4. Run [validation](#validation) and follow the
   [independent review sequence](../CONTRIBUTING.md#context-free-reviews).
5. Follow the [PR workflow](../CONTRIBUTING.md#pull-requests). Report the checked
   revision, reviewed scope, actual check results, and unresolved limits there;
   keep run-specific reports out of permanent guidance.

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

The [documentation check](../tests/test_repository_docs.py) is the source of truth
for maintained file scope, metadata schema, freshness, local links, and navigation.
It runs through the [CI test workflow](../.github/workflows/test.yml). Run it alone
when diagnosing a documentation failure:

```bash
uv run pytest -n 0 tests/test_repository_docs.py
```

Follow the check's diagnostics to repair navigation or review overdue knowledge;
never weaken a rule just to make a stale page pass. Its module documentation
records parser coverage and limitations. Human review still establishes prose
accuracy, the relevance of external references, and whether examples teach the
intended workflow. Use [repository validation](../CONTRIBUTING.md#testing-and-validation)
for changes beyond documentation.

## Document metadata and review deadline

`last_checked` records when a page's claims were checked against source and
evidence. It is neither a file modification time nor proof that all linked tests
were executed. Apply [review evidence principles](#repository-knowledge-maintenance)
before changing it. The format, maintained scope, and deadline are defined and
enforced by the [documentation check](../tests/test_repository_docs.py).

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
   guidance using the [placement table](#put-information-where-it-belongs) and
   [maintenance principles](#repository-knowledge-maintenance).
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

Follow the [independent knowledge review workflow](../CONTRIBUTING.md#context-free-reviews).
Use the revision's requirements and diff to find affected knowledge through the
index and relevant links.

For a normal development change, limit this review and its edits to the revision's
changed behavior, APIs, workflows, guides, and references. Touching a document
does not bring every section into scope. A full inspection is a separate
[explicitly requested or scheduled workflow](#repository-wide-inspections), not
an automatic addition to every code change.

Apply the [maintenance principles](#repository-knowledge-maintenance), run
[validation](#validation), and report corrections or that no cleanup was needed.
