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
| Upgrade steps and compatibility windows | [MIGRATION.md](../MIGRATION.md) |
| User-visible release changes | [CHANGELOG.md](../CHANGELOG.md) |
| A complex task's progress and unresolved decisions | A linked plan under `docs/plans/`, created when needed |

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
links in root Markdown files, this directory, and component/example READMEs:

```bash
uv run pytest -n 0 tests/test_repository_docs.py
```

The check catches missing local paths and knowledge pages unreachable from
`AGENTS.md`. It does not validate heading anchors, external URLs, reference-style
links, prose accuracy, or review dates. When it fails, repair the link or add a
route from an indexed page; do not weaken the check to hide obsolete knowledge.

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

Update the plan as work progresses. On completion, retain useful decision history,
move lasting behavior explanations into the relevant guide, and link unresolved
work to an issue or a subsequent plan. A completed plan is history, not current
API documentation. Never record credentials or private operational data in it.

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

This is an ordinary contribution workflow. There is no scheduled maintenance
agent or semantic freshness checker configured by these documents.
