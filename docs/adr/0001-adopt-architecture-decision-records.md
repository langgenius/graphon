# ADR 0001: Adopt Architecture Decision Records

- Status: Accepted
- Date: 2026-06-29
- Related PRs: N/A
- Supersedes: N/A
- Superseded by: N/A

## Context

Graphon is moving quickly, but some semantics need to become explicit. Pull
requests alone do not clearly distinguish between:

- semantics the project intentionally supports
- behavior that is merely historical, evolutionary, or implied by the current
  code

## Decision

Graphon will use ADRs under `docs/adr/` to record intentional semantics.

ADR is lightweight same-PR project memory:

- it lands in the same pull request as the semantic change
- it states the supported contract, behavior, or boundary that future users
  should rely on
- it records consequences and reasonable rejected alternatives
- it does not duplicate implementation detail or document behavior solely
  because the current code happens to do it

Historical, accidental, or code-implied behavior is out of scope unless the
project is explicitly choosing to support it as intended semantics.

Only explicitly experimental APIs or behaviors may defer ADR capture until the
semantics stabilize or become supported.

## Workflow

Add or update an ADR when a pull request does any of the following:

- establishes a semantic or contract that downstream users should rely on
- changes or intentionally retires an existing supported semantic or contract
- defines a cross-cutting boundary or ownership model
- promotes an explicitly experimental behavior into supported behavior

An ADR is usually unnecessary for:

- bug fixes within an already accepted semantic
- local refactors that preserve behavior and boundaries
- dependency updates, releases, CI changes, and routine contributor tooling
  changes
- documentation-only or test-only changes

When an ADR is needed:

1. Create or update it in the same pull request as the semantic change.
2. Run `./new-adr.py --title "Short title"` from the repository root.
3. Pass `--id NNNN` only when you intentionally need a specific ADR number.
4. Keep it short: record the decision, the intended semantic, consequences, and
   reasonable rejected alternatives with their rejection rationale.
5. Reference the ADR path in the pull request body, or explicitly state why no
   ADR is needed.

## Alternatives Considered

- Use pull request discussion only:
  rejected because it does not clearly separate intentionally supported
  semantics from historical or code-implied behavior.
- Require a separate ADR flow before implementation:
  rejected because it adds process cost without improving the quality of the
  recorded decision for a fast-moving project.

## Consequences

- Graphon should have fewer ADRs, focused on stable supported semantics.
- Important semantics changes are in scope even when the public API shape is
  mostly unchanged.
- Contributors should treat ADR as the place where the project states what
  future users are expected to rely on.
