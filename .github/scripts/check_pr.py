# ruff: file-ignore[implicit-namespace-package]
"""Validate the PR body against the repository template."""

import json
import os
import re
from pathlib import Path


def check_pull_request_body(body: str, template: str) -> None:
    # The template owns required headings and checklist text; checkbox state is
    # an attestation for reviewers, not proof that the underlying work was done.
    body_lines = {
        re.sub(r"^- \[[ xX]\] ", "- [ ] ", line.strip()) for line in body.splitlines()
    }
    for line in template.splitlines():
        if line.startswith(("## ", "- [ ] ")) and line not in body_lines:
            message = f"PR body is missing template item: {line}"
            raise SystemExit(message)
    if not re.search(
        r"\b(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?|refs)\s+"
        r"(?:[\w.-]+/[\w.-]+)?#[1-9][0-9]*\b|"
        r"\b(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?|refs)\s+"
        r"https://github\.com/[\w.-]+/[\w.-]+/issues/[1-9][0-9]*\b",
        body,
        re.IGNORECASE,
    ):
        message = "PR body must link an issue with Closes, Fixes, Resolves, or Refs"
        raise SystemExit(message)


if __name__ == "__main__":
    event = json.loads(
        Path(os.environ["GITHUB_EVENT_PATH"]).read_text(encoding="utf-8")
    )
    check_pull_request_body(
        event["pull_request"]["body"] or "",
        Path(".github/pull_request_template.md").read_text(encoding="utf-8"),
    )
