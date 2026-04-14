.PHONY: dev
dev:
	uv sync
	uv run prek install

.PHONY: pre
pre: format lint test

.PHONY: format
format:
	uv run ruff format

.PHONY: lint
lint:
	uv run ruff check --fix

.PHONY: check
check:
	uv run ruff format --check && uv run ruff check

.PHONY: test
test:
	uv run --frozen pytest

.PHONY: build
build:
	uv build --no-create-gitignore --no-sources

.PHONY: clean
clean:
	rm -rf dist/ .pytest_cache/
	find . -type d -name '__pycache__' -prune -exec rm -rf {} +
	uv run ruff clean
