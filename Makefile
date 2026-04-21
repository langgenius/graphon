.PHONY: dev
dev:
	uv sync
	uv run prek install

.PHONY: format
format:
	uv run ruff format

.PHONY: lint
lint: format
	uv run ruff check --fix

.PHONY: tc
tc: lint
	uv run ty check

.PHONY: test
test: tc
	uv run pytest

.PHONY: check
check:
	uv lock --check
	uv run ruff format --check
	uv run ruff check
	uv run ty check

.PHONY: build
build:
	uv build --no-create-gitignore --no-sources

.PHONY: clean
clean:
	find . -type d -name '__pycache__' -prune -exec rm -rf {} +
	rm -rf dist/ .pytest_cache/
	uv run ruff clean
