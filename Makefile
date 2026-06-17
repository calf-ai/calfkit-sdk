.PHONY: help check lint-check lint-fix format-check format-fix type-check test test-kafka test-live fix build build-wheel clean publish-test

# Default target
help:
	@echo "Available commands:"
	@echo ""
	@echo "  Checks (CI):"
	@echo "    make check        - Run all checks (lint, format, type)"
	@echo "    make lint-check   - Run linter (ruff check)"
	@echo "    make format-check - Check code formatting (ruff format --check)"
	@echo "    make type-check   - Run type checker (mypy)"
	@echo "    make test         - Run tests (pytest; opt-in kafka + live lanes deselected)"
	@echo "    make test-kafka   - Run real-broker tests only (needs Docker; -m kafka)"
	@echo "    make test-live    - Run live model-API tests only (needs OPENAI_API_KEY + TEST_LLM_MODEL_NAME; -m live)"
	@echo ""
	@echo "  Fixes:"
	@echo "    make fix          - Fix all auto-fixable issues (lint + format)"
	@echo "    make lint-fix     - Fix linting issues (ruff check --fix)"
	@echo "    make format-fix   - Format code (ruff format)"
	@echo ""
	@echo "  Build:"
	@echo "    make build        - Build sdist and wheel"
	@echo "    make build-wheel  - Build wheel only"
	@echo "    make clean        - Remove build artifacts"
	@echo ""
	@echo "  Publish:"
	@echo "    make publish-test - Build and upload to TestPyPI"

# === Checks ===

check: lint-check format-check type-check
	@echo "✓ All checks passed"

lint-check:
	@echo "Running linter..."
	@uv run ruff check .
	@echo "✓ Lint check passed"

format-check:
	@echo "Checking format..."
	@uv run ruff format --check .
	@echo "✓ Format check passed"

type-check:
	@echo "Running type checker..."
	@uv run mypy calfkit/
	@echo "✓ Type check passed"

test:
	@echo "Running tests (opt-in 'kafka' and 'live' lanes deselected by default)..."
	@uv run pytest tests/ -v

test-kafka:
	@echo "Running real-broker (Redpanda) tests... (requires Docker)"
	@uv run --group integration pytest -m kafka -v --timeout=120

test-live:
	@echo "Running live model-API tests... (requires OPENAI_API_KEY + TEST_LLM_MODEL_NAME)"
	@uv run pytest -m live -v --timeout=300

# === Fixes ===

fix: lint-fix format-fix
	@echo "✓ All auto-fixes applied"

lint-fix:
	@echo "Fixing lint issues..."
	@uv run ruff check . --fix
	@echo "✓ Lint fixes applied"

format-fix:
	@echo "Formatting code..."
	@uv run ruff format .
	@echo "✓ Format fixes applied"

# === Build ===

build: clean
	@echo "Building sdist and wheel..."
	@uv build
	@echo "✓ Build complete (output in dist/)"

build-wheel: clean
	@echo "Building wheel..."
	@uv build --wheel
	@echo "✓ Wheel build complete (output in dist/)"

clean:
	@echo "Cleaning build artifacts..."
	@rm -rf dist/ build/ *.egg-info
	@echo "✓ Clean complete"

# === Publish ===

publish-test: clean
	@echo "Building and uploading to TestPyPI..."
	@uv build
	@uv run twine upload --repository testpypi dist/*
	@echo "✓ Published to TestPyPI"
	@echo "  View at: https://test.pypi.org/project/calfkit/"
