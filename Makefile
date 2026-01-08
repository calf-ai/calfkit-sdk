.PHONY: help check lint-check lint-fix format-check format-fix type-check test fix build build-wheel clean

# Default target
help:
	@echo "Available commands:"
	@echo ""
	@echo "  Checks (CI):"
	@echo "    make check        - Run all checks (lint, format, type)"
	@echo "    make lint-check   - Run linter (ruff check)"
	@echo "    make format-check - Check code formatting (ruff format --check)"
	@echo "    make type-check   - Run type checker (mypy)"
	@echo "    make test         - Run tests (pytest)"
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
	@uv run mypy calf/
	@echo "✓ Type check passed"

test:
	@echo "Running tests..."
	@uv run pytest tests/ -v

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
