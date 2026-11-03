#!/usr/bin/env bash
set -euo pipefail

echo "Running automated tests..."
python -m pytest tests/unit -v

echo "Checking code quality..."
ruff check backend tests

echo "All checks passed!"
