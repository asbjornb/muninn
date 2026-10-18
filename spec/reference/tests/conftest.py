import sys
from pathlib import Path

# Make `muninn_ref` importable however pytest is invoked (e.g. from the repo root via
# `uv run --project spec/reference pytest`).
REFERENCE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REFERENCE_DIR))
