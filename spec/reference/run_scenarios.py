#!/usr/bin/env python3
"""Run conformance scenarios against the reference implementation.

    python3 spec/reference/run_scenarios.py                 # all of spec/scenarios/
    python3 spec/reference/run_scenarios.py path/to/x.json  # specific files or dirs
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from muninn_ref.run import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
