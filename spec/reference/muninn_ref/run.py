"""Scenario runner CLI: python -m muninn_ref.run [paths...]

Runs every scenario file (directories are searched recursively, so `generated/` is
included) against a fresh reference database and prints a diff for each failure.
Exit status is non-zero if any scenario fails.
"""

import argparse
import sys
from pathlib import Path

from .scenario import find_scenarios, run_scenario

DEFAULT_DIR = Path(__file__).resolve().parents[2] / "scenarios"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("paths", nargs="*", help=f"files or directories (default: {DEFAULT_DIR})")
    ap.add_argument("-q", "--quiet", action="store_true", help="only print failures + summary")
    args = ap.parse_args(argv)
    paths = find_scenarios(args.paths or [DEFAULT_DIR])
    if not paths:
        print("no scenarios found", file=sys.stderr)
        return 2
    failed = 0
    for p in paths:
        failures = run_scenario(p)
        if failures:
            failed += 1
            for f in failures:
                print(f.render())
        elif not args.quiet:
            print(f"ok   {p}")
    print(f"{len(paths) - failed} passed, {failed} failed, {len(paths)} scenarios")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
