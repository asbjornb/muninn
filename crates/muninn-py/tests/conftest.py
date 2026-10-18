import sys
from pathlib import Path

# `adapter` (and through it `muninn_ref`) importable however pytest is invoked.
sys.path.insert(0, str(Path(__file__).resolve().parent))
