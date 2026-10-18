"""Muninn reference implementation — the executable oracle for spec/SPEC.md.

Pure Python standard library. Deliberately naive: clarity over speed.
"""

from .db import Database, Version, WriteResult
from .errors import ERROR_CODES, MalformedInput, MuninnError
from .timefmt import format_timestamp, parse_duration, parse_timestamp

__all__ = [
    "Database",
    "Version",
    "WriteResult",
    "ERROR_CODES",
    "MalformedInput",
    "MuninnError",
    "format_timestamp",
    "parse_duration",
    "parse_timestamp",
]
