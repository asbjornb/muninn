"""Muninn: a point-in-time-correct time series database.

Every read answers "what did we know at this frontier?" — versions that arrived later are
invisible by construction, so a backtest cannot look ahead. Results are ``pyarrow.Table``
objects (``.to_pandas()``, ``polars.from_arrow``) that carry a reproducibility receipt.

>>> import muninn
>>> db = muninn.memory()
>>> db.create_table("prices", columns={"value": "f64"})
"""

from ._db import (
    Database,
    GcReport,
    SeriesInfo,
    Snapshot,
    TableInfo,
    WriteResult,
    memory,
    open,
    receipt,
    replay_receipt,
)
from ._errors import (
    AlreadyExistsError,
    ConflictError,
    InternalError,
    InvalidArgumentError,
    InvalidNameError,
    InvalidRangeError,
    InvalidTimestampError,
    IoError,
    MuninnError,
    TombstoneWithValuesError,
    TypeMismatchError,
    UnknownColumnError,
    UnknownSeriesError,
    UnknownTableError,
)

__version__ = "0.1.0"

__all__ = [
    "AlreadyExistsError",
    "ConflictError",
    "Database",
    "GcReport",
    "InternalError",
    "InvalidArgumentError",
    "InvalidNameError",
    "InvalidRangeError",
    "InvalidTimestampError",
    "IoError",
    "MuninnError",
    "SeriesInfo",
    "Snapshot",
    "TableInfo",
    "TombstoneWithValuesError",
    "TypeMismatchError",
    "UnknownColumnError",
    "UnknownSeriesError",
    "UnknownTableError",
    "WriteResult",
    "memory",
    "open",
    "receipt",
    "replay_receipt",
]
