"""The ``MuninnError`` hierarchy: one subclass per error code (SPEC §8, §8.1)."""

from __future__ import annotations


class MuninnError(Exception):
    """Base class of every Muninn error. ``code`` is the SPEC §8 code (or ``io``,
    ``conflict``, ``internal`` for engine-only failures).

    A failed operation has no effect (SPEC §8) — except :class:`IoError` on a write, whose
    outcome is unknown: resend it with the same ``batch_id`` (or rely on deduplication)."""

    code: str = "muninn_error"

    def __init__(self, message: str = "") -> None:
        super().__init__(message)
        self.message = message

    def __str__(self) -> str:
        return f"{self.code}: {self.message}" if self.message else self.code


class UnknownTableError(MuninnError):
    """Table name not in the catalog."""

    code = "unknown_table"


class UnknownSeriesError(MuninnError):
    """Series name not in the catalog, or not in the read's table."""

    code = "unknown_series"


class UnknownColumnError(MuninnError):
    """A write names a column the table does not have."""

    code = "unknown_column"


class TypeMismatchError(MuninnError):
    """A value does not match its column's type."""

    code = "type_mismatch"


class TombstoneWithValuesError(MuninnError):
    """A tombstone (``deleted=True``) carries a non-null value."""

    code = "tombstone_with_values"


class InvalidTimestampError(MuninnError):
    """Out of range, unparsable, naive or sub-microsecond timestamp."""

    code = "invalid_timestamp"


class InvalidRangeError(MuninnError):
    """``value_from > value_to``, ``from_ > to``, or a bad grid window."""

    code = "invalid_range"


class InvalidNameError(MuninnError):
    """A table, column or series name breaks the naming rules or is reserved."""

    code = "invalid_name"


class AlreadyExistsError(MuninnError):
    """Duplicate table, column or series name."""

    code = "already_exists"


class InvalidArgumentError(MuninnError, TypeError):
    """Structurally malformed request (wrong Python type, unknown clock, ...)."""

    code = "invalid_argument"


class IoError(MuninnError):
    """Storage failure. On a write the outcome is unknown (all or nothing, never partial)."""

    code = "io"


class ConflictError(MuninnError):
    """Another writer committed first; nothing happened, safe to retry."""

    code = "conflict"


class InternalError(MuninnError):
    """An internal invariant was violated (a bug — please report it)."""

    code = "internal"


ERRORS: dict[str, type[MuninnError]] = {
    cls.code: cls
    for cls in (
        UnknownTableError,
        UnknownSeriesError,
        UnknownColumnError,
        TypeMismatchError,
        TombstoneWithValuesError,
        InvalidTimestampError,
        InvalidRangeError,
        InvalidNameError,
        AlreadyExistsError,
        InvalidArgumentError,
        IoError,
        ConflictError,
        InternalError,
    )
}


def error_for_code(code: str, message: str) -> MuninnError:
    """The exception instance for ``code`` (used by the native module)."""
    return ERRORS.get(code, InternalError)(message)
