"""§8 Errors.

Every failure the spec defines is a `MuninnError` carrying one of the §8 codes.
`MalformedInput` is only for scenario files the runner cannot interpret at all (e.g. an
unknown op) — a bug in the scenario, not a spec outcome.
"""

# §8 — the complete list of error codes.
ERROR_CODES = frozenset(
    {
        "unknown_table",
        "unknown_series",
        "unknown_column",
        "type_mismatch",
        "tombstone_with_values",
        "invalid_timestamp",
        "invalid_range",
        "invalid_name",
        "already_exists",
        "invalid_argument",
    }
)


class MuninnError(Exception):
    """A spec-defined failure (§8). `code` is one of ERROR_CODES."""

    def __init__(self, code: str, detail: str = ""):
        assert code in ERROR_CODES, code
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail


class MalformedInput(Exception):
    """Input the spec gives no error code for (caller bug / malformed scenario)."""
