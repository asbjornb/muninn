"""§9 Scenario format: execute steps against a fresh Database and check `expect`.

Comparison rules (§9):
- the complete row list is compared exactly, in order;
- timestamps are compared as integers (so an expectation may use offsets or fewer
  fraction digits);
- f64 values bitwise, except that a JSON number in the expectation is parsed to the nearest
  f64; "NaN" / "inf" / "-inf" denote the non-finite values;
- i64 values may be written as JSON integers or decimal strings (compared as integers);
- the `values` sugar compares only the listed fields (and only the listed columns).
"""

import json
import math
from dataclasses import dataclass
from pathlib import Path

from .db import Database
from .errors import MalformedInput, MuninnError
from .timefmt import parse_timestamp
from .values import NEG_ZERO, NegativeZero, f64_bits

TIMESTAMP_FIELDS = ("value_time", "published_time", "received_time", "system_time", "frontier")

# Fields carried by each op's output rows (§6).
ROW_FIELDS = {
    "latest": ["series", "value_time", "published_time", "received_time", "system_time",
               "seq", "values"],
    "history": ["series", "value_time", "published_time", "received_time", "system_time",
                "seq", "deleted", "values"],
    "grid": ["frontier", "series", "value_time", "published_time", "received_time",
             "system_time", "seq", "values"],
    "asof_join": ["series", "value_time", "frontier", "found", "published_time",
                  "received_time", "system_time", "seq", "values"],
}
ROW_FIELDS["replay"] = ROW_FIELDS["history"]

STEP_KEYS = {
    "create_table": {"op", "name", "columns", "default_clock", "partition", "expect"},
    "add_column": {"op", "table", "name", "type", "expect"},
    "create_series": {"op", "name", "table", "tags", "timezone", "resolution", "expect"},
    "write": {"op", "system_time", "batch_id", "rows", "expect"},
    "latest": {"op", "table", "series", "value_from", "value_to", "clock", "frontier",
               "system", "expect"},
    "history": {"op", "table", "series", "value_from", "value_to", "clock", "frontier",
                "system", "expect"},
    "replay": {"op", "table", "series", "value_from", "value_to", "clock", "system",
               "from", "to", "expect"},
    "grid": {"op", "table", "series", "value_from", "value_to", "clock", "system",
             "frontiers", "window_start", "window_end", "expect"},
    "asof_join": {"op", "table", "clock", "system", "left", "expect"},
}


def _reject_constant(name):
    raise ValueError(f"bare {name} is not JSON; write non-finite f64 as a string")


def _parse_int(token: str) -> int:
    # §10.2: the integer token -0 is kept distinguishable (it denotes -0.0 in f64).
    return NEG_ZERO if token == "-0" else int(token)


def loads_json(text: str):
    return json.loads(text, parse_constant=_reject_constant, parse_int=_parse_int)


def load_json(path) -> dict:
    with open(path, encoding="utf-8") as f:
        return loads_json(f.read())


# ---------------------------------------------------------------------------------------
# Executing steps
# ---------------------------------------------------------------------------------------


def execute(db: Database, step: dict):
    """Run one step. Returns ("ok", None) for catalog steps, ("write", WriteResult) or
    ("rows", [native rows]). Raises MuninnError for spec failures."""
    op = step.get("op")
    if op not in STEP_KEYS:
        raise MalformedInput(f"unknown op {op!r}")
    extra = set(step) - STEP_KEYS[op]
    if extra:  # §10.3: unknown request fields are structurally malformed
        raise MuninnError("invalid_argument", f"unexpected fields for {op}: {sorted(extra)}")
    def g(key, default=None):
        # §10.3: a field given as null is the same as omitting it.
        value = step.get(key)
        return default if value is None else value

    if op == "create_table":
        db.create_table(g("name"), g("columns", []), g("default_clock", "received"),
                        g("partition", "day"))
        return "ok", None
    if op == "add_column":
        db.add_column(g("table"), g("name"), g("type"))
        return "ok", None
    if op == "create_series":
        db.create_series(g("name"), g("table"), g("tags"), g("timezone", "UTC"),
                         g("resolution"))
        return "ok", None
    if op == "write":
        return "write", db.write(g("rows", []), g("system_time"), g("batch_id"))
    common = dict(table=g("table"), clock=g("clock"), system=g("system"))
    rng = dict(series=g("series"), value_from=g("value_from"), value_to=g("value_to"))
    if op == "latest":
        return "rows", db.latest(**common, **rng, frontier=g("frontier"))
    if op == "history":
        return "rows", db.history(**common, **rng, frontier=g("frontier"))
    if op == "replay":
        return "rows", db.replay(**common, **rng, from_=g("from"), to=g("to"))
    if op == "grid":
        return "rows", db.grid(**common, **rng, frontiers=g("frontiers"),
                               window_start=g("window_start"), window_end=g("window_end"))
    if op == "asof_join":
        return "rows", db.asof_join(**common, left=g("left"))
    raise AssertionError(op)


# ---------------------------------------------------------------------------------------
# Normalisation for comparison
# ---------------------------------------------------------------------------------------


class _Unparsable:
    """Marks an expectation value that could not be interpreted; never equal to anything."""

    def __init__(self, raw):
        self.raw = raw

    def __eq__(self, other):
        return False

    def __repr__(self):
        return f"<unparsable {self.raw!r}>"


def _norm_ts(x):
    if x is None:
        return None
    try:
        return ("ts", parse_timestamp(x))
    except MuninnError:
        return _Unparsable(x)


def _norm_value(col_type, x):
    """JSON value of a column -> comparable, type-tagged form."""
    if x is None:
        return None
    if col_type == "f64":
        if isinstance(x, bool):
            return _Unparsable(x)
        if isinstance(x, NegativeZero):
            return ("f64", f64_bits(-0.0))
        if isinstance(x, (int, float)):
            return ("f64", f64_bits(float(x)))
        if x in ("NaN", "inf", "-inf"):
            return ("f64", f64_bits({"NaN": math.nan, "inf": math.inf, "-inf": -math.inf}[x]))
        return _Unparsable(x)
    if col_type == "i64":
        if isinstance(x, bool):
            return _Unparsable(x)
        if isinstance(x, int):
            return ("i64", int(x))
        if isinstance(x, str):
            try:
                return ("i64", int(x))
            except ValueError:
                return _Unparsable(x)
        return _Unparsable(x)
    if col_type == "timestamp":
        return _norm_ts(x)
    return (col_type, type(x).__name__, x)  # bool/string: type-strict equality


def _norm_values(columns: dict, vals, subset: bool):
    if vals is None:
        return None
    if not isinstance(vals, dict):
        return _Unparsable(vals)
    keys = list(vals) if subset else list(columns)
    if not subset and set(vals) != set(columns):
        return _Unparsable(vals)
    out = {}
    for k in keys:
        if k not in columns:
            return _Unparsable(vals)
        out[k] = _norm_value(columns[k], vals.get(k))
    return out


def _norm_field(columns, k, v):
    if k in TIMESTAMP_FIELDS:
        return _norm_ts(v)
    if k == "values":
        return _norm_values(columns, v, subset=False)
    return (type(v).__name__, v)  # series (str), seq (int), deleted/found (bool)


def _norm_row(columns, op, row):
    if not isinstance(row, dict) or set(row) != set(ROW_FIELDS[op]):
        return _Unparsable(row)
    return {k: _norm_field(columns, k, row[k]) for k in ROW_FIELDS[op]}


def _norm_sugar(columns, op, entry):
    """Expected `values`-sugar entry -> comparable form."""
    if not isinstance(entry, list):
        return _Unparsable(entry)
    if op == "grid":
        if len(entry) != 4:
            return _Unparsable(entry)
        f, s, vt, vals = entry
        return [_norm_ts(f), ("str", s), _norm_ts(vt), _norm_values(columns, vals, True)]
    if op == "asof_join":
        if len(entry) != 4:
            return _Unparsable(entry)
        s, vt, f, vals = entry
        return [("str", s), _norm_ts(vt), _norm_ts(f),
                None if vals is None else _norm_values(columns, vals, True)]
    if len(entry) != 3:
        return _Unparsable(entry)
    s, vt, vals = entry
    if op in ("history", "replay") and vals == "deleted":
        return [("str", s), _norm_ts(vt), "deleted"]
    return [("str", s), _norm_ts(vt), _norm_values(columns, vals, True)]


def _actual_sugar(columns, op, row, expected_entry):
    """Project an actual JSON row onto the shape of an expected sugar entry."""
    want = expected_entry[-1] if isinstance(expected_entry, list) and expected_entry else None
    subset = list(want) if isinstance(want, dict) else []

    def vals():
        if row["values"] is None:
            return None
        return {k: _norm_value(columns[k], row["values"][k]) if k in columns else None
                for k in subset}

    if op == "grid":
        return [_norm_ts(row["frontier"]), ("str", row["series"]), _norm_ts(row["value_time"]),
                vals()]
    if op == "asof_join":
        last = vals() if row["found"] and isinstance(want, dict) else (
            None if not row["found"] else "found")
        return [("str", row["series"]), _norm_ts(row["value_time"]), _norm_ts(row["frontier"]),
                last]
    last = "deleted" if row.get("deleted") else vals()
    return [("str", row["series"]), _norm_ts(row["value_time"]), last]


def _eq(a, b) -> bool:
    """Strict structural equality (no bool/int confusion)."""
    if isinstance(a, _Unparsable) or isinstance(b, _Unparsable):
        return False
    if type(a) is not type(b):
        return False
    if isinstance(a, dict):
        return a.keys() == b.keys() and all(_eq(a[k], b[k]) for k in a)
    if isinstance(a, (list, tuple)):
        return len(a) == len(b) and all(_eq(x, y) for x, y in zip(a, b))
    return a == b


# ---------------------------------------------------------------------------------------
# Checking a whole scenario
# ---------------------------------------------------------------------------------------


@dataclass
class Failure:
    path: str
    step_index: int
    op: str
    message: str
    expected: object = None
    actual: object = None

    def render(self) -> str:
        lines = [f"FAIL {self.path} step {self.step_index} ({self.op}): {self.message}"]
        if self.expected is not None or self.actual is not None:
            lines.append("  expected: " + _dump(self.expected))
            lines.append("  actual:   " + _dump(self.actual))
            if isinstance(self.expected, list) and isinstance(self.actual, list):
                for i in range(max(len(self.expected), len(self.actual))):
                    e = self.expected[i] if i < len(self.expected) else "<missing>"
                    a = self.actual[i] if i < len(self.actual) else "<missing>"
                    if e != a:
                        lines.append(f"  first differing row #{i}:")
                        lines.append("    expected: " + _dump(e))
                        lines.append("    actual:   " + _dump(a))
                        break
        return "\n".join(lines)


def _dump(x) -> str:
    return json.dumps(x, ensure_ascii=False, allow_nan=False, default=str)


def _columns_for(db: Database, step: dict) -> dict:
    t = db.table(step["table"])
    return {c.name: c.type for c in t.columns}


def check_step(db: Database, step: dict) -> str | tuple[str, object, object] | None:
    """Execute a step and compare to its expectation. Returns None on success, else a
    message or (message, expected, actual)."""
    expect = step.get("expect")
    op = step.get("op")
    try:
        kind, result = execute(db, step)
    except MuninnError as e:
        if isinstance(expect, dict) and "error" in expect:
            if expect["error"] == e.code:
                return None
            return ("wrong error", {"error": expect["error"]}, {"error": e.code, "detail": e.detail})
        return ("unexpected error", expect, {"error": e.code, "detail": e.detail})
    if isinstance(expect, dict) and "error" in expect:
        actual = result.to_json() if kind == "write" else (
            [db.encode_row(step["table"], r) for r in result] if kind == "rows" else "ok")
        return ("expected an error, step succeeded", expect, actual)
    if expect is None:
        return None
    if kind == "ok":
        return ("catalog steps take no expectation other than an error", expect, None)
    if kind == "write":
        actual = result.to_json()
        if expect != actual or any(type(expect[k]) is not type(actual[k]) for k in actual):
            return ("write result differs", expect, actual)
        return None
    columns = _columns_for(db, step)
    actual_json = [db.encode_row(step["table"], r) for r in result]
    if set(expect) == {"rows"}:
        exp = expect["rows"]
        if not isinstance(exp, list):
            return ("expect.rows must be a list", exp, actual_json)
        ok = len(exp) == len(actual_json) and all(
            _eq(_norm_row(columns, op, e), _norm_row(columns, op, a))
            for e, a in zip(exp, actual_json)
        )
        return None if ok else ("rows differ", exp, actual_json)
    if set(expect) == {"values"}:
        exp = expect["values"]
        if not isinstance(exp, list):
            return ("expect.values must be a list", exp, actual_json)
        ok = len(exp) == len(actual_json) and all(
            _eq(_norm_sugar(columns, op, e), _actual_sugar(columns, op, a, e))
            for e, a in zip(exp, actual_json)
        )
        if ok:
            return None
        return ("rows differ (values sugar)", exp,
                [_sugar_view(op, a) for a in actual_json])
    return ("expect must be {rows}, {values} or {error}", expect, actual_json)


def _sugar_view(op, row):
    """Human-readable sugar form of an actual row (for diffs only)."""
    if op == "grid":
        return [row["frontier"], row["series"], row["value_time"], row["values"]]
    if op == "asof_join":
        return [row["series"], row["value_time"], row["frontier"],
                row["values"] if row["found"] else None]
    if op in ("history", "replay") and row["deleted"]:
        return [row["series"], row["value_time"], "deleted"]
    return [row["series"], row["value_time"], row["values"]]


def run_scenario(path) -> list[Failure]:
    """Run one scenario file against a fresh database. Stops at the first failing step
    (later steps would only report knock-on differences)."""
    path = str(path)
    try:
        scenario = load_json(path)
        steps = scenario["steps"]
        for key in ("name", "description"):
            if not isinstance(scenario.get(key), str):
                raise MalformedInput(f"scenario needs a string {key!r}")
    except (OSError, ValueError, KeyError, MalformedInput) as e:
        return [Failure(path, -1, "-", f"cannot load scenario: {e}")]
    db = Database()
    for i, step in enumerate(steps):
        try:
            problem = check_step(db, step)
        except (MalformedInput, KeyError, TypeError) as e:
            return [Failure(path, i, str(step.get("op")), f"malformed step: {e!r}")]
        if problem is not None:
            if isinstance(problem, tuple):
                msg, exp, act = problem
                return [Failure(path, i, step.get("op"), msg, exp, act)]
            return [Failure(path, i, step.get("op"), problem)]
    return []


def find_scenarios(paths) -> list[Path]:
    """Expand files and directories (recursively) into sorted *.json scenario paths."""
    out: list[Path] = []
    for p in map(Path, paths):
        if p.is_dir():
            out.extend(sorted(p.rglob("*.json")))
        else:
            out.append(p)
    return out
