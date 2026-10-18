"""Drive the Python bindings with spec-shaped (SPEC §9 JSON) requests.

`PyMuninn` has the method signatures of the reference `muninn_ref.Database` that
`muninn_ref.scenario.execute` / `check_step` call, so the reference's scenario runner and
comparison code run unchanged against the bindings. The adapter only *translates* JSON into
Python API calls; it never validates. Where JSON and Python give the same input different
meanings it maps to the Python value with the JSON meaning:

- a JSON number in a timestamp or duration field is not a timestamp (SPEC §10.1) while a
  Python int means µs, so such fields become an unusable object;
- the integer token ``-0`` (parsed as the reference's ``NegativeZero``) becomes ``-0.0`` in
  an ``f64`` column;
- a write row without ``values`` gets ``values=None`` so that it stays in the strict
  nested (spec) form, where any other key is ``invalid_argument``;
- a ``series`` filter that is not a list, ``columns`` that is not a list, and ``columns``
  entries that are not objects,
  become unusable objects (the Python API is more lenient: it takes one name / pairs).

Results are converted back to the JSON row encoding of `Database.encode_row`.
"""

from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "spec" / "reference"))

import muninn  # noqa: E402
from muninn_ref.errors import MuninnError as RefError  # noqa: E402
from muninn_ref.scenario import ROW_FIELDS  # noqa: E402
from muninn_ref.timefmt import format_timestamp  # noqa: E402
from muninn_ref.values import NegativeZero, encode_value  # noqa: E402

SCENARIOS = ROOT / "spec" / "scenarios"
EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)


class _NotATime:
    """What a JSON non-string becomes in a timestamp / duration field."""

    def __init__(self, raw):
        self.raw = raw

    def __repr__(self):
        return f"<JSON {self.raw!r}>"


class _NotAList:
    def __init__(self, raw):
        self.raw = raw


def jtime(x):
    return x if x is None or isinstance(x, str) else _NotATime(x)


def us(x):
    if x is None:
        return None
    if isinstance(x, dt.datetime):
        return (x - EPOCH) // dt.timedelta(microseconds=1)
    return x


class Column:
    def __init__(self, name, type):  # noqa: A002
        self.name, self.type = name, type


class Table:
    def __init__(self, info: muninn.TableInfo):
        self.name = info.name
        self.columns = [Column(n, t) for n, t in info.columns.items()]


class Result:
    def __init__(self, r: muninn.WriteResult):
        self.r = r

    def to_json(self):
        return {
            "stored": self.r.stored,
            "duplicates": self.r.duplicates,
            "batch_already_committed": self.r.batch_already_committed,
        }


def _call(f, *a, **k):
    try:
        return f(*a, **k)
    except muninn.MuninnError as e:
        raise RefError(e.code, e.message) from e


class PyMuninn:
    """Reference-`Database`-shaped facade over a `muninn.Database`."""

    def __init__(self, db: muninn.Database | None = None):
        self.db = db if db is not None else muninn.memory()
        self.reader = self.db  # reads go here (the db, or a pinned snapshot)

    # -- catalog -------------------------------------------------------------------------

    def table(self, name) -> Table:
        return Table(_call(self.db.table, name))

    def create_table(self, name, columns=(), default_clock="received", partition="day"):
        if isinstance(columns, list):
            columns = [c if isinstance(c, dict) else _NotAList(c) for c in columns]
        elif columns is not None:
            columns = _NotAList(columns)
        _call(self.db.create_table, name, columns, default_clock, partition)

    def add_column(self, table, name, type):  # noqa: A002
        _call(self.db.add_column, table, name, type)

    def create_series(self, name, table, tags=None, timezone="UTC", resolution=None):
        if resolution is not None and not isinstance(resolution, str):
            resolution = _NotATime(resolution)
        _call(self.db.create_series, name, table, tags, timezone, resolution)

    # -- writes --------------------------------------------------------------------------

    def _col_type(self, series, column):
        try:
            info = {s.name: s.table for s in self.db.series()}
            return self.db.table(info[series]).columns.get(column)
        except (KeyError, muninn.MuninnError, TypeError):
            return None

    def _row(self, row):
        if not isinstance(row, dict):
            return row
        r = dict(row)
        for k in ("value_time", "received_time", "published_time"):
            if k in r:
                r[k] = jtime(r[k])
        if "values" not in r:
            r["values"] = None
        vals = r.get("values")
        if isinstance(vals, dict):
            vals = dict(vals)
            for c, v in vals.items():
                if isinstance(v, NegativeZero):
                    vals[c] = -0.0 if self._col_type(r.get("series"), c) == "f64" else 0
            r["values"] = vals
        return r

    def write(self, rows, system_time, batch_id=None):
        if isinstance(rows, list):
            rows = [self._row(x) for x in rows]
        clock = system_time if isinstance(system_time, str) else _NotATime(system_time)
        return Result(_call(self.db.write, rows, batch_id=batch_id, clock=clock))

    # -- reads ---------------------------------------------------------------------------

    @staticmethod
    def _series(series):
        return series if series is None or isinstance(series, list) else _NotAList(series)

    def latest(self, table, series=None, value_from=None, value_to=None, clock=None,
               frontier=None, system=None):
        t = _call(self.reader.latest, table, self._series(series), jtime(value_from),
                  jtime(value_to), clock, jtime(frontier), jtime(system))
        return self.rows(t, "latest")

    def history(self, table, series=None, value_from=None, value_to=None, clock=None,
                frontier=None, system=None):
        t = _call(self.reader.history, table, self._series(series), jtime(value_from),
                  jtime(value_to), clock, jtime(frontier), jtime(system))
        return self.rows(t, "history")

    def replay(self, table, series=None, value_from=None, value_to=None, clock=None,
               system=None, from_=None, to=None):
        t = _call(self.reader.replay, table, jtime(from_), jtime(to), series=self._series(series),
                  value_from=jtime(value_from), value_to=jtime(value_to), clock=clock,
                  system=jtime(system))
        return self.rows(t, "replay")

    def grid(self, table, frontiers, series=None, value_from=None, value_to=None, clock=None,
             system=None, window_start=None, window_end=None):
        fs = [jtime(f) if f is not None else _NotATime(None) for f in frontiers] if isinstance(
            frontiers, list) else None
        window = None
        if window_start is not None or window_end is not None:
            window = tuple(None if w is None else w if isinstance(w, str) else _NotATime(w)
                           for w in (window_start, window_end))
        t = _call(self.reader.grid, table, fs, window=window, series=self._series(series),
                  value_from=jtime(value_from), value_to=jtime(value_to), clock=clock,
                  system=jtime(system))
        return self.rows(t, "grid")

    def asof_join(self, table, left, clock=None, system=None):
        if isinstance(left, list):
            left = [
                {k: (jtime(v) if k in ("value_time", "frontier") else v) for k, v in r.items()}
                if isinstance(r, dict) else r
                for r in left
            ]
        t = _call(self.reader.asof_join, table, left, clock, jtime(system))
        return self.rows(t, "asof_join")

    # -- results -------------------------------------------------------------------------

    def rows(self, t, op):
        """A result table → JSON-encoded rows (the reference's `encode_row` form)."""
        table = muninn.receipt(t)["table"]
        cols = self.db.table(table).columns
        out = []
        for r in t.to_pylist():
            row = {}
            for k in ROW_FIELDS[op]:
                if k == "values":
                    found = r.get("found", True)
                    row[k] = None if not found else {
                        c: encode_value(ty, us(r[c]) if ty == "timestamp" else r[c])
                        for c, ty in cols.items()
                    }
                elif k in ("value_time", "published_time", "received_time", "system_time",
                           "frontier"):
                    row[k] = None if r[k] is None else format_timestamp(us(r[k]))
                else:
                    row[k] = r[k]
            out.append(row)
        return out

    def encode_row(self, table, row):
        return row
