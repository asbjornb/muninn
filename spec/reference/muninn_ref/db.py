"""The Muninn reference database: §2 catalog, §3 versions, §4 writes, §5 visibility,
§6 reads.

Deliberately naive. All versions live in one Python list in commit order; every read is
a full scan. Each method is a direct translation of its spec section — if this file and
SPEC.md disagree, one of them has a bug.

Inputs use the JSON shapes of §9 (timestamps as §1 text). Outputs are lists of dicts
with *native* values (timestamps as int µs, f64 as float, ...); see `encode_row` for the
JSON form.
"""

import re
from dataclasses import dataclass, field

from .errors import MalformedInput, MuninnError
from .timefmt import parse_duration, to_timestamp
from .values import COLUMN_TYPES, check_type, encode_value, same_value

# ---------------------------------------------------------------------------------------
# §2 Catalog
# ---------------------------------------------------------------------------------------

CLOCKS = ("received", "published", "system")
PARTITIONS = ("day", "month", "year")

# §2.1 table and column names: [a-z][a-z0-9_]*
_NAME_RE = re.compile(r"[a-z][a-z0-9_]*")

# §2.1 reserved column names.
RESERVED_COLUMN_NAMES = frozenset(
    {
        "series",
        "value_time",
        "published_time",
        "received_time",
        "system_time",
        "seq",
        "deleted",
        "frontier",
        "found",
        "values",
    }
)


@dataclass
class Column:
    name: str
    type: str


@dataclass
class Table:
    name: str
    columns: list[Column]
    default_clock: str = "received"
    partition: str = "day"  # physical only; no effect on any result

    def column(self, name: str) -> Column | None:
        for c in self.columns:
            if c.name == name:
                return c
        return None


@dataclass
class Series:
    id: int
    name: str
    table: str
    tags: dict[str, str] = field(default_factory=dict)
    timezone: str = "UTC"  # metadata only in v0.1
    resolution: str | None = None  # metadata only in v0.1


@dataclass(frozen=True)
class CatalogChange:
    """§2.3 one entry of the catalog change log."""

    system_time: int | None
    kind: str  # create_table | add_column | create_series | set_tags
    detail: dict


# ---------------------------------------------------------------------------------------
# §3 Versions
# ---------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Version:
    series: int  # series id
    value_time: int
    published_time: int | None  # None = unknown
    received_time: int
    deleted: bool
    # column name -> native value or None. A column added after this version was written
    # is simply absent here and reads as null (§2.1 schema evolution).
    values: dict
    system_time: int  # set by the engine (§4.2)
    seq: int  # set by the engine (§4.2)


@dataclass(frozen=True)
class WriteResult:
    """§4.5"""

    stored: int
    duplicates: int
    batch_already_committed: bool

    def to_json(self) -> dict:
        return {
            "stored": self.stored,
            "duplicates": self.duplicates,
            "batch_already_committed": self.batch_already_committed,
        }


_ROW_KEYS = {"series", "value_time", "published_time", "received_time", "deleted", "values"}


class Database:
    def __init__(self):
        self.tables: list[Table] = []  # creation order
        self.series: list[Series] = []  # creation order == id order
        self.catalog_log: list[CatalogChange] = []  # §2.3
        self.versions: list[Version] = []  # every stored version, commit order (§3)
        self.batch_ids: set[str] = set()  # §4.4: remembered forever

    # -- catalog lookups ---------------------------------------------------------------

    def table(self, name) -> Table:
        for t in self.tables:
            if t.name == name:
                return t
        raise MuninnError("unknown_table", repr(name))

    def series_by_name(self, name) -> Series | None:
        for s in self.series:
            if s.name == name:
                return s
        return None

    def series_by_id(self, sid: int) -> Series:
        return self.series[sid - 1]

    def _log(self, kind: str, detail: dict, system_time=None):
        # §2.3: every catalog change is logged with its system_time. The scenario format
        # has no clock on catalog steps; we stamp max(c, greatest system_time seen so far),
        # or None before anything has a time (AMBIGUITIES.md A10). Catalog changes never
        # affect `last` of §4.2, which is defined over stored versions only.
        times = [v.system_time for v in self.versions]
        times += [c.system_time for c in self.catalog_log if c.system_time is not None]
        if system_time is not None:
            times.append(to_timestamp(system_time))
        stamp = max(times) if times else None
        self.catalog_log.append(CatalogChange(stamp, kind, detail))

    @staticmethod
    def _check_column_name(name):
        # §2.1 column names: pattern, not reserved.
        if not isinstance(name, str) or not _NAME_RE.fullmatch(name):
            raise MuninnError("invalid_name", f"bad column name {name!r}")
        if name in RESERVED_COLUMN_NAMES:
            raise MuninnError("invalid_name", f"reserved column name {name!r}")

    @staticmethod
    def _check_column_type(col_type):
        if col_type not in COLUMN_TYPES:
            raise MalformedInput(f"unknown column type {col_type!r}")  # AMBIGUITIES.md A9

    # -- §2.1 tables -------------------------------------------------------------------

    def create_table(self, name, columns=(), default_clock="received", partition="day",
                     system_time=None):
        # Check order (AMBIGUITIES.md A8): table name syntax, table uniqueness, then
        # each column in order (name syntax / reserved, type, uniqueness).
        if default_clock not in CLOCKS:
            raise MalformedInput(f"bad default_clock {default_clock!r}")
        if partition not in PARTITIONS:
            raise MalformedInput(f"bad partition {partition!r}")
        if not isinstance(name, str) or not _NAME_RE.fullmatch(name):
            raise MuninnError("invalid_name", f"bad table name {name!r}")
        if any(t.name == name for t in self.tables):
            raise MuninnError("already_exists", f"table {name!r}")
        cols: list[Column] = []
        for c in columns:
            self._check_column_name(c["name"])
            self._check_column_type(c["type"])
            if any(x.name == c["name"] for x in cols):
                raise MuninnError("already_exists", f"column {c['name']!r}")
            cols.append(Column(c["name"], c["type"]))
        self.tables.append(Table(name, cols, default_clock, partition))
        self._log("create_table", {"name": name}, system_time)

    def add_column(self, table, name, type, system_time=None):
        # §2.1 schema evolution: append a column. Old versions read it as null.
        t = self.table(table)
        self._check_column_name(name)
        self._check_column_type(type)
        if t.column(name) is not None:
            raise MuninnError("already_exists", f"column {name!r}")
        t.columns.append(Column(name, type))
        self._log("add_column", {"table": table, "name": name, "type": type}, system_time)

    # -- §2.2 series -------------------------------------------------------------------

    def create_series(self, name, table, tags=None, timezone="UTC", resolution=None,
                      system_time=None):
        # Check order (AMBIGUITIES.md A8): name syntax, table exists, name uniqueness.
        if not isinstance(name, str) or name == "":
            raise MuninnError("invalid_name", f"bad series name {name!r}")
        self.table(table)  # unknown_table
        if self.series_by_name(name) is not None:
            raise MuninnError("already_exists", f"series {name!r}")
        # ids 1, 2, 3, ... in creation order, never reused.
        s = Series(len(self.series) + 1, name, table, dict(tags or {}), timezone, resolution)
        self.series.append(s)
        self._log("create_series", {"name": name, "id": s.id, "table": table}, system_time)
        return s.id

    def set_tags(self, name, tags, system_time=None):
        # §2.3 "change tags" — descriptive only, no effect on any read.
        s = self.series_by_name(name)
        if s is None:
            raise MuninnError("unknown_series", repr(name))
        s.tags = dict(tags)
        self._log("set_tags", {"name": name, "tags": dict(tags)}, system_time)

    # -------------------------------------------------------------------------------------
    # §4 Writes
    # -------------------------------------------------------------------------------------

    def _validate_row(self, row: dict) -> dict:
        """§4.1 validation of one row, checks in spec order; first failure wins."""
        if not isinstance(row, dict) or not set(row) <= _ROW_KEYS:
            raise MalformedInput(f"bad row {row!r}")
        # 1. Series name exists.
        s = self.series_by_name(row.get("series"))
        if s is None:
            raise MuninnError("unknown_series", repr(row.get("series")))
        t = self.table(s.table)
        raw_values = row.get("values")
        if raw_values is None:
            raw_values = {}
        if not isinstance(raw_values, dict):
            raise MalformedInput(f"values must be an object: {raw_values!r}")
        # 2. Every key in `values` is a column of the series' table.
        for key in raw_values:
            if t.column(key) is None:
                raise MuninnError("unknown_column", f"{key!r} not in table {t.name!r}")
        # 3. Every non-null value matches its column's type. Missing keys are null.
        values = {}
        for c in t.columns:
            raw = raw_values.get(c.name)
            values[c.name] = None if raw is None else check_type(c.type, raw)
        # 4. Tombstone with any non-null value.
        deleted = row.get("deleted", False)
        if deleted is None:
            deleted = False
        if not isinstance(deleted, bool):
            raise MalformedInput(f"deleted must be a bool: {deleted!r}")
        if deleted and any(v is not None for v in values.values()):
            raise MuninnError("tombstone_with_values")
        # 5. value_time, received_time, published_time (if non-null) are valid timestamps.
        # value_time and received_time are required; missing counts as invalid
        # (AMBIGUITIES.md A5).
        value_time = to_timestamp(row.get("value_time"))
        received_time = to_timestamp(row.get("received_time"))
        published = row.get("published_time")
        published_time = None if published is None else to_timestamp(published)
        return {
            "series": s.id,
            "value_time": value_time,
            "published_time": published_time,
            "received_time": received_time,
            "deleted": deleted,
            "values": values,
        }

    def _content_identical(self, a: dict | Version, b: dict | Version) -> bool:
        """§4.3 content identity over every user field; f64 compared bitwise."""
        get = lambda x, k: x[k] if isinstance(x, dict) else getattr(x, k)
        for k in ("series", "value_time", "published_time", "received_time", "deleted"):
            if get(a, k) != get(b, k):
                return False
        t = self.table(self.series_by_id(get(a, "series")).table)
        va, vb = get(a, "values"), get(b, "values")
        # A column that did not exist when an old version was written compares as null.
        return all(same_value(c.type, va.get(c.name), vb.get(c.name)) for c in t.columns)

    def write(self, rows, system_time, batch_id=None) -> WriteResult:
        """§4: commit a batch atomically. `system_time` is the writer clock reading c."""
        # §4.4 batch ids: a known id makes the write a no-op — no validation at all.
        if batch_id is not None:
            if not isinstance(batch_id, str) or batch_id == "":
                raise MalformedInput(f"batch_id must be a non-empty string: {batch_id!r}")
            if batch_id in self.batch_ids:
                return WriteResult(0, 0, True)
        c = to_timestamp(system_time)

        # §4.1: validate every row, in input order, before changing anything. Any error
        # propagates from here and the database is untouched (atomicity).
        prepared = [self._validate_row(r) for r in rows]

        # §4.3: drop rows content-identical to any stored version or an earlier kept row.
        kept: list[dict] = []
        duplicates = 0
        for p in prepared:
            if any(self._content_identical(p, v) for v in self.versions) or any(
                self._content_identical(p, k) for k in kept
            ):
                duplicates += 1
            else:
                kept.append(p)

        # §4.2: batch.system_time = max(c, last), last = greatest stored system_time.
        last = max((v.system_time for v in self.versions), default=None)
        batch_system_time = c if last is None else max(c, last)

        # §4.2: seq — one database-wide counter starting at 1; each stored row takes the
        # next value in batch input order. Versions are never removed, so the next seq is
        # simply one more than the number stored.
        for p in kept:
            self.versions.append(
                Version(
                    series=p["series"],
                    value_time=p["value_time"],
                    published_time=p["published_time"],
                    received_time=p["received_time"],
                    deleted=p["deleted"],
                    values=p["values"],
                    system_time=batch_system_time,
                    seq=len(self.versions) + 1,
                )
            )
        # §4.5: a dup-only (or empty) batch still records its id.
        if batch_id is not None:
            self.batch_ids.add(batch_id)
        return WriteResult(len(kept), duplicates, False)

    # -------------------------------------------------------------------------------------
    # §5 Clocks, visibility and knowledge order
    # -------------------------------------------------------------------------------------

    @staticmethod
    def t_clock(v: Version, clock: str) -> int:
        if clock == "received":
            return v.received_time
        if clock == "published":
            # Unknown published time falls back to received time.
            return v.published_time if v.published_time is not None else v.received_time
        if clock == "system":
            return v.system_time
        raise MalformedInput(f"bad clock {clock!r}")

    @classmethod
    def visible(cls, v: Version, clock: str, frontier: int | None, system: int | None) -> bool:
        # v.system_time <= S and t_clock(v) <= F; both inclusive; None = unbounded.
        return (system is None or v.system_time <= system) and (
            frontier is None or cls.t_clock(v, clock) <= frontier
        )

    @classmethod
    def k_clock(cls, v: Version, clock: str) -> tuple[int, int, int]:
        # Knowledge order key; seq is unique so the order is total.
        return (cls.t_clock(v, clock), v.system_time, v.seq)

    # -------------------------------------------------------------------------------------
    # §6 Reads
    # -------------------------------------------------------------------------------------

    def _resolve_series(self, table: Table, names) -> set[int]:
        """§6 series filter: omitted or empty = every series in the table."""
        if not names:
            return {s.id for s in self.series if s.table == table.name}
        ids = set()
        for name in names:
            s = self.series_by_name(name)
            if s is None or s.table != table.name:
                raise MuninnError("unknown_series", repr(name))
            ids.add(s.id)
        return ids

    @staticmethod
    def _opt_ts(x) -> int | None:
        return None if x is None else to_timestamp(x)

    @staticmethod
    def _check_range(lo, hi):
        # §6: value_from > value_to -> invalid_range (equal bounds = empty range).
        if lo is not None and hi is not None and lo > hi:
            raise MuninnError("invalid_range", "lower bound > upper bound")

    @staticmethod
    def _in_half_open(x: int, lo: int | None, hi: int | None) -> bool:
        return (lo is None or lo <= x) and (hi is None or x < hi)

    def _context(self, table: Table, clock, frontier, system):
        """§5 read context; clock defaults to the table's default_clock."""
        clock = table.default_clock if clock is None else clock
        if clock not in CLOCKS:
            raise MalformedInput(f"bad clock {clock!r}")
        return clock, self._opt_ts(frontier), self._opt_ts(system)

    def _row(self, v: Version, table: Table, with_deleted: bool) -> dict:
        """§6 output row: every current table column, null where absent."""
        row = {
            "series": self.series_by_id(v.series).name,
            "value_time": v.value_time,
            "published_time": v.published_time,
            "received_time": v.received_time,
            "system_time": v.system_time,
            "seq": v.seq,
        }
        if with_deleted:
            row["deleted"] = v.deleted
        row["values"] = {c.name: v.values.get(c.name) for c in table.columns}
        return row

    def _latest_versions(self, table, ids, lo, hi, clock, frontier, system) -> list[Version]:
        """§6.1 core: per point, the visible version with the greatest k_clock, unless it
        is a tombstone. Sorted by (series, value_time)."""
        best: dict[tuple[int, int], Version] = {}
        for v in self.versions:
            if v.series not in ids or not self._in_half_open(v.value_time, lo, hi):
                continue
            if not self.visible(v, clock, frontier, system):
                continue
            key = (v.series, v.value_time)
            if key not in best or self.k_clock(v, clock) > self.k_clock(best[key], clock):
                best[key] = v
        return [best[k] for k in sorted(best) if not best[k].deleted]

    # Read argument checks happen in this order (AMBIGUITIES.md A7): unknown_table,
    # unknown_series, invalid_timestamp (fields in signature order), invalid_range.

    def latest(self, table, series=None, value_from=None, value_to=None, clock=None,
               frontier=None, system=None) -> list[dict]:
        """§6.1 latest-known as of a frontier. Order: (series, value_time)."""
        t = self.table(table)
        ids = self._resolve_series(t, series)
        lo, hi = self._opt_ts(value_from), self._opt_ts(value_to)
        clock, frontier, system = self._context(t, clock, frontier, system)
        self._check_range(lo, hi)
        return [
            self._row(v, t, with_deleted=False)
            for v in self._latest_versions(t, ids, lo, hi, clock, frontier, system)
        ]

    def history(self, table, series=None, value_from=None, value_to=None, clock=None,
                frontier=None, system=None) -> list[dict]:
        """§6.2 every visible version, tombstones included.
        Order: (series, value_time, k_clock)."""
        t = self.table(table)
        ids = self._resolve_series(t, series)
        lo, hi = self._opt_ts(value_from), self._opt_ts(value_to)
        clock, frontier, system = self._context(t, clock, frontier, system)
        self._check_range(lo, hi)
        selected = [
            v
            for v in self.versions
            if v.series in ids
            and self._in_half_open(v.value_time, lo, hi)
            and self.visible(v, clock, frontier, system)
        ]
        selected.sort(key=lambda v: (v.series, v.value_time, self.k_clock(v, clock)))
        return [self._row(v, t, with_deleted=True) for v in selected]

    def replay(self, table, series=None, value_from=None, value_to=None, clock=None,
               system=None, from_=None, to=None) -> list[dict]:
        """§6.3 arrival order: system_time <= S and t_clock in [from, to).
        Order: k_clock alone, across all series. Tombstones included."""
        t = self.table(table)
        ids = self._resolve_series(t, series)
        lo, hi = self._opt_ts(value_from), self._opt_ts(value_to)
        clock, _, system = self._context(t, clock, None, system)
        t_from, t_to = self._opt_ts(from_), self._opt_ts(to)
        self._check_range(lo, hi)
        self._check_range(t_from, t_to)  # §8: from > to -> invalid_range
        selected = [
            v
            for v in self.versions
            if v.series in ids
            and self._in_half_open(v.value_time, lo, hi)
            and (system is None or v.system_time <= system)
            and self._in_half_open(self.t_clock(v, clock), t_from, t_to)
        ]
        selected.sort(key=lambda v: self.k_clock(v, clock))
        return [self._row(v, t, with_deleted=True) for v in selected]

    def grid(self, table, frontiers, series=None, value_from=None, value_to=None,
             window_start=None, window_end=None, clock=None, system=None) -> list[dict]:
        """§6.4 for each distinct frontier (ascending), the rows of `latest` at that
        frontier, prefixed with it. Order: (frontier, series, value_time)."""
        t = self.table(table)
        self._resolve_series(t, series)  # unknown_series before anything else
        absolute = value_from is not None or value_to is not None
        relative = window_start is not None or window_end is not None
        lo, hi = self._opt_ts(value_from), self._opt_ts(value_to)
        clock, _, system = self._context(t, clock, None, system)
        fs = sorted({to_timestamp(f) for f in frontiers})  # duplicates removed, ascending
        if absolute and relative:
            raise MuninnError("invalid_range", "both absolute and relative window given")
        if relative:
            # A relative window needs both ends (AMBIGUITIES.md A6).
            if window_start is None or window_end is None:
                raise MuninnError("invalid_range", "relative window needs start and end")
            ws, we = parse_duration(window_start), parse_duration(window_end)
            if ws > we:
                raise MuninnError("invalid_range", "window_start > window_end")
        else:
            self._check_range(lo, hi)
        out = []
        for f in fs:
            if relative:
                # [F_i + window_start, F_i + window_end), exact integer arithmetic; the
                # bounds need not be valid timestamps (they only filter value_time).
                lo, hi = f + ws, f + we
            # Exactly the n-query definition: call the §6.1 core per frontier.
            ids = self._resolve_series(t, series)
            for v in self._latest_versions(t, ids, lo, hi, clock, f, system):
                out.append({"frontier": f, **self._row(v, t, with_deleted=False)})
        return out

    def asof_join(self, table, left, clock=None, system=None) -> list[dict]:
        """§6.5 for each left row (input order) the latest visible version of the point
        (row.series, row.value_time) at frontier = row.frontier."""
        t = self.table(table)
        resolved = []
        for r in left:
            s = self.series_by_name(r.get("series"))
            if s is None or s.table != t.name:
                raise MuninnError("unknown_series", repr(r.get("series")))
            resolved.append(s)
        clock, _, system = self._context(t, clock, None, system)
        parsed = [(to_timestamp(r.get("value_time")), to_timestamp(r.get("frontier")))
                  for r in left]
        out = []
        for s, (vt, f) in zip(resolved, parsed):
            # Exact value_time match: the value range containing exactly vt is [vt, vt+1).
            found = self._latest_versions(t, {s.id}, vt, vt + 1, clock, f, system)
            row = {"series": s.name, "value_time": vt, "frontier": f}
            if found:
                w = self._row(found[0], t, with_deleted=False)
                row["found"] = True
                for k in ("published_time", "received_time", "system_time", "seq", "values"):
                    row[k] = w[k]
            else:
                # Not found or tombstone: all right-side fields null (values included).
                row["found"] = False
                for k in ("published_time", "received_time", "system_time", "seq", "values"):
                    row[k] = None
            out.append(row)
        return out

    # -------------------------------------------------------------------------------------
    # JSON form of output rows (§9)
    # -------------------------------------------------------------------------------------

    def encode_row(self, table: str, row: dict) -> dict:
        """Native output row -> JSON-ready dict (§1 canonical timestamps, §4.1 value
        encodings: non-finite f64 as strings, i64 beyond 2^53 as decimal strings)."""
        t = self.table(table)
        out = {}
        for k, v in row.items():
            if k in ("value_time", "published_time", "received_time", "system_time",
                     "frontier"):
                out[k] = None if v is None else encode_value("timestamp", v)
            elif k == "values":
                out[k] = None if v is None else {
                    c.name: encode_value(c.type, v[c.name]) for c in t.columns
                }
            else:
                out[k] = v
        return out
