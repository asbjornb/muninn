"""``Database`` and ``Snapshot``: the public Python API over the native engine."""

from __future__ import annotations

import datetime as _dt
import json
import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Sequence, Union

from . import _convert as cv
from . import _native
from ._errors import InternalError, InvalidArgumentError, IoError

if TYPE_CHECKING:
    import pyarrow as pa

#: Anything accepted where a timestamp is expected: ``int`` (µs since the epoch, UTC),
#: ``str`` (RFC 3339 with ``Z`` or an offset, SPEC §1), a timezone-aware ``datetime`` /
#: ``pandas.Timestamp``, or ``numpy.datetime64`` (taken as UTC).
TimeLike = Union[int, str, _dt.datetime, Any]
#: A duration: ``str`` (``"36h"``, ``"-1d"``, ``"90m"``), ``datetime.timedelta`` /
#: ``pandas.Timedelta``, ``numpy.timedelta64`` or ``int`` µs.
DurationLike = Union[str, int, _dt.timedelta, Any]
#: Series filter: one name, an iterable of names, or ``None`` for every series of the table.
SeriesFilter = Union[str, Iterable[str], None]

RECEIPT_KEY = _native.RECEIPT_KEY.encode()


@dataclass(frozen=True)
class TableInfo:
    """A table of the catalog (SPEC §2.1)."""

    name: str
    columns: dict[str, str]
    default_clock: str
    partition: str


@dataclass(frozen=True)
class SeriesInfo:
    """A series of the catalog (SPEC §2.2). ``resolution`` is ``None`` for irregular series."""

    id: int
    name: str
    table: str
    tags: dict[str, str] = field(default_factory=dict)
    timezone: str = "UTC"
    resolution: _dt.timedelta | None = None


@dataclass(frozen=True)
class WriteResult:
    """Result of :meth:`Database.write` (SPEC §4.5) plus commit metadata.

    ``system_time`` is the batch's commit time and ``first_seq`` the ``seq`` of its first
    stored row (the rest follow consecutively); both are ``None`` when nothing was stored."""

    stored: int
    duplicates: int
    batch_already_committed: bool
    system_time: _dt.datetime | None
    first_seq: int | None


@dataclass(frozen=True)
class GcReport:
    """Result of :meth:`Database.gc`."""

    deleted: list[str]
    kept: int
    #: log entries and checkpoints removed by log truncation
    log_deleted: int = 0


def _table_info(d: dict) -> TableInfo:
    return TableInfo(d["name"], dict(d["columns"]), d["default_clock"], d["partition"])


def _series_info(d: dict) -> SeriesInfo:
    res = d["resolution"]
    return SeriesInfo(
        d["id"],
        d["name"],
        d["table"],
        dict(d["tags"]),
        d["timezone"],
        None if res is None else _dt.timedelta(microseconds=res),
    )




class _Reads:
    """The read API shared by :class:`Database` (fresh snapshot per call) and
    :class:`Snapshot` (one pinned snapshot)."""

    def _snap(self) -> Any:  # pragma: no cover - overridden
        raise NotImplementedError

    # -- catalog ---------------------------------------------------------------------

    def tables(self) -> list[TableInfo]:
        """Every table, in creation order."""
        return [_table_info(t) for t in self._snap().tables()]

    def table(self, name: str) -> TableInfo:
        """One table (``UnknownTableError`` if absent)."""
        return _table_info(self._snap().table(name))

    def series(self, table: str | None = None, **tag_filters: str) -> list[SeriesInfo]:
        """Series ordered by id — of ``table`` or of every table — whose tags include every
        ``tag=value`` given, e.g. ``db.series("prices", area="DK1")``."""
        out = [_series_info(s) for s in self._snap().series_list(table)]
        return [s for s in out if all(s.tags.get(k) == v for k, v in tag_filters.items())]

    # -- reads (SPEC §6) ---------------------------------------------------------------

    def latest(
        self,
        table: str,
        series: SeriesFilter = None,
        value_from: TimeLike | None = None,
        value_to: TimeLike | None = None,
        clock: str | None = None,
        frontier: TimeLike | None = None,
        system: TimeLike | None = None,
        _as_of: dict | None = None,
    ) -> pa.Table:
        """What was known at ``frontier`` (SPEC §6.1): for each point ``(series,
        value_time)`` with ``value_time`` in ``[value_from, value_to)``, its latest version
        visible under ``clock`` (default: the table's) at ``frontier`` and system bound
        ``system`` (both inclusive; ``None`` = unbounded). Deleted points are absent.

        Returns a ``pyarrow.Table`` ordered by ``(series, value_time)`` with columns
        ``series, series_id, value_time, published_time, received_time, system_time, seq``
        and one column per table column; the reproducibility receipt is in the schema
        metadata (:func:`muninn.receipt`)."""
        return self._snap().latest(
            table,
            cv.series_arg(series),
            cv.read_time(value_from),
            cv.read_time(value_to),
            clock,
            cv.read_time(frontier),
            cv.read_time(system),
            **(_as_of or {}),
        )

    def history(
        self,
        table: str,
        series: SeriesFilter = None,
        value_from: TimeLike | None = None,
        value_to: TimeLike | None = None,
        clock: str | None = None,
        frontier: TimeLike | None = None,
        system: TimeLike | None = None,
        _as_of: dict | None = None,
    ) -> pa.Table:
        """Every version visible at ``(clock, frontier, system)`` (SPEC §6.2), tombstones
        included (``deleted`` column), ordered by ``(series, value_time, knowledge order)``."""
        return self._snap().history(
            table,
            cv.series_arg(series),
            cv.read_time(value_from),
            cv.read_time(value_to),
            clock,
            cv.read_time(frontier),
            cv.read_time(system),
            **(_as_of or {}),
        )

    def replay(
        self,
        table: str,
        from_: TimeLike | None = None,
        to: TimeLike | None = None,
        *,
        series: SeriesFilter = None,
        value_from: TimeLike | None = None,
        value_to: TimeLike | None = None,
        clock: str | None = None,
        system: TimeLike | None = None,
        _as_of: dict | None = None,
    ) -> pa.Table:
        """Versions in arrival order (SPEC §6.3): those whose ``t_clock`` is in ``(from_,
        to]`` — exactly what becomes visible when the frontier moves from ``from_`` to
        ``to``. Tombstones included. Ordered by ``(t_clock, system_time, seq)``."""
        return self._snap().replay(
            table,
            cv.series_arg(series),
            cv.read_time(value_from),
            cv.read_time(value_to),
            clock,
            cv.read_time(system),
            cv.read_time(from_),
            cv.read_time(to),
            **(_as_of or {}),
        )

    def grid(
        self,
        table: str,
        frontiers: Iterable[TimeLike],
        *,
        window: tuple[DurationLike, DurationLike] | None = None,
        series: SeriesFilter = None,
        value_from: TimeLike | None = None,
        value_to: TimeLike | None = None,
        clock: str | None = None,
        system: TimeLike | None = None,
        _as_of: dict | None = None,
    ) -> pa.Table:
        """``latest`` at many frontiers in one pass (SPEC §6.4): for each frontier ``F``
        (deduplicated, ascending), the rows of ``latest(frontier=F)`` prefixed with a
        ``frontier`` column. The value range is either relative — ``window=("0h", "36h")``
        means ``[F, F + 36h)`` — or absolute via ``value_from`` / ``value_to``.
        Ordered by ``(frontier, series, value_time)``."""
        ws, we = cv.window_arg(window)
        return self._snap().grid(
            table,
            cv.frontiers_arg(frontiers),
            cv.series_arg(series),
            cv.read_time(value_from),
            cv.read_time(value_to),
            ws,
            we,
            clock,
            cv.read_time(system),
            **(_as_of or {}),
        )

    def asof_join(
        self,
        table: str,
        left: Any,
        clock: str | None = None,
        system: TimeLike | None = None,
        *,
        assume_utc: bool = False,
        suffix: str = "_right",
        _as_of: dict | None = None,
    ) -> pa.Table:
        """Per-row point-in-time lookup (SPEC §6.5). ``left`` (pyarrow / pandas / polars
        table or a list of dicts) has columns ``series``, ``value_time`` and ``frontier``;
        for each row, the version of ``(series, value_time)`` that ``latest`` returns at
        that row's ``frontier``. Output: ``left`` with ``found, published_time,
        received_time, system_time, seq`` and the value columns appended (all null where
        not found), one row per left row in input order. Appended names that clash with a
        left column get ``suffix``."""
        snap = self._snap()
        if isinstance(left, list):
            return snap.asof_join(table, left, clock, cv.read_time(system), assume_utc,
                                  **(_as_of or {}))
        lt = cv.as_pa_table(left)
        if lt is None:
            raise InvalidArgumentError(
                f"left must be a table (pyarrow/pandas/polars) or a list of dicts, "
                f"not {type(left).__name__}"
            )
        res = snap.asof_join(table, lt, clock, cv.read_time(system), assume_utc,
                             **(_as_of or {}))
        out = lt
        # Everything from `found` on is the right side (positionally: a value column may be
        # called `series_id`, like the spec-form id column).
        for name in res.column_names[res.column_names.index("found"):]:
            new = name if name not in out.column_names else name + suffix
            if new in out.column_names:
                raise InvalidArgumentError(
                    f"left already has columns {name!r} and {new!r}; rename them or pick "
                    "another suffix"
                )
            out = out.append_column(res.schema.field(name).with_name(new), res.column(name))
        meta = dict(lt.schema.metadata or {})
        meta[RECEIPT_KEY] = res.schema.metadata[RECEIPT_KEY]
        return out.replace_schema_metadata(meta)


class Snapshot(_Reads):
    """A pinned, read-only view of one committed state (SPEC §5.1): every read sees exactly
    the same data, whatever is written meanwhile. Get one with :meth:`Database.snapshot`."""

    def __init__(self, native: Any) -> None:
        self._native = native

    def _snap(self) -> Any:
        return self._native

    @property
    def version(self) -> int:
        """The commit-log version this snapshot reflects."""
        return self._native.version()

    @property
    def last_system_time(self) -> _dt.datetime | None:
        """The greatest ``system_time`` stored in this snapshot (``None`` if empty) — the
        ``S`` that makes every read on it reproducible later (SPEC §7.3)."""
        return cv.micros_to_datetime(self._native.last_system_time())

    def __repr__(self) -> str:
        return f"<muninn.Snapshot version={self.version}>"


class Database(_Reads):
    """A Muninn database. Open one with :func:`muninn.open` or :func:`muninn.memory`.

    Every read method takes a fresh snapshot; use :meth:`snapshot` to pin one."""

    def __init__(self, native: Any, path: str | None = None) -> None:
        self._native = native
        self.path = path

    def _snap(self) -> Any:
        return self._native.snapshot()

    def __repr__(self) -> str:
        where = self.path if self.path is not None else "memory"
        return f"<muninn.Database {where} version={self._native.version()}>"

    def snapshot(self) -> Snapshot:
        """Pins the current committed state."""
        return Snapshot(self._native.snapshot())

    @property
    def version(self) -> int:
        """The current commit-log version."""
        return self._native.version()

    def refresh(self) -> int:
        """Picks up commits made by another process; returns the new version."""
        return self._native.refresh()

    # -- catalog ---------------------------------------------------------------------

    def create_table(
        self,
        name: str,
        columns: Mapping[str, str] | Sequence[Any] | None = None,
        default_clock: str = "received",
        partition: str = "day",
    ) -> TableInfo:
        """Creates a table. ``columns`` maps column names to types (``f64``, ``i64``,
        ``bool``, ``string``, ``timestamp``; Python types ``float``/``int``/``bool``/``str``/
        ``datetime`` work too). ``default_clock`` is the clock reads use by default
        (``received``, ``published`` or ``system``); ``partition`` (``day``/``month``/
        ``year``) only affects the physical layout."""
        return _table_info(
            self._native.create_table(name, cv.columns_arg(columns), default_clock, partition)
        )

    def add_column(self, table: str, name: str, type: str) -> TableInfo:  # noqa: A002
        """Appends a column; versions stored before read it as null."""
        if isinstance(type, _pytype):
            type = cv._TYPE_ALIASES.get(type, type)  # noqa: A001
        return _table_info(self._native.add_column(table, name, type))

    def create_series(
        self,
        name: str,
        table: str,
        tags: Mapping[str, str] | None = None,
        timezone: str | None = None,
        resolution: DurationLike | None = None,
    ) -> SeriesInfo:
        """Creates a series of ``table``. ``tags`` are descriptive; ``timezone`` (IANA) and
        ``resolution`` (e.g. ``"1h"``) are metadata only."""
        res = None if resolution is None else cv.dur_arg(resolution)
        return _series_info(
            self._native.create_series(
                name, table, dict(tags) if isinstance(tags, Mapping) else tags, timezone, res
            )
        )

    def create_series_batch(
        self, series: Iterable[Mapping[str, Any] | str], table: str | None = None
    ) -> list[SeriesInfo]:
        """Creates many series in one atomic commit (far faster than one
        :meth:`create_series` call each): either all are created or, if any request
        fails, none. Each item is a mapping with ``name`` and optionally ``table``,
        ``tags``, ``timezone``, ``resolution`` (as for :meth:`create_series`), or a bare
        name; ``table`` is the default table. Requests are checked in order, as if created
        one by one (a repeated name is ``already_exists``)."""
        items = []
        for s in series:
            if isinstance(s, str):
                s = {"name": s}
            if not isinstance(s, Mapping):
                raise InvalidArgumentError("series must be mappings or names")
            unknown = set(s) - {"name", "table", "tags", "timezone", "resolution"}
            if unknown:
                raise InvalidArgumentError(f"unknown series fields: {sorted(unknown)}")
            res = s.get("resolution")
            tags = s.get("tags")
            items.append((
                s.get("name"),
                s.get("table", table),
                dict(tags) if isinstance(tags, Mapping) else tags,
                s.get("timezone"),
                None if res is None else cv.dur_arg(res),
            ))
        return [_series_info(d) for d in self._native.create_series_batch(items)]

    # -- writes (SPEC §4) --------------------------------------------------------------

    def write(
        self,
        data: Any,
        *,
        batch_id: str | None = None,
        clock: TimeLike | None = None,
        assume_utc: bool = False,
    ) -> WriteResult:
        """Writes one atomic batch: every row is stored or none is.

        ``data`` is a pyarrow Table / RecordBatch, a pandas or polars DataFrame (or anything
        with ``__arrow_c_stream__``), or a list of dicts. Columns / keys:

        - ``series`` (name), ``value_time``, ``received_time`` — required;
        - ``published_time`` (optional, null = unknown), ``deleted`` (optional bool);
        - every other column is a value column of the series' table. A dict row may
          instead carry its values in a nested ``values`` dict (the spec's JSON form).

        Timestamps: Arrow timestamps of any unit and time zone (naive ones only with
        ``assume_utc=True``; sub-microsecond parts are rejected), integers (µs since the
        epoch) or SPEC §1 strings. pandas missing values (``NaN``, ``None``, ``NaT``) are
        nulls; in Arrow / polars / dict input ``NaN`` is a real NaN.

        ``batch_id`` makes the whole batch idempotent (a resend is a no-op); identical rows
        are always deduplicated. ``clock`` is the writer clock reading (default: now); the
        engine commits at ``max(clock, last + 1µs)``."""
        if not isinstance(data, list):
            a = cv.to_arrow(data, records_fallback=True)
            if isinstance(a, list):
                data = a
        if isinstance(data, list):
            r = self._native.write(data, None, batch_id, cv.time_arg(clock, assume_utc), assume_utc)
        else:
            if a is None:
                raise InvalidArgumentError(
                    "data must be a pyarrow/pandas/polars table or a list of dicts, "
                    f"not {type(data).__name__}"
                )
            r = self._native.write(None, a, batch_id, cv.time_arg(clock, assume_utc), assume_utc)
        stored, dups, already, st, seq = r
        return WriteResult(stored, dups, already, cv.micros_to_datetime(st), seq)

    # -- maintenance -------------------------------------------------------------------

    def compact(self, table: str | None = None, partition: str | None = None) -> int:
        """Merges the segments of one partition (``table`` + partition label such as
        ``"2025-01-15"``), or of every partition with more than one segment. Never changes
        a result. Returns the number of partitions compacted."""
        return self._native.compact(table, partition)

    def gc(self, grace: DurationLike | None = None) -> GcReport:
        """Deletes segment files nothing references any more (orphans of failed writes,
        segments replaced by compaction) that are older than ``grace`` (default: one hour)
        and not used by a live snapshot of this process, and truncates the commit log
        below the second-newest checkpoint (writing a checkpoint of the head first)."""
        us = None
        if grace is not None:
            g = cv.dur_arg(grace)
            if isinstance(g, str):
                g = _parse_duration(g)
            if not isinstance(g, int):
                raise InvalidArgumentError(f"grace: {g}")
            us = g
        deleted, kept, log_deleted = self._native.gc(us)
        return GcReport(list(deleted), kept, log_deleted)


_pytype = type

_DUR_UNITS = {"us": 1, "ms": 1_000, "s": 1_000_000, "m": 60_000_000, "h": 3_600_000_000,
              "d": 86_400_000_000}


def _parse_duration(s: str) -> int | str:
    import re

    m = re.fullmatch(r"(-?)([0-9]+)(us|ms|s|m|h|d)", s)
    if m is None:
        return f"unparsable duration {s!r}"
    v = int(m.group(2)) * _DUR_UNITS[m.group(3)]
    return -v if m.group(1) else v


# ---------------------------------------------------------------------------------------
# Opening
# ---------------------------------------------------------------------------------------


def open(path: str | os.PathLike[str], *, create: bool = False) -> Database:  # noqa: A001
    """Opens the database in directory ``path`` (local filesystem).

    With ``create=False`` (the default) the directory must already hold a Muninn database
    (or be an empty directory); ``create=True`` also creates a missing directory."""
    p = os.fspath(path)
    if os.path.exists(p):
        if not os.path.isdir(p):
            raise IoError(f"{p} is not a directory")
        entries = [e for e in os.listdir(p) if not e.startswith(".")]
        if entries and "log" not in entries:
            raise IoError(f"{p} is not a Muninn database (no log/ directory)")
    elif not create:
        raise IoError(f"{p} does not exist (pass create=True to create a database)")
    return Database(_native.open_local(p), p)


def memory() -> Database:
    """A fresh in-memory database (for tests and notebooks)."""
    return Database(_native.memory())


# ---------------------------------------------------------------------------------------
# Receipts (SPEC §11)
# ---------------------------------------------------------------------------------------


def receipt(result: Any) -> dict:
    """The reproducibility receipt of a read result (a ``pyarrow.Table`` returned by
    Muninn, its schema, a JSON string or a dict): ``table``, ``op``, ``request``,
    ``clock``, ``frontier``, ``system`` and ``columns``. ``system`` is the effective system
    bound of SPEC §11: ``min(S_given, L)`` with ``L`` the snapshot's greatest system_time
    (``L`` when the read gave none), or ``"EMPTY"`` for an empty snapshot."""
    if isinstance(result, dict):
        return result
    if isinstance(result, (str, bytes)):
        return json.loads(result)
    schema = getattr(result, "schema", result)
    meta = getattr(schema, "metadata", None) or {}
    if RECEIPT_KEY not in meta:
        raise InvalidArgumentError("no Muninn receipt in this object's schema metadata")
    return json.loads(meta[RECEIPT_KEY])




def replay_receipt(db: Database | Snapshot, rcpt: Any, *, left: Any = None) -> pa.Table:
    """Re-runs the read a receipt describes, against ``db`` (the same snapshot or any later
    one). By SPEC §7.3 / §11 the rows are identical, bit for bit, and so is the schema: the
    value columns are those recorded in the receipt (columns added since are left out). A
    receipt whose ``system`` is ``"EMPTY"`` (taken on an empty snapshot) replays with no
    visible versions. For ``asof_join`` pass the original ``left`` table to get its other
    columns back too; without it the result has the spec columns ``series``, the id column,
    ``value_time`` and ``frontier`` on the left."""
    r = receipt(rcpt)
    if r.get("muninn_receipt") != 1:
        raise InvalidArgumentError("not a Muninn receipt (or an unsupported version)")
    op, table, q = r["op"], r["table"], r["request"]
    empty = r["system"] == EMPTY
    system = None if empty else r["system"]
    clock = r["clock"]
    as_of = {"as_of_columns": list(r["columns"]), "empty": empty}
    common = dict(series=q.get("series") or None, clock=clock, system=system, _as_of=as_of)
    rng = dict(value_from=q.get("value_from"), value_to=q.get("value_to"))
    if op in ("latest", "history"):
        res = getattr(db, op)(table, **common, **rng, frontier=r["frontier"])
    elif op == "replay":
        res = db.replay(table, q.get("from"), q.get("to"), **common, **rng)
    elif op == "grid":
        w = q.get("window")
        res = db.grid(table, q["frontiers"], window=tuple(w) if w else None, **common, **rng)
    elif op == "asof_join":
        rows = [{"series": s, "value_time": vt, "frontier": f} for s, vt, f in q["left"]]
        del common["series"]
        res = db.asof_join(table, rows if left is None else left, **common)
        if receipt(res)["request"]["left"] != q["left"]:
            raise InvalidArgumentError("left does not match the receipt's left rows")
    else:
        raise InvalidArgumentError(f"unknown op {op!r} in receipt")
    new = receipt(res)
    if any(new[k] != r[k] for k in ("table", "op", "request", "clock", "frontier", "system",
                                    "columns")):
        raise InternalError(f"replayed read does not match its receipt: {new} vs {r}")
    return res


#: The ``system`` of a receipt taken on an empty snapshot (SPEC §11): not a timestamp.
EMPTY = "EMPTY"
