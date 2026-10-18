#!/usr/bin/env python3
"""Randomised conformance scenarios with expectations computed by the reference.

    python3 spec/reference/generate.py                      # regenerate the corpus
    python3 spec/reference/generate.py --seed 7 --print     # one scenario to stdout
    python3 spec/reference/generate.py --count 500 --out /tmp/x   # a bigger batch

Each scenario is a small world (1-2 tables, a few series, 4 value_times, a 15-minute
knowledge grid) so that ties, duplicates and collisions are common. Steps are random
writes (duplicates, near-duplicates, tombstones, null published times, batch ids,
writer-clock regressions, occasional invalid rows), schema changes and reads of every op
with random contexts. Expectations are the reference's results in the full §9 `rows`
form. Generation is deterministic per seed (only `random.Random(seed)` is used).

A read or catalog step is given at most ONE fault, so the corpus never depends on the
order of argument checks for reads/catalog ops (AMBIGUITIES.md A7/A8). Write batches may
contain several faults: their order is normative (§4.1).
"""

import argparse
import json
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from muninn_ref.db import CLOCKS, Database  # noqa: E402
from muninn_ref.errors import MuninnError  # noqa: E402
from muninn_ref.scenario import check_step, execute  # noqa: E402
from muninn_ref.timefmt import format_timestamp, parse_timestamp  # noqa: E402

DEFAULT_OUT = Path(__file__).resolve().parents[1] / "scenarios" / "generated"
DEFAULT_COUNT = 150

BASE = parse_timestamp("2025-01-01T00:00:00Z")
US_MIN = 60_000_000
US_HOUR = 60 * US_MIN

# Value pools. Inputs deliberately include equivalent spellings (42 vs 42.0; an offset
# timestamp equal to a Z one) so dedup across spellings is exercised. Never the integer
# token -0 in an f64 position (AMBIGUITIES.md A2).
F64_POOL = [0.0, -0.0, 1.0, -1.5, 2.5, 100.0, 42, 42.0, 0.1, 1e300, -1e-300, 5e-324,
            "NaN", "inf", "-inf"]
I64_POOL = [0, 1, -1, 7, "7", 2**53, "9007199254740993", "-9223372036854775808",
            9223372036854775807]
BOOL_POOL = [True, False]
STRING_POOL = ["", "a", "b", "ÆØÅ ☃", "NaN"]
TIMESTAMP_POOL = ["2025-01-01T00:00:00Z", "2025-01-01T01:00:00+01:00",
                  "2025-01-01T00:00:00.000001Z", "1999-12-31T23:59:59.999999Z"]
POOLS = {"f64": F64_POOL, "i64": I64_POOL, "bool": BOOL_POOL, "string": STRING_POOL,
         "timestamp": TIMESTAMP_POOL}
BAD_VALUES = {
    "f64": ["x", True, "Infinity"],
    "i64": [1.5, "1.5", "9223372036854775808", True, 2.0],
    "bool": [1, "true"],
    "string": [5, False],
    "timestamp": ["2025-01-01", 0, "2025-02-30T00:00:00Z"],
}
BAD_TIMESTAMPS = ["2025-01-01T00:00:00", "2025-02-30T00:00:00Z", "10000-01-01T00:00:00Z",
                  "9999-12-31T23:30:00-01:00", "2025-01-01T00:00:00.1234567Z", None]
# Relative grid windows: start from the first list, end from the second (always valid).
WINDOW_STARTS = ["-1d", "-3h", "-2h", "-90m", "-1h", "-15m", "-0m", "0h", "-3600000000us"]
WINDOW_ENDS = ["0h", "1us", "30m", "1h", "5400s", "2h", "3h", "1d", "10800000ms"]
BAD_DURATIONS = ["1 h", "1H", "1w", "+1h", "1.5h", ""]
COLUMN_TYPES = ["f64", "f64", "f64", "i64", "bool", "string", "timestamp"]
RESERVED = ["series", "value_time", "seq", "deleted", "values", "frontier"]


def ts(us: int) -> str:
    return format_timestamp(us)


class Gen:
    def __init__(self, seed: int):
        self.seed = seed
        self.r = random.Random(seed)
        self.db = Database()  # the oracle, stepped alongside generation
        self.steps: list[dict] = []
        self.tables: list[str] = []
        self.columns: dict[str, list[tuple[str, str]]] = {}
        self.series: dict[str, str] = {}  # series -> table
        self.rows_written: list[dict] = []  # every input row ever sent (for resends)
        self.batch_ids: list[str] = []
        self.clock = BASE + self.r.randrange(2, 10) * 15 * US_MIN  # writer clock reading
        self.clocks_seen: list[int] = []
        self.n_col = 0
        self.n_series = 0

    # -- helpers -----------------------------------------------------------------------

    def chance(self, p: float) -> bool:
        return self.r.random() < p

    def pick(self, xs):
        return xs[self.r.randrange(len(xs))]

    def value_time(self) -> int:
        return BASE + self.r.randrange(0, 4) * US_HOUR

    def knowledge_time(self) -> int:
        """A received/published time on a coarse grid (so ties happen), sometimes +1us."""
        t = BASE + self.r.randrange(0, 9) * 15 * US_MIN
        return t + 1 if self.chance(0.08) else t

    def any_time(self) -> int:
        """A frontier-ish time: knowledge grid, writer clocks seen, or exact edges."""
        choice = self.r.random()
        if choice < 0.5 or not self.clocks_seen:
            t = self.knowledge_time()
        elif choice < 0.75:
            t = self.pick(self.clocks_seen)
        elif choice < 0.9:
            t = BASE + 2 * US_HOUR + self.r.randrange(0, 5) * 30 * US_MIN  # late: most visible
        else:
            t = BASE + self.r.randrange(-2, 12) * 30 * US_MIN
        delta = self.pick([0, 0, 0, -1, 1])
        return t + delta

    def emit(self, step: dict):
        """Run the step on the oracle and attach the expectation it produces."""
        try:
            kind, result = execute(self.db, step)
        except MuninnError as e:
            step["expect"] = {"error": e.code}
        else:
            if kind == "write":
                step["expect"] = result.to_json()
            elif kind == "rows":
                step["expect"] = {"rows": [self.db.encode_row(step["table"], r) for r in result]}
        self.steps.append(step)

    # -- catalog -----------------------------------------------------------------------

    def new_column(self) -> tuple[str, str]:
        self.n_col += 1
        return (f"c{self.n_col}", self.pick(COLUMN_TYPES))

    def create_table(self, name: str):
        cols = [self.new_column() for _ in range(self.r.randrange(1, 4))]
        step = {"op": "create_table", "name": name,
                "columns": [{"name": n, "type": t} for n, t in cols]}
        if self.chance(0.6):
            step["default_clock"] = self.pick(list(CLOCKS))
        if self.chance(0.2):
            step["partition"] = self.pick(["day", "month", "year"])
        self.emit(step)
        assert "expect" not in step, step
        self.tables.append(name)
        self.columns[name] = cols

    def create_series(self, table: str):
        self.n_series += 1
        name = f"s{self.n_series}"
        step = {"op": "create_series", "name": name, "table": table}
        if self.chance(0.2):
            step["tags"] = {"area": self.pick(["DK1", "DK2", "DE"])}
        if self.chance(0.1):
            step["resolution"] = "1h"
        self.emit(step)
        assert "expect" not in step, step
        self.series[name] = table

    def catalog_step(self):
        kind = self.r.random()
        if kind < 0.5:
            table = self.pick(self.tables)
            fault = self.pick([None, None, "dup", "reserved", "bad", "table"])
            if fault is None:
                name, typ = self.new_column()
                self.emit({"op": "add_column", "table": table, "name": name, "type": typ})
                self.columns[table].append((name, typ))
            elif fault == "dup":
                self.emit({"op": "add_column", "table": table,
                           "name": self.pick(self.columns[table])[0], "type": "f64"})
            elif fault == "reserved":
                self.emit({"op": "add_column", "table": table, "name": self.pick(RESERVED),
                           "type": "f64"})
            elif fault == "bad":
                self.emit({"op": "add_column", "table": table,
                           "name": self.pick(["Volume", "1x", "a-b", ""]), "type": "f64"})
            else:
                self.emit({"op": "add_column", "table": "nope", "name": "zz", "type": "f64"})
        elif kind < 0.85:
            fault = self.pick([None, None, None, "dup", "table", "empty"])
            if fault is None:
                self.create_series(self.pick(self.tables))
            elif fault == "dup":
                self.emit({"op": "create_series", "name": self.pick(list(self.series)),
                           "table": self.pick(self.tables)})
            elif fault == "table":
                self.emit({"op": "create_series", "name": "fresh_name", "table": "nope"})
            else:
                self.emit({"op": "create_series", "name": "", "table": self.pick(self.tables)})
        else:
            fault = self.pick(["dup", "bad", "reserved_col", "dup_col"])
            if fault == "dup":
                self.emit({"op": "create_table", "name": self.pick(self.tables), "columns": []})
            elif fault == "bad":
                self.emit({"op": "create_table", "name": self.pick(["T", "9t", "t-1", "_t"])})
            elif fault == "reserved_col":
                self.emit({"op": "create_table", "name": "fresh_table",
                           "columns": [{"name": self.pick(RESERVED), "type": "f64"}]})
            else:
                self.emit({"op": "create_table", "name": "fresh_table",
                           "columns": [{"name": "x", "type": "f64"}, {"name": "x", "type": "i64"}]})

    # -- writes ------------------------------------------------------------------------

    def random_values(self, table: str, deleted: bool):
        if deleted:
            if self.chance(0.3):
                return {n: None for n, _ in self.columns[table]}
            return None
        vals = {}
        for n, t in self.columns[table]:
            p = self.r.random()
            if p < 0.15:
                continue  # missing key = null
            vals[n] = None if p < 0.25 else self.pick(POOLS[t])
        if self.chance(0.3):  # shuffle key order
            items = list(vals.items())
            self.r.shuffle(items)
            vals = dict(items)
        return vals

    def fresh_row(self) -> dict:
        s = self.pick(list(self.series))
        table = self.series[s]
        deleted = self.chance(0.1)
        row = {"series": s, "value_time": ts(self.value_time())}
        rt = self.knowledge_time()
        p = self.r.random()
        if p < 0.35:
            pass  # published_time omitted -> null
        elif p < 0.45:
            row["published_time"] = None
        else:
            # published may be before or after received
            row["published_time"] = ts(rt + self.pick([-30, -15, -15, 0, 0, 15]) * US_MIN)
        row["received_time"] = ts(rt)
        if deleted or self.chance(0.1):
            row["deleted"] = deleted
        vals = self.random_values(table, deleted)
        if vals is not None:
            row["values"] = vals
        return row

    def tweak(self, row: dict) -> dict:
        """A near-duplicate: same as an earlier row except one field."""
        row = json.loads(json.dumps(row))
        what = self.pick(["received", "published", "value", "deleted"])
        if what == "received":
            row["received_time"] = ts(self.knowledge_time())
        elif what == "published":
            row["published_time"] = self.pick([None, ts(self.knowledge_time())])
        elif what == "value" and not row.get("deleted"):
            table = self.series[row["series"]]
            n, t = self.pick(self.columns[table])
            row.setdefault("values", {})[n] = self.pick(POOLS[t] + [None])
        else:
            if row.get("deleted"):
                row["deleted"] = False
            else:
                row["deleted"] = True
                row.pop("values", None)
        return row

    def corrupt(self, row: dict) -> dict:
        row = json.loads(json.dumps(row))
        table = self.series[row["series"]]
        fault = self.pick(["series", "column", "type", "tombstone", "timestamp"])
        if fault == "series":
            row["series"] = self.pick(["nope", "S1", ""])
        elif fault == "column":
            row.setdefault("values", {})["zz_unknown"] = 1.0
        elif fault == "type":
            n, t = self.pick(self.columns[table])
            row.setdefault("values", {})[n] = self.pick(BAD_VALUES[t])
        elif fault == "tombstone":
            n, t = self.pick(self.columns[table])
            row["deleted"] = True
            row["values"] = {n: self.pick(POOLS[t])}
        else:
            field = self.pick(["value_time", "received_time", "published_time"])
            bad = self.pick(BAD_TIMESTAMPS)
            if bad is None and field != "published_time":
                row.pop(field, None)
            else:
                row[field] = bad if bad is not None else "garbage"
        return row

    def advance_clock(self):
        p = self.r.random()
        if p < 0.15:
            self.clock -= self.pick([15, 60, 120]) * US_MIN  # writer clock goes backwards
        elif p < 0.3:
            pass  # same reading again
        else:
            self.clock += self.pick([1, 15 * US_MIN, 30 * US_MIN, US_HOUR])
        self.clock = max(self.clock, BASE - 4 * US_HOUR)

    def write_step(self):
        self.advance_clock()
        rows = []
        for _ in range(self.pick([1, 1, 2, 2, 3, 3, 4, 5, 6]) if self.chance(0.97) else 0):
            p = self.r.random()
            if p < 0.25 and self.rows_written:
                rows.append(json.loads(json.dumps(self.pick(self.rows_written))))  # exact resend
            elif p < 0.4 and self.rows_written:
                rows.append(self.tweak(self.pick(self.rows_written)))
            elif p < 0.48 and rows:
                rows.append(json.loads(json.dumps(self.pick(rows))))  # dup within batch
            else:
                rows.append(self.fresh_row())
        if self.chance(0.08) and len(self.rows_written) >= 2:
            # resend a whole earlier batch-like set
            rows = [json.loads(json.dumps(x)) for x in self.r.sample(self.rows_written, 2)]
        if rows and self.chance(0.1):
            i = self.r.randrange(len(rows))
            rows[i] = self.corrupt(rows[i])
        step = {"op": "write", "system_time": ts(self.clock)}
        if self.chance(0.25):
            if self.batch_ids and self.chance(0.4):
                step["batch_id"] = self.pick(self.batch_ids)
            else:
                step["batch_id"] = f"b{len(self.batch_ids) + 1}"
                self.batch_ids.append(step["batch_id"])
        step["rows"] = rows
        self.emit(step)
        if "error" not in step["expect"]:
            self.clocks_seen.append(self.clock)
            if not step["expect"]["batch_already_committed"]:
                self.rows_written.extend(rows)

    # -- reads -------------------------------------------------------------------------

    def series_filter(self, table: str, fault: str | None):
        own = [s for s, t in self.series.items() if t == table]
        if fault == "series":
            others = [s for s, t in self.series.items() if t != table] + ["nope"]
            chosen = self.r.sample(own, self.r.randrange(0, len(own) + 1)) if own else []
            chosen.insert(self.r.randrange(len(chosen) + 1), self.pick(others))
            return chosen
        p = self.r.random()
        if p < 0.5 or not own:
            return None
        if p < 0.6:
            return []
        chosen = self.r.sample(own, self.r.randrange(1, len(own) + 1))
        if self.chance(0.15):
            chosen.append(chosen[0])  # duplicate name
        return chosen

    def value_range(self, step: dict, fault: str | None):
        if fault == "range":
            lo = self.value_time() + US_HOUR
            step["value_from"], step["value_to"] = ts(lo), ts(lo - self.pick([1, US_HOUR]))
            return
        p = self.r.random()
        if p < 0.55:
            return
        a = BASE + self.r.randrange(-1, 4) * US_HOUR + self.pick([0, 0, 0, 1, -1])
        b = a + self.r.randrange(0, 4) * US_HOUR + self.pick([0, 0, 1])
        if p < 0.75:
            step["value_from"], step["value_to"] = ts(a), ts(b)
        elif p < 0.88:
            step["value_from"] = ts(a)
        else:
            step["value_to"] = ts(b)

    def context(self, step: dict, with_frontier: bool):
        if self.chance(0.6):
            step["clock"] = self.pick(list(CLOCKS))
        if with_frontier and self.chance(0.7):
            step["frontier"] = ts(self.any_time())
        if self.chance(0.35) and self.clocks_seen:
            step["system"] = ts(self.pick(self.clocks_seen) + self.pick([0, 0, 0, -1, 1, 15 * US_MIN]))

    def corrupt_timestamp(self, step: dict, fields: list[str]):
        present = [f for f in fields if f in step]
        field = self.pick(present) if present and self.chance(0.6) else self.pick(fields)
        step[field] = self.pick([b for b in BAD_TIMESTAMPS if b is not None])

    def read_step(self):
        op = self.pick(["latest", "latest", "history", "history", "replay", "grid", "asof_join"])
        fault = None
        if self.chance(0.12):
            fault = self.pick(["table", "series", "range", "timestamp"])
        # Mostly read tables that hold data, so results are rarely trivially empty.
        with_data = sorted({self.db.series_by_id(v.series).table for v in self.db.versions})
        table = self.pick(with_data if with_data and self.chance(0.85) else self.tables)
        step = {"op": op, "table": "nope" if fault == "table" else table}
        if op == "asof_join":
            self.asof_step(step, table, fault)
            return
        # With an unknown table, a series filter would be a second fault (its names are
        # not in that table), so leave it out.
        sf = None if fault == "table" else self.series_filter(table, fault)
        if sf is not None:
            step["series"] = sf
        if op in ("latest", "history"):
            self.value_range(step, fault)
            self.context(step, with_frontier=True)
            ts_fields = ["value_from", "value_to", "frontier", "system"]
        elif op == "replay":
            range_fault = self.pick(["value", "replay"]) if fault == "range" else None
            self.value_range(step, "range" if range_fault == "value" else None)
            self.context(step, with_frontier=False)
            if range_fault == "replay":
                a = self.any_time()
                step["from"], step["to"] = ts(a), ts(a - self.pick([1, 15 * US_MIN]))
            else:
                if self.chance(0.5):
                    step["from"] = ts(self.knowledge_time() - self.pick([0, 0, 30 * US_MIN]))
                if self.chance(0.5):
                    lo = parse_timestamp(step["from"]) if "from" in step else self.any_time()
                    step["to"] = ts(lo + self.r.randrange(0, 12) * 15 * US_MIN)
            ts_fields = ["value_from", "value_to", "system", "from", "to"]
        else:  # grid
            self.context(step, with_frontier=False)
            n = 0 if self.chance(0.05) else self.pick([1, 2, 2, 3, 3, 4, 5])
            fs = [self.any_time() for _ in range(n)]
            if fs and self.chance(0.3):
                fs.append(self.pick(fs))  # duplicate frontier
            step["frontiers"] = [ts(f) for f in fs]
            if self.chance(0.5):
                if fault == "range":
                    if self.chance(0.5):
                        step["window_start"], step["window_end"] = "2h", self.pick(["1h", "-1h"])
                    else:
                        step["window_start"], step["window_end"] = self.pick(BAD_DURATIONS), "1h"
                else:
                    step["window_start"] = self.pick(WINDOW_STARTS)
                    step["window_end"] = self.pick(WINDOW_ENDS)
                    if self.chance(0.08):
                        step["window_end"] = step["window_start"]  # empty window
            else:
                self.value_range(step, fault)
            ts_fields = ["value_from", "value_to", "system"]
            if fault == "timestamp":
                step.setdefault("frontiers", []).append(self.pick(
                    [b for b in BAD_TIMESTAMPS if b is not None]))
                fault = None
        if fault == "timestamp":
            self.corrupt_timestamp(step, ts_fields)
        self.emit(step)

    def asof_step(self, step: dict, table: str, fault: str | None):
        if self.chance(0.6):
            step["clock"] = self.pick(list(CLOCKS))
        if self.chance(0.35) and self.clocks_seen:
            step["system"] = ts(self.pick(self.clocks_seen) + self.pick([0, -1, 1]))
        own = [s for s, t in self.series.items() if t == table]
        if fault == "table":
            own = []  # left rows would be a second fault (not in table "nope")
        left = []
        written = [(r["series"], r["value_time"]) for r in self.rows_written
                   if r["series"] in own]
        for _ in range(self.pick([0, 1, 2, 3, 4, 5]) if own else 0):
            if written and self.chance(0.7):
                s, vt = self.pick(written)  # a point that has versions
            else:
                s = self.pick(own)
                vt = ts(self.value_time() + (30 * US_MIN if self.chance(0.1) else 0))
            left.append({"series": s, "value_time": vt, "frontier": ts(self.any_time())})
        if left and self.chance(0.2):
            left.append(dict(self.pick(left)))  # duplicate left row
        if fault == "series":
            others = [s for s, t in self.series.items() if t != table] + ["nope"]
            left.insert(self.r.randrange(len(left) + 1),
                        {"series": self.pick(others), "value_time": ts(self.value_time()),
                         "frontier": ts(self.any_time())})
        elif fault == "timestamp" and left:
            row = self.pick(left)
            if self.chance(0.3):
                del row["frontier"]
            else:
                row[self.pick(["value_time", "frontier"])] = self.pick(
                    [b for b in BAD_TIMESTAMPS if b is not None])
        step["left"] = left
        self.emit(step)

    # -- scenario ----------------------------------------------------------------------

    def build(self) -> dict:
        n_tables = self.pick([1, 1, 2, 2, 3])
        for i in range(n_tables):
            self.create_table(f"t{i + 1}")
        order = list(self.tables)  # every table gets a series; ids interleave tables
        self.r.shuffle(order)
        for t in order:
            self.create_series(t)
        for _ in range(self.r.randrange(0, 2)):
            self.create_series(self.pick(self.tables))
        n_steps = self.r.randrange(14, 30)
        for i in range(n_steps):
            p = self.r.random()
            if i < 2 or p < 0.42:
                self.write_step()
            elif p < 0.48:
                self.catalog_step()
            else:
                self.read_step()
        # Always finish with a full history of each table: the complete stored state.
        for t in self.tables:
            self.emit({"op": "history", "table": t})
        return {
            "name": f"random-{self.seed:04d}",
            "description": (
                f"Randomised scenario, seed {self.seed}, generated by spec/reference/generate.py; "
                "expectations computed by the reference implementation (see README.md here)."
            ),
            "steps": self.steps,
        }


def generate(seed: int) -> dict:
    return Gen(seed).build()


def dumps(scenario: dict) -> str:
    """Stable, compact-but-diffable JSON: one step per line. Floats use repr (exact
    round-trip); non-finite values were already encoded as strings."""
    head = json.dumps({"name": scenario["name"], "description": scenario["description"]},
                      ensure_ascii=False, allow_nan=False)[:-1]
    steps = ",\n    ".join(
        json.dumps(s, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        for s in scenario["steps"]
    )
    return f'{head}, "steps": [\n    {steps}\n]}}\n'


def self_check(scenario: dict) -> None:
    """Re-run the serialised scenario through the §9 runner logic."""
    reparsed = json.loads(dumps(scenario))
    db = Database()
    for i, step in enumerate(reparsed["steps"]):
        problem = check_step(db, step)
        if problem is not None:
            raise AssertionError(f"{scenario['name']} step {i}: {problem}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--count", type=int, default=DEFAULT_COUNT)
    ap.add_argument("--start", type=int, default=1, help="first seed")
    ap.add_argument("--seed", type=int, help="generate just this seed")
    ap.add_argument("--print", action="store_true", help="write to stdout instead of files")
    args = ap.parse_args(argv)
    seeds = [args.seed] if args.seed is not None else range(args.start, args.start + args.count)
    if not args.print:
        args.out.mkdir(parents=True, exist_ok=True)
    total = 0
    for seed in seeds:
        sc = generate(seed)
        self_check(sc)
        text = dumps(sc)
        if args.print:
            sys.stdout.write(text)
        else:
            (args.out / f"{seed:04d}-random.json").write_text(text, encoding="utf-8")
            total += len(text.encode())
    if not args.print:
        print(f"wrote {len(seeds)} scenarios ({total / 1e6:.2f} MB) to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
