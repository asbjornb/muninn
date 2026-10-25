"""Ingest benchmark: every system loads every dataset in two modes, so the numbers compare
like with like.

* ``per-batch`` -- one transaction/commit per original write batch (= per distinct
  ``system_time``; the dataset's arrival order), each durable before the next starts where
  the system makes it durable at all: Muninn ``write`` (what the query runs do), DuckDB
  ``BEGIN; INSERT; COMMIT`` on a database file (WAL fsync per commit), ClickHouse one
  synchronous ``INSERT`` per batch (``clickhouse``: ClickHouse defaults, parts not fsynced;
  ``clickhouse-fsync``: ``fsync_after_insert=1, fsync_part_directory=1``).
* ``bulk`` -- everything in as few large writes as the system allows: DuckDB and ClickHouse
  exactly as in the query runs (one ``CREATE TABLE AS`` / ``INSERT ... SELECT``), Muninn
  ``write`` calls of ``MuninnAdapter.bulk_rows`` (1M) rows, so its system_time values differ
  from the dataset's (irrelevant for timing).

Each includes the system's own finalisation (Muninn compact+gc, DuckDB CHECKPOINT,
ClickHouse OPTIMIZE FINAL). Where a full per-batch load is too slow (``--prefix``, default
the 1-row-per-batch sparse dataset), *every* system loads the same first N batches and the
report says so; rates are measured on that prefix, never extrapolated.

    uv run --project bench python bench/ingest.py --scale small
    uv run --project bench python bench/ingest.py --scale small --systems muninn,duckdb-native \\
        --workdir /dev/shm/muninn-ingest          # the same without real disk syncs (tmpfs)
    uv run --project bench python bench/ingest.py --render results/<dir>   # just re-render

Results accumulate in ``results/<date>-<host>/ingest-<scale>.{json,md}`` (a run replaces
earlier entries with the same storage/system/dataset/mode). The process re-executes itself
with ``fsyncstat.so`` preloaded (built from ``fsyncstat.c`` with ``cc``) to count fsync
calls and the time spent in them, per kind of file; for Muninn also inside ``write`` only.
"""

from __future__ import annotations

import argparse
import ctypes
import datetime as dt
import json
import os
import resource
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import numpy as np  # noqa: E402

from competitors import registry  # noqa: E402
from competitors.base import INGEST_MODES  # noqa: E402
from datagen.datasets import DATASETS  # noqa: E402
from run import fmt_bytes, machine_info  # noqa: E402

DEFAULT_SYSTEMS = ["muninn", "muninn-100k", "duckdb-native", "clickhouse", "clickhouse-fsync"]
#: report order; per-batch-only Muninn variants (fast write path, ADR 0009) after muninn
ORDER = ["muninn", "muninn-wm64", "muninn-8p", "muninn-100k", "duckdb-native", "clickhouse",
         "clickhouse-fsync"]
PER_BATCH_ONLY = {"muninn-wm64", "muninn-8p"}
BULK_ONLY = {"muninn-100k"}


#: what the Muninn build tags in the reports mean
BUILDS_NOTE = (
    "* **Muninn builds**: `(pre-0009)` = before the fast write path (one segment file per "
    "touched partition plus a log entry per write, ~4-25 fsyncs); `(post-0009)` = ADR 0009 "
    "(rows inline in the commit entry, group commit, memtable, background flush; ~2 fsyncs per "
    "commit); `(post-0009 rerun)` = the same build re-measured on 2026-10-20 in the session of "
    "the next one, for a same-time comparison; `(post-fixes)` = post-0009 plus the three write "
    "path fixes of 2026-10-20 (GC re-anchors log truncation after a memtable load, ADR 0007 "
    "amendment; linear in-batch dedup; columnar Arrow input and validation); `(pre-chunking)` "
    "= the same engine (main of 2026-10-25) re-measured in the session of the next one; "
    "`(chunked)` = ADR 0010 (batches of more than 100k rows validated, deduplicated and "
    "encoded in chunks, still one commit; Arrow input converted chunk by chunk). Per-batch "
    "variants: `muninn-wm64` submits 64 original batches per `write_many` call (one durable "
    "commit per 64 batches, each batch still its own transaction and system_time), "
    "`muninn-8p` has 8 producer threads calling `write` (the engine groups concurrent "
    "writes). Neither has a competitor counterpart here: DuckDB and ClickHouse were measured "
    "with one client committing one batch at a time.")


def sys_key(label: str):
    """Sort key of a report label ``system`` or ``system (tag)``: ORDER, then tag."""
    base, _, tag = label.partition(" ")
    builds = ["(pre-0009)", "(post-0009)", "(post-0009 rerun)", "(post-fixes)", "(pre-chunking)",
              "(chunked)"]
    return (ORDER.index(base) if base in ORDER else 99,
            builds.index(tag) if tag in builds else len(builds), tag)
DEFAULT_PREFIX = "sparse=10000"
FSYNC_CATS = ["segment file", "segment dir", "log file", "log dir", "other file", "other dir"]
SHIM_SRC = HERE / "fsyncstat.c"
SHIM_SO = HERE / "data" / "fsyncstat.so"


def ensure_shim() -> bool:
    """Builds the fsync counter shim and re-execs with it preloaded (once)."""
    if os.environ.get("MUNINN_INGEST_SHIM") == "loaded":
        return True
    if os.environ.get("MUNINN_INGEST_SHIM") == "off" or not shutil.which("cc"):
        return False
    SHIM_SO.parent.mkdir(parents=True, exist_ok=True)
    if not SHIM_SO.exists() or SHIM_SO.stat().st_mtime < SHIM_SRC.stat().st_mtime:
        subprocess.run(["cc", "-O2", "-shared", "-fPIC", "-o", str(SHIM_SO), str(SHIM_SRC), "-ldl"],
                       check=True)
    env = dict(os.environ, MUNINN_INGEST_SHIM="loaded",
               LD_PRELOAD=" ".join(filter(None, [os.environ.get("LD_PRELOAD"), str(SHIM_SO)])))
    os.execve(sys.executable, [sys.executable, *sys.argv], env)


def make_probe():
    try:
        lib = ctypes.CDLL(None)
        fn = lib.fsyncstat_read
    except (OSError, AttributeError):
        return None
    buf = (ctypes.c_longlong * (2 * len(FSYNC_CATS)))()

    def probe():
        fn(buf)
        return np.frombuffer(buf, dtype=np.int64).copy()
    return probe


def fsync_dict(v) -> dict | None:
    if v is None:
        return None
    v = list(v)
    out = {}
    for i, c in enumerate(FSYNC_CATS):
        if v[2 * i]:
            out[c] = {"count": int(v[2 * i]), "seconds": round(v[2 * i + 1] / 1e9, 3)}
    return out


def storage_of(path: Path) -> str:
    path.mkdir(parents=True, exist_ok=True)
    out = subprocess.run(["findmnt", "-no", "FSTYPE,SOURCE", "-T", str(path)],
                         capture_output=True, text=True).stdout.split()
    return out[0] if out else "?"


def cpu_seconds() -> float:
    r = resource.getrusage(resource.RUSAGE_SELF)
    return r.ru_utime + r.ru_stime


def _status_kb(field: str) -> int | None:
    try:
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith(field + ":"):
                return int(line.split()[1])
    except OSError:
        pass
    return None


class PeakRss:
    """The process's peak RSS (``VmHWM``) over a run, Linux only. ``reset`` (called by the
    adapter once its input is prepared) clears the mark (``/proc/self/clear_refs`` = 5) and
    records the RSS then as the base, so ``peak - base`` is what loading added on top of the
    input already in memory. Without a reset the base is the RSS at the start of the run."""

    def __init__(self):
        self.base = _status_kb("VmRSS")
        self.reset()

    def reset(self):
        try:
            Path("/proc/self/clear_refs").write_text("5")
            self.base = _status_kb("VmRSS")
        except OSError:
            pass

    def result(self) -> dict:
        peak = _status_kb("VmHWM")
        if peak is None or self.base is None:
            return {}
        return {"peak_rss_bytes": peak * 1024, "rss_base_bytes": self.base * 1024,
                "peak_rss_over_base_bytes": max(0, peak - self.base) * 1024}


def run_one(system, mode, ds, data_dir, args, max_batches, probe) -> dict:
    cat = json.loads((data_dir / ds / "catalog.json").read_text())
    workdir = Path(args.workdir) / args.scale / "ingest" / system / ds
    a = registry.make(system, workdir, threads=args.threads, memory_limit_mb=args.memory_mb)
    a.probe = probe
    peak = PeakRss()
    a.reset_peak = peak.reset
    try:
        p0 = probe() if probe else None
        c0 = cpu_seconds()
        r = a.ingest(data_dir / ds, cat, mode, max_batches=max_batches if mode == "per-batch" else None)
        r["process_cpu_seconds"] = round(cpu_seconds() - c0, 3)
        # in-process systems only (the ClickHouse server's memory is not ours)
        if not getattr(a, "out_of_process", False):
            r.update(peak.result())
        if probe:  # a server process (ClickHouse) is not covered by the shim
            r["fsync"] = None if getattr(a, "out_of_process", False) else fsync_dict(probe() - p0)
        if r.get("write_fsync") is not None:
            r["write_fsync"] = fsync_dict(r["write_fsync"])
        r["version"] = a.version()
        r["notes"] = a.notes
    finally:
        a.close()
    shutil.rmtree(workdir, ignore_errors=True)
    for k in ("write_seconds", "finalize_seconds", "wall_seconds"):
        r[k] = round(r[k], 3)
    r["writes_per_second"] = round(r["writes"] / r["write_seconds"], 2) if r["write_seconds"] else None
    r["rows_per_second"] = round(r["rows"] / r["wall_seconds"]) if r["wall_seconds"] else None
    r["bytes_per_version"] = round(r["bytes_on_disk"] / max(1, r["rows"]), 2)
    return r


# ---------------------------------------------------------------------------------------
def fmt_s(x):
    return "-" if x is None else (f"{x:.2f}" if x < 10 else f"{x:,.0f}")


def fmt_rate(x):
    if x is None:
        return "-"
    if x >= 1e6:
        return f"{x / 1e6:.2f}M"
    if x >= 1e4:
        return f"{x / 1e3:.0f}k"
    return f"{x:,.0f}" if x >= 100 else f"{x:.1f}"


def fsync_cell(r) -> str:
    f = r.get("fsync")
    if f is None:
        return "n/a"
    if not f:
        return "0"
    n = sum(v["count"] for v in f.values())
    s = sum(v["seconds"] for v in f.values())
    per = f", {n / r['writes']:.1f}/write" if r["writes"] > 1 else ""
    return f"{n:,} ({fmt_s(s)} s{per})"


def rss_cell(r) -> str:
    if r.get("peak_rss_bytes") is None:
        return "-"
    return f"{r['peak_rss_bytes'] / 2**20:,.0f} (+{r['peak_rss_over_base_bytes'] / 2**20:,.0f})"


def markdown(rep) -> str:
    L = [f"# Muninn ingest benchmark — {rep['scale']} scale", ""]
    m = rep["machine"]
    L.append(f"{m.get('cpu', '?')}, {m['cpus']} logical CPUs, {m.get('mem_gb', '?')} GB RAM, "
             f"{m['os']}. Produced by `bench/ingest.py` (see its docstring and "
             "`bench/results/README.md`, section Ingest).")
    L.append("")
    L.append("* **per-batch**: one transaction/commit per original write batch, in arrival order "
             "(Muninn `write`; DuckDB `BEGIN/INSERT/COMMIT` on a database file; ClickHouse one "
             "synchronous `INSERT` per batch, `clickhouse-fsync` with `fsync_after_insert=1`).")
    L.append("* **bulk**: as few writes as possible (DuckDB one `CREATE TABLE AS ... ORDER BY`, "
             "ClickHouse one `INSERT ... SELECT`, Muninn writes of 1M rows).")
    L.append("* Times include the system's finalisation (Muninn compact+gc, DuckDB CHECKPOINT, "
             "ClickHouse OPTIMIZE FINAL); **writes/s** is over the write phase, **rows/s** over "
             "the whole wall time. **prefix** = only the first N batches were loaded (same N for "
             "every system), rates are for that prefix. Size = on disk after finalisation.")
    L.append("* **fsyncs**: calls to fsync/fdatasync in the benchmark process and the time spent in "
             "them (`bench/fsyncstat.c`); ClickHouse runs as its own server process, so not counted." + " Summed over threads, so with background flushes the total can exceed the wall time.")
    L.append("* **peak RSS**: the benchmark process's peak resident set (`VmHWM`), reset with "
             "`/proc/self/clear_refs` once the input table is in memory (Muninn adapter; other "
             "in-process systems from the start of the run), and in brackets how far it rose "
             "above the RSS at that point. Measured from 2026-10-25 on; '-' = not measured.")
    L.append(BUILDS_NOTE)
    L.append("")
    by_storage: dict = {}
    for r in rep["runs"]:
        by_storage.setdefault(r["storage"], []).append(r)
    for storage, runs in by_storage.items():
        where = runs[0].get("workdir", "")
        L.append(f"## Storage: {storage} (`{where}`)")
        L.append("")
        L.append("| dataset | mode | system | writes | rows | wall s | write s | finalize s | "
                 "writes/s | rows/s | per write p50 / p99 ms | size (B/version) | fsyncs (time) | "
                 "peak RSS MB (+ over input) |")
        L.append("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
        runs = sorted(runs, key=lambda r: (list(DATASETS).index(r["dataset"]),
                                           INGEST_MODES.index(r["mode"]), sys_key(r["system"])))
        for r in runs:
            x = r["result"]
            pre = "" if x["complete"] else f" (prefix: {x['writes']:,} of {x['batches_total']:,} batches)"
            lat = x.get("write_latency_ms")
            lat = f"{lat['p50']:.2f} / {lat['p99']:.2f}" if lat else "-"
            L.append(f"| {r['dataset']} | {r['mode']}{pre} | {r['system']} | {x['writes']:,} | "
                     f"{x['rows']:,} | {fmt_s(x['wall_seconds'])} | {fmt_s(x['write_seconds'])} | "
                     f"{fmt_s(x['finalize_seconds'])} | "
                     f"{fmt_rate(x['writes_per_second']) if x['writes'] > 1 else '-'} | "
                     f"{fmt_rate(x['rows_per_second'])} | {lat} | "
                     f"{fmt_bytes(x['bytes_on_disk'])} ({x['bytes_per_version']}) | {fsync_cell(x)} | "
                     f"{rss_cell(x)} |")
        L.append("")
        mrows = [r for r in runs if r["system"].partition(" ")[0] == "muninn"
                 and r["result"].get("write_fsync") is not None
                 and r["mode"] == "per-batch"]
        if mrows:
            L.append("Muninn per-batch, inside `write` only (excludes the periodic compact+gc): "
                     "mean latency and the part of it spent in fsync, per write. The counters "
                     "are process-wide: post-0009, fsyncs of a background flush that overlap a "
                     "write are attributed to it (which can make 'other' negative).")
            L.append("")
            L.append("| dataset | system | writes | mean ms/write | fsyncs/write | segment file+dir fsync ms | "
                     "log file+dir fsync ms | other ms (encode, dedup, file writes, Python) |")
            L.append("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |")
            for r in mrows:
                x = r["result"]
                w = x["writes"]
                f = x["write_fsync"] or {}

                def ms(*cats):
                    return sum(f.get(c, {}).get("seconds", 0) for c in cats) * 1e3 / w
                n = sum(v["count"] for v in f.values()) / w
                mean = x["write_latency_ms"]["mean"]
                seg, log = ms("segment file", "segment dir"), ms("log file", "log dir")
                L.append(f"| {r['dataset']} | {r['system']} | {w:,} | {mean:.2f} | {n:.1f} | {seg:.2f} | {log:.2f} | "
                         f"{mean - seg - log - ms('other file', 'other dir'):.2f} |")
            L.append("")
    L.append("## Runs")
    L.append("")
    seen = set()
    for r in rep["runs"]:
        k = (r["system"], r["result"].get("version"))
        if k in seen:
            continue
        seen.add(k)
        L.append(f"- **{r['system']}** ({r['result'].get('version')}): {r['result'].get('notes')}")
    dates = sorted({r["date"] for r in rep["runs"]})
    L.append("")
    L.append(f"Run dates: {', '.join(dates)}.")
    L.append("")
    return "\n".join(L)


def summary_markdown(rep, link: str) -> list[str]:
    """The Ingest section embedded in the query reports (run.py): wall time and rows/s per
    dataset, mode and system, one table per storage."""
    L = ["## Ingest", "",
         "Two modes, so that ingest compares like with like (details, per-write latency and "
         f"fsync counts: [{link}]({link})):", "",
         "* **per-batch**: one commit per original write batch in arrival order "
         "(Muninn `write`, DuckDB `BEGIN/INSERT/COMMIT` on a file, ClickHouse one `INSERT` per "
         "batch; `clickhouse` does not fsync inserts by default, `clickhouse-fsync` does);",
         "* **bulk**: as few writes as possible (DuckDB/ClickHouse one statement, Muninn 1M-row "
         "writes; `muninn-100k`: 100k-row writes).", "",
         BUILDS_NOTE,
         "",
         "Cells: wall seconds incl. finalisation (rows/s). *prefix*: only the first N batches, "
         "the same N for every system.", ""]
    systems = sorted({r["system"] for r in rep["runs"]}, key=sys_key)
    by_storage: dict = {}
    for r in rep["runs"]:
        by_storage.setdefault(r["storage"], {})[(r["dataset"], r["mode"], r["system"])] = r["result"]
    for storage, runs in by_storage.items():
        present = [s for s in systems if any(k[2] == s for k in runs)]
        L.append(f"Storage: **{storage}**")
        L.append("")
        L.append("| dataset | mode | " + " | ".join(present) + " |")
        L.append("| --- | --- |" + " ---: |" * len(present))
        for ds in DATASETS:
            for mode in INGEST_MODES:
                cells, pre = [], ""
                for s in present:
                    x = runs.get((ds, mode, s))
                    if x is None:
                        cells.append("-")
                        continue
                    if not x["complete"]:
                        pre = f" (prefix {x['writes']:,} of {x['batches_total']:,})"
                    cells.append(f"{fmt_s(x['wall_seconds'])} ({fmt_rate(x['rows_per_second'])})")
                if any(c != "-" for c in cells):
                    L.append(f"| {ds} | {mode}{pre} | " + " | ".join(cells) + " |")
        L.append("")
    return L


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scale", default="small", choices=["tiny", "small", "medium", "large"])
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--systems", default=",".join(DEFAULT_SYSTEMS))
    ap.add_argument("--modes", default=",".join(INGEST_MODES))
    ap.add_argument("--datasets", default=",".join(DATASETS))
    ap.add_argument("--prefix", default=DEFAULT_PREFIX,
                    help="per-batch mode: dataset=N[,dataset=N] loads only the first N batches "
                         "(for every system)")
    ap.add_argument("--threads", type=int, default=6)
    ap.add_argument("--memory-mb", type=int, default=4096)
    ap.add_argument("--data", default=str(HERE / "data"))
    ap.add_argument("--workdir", default=str(HERE / "data" / "work"))
    ap.add_argument("--out", default=None, help="report directory (default results/<date>-<host>)")
    ap.add_argument("--render", default=None, help="only re-render <dir>/ingest-<scale>.md")
    ap.add_argument("--tag", default=None,
                    help="label the systems of this run as '<system> (<tag>)' in the report, e.g. "
                         "the engine build (post-0009)")
    args = ap.parse_args(argv)

    if args.render:
        p = Path(args.render) / f"ingest-{args.scale}.json"
        p.with_suffix(".md").write_text(markdown(json.loads(p.read_text())))
        return 0
    ensure_shim()
    probe = make_probe()
    if probe is None:
        print("fsync counters not available (no cc / shim not loaded)", file=sys.stderr)

    data_dir = Path(args.data) / f"{args.scale}-s{args.seed}"
    datasets = args.datasets.split(",")
    subprocess.run([sys.executable, "-m", "datagen", "--scale", args.scale, "--seed", str(args.seed),
                    "--out", str(data_dir), "--datasets", ",".join(datasets)], cwd=HERE, check=True,
                   env={k: v for k, v in os.environ.items() if k != "LD_PRELOAD"})
    prefix = {k: int(v) for k, v in (x.split("=") for x in args.prefix.split(",") if x)}
    storage = storage_of(Path(args.workdir))
    date = dt.date.today().isoformat()
    out = Path(args.out) if args.out else HERE / "results" / f"{date}-{socket.gethostname()}"
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"ingest-{args.scale}.json"
    rep = json.loads(path.read_text()) if path.exists() else {"scale": args.scale, "runs": []}
    rep["machine"] = machine_info()
    for ds in datasets:
        for mode in args.modes.split(","):
            for system in args.systems.split(","):
                if (system in BULK_ONLY and mode != "bulk") or (system in PER_BATCH_ONLY
                                                                and mode != "per-batch"):
                    continue  # the same as muninn in the other mode
                label = f"{system} ({args.tag})" if args.tag else system
                print(f"[{storage}] {ds} {mode} {label} ...", flush=True)
                try:
                    r = run_one(system, mode, ds, data_dir, args, prefix.get(ds), probe)
                except registry.Unavailable as e:
                    print(f"  skipped: {e}")
                    continue
                lat = r.get("write_latency_ms") or {}
                print(f"  {r['writes']:,} writes, {r['rows']:,} rows{'' if r['complete'] else ' (prefix)'}: "
                      f"wall {r['wall_seconds']:.1f} s (write {r['write_seconds']:.1f}, finalize "
                      f"{r['finalize_seconds']:.1f}), {r['writes_per_second']} writes/s, "
                      f"{r['rows_per_second']:,} rows/s, p50 {lat.get('p50')} ms, "
                      f"{fmt_bytes(r['bytes_on_disk'])}, fsync {fsync_dict_total(r.get('fsync'))}, "
                      f"peak RSS {rss_cell(r)} MB",
                      flush=True)
                key = (storage, label, ds, mode)
                rep["runs"] = [x for x in rep["runs"]
                               if (x["storage"], x["system"], x["dataset"], x["mode"]) != key]
                rep["runs"].append({"storage": storage, "workdir": args.workdir, "system": label,
                                    "dataset": ds, "mode": mode, "date": date,
                                    "host": socket.gethostname(), "result": r})
                path.write_text(json.dumps(rep, indent=1))
                path.with_suffix(".md").write_text(markdown(rep))
    print(f"report: {path.with_suffix('.md')}")
    return 0


def fsync_dict_total(f) -> str:
    if not f:
        return "-"
    return (f"{sum(v['count'] for v in f.values()):,} calls, "
            f"{sum(v['seconds'] for v in f.values()):.1f} s")


if __name__ == "__main__":
    sys.exit(main())
