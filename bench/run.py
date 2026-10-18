"""Run the benchmark end to end: generate data, load each system, run the workload, write
a JSON + Markdown report to bench/results/<date>-<host>/<scale>.{json,md}.

    uv run --project bench python bench/run.py --scale small
    uv run --project bench python bench/run.py --scale medium --systems duckdb-native,clickhouse
    uv run --project bench python bench/run.py --scale tiny --check      # + oracle check

Measurement protocol, per (system, dataset):
  * ingest: the adapter's own load from the generator's Parquet into its tuned layout
    (sorting, compression, merges included); bytes on disk as the adapter reports them;
  * per query template: ``cold()`` then instance 0 once = the *cold* sample; then every
    instance ``--warm-runs`` times = *warm* samples (p50/p99 over those). A per-template
    time budget caps repetitions of very slow queries (the report says how many samples
    there were);
  * every result is hashed in canonical form; the report flags any template where systems
    disagree (at scales the oracle can't check, systems at least check each other).
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import platform
import socket
import statistics
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import pyarrow as pa  # noqa: E402

from competitors import registry  # noqa: E402
from datagen.datasets import DATASETS  # noqa: E402
from queries.params import load_workload, workload_for  # noqa: E402


def stable_hash(t: pa.Table, drop=()) -> str:
    """Layout-independent hash: hashes the normalised Python values row by row."""
    from competitors.compare import rows

    if drop:
        t = t.drop_columns([c for c in drop if c in t.column_names])
    h = hashlib.blake2b(digest_size=12)
    h.update(",".join(t.column_names).encode())
    for r in rows(t):
        h.update(repr(r).encode())
    return h.hexdigest()


def pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    k = (len(xs) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def machine_info() -> dict:
    info = {"host": socket.gethostname(), "os": platform.platform(), "python": platform.python_version(),
            "cpus": os.cpu_count()}
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                info["cpu"] = line.split(":", 1)[1].strip()
                break
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal"):
                info["mem_gb"] = round(int(line.split()[1]) / 1024 / 1024, 1)
    except OSError:
        pass
    try:
        info["disk"] = subprocess.run(["findmnt", "-no", "SOURCE,FSTYPE", "-T", str(HERE)],
                                      capture_output=True, text=True).stdout.strip()
    except OSError:
        pass
    return info


def run_system(system, data_dir, datasets, scale, args, report):
    for ds in datasets:
        cat = json.loads((data_dir / ds / "catalog.json").read_text())
        workdir = Path(args.workdir) / scale / system / ds
        a = None
        try:
            a = registry.make(system, workdir, threads=args.threads, memory_limit_mb=args.memory_mb)
            print(f"[{system}] {ds}: loading ...", flush=True)
            load = a.load(data_dir / ds, cat)
        except registry.Unavailable as e:
            if a is None:
                print(f"[{system}] unavailable: {e}")
                report["systems"][system] = {"unavailable": str(e)}
                return
            print(f"[{system}] {ds}: skipped: {e}")
            sysrep = report["systems"].setdefault(system, {"notes": a.notes, "version": a.version(),
                                                           "datasets": {}})
            sysrep.setdefault("skipped", {})[ds] = str(e)
            a.close()
            continue
        sysrep = report["systems"].setdefault(system, {"notes": a.notes, "version": a.version(),
                                                       "datasets": {}})
        drep = sysrep["datasets"][ds] = {
            "ingest_seconds": round(load["ingest_seconds"], 3),
            "bytes_on_disk": load["bytes_on_disk"],
            "bytes_per_version": round(load["bytes_on_disk"] / max(1, cat["stats"]["versions"]), 2),
            "queries": {},
            "exact_system_time": getattr(a, "exact", True),
        }
        for k, val in load.items():
            if k not in ("ingest_seconds", "bytes_on_disk"):
                drep[k] = val
        print(f"[{system}] {ds}: ingest {load['ingest_seconds']:.1f}s, "
              f"{load['bytes_on_disk'] / 1e6:.1f} MB", flush=True)
        qs = workload_for(ds, cat, scale, set(args.queries.split(",")) if args.queries else None)
        by_id: dict[str, list] = {}
        for q in qs:
            by_id.setdefault(q["id"], []).append(q)
        for qid, inst in by_id.items():
            a.cold()
            t0 = time.perf_counter()
            res = a.run(inst[0])
            cold = time.perf_counter() - t0
            hashes = {0: stable_hash(res)}
            nosys = {0: stable_hash(res, drop=("system_time",))}
            rows = {0: res.num_rows}
            warm = []
            budget_end = time.perf_counter() + args.budget
            done = False
            for rep in range(args.warm_runs):
                for i, q in enumerate(inst):
                    t0 = time.perf_counter()
                    r = a.run(q)
                    warm.append(time.perf_counter() - t0)
                    if i not in hashes:
                        hashes[i] = stable_hash(r)
                        nosys[i] = stable_hash(r, drop=("system_time",))
                        rows[i] = r.num_rows
                    if time.perf_counter() > budget_end and len(warm) >= 3:
                        done = True
                        break
                if done:
                    break
            drep["queries"][qid] = {
                "instances": len(inst),
                "cold_ms": round(cold * 1e3, 3),
                "warm_samples": len(warm),
                "p50_ms": round(pct(warm, 50) * 1e3, 3),
                "p99_ms": round(pct(warm, 99) * 1e3, 3),
                "mean_rows": round(statistics.mean(rows.values()), 1),
                "result_hashes": {str(k): v for k, v in sorted(hashes.items())},
                "result_hashes_without_system_time": {str(k): v for k, v in sorted(nosys.items())},
            }
            print(f"[{system}] {ds}/{qid}: cold {cold * 1e3:.1f} ms, p50 "
                  f"{drep['queries'][qid]['p50_ms']:.1f} ms, p99 {drep['queries'][qid]['p99_ms']:.1f} ms "
                  f"({len(warm)} samples, ~{drep['queries'][qid]['mean_rows']:.0f} rows)", flush=True)
        a.close()


def cross_check(report) -> dict:
    """For each (dataset, query, instance): do all systems agree?"""
    out = {}
    systems = {k: v for k, v in report["systems"].items() if "datasets" in v}
    # a system that could not reproduce a dataset's system_time values (see its adapter)
    # is compared on everything else for that dataset
    inexact = {ds for rep in systems.values() for ds, d in rep["datasets"].items()
               if not d.get("exact_system_time", True)}
    for s, rep in systems.items():
        for ds, d in rep["datasets"].items():
            key = "result_hashes_without_system_time" if ds in inexact else "result_hashes"
            for qid, q in d["queries"].items():
                for inst, h in q[key].items():
                    out.setdefault((ds, qid, inst), {})[s] = h
    disagreements = []
    for (ds, qid, inst), hs in out.items():
        if len(set(hs.values())) > 1:
            disagreements.append({"dataset": ds, "query": qid, "instance": inst, "hashes": hs})
    return {"compared_results": len(out), "disagreements": disagreements,
            "compared_without_system_time": sorted(inexact)}


def fmt_bytes(n):
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024


def fmt_ms(x):
    if x is None:
        return "-"
    return f"{x:.2f}" if x < 10 else f"{x:.0f}"


def markdown(report) -> str:
    L = []
    m = report["machine"]
    L.append(f"# Muninn benchmark — {report['scale']} scale, seed {report['seed']}")
    L.append("")
    L.append(f"{report['date']} on `{m['host']}`: {m.get('cpu', '?')}, {m['cpus']} logical CPUs, "
             f"{m.get('mem_gb', '?')} GB RAM, {m['os']}. Systems limited to "
             f"{report['threads']} threads and {report['memory_mb']} MB. "
             "Latencies in ms, end to end through the Python client to a canonical Arrow table. "
             "p50/p99 over warm samples (few samples ⇒ p99 ≈ max).")
    L.append("")
    L.append("## Datasets")
    L.append("")
    L.append("| dataset | series | versions | points | versions/point (mean / p99 / max) | "
             "tombstones | null published | generator Parquet |")
    L.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for ds, c in report["datasets"].items():
        st = c["stats"]
        L.append(f"| {ds} | {len(c['series']) if 'series' in c else c['series_count']:,} | {st['versions']:,} | "
                 f"{st['points']:,} | {st['vpp_mean']} / {st['vpp_p99']} / {st['vpp_max']} | "
                 f"{st['tombstones']:,} | {st['null_published']:,} | {fmt_bytes(st['parquet_bytes'])} |")
    L.append("")
    systems = [s for s, v in report["systems"].items() if "datasets" in v]
    L.append("## Ingest and size on disk")
    L.append("")
    L.append("| dataset | " + " | ".join(f"{s} ingest s | {s} size (B/version)" for s in systems) + " |")
    L.append("| --- |" + " ---: | ---: |" * len(systems))
    for ds in report["datasets"]:
        cells = []
        for s in systems:
            d = report["systems"][s]["datasets"].get(ds)
            if d:
                cells += [f"{d['ingest_seconds']:.1f}", f"{fmt_bytes(d['bytes_on_disk'])} ({d['bytes_per_version']})"]
            else:
                cells += ["-", "-"]
        L.append(f"| {ds} | " + " | ".join(cells) + " |")
    L.append("")
    L.append("## Query latency (ms): p50 / p99 warm, cold")
    L.append("")
    L.append("| query | rows | " + " | ".join(systems) + " |")
    L.append("| --- | ---: |" + " ---: |" * len(systems))
    for t in load_workload():
        ds, qid = t["dataset"], t["id"]
        cells, rows = [], None
        for s in systems:
            q = report["systems"][s]["datasets"].get(ds, {}).get("queries", {}).get(qid)
            if q:
                rows = q["mean_rows"]
                cells.append(f"{fmt_ms(q['p50_ms'])} / {fmt_ms(q['p99_ms'])}, {fmt_ms(q['cold_ms'])}")
            else:
                cells.append("-")
        if rows is None:
            continue
        L.append(f"| `{qid}` | {rows:,.0f} | " + " | ".join(cells) + " |")
    L.append("")
    cc = report["cross_check"]
    L.append("## Agreement")
    L.append("")
    if report.get("oracle_check"):
        oc = report["oracle_check"]
        L.append(f"Oracle check (reference implementation): {oc['checked']} results checked, "
                 f"{len(oc['mismatches'])} mismatches.")
        L.append("")
    if cc["disagreements"]:
        L.append(f"**{len(cc['disagreements'])} of {cc['compared_results']} results differ between systems:**")
        for d in cc["disagreements"][:20]:
            L.append(f"- {d['dataset']}/{d['query']}#{d['instance']}: {d['hashes']}")
    else:
        L.append(f"All systems returned identical results for all {cc['compared_results']} "
                 "(dataset, query, instance) combinations.")
    if cc.get("compared_without_system_time"):
        L.append("")
        L.append("Compared on every field except `system_time` (a system loaded them with "
                 "coalesced batches, see its notes): " + ", ".join(cc["compared_without_system_time"]) + ".")
    L.append("")
    L.append("## Systems")
    L.append("")
    for s, v in report["systems"].items():
        if "unavailable" in v:
            L.append(f"- **{s}**: not run ({v['unavailable']})")
        else:
            L.append(f"- **{s}** ({v['version']}): {v['notes']}")
            for ds, why in v.get("skipped", {}).items():
                L.append(f"  - {ds}: not run ({why})")
    L.append("")
    L.append("## What each query stresses")
    L.append("")
    for t in load_workload():
        L.append(f"- `{t['id']}` ({t['dataset']}, {t['op']}): {t['stresses']}")
    L.append("")
    return "\n".join(L)


def oracle_check(data_dir, datasets, scale, systems, args) -> dict:
    from competitors.compare import diff
    from competitors.reference.adapter import ReferenceAdapter

    checked, mismatches = 0, []
    for ds in datasets:
        cat = json.loads((data_dir / ds / "catalog.json").read_text())
        ref = ReferenceAdapter(Path(args.workdir) / "ref")
        ref.load(data_dir / ds, cat)
        qs = workload_for(ds, cat, scale)
        expected = [(q, ref.run(q)) for q in qs]
        for s in systems:
            try:
                a = registry.make(s, Path(args.workdir) / scale / "check" / s / ds, threads=args.threads,
                                  memory_limit_mb=args.memory_mb)
                a.load(data_dir / ds, cat)
            except registry.Unavailable:
                continue
            for q, exp in expected:
                checked += 1
                d = diff(exp, a.run(q))
                if d:
                    mismatches.append({"system": s, "dataset": ds, "query": q["id"],
                                       "instance": q["instance"], "diff": d[:500]})
            a.close()
    return {"checked": checked, "mismatches": mismatches}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scale", default="small", choices=["tiny", "small", "medium", "large"])
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--systems", default=",".join(registry.DEFAULT_SYSTEMS))
    ap.add_argument("--datasets", default=",".join(DATASETS))
    ap.add_argument("--queries", default=None, help="comma-separated query ids (default: all)")
    ap.add_argument("--warm-runs", type=int, default=3, help="passes over the instances")
    ap.add_argument("--budget", type=float, default=30.0, help="seconds of warm runs per query")
    ap.add_argument("--threads", type=int, default=6)
    ap.add_argument("--memory-mb", type=int, default=4096)
    ap.add_argument("--data", default=str(HERE / "data"))
    ap.add_argument("--workdir", default=str(HERE / "data" / "work"))
    ap.add_argument("--out", default=None, help="report directory (default results/<date>-<host>)")
    ap.add_argument("--check", action="store_true", help="also compare with the reference (tiny only)")
    args = ap.parse_args(argv)

    data_dir = Path(args.data) / f"{args.scale}-s{args.seed}"
    datasets = args.datasets.split(",")
    subprocess.run([sys.executable, "-m", "datagen", "--scale", args.scale, "--seed", str(args.seed),
                    "--out", str(data_dir), "--datasets", ",".join(datasets)], cwd=HERE, check=True)
    date = dt.date.today().isoformat()
    report = {
        "scale": args.scale, "seed": args.seed, "date": date, "machine": machine_info(),
        "threads": args.threads, "memory_mb": args.memory_mb, "warm_runs": args.warm_runs,
        "budget_seconds": args.budget, "datasets": {}, "systems": {},
    }
    for ds in datasets:
        cat = json.loads((data_dir / ds / "catalog.json").read_text())
        report["datasets"][ds] = {"series_count": len(cat["series"]), "stats": cat["stats"],
                                  "write": cat["write"], "config": cat["config"]}
    systems = args.systems.split(",")
    for s in systems:
        run_system(s, data_dir, datasets, args.scale, args, report)
    report["cross_check"] = cross_check(report)
    if args.check:
        report["oracle_check"] = oracle_check(data_dir, datasets, args.scale, systems, args)
    out = Path(args.out) if args.out else HERE / "results" / f"{date}-{socket.gethostname()}"
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{args.scale}.json").write_text(json.dumps(report, indent=1))
    (out / f"{args.scale}.md").write_text(markdown(report))
    print(f"report: {out / (args.scale + '.md')}")
    if report["cross_check"]["disagreements"] or (args.check and report["oracle_check"]["mismatches"]):
        print("WARNING: systems disagree / oracle mismatches, see report", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
