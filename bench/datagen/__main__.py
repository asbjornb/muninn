"""Generate the benchmark datasets.

    uv run --project bench python -m datagen --scale small --seed 1 --out bench/data/small-s1

Layout of the output directory::

    <out>/<dataset>/versions.parquet   stored versions, commit (seq) order
    <out>/<dataset>/resends.parquet    exact duplicate re-sends dropped by SPEC §4.3
    <out>/<dataset>/catalog.json       tables, columns, series, generator config, stats
    <out>/<dataset>/scenario.json      (--export-scenario) SPEC §9 write steps
"""

from __future__ import annotations

import argparse
import json
import resource
import shutil
import time
from pathlib import Path

from .common import SeriesSpec, VersionWriter, write_catalog
from .datasets import DATASETS, SCALES, START, now_of

GENERATOR_VERSION = 2


def generate_dataset(name: str, scale: str, seed: int, out: Path) -> dict:
    cfg = SCALES[scale]
    from . import datasets
    datasets.QUIRK = 20.0 if scale == "tiny" else 1.0
    table, series_fn, gen = DATASETS[name]
    series: list[SeriesSpec] = series_fn(cfg)
    d = out / name
    t0 = time.perf_counter()
    w = VersionWriter(d, table, series)
    if name == "da_prices":
        gen(cfg, seed, w)
    else:
        gen(cfg, seed, w, series)
    info = w.close()
    gen_s = time.perf_counter() - t0
    catalog = {
        "database": name,
        "generator_version": GENERATOR_VERSION,
        "scale": scale,
        "seed": seed,
        "start": START,
        "now": now_of(name, cfg),
        "config": {k: v for k, v in cfg.items()},
        "tables": [{
            "name": table.name,
            "columns": [{"name": n, "type": t} for n, t in table.columns],
            "default_clock": table.default_clock,
            "partition": table.partition,
        }],
        "series": [{"id": s.id, "name": s.name, "table": table.name, "tags": s.tags,
                    "resolution": s.resolution} for s in series],
        "write": info,
        "generate_seconds": round(gen_s, 2),
    }
    catalog["stats"] = dataset_stats(d)
    write_catalog(d / "catalog.json", catalog)
    return catalog


def dataset_stats(d: Path) -> dict:
    import duckdb

    con = duckdb.connect(config={"threads": 2, "memory_limit": "600MB",
                                 "temp_directory": str(d / ".tmp"), "preserve_insertion_order": False})
    p = str(d / "versions.parquet")
    q = f"""
    WITH v AS (SELECT * FROM read_parquet('{p}')),
    per_point AS (SELECT count(*) n FROM v GROUP BY series_id, value_time)
    SELECT
      (SELECT count(*) FROM v) AS versions,
      (SELECT count(*) FROM per_point) AS points,
      (SELECT count(DISTINCT series_id) FROM v) AS series_with_data,
      (SELECT avg(n) FROM per_point) AS vpp_mean,
      (SELECT quantile_disc(n, 0.5) FROM per_point) AS vpp_p50,
      (SELECT quantile_disc(n, 0.99) FROM per_point) AS vpp_p99,
      (SELECT max(n) FROM per_point) AS vpp_max,
      (SELECT count(*) FILTER (WHERE deleted) FROM v) AS tombstones,
      (SELECT count(*) FILTER (WHERE published_time IS NULL) FROM v) AS null_published,
      (SELECT count(*) FILTER (WHERE published_time > received_time) FROM v) AS published_after_received,
      (SELECT epoch_us(min(value_time)) FROM v) AS value_time_min,
      (SELECT epoch_us(max(value_time)) FROM v) AS value_time_max,
      (SELECT epoch_us(min(received_time)) FROM v) AS received_min,
      (SELECT epoch_us(max(received_time)) FROM v) AS received_max,
      (SELECT epoch_us(max(system_time)) FROM v) AS system_max
    """
    cur = con.execute(q)
    row = cur.fetchone()
    cols = [c[0] for c in cur.description]
    stats = {k: (float(v) if k == "vpp_mean" else int(v)) for k, v in zip(cols, row)}
    stats["vpp_mean"] = round(stats["vpp_mean"], 2)
    stats["parquet_bytes"] = (d / "versions.parquet").stat().st_size
    # SPEC 4.3 sanity: no two stored versions may be content-identical (the generator
    # must have flagged such rows as duplicates instead)
    content = "series_id, value_time, published_time, received_time, deleted, " + ", ".join(
        c for c in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{p}')").fetchnumpy()["column_name"]
        if c not in ("series", "series_id", "value_time", "published_time", "received_time",
                     "system_time", "seq", "deleted"))
    stats["content_duplicates"] = con.execute(
        f"SELECT count(*) FROM (SELECT count(*) n FROM read_parquet('{p}') GROUP BY {content} "
        f"HAVING n > 1)").fetchone()[0]
    assert stats["content_duplicates"] == 0, f"{d}: content-identical stored versions"
    stats["resends"] = con.execute(
        f"SELECT count(*) FROM read_parquet('{d / 'resends.parquet'}')").fetchone()[0]
    con.close()
    shutil.rmtree(d / ".tmp", ignore_errors=True)
    return stats


def main(argv=None):
    ap = argparse.ArgumentParser(prog="datagen", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scale", choices=list(SCALES), default="tiny")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--datasets", default=",".join(DATASETS),
                    help="comma-separated subset of: " + ", ".join(DATASETS))
    ap.add_argument("--export-scenario", action="store_true",
                    help="also write a SPEC §9 scenario with the write steps (tiny only)")
    ap.add_argument("--force", action="store_true", help="regenerate even if present")
    a = ap.parse_args(argv)
    a.out.mkdir(parents=True, exist_ok=True)
    for name in a.datasets.split(","):
        cat_path = a.out / name / "catalog.json"
        if cat_path.exists() and not a.force:
            cat = json.loads(cat_path.read_text())
            if (cat.get("scale"), cat.get("seed"), cat.get("generator_version")) == \
                    (a.scale, a.seed, GENERATOR_VERSION):
                print(f"{name}: up to date ({cat['stats']['versions']:,} versions)")
                continue
        cat = generate_dataset(name, a.scale, a.seed, a.out)
        st = cat["stats"]
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        print(f"{name}: {st['versions']:,} versions, {st['points']:,} points, "
              f"{st['vpp_mean']} v/pt, {st['parquet_bytes'] / 1e6:.1f} MB parquet, "
              f"{cat['generate_seconds']}s, peak RSS {rss:.0f} MB", flush=True)
        if a.export_scenario:
            from .scenario_export import export_scenario
            export_scenario(a.out / name)


if __name__ == "__main__":
    main()
