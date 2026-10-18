"""Muninn engine adapter — placeholder until the engine exposes a binding or CLI.

It is written against the API the spec implies, so wiring the engine in should only mean
filling in ``_open``/``_ingest`` and checking the result conversion:

* a Python binding ``muninn`` (e.g. PyO3) with ``Database.open(path)``,
  ``create_table``/``create_series``, a bulk ``write_parquet(path)`` that keeps the given
  system_time/seq (or a ``write`` of batches), and reads ``latest``, ``history``,
  ``replay``, ``grid``, ``asof_join`` taking the query dict fields of
  competitors/base.py and returning Arrow tables; or
* a CLI ``$MUNINN_BIN`` with ``load`` and ``query --json`` subcommands writing Arrow IPC to
  stdout.

Until either exists the adapter raises Unavailable, so tests skip and the runner reports
"not run".
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import pyarrow as pa

from ..base import Adapter, Unavailable, result_fields


class MuninnAdapter(Adapter):
    name = "muninn"

    def __init__(self, workdir, threads=6, memory_limit_mb=4096):
        super().__init__(workdir, threads, memory_limit_mb)
        try:
            import muninn  # noqa: F401
            self.mode = "python"
        except ImportError:
            self.bin = os.environ.get("MUNINN_BIN")
            if not self.bin:
                raise Unavailable("Muninn engine not available: no `muninn` Python module and "
                                  "MUNINN_BIN not set")
            self.mode = "cli"
        self.notes = f"Muninn engine via {self.mode}."

    def load(self, dataset_dir: Path, catalog: dict) -> dict:
        import subprocess

        self.columns = [c["name"] for c in catalog["tables"][0]["columns"]]
        t0 = time.perf_counter()
        if self.mode == "python":
            import muninn

            self.db = muninn.Database.open(str(self.workdir))
            self.db.load_bench_dataset(str(dataset_dir / "catalog.json"),
                                       str(dataset_dir / "versions.parquet"))
        else:
            subprocess.run([self.bin, "load", "--db", str(self.workdir),
                            "--catalog", str(dataset_dir / "catalog.json"),
                            "--parquet", str(dataset_dir / "versions.parquet")], check=True)
        ingest = time.perf_counter() - t0
        size = sum(p.stat().st_size for p in Path(self.workdir).rglob("*") if p.is_file())
        return {"ingest_seconds": ingest, "bytes_on_disk": size}

    def run(self, q: dict) -> pa.Table:
        import json
        import subprocess

        if self.mode == "python":
            res = getattr(self.db, q["op"])(**{k: v for k, v in q.items()
                                               if k not in ("id", "instance", "dataset", "op")})
        else:
            out = subprocess.run([self.bin, "query", "--db", str(self.workdir), "--json", json.dumps(q)],
                                 check=True, capture_output=True).stdout
            res = pa.ipc.open_stream(out).read_all()
        return res.select(result_fields(q["op"], self.columns))
