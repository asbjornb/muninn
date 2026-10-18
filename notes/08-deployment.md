# Deployment, scale and adoption

## Is single-node still a thing?

Yes — arguably more than five years ago:

- **DuckDB** is single-node and one of the most-adopted data engines of recent years.
  The "your data isn't that big" argument has largely won for analytics.
- **kdb+** shops run single (big) boxes for tick data, at volumes far beyond this use case.
- One modern machine has 100+ cores, TBs of RAM and many TB of NVMe. Scale *up* goes a
  very long way before scale *out* is needed.

Rough sizing to sanity-check (to be replaced with real numbers): 10,000 series × 15-min
resolution × ~20 versions per point × 5 years ≈ 35 billion rows. Columnar and compressed,
that is on the order of hundreds of GB — one node. Read-heavy backtests scale
horizontally anyway (see below) because the data is immutable.

What *doesn't* scale on one node: write throughput (irrelevant here — market data is
modest) and availability (one writer down = no ingest for a while; reads can continue).

## What gets adopted in a cloud-first world

The pattern that has won recently is **compute/storage separation on object storage**:
immutable files in S3 / Azure Blob / GCS, stateless compute in front, local disk only as a
cache. Examples: Delta Lake / Iceberg / DuckLake, LanceDB, turbopuffer, Neon, WarpStream,
SlateDB.

Our design fits this almost for free:

- Segments are immutable → plain objects.
- The only mutable thing is the manifest → atomic commit via conditional write
  (compare-and-swap; S3, Azure Blob and GCS all support this now).
- Single writer → no distributed coordination; just a lease so two writers can't run.
- Readers → any number of stateless processes reading the same bucket, each with a local
  NVMe/RAM cache. That's how read scaling works without "distributed database" machinery.

So "single node" really means **single writer**. Readers are unlimited.

## Deployment shapes (in order of how easy they are to adopt)

1. **Embedded library** (`pip install`, NuGet package) that reads directly from a
   local dir or a bucket. Zero infrastructure — this is why DuckDB/LanceDB spread. A
   backtest in a notebook just points at `s3://.../muninn/` (or `az://...`).
2. **A container**: writer + query server (HTTP / Arrow Flight). Runs on k8s, ECS,
   Azure Container Apps, Cloud Run, or `docker run` on a VM. Data lives in the bucket, so
   the container is disposable.
3. **Managed service** — not our problem. Not being a clickable AWS/Azure/GCP component
   is fine if (1) and (2) are trivial; plenty of things people self-host (Grafana,
   ClickHouse OSS, QuestDB, kdb) are adopted this way.

## How this might look at work (.NET shop)

Something like:

- Writer/ingest container on AKS or Azure Container Apps, data in Azure Blob Storage.
- C# services talk to the server over Arrow Flight or HTTP (thin .NET client).
- Quants/backtests use the Python library directly against Blob storage (read-only),
  skipping the server entirely.

Whether the engine itself is written in C# or Rust matters less for this than having a
good .NET client. A Rust core with Python bindings + a .NET client over Flight is a
common shape; a C# core makes .NET embedding trivial but Python bindings harder.

## Testing locally

Must be possible to develop and test everything on a laptop:

- Storage behind a small interface (`get / put / put_if_match / list / delete`) with a
  local-filesystem backend and an in-memory backend for tests.
- Object store emulators in docker compose: **MinIO** (S3) and **Azurite** (Azure Blob)
  to run the same tests against real-ish cloud semantics.
- The single-writer design makes **deterministic simulation testing** feasible
  (TigerBeetle/FoundationDB style): inject crashes, slow/failed writes, reordered
  responses, and check results against the reference model from roadmap step 0.
- Containers for the server mode, so "works in k8s" is the same image we test locally.

## Blob storage is slow — and that's OK

Object stores have high latency (often 10–100 ms to first byte, worse in the tail) but
good throughput. That hurts small random reads and per-write commits, not big scans.
Mitigations, all standard:

- Large segments, ranged reads, few requests per query; prefetch in parallel.
- Local NVMe/RAM cache of hot segments (recent partitions are what backtests hammer).
- Local WAL for ingest; flush to the bucket in batches.

But this isn't something Muninn has to win at. Because storage sits behind an interface,
**a single physical server with local NVMe is just another backend**. If blob latency is
a problem for someone, they run it on a box and serve reads over the network. Object
storage is an option, not a requirement.

## Central data, many teams

A trading firm most likely wants one central copy that many teams (trading, analysts,
quants, risk) read from, not a copy per team. (To verify: is that really how firms
want it?) Both shapes support this:

- **Central server** (the kdb way): one Muninn server on a big box or in a container;
  everyone reads through it over Flight/HTTP. Also the natural place for access control.
- **Central bucket**: one writer, teams read the bucket directly with the library.
  Simpler, scales reads for free, but access control is coarser (bucket/prefix level).

Likely both exist eventually; the server is probably what a firm deploys first.

## Open

- Object storage as source of truth from v1, or local disk first with the interface in
  place? (Leaning: interface from day one, local-fs backend first, S3/Blob soon after.)
- Write latency: object store PUTs are ~10–100 ms. Fine for batch ingest; for streaming,
  a local WAL + periodic segment flush to the bucket.
- Writer failover: lease in the bucket? How long can ingest be down?
