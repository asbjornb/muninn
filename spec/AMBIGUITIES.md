# Open spec questions (v0.1)

Former items A1–A9 and A11 were ratified into `SPEC.md` §10 (input rules and check order)
and §8 (`invalid_argument`). What remains open:

## Catalog change log timestamps (§2.3)

§2.3 says every catalog change is logged with its `system_time`, but catalog steps carry
no clock in the scenario format and no v0.1 read exposes the log, so the value is not
observable or testable.

The reference stamps each change with `max(c, greatest system_time seen so far in
versions or the catalog log)` — `c` being an optional clock reading passed by library
callers — or no time before anything has one. Catalog changes do **not** advance `last`
in §4.2, which is defined over stored versions only.

To close: decide whether catalog changes share the write clock and the §4.2 `max(c, last + 1us)` rule
(and whether they advance `last`), and add a clock field to catalog steps once
point-in-time catalog reads exist.
