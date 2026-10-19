//! Integration tests: run the real `muninn` binary in temporary directories.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use tempfile::TempDir;

const BIN: &str = env!("CARGO_BIN_EXE_muninn");

/// A temporary working directory with a database at `db/`.
struct T {
    dir: TempDir,
}

impl T {
    fn new() -> T {
        let t = T {
            dir: tempfile::tempdir().unwrap(),
        };
        t.ok(&["init", t.db().to_str().unwrap()], "");
        t
    }

    fn db(&self) -> PathBuf {
        self.dir.path().join("db")
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn write_file(&self, name: &str, text: &str) -> String {
        let p = self.path(name);
        fs::write(&p, text).unwrap();
        p.to_str().unwrap().to_owned()
    }

    fn run_on(&self, db: &Path, args: &[&str], stdin: &str) -> Output {
        let mut child = Command::new(BIN)
            .arg("--db")
            .arg(db)
            .args(args)
            .env_remove("MUNINN_DB")
            .env_remove("MUNINN_WRITER_CLOCK")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        // Feed stdin from a thread so a child filling its stdout pipe cannot deadlock us.
        let mut pipe = child.stdin.take().unwrap();
        let input = stdin.as_bytes().to_vec();
        let feeder = std::thread::spawn(move || {
            let _ = pipe.write_all(&input);
        });
        let out = child.wait_with_output().unwrap();
        feeder.join().unwrap();
        out
    }

    fn run(&self, args: &[&str], stdin: &str) -> Output {
        self.run_on(&self.db(), args, stdin)
    }

    fn ok_on(&self, db: &Path, args: &[&str], stdin: &str) -> String {
        let out = self.run_on(db, args, stdin);
        assert!(
            out.status.success(),
            "muninn {args:?} failed: {}\n{}",
            String::from_utf8_lossy(&out.stderr),
            String::from_utf8_lossy(&out.stdout)
        );
        String::from_utf8(out.stdout).unwrap()
    }

    fn ok(&self, args: &[&str], stdin: &str) -> String {
        self.ok_on(&self.db(), args, stdin)
    }

    /// Runs a command that must fail; returns stderr.
    fn fail(&self, args: &[&str], stdin: &str) -> String {
        let out = self.run(args, stdin);
        assert!(
            !out.status.success(),
            "muninn {args:?} unexpectedly succeeded: {}",
            String::from_utf8_lossy(&out.stdout)
        );
        String::from_utf8(out.stderr).unwrap()
    }
}

fn csv_lines(s: &str) -> Vec<&str> {
    s.lines().collect()
}

// ---------------------------------------------------------------------------------------
// Happy path
// ---------------------------------------------------------------------------------------

#[test]
fn happy_path_wind_forecast() {
    let t = T::new();
    t.ok(
        &[
            "table",
            "create",
            "wind",
            "--column",
            "mw:f64",
            "--clock",
            "published",
        ],
        "",
    );
    t.ok(
        &[
            "series",
            "create",
            "dk1",
            "--table",
            "wind",
            "--tag",
            "area=DK1",
            "--resolution",
            "1h",
            "--timezone",
            "Europe/Copenhagen",
        ],
        "",
    );
    t.ok(
        &[
            "series", "create", "dk2", "--table", "wind", "--tag", "area=DK2",
        ],
        "",
    );
    // Run 06: published 06:00, received 06:05. Run 12: published 12:00, received 12:03.
    let run06 = "series,value_time,published_time,received_time,mw\n\
                 dk1,2025-01-16T00:00:00Z,2025-01-15T06:00:00Z,2025-01-15T06:05:00Z,100\n\
                 dk1,2025-01-16T01:00:00Z,2025-01-15T06:00:00Z,2025-01-15T06:05:00Z,110\n\
                 dk2,2025-01-16T00:00:00Z,2025-01-15T06:00:00Z,2025-01-15T06:05:00Z,50\n";
    let run12 = "series,value_time,published_time,received_time,mw\n\
                 dk1,2025-01-16T00:00:00Z,2025-01-15T12:00:00Z,2025-01-15T12:03:00Z,120\n\
                 dk1,2025-01-16T01:00:00Z,2025-01-15T12:00:00Z,2025-01-15T12:03:00Z,130\n";
    let out = t.ok(
        &["write", "--writer-clock", "2025-01-15T06:06:00Z", "-"],
        run06,
    );
    assert!(out.contains("stored 3"), "{out}");
    let f = t.write_file("run12.csv", run12);
    t.ok(&["write", &f, "--writer-clock", "2025-01-15T12:04:00Z"], "");
    // Re-sending is harmless (dedup).
    let out = t.ok(&["write", &f, "--writer-clock", "2025-01-15T13:00:00Z"], "");
    assert!(out.contains("stored 0, duplicates 2"), "{out}");

    // What did we know at 11:00 (published clock)? Run 06 only.
    let out = t.ok(
        &[
            "latest",
            "--table",
            "wind",
            "--series",
            "dk1",
            "--frontier",
            "2025-01-15T11:00:00Z",
            "--format",
            "csv",
        ],
        "",
    );
    let lines = csv_lines(&out);
    assert_eq!(
        lines[0],
        "series,value_time,published_time,received_time,system_time,seq,mw"
    );
    assert_eq!(
        lines[1],
        "dk1,2025-01-16T00:00:00Z,2025-01-15T06:00:00Z,2025-01-15T06:05:00Z,2025-01-15T06:06:00Z,1,100.0"
    );
    assert_eq!(lines.len(), 3);
    // Under the received clock at 12:02, still run 06; at 12:03 run 12.
    let at = |f: &str| {
        t.ok(
            &[
                "latest",
                "--table",
                "wind",
                "--series",
                "dk1",
                "--clock",
                "received",
                "--frontier",
                f,
                "--format",
                "csv",
            ],
            "",
        )
    };
    assert!(at("2025-01-15T12:02:00Z").contains(",100.0"));
    assert!(at("2025-01-15T12:03:00Z").contains(",120.0"));
    // The system bound hides run 12.
    let out = t.ok(
        &[
            "latest",
            "--table",
            "wind",
            "--system",
            "2025-01-15T12:00:00Z",
            "--format",
            "ndjson",
        ],
        "",
    );
    assert_eq!(out.lines().count(), 3);
    assert!(out.contains("\"values\": {\"mw\": 100.0}"));

    // History, replay, grid, asof-join.
    let out = t.ok(&["history", "--table", "wind", "--format", "csv"], "");
    assert_eq!(out.lines().count(), 6);
    let out = t.ok(
        &[
            "replay",
            "--table",
            "wind",
            "--since",
            "2025-01-15T06:00:00Z",
            "--until",
            "2025-01-15T12:00:00Z",
            "--format",
            "csv",
        ],
        "",
    );
    // (06:00, 12:00] on the published clock: run 12 (published 12:00) but not run 06.
    assert_eq!(out.lines().count(), 3, "{out}");
    assert!(
        out.lines()
            .skip(1)
            .all(|l| l.contains(",2025-01-15T12:00:00Z,")),
        "{out}"
    );
    let out = t.ok(
        &[
            "grid",
            "--table",
            "wind",
            "--series",
            "dk1",
            "--frontier-range",
            "2025-01-15T00:00:00Z,2025-01-15T12:00:00Z,6h",
            "--window",
            "0h,36h",
            "--format",
            "csv",
        ],
        "",
    );
    let lines = csv_lines(&out);
    assert!(lines[0].starts_with("frontier,series,"));
    assert_eq!(lines.len(), 1 + 2 + 2, "{out}"); // 06:00 and 12:00, two points each
    assert!(lines[3].starts_with("2025-01-15T12:00:00Z,dk1,2025-01-16T00:00:00Z"));
    assert!(lines[3].ends_with(",120.0"));
    let left = "series,value_time,frontier\n\
                dk1,2025-01-16T00:00:00Z,2025-01-15T05:00:00Z\n\
                dk1,2025-01-16T00:00:00Z,2025-01-15T12:00:00Z\n";
    let out = t.ok(
        &[
            "asof-join",
            "--table",
            "wind",
            "--format",
            "csv",
            "--left",
            "-",
        ],
        left,
    );
    let lines = csv_lines(&out);
    assert_eq!(
        lines[1],
        "dk1,2025-01-16T00:00:00Z,2025-01-15T05:00:00Z,false,,,,,"
    );
    assert!(
        lines[2].ends_with(
            ",true,2025-01-15T12:00:00Z,2025-01-15T12:03:00Z,2025-01-15T12:04:00Z,4,120.0"
        )
    );

    // Catalog listings.
    let out = t.ok(
        &["series", "list", "--tag", "area=DK2", "--format", "csv"],
        "",
    );
    assert_eq!(
        out,
        "id,name,table,tags,timezone,resolution\n2,dk2,wind,area=DK2,UTC,\n"
    );
    let out = t.ok(&["table", "list", "--format", "csv"], "");
    assert!(out.contains("wind,mw:f64,published,day,2"), "{out}");
    let out = t.ok(&["table", "show", "wind", "--format", "table"], "");
    assert!(
        out.contains("Europe/Copenhagen") && out.contains("mw"),
        "{out}"
    );
    t.ok(&["table", "add-column", "wind", "quality:string"], "");
    let out = t.ok(&["latest", "--table", "wind", "--format", "csv"], "");
    assert!(out.lines().next().unwrap().ends_with(",mw,quality"));
    assert!(out.lines().nth(1).unwrap().ends_with(",120.0,"));

    // Ops.
    let out = t.ok(&["info", "--format", "ndjson"], "");
    assert!(out.contains("\"next_seq\": 6"), "{out}");
    t.ok(&["verify"], "");
    // Writes went to the memtable (rows in the log, decision 0009): compact flushes
    // them into one segment file per partition first, leaving nothing to merge.
    let out = t.ok(&["compact"], "");
    assert!(out.contains("flushed 5 row(s)"), "{out}");
    assert!(out.contains("compacted 0 partition"), "{out}");
    let out = t.ok(&["gc", "--grace", "0s"], "");
    assert!(out.contains("deleted 0 file"), "{out}");
    t.ok(&["verify"], "");
    // A later write goes to the memtable again; the next flush adds a second file to
    // its partition, which compaction merges with the first.
    let f = t.write_file(
        "run13.csv",
        "series,value_time,published_time,received_time,mw\n\
         dk1,2025-01-16T00:00:00Z,2025-01-15T13:00:00Z,2025-01-15T13:01:00Z,130\n",
    );
    t.ok(&["write", &f, "--writer-clock", "2025-01-15T13:02:00Z"], "");
    let out = t.ok(&["compact"], "");
    assert!(out.contains("flushed 1 row(s)"), "{out}");
    assert!(out.contains("compacted 1 partition"), "{out}");
    let out = t.ok(&["gc", "--grace", "0s"], "");
    assert!(out.contains("deleted 2 file"), "{out}");
    t.ok(&["verify"], "");
    let out = t.ok(&["history", "--table", "wind", "--format", "csv"], "");
    assert_eq!(out.lines().count(), 7);

    // query: a SPEC §9 read step.
    let out = t.ok(
        &[
            "query",
            r#"{"op": "latest", "table": "wind", "series": ["dk2"], "expect": {"rows": []}}"#,
        ],
        "",
    );
    assert_eq!(
        out.trim(),
        r#"{"rows": [{"series": "dk2", "value_time": "2025-01-16T00:00:00Z", "published_time": "2025-01-15T06:00:00Z", "received_time": "2025-01-15T06:05:00Z", "system_time": "2025-01-15T06:06:00Z", "seq": 3, "values": {"mw": 50.0, "quality": null}}]}"#
    );
    let out = t.run(&["query", r#"{"op": "latest", "table": "nope"}"#], "");
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stdout).contains("\"error\": \"unknown_table\""));
}

#[test]
fn errors_are_clear() {
    let t = T::new();
    let err = t.fail(&["latest", "--table", "nope"], "");
    assert!(err.contains("unknown_table"), "{err}");
    let err = t.fail(&["table", "create", "Bad"], "");
    assert!(err.contains("invalid_name"), "{err}");
    let err = t.fail(&["table", "create", "x", "--column", "v:float"], "");
    assert!(err.contains("unknown column type"), "{err}");
    let out = Command::new(BIN)
        .args(["--db", t.path("missing").to_str().unwrap(), "info"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("not a muninn database"));
    let out = Command::new(BIN)
        .args(["init", t.db().to_str().unwrap()])
        .output()
        .unwrap();
    assert!(!out.status.success(), "init refuses a non-empty directory");
}

// ---------------------------------------------------------------------------------------
// CSV / NDJSON edge cases
// ---------------------------------------------------------------------------------------

fn edge_db() -> T {
    let t = T::new();
    t.ok(
        &[
            "table",
            "create",
            "m",
            "--column",
            "x:f64",
            "--column",
            "n:i64",
            "--column",
            "s:string",
            "--column",
            "b:bool",
            "--column",
            "at:timestamp",
        ],
        "",
    );
    t.ok(&["series", "create", "a", "--table", "m"], "");
    t.ok(&["table", "create", "other", "--column", "y:f64"], "");
    t.ok(&["series", "create", "o", "--table", "other"], "");
    t
}

#[test]
fn csv_values_round_trip() {
    let t = edge_db();
    let csv = "series,value_time,received_time,x,n,s,b,at,y\n\
        a,2025-01-01T00:00:00Z,2025-01-01T00:00:00Z,NaN,9223372036854775807,\"quoted, with comma\",true,2025-01-01T01:00:00.5Z,\n\
        a,2025-01-01T01:00:00Z,2025-01-01T00:00:00Z,inf,-9223372036854775808,\"multi\nline \"\"q\"\"\",false,,\n\
        a,2025-01-01T02:00:00Z,2025-01-01T00:00:00Z,-inf,9007199254740993,,1,,\n\
        a,2025-01-01T03:00:00Z,2025-01-01T00:00:00+01:00,-0,0,plain,0,2025-01-01T00:00:00-01:00,\n\
        a,2025-01-01T04:00:00Z,2025-01-01T00:00:00Z,,,,,,\n\
        o,2025-01-01T00:00:00Z,2025-01-01T00:00:00Z,,,,,,1.5e3\n";
    let out = t.ok(&["write", "--writer-clock", "2025-01-02T00:00:00Z"], csv);
    assert!(out.contains("stored 6"), "{out}");
    let out = t.ok(&["latest", "--table", "m", "--format", "ndjson"], "");
    let lines: Vec<&str> = out.lines().collect();
    assert!(lines[0].contains(r#""values": {"x": "NaN", "n": "9223372036854775807", "s": "quoted, with comma", "b": true, "at": "2025-01-01T01:00:00.500000Z"}"#), "{}", lines[0]);
    assert!(lines[1].contains(r#""x": "inf", "n": "-9223372036854775808", "s": "multi\nline \"q\"", "b": false, "at": null"#), "{}", lines[1]);
    assert!(
        lines[2].contains(r#""x": "-inf", "n": "9007199254740993", "s": null, "b": true"#),
        "{}",
        lines[2]
    );
    // -0 keeps its sign; offsets convert to UTC.
    assert!(
        lines[3].contains(r#""received_time": "2024-12-31T23:00:00Z""#),
        "{}",
        lines[3]
    );
    assert!(
        lines[3].contains(
            r#""x": -0.0, "n": 0, "s": "plain", "b": false, "at": "2025-01-01T01:00:00Z""#
        ),
        "{}",
        lines[3]
    );
    // Empty = null everywhere.
    assert!(lines[4].contains(r#""published_time": null"#));
    assert!(
        lines[4].contains(r#""values": {"x": null, "n": null, "s": null, "b": null, "at": null}"#)
    );
    let out = t.ok(&["latest", "--table", "other", "--format", "csv"], "");
    assert!(out.lines().nth(1).unwrap().ends_with(",1500.0"));
    // CSV output quotes what needs quoting and round-trips through `write`.
    let out = t.ok(&["latest", "--table", "m", "--format", "csv"], "");
    assert!(out.contains("\"quoted, with comma\""));
    assert!(out.contains("\"multi\nline \"\"q\"\"\""));
}

#[test]
fn csv_errors_point_at_lines() {
    let t = edge_db();
    let w = |csv: &str| t.fail(&["write"], csv);
    let head = "series,value_time,received_time,x,n\n";
    let ok_row = "a,2025-01-01T00:00:00Z,2025-01-01T00:00:00Z,1,1\n";
    // A quoted field spanning two lines shifts the line numbers.
    let multi = "a,2025-01-01T00:00:00Z,2025-01-01T00:00:00Z,1,1\n";
    let err = w(&format!(
        "series,value_time,received_time,x,s\n{}a,2025-01-01T01:00:00Z,2025-01-01T00:00:00Z,1,\"two\nlines\"\na,2025-01-01T25:00:00Z,2025-01-01T00:00:00Z,1,\n",
        multi.replace(",1,1\n", ",1,x\n")
    ));
    assert!(
        err.contains("line 5, column \"value_time\": invalid_timestamp"),
        "{err}"
    );
    let err = w(&format!(
        "{head}{ok_row}a,2025-01-01,2025-01-01T00:00:00Z,1,1\n"
    ));
    assert!(
        err.contains("line 3, column \"value_time\": invalid_timestamp"),
        "{err}"
    );
    let err = w(&format!(
        "{head}{ok_row}{ok_row}a,2025-01-01T00:00:00Z,,1,1\n"
    ));
    assert!(
        err.contains("line 4, column \"received_time\"") && err.contains("--received-time"),
        "{err}"
    );
    let err = w(&format!(
        "{head}a,2025-01-01T00:00:00Z,2025-01-01T00:00:00Z,abc,1\n"
    ));
    assert!(err.contains("line 2, column \"x\": type_mismatch"), "{err}");
    let err = w(&format!(
        "{head}a,2025-01-01T00:00:00Z,2025-01-01T00:00:00Z,1,1.5\n"
    ));
    assert!(err.contains("line 2, column \"n\": type_mismatch"), "{err}");
    let err = w(&format!(
        "{head}a,2025-01-01T00:00:00Z,2025-01-01T00:00:00Z,1,9223372036854775808\n"
    ));
    assert!(err.contains("line 2, column \"n\": type_mismatch"), "{err}");
    let err = w(&format!(
        "{head}a,2025-01-01T00:00:00Z,2025-01-01T00:00:00Z,1\n"
    ));
    assert!(
        err.contains("line 2: the row has 4 fields but the header has 5"),
        "{err}"
    );
    let err = w(&format!(
        "{head}zz,2025-01-01T00:00:00Z,2025-01-01T00:00:00Z,1,1\n"
    ));
    assert!(
        err.contains("line 2, column \"series\": unknown_series"),
        "{err}"
    );
    let err =
        w("series,value_time,received_time,y\na,2025-01-01T00:00:00Z,2025-01-01T00:00:00Z,1\n");
    assert!(
        err.contains("line 2, column \"y\": unknown_column"),
        "{err}"
    );
    let err = w("series,received_time,x\na,2025-01-01T00:00:00Z,1\n");
    assert!(
        err.contains("line 1") && err.contains("value_time"),
        "{err}"
    );
    let err = w("series,value_time,received_time,seq\n");
    assert!(err.contains("muninn restore"), "{err}");
    let err = w(
        "series,value_time,received_time,deleted,x\na,2025-01-01T00:00:00Z,2025-01-01T00:00:00Z,true,1\n",
    );
    assert!(err.contains("line 2: tombstone_with_values"), "{err}");
    let err =
        w("series,value_time,received_time,x\na,0000-12-31T23:00:00Z,2025-01-01T00:00:00Z,1\n");
    assert!(
        err.contains("line 2, column \"value_time\": invalid_timestamp"),
        "{err}"
    );
    // Nothing was written by any failed import.
    let out = t.ok(&["history", "--table", "m", "--format", "csv"], "");
    assert_eq!(out.lines().count(), 1);
}

#[test]
fn defaults_time_formats_and_ndjson() {
    let t = edge_db();
    // No published_time / received_time columns: --received-time fills in.
    let out = t.ok(
        &[
            "write",
            "--received-time",
            "2025-02-01T00:00:00Z",
            "--writer-clock",
            "2025-02-02T00:00:00Z",
        ],
        "series,value_time,x\na,2025-01-01T00:00:00Z,1\n",
    );
    assert!(out.contains("stored 1"));
    // --received-time now = the writer clock.
    t.ok(
        &[
            "write",
            "--received-time",
            "now",
            "--writer-clock",
            "2025-02-03T00:00:00Z",
        ],
        "series,value_time,x\na,2025-01-01T01:00:00Z,2\n",
    );
    // Unix milliseconds for every timestamp, including timestamp columns.
    t.ok(
        &[
            "write",
            "--time-format",
            "unix-ms",
            "--writer-clock",
            "2025-02-04T00:00:00Z",
        ],
        "series,value_time,received_time,at\na,1735696800000,1738368000000.5,1735693200000\n",
    );
    // NDJSON: nested and flat values, big ints as strings and numbers, -0, NaN.
    let nd = r#"{"series": "a", "value_time": "2025-01-01T03:00:00Z", "received_time": "2025-02-01T00:00:00Z", "values": {"n": "-9223372036854775808", "x": -0}}

{"series": "a", "value_time": "2025-01-01T04:00:00Z", "received_time": "2025-02-01T00:00:00Z", "n": 9007199254740993, "x": "NaN", "s": ""}
{"series": "a", "value_time": "2025-01-01T05:00:00Z", "received_time": "2025-02-01T00:00:00Z", "deleted": true}
"#;
    let f = t.write_file("rows.ndjson", nd);
    let out = t.ok(&["write", &f, "--writer-clock", "2025-02-05T00:00:00Z"], "");
    assert!(out.contains("stored 3"), "{out}");
    let out = t.ok(&["history", "--table", "m", "--format", "ndjson"], "");
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines.len(), 6, "{out}");
    assert!(lines[0].contains(r#""received_time": "2025-02-01T00:00:00Z""#));
    assert!(lines[1].contains(r#""received_time": "2025-02-03T00:00:00Z""#));
    assert!(lines[2].contains(r#""value_time": "2025-01-01T02:00:00Z", "published_time": null, "received_time": "2025-02-01T00:00:00.000500Z""#), "{}", lines[2]);
    assert!(lines[2].contains(r#""at": "2025-01-01T01:00:00Z""#));
    assert!(
        lines[3].contains(r#""x": -0.0, "n": "-9223372036854775808""#),
        "{}",
        lines[3]
    );
    assert!(
        lines[4].contains(r#""x": "NaN", "n": "9007199254740993", "s": """#),
        "{}",
        lines[4]
    );
    assert!(lines[5].contains(r#""deleted": true"#));
    // NDJSON errors carry line numbers too (blank lines count).
    let err = t.fail(
        &["write", "--input", "ndjson"],
        "{\"series\": \"a\", \"value_time\": \"2025-01-01T00:00:00Z\", \"received_time\": \"2025-01-01T00:00:00Z\"}\n\n{\"series\": \"a\", \"value_time\": 5, \"received_time\": \"2025-01-01T00:00:00Z\"}\n",
    );
    assert!(
        err.contains("line 3, column \"value_time\": invalid_timestamp"),
        "{err}"
    );
    let err = t.fail(
        &["write", "--input", "ndjson"],
        "{\"series\": \"a\", \"value_time\": \"2025-01-01T00:00:00Z\", \"received_time\": \"2025-01-01T00:00:00Z\", \"x\": 1.5e999999, \"n\": 2.0}\n",
    );
    assert!(err.contains("line 1, column \"n\": type_mismatch"), "{err}");
    let err = t.fail(&["write", "--input", "ndjson"], "{\"series\": \"a\",\n");
    assert!(err.contains("line 1: invalid JSON"), "{err}");
}

// ---------------------------------------------------------------------------------------
// Chunked import and resume
// ---------------------------------------------------------------------------------------

#[test]
fn chunked_import_resumes_by_batch_id() {
    let t = T::new();
    t.ok(&["table", "create", "p", "--column", "v:f64"], "");
    t.ok(&["series", "create", "s", "--table", "p"], "");
    let rows = |bad_at: Option<usize>| {
        let mut csv = String::from("series,value_time,v\n");
        for i in 0..10 {
            let v = if Some(i) == bad_at {
                "oops".to_owned()
            } else {
                i.to_string()
            };
            csv += &format!("s,2025-01-{:02}T00:00:00Z,{v}\n", i + 1);
        }
        csv
    };
    let args = [
        "write",
        "--chunk-rows",
        "3",
        "--batch-id",
        "imp",
        "--received-time",
        "now",
    ];
    // Line 9 (row 7) is bad: chunks imp/0 and imp/1 (rows 0..6) are committed.
    let err = t.fail(&args, &rows(Some(7)));
    assert!(err.contains("line 9, column \"v\": type_mismatch"), "{err}");
    assert!(err.contains("2 earlier chunk(s) were committed"), "{err}");
    let count = || {
        t.ok(&["history", "--table", "p", "--format", "csv"], "")
            .lines()
            .count()
            - 1
    };
    assert_eq!(count(), 6);
    // Resume: received_time "now" differs, but the committed chunks are skipped by id.
    let out = t.ok(&args, &rows(None));
    assert!(
        out.contains("stored 4, duplicates 0, batches 4 (2 already committed, skipped)"),
        "{out}"
    );
    assert_eq!(count(), 10);
    let out = t.ok(&args, &rows(None));
    assert!(
        out.contains("stored 0") && out.contains("4 already committed"),
        "{out}"
    );
    assert_eq!(count(), 10);
    // A changed chunk (row 1 corrected) gets a new id and is written; the others are
    // skipped because their rows are unchanged.
    let fixed = rows(None).replace("2025-01-02T00:00:00Z,1\n", "2025-01-02T00:00:00Z,1.5\n");
    let out = t.ok(&args, &fixed);
    assert!(
        out.contains("stored 3, duplicates 0, batches 4 (3 already committed"),
        "{out}"
    );
    assert_eq!(count(), 13);
    // Seqs are contiguous: no seq was consumed by the failed or skipped chunks.
    let out = t.ok(&["history", "--table", "p", "--format", "csv"], "");
    let seqs: Vec<u64> = out
        .lines()
        .skip(1)
        .map(|l| l.split(',').nth(5).unwrap().parse().unwrap())
        .collect();
    let mut sorted = seqs.clone();
    sorted.sort();
    assert_eq!(sorted, (1..=13).collect::<Vec<_>>());
    // Without chunking, one bad line rejects everything.
    t.ok(&["series", "create", "s2", "--table", "p"], "");
    let err = t.fail(
        &["write", "--received-time", "now"],
        "series,value_time,v\ns2,2025-01-01T00:00:00Z,1\ns2,2025-01-02T00:00:00Z,x\n",
    );
    assert!(!err.contains("earlier chunk"), "{err}");
    assert_eq!(count(), 13);
}

// ---------------------------------------------------------------------------------------
// Dump / restore on a randomised database
// ---------------------------------------------------------------------------------------

/// SplitMix64.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn chance(&mut self, pct: u64) -> bool {
        self.below(100) < pct
    }
    fn pick<'a>(&mut self, xs: &[&'a str]) -> &'a str {
        xs[self.below(xs.len() as u64) as usize]
    }
}

/// `2025-01-01T00:00:00Z` + `h` hours (+ `m` minutes).
fn hour(h: i64, m: i64) -> String {
    let base = 1_735_689_600i64 + h * 3600 + m * 60;
    let days = base.div_euclid(86_400);
    let secs = base.rem_euclid(86_400);
    // civil from days (Howard Hinnant)
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if mo <= 2 { y + 1 } else { y };
    format!(
        "{y:04}-{mo:02}-{d:02}T{:02}:{:02}:{:02}Z",
        secs / 3600,
        secs % 3600 / 60,
        secs % 60
    )
}

fn random_value(r: &mut Rng, ty: &str) -> String {
    if r.chance(15) {
        return "null".into();
    }
    match ty {
        "f64" => match r.below(8) {
            0 => "\"NaN\"".into(),
            1 => "\"inf\"".into(),
            2 => "\"-inf\"".into(),
            3 => "-0".into(),
            4 => format!("{}", r.below(1000)),
            _ => format!("{}.{}", r.below(1000) as i64 - 500, r.below(1000)),
        },
        "i64" => match r.below(4) {
            0 => "\"9223372036854775807\"".into(),
            1 => "\"-9223372036854775808\"".into(),
            _ => format!("{}", r.below(1 << 60) as i64 - (1 << 59)),
        },
        "bool" => if r.chance(50) { "true" } else { "false" }.into(),
        "string" => format!("\"s{},\\\"{}\\n\"", r.below(5), r.below(3)),
        _ => format!("\"{}\"", hour(r.below(500) as i64, 0)),
    }
}

/// (table, partition flag, columns as (name, type)).
type TableSpec = (
    &'static str,
    &'static str,
    &'static [(&'static str, &'static str)],
);

const TABLES: [TableSpec; 3] = [
    ("fc", "--partition=day", &[("v", "f64"), ("n", "i64")]),
    (
        "px",
        "--partition=month",
        &[("p", "f64"), ("ok", "bool"), ("tag", "string")],
    ),
    ("ev", "--partition=year", &[("at", "timestamp")]),
];
const SERIES: [(&str, &str); 6] = [
    ("fc_a", "fc"),
    ("fc b", "fc"),
    ("px_1", "px"),
    ("px_2", "px"),
    ("ev,1", "ev"),
    ("ev_2", "ev"),
];

fn build_random_db(t: &T, r: &mut Rng) {
    for (name, part, cols) in TABLES {
        let mut args = vec!["table", "create", name, part];
        let specs: Vec<String> = cols.iter().map(|(c, ty)| format!("{c}:{ty}")).collect();
        for s in &specs {
            args.push("--column");
            args.push(s);
        }
        let clock = r.pick(&["received", "published", "system"]);
        args.extend(["--clock", clock, "--writer-clock", "2024-12-01T00:00:00Z"]);
        t.ok(&args, "");
    }
    for (i, (s, table)) in SERIES.iter().enumerate() {
        let tag = format!("k=v{}", i % 2);
        t.ok(
            &["series", "create", s, "--table", table, "--tag", &tag],
            "",
        );
    }
    let mut clock = 0i64;
    let mut sent: Vec<String> = Vec::new();
    for w in 0..30 {
        if w == 15 {
            t.ok(
                &[
                    "table",
                    "add-column",
                    "fc",
                    "late:string",
                    "--writer-clock",
                    &hour(clock, 1),
                ],
                "",
            );
        }
        if w % 10 == 9 {
            t.ok(&["compact"], "");
        }
        // The writer clock usually moves forward, sometimes back (system_time then
        // stays at the high-water mark).
        clock += r.below(5) as i64 - 1;
        let mut nd = String::new();
        if !sent.is_empty() && r.chance(20) {
            nd = sent[r.below(sent.len() as u64) as usize].clone(); // resend (dedup)
        } else {
            for _ in 0..1 + r.below(12) {
                let (s, table) = SERIES[r.below(SERIES.len() as u64) as usize];
                let cols = TABLES.iter().find(|x| x.0 == table).unwrap().2;
                let vt = hour(r.below(24 * 70) as i64 - 24 * 3, 0);
                let rt = hour(clock - r.below(6) as i64, r.below(60) as i64);
                let pt = if r.chance(30) {
                    "null".to_owned()
                } else {
                    format!("\"{}\"", hour(clock - r.below(8) as i64, 0))
                };
                let deleted = r.chance(10);
                let mut vals: Vec<String> = Vec::new();
                if !deleted {
                    for (c, ty) in cols {
                        vals.push(format!("\"{c}\": {}", random_value(r, ty)));
                    }
                    if table == "fc" && w >= 15 && r.chance(50) {
                        vals.push(format!("\"late\": \"w{w}\""));
                    }
                }
                nd += &format!(
                    "{{\"series\": \"{}\", \"value_time\": \"{vt}\", \"published_time\": {pt}, \"received_time\": \"{rt}\", \"deleted\": {deleted}, \"values\": {{{}}}}}\n",
                    s.replace('"', "\\\""),
                    vals.join(", ")
                );
            }
            sent.push(nd.clone());
        }
        let wc = hour(clock, 30);
        let id = format!("w{}", r.below(12));
        let mut args = vec!["write", "--input", "ndjson", "--writer-clock", &wc];
        if r.chance(30) {
            args.extend(["--batch-id", &id]);
        }
        t.ok(&args, &nd);
    }
}

fn random_steps(r: &mut Rng, n: usize) -> String {
    let mut out = String::new();
    for _ in 0..n {
        let (table, _, _) = TABLES[r.below(3) as usize];
        let names: Vec<&str> = SERIES
            .iter()
            .filter(|s| s.1 == table)
            .map(|s| s.0)
            .collect();
        let mut f: Vec<String> = vec![format!("\"table\": \"{table}\"")];
        if r.chance(50) {
            let pick: Vec<String> = names
                .iter()
                .filter(|_| r.chance(60))
                .map(|s| format!("\"{s}\""))
                .collect();
            f.push(format!("\"series\": [{}]", pick.join(", ")));
        }
        if r.chance(60) {
            f.push(format!(
                "\"clock\": \"{}\"",
                r.pick(&["received", "published", "system"])
            ));
        }
        let ts = |r: &mut Rng| format!("\"{}\"", hour(r.below(40) as i64 - 4, r.below(60) as i64));
        if r.chance(60) {
            f.push(format!("\"system\": {}", ts(r)));
        }
        let vrange = |r: &mut Rng, f: &mut Vec<String>| {
            if r.chance(40) {
                f.push(format!(
                    "\"value_from\": \"{}\"",
                    hour(r.below(24 * 40) as i64, 0)
                ));
            }
            if r.chance(40) {
                f.push(format!(
                    "\"value_to\": \"{}\"",
                    hour(r.below(24 * 70) as i64, 0)
                ));
            }
        };
        let op = r.pick(&["latest", "history", "replay", "grid", "asof_join"]);
        match op {
            "latest" | "history" => {
                vrange(r, &mut f);
                if r.chance(70) {
                    f.push(format!("\"frontier\": {}", ts(r)));
                }
            }
            "replay" => {
                vrange(r, &mut f);
                if r.chance(60) {
                    f.push(format!("\"from\": {}", ts(r)));
                }
                if r.chance(60) {
                    f.push(format!("\"to\": {}", ts(r)));
                }
            }
            "grid" => {
                let fs: Vec<String> = (0..r.below(5)).map(|_| ts(r)).collect();
                f.push(format!("\"frontiers\": [{}]", fs.join(", ")));
                if r.chance(50) {
                    f.push(format!(
                        "\"window_start\": \"{}h\"",
                        r.below(48) as i64 - 24
                    ));
                    f.push(format!("\"window_end\": \"{}h\"", 24 + r.below(24 * 40)));
                } else {
                    vrange(r, &mut f);
                }
            }
            _ => {
                f.retain(|x| !x.starts_with("\"series\""));
                let left: Vec<String> = (0..r.below(6))
                    .map(|_| {
                        format!(
                            "{{\"series\": \"{}\", \"value_time\": \"{}\", \"frontier\": {}}}",
                            names[r.below(names.len() as u64) as usize],
                            hour(r.below(24 * 70) as i64 - 24 * 3, 0),
                            ts(r)
                        )
                    })
                    .collect();
                f.push(format!("\"left\": [{}]", left.join(", ")));
            }
        }
        out += &format!("{{\"op\": \"{op}\", {}}}\n", f.join(", "));
    }
    out
}

fn dump_files(dir: &Path) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for rel in ["catalog.json", "batch_ids.ndjson"] {
        out.push((rel.to_owned(), fs::read_to_string(dir.join(rel)).unwrap()));
    }
    let mut tables: Vec<_> = fs::read_dir(dir.join("tables"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    tables.sort();
    for p in tables {
        out.push((
            p.file_name().unwrap().to_string_lossy().into_owned(),
            fs::read_to_string(&p).unwrap(),
        ));
    }
    out
}

#[test]
fn dump_restore_preserves_every_read() {
    for seed in [1u64, 2, 3] {
        let t = T::new();
        let mut r = Rng(seed);
        build_random_db(&t, &mut r);
        let dump = t.path("dump");
        t.ok(&["dump", dump.to_str().unwrap()], "");
        let restored = t.path("restored");
        // Small chunks: many restore commits, batch ids spread over them.
        t.ok(
            &[
                "restore",
                dump.to_str().unwrap(),
                restored.to_str().unwrap(),
                "--chunk-rows",
                "7",
            ],
            "",
        );
        t.ok_on(&restored, &["verify"], "");
        // Every read, in many random contexts, is identical (system_time and seq too).
        let steps = random_steps(&mut r, 400);
        // (Some random steps are invalid, e.g. from > to: both sides must agree on that.)
        let stdout = |o: Output| String::from_utf8(o.stdout).unwrap();
        let a = stdout(t.run(&["query", "-"], &steps));
        let b = stdout(t.run_on(&restored, &["query", "-"], &steps));
        assert_eq!(a.lines().count(), 400);
        let with_rows = a.lines().filter(|l| l.contains("\"seq\": 1")).count();
        let errors = a.lines().filter(|l| l.starts_with("{\"error\"")).count();
        eprintln!("seed {seed}: {with_rows} steps with rows, {errors} errors");
        assert!(with_rows > 100 && errors < 100, "{with_rows} {errors}");
        for (i, (x, y)) in a.lines().zip(b.lines()).enumerate() {
            assert_eq!(
                x,
                y,
                "seed {seed}, step {i}: {}",
                steps.lines().nth(i).unwrap()
            );
        }
        // Full history of every table, and a re-dump, are identical.
        for (table, _, _) in TABLES {
            let args = ["history", "--table", table, "--format", "ndjson"];
            assert_eq!(t.ok(&args, ""), t.ok_on(&restored, &args, ""));
        }
        let dump2 = t.path("dump2");
        t.ok_on(&restored, &["dump", dump2.to_str().unwrap()], "");
        assert_eq!(dump_files(&dump), dump_files(&dump2));
        // Later writes continue identically: same system_time and seq, same batch ids.
        let more = "{\"series\": \"fc_a\", \"value_time\": \"2025-03-01T00:00:00Z\", \"received_time\": \"2025-01-01T00:00:00Z\", \"values\": {\"v\": 1}}\n";
        for id in ["w0", "w5", "fresh"] {
            let args = [
                "write",
                "--input",
                "ndjson",
                "--batch-id",
                id,
                "--writer-clock",
                "2024-01-01T00:00:00Z",
            ];
            assert_eq!(t.ok(&args, more), t.ok_on(&restored, &args, more));
        }
        let args = ["history", "--table", "fc", "--format", "csv"];
        assert_eq!(t.ok(&args, ""), t.ok_on(&restored, &args, ""));
    }
}

#[test]
fn restore_rejects_damaged_dumps() {
    let t = T::new();
    t.ok(&["table", "create", "p", "--column", "v:f64"], "");
    t.ok(&["series", "create", "s", "--table", "p"], "");
    t.ok(
        &["write", "--received-time", "now"],
        "series,value_time,v\ns,2025-01-01T00:00:00Z,1\n",
    );
    let dump = t.path("dump");
    t.ok(&["dump", dump.to_str().unwrap()], "");
    let f = dump.join("tables/p.ndjson");
    let text = fs::read_to_string(&f).unwrap();
    fs::write(&f, text.replace("1.0", "2.0")).unwrap();
    let out = Command::new(BIN)
        .args([
            "restore",
            dump.to_str().unwrap(),
            t.path("r").to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("checksum") && err.contains("damaged"), "{err}");
    assert!(
        !t.path("r").exists(),
        "nothing is created for a damaged dump"
    );
}

// ---------------------------------------------------------------------------------------
// verify
// ---------------------------------------------------------------------------------------

fn segment_files(db: &Path) -> Vec<PathBuf> {
    fn walk(p: &Path, out: &mut Vec<PathBuf>) {
        for e in fs::read_dir(p).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                walk(&p, out);
            } else if p.extension().is_some_and(|x| x == "seg") {
                out.push(p);
            }
        }
    }
    let mut out = Vec::new();
    walk(&db.join("segments"), &mut out);
    out.sort();
    out
}

fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for e in fs::read_dir(from).unwrap() {
        let p = e.unwrap().path();
        let dest = to.join(p.file_name().unwrap());
        if p.is_dir() {
            copy_dir(&p, &dest);
        } else {
            fs::copy(&p, &dest).unwrap();
        }
    }
}

#[test]
fn verify_detects_a_flipped_byte() {
    let t = T::new();
    t.ok(
        &[
            "table", "create", "p", "--column", "v:f64", "--column", "s:string",
        ],
        "",
    );
    t.ok(&["series", "create", "a", "--table", "p"], "");
    let mut csv = String::from("series,value_time,v,s\n");
    for i in 0..200 {
        csv += &format!(
            "a,2025-01-01T{:02}:{:02}:00Z,{i}.25,text{i}\n",
            i / 60,
            i % 60
        );
    }
    t.ok(&["write", "--received-time", "now"], &csv);
    let out = t.ok(&["verify"], "");
    assert!(out.starts_with("ok:"), "{out}");
    // The rows are in the memtable: inline in log entry 3 (decision 0009). A changed
    // character of its data is caught.
    let entry = t.db().join("log/00000000000000000003.json");
    let text = fs::read_to_string(&entry).unwrap();
    let data_at = text.find("\"data\":\"").unwrap() + 8;
    for at in [
        data_at + 40,
        data_at + text[data_at..].find('"').unwrap() / 2,
    ] {
        let copy = t.path(&format!("copyinline{at}"));
        copy_dir(&t.db(), &copy);
        let mut bytes = text.clone().into_bytes();
        bytes[at] = if bytes[at] == b'A' { b'B' } else { b'A' };
        fs::write(copy.join("log/00000000000000000003.json"), bytes).unwrap();
        let out = t.run_on(&copy, &["verify"], "");
        assert!(!out.status.success(), "change at {at} not detected");
    }
    // Flushed to a segment file.
    let out = t.ok(&["compact"], "");
    assert!(out.contains("flushed 200 row(s)"), "{out}");
    let seg = segment_files(&t.db()).pop().unwrap();
    let len = fs::metadata(&seg).unwrap().len() as usize;
    for (i, at) in [0, 9, len / 4, len / 2, len * 3 / 4, len - 30, len - 1]
        .into_iter()
        .enumerate()
    {
        let copy = t.path(&format!("copy{i}"));
        copy_dir(&t.db(), &copy);
        let seg_copy = copy.join(seg.strip_prefix(t.db()).unwrap());
        let mut bytes = fs::read(&seg_copy).unwrap();
        bytes[at] ^= 0x10;
        fs::write(&seg_copy, bytes).unwrap();
        let out = t.run_on(&copy, &["verify"], "");
        assert!(!out.status.success(), "flip at {at} of {len} not detected");
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(stdout.contains("problem: segment"), "{stdout}");
    }
    // A damaged log entry is caught too.
    let copy = t.path("copylog");
    copy_dir(&t.db(), &copy);
    let entry = copy.join("log/00000000000000000003.json");
    let text = fs::read_to_string(&entry).unwrap();
    fs::write(
        &entry,
        text.replacen("\"next_seq\":201", "\"next_seq\":150", 1),
    )
    .unwrap();
    let out = t.run_on(&copy, &["verify"], "");
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stdout).contains("problem: log entry 3"));
}

// ---------------------------------------------------------------------------------------
// series import, verify on a truncated log
// ---------------------------------------------------------------------------------------

#[test]
fn series_import_creates_all_or_nothing() {
    let t = T::new();
    t.ok(&["table", "create", "wind", "--column", "mw:f64"], "");
    t.ok(&["table", "create", "solar", "--column", "mw:f64"], "");
    let csv = "name,table,resolution,area,kind\n\
               dk1,wind,1h,DK1,forecast\n\
               dk2,,15m,DK2,\n\
               \"se,1\",solar,,SE1,actual\n";
    let out = t.ok(&["series", "import", "--table", "wind", "-"], csv);
    assert!(out.contains("created 3 series (ids 1..=3)"), "{out}");
    let list = t.ok(&["series", "list", "--format", "csv"], "");
    let lines = csv_lines(&list);
    assert_eq!(lines.len(), 4, "{list}");
    assert!(lines[1].starts_with("1,dk1,wind,"), "{list}");
    assert!(lines[1].contains("area=DK1,kind=forecast"), "{list}");
    assert!(lines[2].starts_with("2,dk2,wind,area=DK2,"), "{list}");
    assert!(lines[3].starts_with("3,\"se,1\",solar,"), "{list}");
    // NDJSON; the third line repeats a name, so nothing is created.
    let nd = r#"{"name": "a", "table": "wind", "tags": {"x": "1"}}
{"name": "b", "table": "solar", "timezone": "Europe/Oslo", "resolution": "1h"}
{"name": "a", "table": "wind"}
"#;
    let path = t.write_file("s.ndjson", nd);
    let err = t.fail(&["series", "import", &path], "");
    assert!(
        err.contains("line 3") && err.contains("already_exists"),
        "{err}"
    );
    let n = csv_lines(&t.ok(&["series", "list", "--format", "csv"], "")).len();
    assert_eq!(n, 4);
    let nd = nd.lines().take(2).collect::<Vec<_>>().join("\n");
    let path = t.write_file("ok.ndjson", &nd);
    let out = t.ok(&["series", "import", &path], "");
    assert!(out.contains("created 2 series (ids 4..=5)"), "{out}");
    let err = t.fail(&["series", "import", "-"], "name,table\nq,nope\n");
    assert!(
        err.contains("line 2") && err.contains("unknown_table"),
        "{err}"
    );
    t.ok(&["verify"], "");
}

#[test]
fn verify_accepts_a_truncated_log() {
    let t = T::new();
    t.ok(&["table", "create", "wind", "--column", "mw:f64"], "");
    t.ok(&["series", "create", "dk1", "--table", "wind"], "");
    for i in 0..4 {
        let csv = format!(
            "series,value_time,received_time,mw\n\
             dk1,2025-01-01T0{i}:00:00Z,2025-01-01T00:00:0{i}Z,{i}\n"
        );
        t.ok(&["write", "-"], &csv);
        t.ok(&["gc", "--grace", "0s"], "");
    }
    let first = fs::read_to_string(t.db().join("log/00000000000000000001.json")).unwrap();
    assert!(first.contains("\"truncated\""), "{first}");
    assert!(!t.db().join("log/00000000000000000002.json").exists());
    let out = t.ok(&["verify"], "");
    assert!(out.contains("note: the log is truncated"), "{out}");
    assert!(out.contains("ok: "), "{out}");
    let latest = t.ok(&["latest", "--table", "wind", "--format", "csv"], "");
    assert_eq!(csv_lines(&latest).len(), 5, "{latest}");
    let info = t.ok(&["info", "--format", "ndjson"], "");
    assert!(info.contains("\"log_bytes\""), "{info}");
}
