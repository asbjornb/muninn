//! Adversarial review (round 2) of the CLI: each test reproduces one finding (all fixed).

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};

use tempfile::TempDir;

const BIN: &str = env!("CARGO_BIN_EXE_muninn");

struct T {
    dir: TempDir,
}

impl T {
    fn new() -> T {
        let t = T {
            dir: tempfile::tempdir().unwrap(),
        };
        let db = t.db();
        t.ok(&["init", db.to_str().unwrap()], "");
        t.ok(&["table", "create", "t", "--column", "x:f64"], "");
        t
    }

    fn db(&self) -> PathBuf {
        self.dir.path().join("db")
    }

    fn run(&self, args: &[&str], stdin: &[u8]) -> Output {
        let mut child = Command::new(BIN)
            .arg("--db")
            .arg(self.db())
            .args(args)
            .env_remove("MUNINN_DB")
            .env_remove("MUNINN_WRITER_CLOCK")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut pipe = child.stdin.take().unwrap();
        let input = stdin.to_vec();
        let feeder = std::thread::spawn(move || {
            let _ = pipe.write_all(&input);
        });
        let out = child.wait_with_output().unwrap();
        feeder.join().unwrap();
        out
    }

    fn ok(&self, args: &[&str], stdin: &str) -> String {
        self.ok_bytes(args, stdin.as_bytes())
    }

    fn ok_bytes(&self, args: &[&str], stdin: &[u8]) -> String {
        let out = self.run(args, stdin);
        assert!(
            out.status.success(),
            "muninn {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap()
    }
}

/// Review finding 2: a chunked, batch-id'd import of a header-only input (an upstream
/// export that was not ready yet, an empty stdin) commits an empty batch under `X/0`.
/// Re-running "the same command" once the data is there then skips chunk 0 as
/// `batch_already_committed`: its rows are silently never written.
#[test]
fn chunked_resume_after_empty_input_does_not_skip_rows() {
    let t = T::new();
    t.ok(&["series", "create", "a", "--table", "t"], "");
    let args = ["write", "-", "--chunk-rows", "2", "--batch-id", "imp"];
    t.ok(&args, "series,value_time,received_time,x\n");
    let data = "series,value_time,received_time,x\n\
                a,2025-01-01T00:00:00Z,2025-01-01T00:00:00Z,1\n\
                a,2025-01-02T00:00:00Z,2025-01-01T00:00:00Z,2\n\
                a,2025-01-03T00:00:00Z,2025-01-01T00:00:00Z,3\n";
    t.ok(&args, data);
    let out = t.ok(&["latest", "--table", "t", "--format", "csv"], "");
    assert_eq!(out.lines().count(), 4, "three rows expected:\n{out}");
}

/// Review finding 6: `--series` splits on commas, but SPEC §10.6 allows any non-empty
/// series name. `--series 'a,b'` silently reads series `a` and `b` instead of `a,b`.
#[test]
fn series_filter_with_comma_in_name() {
    let t = T::new();
    for s in ["a", "b", "a,b"] {
        t.ok(&["series", "create", s, "--table", "t"], "");
    }
    t.ok(
        &["write", "-"],
        "series,value_time,received_time,x\n\
         a,2025-01-01T00:00:00Z,2025-01-01T00:00:00Z,1\n\
         b,2025-01-01T00:00:00Z,2025-01-01T00:00:00Z,2\n\
         \"a,b\",2025-01-01T00:00:00Z,2025-01-01T00:00:00Z,3\n",
    );
    let out = t.ok(
        &[
            "latest", "--table", "t", "--series", "a,b", "--format", "ndjson",
        ],
        "",
    );
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines.len(), 1, "{out}");
    assert!(lines[0].contains("\"series\": \"a,b\""), "{out}");
}

/// Review finding 6b: `--series ''` (e.g. an unset shell variable) is dropped and the read
/// silently returns every series of the table instead of failing.
#[test]
fn empty_series_filter_is_not_all_series() {
    let t = T::new();
    t.ok(&["series", "create", "a", "--table", "t"], "");
    t.ok(
        &["write", "-"],
        "series,value_time,received_time,x\na,2025-01-01T00:00:00Z,2025-01-01T00:00:00Z,1\n",
    );
    let out = t.run(
        &["latest", "--table", "t", "--series", "", "--format", "csv"],
        b"",
    );
    assert!(
        !out.status.success(),
        "an empty series name returned rows: {}",
        String::from_utf8_lossy(&out.stdout)
    );
}

/// Review finding 7: CSV input tolerates a UTF-8 BOM (the `csv` crate strips it), NDJSON
/// input does not: a BOM-prefixed file (Windows / Excel / PowerShell exports) fails on
/// line 1 with "invalid JSON".
#[test]
fn ndjson_with_utf8_bom() {
    let t = T::new();
    t.ok(&["series", "create", "a", "--table", "t"], "");
    let mut input = b"\xef\xbb\xbf".to_vec();
    input.extend_from_slice(
        br#"{"series":"a","value_time":"2025-01-01T00:00:00Z","received_time":"2025-01-01T00:00:00Z","x":1}"#,
    );
    input.push(b'\n');
    t.ok_bytes(&["write", "-", "--input", "ndjson"], &input);
}

/// Review finding 10: `verify` must catch content-identical duplicate versions of a point
/// (here smuggled in through a hand-edited dump, since restore does not deduplicate).
#[test]
fn verify_detects_duplicate_versions() {
    let t = T::new();
    t.ok(&["series", "create", "a", "--table", "t"], "");
    t.ok(
        &["write", "-"],
        "series,value_time,received_time,x\na,2025-01-01T00:00:00Z,2025-01-01T00:00:00Z,1\n",
    );
    let dump = t.dir.path().join("dump");
    t.ok(&["dump", dump.to_str().unwrap()], "");
    let table = dump.join("tables/t.ndjson");
    let line = std::fs::read_to_string(&table).unwrap();
    let dup = line.replacen("\"seq\": 1,", "\"seq\": 2,", 1);
    let text = format!("{line}{dup}");
    std::fs::write(&table, &text).unwrap();
    let crc = format!("{:08x}", crc32c::crc32c(text.as_bytes()));
    let manifest = dump.join("manifest.json");
    let m = std::fs::read_to_string(&manifest).unwrap();
    let old_crc = m.split("\"crc32c\": \"").last().unwrap()[..8].to_owned();
    let m = m
        .replace(&old_crc, &crc)
        .replace("\"next_seq\": 2", "\"next_seq\": 3")
        .replace("\"versions\": 1", "\"versions\": 2");
    std::fs::write(&manifest, m).unwrap();
    let restored = t.dir.path().join("restored");
    t.ok(
        &[
            "restore",
            dump.to_str().unwrap(),
            restored.to_str().unwrap(),
        ],
        "",
    );
    let out = Command::new(BIN)
        .args(["--db", restored.to_str().unwrap(), "verify"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("content-identical"), "{stdout}");
}
