//! # muninn-conformance
//!
//! Runs the language-neutral JSON scenarios of `spec/scenarios/**/*.json` (format: SPEC §9)
//! against any [`Database`]. Used by this crate's tests against
//! [`muninn_core::ModelDb`], and by the storage engine's tests later:
//!
//! ```no_run
//! # use muninn_conformance::*;
//! let report = run_corpus(&spec_scenarios_dir(), muninn_core::ModelDb::new);
//! report.assert_ok();
//! ```
//!
//! Every step's outcome is compared exactly: errors by SPEC §8 code, write results by
//! field, read results as the complete row list in order (timestamps as integers, `f64`
//! bitwise with JSON numbers parsed to the nearest `f64`), with the `values` sugar of §9.
//!
//! Requests are passed to the database through [`muninn_core::wire::execute`], loose:
//! anything malformed *inside* a request (unknown fields, wrong JSON types, bad
//! timestamps or values) is reported by the database as its SPEC §8 code and compared
//! with the expectation. Only a file that does not have the §9 scenario shape (not JSON,
//! no `steps`, a step without a known `op`, a malformed `expect`) is a runner error.

#![forbid(unsafe_code)]

mod compare;

use std::fmt;
use std::path::{Path, PathBuf};

use muninn_core::wire::{self, Outcome, Wire};
use muninn_core::{Database, Error, ErrorKind};

use compare::{Actual, ReadOp};

/// A scenario that did not pass (or could not be run).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Failure {
    /// The scenario file.
    pub file: PathBuf,
    /// The scenario's `name`, if it could be read.
    pub scenario: Option<String>,
    /// 0-based step index, if the failure belongs to a step.
    pub step: Option<usize>,
    /// The step's `op`, if known.
    pub op: Option<String>,
    /// What went wrong, including an expected-vs-actual diff where applicable.
    pub message: String,
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.file.display())?;
        if let Some(name) = &self.scenario {
            write!(f, " ({name})")?;
        }
        if let Some(step) = self.step {
            write!(f, ", step {step}")?;
        }
        if let Some(op) = &self.op {
            write!(f, " [{op}]")?;
        }
        write!(f, ":\n{}", indent(&self.message, "    "))
    }
}

impl std::error::Error for Failure {}

fn indent(text: &str, prefix: &str) -> String {
    text.lines()
        .map(|l| format!("{prefix}{l}"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The result of running a directory of scenarios.
#[derive(Clone, Debug, Default)]
pub struct CorpusReport {
    /// Every scenario file that was run, in order.
    pub files: Vec<PathBuf>,
    /// The failures (one per failing file).
    pub failures: Vec<Failure>,
}

impl CorpusReport {
    /// Number of passing scenarios.
    pub fn passed(&self) -> usize {
        self.files.len() - self.failures.len()
    }

    /// Panics with every failure if any scenario failed, or if the corpus is empty.
    pub fn assert_ok(&self) {
        assert!(!self.files.is_empty(), "no scenario files found");
        if !self.failures.is_empty() {
            let all: Vec<String> = self.failures.iter().map(|f| f.to_string()).collect();
            panic!(
                "{} of {} scenarios failed:\n\n{}",
                self.failures.len(),
                self.files.len(),
                all.join("\n\n")
            );
        }
    }
}

impl fmt::Display for CorpusReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} scenarios, {} passed, {} failed",
            self.files.len(),
            self.passed(),
            self.failures.len()
        )
    }
}

/// `spec/scenarios` of this repository (resolved from this crate's location).
pub fn spec_scenarios_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../spec/scenarios")
}

/// Every `*.json` file under `dir`, recursively, sorted by path.
pub fn scenario_files(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
        for entry in std::fs::read_dir(dir)? {
            let path = entry?.path();
            if path.is_dir() {
                walk(&path, out)?;
            } else if path.extension().is_some_and(|e| e == "json") {
                out.push(path);
            }
        }
        Ok(())
    }
    let mut out = Vec::new();
    walk(dir, &mut out)?;
    out.sort();
    Ok(out)
}

/// Runs every scenario under `dir` (recursively), each against a fresh database from
/// `factory`.
pub fn run_corpus<D: Database>(dir: &Path, factory: impl Fn() -> D) -> CorpusReport {
    let files = match scenario_files(dir) {
        Ok(f) => f,
        Err(e) => {
            return CorpusReport {
                files: vec![dir.to_path_buf()],
                failures: vec![Failure {
                    file: dir.to_path_buf(),
                    scenario: None,
                    step: None,
                    op: None,
                    message: format!("cannot list scenarios: {e}"),
                }],
            };
        }
    };
    let failures = files
        .iter()
        .filter_map(|f| run_scenario(f, &factory).err())
        .collect();
    CorpusReport { files, failures }
}

/// Runs one scenario file against a fresh database from `factory`.
pub fn run_scenario<D: Database>(path: &Path, factory: impl Fn() -> D) -> Result<(), Failure> {
    let fail = |message: String| Failure {
        file: path.to_path_buf(),
        scenario: None,
        step: None,
        op: None,
        message,
    };
    let text = std::fs::read_to_string(path).map_err(|e| fail(format!("cannot read: {e}")))?;
    let json = Wire::parse(&text).map_err(|e| fail(e.to_string()))?;
    run_scenario_json(path, &json, &factory())
}

/// Runs an already-parsed scenario against `db` (which must be empty). `file` is used for
/// failure reports only.
pub fn run_scenario_json<D: Database>(file: &Path, scenario: &Wire, db: &D) -> Result<(), Failure> {
    let mut failure = Failure {
        file: file.to_path_buf(),
        scenario: None,
        step: None,
        op: None,
        message: String::new(),
    };
    if scenario.as_object().is_none() {
        failure.message = "scenario must be a JSON object".into();
        return Err(failure);
    }
    failure.scenario = scenario
        .get("name")
        .and_then(Wire::as_str)
        .map(str::to_owned);
    for key in ["name", "description"] {
        if scenario.get(key).and_then(Wire::as_str).is_none() {
            failure.message = format!("scenario needs a string `{key}`");
            return Err(failure);
        }
    }
    let Some(steps) = scenario.get("steps").and_then(Wire::as_array) else {
        failure.message = "scenario needs a `steps` array".into();
        return Err(failure);
    };
    for (i, step) in steps.iter().enumerate() {
        failure.step = Some(i);
        failure.op = step.get("op").and_then(Wire::as_str).map(str::to_owned);
        if let Err(message) = run_step(db, step) {
            failure.message = message;
            return Err(failure);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------
// Step execution
// ---------------------------------------------------------------------------------------

fn run_step<D: Database>(db: &D, step: &Wire) -> Result<(), String> {
    let Some(fields) = step.as_object() else {
        return Err(format!(
            "malformed scenario: step must be an object, found {step}"
        ));
    };
    let op = match step.get("op") {
        Some(Wire::String(op)) if wire::OPS.contains(&op.as_str()) => op.as_str(),
        other => {
            return Err(format!(
                "malformed scenario: unknown op {}",
                other.map_or("(missing)".into(), Wire::to_string)
            ));
        }
    };
    // Everything except `op` and `expect` is the request, passed through as is.
    let request: Vec<(String, Wire)> = fields
        .iter()
        .filter(|(k, _)| k != "op" && k != "expect")
        .cloned()
        .collect();
    let outcome = wire::execute(db, op, &request);
    check_expectation(op, step.get("expect"), outcome)
}

fn describe_outcome(outcome: &Result<Outcome, Error>) -> String {
    match outcome {
        Err(e) => format!("error {e}"),
        Ok(Outcome::Done) => "success".into(),
        Ok(Outcome::Write(w)) => format!(
            "success {{\"stored\": {}, \"duplicates\": {}, \"batch_already_committed\": {}}}",
            w.stored, w.duplicates, w.batch_already_committed
        ),
        Ok(Outcome::Versions(r)) => format!("success with {} rows", r.rows.len()),
        Ok(Outcome::Grid(r)) => format!("success with {} rows", r.rows.len()),
        Ok(Outcome::Asof(r)) => format!("success with {} rows", r.rows.len()),
    }
}

fn check_expectation(
    op: &str,
    expect: Option<&Wire>,
    outcome: Result<Outcome, Error>,
) -> Result<(), String> {
    let Some(expect) = expect else {
        // A step without `expect` must simply succeed.
        return match outcome {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("expected success, got error {e}")),
        };
    };
    let Some(exp) = expect.as_object() else {
        return Err(format!(
            "malformed scenario: `expect` must be an object, found {expect}"
        ));
    };

    if let Some(code) = expect.get("error") {
        if exp.len() != 1 {
            return Err("malformed scenario: an `error` expectation stands alone".into());
        }
        let Some(code) = code
            .as_str()
            .filter(|c| ErrorKind::from_spec_code(c).is_some())
        else {
            return Err(format!("malformed scenario: unknown error code {code}"));
        };
        return match &outcome {
            Err(e) if e.kind().spec_code() == Some(code) => Ok(()),
            _ => Err(format!(
                "expected error {code}, got {}",
                describe_outcome(&outcome)
            )),
        };
    }

    let outcome = outcome.map_err(|e| format!("expected success ({expect}), got error {e}"))?;
    let actual = match outcome {
        Outcome::Done => {
            return Err(format!(
                "malformed scenario: catalog steps take no expectation other than an error: {expect}"
            ));
        }
        Outcome::Write(w) => {
            // §9: a write's `expect` is the complete §4.5 result object.
            let actual = Wire::Object(vec![
                ("stored".into(), Wire::Number(w.stored.to_string())),
                ("duplicates".into(), Wire::Number(w.duplicates.to_string())),
                (
                    "batch_already_committed".into(),
                    Wire::Bool(w.batch_already_committed),
                ),
            ]);
            let same = exp.len() == 3
                && ["stored", "duplicates"].iter().all(|k| {
                    expect
                        .get(k)
                        .filter(|e| e.is_integer_token())
                        .and_then(Wire::as_number)
                        .and_then(|n| n.parse::<u64>().ok())
                        == actual
                            .get(k)
                            .and_then(Wire::as_number)
                            .and_then(|n| n.parse().ok())
                })
                && expect.get("batch_already_committed") == actual.get("batch_already_committed");
            return if same {
                Ok(())
            } else {
                Err(format!(
                    "write result differs:\n  expected {expect}\n  actual   {actual}"
                ))
            };
        }
        Outcome::Versions(r) => Actual::versions(ReadOp::from_op(op).expect("read op"), r),
        Outcome::Grid(r) => Actual::grid(r),
        Outcome::Asof(r) => Actual::asof(r),
    };
    let (key, full) = match (expect.get("rows"), expect.get("values")) {
        (Some(_), None) if exp.len() == 1 => ("rows", true),
        (None, Some(_)) if exp.len() == 1 => ("values", false),
        _ => {
            return Err(format!(
                "malformed scenario: read expectation needs exactly one of `rows` / `values`: {expect}"
            ));
        }
    };
    let Some(expected) = expect.get(key).and_then(Wire::as_array) else {
        return Err(format!("malformed scenario: `expect.{key}` must be a list"));
    };
    compare_rows(&actual, expected, full)
}

/// Compares the complete row list; on mismatch, renders a readable side-by-side diff.
fn compare_rows(actual: &Actual, expected: &[Wire], full: bool) -> Result<(), String> {
    let n = expected.len().max(actual.rows.len());
    let mut first: Option<(usize, String)> = None;
    let mut equal = vec![false; n];
    for (i, eq) in equal.iter_mut().enumerate() {
        let r = match (expected.get(i), actual.rows.get(i)) {
            (Some(e), Some(a)) => {
                if full {
                    compare::compare_full(actual, e, a)
                } else {
                    compare::compare_sugar(actual, e, a)
                }
            }
            (Some(_), None) => Err("row missing from actual result".into()),
            (None, Some(_)) => Err("unexpected extra row in actual result".into()),
            (None, None) => unreachable!(),
        };
        match r {
            Ok(()) => *eq = true,
            Err(m) if first.is_none() => first = Some((i, m)),
            Err(_) => {}
        }
    }
    let Some((at, why)) = first else {
        return Ok(());
    };
    let render_actual = |i: usize| {
        let a = &actual.rows[i];
        if full {
            compare::render_full(actual, a)
        } else {
            compare::render_sugar(actual, a, expected.get(i))
        }
    };
    // Show a window of rows around the first mismatch.
    let lo = at.saturating_sub(3);
    let hi = (at + 20).min(n);
    let mut out = format!(
        "{} differ: expected {} rows, got {}; first mismatch at row {at}: {why}\n",
        if full { "rows" } else { "values" },
        expected.len(),
        actual.rows.len()
    );
    if lo > 0 {
        out.push_str(&format!("  ... {lo} matching rows omitted\n"));
    }
    for (i, &eq) in equal.iter().enumerate().take(hi).skip(lo) {
        if eq {
            out.push_str(&format!("  [{i}]   {}\n", render_actual(i)));
            continue;
        }
        if let Some(e) = expected.get(i) {
            out.push_str(&format!("  [{i}] - {e}\n"));
        }
        if i < actual.rows.len() {
            out.push_str(&format!("  [{i}] + {}\n", render_actual(i)));
        }
    }
    if hi < n {
        out.push_str(&format!("  ... {} more rows\n", n - hi));
    }
    out.push_str("  (- expected, + actual)");
    Err(out)
}
