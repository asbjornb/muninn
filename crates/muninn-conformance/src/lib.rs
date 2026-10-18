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
//! The runner is deliberately strict about scenario shape (unknown ops, unknown keys,
//! wrong field sets) so that drift between the Python reference and Rust readers of the
//! format shows up as a failure instead of being silently ignored.

#![forbid(unsafe_code)]

mod compare;

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

use muninn_core::api::{
    AddColumn, AsofJoinRequest, AsofLeftRow, CreateSeries, CreateTable, GridRequest, GridWindow,
    LatestRequest, ReadContext, ReplayRequest, TimeInput, WriteBatch, WriteResult, WriteRow,
};
use muninn_core::{
    Clock, ColumnDef, ColumnType, Database, Duration, Error, ErrorKind, Partitioning, TimeRange,
    Timestamp, Value,
};
use serde_json::{Map, Value as Json};

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
    let json: Json = serde_json::from_str(&text).map_err(|e| fail(format!("invalid JSON: {e}")))?;
    run_scenario_json(path, &json, &factory())
}

/// Runs an already-parsed scenario against `db` (which must be empty). `file` is used for
/// failure reports only.
pub fn run_scenario_json<D: Database>(file: &Path, scenario: &Json, db: &D) -> Result<(), Failure> {
    let mut failure = Failure {
        file: file.to_path_buf(),
        scenario: None,
        step: None,
        op: None,
        message: String::new(),
    };
    let Some(obj) = scenario.as_object() else {
        failure.message = "scenario must be a JSON object".into();
        return Err(failure);
    };
    failure.scenario = obj.get("name").and_then(Json::as_str).map(str::to_owned);
    let Some(steps) = obj.get("steps").and_then(Json::as_array) else {
        failure.message = "scenario needs a `steps` array".into();
        return Err(failure);
    };
    for (i, step) in steps.iter().enumerate() {
        failure.step = Some(i);
        failure.op = step.get("op").and_then(Json::as_str).map(str::to_owned);
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

/// Why a request could not be built: the scenario is malformed (a runner failure), or an
/// input is invalid in the spec's sense (an expected-able error outcome).
enum BuildError {
    Format(String),
    Spec(Error),
}

impl From<Error> for BuildError {
    fn from(e: Error) -> BuildError {
        BuildError::Spec(e)
    }
}

type Build<T> = Result<T, BuildError>;

fn format_err<T>(msg: impl Into<String>) -> Build<T> {
    Err(BuildError::Format(msg.into()))
}

/// What a successful step produced.
enum Outcome {
    Done,
    Write(WriteResult),
    Read(Actual),
}

/// Keys every step may carry besides its op-specific ones.
const COMMON_KEYS: &[&str] = &["op", "expect", "comment", "description", "note"];
const READ_KEYS: &[&str] = &[
    "table",
    "series",
    "value_from",
    "value_to",
    "clock",
    "frontier",
    "system",
];

fn check_keys(step: &Map<String, Json>, allowed: &[&str]) -> Build<()> {
    for k in step.keys() {
        if !COMMON_KEYS.contains(&k.as_str()) && !allowed.contains(&k.as_str()) {
            return format_err(format!(
                "unknown key `{k}` for this op (allowed: {allowed:?})"
            ));
        }
    }
    Ok(())
}

fn get<'a>(o: &'a Map<String, Json>, key: &str) -> Option<&'a Json> {
    o.get(key).filter(|v| !v.is_null())
}

fn req_str<'a>(o: &'a Map<String, Json>, key: &str) -> Build<&'a str> {
    match o.get(key) {
        Some(Json::String(s)) => Ok(s),
        Some(other) => format_err(format!("`{key}` must be a string, found {other}")),
        None => format_err(format!("missing `{key}`")),
    }
}

fn opt_str<'a>(o: &'a Map<String, Json>, key: &str) -> Build<Option<&'a str>> {
    match get(o, key) {
        None => Ok(None),
        Some(Json::String(s)) => Ok(Some(s)),
        Some(other) => format_err(format!("`{key}` must be a string or null, found {other}")),
    }
}

/// Timestamps in requests: unparsable text is a spec error (`invalid_timestamp`).
fn opt_ts(o: &Map<String, Json>, key: &str) -> Build<Option<Timestamp>> {
    Ok(match opt_str(o, key)? {
        None => None,
        Some(s) => Some(Timestamp::parse(s).map_err(Error::from)?),
    })
}

fn req_ts(o: &Map<String, Json>, key: &str) -> Build<Timestamp> {
    Ok(Timestamp::parse(req_str(o, key)?).map_err(Error::from)?)
}

/// Durations: an unparsable one is a spec error (`invalid_range`, a "bad window").
fn opt_duration(o: &Map<String, Json>, key: &str) -> Build<Option<Duration>> {
    Ok(match opt_str(o, key)? {
        None => None,
        Some(s) => Some(Duration::parse(s).map_err(Error::from)?),
    })
}

fn opt_clock(o: &Map<String, Json>, key: &str) -> Build<Option<Clock>> {
    match opt_str(o, key)? {
        None => Ok(None),
        Some(s) => match Clock::parse(s) {
            Some(c) => Ok(Some(c)),
            None => format_err(format!("unknown clock {s:?}")),
        },
    }
}

fn opt_series_list(o: &Map<String, Json>) -> Build<Vec<String>> {
    match get(o, "series") {
        None => Ok(Vec::new()),
        Some(Json::Array(a)) => a
            .iter()
            .map(|v| match v {
                Json::String(s) => Ok(s.clone()),
                other => format_err(format!("series names must be strings, found {other}")),
            })
            .collect(),
        Some(other) => format_err(format!("`series` must be a list of names, found {other}")),
    }
}

fn value_range(o: &Map<String, Json>) -> Build<TimeRange> {
    Ok(TimeRange::new(
        opt_ts(o, "value_from")?,
        opt_ts(o, "value_to")?,
    ))
}

fn time_input(o: &Map<String, Json>, key: &str) -> Build<Option<TimeInput>> {
    // Passed through as text: the implementation validates times in SPEC §4.1 step 5.
    Ok(opt_str(o, key)?.map(TimeInput::from))
}

/// A JSON value as a loose write input (coerced by the implementation, SPEC §4.1 step 3).
fn input_value(j: &Json) -> Build<Option<Value>> {
    Ok(match j {
        Json::Null => None,
        Json::Bool(b) => Some(Value::Bool(*b)),
        Json::Number(n) => Some(if let Some(i) = n.as_i64() {
            Value::I64(i)
        } else {
            // Floats, and integers beyond i64 (u64 range or larger): only an f64 column
            // accepts these; they are a type_mismatch for i64 columns.
            Value::F64(
                n.as_f64()
                    .ok_or(BuildError::Format(format!("unrepresentable number {n}")))?,
            )
        }),
        Json::String(s) => Some(Value::String(s.clone())),
        other => {
            return format_err(format!(
                "unsupported value {other} (arrays/objects are not values)"
            ));
        }
    })
}

fn write_row(j: &Json) -> Build<WriteRow> {
    let Some(o) = j.as_object() else {
        return format_err(format!("write row must be an object, found {j}"));
    };
    check_keys(
        o,
        &[
            "series",
            "value_time",
            "published_time",
            "received_time",
            "deleted",
            "values",
        ],
    )?;
    let mut values = BTreeMap::new();
    match get(o, "values") {
        None => {}
        Some(Json::Object(m)) => {
            for (k, v) in m {
                values.insert(k.clone(), input_value(v)?);
            }
        }
        Some(other) => return format_err(format!("`values` must be an object, found {other}")),
    }
    let deleted = match get(o, "deleted") {
        None => false,
        Some(Json::Bool(b)) => *b,
        Some(other) => return format_err(format!("`deleted` must be a bool, found {other}")),
    };
    Ok(WriteRow {
        series: req_str(o, "series")?.to_owned(),
        value_time: time_input(o, "value_time")?
            .ok_or(BuildError::Format("missing `value_time`".into()))?,
        published_time: time_input(o, "published_time")?,
        received_time: time_input(o, "received_time")?
            .ok_or(BuildError::Format("missing `received_time`".into()))?,
        deleted,
        values,
    })
}

fn column_def(j: &Json) -> Build<ColumnDef> {
    let Some(o) = j.as_object() else {
        return format_err(format!("column must be an object, found {j}"));
    };
    let ty = req_str(o, "type")?;
    let Some(ty) = ColumnType::parse(ty) else {
        return format_err(format!("unknown column type {ty:?}"));
    };
    Ok(ColumnDef::new(req_str(o, "name")?, ty))
}

fn execute<D: Database>(db: &D, op: &str, s: &Map<String, Json>) -> Build<Result<Outcome, Error>> {
    Ok(match op {
        "create_table" => {
            check_keys(s, &["name", "columns", "default_clock", "partition"])?;
            let columns = match get(s, "columns") {
                None => Vec::new(),
                Some(Json::Array(a)) => a.iter().map(column_def).collect::<Build<_>>()?,
                Some(other) => {
                    return format_err(format!("`columns` must be a list, found {other}"));
                }
            };
            let mut req = CreateTable::new(req_str(s, "name")?, columns);
            if let Some(c) = opt_clock(s, "default_clock")? {
                req.default_clock = c;
            }
            if let Some(p) = opt_str(s, "partition")? {
                req.partition = Partitioning::parse(p)
                    .ok_or(BuildError::Format(format!("unknown partitioning {p:?}")))?;
            }
            db.create_table(&req).map(|_| Outcome::Done)
        }
        "add_column" => {
            check_keys(s, &["table", "name", "type"])?;
            let ty = req_str(s, "type")?;
            let ty = ColumnType::parse(ty)
                .ok_or(BuildError::Format(format!("unknown column type {ty:?}")))?;
            let req = AddColumn {
                table: req_str(s, "table")?.to_owned(),
                column: ColumnDef::new(req_str(s, "name")?, ty),
                clock: None,
            };
            db.add_column(&req).map(|_| Outcome::Done)
        }
        "create_series" => {
            check_keys(s, &["name", "table", "tags", "timezone", "resolution"])?;
            let mut req = CreateSeries::new(req_str(s, "name")?, req_str(s, "table")?);
            match get(s, "tags") {
                None => {}
                Some(Json::Object(m)) => {
                    for (k, v) in m {
                        let Some(v) = v.as_str() else {
                            return format_err(format!("tag values must be strings, found {v}"));
                        };
                        req.tags.insert(k.clone(), v.to_owned());
                    }
                }
                Some(other) => {
                    return format_err(format!("`tags` must be an object, found {other}"));
                }
            }
            req.timezone = opt_str(s, "timezone")?.map(str::to_owned);
            req.resolution = opt_duration(s, "resolution")?;
            db.create_series(&req).map(|_| Outcome::Done)
        }
        "write" => {
            check_keys(s, &["system_time", "batch_id", "rows"])?;
            let rows = match s.get("rows") {
                Some(Json::Array(a)) => a.iter().map(write_row).collect::<Build<Vec<_>>>()?,
                Some(other) => return format_err(format!("`rows` must be a list, found {other}")),
                None => return format_err("missing `rows`"),
            };
            let batch = WriteBatch {
                rows,
                batch_id: opt_str(s, "batch_id")?.map(str::to_owned),
                clock: req_ts(s, "system_time")?,
            };
            db.write(&batch).map(Outcome::Write)
        }
        "latest" | "history" => {
            check_keys(s, READ_KEYS)?;
            let req = LatestRequest {
                table: req_str(s, "table")?.to_owned(),
                series: opt_series_list(s)?,
                range: value_range(s)?,
                context: ReadContext {
                    clock: opt_clock(s, "clock")?,
                    frontier: opt_ts(s, "frontier")?,
                    system: opt_ts(s, "system")?,
                },
            };
            if op == "latest" {
                db.latest(&req)
                    .map(|r| Outcome::Read(Actual::versions(ReadOp::Latest, r)))
            } else {
                db.history(&req)
                    .map(|r| Outcome::Read(Actual::versions(ReadOp::History, r)))
            }
        }
        "replay" => {
            check_keys(
                s,
                &[
                    "table",
                    "series",
                    "value_from",
                    "value_to",
                    "clock",
                    "system",
                    "from",
                    "to",
                ],
            )?;
            let req = ReplayRequest {
                table: req_str(s, "table")?.to_owned(),
                series: opt_series_list(s)?,
                range: value_range(s)?,
                clock: opt_clock(s, "clock")?,
                system: opt_ts(s, "system")?,
                arrival: TimeRange::new(opt_ts(s, "from")?, opt_ts(s, "to")?),
            };
            db.replay(&req)
                .map(|r| Outcome::Read(Actual::versions(ReadOp::Replay, r)))
        }
        "grid" => {
            check_keys(
                s,
                &[
                    "table",
                    "series",
                    "value_from",
                    "value_to",
                    "clock",
                    "system",
                    "frontiers",
                    "window_start",
                    "window_end",
                ],
            )?;
            let frontiers = match s.get("frontiers") {
                Some(Json::Array(a)) => a
                    .iter()
                    .map(|f| match f {
                        Json::String(t) => Ok(Timestamp::parse(t).map_err(Error::from)?),
                        other => format_err(format!(
                            "frontiers must be timestamp strings, found {other}"
                        )),
                    })
                    .collect::<Build<Vec<_>>>()?,
                Some(other) => {
                    return format_err(format!("`frontiers` must be a list, found {other}"));
                }
                None => return format_err("missing `frontiers`"),
            };
            let absolute = get(s, "value_from").is_some() || get(s, "value_to").is_some();
            let window = match (
                opt_duration(s, "window_start")?,
                opt_duration(s, "window_end")?,
            ) {
                (None, None) => GridWindow::Absolute(value_range(s)?),
                // Relative and absolute windows together, or a half-specified relative
                // window, are a bad window (conservative reading; see SPEC-QUESTIONS.md).
                (Some(_), Some(_)) if absolute => {
                    return Ok(Err(Error::new(
                        ErrorKind::InvalidRange,
                        "grid window is either absolute or relative, not both",
                    )));
                }
                (Some(start), Some(end)) => GridWindow::Relative { start, end },
                _ => {
                    return Ok(Err(Error::new(
                        ErrorKind::InvalidRange,
                        "a relative grid window needs both window_start and window_end",
                    )));
                }
            };
            let req = GridRequest {
                table: req_str(s, "table")?.to_owned(),
                series: opt_series_list(s)?,
                frontiers,
                window,
                clock: opt_clock(s, "clock")?,
                system: opt_ts(s, "system")?,
            };
            db.grid(&req).map(|r| Outcome::Read(Actual::grid(r)))
        }
        "asof_join" => {
            check_keys(s, &["table", "clock", "system", "left"])?;
            let left = match s.get("left") {
                Some(Json::Array(a)) => a
                    .iter()
                    .map(|l| {
                        let Some(o) = l.as_object() else {
                            return format_err(format!("left rows must be objects, found {l}"));
                        };
                        check_keys(o, &["series", "value_time", "frontier"])?;
                        Ok(AsofLeftRow {
                            series: req_str(o, "series")?.to_owned(),
                            value_time: req_ts(o, "value_time")?,
                            frontier: req_ts(o, "frontier")?,
                        })
                    })
                    .collect::<Build<Vec<_>>>()?,
                Some(other) => return format_err(format!("`left` must be a list, found {other}")),
                None => return format_err("missing `left`"),
            };
            let req = AsofJoinRequest {
                table: req_str(s, "table")?.to_owned(),
                clock: opt_clock(s, "clock")?,
                system: opt_ts(s, "system")?,
                left,
            };
            db.asof_join(&req).map(|r| Outcome::Read(Actual::asof(r)))
        }
        other => return format_err(format!("unknown op {other:?}")),
    })
}

fn run_step<D: Database>(db: &D, step: &Json) -> Result<(), String> {
    let Some(s) = step.as_object() else {
        return Err(format!("step must be an object, found {step}"));
    };
    let op = s
        .get("op")
        .and_then(Json::as_str)
        .ok_or("step has no string `op`")?;
    let expect = s.get("expect");
    let outcome = match execute(db, op, s) {
        Ok(outcome) => outcome,
        Err(BuildError::Spec(e)) => Err(e),
        Err(BuildError::Format(m)) => return Err(format!("malformed scenario step: {m}")),
    };
    check_expectation(op, expect, outcome)
}

fn describe_outcome(outcome: &Result<Outcome, Error>) -> String {
    match outcome {
        Err(e) => format!("error {e}"),
        Ok(Outcome::Done) => "success".into(),
        Ok(Outcome::Write(w)) => format!(
            "success {{stored: {}, duplicates: {}, batch_already_committed: {}}}",
            w.stored, w.duplicates, w.batch_already_committed
        ),
        Ok(Outcome::Read(a)) => format!("success with {} rows", a.rows.len()),
    }
}

fn check_expectation(
    op: &str,
    expect: Option<&Json>,
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
        return Err(format!("`expect` must be an object, found {expect}"));
    };

    if let Some(code) = exp.get("error") {
        if exp.len() != 1 {
            return Err("an `error` expectation cannot be combined with other fields".into());
        }
        let Some(code) = code.as_str() else {
            return Err(format!("`error` must be a string, found {code}"));
        };
        if ErrorKind::from_spec_code(code).is_none() {
            return Err(format!("unknown error code {code:?} in expectation"));
        }
        return match &outcome {
            Err(e) if e.kind().spec_code() == Some(code) => Ok(()),
            _ => Err(format!(
                "expected error {code}, got {}",
                describe_outcome(&outcome)
            )),
        };
    }

    let outcome = outcome.map_err(|e| format!("expected success ({expect}), got error {e}"))?;
    match outcome {
        Outcome::Done => {
            if exp.is_empty() {
                Ok(())
            } else {
                Err(format!("unsupported expectation for {op}: {expect}"))
            }
        }
        Outcome::Write(w) => {
            let mut diffs = Vec::new();
            for (k, v) in exp {
                let actual = match k.as_str() {
                    "stored" => Json::from(w.stored),
                    "duplicates" => Json::from(w.duplicates),
                    "batch_already_committed" => Json::from(w.batch_already_committed),
                    other => return Err(format!("unknown write expectation field `{other}`")),
                };
                if *v != actual {
                    diffs.push(format!("{k}: expected {v}, got {actual}"));
                }
            }
            if diffs.is_empty() {
                Ok(())
            } else {
                Err(format!("write result differs:\n{}", diffs.join("\n")))
            }
        }
        Outcome::Read(actual) => {
            let (key, full) = match (exp.get("rows"), exp.get("values")) {
                (Some(_), None) if exp.len() == 1 => ("rows", true),
                (None, Some(_)) if exp.len() == 1 => ("values", false),
                _ => {
                    return Err(format!(
                        "read expectation needs exactly one of `rows` / `values`: {expect}"
                    ));
                }
            };
            let Some(expected) = exp[key].as_array() else {
                return Err(format!("`expect.{key}` must be a list"));
            };
            compare_rows(&actual, expected, full)
        }
    }
}

/// Compares the complete row list; on mismatch, renders a readable side-by-side diff.
fn compare_rows(actual: &Actual, expected: &[Json], full: bool) -> Result<(), String> {
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
