//! The JSON conformance corpus against the in-memory model, plus self-tests of the runner.

use std::path::Path;

use muninn_conformance::{run_corpus, run_scenario_json, scenario_files, spec_scenarios_dir};
use muninn_core::ModelDb;
use serde_json::json;

#[test]
fn spec_corpus_passes_on_model() {
    let dir = spec_scenarios_dir();
    let report = run_corpus(&dir, ModelDb::new);
    eprintln!("{report}");
    report.assert_ok();
    assert!(
        report.files.len() >= 3,
        "seed scenarios missing from {}",
        dir.display()
    );
}

#[test]
fn finds_nested_scenarios_sorted() {
    let files = scenario_files(&spec_scenarios_dir()).unwrap();
    let mut sorted = files.clone();
    sorted.sort();
    assert_eq!(files, sorted);
    assert!(files.iter().all(|f| f.extension().unwrap() == "json"));
}

fn run(steps: serde_json::Value) -> Result<(), String> {
    let scenario = json!({"name": "inline", "steps": steps});
    run_scenario_json(Path::new("<inline>"), &scenario, &ModelDb::new()).map_err(|f| f.to_string())
}

fn setup() -> Vec<serde_json::Value> {
    vec![
        json!({"op": "create_table", "name": "t", "columns": [
            {"name": "value", "type": "f64"}, {"name": "n", "type": "i64"},
            {"name": "flag", "type": "bool"}, {"name": "s", "type": "string"}, {"name": "at", "type": "timestamp"}]}),
        json!({"op": "create_series", "name": "a", "table": "t", "tags": {"k": "v"}, "timezone": "Europe/Copenhagen", "resolution": "15m"}),
        json!({"op": "create_series", "name": "b", "table": "t"}),
    ]
}

fn with(mut steps: Vec<serde_json::Value>, more: serde_json::Value) -> serde_json::Value {
    steps.extend(more.as_array().unwrap().iter().cloned());
    serde_json::Value::Array(steps)
}

#[test]
fn every_value_type_round_trips_through_json() {
    let steps = with(
        setup(),
        json!([
            {"op": "write", "system_time": "2025-01-01T00:00:00Z", "rows": [
                {"series": "a", "value_time": "2025-01-01T00:00:00Z", "received_time": "2025-01-01T00:00:00+01:00",
                 "values": {"value": "NaN", "n": "9007199254740993", "flag": true, "s": "x\"y", "at": "2025-01-01T01:00:00+01:00"}},
                {"series": "a", "value_time": "2025-01-01T01:00:00Z", "received_time": "2025-01-01T00:00:00Z",
                 "values": {"value": -0.0, "n": -5}},
                {"series": "a", "value_time": "2025-01-01T02:00:00Z", "received_time": "2025-01-01T00:00:00Z",
                 "values": {"value": 3, "n": null}},
                {"series": "a", "value_time": "2025-01-01T03:00:00Z", "received_time": "2025-01-01T00:00:00Z",
                 "values": {"value": "-inf"}}
            ], "expect": {"stored": 4, "duplicates": 0, "batch_already_committed": false}},
            {"op": "latest", "table": "t", "expect": {"rows": [
                {"series": "a", "value_time": "2025-01-01T00:00:00Z", "published_time": null,
                 "received_time": "2024-12-31T23:00:00Z", "system_time": "2025-01-01T00:00:00Z", "seq": 1,
                 "values": {"value": "NaN", "n": 9007199254740993_i64, "flag": true, "s": "x\"y", "at": "2025-01-01T00:00:00Z"}},
                {"series": "a", "value_time": "2025-01-01T01:00:00Z", "published_time": null,
                 "received_time": "2025-01-01T00:00:00Z", "system_time": "2025-01-01T00:00:00Z", "seq": 2,
                 "values": {"value": -0.0, "n": "-5", "flag": null, "s": null, "at": null}},
                {"series": "a", "value_time": "2025-01-01T02:00:00Z", "published_time": null,
                 "received_time": "2025-01-01T00:00:00Z", "system_time": "2025-01-01T00:00:00Z", "seq": 3,
                 "values": {"value": 3.0, "n": null, "flag": null, "s": null, "at": null}},
                {"series": "a", "value_time": "2025-01-01T03:00:00Z", "published_time": null,
                 "received_time": "2025-01-01T00:00:00Z", "system_time": "2025-01-01T00:00:00Z", "seq": 4,
                 "values": {"value": "-inf", "n": null, "flag": null, "s": null, "at": null}}
            ]}},
            // -0.0 vs 0.0 is a mismatch in comparisons too (bitwise).
            {"op": "latest", "table": "t", "value_from": "2025-01-01T01:00:00Z", "value_to": "2025-01-01T02:00:00Z",
             "expect": {"values": [["a", "2025-01-01T01:00:00Z", {"value": -0.0}]]}}
        ]),
    );
    run(steps).unwrap();
}

#[test]
fn sugar_forms_and_all_read_ops() {
    let steps = with(
        setup(),
        json!([
            {"op": "write", "system_time": "2025-01-01T00:00:00Z", "batch_id": "b1", "rows": [
                {"series": "a", "value_time": "2025-01-02T00:00:00Z", "received_time": "2025-01-01T00:00:00Z", "values": {"value": 1}},
                {"series": "b", "value_time": "2025-01-02T00:00:00Z", "received_time": "2025-01-01T00:00:00Z", "values": {"value": 2}}]},
            {"op": "write", "system_time": "2025-01-01T00:00:00Z", "batch_id": "b1", "rows": [],
             "expect": {"batch_already_committed": true, "stored": 0}},
            {"op": "write", "system_time": "2025-01-01T06:00:00Z", "rows": [
                {"series": "a", "value_time": "2025-01-02T00:00:00Z", "received_time": "2025-01-01T06:00:00Z", "deleted": true}]},
            {"op": "history", "table": "t", "series": ["a"], "expect": {"values": [
                ["a", "2025-01-02T00:00:00Z", {"value": 1.0}],
                ["a", "2025-01-02T00:00:00Z", "deleted"]]}},
            {"op": "replay", "table": "t", "from": "2025-01-01T00:00:00Z", "expect": {"values": [
                ["a", "2025-01-02T00:00:00Z", {"value": 1.0, "n": null}],
                ["b", "2025-01-02T00:00:00Z", {"value": 2.0}],
                ["a", "2025-01-02T00:00:00Z", "deleted"]]}},
            {"op": "grid", "table": "t", "frontiers": ["2025-01-01T06:00:00Z", "2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z"],
             "window_start": "0h", "window_end": "25h", "expect": {"values": [
                ["2025-01-01T00:00:00Z", "a", "2025-01-02T00:00:00Z", {"value": 1.0}],
                ["2025-01-01T00:00:00Z", "b", "2025-01-02T00:00:00Z", {"value": 2.0}],
                ["2025-01-01T06:00:00Z", "b", "2025-01-02T00:00:00Z", {"value": 2.0}]]}},
            {"op": "grid", "table": "t", "frontiers": ["2025-01-01T06:00:00Z"], "window_start": "0h", "window_end": "1h",
             "expect": {"rows": []}},
            {"op": "grid", "table": "t", "frontiers": ["2025-01-01T06:00:00Z"], "series": ["b"], "expect": {"rows": [
                {"frontier": "2025-01-01T06:00:00Z", "series": "b", "value_time": "2025-01-02T00:00:00Z", "published_time": null,
                 "received_time": "2025-01-01T00:00:00Z", "system_time": "2025-01-01T00:00:00Z", "seq": 2,
                 "values": {"value": 2.0, "n": null, "flag": null, "s": null, "at": null}}]}},
            {"op": "asof_join", "table": "t", "left": [
                {"series": "a", "value_time": "2025-01-02T00:00:00Z", "frontier": "2025-01-01T05:00:00Z"},
                {"series": "a", "value_time": "2025-01-02T00:00:00Z", "frontier": "2025-01-01T06:00:00Z"}],
             "expect": {"values": [
                ["a", "2025-01-02T00:00:00Z", "2025-01-01T05:00:00Z", {"value": 1.0}],
                ["a", "2025-01-02T00:00:00Z", "2025-01-01T06:00:00Z", null]]}},
            {"op": "asof_join", "table": "t", "left": [
                {"series": "a", "value_time": "2025-01-02T00:00:00Z", "frontier": "2025-01-01T06:00:00Z"}],
             "expect": {"rows": [
                {"series": "a", "value_time": "2025-01-02T00:00:00Z", "frontier": "2025-01-01T06:00:00Z", "found": false,
                 "published_time": null, "received_time": null, "system_time": null, "seq": null, "values": null}]}},
            {"op": "add_column", "table": "t", "name": "extra", "type": "f64"},
            {"op": "latest", "table": "t", "expect": {"values": [["b", "2025-01-02T00:00:00Z", {"value": 2.0, "extra": null}]]}}
        ]),
    );
    run(steps).unwrap();
}

#[test]
fn error_expectations() {
    let steps = with(
        setup(),
        json!([
            {"op": "create_table", "name": "t", "expect": {"error": "already_exists"}},
            {"op": "create_table", "name": "Bad", "expect": {"error": "invalid_name"}},
            {"op": "add_column", "table": "t", "name": "seq", "type": "i64", "expect": {"error": "invalid_name"}},
            {"op": "create_series", "name": "x", "table": "nope", "expect": {"error": "unknown_table"}},
            {"op": "write", "system_time": "2025-01-01T00:00:00Z", "rows": [
                {"series": "a", "value_time": "2025-01-01T00:00:00Z", "received_time": "2025-01-01T00:00:00Z", "values": {"n": 1.5}}],
             "expect": {"error": "type_mismatch"}},
            {"op": "write", "system_time": "2025-01-01T00:00:00Z", "rows": [
                {"series": "a", "value_time": "2025-13-01T00:00:00Z", "received_time": "2025-01-01T00:00:00Z"}],
             "expect": {"error": "invalid_timestamp"}},
            {"op": "write", "system_time": "2025-01-01T00:00:00Z", "rows": [
                {"series": "a", "value_time": "2025-01-01T00:00:00Z", "received_time": "2025-01-01T00:00:00Z", "values": {"n": 18446744073709551615_u64}}],
             "expect": {"error": "type_mismatch"}},
            {"op": "latest", "table": "t", "frontier": "yesterday", "expect": {"error": "invalid_timestamp"}},
            {"op": "latest", "table": "t", "value_from": "2025-01-02T00:00:00Z", "value_to": "2025-01-01T00:00:00Z", "expect": {"error": "invalid_range"}},
            {"op": "grid", "table": "t", "frontiers": [], "window_start": "1h", "expect": {"error": "invalid_range"}},
            {"op": "grid", "table": "t", "frontiers": [], "window_start": "1x", "window_end": "2h", "expect": {"error": "invalid_range"}},
            {"op": "history", "table": "nope", "expect": {"error": "unknown_table"}},
            {"op": "history", "table": "t", "expect": {"rows": []}}
        ]),
    );
    run(steps).unwrap();
}

#[test]
fn failures_are_reported_with_location_and_diff() {
    let steps = with(
        setup(),
        json!([
            {"op": "write", "system_time": "2025-01-01T00:00:00Z", "rows": [
                {"series": "a", "value_time": "2025-01-01T00:00:00Z", "received_time": "2025-01-01T00:00:00Z", "values": {"value": 1.0}},
                {"series": "a", "value_time": "2025-01-01T01:00:00Z", "received_time": "2025-01-01T00:00:00Z", "values": {"value": 2.0}}]},
            {"op": "latest", "table": "t", "expect": {"values": [
                ["a", "2025-01-01T00:00:00Z", {"value": 1.0}],
                ["a", "2025-01-01T01:00:00Z", {"value": 2.5}]]}}
        ]),
    );
    let msg = run(steps).unwrap_err();
    assert!(msg.contains("<inline> (inline), step 4 [latest]"), "{msg}");
    assert!(
        msg.contains("first mismatch at row 1: values.value: expected 2.5, got 2.0"),
        "{msg}"
    );
    assert!(
        msg.contains("[1] - [\"a\",\"2025-01-01T01:00:00Z\",{\"value\":2.5}]"),
        "{msg}"
    );
    assert!(
        msg.contains("[1] + [\"a\", \"2025-01-01T01:00:00Z\", {\"value\": 2.0}]"),
        "{msg}"
    );

    let cases = [
        (
            json!([{"op": "create_table", "name": "t", "expect": {"error": "invalid_name"}}]),
            "expected error invalid_name, got success",
        ),
        (
            json!([{"op": "create_table", "name": "t"}, {"op": "create_table", "name": "t"}]),
            "expected success, got error already_exists",
        ),
        (json!([{"op": "frobnicate"}]), "unknown op \"frobnicate\""),
        (
            json!([{"op": "create_table", "name": "t", "colums": []}]),
            "unknown key `colums`",
        ),
        (
            json!([{"op": "create_table", "name": "t", "expect": {"error": "nope"}}]),
            "unknown error code",
        ),
        (
            json!([{"op": "create_table", "name": "t"}, {"op": "latest", "table": "t", "expect": {"rows": [], "values": []}}]),
            "exactly one of",
        ),
        (
            json!([{"op": "create_table", "name": "t", "columns": [{"name": "v", "type": "f64"}]},
                {"op": "create_series", "name": "s", "table": "t"},
                {"op": "write", "system_time": "2025-01-01T00:00:00Z", "rows": [
                    {"series": "s", "value_time": "2025-01-01T00:00:00Z", "received_time": "2025-01-01T00:00:00Z"}]},
                {"op": "latest", "table": "t", "expect": {"rows": [
                    {"series": "s", "value_time": "2025-01-01T00:00:00Z", "published_time": null, "received_time": "2025-01-01T00:00:00Z",
                     "system_time": "2025-01-01T00:00:00Z", "seq": 1, "deleted": false, "values": {"v": null}}]}}]),
            "must have exactly the fields",
        ),
        (
            json!([{"op": "create_table", "name": "t", "columns": [{"name": "v", "type": "f64"}]},
                {"op": "create_series", "name": "s", "table": "t"},
                {"op": "write", "system_time": "2025-01-01T00:00:00Z", "rows": [
                    {"series": "s", "value_time": "2025-01-01T00:00:00Z", "received_time": "2025-01-01T00:00:00Z"}],
                 "expect": {"stored": 1, "duplicates": 1}}]),
            "duplicates: expected 1, got 0",
        ),
    ];
    for (steps, needle) in cases {
        let msg = run(steps).unwrap_err();
        assert!(msg.contains(needle), "expected {needle:?} in:\n{msg}");
    }
}

#[test]
fn negative_zero_and_nan_are_compared_bitwise() {
    let base = || {
        with(
            setup(),
            json!([
                {"op": "write", "system_time": "2025-01-01T00:00:00Z", "rows": [
                    {"series": "a", "value_time": "2025-01-01T00:00:00Z", "received_time": "2025-01-01T00:00:00Z", "values": {"value": -0.0}},
                    {"series": "b", "value_time": "2025-01-01T00:00:00Z", "received_time": "2025-01-01T00:00:00Z", "values": {"value": "NaN"}}]}
            ]),
        )
    };
    let check = |a: serde_json::Value, b: serde_json::Value| {
        let mut steps = base();
        steps.as_array_mut().unwrap().push(json!({"op": "latest", "table": "t", "expect": {"values": [
            ["a", "2025-01-01T00:00:00Z", {"value": a}], ["b", "2025-01-01T00:00:00Z", {"value": b}]]}}));
        run(steps)
    };
    check(json!(-0.0), json!("NaN")).unwrap();
    assert!(
        check(json!(0.0), json!("NaN")).is_err(),
        "-0.0 must not match 0.0"
    );
    assert!(
        check(json!(0), json!("NaN")).is_err(),
        "-0.0 must not match 0"
    );
    assert!(check(json!(-0.0), json!(null)).is_err());
    assert!(check(json!(-0.0), json!("inf")).is_err());
    // serde_json keeps the sign of a negative-zero literal.
    let v: serde_json::Value = serde_json::from_str("-0.0").unwrap();
    assert_eq!(v.as_f64().unwrap().to_bits(), (-0.0f64).to_bits());
    // Writing -0.0 and then 0.0 stores two versions (bitwise dedup through JSON).
    let mut steps = base();
    steps.as_array_mut().unwrap().push(json!({"op": "write", "system_time": "2025-01-01T00:00:00Z", "rows": [
        {"series": "a", "value_time": "2025-01-01T00:00:00Z", "received_time": "2025-01-01T00:00:00Z", "values": {"value": 0.0}},
        {"series": "b", "value_time": "2025-01-01T00:00:00Z", "received_time": "2025-01-01T00:00:00Z", "values": {"value": "NaN"}}],
        "expect": {"stored": 1, "duplicates": 1}}));
    run(steps).unwrap();
}
