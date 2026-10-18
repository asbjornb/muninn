//! `query`: execute SPEC §9 step JSON through `muninn_core::wire`.
//!
//! Output, one line per step: `{"rows": [...]}` for reads (rows exactly as in a
//! scenario's `expect.rows`), the §4.5 result object for writes, `{"ok": true}` for catalog
//! steps, and `{"error": "<code>", "message": "..."}` for failures (exit code 1).

use std::io::BufRead;

use muninn_core::wire::{self, Outcome, Wire};

use crate::db::{Context, Db};
use crate::output::{self, Rows};
use crate::{CliError, QueryArgs, Res};

fn rows_json(rows: &Rows) -> Wire {
    Wire::Array(
        rows.rows
            .iter()
            .map(|r| output::json_object(rows, r))
            .collect(),
    )
}

fn error_json(code: &str, message: &str) -> Wire {
    Wire::Object(vec![
        ("error".into(), Wire::String(code.into())),
        ("message".into(), Wire::String(message.into())),
    ])
}

/// Runs one step; `Err` carries the JSON error object.
fn step(db: &Db, ctx: &Context, text: &str) -> Result<Wire, Wire> {
    let parsed = Wire::parse(text).map_err(|e| error_json("invalid_argument", &e.to_string()))?;
    let Wire::Object(fields) = parsed else {
        return Err(error_json(
            "invalid_argument",
            "a step must be a JSON object",
        ));
    };
    let op = match fields.iter().find(|(k, _)| k == "op") {
        Some((_, Wire::String(op))) => op.clone(),
        _ => return Err(error_json("invalid_argument", "a step needs a string `op`")),
    };
    let mut rest: Vec<(String, Wire)> = fields
        .into_iter()
        .filter(|(k, _)| k != "op" && k != "expect")
        .collect();
    if op == "write" && !rest.iter().any(|(k, _)| k == "system_time") {
        rest.push(("system_time".into(), Wire::String(ctx.now().to_string())));
    }
    match wire::execute(db, &op, &rest) {
        Ok(Outcome::Done) => Ok(Wire::Object(vec![("ok".into(), Wire::Bool(true))])),
        Ok(Outcome::Write(r)) => Ok(Wire::Object(vec![
            ("stored".into(), Wire::Number(r.stored.to_string())),
            ("duplicates".into(), Wire::Number(r.duplicates.to_string())),
            (
                "batch_already_committed".into(),
                Wire::Bool(r.batch_already_committed),
            ),
        ])),
        Ok(Outcome::Versions(res)) => {
            let deleted = op == "history" || op == "replay";
            Ok(rows_obj(rows_json(&output::versions(&res, deleted))))
        }
        Ok(Outcome::Grid(res)) => Ok(rows_obj(rows_json(&output::grid(&res)))),
        Ok(Outcome::Asof(res)) => Ok(rows_obj(rows_json(&output::asof(&res)))),
        Err(e) => Err(error_json(e.kind().name(), e.message())),
    }
}

fn rows_obj(rows: Wire) -> Wire {
    Wire::Object(vec![("rows".into(), rows)])
}

/// `query`.
pub fn run(ctx: &Context, a: &QueryArgs) -> Res<()> {
    let db = ctx.open()?;
    let mut failed = false;
    let mut emit = |text: &str| -> Res<()> {
        let out = match step(&db, ctx, text) {
            Ok(w) => w,
            Err(w) => {
                failed = true;
                w
            }
        };
        output::line(&out.to_string())
    };
    if a.step == "-" {
        for line in std::io::stdin().lock().lines() {
            let line = line?;
            if !line.trim().is_empty() {
                emit(&line)?;
            }
        }
    } else if let Some(path) = a.step.strip_prefix('@') {
        let text = std::fs::read_to_string(path)
            .map_err(|e| CliError::new(format!("io: cannot read {path}: {e}")))?;
        emit(&text)?;
    } else {
        emit(&a.step)?;
    }
    if failed {
        Err(CliError {
            message: String::new(),
            code: 1,
        })
    } else {
        Ok(())
    }
}
