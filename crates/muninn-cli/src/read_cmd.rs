//! Read commands: `latest`, `history`, `replay`, `grid`, `asof-join`.

use muninn_core::api::{
    AsofJoinRequest, AsofLeftRow, GridRequest, GridWindow, LatestRequest, ReadContext,
    ReplayRequest,
};
use muninn_core::{Clock, Duration, Reader, TimeRange, Timestamp};

use crate::catalog_cmd::parse_clock;
use crate::db::{Context, opt_ts, parse_ts};
use crate::input::open_input;
use crate::output;
use crate::{AsofArgs, CliError, GridArgs, LatestArgs, ReadCommon, ReplayArgs, Res};

/// Most frontiers a `--frontier-range` may expand to.
const MAX_FRONTIERS: usize = 1_000_000;

fn clock(c: &Option<String>) -> Res<Option<Clock>> {
    c.as_deref().map(parse_clock).transpose()
}

fn range(c: &ReadCommon) -> Res<TimeRange> {
    Ok(TimeRange::new(
        opt_ts(&c.from, "--from")?,
        opt_ts(&c.to, "--to")?,
    ))
}

fn series(c: &ReadCommon) -> Res<Vec<String>> {
    if c.series.iter().any(String::is_empty) {
        return Err(CliError::new(
            "invalid_name: --series needs a non-empty series name (omit it to read every series)",
        ));
    }
    Ok(c.series.clone())
}

fn duration(s: &str, what: &str) -> Res<Duration> {
    Duration::parse(s.trim()).map_err(|e| CliError::new(format!("invalid_range: {what}: {e}")))
}

/// `latest` / `history`.
pub fn latest(ctx: &Context, a: &LatestArgs, history: bool) -> Res<()> {
    let req = LatestRequest {
        table: a.common.table.clone(),
        series: series(&a.common)?,
        range: range(&a.common)?,
        context: ReadContext {
            clock: clock(&a.common.clock)?,
            frontier: opt_ts(&a.frontier, "--frontier")?,
            system: opt_ts(&a.common.system, "--system")?,
        },
    };
    let db = ctx.open()?;
    let res = if history {
        db.history(&req)?
    } else {
        db.latest(&req)?
    };
    output::print(&output::versions(&res, history), a.common.format.resolve())
}

/// `replay`.
pub fn replay(ctx: &Context, a: &ReplayArgs) -> Res<()> {
    let req = ReplayRequest {
        table: a.common.table.clone(),
        series: series(&a.common)?,
        range: range(&a.common)?,
        clock: clock(&a.common.clock)?,
        system: opt_ts(&a.common.system, "--system")?,
        arrival: TimeRange::new(opt_ts(&a.since, "--since")?, opt_ts(&a.until, "--until")?),
    };
    let db = ctx.open()?;
    let res = db.replay(&req)?;
    output::print(&output::versions(&res, true), a.common.format.resolve())
}

/// Expands `start,end,step` (end inclusive).
pub fn frontier_range(spec: &str) -> Res<Vec<Timestamp>> {
    let parts: Vec<&str> = spec.split(',').collect();
    let [start, end, step] = parts[..] else {
        return Err(CliError::new(format!(
            "--frontier-range {spec:?} must be START,END,STEP (e.g. \
             2025-01-01T00:00:00Z,2025-01-02T00:00:00Z,1h)"
        )));
    };
    let (start, end) = (
        parse_ts(start.trim(), "--frontier-range start")?,
        parse_ts(end.trim(), "--frontier-range end")?,
    );
    let step = duration(step, "--frontier-range step")?;
    if step.as_micros() <= 0 {
        return Err(CliError::new(
            "invalid_range: --frontier-range step must be positive",
        ));
    }
    if start > end {
        return Err(CliError::new(
            "invalid_range: --frontier-range start is after its end",
        ));
    }
    let count = (end.as_micros() - start.as_micros()) / step.as_micros() + 1;
    if count as u128 > MAX_FRONTIERS as u128 {
        return Err(CliError::new(format!(
            "--frontier-range expands to {count} frontiers (at most {MAX_FRONTIERS})"
        )));
    }
    let mut out = Vec::with_capacity(count as usize);
    let mut t = Some(start);
    while let Some(f) = t.filter(|f| *f <= end) {
        out.push(f);
        t = f.checked_add(step);
    }
    Ok(out)
}

/// `grid`.
pub fn grid(ctx: &Context, a: &GridArgs) -> Res<()> {
    let mut frontiers = a
        .frontiers
        .iter()
        .filter(|s| !s.is_empty())
        .map(|s| parse_ts(s.trim(), "--frontiers"))
        .collect::<Res<Vec<_>>>()?;
    if let Some(spec) = &a.frontier_range {
        frontiers.extend(frontier_range(spec)?);
    }
    if a.frontiers.is_empty() && a.frontier_range.is_none() {
        return Err(CliError::new(
            "grid needs --frontiers T1,T2,... and/or --frontier-range START,END,STEP",
        ));
    }
    let abs = range(&a.common)?;
    let window = match &a.window {
        None => GridWindow::Absolute(abs),
        Some(_) if abs != TimeRange::ALL => {
            return Err(CliError::new(
                "invalid_range: give either --window or --from/--to, not both",
            ));
        }
        Some(w) => {
            let (s, e) = w.split_once(',').ok_or_else(|| {
                CliError::new(format!("--window {w:?} must be START,END (e.g. 0h,36h)"))
            })?;
            GridWindow::Relative {
                start: duration(s, "--window start")?,
                end: duration(e, "--window end")?,
            }
        }
    };
    let req = GridRequest {
        table: a.common.table.clone(),
        series: series(&a.common)?,
        frontiers,
        window,
        clock: clock(&a.common.clock)?,
        system: opt_ts(&a.common.system, "--system")?,
    };
    let db = ctx.open()?;
    let res = db.grid(&req)?;
    output::print(&output::grid(&res), a.common.format.resolve())
}

/// `asof-join`.
pub fn asof(ctx: &Context, a: &AsofArgs) -> Res<()> {
    let reader = open_input(a.left.as_deref())?;
    let mut r = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(reader);
    let header: Vec<String> = r
        .headers()
        .map_err(|e| CliError::new(format!("line 1: cannot read the CSV header: {e}")))?
        .iter()
        .map(str::to_owned)
        .collect();
    let pos = |name: &str| {
        header.iter().position(|h| h == name).ok_or_else(|| {
            CliError::new(format!(
                "line 1: the left CSV needs columns series, value_time, frontier (missing {name})"
            ))
        })
    };
    let (si, vi, fi) = (pos("series")?, pos("value_time")?, pos("frontier")?);
    let mut left = Vec::new();
    for rec in r.records() {
        let rec = rec.map_err(|e| {
            let line = e.position().map_or(0, |p| p.line());
            CliError::new(format!("line {line}: {e}"))
        })?;
        let line = rec.position().map_or(0, |p| p.line());
        let t = |i: usize, name: &str| {
            a.time_format.parse(&rec[i]).map_err(|e| {
                CliError::new(format!(
                    "line {line}, column {name:?}: invalid_timestamp: {e}"
                ))
            })
        };
        left.push(AsofLeftRow {
            series: rec[si].to_owned(),
            value_time: t(vi, "value_time")?,
            frontier: t(fi, "frontier")?,
        });
    }
    let req = AsofJoinRequest {
        table: a.table.clone(),
        clock: clock(&a.clock)?,
        system: opt_ts(&a.system, "--system")?,
        left,
    };
    let db = ctx.open()?;
    let res = db.asof_join(&req)?;
    output::print(&output::asof(&res), a.format.resolve())
}
