//! `muninn`: the command-line tool for Muninn databases on a local directory.
//!
//! See `docs/cli.md` for a walkthrough and `spec/DUMP.md` for the dump format.

#![forbid(unsafe_code)]

mod catalog_cmd;
mod db;
mod dump;
mod input;
mod ops;
mod output;
mod query;
mod read_cmd;
mod timefmt;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand, ValueEnum};

use crate::output::Format;
use crate::timefmt::TimeFormat;

/// A CLI failure: a message for stderr and an exit code.
#[derive(Debug)]
pub struct CliError {
    /// What went wrong (printed after `error: `).
    pub message: String,
    /// Process exit code.
    pub code: u8,
}

impl CliError {
    /// A failure with exit code 1.
    pub fn new(message: impl Into<String>) -> CliError {
        CliError {
            message: message.into(),
            code: 1,
        }
    }
}

impl From<muninn_core::Error> for CliError {
    fn from(e: muninn_core::Error) -> CliError {
        CliError::new(format!("{}: {}", e.kind(), e.message()))
    }
}

impl From<std::io::Error> for CliError {
    fn from(e: std::io::Error) -> CliError {
        CliError::new(format!("io: {e}"))
    }
}

/// Result type of every command.
pub type Res<T> = Result<T, CliError>;

/// Muninn: a point-in-time-correct time series database.
#[derive(Parser, Debug)]
#[command(name = "muninn", version, about, propagate_version = true)]
pub struct Cli {
    /// Database directory (or set MUNINN_DB).
    #[arg(long, global = true, env = "MUNINN_DB", value_name = "DIR")]
    pub db: Option<PathBuf>,

    /// Writer clock reading used for catalog changes, writes and `--received-time now`
    /// (default: the system clock). The engine still applies system_time = max(clock, last).
    #[arg(
        long,
        global = true,
        env = "MUNINN_WRITER_CLOCK",
        value_name = "TIMESTAMP"
    )]
    pub writer_clock: Option<String>,

    /// The command.
    #[command(subcommand)]
    pub command: Command,
}

/// Commands.
#[derive(Subcommand, Debug)]
pub enum Command {
    /// Create a new, empty database directory.
    Init {
        /// Directory to create (must not exist or be empty).
        dir: PathBuf,
    },
    /// Tables: create, add-column, list, show.
    #[command(subcommand)]
    Table(TableCmd),
    /// Series: create, list.
    #[command(subcommand)]
    Series(SeriesCmd),
    /// Write versions from CSV or NDJSON (stdin or a file).
    Write(WriteArgs),
    /// Latest-known value of every point as of a frontier (SPEC §6.1).
    Latest(LatestArgs),
    /// Every version of every point, tombstones included (SPEC §6.2).
    History(LatestArgs),
    /// Versions in arrival (knowledge) order with t_clock in (since, until] (SPEC §6.3).
    Replay(ReplayArgs),
    /// `latest` at many frontiers at once (SPEC §6.4).
    Grid(GridArgs),
    /// Per-row frontier lookup for left rows read from CSV (SPEC §6.5).
    AsofJoin(AsofArgs),
    /// Execute SPEC §9 step JSON (a single object, `@file`, or `-` for NDJSON on stdin).
    Query(QueryArgs),
    /// Merge each partition's segments into one (never changes a result).
    Compact {
        /// Only this table.
        #[arg(long)]
        table: Option<String>,
    },
    /// Delete segment files no committed state references.
    Gc {
        /// Only delete files older than this (duration, e.g. 1h, 0s).
        #[arg(long, default_value = "1h")]
        grace: String,
    },
    /// Tables, series counts, segments, bytes and log version.
    Info(FormatArg),
    /// Check every segment, checksum, footer and the log; non-zero exit on problems.
    Verify,
    /// Export the whole database to a portable, text-based dump directory (spec/DUMP.md).
    Dump {
        /// Output directory (must not exist or be empty).
        out: PathBuf,
    },
    /// Recreate a database from a dump, identical in every read result.
    Restore {
        /// Dump directory written by `muninn dump`.
        dump: PathBuf,
        /// New database directory (must not exist or be empty).
        new_db: PathBuf,
        /// Versions per restore commit (approximate: an original batch is never split).
        #[arg(long, default_value_t = 100_000)]
        chunk_rows: usize,
        /// Skip compacting the restored database at the end.
        #[arg(long)]
        no_compact: bool,
    },
}

/// `--format` for commands that print rows.
#[derive(Args, Debug, Clone, Copy)]
pub struct FormatArg {
    /// Output format (default: table on a terminal, csv otherwise).
    #[arg(long, value_enum)]
    pub format: Option<Format>,
}

/// Table subcommands.
#[derive(Subcommand, Debug)]
pub enum TableCmd {
    /// Create a table.
    Create {
        /// Table name, [a-z][a-z0-9_]*.
        name: String,
        /// A value column `name:type` (type: f64, i64, bool, string, timestamp); repeatable.
        #[arg(long = "column", value_name = "NAME:TYPE")]
        columns: Vec<String>,
        /// Default read clock: received, published or system.
        #[arg(long, default_value = "received")]
        clock: String,
        /// Physical partitioning: day, month or year.
        #[arg(long, default_value = "day")]
        partition: String,
    },
    /// Append a column (existing versions read it as null).
    AddColumn {
        /// Table name.
        table: String,
        /// The column, `name:type`.
        #[arg(value_name = "NAME:TYPE")]
        column: String,
    },
    /// List tables.
    List(FormatArg),
    /// Show a table's columns and series.
    Show {
        /// Table name.
        name: String,
        /// Output format.
        #[command(flatten)]
        format: FormatArg,
    },
}

/// Series subcommands.
#[derive(Subcommand, Debug)]
pub enum SeriesCmd {
    /// Create a series.
    Create {
        /// Series name (any non-empty string, unique in the database).
        name: String,
        /// Owning table.
        #[arg(long)]
        table: String,
        /// A descriptive tag `key=value`; repeatable.
        #[arg(long = "tag", value_name = "KEY=VALUE")]
        tags: Vec<String>,
        /// IANA timezone (metadata only).
        #[arg(long)]
        timezone: Option<String>,
        /// Nominal resolution, e.g. 1h (metadata only).
        #[arg(long)]
        resolution: Option<String>,
    },
    /// Create many series from a CSV or NDJSON file, all in one atomic commit.
    ///
    /// CSV: a header with `name` (required), optional `table`, `timezone`, `resolution`;
    /// every other column is a tag (an empty field = no such tag). NDJSON: one object per
    /// line with `name`, `table`, optional `tags` (object of strings), `timezone`,
    /// `resolution`. Either all series are created or, on the first bad line, none.
    Import {
        /// Input file; `-` or omitted = stdin.
        file: Option<PathBuf>,
        /// Input format (default: from the file extension, else csv).
        #[arg(long, value_enum)]
        input: Option<InputFormat>,
        /// Table for lines that do not name one.
        #[arg(long)]
        table: Option<String>,
    },
    /// List series.
    List {
        /// Only series of this table.
        #[arg(long)]
        table: Option<String>,
        /// Only series with this tag `key=value`; repeatable (all must match).
        #[arg(long = "tag", value_name = "KEY=VALUE")]
        tags: Vec<String>,
        /// Output format.
        #[command(flatten)]
        format: FormatArg,
    },
}

/// Input file format of `write`.
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputFormat {
    /// Comma-separated values with a header row.
    Csv,
    /// One JSON object per line.
    Ndjson,
}

/// `write` arguments.
#[derive(Args, Debug)]
pub struct WriteArgs {
    /// Input file; `-` or omitted = stdin.
    pub file: Option<PathBuf>,
    /// Input format (default: from the file extension, else csv).
    #[arg(long, value_enum)]
    pub input: Option<InputFormat>,
    /// Batch id: the import is a no-op if it was committed before. With --chunk-rows,
    /// chunk k gets the id `<ID>/<k>`.
    #[arg(long)]
    pub batch_id: Option<String>,
    /// received_time for rows that have none: `now` (one reading for the whole import)
    /// or a timestamp.
    #[arg(long, value_name = "now|TIMESTAMP")]
    pub received_time: Option<String>,
    /// Split the input into batches of this many rows (atomic per chunk, not per import).
    #[arg(long, value_name = "N")]
    pub chunk_rows: Option<usize>,
    /// How timestamps in the input are written.
    #[arg(long, value_enum, default_value = "rfc3339")]
    pub time_format: TimeFormat,
}

/// Arguments shared by the reads.
#[derive(Args, Debug, Clone)]
pub struct ReadCommon {
    /// Table to read.
    #[arg(long)]
    pub table: String,
    /// Series filter: one exact series name per flag (repeat for several; names may
    /// contain commas); default every series of the table.
    #[arg(long = "series", value_name = "NAME")]
    pub series: Vec<String>,
    /// Value range start (inclusive).
    #[arg(long, value_name = "TIMESTAMP")]
    pub from: Option<String>,
    /// Value range end (exclusive).
    #[arg(long, value_name = "TIMESTAMP")]
    pub to: Option<String>,
    /// Clock: received, published or system (default: the table's).
    #[arg(long)]
    pub clock: Option<String>,
    /// System bound S, inclusive: only versions committed by then.
    #[arg(long, value_name = "TIMESTAMP")]
    pub system: Option<String>,
    /// Output format.
    #[command(flatten)]
    pub format: FormatArg,
}

/// `latest` / `history` arguments.
#[derive(Args, Debug)]
pub struct LatestArgs {
    /// Shared read arguments.
    #[command(flatten)]
    pub common: ReadCommon,
    /// Frontier F on the clock, inclusive (default: unbounded).
    #[arg(long, value_name = "TIMESTAMP")]
    pub frontier: Option<String>,
}

/// `replay` arguments.
#[derive(Args, Debug)]
pub struct ReplayArgs {
    /// Shared read arguments.
    #[command(flatten)]
    pub common: ReadCommon,
    /// Arrival lower bound on t_clock, exclusive.
    #[arg(long, value_name = "TIMESTAMP")]
    pub since: Option<String>,
    /// Arrival upper bound on t_clock, inclusive.
    #[arg(long, value_name = "TIMESTAMP")]
    pub until: Option<String>,
}

/// `grid` arguments.
#[derive(Args, Debug)]
pub struct GridArgs {
    /// Shared read arguments (`--from/--to` give an absolute window).
    #[command(flatten)]
    pub common: ReadCommon,
    /// Frontiers (comma-separated and/or repeated).
    #[arg(long, value_delimiter = ',', value_name = "TIMESTAMP")]
    pub frontiers: Vec<String>,
    /// Frontiers `start,end,step`: start, start+step, … up to end inclusive.
    #[arg(long, value_name = "START,END,STEP")]
    pub frontier_range: Option<String>,
    /// Relative value window `start,end` (durations): [F + start, F + end) per frontier.
    #[arg(long, value_name = "START,END", allow_hyphen_values = true)]
    pub window: Option<String>,
}

/// `asof-join` arguments.
#[derive(Args, Debug)]
pub struct AsofArgs {
    /// Right table.
    #[arg(long)]
    pub table: String,
    /// Left rows: CSV with columns series,value_time,frontier; `-` or omitted = stdin.
    #[arg(long)]
    pub left: Option<PathBuf>,
    /// Clock (default: the table's).
    #[arg(long)]
    pub clock: Option<String>,
    /// System bound S, inclusive.
    #[arg(long, value_name = "TIMESTAMP")]
    pub system: Option<String>,
    /// How timestamps in the left CSV are written.
    #[arg(long, value_enum, default_value = "rfc3339")]
    pub time_format: TimeFormat,
    /// Output format.
    #[command(flatten)]
    pub format: FormatArg,
}

/// `query` arguments.
#[derive(Args, Debug)]
pub struct QueryArgs {
    /// A step JSON object, `@path` to read one from a file, or `-` for one step per line
    /// on stdin.
    pub step: String,
}

fn run(cli: Cli) -> Res<()> {
    let ctx = db::Context::new(cli.db, cli.writer_clock)?;
    match cli.command {
        Command::Init { dir } => {
            db::init(&dir)?;
            output::line(&format!("initialised {}", dir.display()))
        }
        Command::Table(cmd) => catalog_cmd::table(&ctx, cmd),
        Command::Series(cmd) => catalog_cmd::series(&ctx, cmd),
        Command::Write(args) => input::write(&ctx, &args),
        Command::Latest(args) => read_cmd::latest(&ctx, &args, false),
        Command::History(args) => read_cmd::latest(&ctx, &args, true),
        Command::Replay(args) => read_cmd::replay(&ctx, &args),
        Command::Grid(args) => read_cmd::grid(&ctx, &args),
        Command::AsofJoin(args) => read_cmd::asof(&ctx, &args),
        Command::Query(args) => query::run(&ctx, &args),
        Command::Compact { table } => ops::compact(&ctx, table.as_deref()),
        Command::Gc { grace } => ops::gc(&ctx, &grace),
        Command::Info(f) => ops::info(&ctx, f),
        Command::Verify => ops::verify(&ctx),
        Command::Dump { out } => dump::dump(&ctx, &out),
        Command::Restore {
            dump,
            new_db,
            chunk_rows,
            no_compact,
        } => dump::restore(&ctx, &dump, &new_db, chunk_rows, !no_compact),
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            if !e.message.is_empty() {
                eprintln!("error: {}", e.message);
            }
            ExitCode::from(e.code)
        }
    }
}
