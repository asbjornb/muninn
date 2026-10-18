//! Read results → Arrow record batches (timestamps as `timestamp[us, tz=UTC]`).

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Float64Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
    UInt32Builder, UInt64Builder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use muninn_core::api::{AsofRow, GridRow, ReadResult, VersionRow};
use muninn_core::{ColumnDef, ColumnType, Error, Result, Timestamp, Value};

/// The metadata key that carries the reproducibility receipt (SPEC §11).
pub(crate) const RECEIPT_KEY: &str = "muninn.receipt";

fn ts_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

fn ts_builder(n: usize) -> TimestampMicrosecondBuilder {
    TimestampMicrosecondBuilder::with_capacity(n).with_timezone("UTC")
}

/// Column builders, in output order.
struct Out {
    fields: Vec<Field>,
    arrays: Vec<ArrayRef>,
}

impl Out {
    fn new() -> Out {
        Out {
            fields: Vec::new(),
            arrays: Vec::new(),
        }
    }

    fn push(&mut self, name: &str, ty: DataType, nullable: bool, a: ArrayRef) {
        self.fields.push(Field::new(name, ty, nullable));
        self.arrays.push(a);
    }

    fn ts<'a>(
        &mut self,
        name: &str,
        nullable: bool,
        it: impl Iterator<Item = Option<Timestamp>> + 'a,
    ) {
        let mut b = ts_builder(0);
        for t in it {
            b.append_option(t.map(Timestamp::as_micros));
        }
        self.push(name, ts_type(), nullable, Arc::new(b.finish()));
    }

    fn finish(self, receipt: String) -> Result<RecordBatch> {
        let schema = Schema::new_with_metadata(
            self.fields,
            HashMap::from([(RECEIPT_KEY.to_owned(), receipt)]),
        );
        RecordBatch::try_new(Arc::new(schema), self.arrays)
            .map_err(|e| Error::internal(format!("building the result batch: {e}")))
    }
}

/// The version fields shared by every op (`r` is `None` for an `asof_join` miss). The
/// schema is fixed per op: `nullable` (only `asof_join`, whose right side may be missing)
/// never depends on the rows.
fn version_fields(
    out: &mut Out,
    columns: &[ColumnDef],
    rows: &[Option<&VersionRow>],
    with_deleted: bool,
    nullable: bool,
) {
    out.ts(
        "published_time",
        true,
        rows.iter().map(|r| r.and_then(|r| r.published_time)),
    );
    out.ts(
        "received_time",
        nullable,
        rows.iter().map(|r| r.map(|r| r.received_time)),
    );
    out.ts(
        "system_time",
        nullable,
        rows.iter().map(|r| r.map(|r| r.system_time)),
    );
    let mut seq = UInt64Builder::with_capacity(rows.len());
    for r in rows {
        seq.append_option(r.map(|r| r.seq));
    }
    out.push("seq", DataType::UInt64, nullable, Arc::new(seq.finish()));
    if with_deleted {
        let mut d = BooleanBuilder::with_capacity(rows.len());
        for r in rows {
            d.append_option(r.map(|r| r.deleted));
        }
        out.push("deleted", DataType::Boolean, false, Arc::new(d.finish()));
    }
    for (ci, c) in columns.iter().enumerate() {
        let cell = |r: &Option<&VersionRow>| r.and_then(|r| r.values.get(ci).cloned().flatten());
        let (ty, a): (DataType, ArrayRef) = match c.ty {
            ColumnType::F64 => {
                let mut b = Float64Builder::with_capacity(rows.len());
                for r in rows {
                    b.append_option(match cell(r) {
                        Some(Value::F64(x)) => Some(x),
                        _ => None,
                    });
                }
                (DataType::Float64, Arc::new(b.finish()))
            }
            ColumnType::I64 => {
                let mut b = Int64Builder::with_capacity(rows.len());
                for r in rows {
                    b.append_option(match cell(r) {
                        Some(Value::I64(x)) => Some(x),
                        _ => None,
                    });
                }
                (DataType::Int64, Arc::new(b.finish()))
            }
            ColumnType::Bool => {
                let mut b = BooleanBuilder::with_capacity(rows.len());
                for r in rows {
                    b.append_option(match cell(r) {
                        Some(Value::Bool(x)) => Some(x),
                        _ => None,
                    });
                }
                (DataType::Boolean, Arc::new(b.finish()))
            }
            ColumnType::String => {
                let mut b = StringBuilder::new();
                for r in rows {
                    match cell(r) {
                        Some(Value::String(s)) => b.append_value(s),
                        _ => b.append_null(),
                    }
                }
                (DataType::Utf8, Arc::new(b.finish()))
            }
            ColumnType::Timestamp => {
                let mut b = ts_builder(rows.len());
                for r in rows {
                    b.append_option(match cell(r) {
                        Some(Value::Timestamp(t)) => Some(t.as_micros()),
                        _ => None,
                    });
                }
                (ts_type(), Arc::new(b.finish()))
            }
        };
        out.push(&c.name, ty, true, a);
    }
}

fn has_column(columns: &[ColumnDef], name: &str) -> bool {
    columns.iter().any(|c| c.name == name)
}

/// The name of the series id output column: `series_id`, or `_series_id` if the output
/// has a value column called `series_id` (not reserved by SPEC §2.1). Column names match
/// `[a-z][a-z0-9_]*`, so `_series_id` can never clash with a value column.
pub(crate) fn id_column(columns: &[ColumnDef]) -> &'static str {
    if has_column(columns, "series_id") {
        "_series_id"
    } else {
        "series_id"
    }
}

/// `series` (name) and the series id column ([`id_column`]).
fn series_fields<'a>(
    out: &mut Out,
    columns: &[ColumnDef],
    it: impl Iterator<Item = (&'a str, u32)> + Clone,
) {
    let mut names = StringBuilder::new();
    let mut ids = UInt32Builder::new();
    for (n, id) in it {
        names.append_value(n);
        ids.append_value(id);
    }
    out.push("series", DataType::Utf8, false, Arc::new(names.finish()));
    out.push(
        id_column(columns),
        DataType::UInt32,
        false,
        Arc::new(ids.finish()),
    );
}

/// `latest` / `history` / `replay` rows.
pub(crate) fn versions_batch(
    res: &ReadResult<VersionRow>,
    with_deleted: bool,
    receipt: String,
) -> Result<RecordBatch> {
    let mut out = Out::new();
    series_fields(
        &mut out,
        &res.columns,
        res.rows.iter().map(|r| (&*r.series, r.series_id.0)),
    );
    out.ts(
        "value_time",
        false,
        res.rows.iter().map(|r| Some(r.value_time)),
    );
    let rows: Vec<Option<&VersionRow>> = res.rows.iter().map(Some).collect();
    version_fields(&mut out, &res.columns, &rows, with_deleted, false);
    out.finish(receipt)
}

/// `grid` rows: `frontier` first.
pub(crate) fn grid_batch(res: &ReadResult<GridRow>, receipt: String) -> Result<RecordBatch> {
    let mut out = Out::new();
    out.ts("frontier", false, res.rows.iter().map(|r| Some(r.frontier)));
    series_fields(
        &mut out,
        &res.columns,
        res.rows.iter().map(|r| (&*r.row.series, r.row.series_id.0)),
    );
    out.ts(
        "value_time",
        false,
        res.rows.iter().map(|r| Some(r.row.value_time)),
    );
    let rows: Vec<Option<&VersionRow>> = res.rows.iter().map(|r| Some(&r.row)).collect();
    version_fields(&mut out, &res.columns, &rows, false, false);
    out.finish(receipt)
}

/// `asof_join` rows in SPEC §6.5 order (the Python layer appends everything from `found`
/// on to the caller's left table).
pub(crate) fn asof_batch(res: &ReadResult<AsofRow>, receipt: String) -> Result<RecordBatch> {
    let mut out = Out::new();
    series_fields(
        &mut out,
        &res.columns,
        res.rows.iter().map(|r| (&*r.series, r.series_id.0)),
    );
    out.ts(
        "value_time",
        false,
        res.rows.iter().map(|r| Some(r.value_time)),
    );
    out.ts("frontier", false, res.rows.iter().map(|r| Some(r.frontier)));
    let mut found = BooleanBuilder::with_capacity(res.rows.len());
    for r in &res.rows {
        found.append_value(r.found());
    }
    out.push("found", DataType::Boolean, false, Arc::new(found.finish()));
    let rows: Vec<Option<&VersionRow>> = res.rows.iter().map(|r| r.right.as_ref()).collect();
    version_fields(&mut out, &res.columns, &rows, false, true);
    out.finish(receipt)
}
