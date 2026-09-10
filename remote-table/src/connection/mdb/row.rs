use crate::DFResult;
use arrow::array::{
    ArrayRef, BinaryBuilder, BinaryViewBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder,
    Int32Builder, Int64Builder, RecordBatch, RecordBatchOptions, StringBuilder, StringViewBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Date32Type, SchemaRef, TimeUnit};
use chrono::{NaiveDate, NaiveTime, Timelike};
use datafusion_common::DataFusionError;
use datafusion_common::project_schema;
use odbc_api::decimal_text_to_i128;

use crate::connection::projections_contains;

macro_rules! read_data {
    ($builder:expr, $field:expr, $builder_ty:ty, $row:expr, $col_idx:expr, $value_ty:ty, $convert:expr) => {{
        let builder = $builder
            .as_any_mut()
            .downcast_mut::<$builder_ty>()
            .unwrap_or_else(|| {
                panic!(
                    "Failed to downcast builder to {} for {:?}",
                    stringify!($builder_ty),
                    $field,
                )
            });
        let mut value = odbc_api::Nullable::<$value_ty>::null();
        $row.get_data($col_idx, &mut value).map_err(|e| {
            DataFusionError::Execution(format!("Failed to get value for field {:?}: {e:?}", $field))
        })?;
        let value = value.into_opt();
        match value {
            Some(v) => builder.append_value($convert(v)?),
            None => builder.append_null(),
        }
    }};
}

macro_rules! read_text {
    ($builder:expr, $field:expr, $builder_ty:ty, $row:expr, $col_idx:expr, $buf:expr, $convert:expr) => {{
        let builder = $builder
            .as_any_mut()
            .downcast_mut::<$builder_ty>()
            .unwrap_or_else(|| {
                panic!(
                    "Failed to downcast builder to {} for {:?}",
                    stringify!($builder_ty),
                    $field,
                )
            });
        $buf.clear();
        let is_not_null = $row.get_text($col_idx, $buf).map_err(|e| {
            DataFusionError::Execution(format!("Failed to get value for field {:?}: {e:?}", $field))
        })?;
        if is_not_null {
            // mdbtools returns text in the MDB file's code page (CP1252/CP936/etc.),
            // not UTF-8. from_utf8_lossy keeps the printable bytes and substitutes
            // U+FFFD for invalid ones, so non-ASCII cells don't abort the batch.
            let value = String::from_utf8_lossy($buf).into_owned();
            builder.append_value($convert(value)?);
        } else {
            builder.append_null();
        }
    }};
}

pub(super) fn append_row_to_builders(
    builders: &mut [Box<dyn arrow::array::ArrayBuilder>],
    mut row: odbc_api::CursorRow,
    table_schema: &SchemaRef,
) -> DFResult<()> {
    // Reuse one Vec per ODBC cell type across all columns/rows in this call so
    // we don't allocate a new buffer for every cell.
    let mut text_buf: Vec<u8> = Vec::new();
    let mut binary_buf: Vec<u8> = Vec::new();
    for (idx, field) in table_schema.fields().iter().enumerate() {
        let builder = &mut builders[idx];
        let odbc_col_idx = (idx + 1) as u16;
        match field.data_type() {
            DataType::Boolean => {
                read_data!(
                    builder,
                    field,
                    BooleanBuilder,
                    row,
                    odbc_col_idx,
                    odbc_api::Bit,
                    |v: odbc_api::Bit| { Ok::<_, DataFusionError>(v.as_bool()) }
                );
            }
            DataType::Int8 => {
                read_data!(
                    builder,
                    field,
                    Int8Builder,
                    row,
                    odbc_col_idx,
                    i8,
                    |v: i8| Ok::<_, DataFusionError>(v)
                );
            }
            DataType::Int16 => {
                read_data!(
                    builder,
                    field,
                    Int16Builder,
                    row,
                    odbc_col_idx,
                    i16,
                    |v: i16| Ok::<_, DataFusionError>(v)
                );
            }
            DataType::Int32 => {
                read_data!(
                    builder,
                    field,
                    Int32Builder,
                    row,
                    odbc_col_idx,
                    i32,
                    |v: i32| Ok::<_, DataFusionError>(v)
                );
            }
            DataType::Int64 => {
                read_data!(
                    builder,
                    field,
                    Int64Builder,
                    row,
                    odbc_col_idx,
                    i64,
                    |v: i64| Ok::<_, DataFusionError>(v)
                );
            }
            DataType::Float32 => {
                read_data!(
                    builder,
                    field,
                    Float32Builder,
                    row,
                    odbc_col_idx,
                    f32,
                    |v: f32| Ok::<_, DataFusionError>(v)
                );
            }
            DataType::Float64 => {
                read_data!(
                    builder,
                    field,
                    Float64Builder,
                    row,
                    odbc_col_idx,
                    f64,
                    |v: f64| Ok::<_, DataFusionError>(v)
                );
            }
            DataType::Decimal128(_precision, scale) => {
                read_text!(
                    builder,
                    field,
                    Decimal128Builder,
                    row,
                    odbc_col_idx,
                    &mut text_buf,
                    |v: String| {
                        Ok::<_, DataFusionError>(decimal_text_to_i128(
                            v.as_bytes(),
                            *scale as usize,
                        ))
                    }
                );
            }
            DataType::Utf8 => {
                read_text!(
                    builder,
                    field,
                    StringBuilder,
                    row,
                    odbc_col_idx,
                    &mut text_buf,
                    |v: String| Ok::<_, DataFusionError>(v)
                );
            }
            DataType::Utf8View => {
                read_text!(
                    builder,
                    field,
                    StringViewBuilder,
                    row,
                    odbc_col_idx,
                    &mut text_buf,
                    |v: String| Ok::<_, DataFusionError>(v)
                );
            }
            DataType::FixedSizeBinary(_) => {
                let builder = builder
                    .as_any_mut()
                    .downcast_mut::<FixedSizeBinaryBuilder>()
                    .unwrap_or_else(|| {
                        panic!("Failed to downcast builder to FixedSizeBinaryBuilder for {field:?}")
                    });
                binary_buf.clear();
                let is_not_null = row.get_binary(odbc_col_idx, &mut binary_buf).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to get value for field {:?}: {e:?}",
                        field
                    ))
                })?;
                if is_not_null {
                    builder.append_value(&binary_buf)?;
                } else {
                    builder.append_null();
                }
            }
            DataType::Binary => {
                let builder = builder
                    .as_any_mut()
                    .downcast_mut::<BinaryBuilder>()
                    .unwrap_or_else(|| {
                        panic!("Failed to downcast builder to BinaryBuilder for {field:?}")
                    });
                binary_buf.clear();
                let is_not_null = row.get_binary(odbc_col_idx, &mut binary_buf).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to get value for field {:?}: {e:?}",
                        field
                    ))
                })?;
                if is_not_null {
                    builder.append_value(&binary_buf);
                } else {
                    builder.append_null();
                }
            }
            DataType::BinaryView => {
                let builder = builder
                    .as_any_mut()
                    .downcast_mut::<BinaryViewBuilder>()
                    .unwrap_or_else(|| {
                        panic!("Failed to downcast builder to BinaryViewBuilder for {field:?}")
                    });
                binary_buf.clear();
                let is_not_null = row.get_binary(odbc_col_idx, &mut binary_buf).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to get value for field {:?}: {e:?}",
                        field
                    ))
                })?;
                if is_not_null {
                    builder.append_value(&binary_buf);
                } else {
                    builder.append_null();
                }
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                read_data!(
                    builder,
                    field,
                    TimestampMicrosecondBuilder,
                    row,
                    odbc_col_idx,
                    odbc_api::sys::Timestamp,
                    |v| { crate::connection::us_since_epoch(&v) }
                );
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                read_text!(
                    builder,
                    field,
                    Time64MicrosecondBuilder,
                    row,
                    odbc_col_idx,
                    &mut text_buf,
                    |value: String| {
                        let nt = NaiveTime::parse_from_str(&value, "%H:%M:%S%.f").map_err(|e| {
                            DataFusionError::Execution(format!("Failed to parse time: {e:?}"))
                        })?;
                        Ok::<_, DataFusionError>(
                            nt.num_seconds_from_midnight() as i64 * 1_000_000
                                + (nt.nanosecond() / 1000) as i64,
                        )
                    }
                );
            }
            DataType::Date32 => {
                read_data!(
                    builder,
                    field,
                    Date32Builder,
                    row,
                    odbc_col_idx,
                    odbc_api::sys::Date,
                    |value: odbc_api::sys::Date| {
                        let date = NaiveDate::from_ymd_opt(
                            value.year as i32,
                            value.month as u32,
                            value.day as u32,
                        )
                        .ok_or_else(|| {
                            DataFusionError::Execution(format!("Invalid date: {value:?}"))
                        })?;
                        Ok::<_, DataFusionError>(Date32Type::from_naive_date(date))
                    }
                );
            }
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported field type for mdb row conversion: {field:?}"
                )));
            }
        }
    }
    Ok(())
}

pub(super) fn finish_batch(
    builders: Vec<Box<dyn arrow::array::ArrayBuilder>>,
    table_schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
    row_count: usize,
) -> DFResult<RecordBatch> {
    let projected_schema = project_schema(table_schema, projection)?;

    let arrays = builders
        .into_iter()
        .enumerate()
        .filter(|(idx, _)| projections_contains(projection, *idx))
        .map(|(_, mut builder)| builder.finish())
        .collect::<Vec<ArrayRef>>();

    let options = RecordBatchOptions::new().with_row_count(Some(row_count));
    Ok(RecordBatch::try_new_with_options(
        projected_schema,
        arrays,
        &options,
    )?)
}
