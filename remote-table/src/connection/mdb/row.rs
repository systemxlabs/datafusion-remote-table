use crate::DFResult;
use arrow::array::{
    ArrayRef, BinaryBuilder, BinaryViewBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder,
    Int32Builder, Int64Builder, RecordBatch, RecordBatchOptions, StringBuilder, StringViewBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder, make_builder,
};
use arrow::datatypes::{DataType, Date32Type, SchemaRef, TimeUnit};
use chrono::{NaiveDate, NaiveTime, Timelike};
use datafusion_common::DataFusionError;
use datafusion_common::project_schema;
use odbc_api::CursorImpl;
use odbc_api::ResultSetMetadata;
use odbc_api::buffers::{BufferDesc, ColumnarDynBuffer};
use odbc_api::decimal_text_to_i128;
use odbc_api::handles::StatementImpl;

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
                    |v| { crate::connection::odbc_util::us_since_epoch(&v) }
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

// --- Columnar fast path helpers (mirrors dm/buffer.rs) -------------------------
// Used when the underlying mdbtools supports SQL_ATTR_ROW_ARRAY_SIZE /
// SQL_ATTR_ROW_BIND_TYPE. Older mdbtools (<1.0.1) does not, in which case
// the bind_buffer call below will fail and the row-by-row path is used.

#[allow(dead_code)]
fn contains_large_column(cursor: &mut CursorImpl<StatementImpl>) -> DFResult<bool> {
    let col_count = cursor
        .num_result_cols()
        .map_err(|e| DataFusionError::External(Box::new(e)))? as u16;
    for i in 1..=col_count {
        let col_type = cursor
            .col_data_type(i)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        if matches!(
            col_type,
            odbc_api::DataType::LongVarchar { length: _ }
                | odbc_api::DataType::WLongVarchar { length: _ }
                | odbc_api::DataType::LongVarbinary { length: _ }
        ) {
            return Ok(true);
        }
    }
    Ok(false)
}

#[allow(dead_code)] // see comment above contains_large_column
fn build_buffer_desc(
    field: &arrow::datatypes::Field,
    cursor: &mut CursorImpl<StatementImpl>,
    col_idx: usize,
) -> DFResult<BufferDesc> {
    let nullable = field.is_nullable();
    let odbc_col_idx = (col_idx + 1) as u16;
    match field.data_type() {
        DataType::Boolean => Ok(BufferDesc::Bit { nullable }),
        DataType::Int8 => Ok(BufferDesc::I8 { nullable }),
        DataType::Int16 => Ok(BufferDesc::I16 { nullable }),
        DataType::Int32 => Ok(BufferDesc::I32 { nullable }),
        DataType::Int64 => Ok(BufferDesc::I64 { nullable }),
        DataType::Float32 => Ok(BufferDesc::F32 { nullable }),
        DataType::Float64 => Ok(BufferDesc::F64 { nullable }),
        DataType::Decimal128(precision, _scale) => Ok(BufferDesc::Text {
            // precision digits + sign + decimal point
            max_str_len: *precision as usize + 2,
        }),
        DataType::Utf8 | DataType::Utf8View => {
            let column_size = cursor
                .col_data_type(odbc_col_idx)
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .column_size();
            let max_str_len = match column_size {
                Some(size) if size.get() > 0 => size.get() * 4,
                _ => 1024,
            };
            Ok(BufferDesc::Text { max_str_len })
        }
        DataType::FixedSizeBinary(size) => Ok(BufferDesc::Binary {
            max_bytes: *size as usize,
        }),
        DataType::Binary | DataType::BinaryView => {
            let column_size = cursor
                .col_data_type(odbc_col_idx)
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .column_size();
            let max_bytes = match column_size {
                Some(size) if size.get() > 0 => size.get(),
                _ => 1024,
            };
            Ok(BufferDesc::Binary { max_bytes })
        }
        DataType::Timestamp(_, _) => Ok(BufferDesc::Timestamp { nullable }),
        DataType::Date32 => Ok(BufferDesc::Date { nullable }),
        DataType::Time64(_) => {
            let display_size = cursor
                .col_data_type(odbc_col_idx)
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .display_size();
            let max_str_len = match display_size {
                Some(size) if size.get() > 0 => size.get() * 4,
                _ => 32,
            };
            Ok(BufferDesc::Text { max_str_len })
        }
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported data type to build buffer desc for mdb: {:?}",
            field.data_type()
        ))),
    }
}

macro_rules! handle_primitive_type {
    ($builder:expr, $field:expr, $builder_ty:ty, $nullable:expr, $value_ty:ty, $col_slice:expr, $convert:expr) => {{
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
        if $nullable {
            let values = $col_slice.as_nullable_slice::<$value_ty>().ok_or_else(|| {
                DataFusionError::Execution(format!("Failed to get nullable slice for {:?}", $field))
            })?;
            for value in values {
                match value {
                    Some(v) => builder.append_value($convert(v)?),
                    None => builder.append_null(),
                }
            }
        } else {
            let values = $col_slice.as_slice::<$value_ty>().ok_or_else(|| {
                DataFusionError::Execution(format!("Failed to get slice for {:?}", $field))
            })?;
            for value in values {
                builder.append_value($convert(value)?);
            }
        }
    }};
}

macro_rules! handle_text_view {
    ($builder:expr, $field:expr, $builder_ty:ty, $col_slice:expr, $convert:expr) => {{
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
        let values = $col_slice.as_text().ok_or_else(|| {
            DataFusionError::Execution(format!("Failed to get view for {:?}", $field))
        })?;
        for value in values.iter() {
            match value {
                Some(v) => {
                    // mdbtools returns text in the MDB file's code page
                    // (CP1252/CP936/etc.), not UTF-8. from_utf8_lossy keeps the
                    // printable bytes and substitutes U+FFFD for invalid ones.
                    let s = String::from_utf8_lossy(v);
                    builder.append_value($convert(s.as_ref())?);
                }
                None => {
                    builder.append_null();
                }
            }
        }
    }};
}

#[allow(dead_code)] // see comment above contains_large_column
fn buffer_to_batch(
    buffer: &ColumnarDynBuffer,
    table_schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
    chunk_size: usize,
) -> DFResult<RecordBatch> {
    let projected_schema = project_schema(table_schema, projection)?;

    let mut arrays = Vec::with_capacity(projected_schema.fields().len());
    for (col_idx, field) in table_schema.fields().iter().enumerate() {
        if !projections_contains(projection, col_idx) {
            continue;
        }
        let mut builder = make_builder(field.data_type(), chunk_size);
        let col_slice = buffer.column(col_idx);
        let nullable = field.is_nullable();
        match field.data_type() {
            DataType::Boolean => {
                handle_primitive_type!(
                    builder,
                    field,
                    BooleanBuilder,
                    nullable,
                    odbc_api::Bit,
                    col_slice,
                    |bit: &odbc_api::Bit| { Ok::<_, DataFusionError>(bit.as_bool()) }
                );
            }
            DataType::Int8 => {
                handle_primitive_type!(
                    builder,
                    field,
                    Int8Builder,
                    nullable,
                    i8,
                    col_slice,
                    |v: &i8| { Ok::<_, DataFusionError>(*v) }
                );
            }
            DataType::Int16 => {
                handle_primitive_type!(
                    builder,
                    field,
                    Int16Builder,
                    nullable,
                    i16,
                    col_slice,
                    |v: &i16| { Ok::<_, DataFusionError>(*v) }
                );
            }
            DataType::Int32 => {
                handle_primitive_type!(
                    builder,
                    field,
                    Int32Builder,
                    nullable,
                    i32,
                    col_slice,
                    |v: &i32| { Ok::<_, DataFusionError>(*v) }
                );
            }
            DataType::Int64 => {
                handle_primitive_type!(
                    builder,
                    field,
                    Int64Builder,
                    nullable,
                    i64,
                    col_slice,
                    |v: &i64| { Ok::<_, DataFusionError>(*v) }
                );
            }
            DataType::Float32 => {
                handle_primitive_type!(
                    builder,
                    field,
                    Float32Builder,
                    nullable,
                    f32,
                    col_slice,
                    |v: &f32| { Ok::<_, DataFusionError>(*v) }
                );
            }
            DataType::Float64 => {
                handle_primitive_type!(
                    builder,
                    field,
                    Float64Builder,
                    nullable,
                    f64,
                    col_slice,
                    |v: &f64| { Ok::<_, DataFusionError>(*v) }
                );
            }
            DataType::Decimal128(_, scale) => {
                handle_text_view!(
                    builder,
                    field,
                    Decimal128Builder,
                    col_slice,
                    |value: &str| {
                        Ok::<_, DataFusionError>(decimal_text_to_i128(
                            value.as_bytes(),
                            *scale as usize,
                        ))
                    }
                );
            }
            DataType::Utf8 => {
                let convert: for<'a> fn(&'a str) -> DFResult<&'a str> = |v| Ok(v);
                handle_text_view!(builder, field, StringBuilder, col_slice, convert);
            }
            DataType::Utf8View => {
                let convert: for<'a> fn(&'a str) -> DFResult<&'a str> = |v| Ok(v);
                handle_text_view!(builder, field, StringViewBuilder, col_slice, convert);
            }
            DataType::FixedSizeBinary(_) => {
                let builder = builder
                    .as_any_mut()
                    .downcast_mut::<FixedSizeBinaryBuilder>()
                    .unwrap_or_else(|| {
                        panic!("Failed to downcast builder to FixedSizeBinaryBuilder for {field:?}")
                    });
                let values = col_slice.as_binary().ok_or_else(|| {
                    DataFusionError::Execution(format!("Failed to get bin view for {field:?}"))
                })?;
                for value in values.iter() {
                    match value {
                        Some(v) => {
                            builder.append_value(v)?;
                        }
                        None => {
                            builder.append_null();
                        }
                    }
                }
            }
            DataType::Binary => {
                let builder = builder
                    .as_any_mut()
                    .downcast_mut::<BinaryBuilder>()
                    .unwrap_or_else(|| {
                        panic!("Failed to downcast builder to BinaryBuilder for {field:?}")
                    });
                let values = col_slice.as_binary().ok_or_else(|| {
                    DataFusionError::Execution(format!("Failed to get bin view for {field:?}"))
                })?;
                for value in values.iter() {
                    match value {
                        Some(v) => {
                            builder.append_value(v);
                        }
                        None => {
                            builder.append_null();
                        }
                    }
                }
            }
            DataType::BinaryView => {
                let builder = builder
                    .as_any_mut()
                    .downcast_mut::<BinaryViewBuilder>()
                    .unwrap_or_else(|| {
                        panic!("Failed to downcast builder to BinaryViewBuilder for {field:?}")
                    });
                let values = col_slice.as_binary().ok_or_else(|| {
                    DataFusionError::Execution(format!("Failed to get bin view for {field:?}"))
                })?;
                for value in values.iter() {
                    match value {
                        Some(v) => {
                            builder.append_value(v);
                        }
                        None => {
                            builder.append_null();
                        }
                    }
                }
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                handle_primitive_type!(
                    builder,
                    field,
                    TimestampMicrosecondBuilder,
                    nullable,
                    odbc_api::sys::Timestamp,
                    col_slice,
                    crate::connection::odbc_util::us_since_epoch
                );
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                handle_text_view!(
                    builder,
                    field,
                    Time64MicrosecondBuilder,
                    col_slice,
                    |value: &str| {
                        let nt = NaiveTime::parse_from_str(value, "%H:%M:%S%.f").map_err(|e| {
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
                handle_primitive_type!(
                    builder,
                    field,
                    Date32Builder,
                    nullable,
                    odbc_api::sys::Date,
                    col_slice,
                    |value: &odbc_api::sys::Date| {
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
                    "Unsupported field to build record batch for mdb: {field:?}"
                )));
            }
        }
        arrays.push(builder.finish());
    }
    let options = RecordBatchOptions::new().with_row_count(Some(buffer.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        projected_schema,
        arrays,
        &options,
    )?)
}
