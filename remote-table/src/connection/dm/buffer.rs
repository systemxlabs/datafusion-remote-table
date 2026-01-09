use crate::DFResult;
use crate::connection::{
    just_deref, ms_since_epoch, ns_since_epoch, projections_contains, seconds_since_epoch,
    us_since_epoch,
};
use chrono::{NaiveDate, NaiveTime, Timelike};
use datafusion::arrow::array::{
    BinaryBuilder, BinaryViewBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder,
    Int32Builder, Int64Builder, RecordBatch, RecordBatchOptions, StringBuilder, StringViewBuilder,
    Time32MillisecondBuilder, Time32SecondBuilder, Time64MicrosecondBuilder,
    TimestampMicrosecondBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
    TimestampSecondBuilder, make_builder,
};
use datafusion::arrow::datatypes::{DataType, Date32Type, Field, SchemaRef, TimeUnit};
use datafusion::common::{DataFusionError, project_schema};
use odbc_api::buffers::{BufferDesc, ColumnarAnyBuffer};
use odbc_api::handles::StatementImpl;
use odbc_api::{Bit, CursorImpl, ResultSetMetadata, decimal_text_to_i128};

pub(crate) fn build_buffer_desc(
    field: &Field,
    cursor: &mut CursorImpl<StatementImpl>,
    col_idx: usize,
) -> DFResult<BufferDesc> {
    let nullable = field.is_nullable();
    match field.data_type() {
        DataType::Boolean => Ok(BufferDesc::Bit { nullable }),
        DataType::Int8 => Ok(BufferDesc::I8 { nullable }),
        DataType::Int16 => Ok(BufferDesc::I16 { nullable }),
        DataType::Int32 => Ok(BufferDesc::I32 { nullable }),
        DataType::Int64 => Ok(BufferDesc::I64 { nullable }),
        DataType::Float32 => Ok(BufferDesc::F32 { nullable }),
        DataType::Float64 => Ok(BufferDesc::F64 { nullable }),
        DataType::Decimal128(precision, _scale) => {
            Ok(BufferDesc::Text {
                // Must be able to hold num precision digits a sign and a decimal point
                max_str_len: *precision as usize + 2,
            })
        }
        DataType::Utf8 | DataType::Utf8View => {
            let column_size = cursor
                .col_data_type(col_idx as u16 + 1)
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .column_size()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!("Failed to get column size for {field:?}"))
                })?
                .get();
            Ok(BufferDesc::Text {
                max_str_len: column_size * 4,
            })
        }
        DataType::FixedSizeBinary(size) => Ok(BufferDesc::Binary {
            length: *size as usize,
        }),
        DataType::Binary | DataType::BinaryView => {
            let column_size = cursor
                .col_data_type(col_idx as u16 + 1)
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .column_size()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!("Failed to get column size for {field:?}"))
                })?
                .get();
            Ok(BufferDesc::Binary {
                length: column_size,
            })
        }
        DataType::Timestamp(_, _) => Ok(BufferDesc::Timestamp { nullable }),
        DataType::Date32 => Ok(BufferDesc::Date { nullable }),
        DataType::Time32(_) | DataType::Time64(_) => {
            let display_size = cursor
                .col_data_type(col_idx as u16 + 1)
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .display_size()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!("Failed to get display size for {field:?}"))
                })?
                .get();
            Ok(BufferDesc::Text {
                max_str_len: display_size * 4,
            })
        }
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported data type to build buffer desc: {:?}",
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
        let values = $col_slice.as_text_view().ok_or_else(|| {
            DataFusionError::Execution(format!("Failed to get view for {:?}", $field))
        })?;
        for value in values.iter() {
            match value {
                Some(v) => {
                    let s = std::str::from_utf8(v).map_err(|_| {
                        DataFusionError::Execution(format!("Invalid UTF-8 string: {v:?}"))
                    })?;
                    builder.append_value($convert(s)?);
                }
                None => {
                    builder.append_null();
                }
            }
        }
    }};
}

pub(crate) fn buffer_to_batch(
    buffer: &ColumnarAnyBuffer,
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
                    Bit,
                    col_slice,
                    |bit: &Bit| Ok::<_, DataFusionError>(bit.as_bool())
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
                    just_deref
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
                    just_deref
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
                    just_deref
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
                    just_deref
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
                    just_deref
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
                    just_deref
                );
            }
            DataType::Decimal128(_, scale) => {
                handle_text_view!(
                    builder,
                    field,
                    Decimal128Builder,
                    col_slice,
                    |value: &str| Ok::<_, DataFusionError>(decimal_text_to_i128(
                        value.as_bytes(),
                        *scale as usize
                    ))
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
                let values = col_slice.as_bin_view().ok_or_else(|| {
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
                let values = col_slice.as_bin_view().ok_or_else(|| {
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
                let values = col_slice.as_bin_view().ok_or_else(|| {
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
            DataType::Timestamp(TimeUnit::Second, _) => {
                handle_primitive_type!(
                    builder,
                    field,
                    TimestampSecondBuilder,
                    nullable,
                    odbc_api::sys::Timestamp,
                    col_slice,
                    seconds_since_epoch
                );
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                handle_primitive_type!(
                    builder,
                    field,
                    TimestampMillisecondBuilder,
                    nullable,
                    odbc_api::sys::Timestamp,
                    col_slice,
                    ms_since_epoch
                );
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                handle_primitive_type!(
                    builder,
                    field,
                    TimestampMicrosecondBuilder,
                    nullable,
                    odbc_api::sys::Timestamp,
                    col_slice,
                    us_since_epoch
                );
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                handle_primitive_type!(
                    builder,
                    field,
                    TimestampNanosecondBuilder,
                    nullable,
                    odbc_api::sys::Timestamp,
                    col_slice,
                    ns_since_epoch
                );
            }
            DataType::Time32(TimeUnit::Second) => {
                handle_text_view!(
                    builder,
                    field,
                    Time32SecondBuilder,
                    col_slice,
                    |value: &str| {
                        let nt = NaiveTime::parse_from_str(value, "%H:%M:%S%.f").map_err(|e| {
                            DataFusionError::Execution(format!("Failed to parse time: {e:?}"))
                        })?;
                        Ok::<_, DataFusionError>(nt.num_seconds_from_midnight() as i32)
                    }
                );
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                handle_text_view!(
                    builder,
                    field,
                    Time32MillisecondBuilder,
                    col_slice,
                    |value: &str| {
                        let nt = NaiveTime::parse_from_str(value, "%H:%M:%S%.f").map_err(|e| {
                            DataFusionError::Execution(format!("Failed to parse time: {e:?}"))
                        })?;
                        Ok::<_, DataFusionError>(
                            nt.num_seconds_from_midnight() as i32 * 1000
                                + (nt.nanosecond() / 1_000_000) as i32,
                        )
                    }
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
                            nt.num_seconds_from_midnight() as i64 * 1000_1000
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
                    "Unsupported field to build record batch: {field:?}"
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
