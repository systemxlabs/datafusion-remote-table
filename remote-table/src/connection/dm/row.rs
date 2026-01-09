use crate::DFResult;
use crate::connection::{
    just_return, ms_since_epoch, ns_since_epoch, projections_contains, seconds_since_epoch,
    us_since_epoch,
};
use chrono::{NaiveDate, NaiveTime, Timelike};
use datafusion::arrow::array::{
    ArrayRef, BinaryBuilder, BinaryViewBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder,
    Int32Builder, Int64Builder, RecordBatch, RecordBatchOptions, StringBuilder, StringViewBuilder,
    Time32MillisecondBuilder, Time32SecondBuilder, Time64MicrosecondBuilder,
    TimestampMicrosecondBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
    TimestampSecondBuilder, make_builder,
};
use datafusion::arrow::datatypes::{DataType, Date32Type, SchemaRef, TimeUnit};
use datafusion::common::{DataFusionError, project_schema};
use odbc_api::{Bit, CursorRow, decimal_text_to_i128};

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
    ($builder:expr, $field:expr, $builder_ty:ty, $row:expr, $col_idx:expr, $convert:expr) => {{
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
        let mut value = Vec::new();
        let is_not_null = $row.get_text($col_idx, &mut value).map_err(|e| {
            DataFusionError::Execution(format!("Failed to get value for field {:?}: {e:?}", $field))
        })?;
        if is_not_null {
            let value = String::from_utf8(value).map_err(|e| {
                DataFusionError::Execution(format!("Failed to convert value to string: {e:?}"))
            })?;
            builder.append_value($convert(value)?);
        } else {
            builder.append_null();
        }
    }};
}

pub(crate) fn row_to_batch(
    mut row: CursorRow,
    table_schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> DFResult<RecordBatch> {
    let projected_schema = project_schema(table_schema, projection)?;
    let mut array_builders = vec![];
    for field in table_schema.fields() {
        let builder = make_builder(field.data_type(), 1);
        array_builders.push(builder);
    }

    for (idx, field) in table_schema.fields().iter().enumerate() {
        if !projections_contains(projection, idx) {
            continue;
        }
        let builder = &mut array_builders[idx];
        let odbc_col_idx = (idx + 1) as u16;
        match field.data_type() {
            DataType::Boolean => {
                read_data!(
                    builder,
                    field,
                    BooleanBuilder,
                    row,
                    odbc_col_idx,
                    Bit,
                    |v: Bit| { Ok::<_, DataFusionError>(v.as_bool()) }
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
                    just_return
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
                    just_return
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
                    just_return
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
                    just_return
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
                    just_return
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
                    just_return
                );
            }
            DataType::Decimal128(_precision, scale) => {
                read_text!(
                    builder,
                    field,
                    Decimal128Builder,
                    row,
                    odbc_col_idx,
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
                    just_return
                );
            }
            DataType::Utf8View => {
                read_text!(
                    builder,
                    field,
                    StringViewBuilder,
                    row,
                    odbc_col_idx,
                    just_return
                );
            }
            DataType::FixedSizeBinary(_) => {
                let builder = builder
                    .as_any_mut()
                    .downcast_mut::<FixedSizeBinaryBuilder>()
                    .unwrap_or_else(|| {
                        panic!("Failed to downcast builder to FixedSizeBinaryBuilder for {field:?}")
                    });
                let mut value = Vec::new();
                let is_not_null = row.get_binary(odbc_col_idx, &mut value).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to get value for field {:?}: {e:?}",
                        field
                    ))
                })?;
                if is_not_null {
                    builder.append_value(&value)?;
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
                let mut value = Vec::new();
                let is_not_null = row.get_binary(odbc_col_idx, &mut value).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to get value for field {:?}: {e:?}",
                        field
                    ))
                })?;
                if is_not_null {
                    builder.append_value(&value);
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
                let mut value = Vec::new();
                let is_not_null = row.get_binary(odbc_col_idx, &mut value).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to get value for field {:?}: {e:?}",
                        field
                    ))
                })?;
                if is_not_null {
                    builder.append_value(&value);
                } else {
                    builder.append_null();
                }
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                read_data!(
                    builder,
                    field,
                    TimestampSecondBuilder,
                    row,
                    odbc_col_idx,
                    odbc_api::sys::Timestamp,
                    |v| { seconds_since_epoch(&v) }
                );
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                read_data!(
                    builder,
                    field,
                    TimestampMillisecondBuilder,
                    row,
                    odbc_col_idx,
                    odbc_api::sys::Timestamp,
                    |v| { ms_since_epoch(&v) }
                );
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                read_data!(
                    builder,
                    field,
                    TimestampMicrosecondBuilder,
                    row,
                    odbc_col_idx,
                    odbc_api::sys::Timestamp,
                    |v| { us_since_epoch(&v) }
                );
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                read_data!(
                    builder,
                    field,
                    TimestampNanosecondBuilder,
                    row,
                    odbc_col_idx,
                    odbc_api::sys::Timestamp,
                    |v| { ns_since_epoch(&v) }
                );
            }
            DataType::Time32(TimeUnit::Second) => {
                read_text!(
                    builder,
                    field,
                    Time32SecondBuilder,
                    row,
                    odbc_col_idx,
                    |v: String| {
                        let nt = NaiveTime::parse_from_str(&v, "%H:%M:%S%.f").map_err(|e| {
                            DataFusionError::Execution(format!("Failed to parse time: {e:?}"))
                        })?;
                        Ok::<_, DataFusionError>(nt.num_seconds_from_midnight() as i32)
                    }
                );
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                read_text!(
                    builder,
                    field,
                    Time32MillisecondBuilder,
                    row,
                    odbc_col_idx,
                    |v: String| {
                        let nt = NaiveTime::parse_from_str(&v, "%H:%M:%S%.f").map_err(|e| {
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
                read_text!(
                    builder,
                    field,
                    Time64MicrosecondBuilder,
                    row,
                    odbc_col_idx,
                    |v: String| {
                        let nt = NaiveTime::parse_from_str(&v, "%H:%M:%S%.f").map_err(|e| {
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
                read_data!(
                    builder,
                    field,
                    Date32Builder,
                    row,
                    odbc_col_idx,
                    odbc_api::sys::Date,
                    |v: odbc_api::sys::Date| {
                        let date =
                            NaiveDate::from_ymd_opt(v.year as i32, v.month as u32, v.day as u32)
                                .ok_or_else(|| {
                                    DataFusionError::Execution(format!("Invalid date: {v:?}"))
                                })?;
                        Ok::<_, DataFusionError>(Date32Type::from_naive_date(date))
                    }
                );
            }
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported data type: {field:?}",
                )));
            }
        }
    }

    let projected_columns = array_builders
        .into_iter()
        .enumerate()
        .filter(|(idx, _)| projections_contains(projection, *idx))
        .map(|(_, mut builder)| builder.finish())
        .collect::<Vec<ArrayRef>>();
    let options = RecordBatchOptions::new().with_row_count(Some(1));
    Ok(RecordBatch::try_new_with_options(
        projected_schema,
        projected_columns,
        &options,
    )?)
}
