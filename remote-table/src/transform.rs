use crate::{DFResult, RemoteField, RemoteSchemaRef};
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, FieldRef, IntervalUnit, Schema, SchemaRef, TimeUnit};
use datafusion::common::{DataFusionError, project_schema};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use futures::{Stream, StreamExt};
use std::any::Any;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct TransformArgs<'a> {
    pub col_index: usize,
    pub field: &'a FieldRef,
    pub remote_field: Option<&'a RemoteField>,
}

pub trait Transform: Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;

    fn transform_null(
        &self,
        array: &NullArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_boolean(
        &self,
        array: &BooleanArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_int8(
        &self,
        array: &Int8Array,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_int16(
        &self,
        array: &Int16Array,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_int32(
        &self,
        array: &Int32Array,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_int64(
        &self,
        array: &Int64Array,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_uint8(
        &self,
        array: &UInt8Array,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_uint16(
        &self,
        array: &UInt16Array,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_uint32(
        &self,
        array: &UInt32Array,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_uint64(
        &self,
        array: &UInt64Array,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_float16(
        &self,
        array: &Float16Array,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_float32(
        &self,
        array: &Float32Array,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_float64(
        &self,
        array: &Float64Array,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_binary(
        &self,
        array: &BinaryArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_fixed_size_binary(
        &self,
        array: &FixedSizeBinaryArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_large_binary(
        &self,
        array: &LargeBinaryArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_binary_view(
        &self,
        array: &BinaryViewArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_utf8(
        &self,
        array: &StringArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_large_utf8(
        &self,
        array: &LargeStringArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_utf8_view(
        &self,
        array: &StringViewArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_timestamp_second(
        &self,
        array: &TimestampSecondArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_timestamp_millisecond(
        &self,
        array: &TimestampMillisecondArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_timestamp_microsecond(
        &self,
        array: &TimestampMicrosecondArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_timestamp_nanosecond(
        &self,
        array: &TimestampNanosecondArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_date32(
        &self,
        array: &Date32Array,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_date64(
        &self,
        array: &Date64Array,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_time32_second(
        &self,
        array: &Time32SecondArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_time32_millisecond(
        &self,
        array: &Time32MillisecondArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_time64_microsecond(
        &self,
        array: &Time64MicrosecondArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_time64_nanosecond(
        &self,
        array: &Time64NanosecondArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_interval_year_month(
        &self,
        array: &IntervalYearMonthArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_interval_day_time(
        &self,
        array: &IntervalDayTimeArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_interval_month_day_nano(
        &self,
        array: &IntervalMonthDayNanoArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_list(
        &self,
        array: &ListArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_list_view(
        &self,
        array: &ListViewArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_fixed_size_list(
        &self,
        array: &FixedSizeListArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_large_list(
        &self,
        array: &LargeListArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_large_list_view(
        &self,
        array: &LargeListViewArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_decimal128(
        &self,
        array: &Decimal128Array,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_decimal256(
        &self,
        array: &Decimal256Array,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }

    fn transform_struct(
        &self,
        array: &StructArray,
        args: TransformArgs,
    ) -> DFResult<(ArrayRef, FieldRef)> {
        Ok((Arc::new(array.clone()), args.field.clone()))
    }
}

#[derive(Debug)]
pub struct DefaultTransform {}

impl Transform for DefaultTransform {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub(crate) struct TransformStream {
    input: SendableRecordBatchStream,
    transform: Arc<dyn Transform>,
    table_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    projected_schema: SchemaRef,
    remote_schema: Option<RemoteSchemaRef>,
}

impl TransformStream {
    pub fn try_new(
        input: SendableRecordBatchStream,
        transform: Arc<dyn Transform>,
        table_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        remote_schema: Option<RemoteSchemaRef>,
    ) -> DFResult<Self> {
        let projected_schema = project_schema(&table_schema, projection.as_ref())?;
        Ok(Self {
            input,
            transform,
            table_schema,
            projection,
            projected_schema,
            remote_schema,
        })
    }
}

impl Stream for TransformStream {
    type Item = DFResult<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                match transform_batch(
                    batch,
                    self.transform.as_ref(),
                    &self.table_schema,
                    self.projection.as_ref(),
                    self.remote_schema.as_ref(),
                ) {
                    Ok(result) => Poll::Ready(Some(Ok(result))),
                    Err(e) => Poll::Ready(Some(Err(e))),
                }
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for TransformStream {
    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }
}

macro_rules! handle_transform {
    ($batch:expr, $idx:expr, $array_ty:ty, $transform:expr, $transform_method:ident, $transform_args:expr) => {{
        let array = $batch
            .column($idx)
            .as_any()
            .downcast_ref::<$array_ty>()
            .expect(concat!("Failed to downcast to ", stringify!($array_ty)));
        $transform.$transform_method(array, $transform_args)?
    }};
}

pub(crate) fn transform_batch(
    batch: RecordBatch,
    transform: &dyn Transform,
    table_schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
    remote_schema: Option<&RemoteSchemaRef>,
) -> DFResult<RecordBatch> {
    let mut new_arrays: Vec<ArrayRef> = Vec::with_capacity(batch.schema().fields.len());
    let mut new_fields: Vec<FieldRef> = Vec::with_capacity(batch.schema().fields.len());
    let all_col_indexes = (0..table_schema.fields.len()).collect::<Vec<usize>>();
    let projected_col_indexes = projection.unwrap_or(&all_col_indexes);

    for (idx, col_index) in projected_col_indexes.iter().enumerate() {
        let field = unsafe { table_schema.fields.get_unchecked(*col_index) };
        let remote_field = remote_schema.map(|schema| &schema.fields[*col_index]);
        let args = TransformArgs {
            col_index: *col_index,
            field,
            remote_field,
        };

        let (new_array, new_field) = match &field.data_type() {
            DataType::Null => {
                handle_transform!(batch, idx, NullArray, transform, transform_null, args)
            }
            DataType::Boolean => {
                handle_transform!(batch, idx, BooleanArray, transform, transform_boolean, args)
            }
            DataType::Int8 => {
                handle_transform!(batch, idx, Int8Array, transform, transform_int8, args)
            }
            DataType::Int16 => {
                handle_transform!(batch, idx, Int16Array, transform, transform_int16, args)
            }
            DataType::Int32 => {
                handle_transform!(batch, idx, Int32Array, transform, transform_int32, args)
            }
            DataType::Int64 => {
                handle_transform!(batch, idx, Int64Array, transform, transform_int64, args)
            }
            DataType::UInt8 => {
                handle_transform!(batch, idx, UInt8Array, transform, transform_uint8, args)
            }
            DataType::UInt16 => {
                handle_transform!(batch, idx, UInt16Array, transform, transform_uint16, args)
            }
            DataType::UInt32 => {
                handle_transform!(batch, idx, UInt32Array, transform, transform_uint32, args)
            }
            DataType::UInt64 => {
                handle_transform!(batch, idx, UInt64Array, transform, transform_uint64, args)
            }
            DataType::Float16 => {
                handle_transform!(batch, idx, Float16Array, transform, transform_float16, args)
            }
            DataType::Float32 => {
                handle_transform!(batch, idx, Float32Array, transform, transform_float32, args)
            }
            DataType::Float64 => {
                handle_transform!(batch, idx, Float64Array, transform, transform_float64, args)
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                handle_transform!(
                    batch,
                    idx,
                    TimestampSecondArray,
                    transform,
                    transform_timestamp_second,
                    args
                )
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                handle_transform!(
                    batch,
                    idx,
                    TimestampMillisecondArray,
                    transform,
                    transform_timestamp_millisecond,
                    args
                )
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                handle_transform!(
                    batch,
                    idx,
                    TimestampMicrosecondArray,
                    transform,
                    transform_timestamp_microsecond,
                    args
                )
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                handle_transform!(
                    batch,
                    idx,
                    TimestampNanosecondArray,
                    transform,
                    transform_timestamp_nanosecond,
                    args
                )
            }
            DataType::Date32 => {
                handle_transform!(batch, idx, Date32Array, transform, transform_date32, args)
            }
            DataType::Date64 => {
                handle_transform!(batch, idx, Date64Array, transform, transform_date64, args)
            }
            DataType::Time32(TimeUnit::Second) => {
                handle_transform!(
                    batch,
                    idx,
                    Time32SecondArray,
                    transform,
                    transform_time32_second,
                    args
                )
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                handle_transform!(
                    batch,
                    idx,
                    Time32MillisecondArray,
                    transform,
                    transform_time32_millisecond,
                    args
                )
            }
            DataType::Time32(TimeUnit::Microsecond) => unreachable!(),
            DataType::Time32(TimeUnit::Nanosecond) => unreachable!(),
            DataType::Time64(TimeUnit::Second) => unreachable!(),
            DataType::Time64(TimeUnit::Millisecond) => unreachable!(),
            DataType::Time64(TimeUnit::Microsecond) => {
                handle_transform!(
                    batch,
                    idx,
                    Time64MicrosecondArray,
                    transform,
                    transform_time64_microsecond,
                    args
                )
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                handle_transform!(
                    batch,
                    idx,
                    Time64NanosecondArray,
                    transform,
                    transform_time64_nanosecond,
                    args
                )
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                handle_transform!(
                    batch,
                    idx,
                    IntervalYearMonthArray,
                    transform,
                    transform_interval_year_month,
                    args
                )
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                handle_transform!(
                    batch,
                    idx,
                    IntervalDayTimeArray,
                    transform,
                    transform_interval_day_time,
                    args
                )
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                handle_transform!(
                    batch,
                    idx,
                    IntervalMonthDayNanoArray,
                    transform,
                    transform_interval_month_day_nano,
                    args
                )
            }
            DataType::Binary => {
                handle_transform!(batch, idx, BinaryArray, transform, transform_binary, args)
            }
            DataType::FixedSizeBinary(_) => {
                handle_transform!(
                    batch,
                    idx,
                    FixedSizeBinaryArray,
                    transform,
                    transform_fixed_size_binary,
                    args
                )
            }
            DataType::LargeBinary => {
                handle_transform!(
                    batch,
                    idx,
                    LargeBinaryArray,
                    transform,
                    transform_large_binary,
                    args
                )
            }
            DataType::BinaryView => {
                handle_transform!(
                    batch,
                    idx,
                    BinaryViewArray,
                    transform,
                    transform_binary_view,
                    args
                )
            }
            DataType::Utf8 => {
                handle_transform!(batch, idx, StringArray, transform, transform_utf8, args)
            }
            DataType::LargeUtf8 => {
                handle_transform!(
                    batch,
                    idx,
                    LargeStringArray,
                    transform,
                    transform_large_utf8,
                    args
                )
            }
            DataType::Utf8View => {
                handle_transform!(
                    batch,
                    idx,
                    StringViewArray,
                    transform,
                    transform_utf8_view,
                    args
                )
            }
            DataType::List(_field) => {
                handle_transform!(batch, idx, ListArray, transform, transform_list, args)
            }
            DataType::ListView(_field) => {
                handle_transform!(
                    batch,
                    idx,
                    ListViewArray,
                    transform,
                    transform_list_view,
                    args
                )
            }
            DataType::FixedSizeList(_, _) => {
                handle_transform!(
                    batch,
                    idx,
                    FixedSizeListArray,
                    transform,
                    transform_fixed_size_list,
                    args
                )
            }
            DataType::LargeList(_field) => {
                handle_transform!(
                    batch,
                    idx,
                    LargeListArray,
                    transform,
                    transform_large_list,
                    args
                )
            }
            DataType::LargeListView(_field) => {
                handle_transform!(
                    batch,
                    idx,
                    LargeListViewArray,
                    transform,
                    transform_large_list_view,
                    args
                )
            }
            DataType::Decimal128(_, _) => {
                handle_transform!(
                    batch,
                    idx,
                    Decimal128Array,
                    transform,
                    transform_decimal128,
                    args
                )
            }
            DataType::Decimal256(_, _) => {
                handle_transform!(
                    batch,
                    idx,
                    Decimal256Array,
                    transform,
                    transform_decimal256,
                    args
                )
            }
            DataType::Struct(_fields) => {
                handle_transform!(batch, idx, StructArray, transform, transform_struct, args)
            }
            data_type => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported transform arrow type {data_type:?}",
                )));
            }
        };
        new_arrays.push(new_array);
        new_fields.push(new_field);
    }
    let new_schema = Arc::new(Schema::new(new_fields));
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        new_schema, new_arrays, &options,
    )?)
}

pub(crate) fn transform_schema(
    schema: SchemaRef,
    transform: &dyn Transform,
    remote_schema: Option<&RemoteSchemaRef>,
) -> DFResult<SchemaRef> {
    let empty_record = RecordBatch::new_empty(schema.clone());
    transform_batch(empty_record, transform, &schema, None, remote_schema)
        .map(|batch| batch.schema())
}
