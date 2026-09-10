use super::schema::{DOCUMENT_COLUMN, ID_COLUMN};
use super::variant;
use crate::{DFResult, MongoDBType, RemoteSchema, RemoteType};
use arrow::array::{
    Array, ArrayBuilder, ArrayRef, AsArray, BinaryBuilder, BooleanBuilder, Float64Builder,
    Int32Builder, Int64Builder, NullBuilder, RecordBatch, RecordBatchOptions, StringBuilder,
    TimestampMillisecondBuilder, make_builder,
};
use arrow::datatypes::{
    DataType, Date32Type, Decimal128Type, DecimalType, Float32Type, Float64Type, Int32Type,
    Int64Type, SchemaRef, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use datafusion_common::{DataFusionError, project_schema};
use mongodb::bson::spec::BinarySubtype;
use mongodb::bson::{Binary, Bson, DateTime, Decimal128, Document, oid::ObjectId};
use parquet_variant::Variant;
use std::str::FromStr;

/// Convert a chunk of documents into one record batch: the typed key plus the
/// document itself as a Variant column.
pub(crate) fn documents_to_batch(
    documents: &[Document],
    table_schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
    capacity: usize,
) -> DFResult<RecordBatch> {
    let projected_schema = project_schema(table_schema, projection)?;
    let indices: Vec<usize> = match projection {
        Some(projection) => projection.clone(),
        None => (0..table_schema.fields().len()).collect(),
    };

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(indices.len());
    for field_index in indices {
        let field = table_schema.field(field_index);
        if field.name() == DOCUMENT_COLUMN {
            columns.push(variant::documents_to_variant_array(documents)?);
            continue;
        }
        let mut builder = make_builder(field.data_type(), capacity.max(1));
        for document in documents {
            append_value(
                &mut builder,
                document.get(field.name()),
                field.data_type(),
                field.name(),
            )?;
        }
        columns.push(builder.finish());
    }

    let options = RecordBatchOptions::new().with_row_count(Some(documents.len()));
    RecordBatch::try_new_with_options(projected_schema, columns, &options).map_err(Into::into)
}

/// Convert a record batch of `(key, document)` rows back into BSON documents.
///
/// A non-null key overrides the `_id` already present in the document; a null
/// key leaves the document untouched so that the server generates one.
pub(crate) fn batch_to_documents(
    batch: &RecordBatch,
    remote_schema: &RemoteSchema,
) -> DFResult<Vec<Document>> {
    let document_index = remote_schema
        .fields
        .iter()
        .position(|field| field.name == DOCUMENT_COLUMN)
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "MongoDB insert requires a {DOCUMENT_COLUMN:?} column"
            ))
        })?;
    let id_index = remote_schema
        .fields
        .iter()
        .position(|field| field.name == ID_COLUMN);

    // The document column is a Variant: a struct of a shared metadata buffer
    // and one value buffer per row.
    let struct_array = batch.column(document_index).as_struct();
    let metadata = struct_array.column(0).as_binary_view();
    let values = struct_array.column(1).as_binary_view();

    let mut result = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if struct_array.is_null(row) {
            return Err(DataFusionError::Execution(format!(
                "MongoDB insert requires a non-null {DOCUMENT_COLUMN:?} value, got null in row {row}"
            )));
        }
        let variant = Variant::try_new(metadata.value(row), values.value(row)).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to decode the {DOCUMENT_COLUMN:?} column as a Variant: {e:?}"
            ))
        })?;
        let mut document = variant::variant_to_document(&variant)?;
        if let Some(id_index) = id_index {
            let id = batch.column(id_index);
            if !id.is_null(row) {
                document.insert(
                    ID_COLUMN,
                    arrow_value_to_bson(id, row, &remote_schema.fields[id_index].remote_type)?,
                );
            }
        }
        result.push(document);
    }
    Ok(result)
}

fn append_value(
    builder: &mut Box<dyn ArrayBuilder>,
    value: Option<&Bson>,
    data_type: &DataType,
    name: &str,
) -> DFResult<()> {
    let Some(value) = value else {
        return append_null(builder, data_type, name);
    };
    if matches!(value, Bson::Null) {
        return append_null(builder, data_type, name);
    }

    match data_type {
        DataType::Boolean => {
            let builder = downcast::<BooleanBuilder>(builder, data_type, name)?;
            match value.as_bool() {
                Some(value) => builder.append_value(value),
                None => return unexpected(value, data_type, name),
            }
        }
        DataType::Int32 => {
            let builder = downcast::<Int32Builder>(builder, data_type, name)?;
            match value {
                Bson::Int32(value) => builder.append_value(*value),
                Bson::Int64(value) => {
                    builder.append_value(i32::try_from(*value).map_err(|_| {
                        DataFusionError::Execution(format!(
                            "MongoDB value {value} is out of range for column {name:?}"
                        ))
                    })?)
                }
                _ => return unexpected(value, data_type, name),
            }
        }
        DataType::Int64 => {
            let builder = downcast::<Int64Builder>(builder, data_type, name)?;
            match value {
                Bson::Int32(value) => builder.append_value(*value as i64),
                Bson::Int64(value) => builder.append_value(*value),
                _ => return unexpected(value, data_type, name),
            }
        }
        DataType::Float64 => {
            let builder = downcast::<Float64Builder>(builder, data_type, name)?;
            match value {
                Bson::Double(value) => builder.append_value(*value),
                Bson::Int32(value) => builder.append_value(*value as f64),
                Bson::Int64(value) => builder.append_value(*value as f64),
                _ => return unexpected(value, data_type, name),
            }
        }
        DataType::Utf8 => {
            let builder = downcast::<StringBuilder>(builder, data_type, name)?;
            builder.append_value(bson_to_text(value));
        }
        DataType::Binary => {
            let builder = downcast::<BinaryBuilder>(builder, data_type, name)?;
            match value {
                Bson::Binary(binary) => builder.append_value(&binary.bytes),
                _ => return unexpected(value, data_type, name),
            }
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let builder = downcast::<TimestampMillisecondBuilder>(builder, data_type, name)?;
            match value {
                Bson::DateTime(value) => builder.append_value(value.timestamp_millis()),
                _ => return unexpected(value, data_type, name),
            }
        }
        _ => {
            return Err(DataFusionError::NotImplemented(format!(
                "Unsupported MongoDB data type {data_type} for column {name:?}"
            )));
        }
    }
    Ok(())
}

fn append_null(
    builder: &mut Box<dyn ArrayBuilder>,
    data_type: &DataType,
    name: &str,
) -> DFResult<()> {
    match data_type {
        DataType::Null => downcast::<NullBuilder>(builder, data_type, name)?.append_null(),
        DataType::Boolean => downcast::<BooleanBuilder>(builder, data_type, name)?.append_null(),
        DataType::Int32 => downcast::<Int32Builder>(builder, data_type, name)?.append_null(),
        DataType::Int64 => downcast::<Int64Builder>(builder, data_type, name)?.append_null(),
        DataType::Float64 => downcast::<Float64Builder>(builder, data_type, name)?.append_null(),
        DataType::Utf8 => downcast::<StringBuilder>(builder, data_type, name)?.append_null(),
        DataType::Binary => downcast::<BinaryBuilder>(builder, data_type, name)?.append_null(),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            downcast::<TimestampMillisecondBuilder>(builder, data_type, name)?.append_null()
        }
        _ => {
            return Err(DataFusionError::NotImplemented(format!(
                "Unsupported MongoDB data type {data_type} for column {name:?}"
            )));
        }
    }
    Ok(())
}

fn downcast<'a, T: 'static>(
    builder: &'a mut Box<dyn ArrayBuilder>,
    data_type: &DataType,
    name: &str,
) -> DFResult<&'a mut T> {
    builder.as_any_mut().downcast_mut::<T>().ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Failed to downcast array builder for column {name:?} of type {data_type}"
        ))
    })
}

fn unexpected<T>(value: &Bson, data_type: &DataType, name: &str) -> DFResult<T> {
    Err(DataFusionError::Execution(format!(
        "MongoDB value {value} cannot be read as {data_type} for column {name:?}"
    )))
}

/// Textual form of a BSON value: strings verbatim, ObjectIds as hex, and
/// everything else in the driver's display form (MongoDB shell syntax).
fn bson_to_text(value: &Bson) -> String {
    match value {
        Bson::String(value) => value.clone(),
        Bson::ObjectId(value) => value.to_hex(),
        other => other.to_string(),
    }
}

fn arrow_value_to_bson(array: &ArrayRef, row: usize, remote_type: &RemoteType) -> DFResult<Bson> {
    match array.data_type() {
        DataType::Null => Ok(Bson::Null),
        DataType::Boolean => Ok(Bson::Boolean(array.as_boolean().value(row))),
        DataType::Int32 => Ok(Bson::Int32(array.as_primitive::<Int32Type>().value(row))),
        DataType::Int64 => Ok(Bson::Int64(array.as_primitive::<Int64Type>().value(row))),
        DataType::Float32 => Ok(Bson::Double(
            array.as_primitive::<Float32Type>().value(row) as f64
        )),
        DataType::Float64 => Ok(Bson::Double(array.as_primitive::<Float64Type>().value(row))),
        DataType::Utf8 => {
            let value = array.as_string::<i32>().value(row);
            match remote_type {
                RemoteType::MongoDB(MongoDBType::ObjectId) => {
                    Ok(Bson::ObjectId(ObjectId::parse_str(value).map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to parse {value:?} as a MongoDB ObjectId: {e:?}"
                        ))
                    })?))
                }
                RemoteType::MongoDB(MongoDBType::Object) => Ok(Bson::Document(parse_json(value)?)),
                RemoteType::MongoDB(MongoDBType::Array) => match parse_bson(value)? {
                    Bson::Array(array) => Ok(Bson::Array(array)),
                    other => Err(DataFusionError::Execution(format!(
                        "Failed to parse {value:?} as a MongoDB array, got: {other}"
                    ))),
                },
                RemoteType::MongoDB(MongoDBType::Date) => Ok(Bson::DateTime(
                    DateTime::parse_rfc3339_str(value).map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to parse {value:?} as a MongoDB date: {e:?}"
                        ))
                    })?,
                )),
                RemoteType::MongoDB(MongoDBType::Decimal128) => Ok(Bson::Decimal128(
                    Decimal128::from_str(value).map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to parse {value:?} as a MongoDB Decimal128: {e:?}"
                        ))
                    })?,
                )),
                _ => Ok(Bson::String(value.to_string())),
            }
        }
        DataType::Binary => Ok(Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: array.as_binary::<i32>().value(row).to_vec(),
        })),
        DataType::Timestamp(unit, _) => {
            let milliseconds = match unit {
                TimeUnit::Second => array.as_primitive::<TimestampSecondType>().value(row) * 1_000,
                TimeUnit::Millisecond => {
                    array.as_primitive::<TimestampMillisecondType>().value(row)
                }
                TimeUnit::Microsecond => {
                    array.as_primitive::<TimestampMicrosecondType>().value(row) / 1_000
                }
                TimeUnit::Nanosecond => {
                    array.as_primitive::<TimestampNanosecondType>().value(row) / 1_000_000
                }
            };
            Ok(Bson::DateTime(DateTime::from_millis(milliseconds)))
        }
        DataType::Date32 => {
            let days = array.as_primitive::<Date32Type>().value(row);
            Ok(Bson::DateTime(DateTime::from_millis(
                i64::from(days) * 86_400_000,
            )))
        }
        DataType::Decimal128(precision, scale) => {
            let value = array.as_primitive::<Decimal128Type>().value(row);
            let text = Decimal128Type::format_decimal(value, *precision, *scale);
            Ok(Bson::Decimal128(Decimal128::from_str(&text).map_err(
                |e| {
                    DataFusionError::Execution(format!(
                        "Failed to convert {text:?} to a MongoDB Decimal128: {e:?}"
                    ))
                },
            )?))
        }
        data_type => Err(DataFusionError::NotImplemented(format!(
            "Insert is not supported for MongoDB data type {data_type}"
        ))),
    }
}

fn parse_bson(value: &str) -> DFResult<Bson> {
    let json: serde_json::Value = serde_json::from_str(value).map_err(|e| {
        DataFusionError::Execution(format!("Failed to parse {value:?} as JSON: {e:?}"))
    })?;
    Bson::try_from(json).map_err(|e| {
        DataFusionError::Execution(format!("Failed to parse {value:?} as BSON: {e:?}"))
    })
}

fn parse_json(value: &str) -> DFResult<Document> {
    match parse_bson(value)? {
        Bson::Document(document) => Ok(document),
        other => Err(DataFusionError::Execution(format!(
            "Failed to parse {value:?} as a MongoDB document, got: {other}"
        ))),
    }
}
