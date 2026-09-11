//! Conversion from BSON documents to the Parquet Variant binary encoding.
//!
//! Variant has no counterpart for every BSON type. The types it can express
//! exactly (numbers, strings, booleans, dates, binary bytes, nested objects and
//! arrays) are stored natively. The MongoDB specific ones (ObjectId, regular
//! expressions, the internal BSON timestamp, min/max keys, JavaScript, symbols,
//! decimal128) are stored as their canonical extended JSON object form, so that
//! a Variant consumer can still recognise them.
//!
//! Two things are lost, because Variant cannot express them: the subtype of a
//! binary value (generic is assumed) and BSON's field order (a Variant object's
//! fields are stored sorted by name).

use crate::DFResult;
use datafusion_common::DataFusionError;
use mongodb::bson::{Bson, Document, doc};
use parquet_variant::{BuilderSpecificState, ListBuilder, ObjectBuilder, Variant, VariantBuilder};
use parquet_variant_compute::VariantArrayBuilder;

/// Encode documents as a Variant column.
pub(crate) fn documents_to_variant_array(
    documents: &[Document],
) -> DFResult<arrow::array::ArrayRef> {
    let mut builder = VariantArrayBuilder::new(documents.len());
    for document in documents {
        let mut variant_builder = VariantBuilder::new();
        let mut object = variant_builder.try_new_object().map_err(to_df)?;
        fill_object(&mut object, document)?;
        object.finish();
        let (metadata, value) = variant_builder.finish();
        let variant = Variant::try_new(&metadata, &value).map_err(to_df)?;
        builder.append_variant(variant);
    }
    Ok(builder.build().into())
}

fn fill_object<S: BuilderSpecificState>(
    object: &mut ObjectBuilder<'_, S>,
    document: &Document,
) -> DFResult<()> {
    for (key, value) in document {
        match value {
            Bson::Document(inner) => {
                let mut child = object.try_new_object(key).map_err(to_df)?;
                fill_object(&mut child, inner)?;
                child.finish();
            }
            Bson::Array(array) => {
                let mut child = object.try_new_list(key).map_err(to_df)?;
                fill_list(&mut child, array)?;
                child.finish();
            }
            _ => match native_variant(value)? {
                Some(variant) => object.try_insert(key, variant).map_err(to_df)?,
                None => {
                    let mut child = object.try_new_object(key).map_err(to_df)?;
                    fill_object(&mut child, &extended_json(value)?)?;
                    child.finish();
                }
            },
        }
    }
    Ok(())
}

fn fill_list<S: BuilderSpecificState>(
    list: &mut ListBuilder<'_, S>,
    array: &[Bson],
) -> DFResult<()> {
    for value in array {
        match value {
            Bson::Document(inner) => {
                let mut child = list.new_object();
                fill_object(&mut child, inner)?;
                child.finish();
            }
            Bson::Array(inner) => {
                let mut child = list.new_list();
                fill_list(&mut child, inner)?;
                child.finish();
            }
            _ => match native_variant(value)? {
                Some(variant) => list.try_append_value(variant).map_err(to_df)?,
                None => {
                    let mut child = list.new_object();
                    fill_object(&mut child, &extended_json(value)?)?;
                    child.finish();
                }
            },
        }
    }
    Ok(())
}

/// The BSON values Variant can express exactly.
///
/// Binary always uses the generic subtype, since Variant has no subtype field.
fn native_variant(value: &Bson) -> DFResult<Option<Variant<'_, '_>>> {
    Ok(Some(match value {
        Bson::Null => Variant::Null,
        Bson::Boolean(value) => Variant::from(*value),
        Bson::Int32(value) => Variant::from(*value),
        Bson::Int64(value) => Variant::from(*value),
        Bson::Double(value) => Variant::from(*value),
        Bson::String(value) => Variant::from(value.as_str()),
        Bson::Binary(value) => Variant::from(value.bytes.as_slice()),
        Bson::DateTime(value) => {
            let millis = value.timestamp_millis();
            Variant::TimestampMicros(chrono::DateTime::from_timestamp_millis(millis).ok_or_else(
                || {
                    DataFusionError::Execution(format!(
                        "MongoDB date {millis} is out of range for a Variant timestamp"
                    ))
                },
            )?)
        }
        _ => return Ok(None),
    }))
}

/// Canonical extended JSON form of a BSON value Variant cannot express.
///
/// This is a wrapper document such as `{"$oid": "..."}`, which keeps the value
/// readable as the JSON that MongoDB itself would print for it.
fn extended_json(value: &Bson) -> DFResult<Document> {
    Ok(match value {
        Bson::ObjectId(value) => doc! { "$oid": value.to_hex() },
        Bson::RegularExpression(value) => doc! {
            "$regularExpression": {
                "pattern": value.pattern.as_str(),
                "options": value.options.clone(),
            }
        },
        Bson::Timestamp(value) => doc! {
            "$timestamp": { "t": i64::from(value.time), "i": i64::from(value.increment) }
        },
        Bson::MinKey => doc! { "$minKey": 1 },
        Bson::MaxKey => doc! { "$maxKey": 1 },
        Bson::Undefined => doc! { "$undefined": true },
        Bson::Symbol(value) => doc! { "$symbol": value },
        Bson::JavaScriptCode(value) => doc! { "$code": value },
        Bson::JavaScriptCodeWithScope(value) => doc! {
            "$code": value.code.clone(),
            "$scope": value.scope.clone(),
        },
        // `DbPointer` is deprecated and its fields are not public, so it cannot
        // be carried through Variant at all.
        Bson::DbPointer(_) => {
            return Err(DataFusionError::NotImplemented(
                "A deprecated BSON DbPointer cannot be represented".to_string(),
            ));
        }
        Bson::Decimal128(value) => doc! { "$numberDecimal": value.to_string() },
        other => {
            return Err(DataFusionError::NotImplemented(format!(
                "Cannot encode the BSON value {other} as extended JSON"
            )));
        }
    })
}

fn to_df(error: arrow::error::ArrowError) -> DataFusionError {
    DataFusionError::Execution(format!("Failed to build a Variant value: {error:?}"))
}
