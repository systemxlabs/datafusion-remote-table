//! Conversion between BSON documents and the Parquet Variant binary encoding.
//!
//! Variant has no counterpart for every BSON type. The types it can express
//! exactly (numbers, strings, booleans, dates, binary bytes, nested objects and
//! arrays) are stored natively. The MongoDB specific ones (ObjectId, regular
//! expressions, the internal BSON timestamp, min/max keys, JavaScript, symbols,
//! decimal128) are stored as their canonical extended JSON object form, which
//! the reverse direction recognises again.
//!
//! Two things do not survive the round trip, because Variant cannot express
//! them: the subtype of a binary value (generic is assumed) and BSON's field
//! order (a Variant object's fields are stored sorted by name).

use crate::DFResult;
use datafusion_common::DataFusionError;
use mongodb::bson::spec::BinarySubtype;
use mongodb::bson::{Binary, Bson, DateTime, Decimal128, Document, doc};
use parquet_variant::{
    BuilderSpecificState, ListBuilder, ObjectBuilder, Variant, VariantBuilder, VariantObject,
};
use parquet_variant_compute::VariantArrayBuilder;
use std::str::FromStr;

/// Leading keys of the canonical extended JSON wrappers this module writes.
const EXTENDED_JSON_WRAPPER_KEYS: &[&str] = &[
    "$oid",
    "$regularExpression",
    "$timestamp",
    "$minKey",
    "$maxKey",
    "$undefined",
    "$symbol",
    "$code",
    "$dbPointer",
    "$numberDecimal",
];

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

/// Decode a Variant document back into a BSON document.
pub(crate) fn variant_to_document(variant: &Variant<'_, '_>) -> DFResult<Document> {
    match variant_to_bson(variant)? {
        Bson::Document(document) => Ok(document),
        other => Err(DataFusionError::Execution(format!(
            "Expected a MongoDB document, got: {other}"
        ))),
    }
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
/// This is a wrapper document such as `{"$oid": "..."}`; the reverse direction
/// feeds it back to BSON's own extended JSON parser.
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

fn variant_to_bson(variant: &Variant<'_, '_>) -> DFResult<Bson> {
    Ok(match variant {
        Variant::Null => Bson::Null,
        Variant::BooleanTrue => Bson::Boolean(true),
        Variant::BooleanFalse => Bson::Boolean(false),
        Variant::Int8(value) => Bson::Int32(i32::from(*value)),
        Variant::Int16(value) => Bson::Int32(i32::from(*value)),
        Variant::Int32(value) => Bson::Int32(*value),
        Variant::Int64(value) => Bson::Int64(*value),
        Variant::Float(value) => Bson::Double(f64::from(*value)),
        Variant::Double(value) => Bson::Double(*value),
        Variant::String(value) => Bson::String(value.to_string()),
        Variant::ShortString(value) => Bson::String(value.as_str().to_string()),
        Variant::Binary(bytes) => Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: bytes.to_vec(),
        }),
        Variant::Uuid(value) => Bson::Binary(Binary {
            subtype: BinarySubtype::Uuid,
            bytes: value.as_bytes().to_vec(),
        }),
        Variant::Date(value) => Bson::DateTime(DateTime::from_millis(
            value
                .and_hms_opt(0, 0, 0)
                .map(|naive| naive.and_utc().timestamp_millis())
                .ok_or_else(|| DataFusionError::Execution(format!("Invalid date {value}")))?,
        )),
        Variant::TimestampMicros(value) => {
            Bson::DateTime(DateTime::from_millis(value.timestamp_millis()))
        }
        Variant::TimestampNanos(value) => {
            Bson::DateTime(DateTime::from_millis(value.timestamp_millis()))
        }
        Variant::TimestampNtzMicros(value) => {
            Bson::DateTime(DateTime::from_millis(value.and_utc().timestamp_millis()))
        }
        Variant::TimestampNtzNanos(value) => {
            Bson::DateTime(DateTime::from_millis(value.and_utc().timestamp_millis()))
        }
        Variant::Decimal4(value) => decimal_to_bson(i128::from(value.integer()), value.scale())?,
        Variant::Decimal8(value) => decimal_to_bson(i128::from(value.integer()), value.scale())?,
        Variant::Decimal16(value) => decimal_to_bson(value.integer(), value.scale())?,
        Variant::List(list) => {
            let mut array = Vec::new();
            for value in list.iter() {
                array.push(variant_to_bson(&value)?);
            }
            Bson::Array(array)
        }
        Variant::Object(object) => match wrapper_to_bson(object)? {
            Some(bson) => bson,
            None => {
                let mut document = Document::new();
                for (key, value) in object.iter() {
                    document.insert(key, variant_to_bson(&value)?);
                }
                Bson::Document(document)
            }
        },
        other => {
            return Err(DataFusionError::NotImplemented(format!(
                "Cannot convert {other:?} to BSON"
            )));
        }
    })
}

/// Restore a BSON value that was stored as one of the extended JSON wrappers.
fn wrapper_to_bson(object: &VariantObject<'_, '_>) -> DFResult<Option<Bson>> {
    let Some((key, _)) = object.iter().next() else {
        return Ok(None);
    };
    if !EXTENDED_JSON_WRAPPER_KEYS.contains(&key) {
        // An ordinary embedded document. Note that this means a document whose
        // first field happens to be named like one of the wrappers is read back
        // as that BSON type; MongoDB's own extended JSON has the same ambiguity.
        return Ok(None);
    }
    let json = object_to_json(object)?;
    Bson::try_from(json)
        .map(Some)
        .map_err(|e| DataFusionError::Execution(format!("Failed to decode {key}: {e:?}")))
}

fn object_to_json(object: &VariantObject<'_, '_>) -> DFResult<serde_json::Value> {
    let mut map = serde_json::Map::new();
    for (key, value) in object.iter() {
        map.insert(key.to_string(), variant_to_json(&value)?);
    }
    Ok(serde_json::Value::Object(map))
}

/// Render a Variant as plain JSON. Only extended JSON wrappers are rendered
/// this way, so dates and binary values do not occur here.
fn variant_to_json(variant: &Variant<'_, '_>) -> DFResult<serde_json::Value> {
    use serde_json::Value;
    Ok(match variant {
        Variant::Null => Value::Null,
        Variant::BooleanTrue => Value::Bool(true),
        Variant::BooleanFalse => Value::Bool(false),
        Variant::Int8(value) => Value::from(*value),
        Variant::Int16(value) => Value::from(*value),
        Variant::Int32(value) => Value::from(*value),
        Variant::Int64(value) => Value::from(*value),
        Variant::Float(value) => Value::from(*value),
        Variant::Double(value) => Value::from(*value),
        Variant::String(value) => Value::from(value.to_string()),
        Variant::ShortString(value) => Value::from(value.as_str()),
        Variant::Object(object) => object_to_json(object)?,
        Variant::List(list) => {
            let mut array = Vec::new();
            for value in list.iter() {
                array.push(variant_to_json(&value)?);
            }
            Value::Array(array)
        }
        other => {
            return Err(DataFusionError::NotImplemented(format!(
                "Cannot render {other:?} as JSON"
            )));
        }
    })
}

fn decimal_to_bson(integer: i128, scale: u8) -> DFResult<Bson> {
    let text = if scale == 0 {
        integer.to_string()
    } else {
        let scale = usize::from(scale);
        let digits = integer.unsigned_abs().to_string();
        let (whole, fraction) = if digits.len() > scale {
            let split = digits.len() - scale;
            (digits[..split].to_string(), digits[split..].to_string())
        } else {
            ("0".to_string(), format!("{digits:0>scale$}"))
        };
        format!("{}{whole}.{fraction}", if integer < 0 { "-" } else { "" })
    };
    Ok(Bson::Decimal128(Decimal128::from_str(&text).map_err(
        |e| DataFusionError::Execution(format!("Failed to decode the decimal {text}: {e:?}")),
    )?))
}

fn to_df(error: arrow::error::ArrowError) -> DataFusionError {
    DataFusionError::Execution(format!("Failed to build a Variant value: {error:?}"))
}
