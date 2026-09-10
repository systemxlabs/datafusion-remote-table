use crate::{MongoDBType, RemoteField, RemoteSchema, RemoteType};
use mongodb::bson::Bson;

/// Column holding the whole BSON document.
pub(crate) const DOCUMENT_COLUMN: &str = "document";
/// Column holding the document key.
pub(crate) const ID_COLUMN: &str = "_id";

/// Build the two column schema of a collection: `_id` and the raw document.
///
/// A MongoDB collection is schemaless and can be arbitrarily nested, so only
/// `_id` is typed (from the sampled documents) and the rest of the document is
/// exposed as one column of raw BSON bytes. Unlike inferring a column per
/// field, this keeps the collection faithful and the schema stable no matter
/// what the documents contain.
pub(crate) fn infer_remote_schema(id_values: &[Bson]) -> RemoteSchema {
    let mut id_type: Option<MongoDBType> = None;
    for value in id_values {
        if matches!(value, Bson::Null) {
            continue;
        }
        let value_type = bson_to_type(value);
        id_type = Some(match id_type {
            Some(existing) => merge_types(&existing, &value_type),
            None => value_type,
        });
    }
    // MongoDB generates an ObjectId when `_id` is omitted, so that is what a
    // collection with nothing to look at (yet) reports.
    let id_type = id_type.unwrap_or(MongoDBType::ObjectId);

    RemoteSchema::new(vec![
        // The server generates `_id` when an insert omits it.
        RemoteField::new(ID_COLUMN, RemoteType::MongoDB(id_type), false).with_auto_increment(true),
        RemoteField::new(
            DOCUMENT_COLUMN,
            RemoteType::MongoDB(MongoDBType::Document),
            false,
        ),
    ])
}

/// Widen two types observed for the same field into a single type.
///
/// Anything that cannot be widened losslessly (embedded documents, arrays,
/// mixed scalars) falls back to text, which every BSON value can represent.
fn merge_types(left: &MongoDBType, right: &MongoDBType) -> MongoDBType {
    if left == right {
        return left.clone();
    }
    match (left, right) {
        (MongoDBType::Null, other) | (other, MongoDBType::Null) => other.clone(),
        (MongoDBType::Int32, MongoDBType::Int64) | (MongoDBType::Int64, MongoDBType::Int32) => {
            MongoDBType::Int64
        }
        (MongoDBType::Int32 | MongoDBType::Int64, MongoDBType::Double)
        | (MongoDBType::Double, MongoDBType::Int32 | MongoDBType::Int64) => MongoDBType::Double,
        _ => MongoDBType::String,
    }
}

fn bson_to_type(value: &Bson) -> MongoDBType {
    match value {
        Bson::Double(_) => MongoDBType::Double,
        Bson::String(_) => MongoDBType::String,
        Bson::Array(_) => MongoDBType::Array,
        Bson::Document(_) => MongoDBType::Object,
        Bson::Boolean(_) => MongoDBType::Boolean,
        Bson::Null => MongoDBType::Null,
        Bson::RegularExpression(_) => MongoDBType::Regex,
        Bson::JavaScriptCode(_)
        | Bson::JavaScriptCodeWithScope(_)
        | Bson::Symbol(_)
        | Bson::Undefined
        | Bson::MaxKey
        | Bson::MinKey
        | Bson::DbPointer(_) => MongoDBType::JavaScript,
        Bson::Int32(_) => MongoDBType::Int32,
        Bson::Int64(_) => MongoDBType::Int64,
        Bson::Timestamp(_) => MongoDBType::Timestamp,
        Bson::Binary(_) => MongoDBType::Binary,
        Bson::ObjectId(_) => MongoDBType::ObjectId,
        Bson::DateTime(_) => MongoDBType::Date,
        Bson::Decimal128(_) => MongoDBType::Decimal128,
    }
}
