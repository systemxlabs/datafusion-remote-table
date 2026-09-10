use crate::{MongoDBType, RemoteField, RemoteSchema, RemoteType};
use mongodb::bson::{Bson, Document};
use std::collections::HashMap;

/// Infer the schema of a set of sampled documents.
///
/// MongoDB collections are schemaless, so the schema is the union of the fields
/// seen in the sample: fields are ordered by first appearance, a field missing
/// from some documents (or holding a null) is nullable, and conflicting value
/// types widen to the smallest type that can represent both.
pub(crate) fn infer_remote_schema(documents: &[Document]) -> RemoteSchema {
    let mut order: Vec<String> = Vec::new();
    let mut types: HashMap<String, MongoDBType> = HashMap::new();
    let mut has_null: HashMap<String, bool> = HashMap::new();
    let mut seen: HashMap<String, usize> = HashMap::new();

    for document in documents {
        for name in document.keys() {
            if !seen.contains_key(name) {
                order.push(name.clone());
                seen.insert(name.clone(), 0);
                has_null.insert(name.clone(), false);
            }
            *seen.get_mut(name).expect("key was just inserted") += 1;
        }
        for (name, value) in document.iter() {
            if matches!(value, Bson::Null) {
                has_null.insert(name.clone(), true);
                continue;
            }
            let value_type = bson_to_type(value);
            let merged = match types.get(name) {
                Some(existing) => merge_types(existing, &value_type),
                None => value_type,
            };
            types.insert(name.clone(), merged);
        }
    }

    let fields = order
        .into_iter()
        .map(|name| {
            let remote_type = types.get(&name).cloned().unwrap_or(MongoDBType::Null);
            let nullable = seen.get(&name).copied().unwrap_or(0) < documents.len()
                || has_null.get(&name).copied().unwrap_or(false);
            let field = RemoteField::new(name.clone(), RemoteType::MongoDB(remote_type), nullable);
            if name == "_id" {
                // MongoDB generates `_id` when it is omitted from an insert.
                field.with_auto_increment(true)
            } else {
                field
            }
        })
        .collect();

    RemoteSchema::new(fields)
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
