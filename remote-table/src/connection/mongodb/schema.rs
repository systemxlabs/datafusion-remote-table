use crate::{MongoDBType, RemoteField, RemoteSchema, RemoteSchemaRef, RemoteType};
use std::sync::{Arc, LazyLock};

/// Column holding the whole BSON document.
pub(crate) const DOCUMENT_COLUMN: &str = "document";

/// The schema of a collection: one Variant column holding every document.
///
/// A MongoDB collection is schemaless and can be arbitrarily nested, so there
/// is nothing to infer: the collection is a bag of documents and that is
/// exactly what is exposed. The document key is inside the document itself.
///
/// Every collection has the same schema, so it is built once and shared.
pub(crate) static REMOTE_SCHEMA: LazyLock<RemoteSchemaRef> = LazyLock::new(|| {
    Arc::new(RemoteSchema::new(vec![RemoteField::new(
        DOCUMENT_COLUMN,
        RemoteType::MongoDB(MongoDBType::Document),
        false,
    )]))
});
