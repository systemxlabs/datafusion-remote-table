use crate::{DFResult, RemoteSource};
use datafusion_common::DataFusionError;
use serde_json::{Map, Value};

/// Resolve a MongoDB source into the `(database, collection)` to read.
///
/// MongoDB is not queried with SQL and has no query string, so only whole
/// collections are supported; the database defaults to
/// `MongoDBConnectionOptions::database` when the table identifier is a single
/// collection name.
pub(crate) fn resolve_table(source: &RemoteSource) -> DFResult<(Option<String>, String)> {
    match source {
        RemoteSource::Table(identifiers) => split_identifiers(identifiers),
        RemoteSource::Query(_) => Err(DataFusionError::NotImplemented(
            "MongoDB only supports RemoteSource::Table, not RemoteSource::Query".to_string(),
        )),
        RemoteSource::Command(cmd) => Err(DataFusionError::NotImplemented(format!(
            "Command {cmd:?} is not supported for MongoDB"
        ))),
    }
}

/// Rewrite a source into the `find` command document that reads the collection.
///
/// Filters are never pushed down: DataFusion evaluates them locally, because a
/// SQL predicate cannot be unparsed into a BSON filter.
pub(crate) fn rewrite_mongo_query(
    source: &RemoteSource,
    unparsed_filters: &[String],
    limit: Option<usize>,
) -> DFResult<String> {
    if !unparsed_filters.is_empty() {
        return Err(DataFusionError::NotImplemented(
            "MongoDB does not support filter pushdown".to_string(),
        ));
    }
    let (database, collection) = resolve_table(source)?;
    Ok(find_command(database.as_deref(), &collection, limit))
}

/// `SELECT * FROM <table>` equivalent, used through `RemoteSource::query`.
pub(crate) fn select_all_mongo_command(identifiers: &[String]) -> String {
    match split_identifiers(identifiers) {
        Ok((database, collection)) => find_command(database.as_deref(), &collection, None),
        Err(_) => find_command(
            None,
            identifiers.last().map(String::as_str).unwrap_or(""),
            None,
        ),
    }
}

/// Split a table identifier into `(database, collection)`.
pub(crate) fn split_identifiers(identifiers: &[String]) -> DFResult<(Option<String>, String)> {
    match identifiers {
        [collection] => Ok((None, collection.clone())),
        [database, collection] => Ok((Some(database.clone()), collection.clone())),
        _ => Err(DataFusionError::Plan(format!(
            "MongoDB table source must be [collection] or [database, collection], got: {identifiers:?}"
        ))),
    }
}

fn find_command(database: Option<&str>, collection: &str, limit: Option<usize>) -> String {
    let mut command = Map::new();
    command.insert("find".to_string(), Value::String(collection.to_string()));
    command.insert("filter".to_string(), Value::Object(Map::new()));
    if let Some(limit) = limit {
        command.insert("limit".to_string(), Value::from(limit));
    }
    if let Some(database) = database {
        command.insert("$db".to_string(), Value::String(database.to_string()));
    }
    Value::Object(command).to_string()
}
