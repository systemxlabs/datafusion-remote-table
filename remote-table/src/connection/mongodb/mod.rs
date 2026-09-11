mod command;
mod row;
mod variant;

pub(crate) use command::rewrite_mongo_query;
pub(crate) use command::select_all_mongo_command;

use crate::{
    Connection, ConnectionOptions, DFResult, Literalize, MongoDBConnectionOptions, MongoDBType,
    Pool, PoolState, RemoteField, RemoteSchema, RemoteSchemaRef, RemoteSource, RemoteType,
};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use command::resolve_table;
use datafusion_common::{DataFusionError, project_schema};
use datafusion_execution::SendableRecordBatchStream;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::TryStreamExt;
use log::debug;
use mongodb::bson::Document;
use mongodb::options::{ClientOptions, Credential, ServerAddress};
use mongodb::{Client, Cursor};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};

/// Column holding the whole BSON document.
const DOCUMENT_COLUMN: &str = "document";

/// The schema of a collection: one Variant column holding every document.
///
/// A MongoDB collection is schemaless and can be arbitrarily nested, so there
/// is nothing to infer: the collection is a bag of documents and that is
/// exactly what is exposed. The document key is inside the document itself.
///
/// Every collection has the same schema, so it is built once and shared.
static REMOTE_SCHEMA: LazyLock<RemoteSchemaRef> = LazyLock::new(|| {
    Arc::new(RemoteSchema::new(vec![RemoteField::new(
        DOCUMENT_COLUMN,
        RemoteType::MongoDB(MongoDBType::Document),
        false,
    )]))
});

#[derive(Debug)]
pub struct MongoDBPool {
    client: Client,
    connections: Arc<AtomicUsize>,
}

pub async fn connect_mongodb(options: &MongoDBConnectionOptions) -> DFResult<MongoDBPool> {
    // The driver options are built field by field rather than parsed from a
    // connection string, so what is configured here is exactly what is used.
    let mut client_options = ClientOptions::default();
    client_options.hosts = vec![ServerAddress::Tcp {
        host: options.host.clone(),
        port: Some(options.port),
    }];
    // Nothing more is set: a client is not bound to a database (every table
    // names the one it lives in), and no username means the server does not ask
    // for one. With a username, the driver negotiates the mechanism and
    // authenticates against `admin`, its documented default for SCRAM.
    if !options.username.is_empty() {
        client_options.credential = Some(
            Credential::builder()
                .username(options.username.clone())
                .password(options.password.clone())
                .build(),
        );
    }
    if let Some(value) = options.pool_max_size {
        client_options.max_pool_size = Some(value);
    }
    if let Some(value) = options.pool_min_idle {
        client_options.min_pool_size = Some(value);
    }
    if let Some(value) = options.pool_max_connecting {
        client_options.max_connecting = Some(value);
    }
    if let Some(value) = options.pool_idle_timeout {
        client_options.max_idle_time = Some(value);
    }
    let client = Client::with_options(client_options).map_err(|e| {
        DataFusionError::Execution(format!("Failed to create mongodb client: {e:?}"))
    })?;
    Ok(MongoDBPool {
        client,
        connections: Arc::new(AtomicUsize::new(0)),
    })
}

#[async_trait::async_trait]
impl Pool for MongoDBPool {
    async fn get(&self) -> DFResult<Arc<dyn Connection>> {
        self.connections.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(MongoDBConnection {
            client: self.client.clone(),
            pool_connections: self.connections.clone(),
        }))
    }

    async fn state(&self) -> DFResult<PoolState> {
        Ok(PoolState {
            connections: self.connections.load(Ordering::SeqCst),
            idle_connections: 0,
        })
    }
}

#[derive(Debug)]
pub struct MongoDBConnection {
    client: Client,
    pool_connections: Arc<AtomicUsize>,
}

impl Drop for MongoDBConnection {
    fn drop(&mut self) {
        self.pool_connections.fetch_sub(1, Ordering::SeqCst);
    }
}

impl MongoDBConnection {
    async fn open_cursor(
        &self,
        database: &str,
        collection: &str,
        limit: Option<usize>,
    ) -> DFResult<Cursor<Document>> {
        debug!(
            "[remote-table] executing mongodb find: database={database}, collection={collection}, limit={limit:?}"
        );
        let db = self.client.database(database);
        let collection = db.collection::<Document>(collection);
        let mut find = collection.find(Document::new());
        if let Some(limit) = limit {
            find = find.limit(limit as i64);
        }
        find.await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to execute mongodb find: {e:?}"))
        })
    }
}

#[async_trait::async_trait]
impl Connection for MongoDBConnection {
    async fn infer_schema(&self, source: &RemoteSource) -> DFResult<RemoteSchemaRef> {
        // Nothing needs to be read - a collection is a bag of documents, so the
        // schema is the same for every collection - but building a table is
        // where an unusable source should be reported.
        resolve_table(source)?;
        Ok(Arc::clone(&REMOTE_SCHEMA))
    }

    async fn query(
        &self,
        conn_options: &ConnectionOptions,
        source: &RemoteSource,
        table_schema: SchemaRef,
        projection: Option<&Vec<usize>>,
        unparsed_filters: &[String],
        limit: Option<usize>,
    ) -> DFResult<SendableRecordBatchStream> {
        if !unparsed_filters.is_empty() {
            return Err(DataFusionError::NotImplemented(
                "MongoDB does not support filter pushdown".to_string(),
            ));
        }

        let (database, collection) = resolve_table(source)?;
        let cursor = self.open_cursor(&database, &collection, limit).await?;

        let projected_schema = project_schema(&table_schema, projection)?;
        let chunk_size = conn_options.stream_chunk_size().max(1);
        let projection = projection.cloned();
        let table_schema = Arc::clone(&table_schema);

        let stream = async_stream::stream! {
            let mut cursor = Box::pin(cursor);
            let mut exhausted = false;
            while !exhausted {
                let mut documents: Vec<Document> = Vec::with_capacity(chunk_size);
                while documents.len() < chunk_size {
                    match cursor.try_next().await {
                        Ok(Some(document)) => documents.push(document),
                        Ok(None) => {
                            exhausted = true;
                            break;
                        }
                        Err(e) => {
                            yield Err(DataFusionError::Execution(format!(
                                "Failed to fetch documents from mongodb: {e:?}"
                            )));
                            return;
                        }
                    }
                }
                if documents.is_empty() {
                    continue;
                }
                match row::documents_to_batch(&documents, &table_schema, projection.as_ref()) {
                    Ok(batch) => yield Ok(batch),
                    Err(e) => {
                        yield Err(e);
                        return;
                    }
                }
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            projected_schema,
            stream,
        )))
    }

    async fn insert(
        &self,
        _conn_options: &ConnectionOptions,
        _literalizer: Arc<dyn Literalize>,
        _table: &[String],
        _remote_schema: RemoteSchemaRef,
        _batch: RecordBatch,
    ) -> DFResult<usize> {
        Err(DataFusionError::NotImplemented(
            "MongoDB does not support insert".to_string(),
        ))
    }

    async fn count(
        &self,
        _conn_options: &ConnectionOptions,
        source: &RemoteSource,
        unparsed_filters: &[String],
    ) -> DFResult<Option<usize>> {
        if !unparsed_filters.is_empty() {
            return Ok(None);
        }
        let (database, collection) = resolve_table(source)?;
        let count = self
            .client
            .database(&database)
            .collection::<Document>(&collection)
            .count_documents(Document::new())
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!("Failed to count documents on mongodb: {e:?}"))
            })?;
        Ok(Some(count as usize))
    }
}
