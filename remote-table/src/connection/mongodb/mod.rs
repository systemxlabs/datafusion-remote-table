mod command;
mod row;
mod schema;

pub(crate) use command::rewrite_mongo_query;
pub(crate) use command::select_all_mongo_command;

use crate::{
    Connection, ConnectionOptions, DFResult, Literalize, MongoDBConnectionOptions, Pool, PoolState,
    RemoteSchemaRef, RemoteSource,
};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use command::{resolve_table, split_identifiers};
use datafusion_common::{DataFusionError, project_schema};
use datafusion_execution::SendableRecordBatchStream;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::TryStreamExt;
use log::debug;
use mongodb::bson::Document;
use mongodb::options::ClientOptions;
use mongodb::{Client, Cursor};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
pub struct MongoDBPool {
    client: Client,
    options: MongoDBConnectionOptions,
    connections: Arc<AtomicUsize>,
}

pub async fn connect_mongodb(options: &MongoDBConnectionOptions) -> DFResult<MongoDBPool> {
    let client_options = ClientOptions::parse(&options.uri).await.map_err(|e| {
        DataFusionError::Execution(format!("Failed to parse mongodb connection string: {e:?}"))
    })?;
    let client = Client::with_options(client_options).map_err(|e| {
        DataFusionError::Execution(format!("Failed to create mongodb client: {e:?}"))
    })?;
    Ok(MongoDBPool {
        client,
        options: options.clone(),
        connections: Arc::new(AtomicUsize::new(0)),
    })
}

#[async_trait::async_trait]
impl Pool for MongoDBPool {
    async fn get(&self) -> DFResult<Arc<dyn Connection>> {
        self.connections.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(MongoDBConnection {
            client: self.client.clone(),
            database: self.options.database.clone(),
            sample_size: self.options.sample_size,
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
    database: String,
    sample_size: u32,
    pool_connections: Arc<AtomicUsize>,
}

impl Drop for MongoDBConnection {
    fn drop(&mut self) {
        self.pool_connections.fetch_sub(1, Ordering::SeqCst);
    }
}

impl MongoDBConnection {
    fn database_name<'a>(&'a self, database: Option<&'a str>) -> &'a str {
        database.unwrap_or(&self.database)
    }

    async fn open_cursor(
        &self,
        database: Option<&str>,
        collection: &str,
        limit: Option<usize>,
    ) -> DFResult<Cursor<Document>> {
        let database = self.database_name(database);
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
        let (database, collection) = resolve_table(source)?;
        let mut cursor = self
            .open_cursor(
                database.as_deref(),
                &collection,
                Some(self.sample_size.max(1) as usize),
            )
            .await?;

        let mut documents = Vec::new();
        while let Some(document) = cursor.try_next().await.map_err(|e| {
            DataFusionError::Plan(format!("Failed to sample documents from mongodb: {e:?}"))
        })? {
            documents.push(document);
        }
        if documents.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "No documents found to infer schema from {source}"
            )));
        }

        Ok(Arc::new(schema::infer_remote_schema(&documents)))
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
        let cursor = self
            .open_cursor(database.as_deref(), &collection, limit)
            .await?;

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
                match row::documents_to_batch(
                    &documents,
                    &table_schema,
                    projection.as_ref(),
                    chunk_size,
                ) {
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
        table: &[String],
        remote_schema: RemoteSchemaRef,
        batch: RecordBatch,
    ) -> DFResult<usize> {
        let (database, collection) = split_identifiers(table)?;
        let database = database.unwrap_or_else(|| self.database.clone());
        let documents = row::batch_to_documents(&batch, &remote_schema)?;
        let count = documents.len();
        if count > 0 {
            self.client
                .database(&database)
                .collection::<Document>(&collection)
                .insert_many(documents)
                .await
                .map_err(|e| {
                    DataFusionError::Execution(format!("Failed to insert into mongodb: {e:?}"))
                })?;
        }
        Ok(count)
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
        let database = self.database_name(database.as_deref());
        let count = self
            .client
            .database(database)
            .collection::<Document>(&collection)
            .count_documents(Document::new())
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!("Failed to count documents on mongodb: {e:?}"))
            })?;
        Ok(Some(count as usize))
    }
}
