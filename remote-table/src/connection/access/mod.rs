mod row;
mod schema;

use crate::connection::ODBC_ENV;
use crate::{
    AccessConnectionOptions, Connection, ConnectionOptions, DFResult, Literalize, Pool, PoolState,
    RemoteDbType, RemoteSchemaRef, RemoteSource,
};
use arrow::array::RecordBatch;
use arrow::array::make_builder;
use arrow::datatypes::SchemaRef;
use datafusion_common::DataFusionError;
use datafusion_common::project_schema;
use datafusion_execution::SendableRecordBatchStream;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::lock::Mutex;
use log::debug;
use odbc_api::Cursor;
use odbc_api::Environment;
use std::collections::HashMap;
use std::ffi::CString;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::runtime::Handle;

use row::append_row_to_builders;
use row::finish_batch;
use schema::build_remote_schema;

/// Cache key that captures the full ODBC connection identity, not just the
/// `.accdb` file path. Two pools that target the same path but use different
/// drivers, credentials, or extra connection parameters must NOT share a
/// connection.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AccessConnectionCacheKey {
    path: PathBuf,
    driver: String,
    uid: Option<String>,
    pwd: Option<String>,
    // BTreeMap so the key iterates and prints in a stable order; the map is
    // compared by content either way.
    extra_params: std::collections::BTreeMap<String, String>,
}

impl AccessConnectionCacheKey {
    fn from_options(options: &AccessConnectionOptions) -> Self {
        Self {
            path: options.path.clone(),
            driver: options.driver.clone(),
            uid: options.uid.clone(),
            pwd: options.pwd.clone(),
            extra_params: options
                .extra_params
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        }
    }
}

/// Per-path global ODBC connection cache.
///
/// mdbtools' `libmdbodbc.so` keeps process-global state and corrupts it after
/// a handful of successive `SQLDriverConnect` calls to the same `.accdb` file
/// (observed symptom: `SQLDriverConnect: NoDiagnostics`, stderr says
/// "File not found" while the file is plainly on disk). To work around this,
/// every `AccessPool` that targets the same connection identity (path + driver +
/// uid + pwd + extra_params) shares a single underlying `odbc_api::Connection`.
/// Concurrent access on that shared connection is still serialised by
/// `AccessConnection`'s own mutex.
///
/// Cached connections live until process exit. Bounded by the number of
/// distinct connection identities the process touches.
static ACCESS_CONN_CACHE: OnceLock<
    std::sync::Mutex<HashMap<AccessConnectionCacheKey, Arc<Mutex<odbc_api::Connection<'static>>>>>,
> = OnceLock::new();

fn access_conn_cache() -> &'static std::sync::Mutex<
    HashMap<AccessConnectionCacheKey, Arc<Mutex<odbc_api::Connection<'static>>>>,
> {
    ACCESS_CONN_CACHE.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

pub struct AccessPool {
    options: AccessConnectionOptions,
    connections: Arc<AtomicUsize>,
}

impl std::fmt::Debug for AccessPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccessPool")
            .field("options", &self.options)
            .field("connections", &self.connections)
            .finish()
    }
}

pub(crate) fn connect_access(options: &AccessConnectionOptions) -> DFResult<AccessPool> {
    Ok(AccessPool {
        options: options.clone(),
        connections: Arc::new(AtomicUsize::new(0)),
    })
}

#[async_trait::async_trait]
impl Pool for AccessPool {
    async fn get(&self) -> DFResult<Arc<dyn Connection>> {
        let cache_key = AccessConnectionCacheKey::from_options(&self.options);

        // Consult the global cache (see ACCESS_CONN_CACHE). The cache lock is
        // held across the SQLDriverConnect call so two pools racing on the
        // same connection identity don't both open a connection.
        let conn = {
            let mut cache = access_conn_cache().lock().unwrap();
            if let Some(existing) = cache.get(&cache_key) {
                existing.clone()
            } else {
                let env =
                    ODBC_ENV.get_or_init(|| Environment::new().expect("failed to create ODBC env"));
                // libmdbodbc.so ignores the `cbConnStrIn` length passed to
                // SQLDriverConnect and reads the connection string as a
                // NUL-terminated C string, while odbc-api hands the driver the
                // bytes of a Rust `&str`, which carry no terminator. The driver
                // then reads into adjacent heap memory and appends whatever it
                // finds to the DBQ path ("File not found" for a path that
                // exists). `CString` owns a NUL-terminated buffer, and `to_str`
                // borrows exactly its length, so the driver stops at the
                // terminator we own.
                let connection_str =
                    CString::new(self.options.connection_string()).map_err(|e| {
                        DataFusionError::Execution(format!(
                            "access connection string contains a NUL byte: {e}"
                        ))
                    })?;
                let connection_str = connection_str
                    .to_str()
                    .expect("connection string built from a Rust String is valid UTF-8");
                debug!("[remote-table] access connection string: {connection_str}");
                let connection = env
                    .connect_with_connection_string(
                        connection_str,
                        odbc_api::ConnectionOptions::default(),
                    )
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to create odbc connection to access: {e:?}"
                        ))
                    })?;
                let conn = Arc::new(Mutex::new(connection));
                cache.insert(cache_key, conn.clone());
                conn
            }
        };

        self.connections.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(AccessConnection {
            conn,
            pool_connections: self.connections.clone(),
        }))
    }

    async fn state(&self) -> DFResult<PoolState> {
        let active = self.connections.load(Ordering::SeqCst);
        Ok(PoolState {
            connections: active,
            idle_connections: 0,
        })
    }
}

#[derive(Debug)]
pub struct AccessConnection {
    conn: Arc<Mutex<odbc_api::Connection<'static>>>,
    pool_connections: Arc<AtomicUsize>,
}

impl Drop for AccessConnection {
    fn drop(&mut self) {
        self.pool_connections.fetch_sub(1, Ordering::SeqCst);
    }
}
#[async_trait::async_trait]
impl Connection for AccessConnection {
    async fn infer_schema(&self, source: &RemoteSource) -> DFResult<RemoteSchemaRef> {
        if let RemoteSource::Command(cmd) = source {
            return Err(DataFusionError::NotImplemented(format!(
                "{cmd:?} is only supported for mdb sources; an .accdb file is queried \
                 through its MSysObjects catalog table"
            )));
        }

        let sql = RemoteDbType::Access.limit_1_query_if_possible(source)?;
        debug!("[remote-table] inferring access schema with: {sql}");
        let conn = self.conn.lock().await;
        let cursor_opt = conn.execute(&sql, (), None).map_err(|e| {
            DataFusionError::Plan(format!(
                "Failed to execute query for schema inference on access: {e:?}, sql: {sql}"
            ))
        })?;
        match cursor_opt {
            None => Err(DataFusionError::Plan(
                "No rows returned to infer schema".to_string(),
            )),
            Some(cursor) => {
                let remote_schema = Arc::new(build_remote_schema(cursor)?);
                Ok(remote_schema)
            }
        }
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
        if let RemoteSource::Command(cmd) = source {
            return Err(DataFusionError::NotImplemented(format!(
                "{cmd:?} is only supported for mdb sources; an .accdb file is queried \
                 through its MSysObjects catalog table"
            )));
        }

        let projected_schema = project_schema(&table_schema, projection)?;

        let sql = RemoteDbType::Access.rewrite_query(source, unparsed_filters, limit)?;
        debug!("[remote-table] executing access query: {sql}");

        let chunk_size = conn_options.stream_chunk_size();
        let conn = Arc::clone(&self.conn);
        let projection = projection.cloned();
        let table_schema = Arc::clone(&table_schema);
        let (batch_tx, mut batch_rx) = tokio::sync::mpsc::channel::<RecordBatch>(4);

        let join_handle = tokio::task::spawn_blocking(move || {
            let handle = Handle::current();
            let conn = handle.block_on(async { conn.lock().await });

            let mut cursor = conn
                .execute(&sql, (), None)
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to execute query on access: {e:?}, sql: {sql}"
                    ))
                })?
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "No result set returned for access query: {sql}"
                    ))
                })?;

            let mut exhausted = false;

            loop {
                let mut row_count = 0;
                let mut builders: Vec<Box<dyn arrow::array::ArrayBuilder>> = table_schema
                    .fields()
                    .iter()
                    .map(|f| make_builder(f.data_type(), chunk_size))
                    .collect();
                while row_count < chunk_size {
                    match cursor.next_row() {
                        Ok(Some(row)) => {
                            append_row_to_builders(&mut builders, row, &table_schema)?;
                            row_count += 1;
                        }
                        Ok(None) => {
                            exhausted = true;
                            break;
                        }
                        Err(e) => {
                            return Err(DataFusionError::External(Box::new(e)));
                        }
                    }
                }
                if row_count > 0 {
                    let batch =
                        finish_batch(builders, &table_schema, projection.as_ref(), row_count)?;
                    batch_tx.blocking_send(batch).map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to send batch from access: {e:?}"
                        ))
                    })?;
                }
                if exhausted {
                    break;
                }
            }

            Ok::<_, DataFusionError>(())
        });

        let output_stream = async_stream::stream! {
            while let Some(batch) = batch_rx.recv().await {
                yield Ok(batch);
            }

            match join_handle.await {
                Ok(Ok(())) => {},
                Ok(Err(e)) => yield Err(e),
                Err(e) => yield Err(DataFusionError::Execution(format!(
                    "Failed to execute ODBC query on access: {e}"
                ))),
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            projected_schema,
            output_stream,
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
        Err(DataFusionError::Execution(
            "Insert operation is not supported for access".to_string(),
        ))
    }

    async fn count(
        &self,
        conn_options: &ConnectionOptions,
        source: &RemoteSource,
        unparsed_filters: &[String],
    ) -> DFResult<Option<usize>> {
        if let RemoteSource::Command(cmd) = source {
            return Err(DataFusionError::NotImplemented(format!(
                "{cmd:?} is only supported for mdb sources; an .accdb file is queried \
                 through its MSysObjects catalog table"
            )));
        }

        let db_type = conn_options.db_type();
        let source = if unparsed_filters.is_empty() {
            source.clone()
        } else {
            RemoteSource::Query(db_type.rewrite_query(source, unparsed_filters, None)?)
        };
        // Access only supports COUNT on table sources
        if let RemoteSource::Table(table) = &source {
            let count_query = db_type.select_all_query(table);
            debug!("[remote-table] fetching Access row count with query: {count_query}");
            let row_count = self.fetch_table_row_count(&count_query).await?;
            Ok(Some(row_count))
        } else {
            Ok(None)
        }
    }
}

impl AccessConnection {
    async fn fetch_table_row_count(&self, count_query: &str) -> DFResult<usize> {
        let conn = Arc::clone(&self.conn);
        let count_query = count_query.to_string();
        tokio::task::spawn_blocking(move || {
            let handle = Handle::current();
            let conn = handle.block_on(async { conn.lock().await });

            // mdbtools ODBC returns 0 for aggregate COUNT(*) even though
            // mdb-sql returns the correct value. Count table rows by iterating
            // the cursor instead.
            let mut cursor = conn
                .execute(&count_query, (), None)
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to execute Access row count query: {e:?}, sql: {count_query}"
                    ))
                })?
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "No result set for Access row count query: {count_query}"
                    ))
                })?;

            let mut row_count = 0usize;
            loop {
                match cursor.next_row() {
                    Ok(Some(_row)) => {
                        row_count += 1;
                    }
                    Ok(None) => break,
                    Err(e) => {
                        return Err(DataFusionError::Execution(format!(
                            "Failed fetching Access row count: {e}"
                        )));
                    }
                }
            }

            Ok(row_count)
        })
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!("Failed to join Access row count task: {e}"))
        })?
    }
}
