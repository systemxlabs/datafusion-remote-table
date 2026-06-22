mod row;
mod schema;

use crate::connection::ODBC_ENV;
use crate::{
    Connection, ConnectionOptions, DFResult, Literalize, MdbConnectionOptions, MdbType, Pool,
    PoolState, RemoteDbType, RemoteField, RemoteSchema, RemoteSchemaRef, RemoteSource, RemoteType,
    SourceCommand,
};
use arrow::array::ArrayRef;
use arrow::array::RecordBatch;
use arrow::array::StringBuilder;
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
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::runtime::Handle;

use row::append_row_to_builders;
use row::finish_batch;
use schema::build_remote_schema;

fn list_tables_remote_schema() -> RemoteSchema {
    RemoteSchema::new(vec![
        RemoteField::new(
            "table_name",
            RemoteType::Mdb(MdbType::Text(Some(255))),
            false,
        ),
        RemoteField::new(
            "table_type",
            RemoteType::Mdb(MdbType::Text(Some(50))),
            false,
        ),
    ])
}

/// Cache key that captures the full ODBC connection identity, not just the
/// `.mdb` file path. Two pools that target the same path but use different
/// drivers, credentials, or extra connection parameters must NOT share a
/// connection.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MdbConnectionCacheKey {
    path: PathBuf,
    driver: String,
    uid: Option<String>,
    pwd: Option<String>,
    // Sorted so that semantically identical parameter sets (differing only in
    // insertion order) produce the same key.
    extra_params_sorted: Vec<(String, String)>,
}

impl MdbConnectionCacheKey {
    fn from_options(options: &MdbConnectionOptions) -> Self {
        let mut extra_params_sorted = options.extra_params.clone();
        extra_params_sorted.sort();
        Self {
            path: options.path.clone(),
            driver: options.driver.clone(),
            uid: options.uid.clone(),
            pwd: options.pwd.clone(),
            extra_params_sorted,
        }
    }
}

/// Per-path global ODBC connection cache.
///
/// mdbtools' `libmdbodbc.so` keeps process-global state and corrupts it after
/// a handful of successive `SQLDriverConnect` calls to the same `.mdb` file
/// (observed symptom: `SQLDriverConnect: NoDiagnostics`, stderr says
/// "File not found" while the file is plainly on disk). To work around this,
/// every `MdbPool` that targets the same connection identity (path + driver +
/// uid + pwd + extra_params) shares a single underlying `odbc_api::Connection`.
/// Concurrent access on that shared connection is still serialised by
/// `MdbConnection`'s own mutex.
///
/// Cached connections live until process exit. Bounded by the number of
/// distinct connection identities the process touches.
static MDB_CONN_CACHE: OnceLock<
    std::sync::Mutex<HashMap<MdbConnectionCacheKey, Arc<Mutex<odbc_api::Connection<'static>>>>>,
> = OnceLock::new();

fn mdb_conn_cache() -> &'static std::sync::Mutex<
    HashMap<MdbConnectionCacheKey, Arc<Mutex<odbc_api::Connection<'static>>>>,
> {
    MDB_CONN_CACHE.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

pub struct MdbPool {
    options: MdbConnectionOptions,
    connections: Arc<AtomicUsize>,
}

impl std::fmt::Debug for MdbPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MdbPool")
            .field("options", &self.options)
            .field("connections", &self.connections)
            .finish()
    }
}

pub(crate) fn connect_mdb(options: &MdbConnectionOptions) -> DFResult<MdbPool> {
    Ok(MdbPool {
        options: options.clone(),
        connections: Arc::new(AtomicUsize::new(0)),
    })
}

#[async_trait::async_trait]
impl Pool for MdbPool {
    async fn get(&self) -> DFResult<Arc<dyn Connection>> {
        let cache_key = MdbConnectionCacheKey::from_options(&self.options);

        // Consult the global cache (see MDB_CONN_CACHE). The cache lock is
        // held across the SQLDriverConnect call so two pools racing on the
        // same connection identity don't both open a connection.
        let conn = {
            let mut cache = mdb_conn_cache().lock().unwrap();
            if let Some(existing) = cache.get(&cache_key) {
                existing.clone()
            } else {
                let env =
                    ODBC_ENV.get_or_init(|| Environment::new().expect("failed to create ODBC env"));
                let connection_str = self.options.connection_string();
                debug!("[remote-table] mdb connection string: {connection_str}");
                let connection = env
                    .connect_with_connection_string(
                        &connection_str,
                        odbc_api::ConnectionOptions::default(),
                    )
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to create odbc connection to mdb: {e:?}"
                        ))
                    })?;
                let conn = Arc::new(Mutex::new(connection));
                cache.insert(cache_key, conn.clone());
                conn
            }
        };

        self.connections.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(MdbConnection {
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
pub struct MdbConnection {
    conn: Arc<Mutex<odbc_api::Connection<'static>>>,
    pool_connections: Arc<AtomicUsize>,
}

impl Drop for MdbConnection {
    fn drop(&mut self) {
        self.pool_connections.fetch_sub(1, Ordering::SeqCst);
    }
}
#[async_trait::async_trait]
impl Connection for MdbConnection {
    async fn infer_schema(&self, source: &RemoteSource) -> DFResult<RemoteSchemaRef> {
        if matches!(source, RemoteSource::Command(SourceCommand::ListMdbTables)) {
            return Ok(Arc::new(list_tables_remote_schema()));
        }

        let sql = RemoteDbType::Mdb.limit_1_query_if_possible(source)?;
        debug!("[remote-table] inferring mdb schema with: {sql}");
        let conn = self.conn.lock().await;
        let cursor_opt = conn.execute(&sql, (), None).map_err(|e| {
            DataFusionError::Plan(format!(
                "Failed to execute query for schema inference on mdb: {e:?}, sql: {sql}"
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
        if matches!(source, RemoteSource::Command(SourceCommand::ListMdbTables)) {
            return self
                .query_list_tables_impl(table_schema, projection, limit)
                .await;
        }

        let projected_schema = project_schema(&table_schema, projection)?;

        let sql = RemoteDbType::Mdb.rewrite_query(source, unparsed_filters, limit)?;
        debug!("[remote-table] executing mdb query: {sql}");

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
                        "Failed to execute query on mdb: {e:?}, sql: {sql}"
                    ))
                })?
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "No result set returned for mdb query: {sql}"
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
                        DataFusionError::Execution(format!("Failed to send batch from mdb: {e:?}"))
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
                    "Failed to execute ODBC query on mdb: {e}"
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
            "Insert operation is not supported for mdb".to_string(),
        ))
    }

    async fn count(
        &self,
        conn_options: &ConnectionOptions,
        source: &RemoteSource,
        unparsed_filters: &[String],
    ) -> DFResult<Option<usize>> {
        if matches!(source, RemoteSource::Command(SourceCommand::ListMdbTables)) {
            let tables = self.list_tables().await?;
            return Ok(Some(tables.len()));
        }

        let db_type = conn_options.db_type();
        let source = if unparsed_filters.is_empty() {
            source.clone()
        } else {
            RemoteSource::Query(db_type.rewrite_query(source, unparsed_filters, None)?)
        };
        // MDB only supports COUNT on table sources
        if let RemoteSource::Table(table) = &source {
            let count_query = db_type.select_all_query(table);
            debug!("[remote-table] fetching MDB row count with query: {count_query}");
            let row_count = self.fetch_table_row_count(&count_query).await?;
            Ok(Some(row_count))
        } else {
            Ok(None)
        }
    }
}

impl MdbConnection {
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
                        "Failed to execute MDB row count query: {e:?}, sql: {count_query}"
                    ))
                })?
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "No result set for MDB row count query: {count_query}"
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
                            "Failed fetching MDB row count: {e}"
                        )));
                    }
                }
            }

            Ok(row_count)
        })
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!("Failed to join MDB row count task: {e}"))
        })?
    }

    /// Core: query MSysObjects on an already-locked connection.
    /// Returns (table_name, table_type), system tables filtered out.
    ///
    /// Queries the MDB system table directly via `conn.execute()` instead of
    /// the ODBC catalog function `SQLTables`, avoiding the need for unsafe
    /// `CursorImpl::new`. MSysObjects.Type values: 1=local table, 4=linked
    /// ODBC, 5=query/view, 6=linked table.
    fn list_tables_sync(conn: &odbc_api::Connection<'_>) -> DFResult<Vec<(String, String)>> {
        let mut cursor = conn
            .execute(
                "SELECT Name, Type FROM MSysObjects WHERE Type IN (1,4,5,6)",
                (),
                None,
            )
            .map_err(|e| {
                DataFusionError::Execution(format!("Failed to query MSysObjects on mdb: {e:?}"))
            })?
            .ok_or_else(|| {
                DataFusionError::Execution("No result from MSysObjects query".to_string())
            })?;

        let mut tables = Vec::new();
        let mut name_buf: Vec<u8> = Vec::new();
        let mut type_buf: Vec<u8> = Vec::new();

        loop {
            match cursor.next_row() {
                Ok(Some(mut row)) => {
                    // Column 1 = Name
                    name_buf.clear();
                    if !row.get_text(1, &mut name_buf).unwrap_or(false) {
                        continue;
                    }
                    let table_name = String::from_utf8_lossy(&name_buf).into_owned();

                    // Filter system/internal tables
                    if table_name.starts_with("MSys") || table_name.starts_with("~") {
                        continue;
                    }

                    // Column 2 = Type: 5 = query/view
                    type_buf.clear();
                    let is_view = row.get_text(2, &mut type_buf).unwrap_or(false)
                        && String::from_utf8_lossy(&type_buf).trim() == "5";

                    let display_type = if is_view { "View" } else { "Table" };
                    tables.push((table_name, display_type.to_string()));
                }
                Ok(None) => break,
                Err(e) => {
                    return Err(DataFusionError::External(Box::new(e)));
                }
            }
        }

        debug!("[remote-table] list_tables found {} tables", tables.len());
        Ok(tables)
    }

    /// List user tables in the MDB file using the cached ODBC connection.
    pub async fn list_tables(&self) -> DFResult<Vec<(String, String)>> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let handle = Handle::current();
            let guard = handle.block_on(async { conn.lock().await });
            Self::list_tables_sync(&guard)
        })
        .await
        .map_err(|e| DataFusionError::Execution(format!("Failed to join list_tables task: {e}")))?
    }

    /// Build a RecordBatch stream from the in-memory table list.
    async fn query_list_tables_impl(
        &self,
        table_schema: SchemaRef,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> DFResult<SendableRecordBatchStream> {
        let mut tables = self.list_tables().await?;
        if let Some(lim) = limit {
            tables.truncate(lim);
        }

        let mut name_builder = StringBuilder::new();
        let mut type_builder = StringBuilder::new();
        for (name, typ) in &tables {
            name_builder.append_value(name);
            type_builder.append_value(typ);
        }

        let full_schema = Arc::new(list_tables_remote_schema().to_arrow_schema());
        let batch = RecordBatch::try_new(
            full_schema.clone(),
            vec![
                Arc::new(name_builder.finish()),
                Arc::new(type_builder.finish()),
            ],
        )?;

        let projected_schema = project_schema(&table_schema, projection)?;
        let output_batch = match projection {
            Some(indices) => {
                let columns: Vec<ArrayRef> =
                    indices.iter().map(|&i| batch.column(i).clone()).collect();
                RecordBatch::try_new(projected_schema.clone(), columns)?
            }
            None => batch,
        };

        let output_stream = async_stream::stream! {
            yield Ok(output_batch);
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            projected_schema,
            output_stream,
        )))
    }
}
