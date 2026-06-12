mod row;
mod schema;

use crate::connection::ODBC_ENV;
use crate::{
    Connection, ConnectionOptions, DFResult, Literalize, MdbConnectionOptions, Pool, PoolState,
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
use odbc_api::Environment;
use odbc_api::handles::{SqlResult, SqlText, Statement, StatementImpl};
use odbc_api::{Cursor, CursorImpl};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::runtime::Handle;

use row::append_row_to_builders;
use row::finish_batch;
use schema::build_remote_schema;

#[derive(Debug)]
pub struct MdbPool {
    connection_string: String,
    connections: Arc<AtomicUsize>,
}

pub(crate) fn connect_mdb(options: &MdbConnectionOptions) -> DFResult<MdbPool> {
    Ok(MdbPool {
        connection_string: options.connection_string(),
        connections: Arc::new(AtomicUsize::new(0)),
    })
}

#[async_trait::async_trait]
impl Pool for MdbPool {
    async fn get(&self) -> DFResult<Arc<dyn Connection>> {
        self.connections.fetch_add(1, Ordering::SeqCst);
        let env = ODBC_ENV.get_or_init(|| Environment::new().expect("failed to create ODBC env"));
        debug!(
            "[remote-table] mdb connection string: {}",
            self.connection_string
        );
        let connection = env
            .connect_with_connection_string(
                &self.connection_string,
                odbc_api::ConnectionOptions::default(),
            )
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to create odbc connection to mdb: {e:?}"
                ))
            })?;
        let conn = Arc::new(Mutex::new(connection));
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
        let sql = RemoteDbType::Mdb.limit_1_query_if_possible(source);
        debug!("[remote-table] inferring mdb schema with: {sql}");
        let conn = self.conn.lock().await;
        // mdbtools 1.0.0 does not support SQL_ATTR_PARAMSET_SIZE, and
        // `SQLExecDirect` + `SQLFetch` hangs. Use SQLPrepare + SQLExecute.
        let pre = conn.preallocate().map_err(|e| {
            DataFusionError::Plan(format!(
                "Failed to preallocate statement for schema inference on mdb: {e:?}, sql: {sql}"
            ))
        })?;
        let mut stmt = pre.into_handle();
        let sql_text = SqlText::new(&sql);
        // SAFETY: `sql` is owned by this stack frame and outlives `stmt` (which
        // is consumed by `CursorImpl::new` below), so the `SqlText` borrow is
        // valid for the duration of the prepare + execute. `prepare` is safe;
        // `execute` is unsafe (may deref bound parameters; we have none).
        if stmt.prepare(&sql_text).is_err() {
            return Err(DataFusionError::Plan(format!(
                "Failed to prepare query for schema inference on mdb: {sql}"
            )));
        }
        if unsafe { stmt.execute() }.is_err() {
            return Err(DataFusionError::Plan(format!(
                "Failed to execute query for schema inference on mdb: {sql}"
            )));
        }
        // SAFETY: `stmt` is in cursor state after a successful execute that
        // produced a result set; we verify with num_result_cols() in
        // build_remote_schema.
        let cursor = unsafe { CursorImpl::new(stmt) };
        let remote_schema = Arc::new(build_remote_schema(cursor)?);
        Ok(remote_schema)
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
        let projected_schema = project_schema(&table_schema, projection)?;

        let sql = RemoteDbType::Mdb.rewrite_query(source, unparsed_filters, limit);
        debug!("[remote-table] executing mdb query: {sql}");

        let chunk_size = conn_options.stream_chunk_size();
        let conn = Arc::clone(&self.conn);
        let projection = projection.cloned();
        let table_schema = Arc::clone(&table_schema);
        let (batch_tx, mut batch_rx) = tokio::sync::mpsc::channel::<RecordBatch>(4);

        let join_handle = tokio::task::spawn_blocking(move || {
            let handle = Handle::current();
            let conn = handle.block_on(async { conn.lock().await });

            // mdbtools does not support SQL_ATTR_PARAMSET_SIZE, so odbc-api's safe
            // `conn.execute()` fails with HY092. Use SQLExecDirect (like the
            // mdbtools C unit test does) followed by SQLBindCol + SQLFetch.
            let pre = conn.preallocate().map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to preallocate statement for mdb query: {e:?}"
                ))
            })?;
            let mut stmt = pre.into_handle();
            let sql_text = SqlText::new(&sql);
            // SAFETY: `sql` is owned by this closure and outlives `stmt`.
            // `exec_direct` is unsafe (may dereference bound parameters; we have none).
            if unsafe { stmt.exec_direct(&sql_text) }.is_err() {
                return Err(DataFusionError::Execution(format!(
                    "Failed to execute query on mdb: {sql}"
                )));
            }
            // mdbtools 1.0.x requires SQLBindCol before SQLFetch — without it,
            // SQLFetch hangs indefinitely. odbc-api's Cursor::next_row() calls
            // SQLFetch without binding columns, so we bypass CursorImpl entirely
            // and use the Statement trait's bind_col() + fetch() directly.
            // Each column is bound to a single-row buffer; each fetch() call
            // writes one row into the bound buffers.

            // mdbtools' SQLFetch requires at least one column to be bound with
            // SQLBindCol before the first SQLFetch call, otherwise it hangs.
            // However, with all columns bound, SQLFetch never returns SQL_NO_DATA.
            //
            // Workaround: bind a single dummy column (column 1) to allow
            // SQLFetch to proceed, then use the row-by-row Cursor::next_row()
            // path which calls SQLGetData for each cell. SQLGetData after
            // SQLBindCol works because the dummy buffer is ignored.
            let mut dummy = odbc_api::Nullable::<i32>::null();
            match unsafe { stmt.bind_col(1, &mut dummy) } {
                SqlResult::Success(()) | SqlResult::SuccessWithInfo(()) => {}
                SqlResult::Error { function } => {
                    return Err(DataFusionError::Execution(format!(
                        "{function} failed binding dummy column for mdb"
                    )));
                }
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "Unexpected result binding dummy column: {other:?}"
                    )));
                }
            }

            // SAFETY: stmt is in cursor state after exec_direct.
            let mut cursor: CursorImpl<StatementImpl> = unsafe { CursorImpl::new(stmt) };
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
        let db_type = conn_options.db_type();
        let source = if unparsed_filters.is_empty() {
            source.clone()
        } else {
            RemoteSource::Query(db_type.rewrite_query(source, unparsed_filters, None))
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
            let pre = conn.preallocate().map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to preallocate statement for MDB row count: {e:?}"
                ))
            })?;
            let mut stmt = pre.into_handle();
            let sql_text = SqlText::new(&count_query);
            // SAFETY: `count_query` is owned by this closure and outlives `stmt`.
            if unsafe { stmt.exec_direct(&sql_text) }.is_err() {
                return Err(DataFusionError::Execution(format!(
                    "Failed to execute MDB row count query: {count_query}"
                )));
            }

            // mdbtools ODBC returns 0 for aggregate COUNT(*) even though
            // mdb-sql returns the correct value. Count table rows by fetching
            // the first column instead; this keeps COUNT pushdown table-only
            // without materializing Arrow batches.
            let mut dummy = odbc_api::Nullable::<i32>::null();
            match unsafe { stmt.bind_col(1, &mut dummy) } {
                SqlResult::Success(()) | SqlResult::SuccessWithInfo(()) => {}
                SqlResult::Error { function } => {
                    return Err(DataFusionError::Execution(format!(
                        "{function} failed binding MDB row-count column"
                    )));
                }
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "Unexpected result binding MDB row-count column: {other:?}"
                    )));
                }
            }

            let mut row_count = 0usize;
            loop {
                match unsafe { stmt.fetch() } {
                    SqlResult::Success(()) | SqlResult::SuccessWithInfo(()) => {
                        row_count += 1;
                    }
                    SqlResult::NoData => break,
                    SqlResult::Error { function } => {
                        return Err(DataFusionError::Execution(format!(
                            "{function} failed fetching MDB row count"
                        )));
                    }
                    other => {
                        return Err(DataFusionError::Execution(format!(
                            "Unexpected result fetching MDB row count: {other:?}"
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
}

/// List user tables in an MDB file using the ODBC `SQLTables` function.
///
/// Returns `Vec<(table_name, table_type)>` where `table_type` is `"Table"` or `"View"`.
/// System tables (prefixed with `MSys`) are filtered out.
pub fn mdb_list_tables(options: &MdbConnectionOptions) -> DFResult<Vec<(String, String)>> {
    let env = ODBC_ENV.get_or_init(|| Environment::new().expect("failed to create ODBC env"));
    let connection_str = options.connection_string();
    debug!("[remote-table] mdb_list_tables connection string: {connection_str}");
    let conn = env
        .connect_with_connection_string(&connection_str, odbc_api::ConnectionOptions::default())
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to create odbc connection for mdb_list_tables: {e:?}"
            ))
        })?;

    let pre = conn.preallocate().map_err(|e| {
        DataFusionError::Execution(format!(
            "Failed to preallocate statement for mdb_list_tables: {e:?}"
        ))
    })?;
    let mut stmt = pre.into_handle();

    // Call SQLTables via the Statement trait
    let sql_cat = SqlText::new("");
    let sql_sch = SqlText::new("");
    let sql_tbl = SqlText::new("%");
    let sql_typ = SqlText::new("TABLE,VIEW");
    stmt.tables(&sql_cat, &sql_sch, &sql_tbl, &sql_typ)
        .into_result(&stmt)
        .map_err(|e| {
            DataFusionError::Execution(format!("Failed to execute SQLTables on mdb: {e:?}"))
        })?;

    // Bind dummy column (mdbtools workaround: SQLFetch hangs without at
    // least one bound column, same as the query path).
    let mut dummy = odbc_api::Nullable::<i32>::null();
    match unsafe { Statement::bind_col(&mut stmt, 1, &mut dummy) } {
        SqlResult::Success(()) | SqlResult::SuccessWithInfo(()) => {}
        other => {
            return Err(DataFusionError::Execution(format!(
                "Failed to bind dummy column for mdb_list_tables: {other:?}"
            )));
        }
    }

    // SAFETY: stmt is in cursor state after a successful tables() call.
    let mut cursor: CursorImpl<StatementImpl> = unsafe { CursorImpl::new(stmt) };
    let mut tables = Vec::new();
    let mut text_buf: Vec<u8> = Vec::new();

    loop {
        match cursor.next_row() {
            Ok(Some(mut row)) => {
                // Column 3 = TABLE_NAME
                text_buf.clear();
                if row.get_text(3, &mut text_buf).unwrap_or(false) {
                    let table_name = String::from_utf8_lossy(&text_buf).into_owned();

                    // Column 4 = TABLE_TYPE
                    text_buf.clear();
                    let table_type = if row.get_text(4, &mut text_buf).unwrap_or(false) {
                        String::from_utf8_lossy(&text_buf).into_owned()
                    } else {
                        "TABLE".to_string()
                    };

                    // Filter system/internal tables
                    if !table_name.starts_with("MSys") && !table_name.starts_with("~") {
                        let display_type = if table_type.eq_ignore_ascii_case("VIEW") {
                            "View"
                        } else {
                            "Table"
                        };
                        tables.push((table_name, display_type.to_string()));
                    }
                }
            }
            Ok(None) => break,
            Err(e) => {
                return Err(DataFusionError::External(Box::new(e)));
            }
        }
    }

    debug!(
        "[remote-table] mdb_list_tables found {} tables",
        tables.len()
    );
    Ok(tables)
}
