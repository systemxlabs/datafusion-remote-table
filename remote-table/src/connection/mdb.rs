use crate::connection::ODBC_ENV;
use crate::connection::odbc_util::us_since_epoch;
use crate::{
    Connection, ConnectionOptions, DFResult, Literalize, MdbConnectionOptions, MdbType, Pool,
    PoolState, RemoteDbType, RemoteField, RemoteSchema, RemoteSchemaRef, RemoteSource, RemoteType,
};
use arrow::array::{
    ArrayRef, BinaryBuilder, BinaryViewBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder,
    Int32Builder, Int64Builder, RecordBatch, RecordBatchOptions, StringBuilder, StringViewBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder, make_builder,
};
use arrow::datatypes::{DataType, Date32Type, SchemaRef, TimeUnit};
use chrono::{NaiveDate, NaiveTime, Timelike};
use datafusion_common::DataFusionError;
use datafusion_common::project_schema;
use datafusion_execution::SendableRecordBatchStream;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::lock::Mutex;
use log::debug;
use odbc_api::buffers::{BufferDesc, ColumnarDynBuffer};
use odbc_api::handles::{
    AsStatementRef, ColumnDescription, SqlResult, SqlText, Statement, StatementImpl,
};
use odbc_api::{Cursor, CursorImpl, Environment, ResultSetMetadata, decimal_text_to_i128};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::runtime::Handle;

use crate::connection::projections_contains;

/// Per-path global ODBC connection cache.
///
/// mdbtools' `libmdbodbc.so` keeps process-global state and corrupts it after
/// a handful of successive `SQLDriverConnect` calls to the same `.mdb` file
/// (observed symptom: `SQLDriverConnect: NoDiagnostics`, stderr says
/// "File not found" while the file is plainly on disk). To work around this,
/// every `MdbPool` that targets the same file path shares a single underlying
/// `odbc_api::Connection`. Concurrent access on that shared connection is
/// still serialised by `MdbConnection`'s own mutex.
///
/// Cached connections live until process exit. Bounded by the number of
/// distinct `.mdb` paths the process touches.
static MDB_CONN_CACHE: OnceLock<
    std::sync::Mutex<HashMap<PathBuf, Arc<Mutex<odbc_api::Connection<'static>>>>>,
> = OnceLock::new();

fn mdb_conn_cache()
-> &'static std::sync::Mutex<HashMap<PathBuf, Arc<Mutex<odbc_api::Connection<'static>>>>> {
    MDB_CONN_CACHE.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

pub struct MdbPool {
    options: MdbConnectionOptions,
    connections: Arc<AtomicUsize>,
    cached_conn: std::sync::Mutex<Option<Arc<Mutex<odbc_api::Connection<'static>>>>>,
}

impl std::fmt::Debug for MdbPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MdbPool")
            .field("options", &self.options)
            .field("connections", &self.connections)
            .field(
                "cached_conn",
                &if self.cached_conn.lock().unwrap().is_some() {
                    "Some(Connection)"
                } else {
                    "None"
                },
            )
            .finish()
    }
}

pub(crate) fn connect_mdb(options: &MdbConnectionOptions) -> DFResult<MdbPool> {
    Ok(MdbPool {
        options: options.clone(),
        connections: Arc::new(AtomicUsize::new(0)),
        cached_conn: std::sync::Mutex::new(None),
    })
}

#[async_trait::async_trait]
impl Pool for MdbPool {
    async fn get(&self) -> DFResult<Arc<dyn Connection>> {
        // Fast path: this pool has already handed out a connection.
        if let Some(cached) = self.cached_conn.lock().unwrap().as_ref() {
            self.connections.fetch_add(1, Ordering::SeqCst);
            return Ok(Arc::new(MdbConnection {
                conn: cached.clone(),
                pool_connections: self.connections.clone(),
            }));
        }

        // Slow path: consult the per-path global cache (see MDB_CONN_CACHE).
        // The cache lock is held across the SQLDriverConnect call so two
        // pools racing on the same path don't both open a connection.
        let conn = {
            let mut cache = mdb_conn_cache().lock().unwrap();
            if let Some(existing) = cache.get(&self.options.path) {
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
                cache.insert(self.options.path.clone(), conn.clone());
                conn
            }
        };

        // Mirror into the per-pool cache so the fast path hits next time.
        *self.cached_conn.lock().unwrap() = Some(conn.clone());

        self.connections.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(MdbConnection {
            conn,
            pool_connections: self.connections.clone(),
        }))
    }

    async fn state(&self) -> DFResult<PoolState> {
        let active = self.connections.load(Ordering::SeqCst);
        let has_cached = self.cached_conn.lock().unwrap().is_some();
        Ok(PoolState {
            connections: active,
            idle_connections: if has_cached && active == 0 { 1 } else { 0 },
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
}

fn build_remote_schema(mut cursor: CursorImpl<StatementImpl>) -> DFResult<RemoteSchema> {
    let col_count = cursor
        .num_result_cols()
        .map_err(|e| DataFusionError::External(Box::new(e)))? as u16;
    let mut remote_fields = vec![];
    for i in 1..=col_count {
        // mdbtools' libmdbodbc.so doesn't fully support the higher-level
        // SQLColAttribute wrappers (cursor.col_name / col_data_type /
        // col_nullability return NoDiagnostics), so we go through the
        // low-level SQLDescribeCol path.
        let mut col_desc = ColumnDescription::default();
        let describe_result = cursor.as_stmt_ref().describe_col(i, &mut col_desc);
        if describe_result.is_err() {
            return Err(DataFusionError::Plan(format!(
                "describe_col failed for column {i} on mdb"
            )));
        }
        // mdbtools reports column names in the MDB file's code page
        // (CP1252/CP936/etc.), not UTF-8. from_utf8_lossy keeps the printable
        // bytes and substitutes U+FFFD for invalid ones.
        let col_name = String::from_utf8_lossy(&col_desc.name).into_owned();
        let col_nullable = col_desc.nullability.could_be_nullable();

        let remote_type = RemoteType::Mdb(mdb_type_to_remote_type(col_desc.data_type)?);
        remote_fields.push(RemoteField::new(col_name, remote_type, col_nullable));
    }

    Ok(RemoteSchema::new(remote_fields))
}

fn mdb_type_to_remote_type(data_type: odbc_api::DataType) -> DFResult<MdbType> {
    match data_type {
        odbc_api::DataType::Bit => Ok(MdbType::Bit),
        odbc_api::DataType::TinyInt => Ok(MdbType::TinyInt),
        odbc_api::DataType::SmallInt => Ok(MdbType::SmallInt),
        odbc_api::DataType::Integer => Ok(MdbType::Integer),
        odbc_api::DataType::Real => Ok(MdbType::Real),
        odbc_api::DataType::Double => Ok(MdbType::Double),
        odbc_api::DataType::Numeric { .. } | odbc_api::DataType::Decimal { .. } => {
            Ok(MdbType::Currency)
        }
        odbc_api::DataType::Char { length } | odbc_api::DataType::WVarchar { length } => {
            Ok(MdbType::Text(length.map(|l| l.get() as u16)))
        }
        odbc_api::DataType::Varchar { length } => Ok(MdbType::Text(length.map(|l| l.get() as u16))),
        odbc_api::DataType::LongVarchar { .. } | odbc_api::DataType::WLongVarchar { .. } => {
            Ok(MdbType::Memo)
        }
        odbc_api::DataType::Binary { length } | odbc_api::DataType::Varbinary { length } => {
            Ok(MdbType::Binary(length.map(|l| l.get() as u16)))
        }
        odbc_api::DataType::LongVarbinary { .. } => Ok(MdbType::OleObject),
        odbc_api::DataType::Timestamp { .. } => Ok(MdbType::DateTime),
        odbc_api::DataType::Date => Ok(MdbType::Date),
        odbc_api::DataType::Time { .. } => Ok(MdbType::Time),
        odbc_api::DataType::WChar { length } => Ok(MdbType::Text(length.map(|l| l.get() as u16))),
        odbc_api::DataType::Other { data_type, .. }
            if data_type == odbc_api::sys::SqlDataType::EXT_GUID =>
        {
            Ok(MdbType::Guid)
        }
        _ => Err(DataFusionError::Execution(format!(
            "Unsupported MDB type: {data_type:?}"
        ))),
    }
}

macro_rules! read_data {
    ($builder:expr, $field:expr, $builder_ty:ty, $row:expr, $col_idx:expr, $value_ty:ty, $convert:expr) => {{
        let builder = $builder
            .as_any_mut()
            .downcast_mut::<$builder_ty>()
            .unwrap_or_else(|| {
                panic!(
                    "Failed to downcast builder to {} for {:?}",
                    stringify!($builder_ty),
                    $field,
                )
            });
        let mut value = odbc_api::Nullable::<$value_ty>::null();
        $row.get_data($col_idx, &mut value).map_err(|e| {
            DataFusionError::Execution(format!("Failed to get value for field {:?}: {e:?}", $field))
        })?;
        let value = value.into_opt();
        match value {
            Some(v) => builder.append_value($convert(v)?),
            None => builder.append_null(),
        }
    }};
}

macro_rules! read_text {
    ($builder:expr, $field:expr, $builder_ty:ty, $row:expr, $col_idx:expr, $buf:expr, $convert:expr) => {{
        let builder = $builder
            .as_any_mut()
            .downcast_mut::<$builder_ty>()
            .unwrap_or_else(|| {
                panic!(
                    "Failed to downcast builder to {} for {:?}",
                    stringify!($builder_ty),
                    $field,
                )
            });
        $buf.clear();
        let is_not_null = $row.get_text($col_idx, $buf).map_err(|e| {
            DataFusionError::Execution(format!("Failed to get value for field {:?}: {e:?}", $field))
        })?;
        if is_not_null {
            // mdbtools returns text in the MDB file's code page (CP1252/CP936/etc.),
            // not UTF-8. from_utf8_lossy keeps the printable bytes and substitutes
            // U+FFFD for invalid ones, so non-ASCII cells don't abort the batch.
            let value = String::from_utf8_lossy($buf).into_owned();
            builder.append_value($convert(value)?);
        } else {
            builder.append_null();
        }
    }};
}

#[allow(dead_code)]
fn append_row_to_builders(
    builders: &mut [Box<dyn arrow::array::ArrayBuilder>],
    mut row: odbc_api::CursorRow,
    table_schema: &SchemaRef,
) -> DFResult<()> {
    // Reuse one Vec per ODBC cell type across all columns/rows in this call so
    // we don't allocate a new buffer for every cell.
    let mut text_buf: Vec<u8> = Vec::new();
    let mut binary_buf: Vec<u8> = Vec::new();
    for (idx, field) in table_schema.fields().iter().enumerate() {
        let builder = &mut builders[idx];
        let odbc_col_idx = (idx + 1) as u16;
        match field.data_type() {
            DataType::Boolean => {
                read_data!(
                    builder,
                    field,
                    BooleanBuilder,
                    row,
                    odbc_col_idx,
                    odbc_api::Bit,
                    |v: odbc_api::Bit| { Ok::<_, DataFusionError>(v.as_bool()) }
                );
            }
            DataType::Int8 => {
                read_data!(
                    builder,
                    field,
                    Int8Builder,
                    row,
                    odbc_col_idx,
                    i8,
                    |v: i8| Ok::<_, DataFusionError>(v)
                );
            }
            DataType::Int16 => {
                read_data!(
                    builder,
                    field,
                    Int16Builder,
                    row,
                    odbc_col_idx,
                    i16,
                    |v: i16| Ok::<_, DataFusionError>(v)
                );
            }
            DataType::Int32 => {
                read_data!(
                    builder,
                    field,
                    Int32Builder,
                    row,
                    odbc_col_idx,
                    i32,
                    |v: i32| Ok::<_, DataFusionError>(v)
                );
            }
            DataType::Int64 => {
                read_data!(
                    builder,
                    field,
                    Int64Builder,
                    row,
                    odbc_col_idx,
                    i64,
                    |v: i64| Ok::<_, DataFusionError>(v)
                );
            }
            DataType::Float32 => {
                read_data!(
                    builder,
                    field,
                    Float32Builder,
                    row,
                    odbc_col_idx,
                    f32,
                    |v: f32| Ok::<_, DataFusionError>(v)
                );
            }
            DataType::Float64 => {
                read_data!(
                    builder,
                    field,
                    Float64Builder,
                    row,
                    odbc_col_idx,
                    f64,
                    |v: f64| Ok::<_, DataFusionError>(v)
                );
            }
            DataType::Decimal128(_precision, scale) => {
                read_text!(
                    builder,
                    field,
                    Decimal128Builder,
                    row,
                    odbc_col_idx,
                    &mut text_buf,
                    |v: String| {
                        Ok::<_, DataFusionError>(decimal_text_to_i128(
                            v.as_bytes(),
                            *scale as usize,
                        ))
                    }
                );
            }
            DataType::Utf8 => {
                read_text!(
                    builder,
                    field,
                    StringBuilder,
                    row,
                    odbc_col_idx,
                    &mut text_buf,
                    |v: String| Ok::<_, DataFusionError>(v)
                );
            }
            DataType::Utf8View => {
                read_text!(
                    builder,
                    field,
                    StringViewBuilder,
                    row,
                    odbc_col_idx,
                    &mut text_buf,
                    |v: String| Ok::<_, DataFusionError>(v)
                );
            }
            DataType::FixedSizeBinary(_) => {
                let builder = builder
                    .as_any_mut()
                    .downcast_mut::<FixedSizeBinaryBuilder>()
                    .unwrap_or_else(|| {
                        panic!("Failed to downcast builder to FixedSizeBinaryBuilder for {field:?}")
                    });
                binary_buf.clear();
                let is_not_null = row.get_binary(odbc_col_idx, &mut binary_buf).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to get value for field {:?}: {e:?}",
                        field
                    ))
                })?;
                if is_not_null {
                    builder.append_value(&binary_buf)?;
                } else {
                    builder.append_null();
                }
            }
            DataType::Binary => {
                let builder = builder
                    .as_any_mut()
                    .downcast_mut::<BinaryBuilder>()
                    .unwrap_or_else(|| {
                        panic!("Failed to downcast builder to BinaryBuilder for {field:?}")
                    });
                binary_buf.clear();
                let is_not_null = row.get_binary(odbc_col_idx, &mut binary_buf).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to get value for field {:?}: {e:?}",
                        field
                    ))
                })?;
                if is_not_null {
                    builder.append_value(&binary_buf);
                } else {
                    builder.append_null();
                }
            }
            DataType::BinaryView => {
                let builder = builder
                    .as_any_mut()
                    .downcast_mut::<BinaryViewBuilder>()
                    .unwrap_or_else(|| {
                        panic!("Failed to downcast builder to BinaryViewBuilder for {field:?}")
                    });
                binary_buf.clear();
                let is_not_null = row.get_binary(odbc_col_idx, &mut binary_buf).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to get value for field {:?}: {e:?}",
                        field
                    ))
                })?;
                if is_not_null {
                    builder.append_value(&binary_buf);
                } else {
                    builder.append_null();
                }
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                read_data!(
                    builder,
                    field,
                    TimestampMicrosecondBuilder,
                    row,
                    odbc_col_idx,
                    odbc_api::sys::Timestamp,
                    |v| { us_since_epoch(&v) }
                );
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                read_text!(
                    builder,
                    field,
                    Time64MicrosecondBuilder,
                    row,
                    odbc_col_idx,
                    &mut text_buf,
                    |value: String| {
                        let nt = NaiveTime::parse_from_str(&value, "%H:%M:%S%.f").map_err(|e| {
                            DataFusionError::Execution(format!("Failed to parse time: {e:?}"))
                        })?;
                        Ok::<_, DataFusionError>(
                            nt.num_seconds_from_midnight() as i64 * 1_000_000
                                + (nt.nanosecond() / 1000) as i64,
                        )
                    }
                );
            }
            DataType::Date32 => {
                read_data!(
                    builder,
                    field,
                    Date32Builder,
                    row,
                    odbc_col_idx,
                    odbc_api::sys::Date,
                    |value: odbc_api::sys::Date| {
                        let date = NaiveDate::from_ymd_opt(
                            value.year as i32,
                            value.month as u32,
                            value.day as u32,
                        )
                        .ok_or_else(|| {
                            DataFusionError::Execution(format!("Invalid date: {value:?}"))
                        })?;
                        Ok::<_, DataFusionError>(Date32Type::from_naive_date(date))
                    }
                );
            }
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported field type for mdb row conversion: {field:?}"
                )));
            }
        }
    }
    Ok(())
}

fn finish_batch(
    builders: Vec<Box<dyn arrow::array::ArrayBuilder>>,
    table_schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
    row_count: usize,
) -> DFResult<RecordBatch> {
    let projected_schema = project_schema(table_schema, projection)?;

    let arrays = builders
        .into_iter()
        .enumerate()
        .filter(|(idx, _)| projections_contains(projection, *idx))
        .map(|(_, mut builder)| builder.finish())
        .collect::<Vec<ArrayRef>>();

    let options = RecordBatchOptions::new().with_row_count(Some(row_count));
    Ok(RecordBatch::try_new_with_options(
        projected_schema,
        arrays,
        &options,
    )?)
}

// --- Columnar fast path helpers (mirrors dm/buffer.rs) -------------------------
// Used when the underlying mdbtools supports SQL_ATTR_ROW_ARRAY_SIZE /
// SQL_ATTR_ROW_BIND_TYPE. Older mdbtools (<1.0.1) does not, in which case
// the bind_buffer call below will fail and the row-by-row path is used.

#[allow(dead_code)]
fn contains_large_column(cursor: &mut CursorImpl<StatementImpl>) -> DFResult<bool> {
    let col_count = cursor
        .num_result_cols()
        .map_err(|e| DataFusionError::External(Box::new(e)))? as u16;
    for i in 1..=col_count {
        let col_type = cursor
            .col_data_type(i)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        if matches!(
            col_type,
            odbc_api::DataType::LongVarchar { length: _ }
                | odbc_api::DataType::WLongVarchar { length: _ }
                | odbc_api::DataType::LongVarbinary { length: _ }
        ) {
            return Ok(true);
        }
    }
    Ok(false)
}

#[allow(dead_code)] // see comment above contains_large_column
fn build_buffer_desc(
    field: &arrow::datatypes::Field,
    cursor: &mut CursorImpl<StatementImpl>,
    col_idx: usize,
) -> DFResult<BufferDesc> {
    let nullable = field.is_nullable();
    let odbc_col_idx = (col_idx + 1) as u16;
    match field.data_type() {
        DataType::Boolean => Ok(BufferDesc::Bit { nullable }),
        DataType::Int8 => Ok(BufferDesc::I8 { nullable }),
        DataType::Int16 => Ok(BufferDesc::I16 { nullable }),
        DataType::Int32 => Ok(BufferDesc::I32 { nullable }),
        DataType::Int64 => Ok(BufferDesc::I64 { nullable }),
        DataType::Float32 => Ok(BufferDesc::F32 { nullable }),
        DataType::Float64 => Ok(BufferDesc::F64 { nullable }),
        DataType::Decimal128(precision, _scale) => Ok(BufferDesc::Text {
            // precision digits + sign + decimal point
            max_str_len: *precision as usize + 2,
        }),
        DataType::Utf8 | DataType::Utf8View => {
            let column_size = cursor
                .col_data_type(odbc_col_idx)
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .column_size();
            let max_str_len = match column_size {
                Some(size) if size.get() > 0 => size.get() * 4,
                _ => 1024,
            };
            Ok(BufferDesc::Text { max_str_len })
        }
        DataType::FixedSizeBinary(size) => Ok(BufferDesc::Binary {
            max_bytes: *size as usize,
        }),
        DataType::Binary | DataType::BinaryView => {
            let column_size = cursor
                .col_data_type(odbc_col_idx)
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .column_size();
            let max_bytes = match column_size {
                Some(size) if size.get() > 0 => size.get(),
                _ => 1024,
            };
            Ok(BufferDesc::Binary { max_bytes })
        }
        DataType::Timestamp(_, _) => Ok(BufferDesc::Timestamp { nullable }),
        DataType::Date32 => Ok(BufferDesc::Date { nullable }),
        DataType::Time64(_) => {
            let display_size = cursor
                .col_data_type(odbc_col_idx)
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .display_size();
            let max_str_len = match display_size {
                Some(size) if size.get() > 0 => size.get() * 4,
                _ => 32,
            };
            Ok(BufferDesc::Text { max_str_len })
        }
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported data type to build buffer desc for mdb: {:?}",
            field.data_type()
        ))),
    }
}

macro_rules! handle_primitive_type {
    ($builder:expr, $field:expr, $builder_ty:ty, $nullable:expr, $value_ty:ty, $col_slice:expr, $convert:expr) => {{
        let builder = $builder
            .as_any_mut()
            .downcast_mut::<$builder_ty>()
            .unwrap_or_else(|| {
                panic!(
                    "Failed to downcast builder to {} for {:?}",
                    stringify!($builder_ty),
                    $field,
                )
            });
        if $nullable {
            let values = $col_slice.as_nullable_slice::<$value_ty>().ok_or_else(|| {
                DataFusionError::Execution(format!("Failed to get nullable slice for {:?}", $field))
            })?;
            for value in values {
                match value {
                    Some(v) => builder.append_value($convert(v)?),
                    None => builder.append_null(),
                }
            }
        } else {
            let values = $col_slice.as_slice::<$value_ty>().ok_or_else(|| {
                DataFusionError::Execution(format!("Failed to get slice for {:?}", $field))
            })?;
            for value in values {
                builder.append_value($convert(value)?);
            }
        }
    }};
}

macro_rules! handle_text_view {
    ($builder:expr, $field:expr, $builder_ty:ty, $col_slice:expr, $convert:expr) => {{
        let builder = $builder
            .as_any_mut()
            .downcast_mut::<$builder_ty>()
            .unwrap_or_else(|| {
                panic!(
                    "Failed to downcast builder to {} for {:?}",
                    stringify!($builder_ty),
                    $field,
                )
            });
        let values = $col_slice.as_text().ok_or_else(|| {
            DataFusionError::Execution(format!("Failed to get view for {:?}", $field))
        })?;
        for value in values.iter() {
            match value {
                Some(v) => {
                    // mdbtools returns text in the MDB file's code page
                    // (CP1252/CP936/etc.), not UTF-8. from_utf8_lossy keeps the
                    // printable bytes and substitutes U+FFFD for invalid ones.
                    let s = String::from_utf8_lossy(v);
                    builder.append_value($convert(s.as_ref())?);
                }
                None => {
                    builder.append_null();
                }
            }
        }
    }};
}

#[allow(dead_code)] // see comment above contains_large_column
fn buffer_to_batch(
    buffer: &ColumnarDynBuffer,
    table_schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
    chunk_size: usize,
) -> DFResult<RecordBatch> {
    let projected_schema = project_schema(table_schema, projection)?;

    let mut arrays = Vec::with_capacity(projected_schema.fields().len());
    for (col_idx, field) in table_schema.fields().iter().enumerate() {
        if !projections_contains(projection, col_idx) {
            continue;
        }
        let mut builder = make_builder(field.data_type(), chunk_size);
        let col_slice = buffer.column(col_idx);
        let nullable = field.is_nullable();
        match field.data_type() {
            DataType::Boolean => {
                handle_primitive_type!(
                    builder,
                    field,
                    BooleanBuilder,
                    nullable,
                    odbc_api::Bit,
                    col_slice,
                    |bit: &odbc_api::Bit| { Ok::<_, DataFusionError>(bit.as_bool()) }
                );
            }
            DataType::Int8 => {
                handle_primitive_type!(
                    builder,
                    field,
                    Int8Builder,
                    nullable,
                    i8,
                    col_slice,
                    |v: &i8| { Ok::<_, DataFusionError>(*v) }
                );
            }
            DataType::Int16 => {
                handle_primitive_type!(
                    builder,
                    field,
                    Int16Builder,
                    nullable,
                    i16,
                    col_slice,
                    |v: &i16| { Ok::<_, DataFusionError>(*v) }
                );
            }
            DataType::Int32 => {
                handle_primitive_type!(
                    builder,
                    field,
                    Int32Builder,
                    nullable,
                    i32,
                    col_slice,
                    |v: &i32| { Ok::<_, DataFusionError>(*v) }
                );
            }
            DataType::Int64 => {
                handle_primitive_type!(
                    builder,
                    field,
                    Int64Builder,
                    nullable,
                    i64,
                    col_slice,
                    |v: &i64| { Ok::<_, DataFusionError>(*v) }
                );
            }
            DataType::Float32 => {
                handle_primitive_type!(
                    builder,
                    field,
                    Float32Builder,
                    nullable,
                    f32,
                    col_slice,
                    |v: &f32| { Ok::<_, DataFusionError>(*v) }
                );
            }
            DataType::Float64 => {
                handle_primitive_type!(
                    builder,
                    field,
                    Float64Builder,
                    nullable,
                    f64,
                    col_slice,
                    |v: &f64| { Ok::<_, DataFusionError>(*v) }
                );
            }
            DataType::Decimal128(_, scale) => {
                handle_text_view!(
                    builder,
                    field,
                    Decimal128Builder,
                    col_slice,
                    |value: &str| {
                        Ok::<_, DataFusionError>(decimal_text_to_i128(
                            value.as_bytes(),
                            *scale as usize,
                        ))
                    }
                );
            }
            DataType::Utf8 => {
                let convert: for<'a> fn(&'a str) -> DFResult<&'a str> = |v| Ok(v);
                handle_text_view!(builder, field, StringBuilder, col_slice, convert);
            }
            DataType::Utf8View => {
                let convert: for<'a> fn(&'a str) -> DFResult<&'a str> = |v| Ok(v);
                handle_text_view!(builder, field, StringViewBuilder, col_slice, convert);
            }
            DataType::FixedSizeBinary(_) => {
                let builder = builder
                    .as_any_mut()
                    .downcast_mut::<FixedSizeBinaryBuilder>()
                    .unwrap_or_else(|| {
                        panic!("Failed to downcast builder to FixedSizeBinaryBuilder for {field:?}")
                    });
                let values = col_slice.as_binary().ok_or_else(|| {
                    DataFusionError::Execution(format!("Failed to get bin view for {field:?}"))
                })?;
                for value in values.iter() {
                    match value {
                        Some(v) => {
                            builder.append_value(v)?;
                        }
                        None => {
                            builder.append_null();
                        }
                    }
                }
            }
            DataType::Binary => {
                let builder = builder
                    .as_any_mut()
                    .downcast_mut::<BinaryBuilder>()
                    .unwrap_or_else(|| {
                        panic!("Failed to downcast builder to BinaryBuilder for {field:?}")
                    });
                let values = col_slice.as_binary().ok_or_else(|| {
                    DataFusionError::Execution(format!("Failed to get bin view for {field:?}"))
                })?;
                for value in values.iter() {
                    match value {
                        Some(v) => {
                            builder.append_value(v);
                        }
                        None => {
                            builder.append_null();
                        }
                    }
                }
            }
            DataType::BinaryView => {
                let builder = builder
                    .as_any_mut()
                    .downcast_mut::<BinaryViewBuilder>()
                    .unwrap_or_else(|| {
                        panic!("Failed to downcast builder to BinaryViewBuilder for {field:?}")
                    });
                let values = col_slice.as_binary().ok_or_else(|| {
                    DataFusionError::Execution(format!("Failed to get bin view for {field:?}"))
                })?;
                for value in values.iter() {
                    match value {
                        Some(v) => {
                            builder.append_value(v);
                        }
                        None => {
                            builder.append_null();
                        }
                    }
                }
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                handle_primitive_type!(
                    builder,
                    field,
                    TimestampMicrosecondBuilder,
                    nullable,
                    odbc_api::sys::Timestamp,
                    col_slice,
                    us_since_epoch
                );
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                handle_text_view!(
                    builder,
                    field,
                    Time64MicrosecondBuilder,
                    col_slice,
                    |value: &str| {
                        let nt = NaiveTime::parse_from_str(value, "%H:%M:%S%.f").map_err(|e| {
                            DataFusionError::Execution(format!("Failed to parse time: {e:?}"))
                        })?;
                        Ok::<_, DataFusionError>(
                            nt.num_seconds_from_midnight() as i64 * 1_000_000
                                + (nt.nanosecond() / 1000) as i64,
                        )
                    }
                );
            }
            DataType::Date32 => {
                handle_primitive_type!(
                    builder,
                    field,
                    Date32Builder,
                    nullable,
                    odbc_api::sys::Date,
                    col_slice,
                    |value: &odbc_api::sys::Date| {
                        let date = NaiveDate::from_ymd_opt(
                            value.year as i32,
                            value.month as u32,
                            value.day as u32,
                        )
                        .ok_or_else(|| {
                            DataFusionError::Execution(format!("Invalid date: {value:?}"))
                        })?;
                        Ok::<_, DataFusionError>(Date32Type::from_naive_date(date))
                    }
                );
            }
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported field to build record batch for mdb: {field:?}"
                )));
            }
        }
        arrays.push(builder.finish());
    }
    let options = RecordBatchOptions::new().with_row_count(Some(buffer.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        projected_schema,
        arrays,
        &options,
    )?)
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
