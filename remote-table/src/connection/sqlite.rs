use crate::connection::{RemoteDbType, projections_contains};
use crate::{
    Connection, ConnectionOptions, DFResult, Pool, RemoteField, RemoteSchema, RemoteSchemaRef,
    RemoteType, SqliteType, TableSource, Unparse, unparse_array,
};
use datafusion::arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, Float64Builder, Int32Builder, Int64Builder, NullBuilder,
    RecordBatch, RecordBatchOptions, StringBuilder, make_builder,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::{DataFusionError, project_schema};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use derive_getters::Getters;
use derive_with::With;
use futures::StreamExt;
use itertools::Itertools;
use log::{debug, error};
use rusqlite::types::ValueRef;
use rusqlite::{Column, Row, Rows};
use std::any::Any;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, Clone, With, Getters)]
pub struct SqliteConnectionOptions {
    pub path: PathBuf,
    pub stream_chunk_size: usize,
}

impl SqliteConnectionOptions {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            stream_chunk_size: 2048,
        }
    }
}

impl From<SqliteConnectionOptions> for ConnectionOptions {
    fn from(options: SqliteConnectionOptions) -> Self {
        ConnectionOptions::Sqlite(options)
    }
}

#[derive(Debug)]
pub struct SqlitePool {
    path: PathBuf,
}

pub async fn connect_sqlite(options: &SqliteConnectionOptions) -> DFResult<SqlitePool> {
    let _ = rusqlite::Connection::open(&options.path).map_err(|e| {
        DataFusionError::Execution(format!("Failed to open sqlite connection: {e:?}"))
    })?;
    Ok(SqlitePool {
        path: options.path.clone(),
    })
}

#[async_trait::async_trait]
impl Pool for SqlitePool {
    async fn get(&self) -> DFResult<Arc<dyn Connection>> {
        Ok(Arc::new(SqliteConnection {
            path: self.path.clone(),
        }))
    }
}

#[derive(Debug)]
pub struct SqliteConnection {
    path: PathBuf,
}

#[async_trait::async_trait]
impl Connection for SqliteConnection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn infer_schema(&self, source: &TableSource) -> DFResult<RemoteSchemaRef> {
        let conn = rusqlite::Connection::open(&self.path).map_err(|e| {
            DataFusionError::Execution(format!("Failed to open sqlite connection: {e:?}"))
        })?;
        match source {
            TableSource::Table(table) => {
                // TODO missing auto increment, could use sqlparser to parse create table sql
                let sql = format!(
                    "PRAGMA table_info({})",
                    RemoteDbType::Sqlite.sql_table_name(table)
                );
                let mut stmt = conn.prepare(&sql).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to prepare sqlite statement: {e:?}"))
                })?;
                let rows = stmt.query([]).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to query sqlite statement: {e:?}"))
                })?;
                let remote_schema = Arc::new(build_remote_schema_for_table(rows)?);
                Ok(remote_schema)
            }
            TableSource::Query(_query) => {
                let sql = RemoteDbType::Sqlite.limit_1_query_if_possible(source);
                let mut stmt = conn.prepare(&sql).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to prepare sqlite statement: {e:?}"))
                })?;
                let columns: Vec<OwnedColumn> =
                    stmt.columns().iter().map(sqlite_col_to_owned_col).collect();
                let rows = stmt.query([]).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to query sqlite statement: {e:?}"))
                })?;

                let remote_schema =
                    Arc::new(build_remote_schema_for_query(columns.as_slice(), rows)?);
                Ok(remote_schema)
            }
        }
    }

    async fn query(
        &self,
        conn_options: &ConnectionOptions,
        source: &TableSource,
        table_schema: SchemaRef,
        projection: Option<&Vec<usize>>,
        unparsed_filters: &[String],
        limit: Option<usize>,
    ) -> DFResult<SendableRecordBatchStream> {
        let projected_schema = project_schema(&table_schema, projection)?;
        let sql = RemoteDbType::Sqlite.rewrite_query(source, unparsed_filters, limit);
        debug!("[remote-table] executing sqlite query: {sql}");

        let (tx, mut rx) = tokio::sync::mpsc::channel::<DFResult<RecordBatch>>(1);
        let conn = rusqlite::Connection::open(&self.path).map_err(|e| {
            DataFusionError::Execution(format!("Failed to open sqlite connection: {e:?}"))
        })?;

        let projection = projection.cloned();
        let chunk_size = conn_options.stream_chunk_size();

        spawn_background_task(tx, conn, sql, table_schema, projection, chunk_size);

        let stream = async_stream::stream! {
            while let Some(batch) = rx.recv().await {
                yield batch;
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
        unparser: Arc<dyn Unparse>,
        table: &[String],
        remote_schema: RemoteSchemaRef,
        mut input: SendableRecordBatchStream,
    ) -> DFResult<usize> {
        let input_schema = input.schema();
        let conn = rusqlite::Connection::open(&self.path).map_err(|e| {
            DataFusionError::Execution(format!("Failed to open sqlite connection: {e:?}"))
        })?;

        let mut total_count = 0;
        while let Some(batch) = input.next().await {
            let batch = batch?;

            let mut columns = Vec::with_capacity(remote_schema.fields.len());
            for i in 0..batch.num_columns() {
                let input_field = input_schema.field(i);
                let remote_field = &remote_schema.fields[i];
                if remote_field.auto_increment && input_field.is_nullable() {
                    continue;
                }

                let remote_type = remote_schema.fields[i].remote_type.clone();
                let array = batch.column(i);
                let column = unparse_array(unparser.as_ref(), array, remote_type)?;
                columns.push(column);
            }

            let num_rows = columns[0].len();
            let num_columns = columns.len();

            let mut values = Vec::with_capacity(num_rows);
            for i in 0..num_rows {
                let mut value = Vec::with_capacity(num_columns);
                for col in columns.iter() {
                    value.push(col[i].as_str());
                }
                values.push(format!("({})", value.join(",")));
            }

            let mut col_names = Vec::with_capacity(remote_schema.fields.len());
            for (remote_field, input_field) in
                remote_schema.fields.iter().zip(input_schema.fields.iter())
            {
                if remote_field.auto_increment && input_field.is_nullable() {
                    continue;
                }
                col_names.push(RemoteDbType::Sqlite.sql_identifier(&remote_field.name));
            }

            let sql = format!(
                "INSERT INTO {} ({}) VALUES {}",
                RemoteDbType::Sqlite.sql_table_name(table),
                col_names.join(","),
                values.join(",")
            );

            let count = conn.execute(&sql, []).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to execute insert statement on sqlite: {e:?}, sql: {sql}"
                ))
            })?;
            total_count += count as usize;
        }

        Ok(total_count)
    }
}

#[derive(Debug)]
struct OwnedColumn {
    name: String,
    decl_type: Option<String>,
}

fn sqlite_col_to_owned_col(sqlite_col: &Column) -> OwnedColumn {
    OwnedColumn {
        name: sqlite_col.name().to_string(),
        decl_type: sqlite_col.decl_type().map(|x| x.to_string()),
    }
}

fn decl_type_to_remote_type(decl_type: &str) -> DFResult<SqliteType> {
    if [
        "tinyint", "smallint", "int", "integer", "bigint", "int2", "int4", "int8",
    ]
    .contains(&decl_type)
    {
        return Ok(SqliteType::Integer);
    }
    if ["real", "float", "double", "numeric"].contains(&decl_type) {
        return Ok(SqliteType::Real);
    }
    if decl_type.starts_with("real") || decl_type.starts_with("numeric") {
        return Ok(SqliteType::Real);
    }
    if ["text", "varchar", "char", "string"].contains(&decl_type) {
        return Ok(SqliteType::Text);
    }
    if decl_type.starts_with("char")
        || decl_type.starts_with("varchar")
        || decl_type.starts_with("text")
    {
        return Ok(SqliteType::Text);
    }
    if ["binary", "varbinary", "tinyblob", "blob"].contains(&decl_type) {
        return Ok(SqliteType::Blob);
    }
    if decl_type.starts_with("binary") || decl_type.starts_with("varbinary") {
        return Ok(SqliteType::Blob);
    }
    Err(DataFusionError::NotImplemented(format!(
        "Unsupported sqlite decl type: {decl_type}",
    )))
}

fn build_remote_schema_for_table(mut rows: Rows) -> DFResult<RemoteSchema> {
    let mut remote_fields = vec![];
    while let Some(row) = rows.next().map_err(|e| {
        DataFusionError::Execution(format!("Failed to get next row from sqlite: {e:?}"))
    })? {
        let name = row.get::<_, String>(1).map_err(|e| {
            DataFusionError::Execution(format!("Failed to get col name from sqlite row: {e:?}"))
        })?;
        let decl_type = row.get::<_, String>(2).map_err(|e| {
            DataFusionError::Execution(format!("Failed to get decl type from sqlite row: {e:?}"))
        })?;
        let remote_type = decl_type_to_remote_type(&decl_type.to_ascii_lowercase())?;
        let nullable = row.get::<_, i64>(3).map_err(|e| {
            DataFusionError::Execution(format!("Failed to get nullable from sqlite row: {e:?}"))
        })? == 0;
        remote_fields.push(RemoteField::new(
            &name,
            RemoteType::Sqlite(remote_type),
            nullable,
        ));
    }
    Ok(RemoteSchema::new(remote_fields))
}

fn build_remote_schema_for_query(
    columns: &[OwnedColumn],
    mut rows: Rows,
) -> DFResult<RemoteSchema> {
    let mut remote_field_map = HashMap::with_capacity(columns.len());
    let mut unknown_cols = vec![];
    for (col_idx, col) in columns.iter().enumerate() {
        if let Some(decl_type) = &col.decl_type {
            let remote_type =
                RemoteType::Sqlite(decl_type_to_remote_type(&decl_type.to_ascii_lowercase())?);
            remote_field_map.insert(col_idx, RemoteField::new(&col.name, remote_type, true));
        } else {
            // None for expressions
            unknown_cols.push(col_idx);
        }
    }

    if !unknown_cols.is_empty() {
        while let Some(row) = rows.next().map_err(|e| {
            DataFusionError::Execution(format!("Failed to get next row from sqlite: {e:?}"))
        })? {
            let mut to_be_removed = vec![];
            for col_idx in unknown_cols.iter() {
                let value_ref = row.get_ref(*col_idx).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to get value ref for column {col_idx}: {e:?}"
                    ))
                })?;
                match value_ref {
                    ValueRef::Null => {}
                    ValueRef::Integer(_) => {
                        remote_field_map.insert(
                            *col_idx,
                            RemoteField::new(
                                columns[*col_idx].name.clone(),
                                RemoteType::Sqlite(SqliteType::Integer),
                                true,
                            ),
                        );
                        to_be_removed.push(*col_idx);
                    }
                    ValueRef::Real(_) => {
                        remote_field_map.insert(
                            *col_idx,
                            RemoteField::new(
                                columns[*col_idx].name.clone(),
                                RemoteType::Sqlite(SqliteType::Real),
                                true,
                            ),
                        );
                        to_be_removed.push(*col_idx);
                    }
                    ValueRef::Text(_) => {
                        remote_field_map.insert(
                            *col_idx,
                            RemoteField::new(
                                columns[*col_idx].name.clone(),
                                RemoteType::Sqlite(SqliteType::Text),
                                true,
                            ),
                        );
                        to_be_removed.push(*col_idx);
                    }
                    ValueRef::Blob(_) => {
                        remote_field_map.insert(
                            *col_idx,
                            RemoteField::new(
                                columns[*col_idx].name.clone(),
                                RemoteType::Sqlite(SqliteType::Blob),
                                true,
                            ),
                        );
                        to_be_removed.push(*col_idx);
                    }
                }
            }
            for col_idx in to_be_removed.iter() {
                unknown_cols.retain(|&x| x != *col_idx);
            }
            if unknown_cols.is_empty() {
                break;
            }
        }
    }

    if !unknown_cols.is_empty() {
        return Err(DataFusionError::NotImplemented(format!(
            "Failed to infer sqlite decl type for columns: {unknown_cols:?}"
        )));
    }
    let remote_fields = remote_field_map
        .into_iter()
        .sorted_by_key(|entry| entry.0)
        .map(|entry| entry.1)
        .collect::<Vec<_>>();
    Ok(RemoteSchema::new(remote_fields))
}

fn spawn_background_task(
    tx: tokio::sync::mpsc::Sender<DFResult<RecordBatch>>,
    conn: rusqlite::Connection,
    sql: String,
    table_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    chunk_size: usize,
) {
    std::thread::spawn(move || {
        let runtime = match tokio::runtime::Builder::new_current_thread().build() {
            Ok(runtime) => runtime,
            Err(e) => {
                error!("Failed to create tokio runtime to run sqlite query: {e:?}");
                return;
            }
        };
        let local_set = tokio::task::LocalSet::new();
        local_set.block_on(&runtime, async move {
            let mut stmt = match conn.prepare(&sql) {
                Ok(stmt) => stmt,
                Err(e) => {
                    let _ = tx
                        .send(Err(DataFusionError::Execution(format!(
                            "Failed to prepare sqlite statement: {e:?}"
                        ))))
                        .await;
                    return;
                }
            };
            let columns: Vec<OwnedColumn> =
                stmt.columns().iter().map(sqlite_col_to_owned_col).collect();
            let mut rows = match stmt.query([]) {
                Ok(rows) => rows,
                Err(e) => {
                    let _ = tx
                        .send(Err(DataFusionError::Execution(format!(
                            "Failed to query sqlite statement: {e:?}"
                        ))))
                        .await;
                    return;
                }
            };

            loop {
                let (batch, is_empty) = match rows_to_batch(
                    &mut rows,
                    &table_schema,
                    &columns,
                    projection.as_ref(),
                    chunk_size,
                ) {
                    Ok((batch, is_empty)) => (batch, is_empty),
                    Err(e) => {
                        let _ = tx
                            .send(Err(DataFusionError::Execution(format!(
                                "Failed to convert rows to batch: {e:?}"
                            ))))
                            .await;
                        return;
                    }
                };
                if is_empty {
                    break;
                }
                if tx.send(Ok(batch)).await.is_err() {
                    return;
                }
            }
        });
    });
}

fn rows_to_batch(
    rows: &mut Rows,
    table_schema: &SchemaRef,
    columns: &[OwnedColumn],
    projection: Option<&Vec<usize>>,
    chunk_size: usize,
) -> DFResult<(RecordBatch, bool)> {
    let projected_schema = project_schema(table_schema, projection)?;
    let mut array_builders = vec![];
    for field in table_schema.fields() {
        let builder = make_builder(field.data_type(), 1000);
        array_builders.push(builder);
    }

    let mut is_empty = true;
    let mut row_count = 0;
    while let Some(row) = rows.next().map_err(|e| {
        DataFusionError::Execution(format!("Failed to get next row from sqlite: {e:?}"))
    })? {
        is_empty = false;
        row_count += 1;
        append_rows_to_array_builders(
            row,
            table_schema,
            columns,
            projection,
            array_builders.as_mut_slice(),
        )?;
        if row_count >= chunk_size {
            break;
        }
    }

    let projected_columns = array_builders
        .into_iter()
        .enumerate()
        .filter(|(idx, _)| projections_contains(projection, *idx))
        .map(|(_, mut builder)| builder.finish())
        .collect::<Vec<ArrayRef>>();
    let options = RecordBatchOptions::new().with_row_count(Some(row_count));
    Ok((
        RecordBatch::try_new_with_options(projected_schema, projected_columns, &options)?,
        is_empty,
    ))
}

macro_rules! handle_primitive_type {
    ($builder:expr, $field:expr, $col:expr, $builder_ty:ty, $value_ty:ty, $row:expr, $index:expr) => {{
        let builder = $builder
            .as_any_mut()
            .downcast_mut::<$builder_ty>()
            .unwrap_or_else(|| {
                panic!(
                    "Failed to downcast builder to {} for {:?} and {:?}",
                    stringify!($builder_ty),
                    $field,
                    $col
                )
            });

        let v: Option<$value_ty> = $row.get($index).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to get optional {} value for {:?} and {:?}: {e:?}",
                stringify!($value_ty),
                $field,
                $col
            ))
        })?;

        match v {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }};
}

fn append_rows_to_array_builders(
    row: &Row,
    table_schema: &SchemaRef,
    columns: &[OwnedColumn],
    projection: Option<&Vec<usize>>,
    array_builders: &mut [Box<dyn ArrayBuilder>],
) -> DFResult<()> {
    for (idx, field) in table_schema.fields.iter().enumerate() {
        if !projections_contains(projection, idx) {
            continue;
        }
        let builder = &mut array_builders[idx];
        let col = columns.get(idx);
        match field.data_type() {
            DataType::Null => {
                let builder = builder
                    .as_any_mut()
                    .downcast_mut::<NullBuilder>()
                    .expect("Failed to downcast builder to NullBuilder");
                builder.append_null();
            }
            DataType::Int32 => {
                handle_primitive_type!(builder, field, col, Int32Builder, i32, row, idx);
            }
            DataType::Int64 => {
                handle_primitive_type!(builder, field, col, Int64Builder, i64, row, idx);
            }
            DataType::Float64 => {
                handle_primitive_type!(builder, field, col, Float64Builder, f64, row, idx);
            }
            DataType::Utf8 => {
                handle_primitive_type!(builder, field, col, StringBuilder, String, row, idx);
            }
            DataType::Binary => {
                handle_primitive_type!(builder, field, col, BinaryBuilder, Vec<u8>, row, idx);
            }
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported data type {} for col: {:?}",
                    field.data_type(),
                    col
                )));
            }
        }
    }
    Ok(())
}
