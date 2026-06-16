use crate::connection::{RemoteDbType, projections_contains};
use crate::{
    Connection, ConnectionOptions, DFResult, Literalize, OpenGaussConnectionOptions, OpenGaussType,
    Pool, PoolState, RemoteField, RemoteSchema, RemoteSchemaRef, RemoteSource, RemoteType,
    literalize_array,
};
use arrow::array::{
    ArrayBuilder, ArrayRef, Int32Builder, RecordBatch, RecordBatchOptions, make_builder,
};
use arrow::datatypes::{DataType, SchemaRef};
use bb8_postgres::PostgresConnectionManager;
use bb8_postgres::tokio_postgres::{NoTls, Row, Statement};
use datafusion_common::DataFusionError;
use datafusion_common::project_schema;
use datafusion_execution::SendableRecordBatchStream;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use log::debug;
use std::sync::Arc;

#[derive(Debug)]
pub struct OpenGaussPool {
    pool: bb8::Pool<PostgresConnectionManager<NoTls>>,
    options: Arc<OpenGaussConnectionOptions>,
}

#[async_trait::async_trait]
impl Pool for OpenGaussPool {
    async fn get(&self) -> DFResult<Arc<dyn Connection>> {
        let conn = self.pool.get_owned().await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to get opengauss connection due to {e:?}"))
        })?;
        Ok(Arc::new(OpenGaussConnection {
            conn,
            options: self.options.clone(),
        }))
    }

    async fn state(&self) -> DFResult<PoolState> {
        let bb8_state = self.pool.state();
        Ok(PoolState {
            connections: bb8_state.connections as usize,
            idle_connections: bb8_state.idle_connections as usize,
        })
    }
}

pub async fn connect_opengauss(options: &OpenGaussConnectionOptions) -> DFResult<OpenGaussPool> {
    let mut config = bb8_postgres::tokio_postgres::config::Config::new();
    config
        .host(&options.host)
        .port(options.port)
        .user(&options.username)
        .password(&options.password);
    if let Some(database) = &options.database {
        config.dbname(database);
    }
    let manager = PostgresConnectionManager::new(config, NoTls);
    let pool = bb8::Pool::builder()
        .max_size(options.pool_max_size as u32)
        .min_idle(Some(options.pool_min_idle as u32))
        .idle_timeout(Some(options.pool_idle_timeout))
        .reaper_rate(options.pool_ttl_check_interval)
        .build(manager)
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to create opengauss connection pool due to {e}",
            ))
        })?;

    Ok(OpenGaussPool {
        pool,
        options: Arc::new(options.clone()),
    })
}

#[derive(Debug)]
pub struct OpenGaussConnection {
    pub conn: bb8::PooledConnection<'static, PostgresConnectionManager<NoTls>>,
    pub options: Arc<OpenGaussConnectionOptions>,
}

#[async_trait::async_trait]
impl Connection for OpenGaussConnection {
    async fn infer_schema(&self, source: &RemoteSource) -> DFResult<RemoteSchemaRef> {
        match source {
            RemoteSource::Table(table) => {
                let db_type = RemoteDbType::OpenGauss;
                let where_condition = if table.len() == 1 {
                    format!("table_name = {}", db_type.sql_string_literal(&table[0]))
                } else if table.len() == 2 {
                    format!(
                        "table_schema = {} AND table_name = {}",
                        db_type.sql_string_literal(&table[0]),
                        db_type.sql_string_literal(&table[1])
                    )
                } else {
                    format!(
                        "table_catalog = {} AND table_schema = {} AND table_name = {}",
                        db_type.sql_string_literal(&table[0]),
                        db_type.sql_string_literal(&table[1]),
                        db_type.sql_string_literal(&table[2])
                    )
                };
                let sql = format!(
                    "select column_name, data_type from information_schema.columns
                    where {} order by ordinal_position",
                    where_condition
                );
                let rows = self.conn.query(&sql, &[]).await.map_err(|e| {
                    DataFusionError::Plan(format!(
                        "Failed to execute query {sql} on opengauss: {e:?}",
                    ))
                })?;
                let remote_schema = Arc::new(build_remote_schema_for_table(rows)?);
                Ok(remote_schema)
            }
            RemoteSource::Query(query) => {
                let stmt = self.conn.prepare(query).await.map_err(|e| {
                    DataFusionError::Plan(format!(
                        "Failed to execute query {query} on opengauss: {e:?}",
                    ))
                })?;
                let remote_schema = Arc::new(build_remote_schema_for_query(stmt).await?);
                Ok(remote_schema)
            }
            RemoteSource::Command(cmd) => Err(DataFusionError::NotImplemented(format!(
                "Command {cmd:?} is not supported for OpenGauss"
            ))),
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
        let projected_schema = project_schema(&table_schema, projection)?;

        let sql = RemoteDbType::OpenGauss.rewrite_query(source, unparsed_filters, limit)?;
        debug!("[remote-table] executing opengauss query: {sql}");

        let projection = projection.cloned();
        let chunk_size = conn_options.stream_chunk_size();
        let stream = self
            .conn
            .query_raw(&sql, Vec::<String>::new())
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to execute query {sql} on opengauss: {e}",
                ))
            })?
            .chunks(chunk_size)
            .boxed();

        let stream = stream.map(move |rows| {
            let rows: Vec<Row> = rows
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to collect rows from opengauss due to {e}",
                    ))
                })?;
            rows_to_batch(rows.as_slice(), &table_schema, projection.as_ref())
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            projected_schema,
            stream,
        )))
    }

    async fn insert(
        &self,
        _conn_options: &ConnectionOptions,
        literalizer: Arc<dyn Literalize>,
        table: &[String],
        remote_schema: RemoteSchemaRef,
        batch: RecordBatch,
    ) -> DFResult<usize> {
        let mut columns = Vec::with_capacity(remote_schema.fields.len());
        for i in 0..batch.num_columns() {
            let remote_type = remote_schema.fields[i].remote_type.clone();
            let array = batch.column(i);
            let column = literalize_array(literalizer.as_ref(), array, remote_type)?;
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
        for remote_field in remote_schema.fields.iter() {
            col_names.push(RemoteDbType::OpenGauss.sql_identifier(&remote_field.name));
        }

        let sql = format!(
            "INSERT INTO {} ({}) VALUES {}",
            RemoteDbType::OpenGauss.sql_table_name(table),
            col_names.join(","),
            values.join(",")
        );

        let count = self.conn.execute(&sql, &[]).await.map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to execute insert statement on opengauss: {e:?}, sql: {sql}"
            ))
        })?;

        Ok(count as usize)
    }

    async fn count(
        &self,
        conn_options: &ConnectionOptions,
        source: &RemoteSource,
        unparsed_filters: &[String],
    ) -> DFResult<Option<usize>> {
        crate::connection::connection_count(self, conn_options, source, unparsed_filters).await
    }
}

fn parse_og_type(data_type: &str) -> DFResult<OpenGaussType> {
    match data_type {
        "integer" | "int" | "int4" => Ok(OpenGaussType::Integer),
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported opengauss type: {data_type}"
        ))),
    }
}

async fn build_remote_schema_for_query(stmt: Statement) -> DFResult<RemoteSchema> {
    let mut remote_fields = Vec::new();
    for col in stmt.columns().iter() {
        let data_type = col.type_().name().to_string();
        let og_type = parse_og_type(&data_type)?;
        remote_fields.push(RemoteField::new(
            col.name(),
            RemoteType::OpenGauss(og_type),
            true,
        ));
    }
    Ok(RemoteSchema::new(remote_fields))
}

fn build_remote_schema_for_table(rows: Vec<Row>) -> DFResult<RemoteSchema> {
    let mut remote_fields = vec![];
    for row in rows {
        let column_name: String = row.try_get(0).map_err(|e| {
            DataFusionError::Plan(format!(
                "Failed to get column name from opengauss row: {e:?}"
            ))
        })?;
        let data_type: String = row.try_get(1).map_err(|e| {
            DataFusionError::Plan(format!("Failed to get data type from opengauss row: {e:?}"))
        })?;
        let og_type = parse_og_type(&data_type)?;
        remote_fields.push(RemoteField::new(
            column_name,
            RemoteType::OpenGauss(og_type),
            true,
        ));
    }
    Ok(RemoteSchema::new(remote_fields))
}

fn rows_to_batch(
    rows: &[Row],
    table_schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> DFResult<RecordBatch> {
    let projected_schema = project_schema(table_schema, projection)?;
    let mut array_builders = vec![];
    for field in table_schema.fields() {
        let builder = make_builder(field.data_type(), rows.len());
        array_builders.push(builder);
    }

    for row in rows {
        for (idx, field) in table_schema.fields.iter().enumerate() {
            if !projections_contains(projection, idx) {
                continue;
            }
            let builder = &mut array_builders[idx];
            match field.data_type() {
                DataType::Int32 => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<Int32Builder>()
                        .expect("Failed to downcast to Int32Builder");
                    let v: Option<i32> = row.try_get(idx).map_err(|e| {
                        DataFusionError::Execution(format!("Failed to get Int32 value: {e:?}"))
                    })?;
                    match v {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                }
                _ => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Unsupported data type {} for opengauss",
                        field.data_type()
                    )));
                }
            }
        }
    }

    let projected_columns = array_builders
        .into_iter()
        .enumerate()
        .filter(|(idx, _)| projections_contains(projection, *idx))
        .map(|(_, mut builder)| builder.finish())
        .collect::<Vec<ArrayRef>>();
    let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
    Ok(RecordBatch::try_new_with_options(
        projected_schema,
        projected_columns,
        &options,
    )?)
}
