use crate::connection::ODBC_ENV;
use crate::connection::dm::buffer::{buffer_to_batch, build_buffer_desc};
use crate::connection::dm::row::row_to_batch;
use crate::{
    Connection, ConnectionOptions, DFResult, DmType, Pool, RemoteDbType, RemoteField, RemoteSchema,
    RemoteSchemaRef, RemoteType,
};
use async_stream::stream;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::project_schema;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use derive_getters::Getters;
use derive_with::With;
use futures::lock::Mutex;
use log::debug;
use odbc_api::buffers::ColumnarAnyBuffer;
use odbc_api::handles::StatementImpl;
use odbc_api::{Cursor, CursorImpl, Environment, ResultSetMetadata};
use std::sync::Arc;
use tokio::runtime::Handle;

mod buffer;
mod row;

#[derive(Debug, Clone, With, Getters)]
pub struct DmConnectionOptions {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) schema: Option<String>,
    pub(crate) stream_chunk_size: usize,
    pub(crate) driver: String,
}

impl DmConnectionOptions {
    pub fn new(
        host: impl Into<String>,
        port: u16,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            username: username.into(),
            password: password.into(),
            schema: None,
            stream_chunk_size: 1024,
            driver: "DM8 ODBC DRIVER".to_string(),
        }
    }
}

impl From<DmConnectionOptions> for ConnectionOptions {
    fn from(options: DmConnectionOptions) -> Self {
        ConnectionOptions::Dm(options)
    }
}

#[derive(Debug)]
pub struct DmPool {
    options: DmConnectionOptions,
}

pub(crate) fn connect_dm(options: &DmConnectionOptions) -> DFResult<DmPool> {
    Ok(DmPool {
        options: options.clone(),
    })
}

#[async_trait::async_trait]
impl Pool for DmPool {
    async fn get(&self) -> DFResult<Arc<dyn Connection>> {
        let env = ODBC_ENV.get_or_init(|| Environment::new().expect("failed to create ODBC env"));
        let mut connection_str = format!(
            "Driver={{{}}};Server={};Port={};UID={};PWD={}",
            self.options.driver,
            self.options.host,
            self.options.port,
            self.options.username,
            self.options.password,
        );
        if let Some(schema) = &self.options.schema {
            connection_str.push_str(&format!(";SCHEMA={schema}"));
        }
        let connection = env
            .connect_with_connection_string(&connection_str, odbc_api::ConnectionOptions::default())
            .map_err(|e| {
                DataFusionError::Execution(format!("Failed to create odbc connection: {e:?}"))
            })?;
        Ok(Arc::new(DmConnection {
            conn: Arc::new(Mutex::new(connection)),
        }))
    }
}

#[derive(Debug)]
pub struct DmConnection {
    conn: Arc<Mutex<odbc_api::Connection<'static>>>,
}

#[async_trait::async_trait]
impl Connection for DmConnection {
    async fn infer_schema(&self, sql: &str) -> DFResult<RemoteSchemaRef> {
        let sql = RemoteDbType::Dm.query_limit_1(sql);
        let conn = self.conn.lock().await;
        let cursor_opt = conn
            .execute(&sql, (), None)
            .map_err(|e| DataFusionError::Execution(format!("Failed to infer schema: {e:?}")))?;
        match cursor_opt {
            None => Err(DataFusionError::Execution(
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
        sql: &str,
        table_schema: SchemaRef,
        projection: Option<&Vec<usize>>,
        unparsed_filters: &[String],
        limit: Option<usize>,
    ) -> DFResult<SendableRecordBatchStream> {
        let projected_schema = project_schema(&table_schema, projection)?;

        let sql = RemoteDbType::Dm.rewrite_query(sql, unparsed_filters, limit);
        debug!("[remote-table] executing dm query: {sql}");

        let chunk_size = conn_options.stream_chunk_size();
        let conn = Arc::clone(&self.conn);
        let projection = projection.cloned();
        let (batch_tx, mut batch_rx) = tokio::sync::mpsc::channel::<RecordBatch>(4);

        let join_handle = tokio::task::spawn_blocking(move || {
            let handle = Handle::current();
            let conn = handle.block_on(async { conn.lock().await });

            let cursor_opt = conn.execute(&sql, (), None).map_err(|e| {
                DataFusionError::Execution(format!("Failed to execute query: {e:?}"))
            })?;

            match cursor_opt {
                None => {}
                Some(mut cursor) => {
                    if contains_large_column(&mut cursor)? {
                        while let Some(row) = cursor.next_row().map_err(|e| {
                            DataFusionError::Execution(format!("Failed to fetch row: {e:?}"))
                        })? {
                            let batch = row_to_batch(row, &table_schema, projection.as_ref())?;
                            batch_tx.blocking_send(batch).map_err(|e| {
                                DataFusionError::Execution(format!("Failed to send batch: {e:?}"))
                            })?;
                        }
                    } else {
                        let buffer_descs = table_schema
                            .fields()
                            .iter()
                            .enumerate()
                            .map(|(idx, field)| build_buffer_desc(field, &mut cursor, idx))
                            .collect::<DFResult<Vec<_>>>()?;

                        let row_set_buffer = ColumnarAnyBuffer::try_from_descs(
                            chunk_size,
                            buffer_descs,
                        )
                        .map_err(|e| {
                            DataFusionError::Execution(format!("Failed to create buffer: {e:?}"))
                        })?;

                        let mut block_cursor = cursor.bind_buffer(row_set_buffer).map_err(|e| {
                            DataFusionError::Execution(format!("Failed to bind buffer: {e:?}"))
                        })?;
                        loop {
                            match block_cursor.fetch_with_truncation_check(true) {
                                Ok(Some(buffer)) => {
                                    let batch = buffer_to_batch(
                                        buffer,
                                        &table_schema,
                                        projection.as_ref(),
                                        chunk_size,
                                    )?;
                                    batch_tx.blocking_send(batch).map_err(|e| {
                                        DataFusionError::Execution(format!(
                                            "Failed to send batch: {e:?}"
                                        ))
                                    })?;
                                }
                                Ok(None) => break,
                                Err(odbc_error) => {
                                    return Err(DataFusionError::External(Box::new(odbc_error)));
                                }
                            }
                        }
                    }
                }
            }

            Ok::<_, DataFusionError>(())
        });

        let output_stream = stream! {
            while let Some(batch) = batch_rx.recv().await {
                yield Ok(batch);
            }

            match join_handle.await {
                Ok(Ok(())) => {},
                Ok(Err(e)) => yield Err(e),
                Err(e) => yield Err(DataFusionError::Execution(format!(
                    "Failed to execute ODBC query: {e}"
                ))),
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            projected_schema,
            output_stream,
        )))
    }
}

fn build_remote_schema(mut cursor: CursorImpl<StatementImpl>) -> DFResult<RemoteSchema> {
    let col_count = cursor
        .num_result_cols()
        .map_err(|e| DataFusionError::External(Box::new(e)))? as u16;
    let mut remote_fields = vec![];
    for i in 1..=col_count {
        let col_name = cursor
            .col_name(i)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let col_type = cursor
            .col_data_type(i)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let remote_type = RemoteType::Dm(dm_type_to_remote_type(col_type)?);
        let col_nullable = cursor
            .col_nullability(i)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .could_be_nullable();

        remote_fields.push(RemoteField::new(col_name, remote_type, col_nullable));
    }

    Ok(RemoteSchema::new(remote_fields))
}

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
                | odbc_api::DataType::LongVarbinary { length: _ }
        ) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn dm_type_to_remote_type(data_type: odbc_api::DataType) -> DFResult<DmType> {
    match data_type {
        odbc_api::DataType::TinyInt => Ok(DmType::TinyInt),
        odbc_api::DataType::SmallInt => Ok(DmType::SmallInt),
        odbc_api::DataType::Integer => Ok(DmType::Integer),
        odbc_api::DataType::BigInt => Ok(DmType::BigInt),
        odbc_api::DataType::Real => Ok(DmType::Real),
        odbc_api::DataType::Double => Ok(DmType::Double),
        odbc_api::DataType::Numeric { precision, scale } => {
            assert!(precision >= 1);
            assert!(precision <= 38);
            assert!(scale <= 38);
            Ok(DmType::Numeric(precision as u8, scale as i8))
        }
        odbc_api::DataType::Decimal { precision, scale } => {
            assert!(precision >= 1);
            assert!(precision <= 38);
            assert!(scale <= 38);
            Ok(DmType::Decimal(precision as u8, scale as i8))
        }
        odbc_api::DataType::Char { length } => Ok(DmType::Char(length.map(|l| l.get() as u16))),
        odbc_api::DataType::Varchar { length } => {
            Ok(DmType::Varchar(length.map(|l| l.get() as u16)))
        }
        odbc_api::DataType::Binary { length } => Ok(DmType::Binary(
            length.expect("length should not be none").get() as u16,
        )),
        odbc_api::DataType::Varbinary { length } => {
            Ok(DmType::Varbinary(length.map(|l| l.get() as u16)))
        }
        odbc_api::DataType::LongVarchar { .. } => Ok(DmType::Text),
        odbc_api::DataType::LongVarbinary { .. } => Ok(DmType::Image),
        odbc_api::DataType::Bit => Ok(DmType::Bit),
        odbc_api::DataType::Timestamp { precision } => {
            assert!(precision >= 0);
            assert!(precision <= 9);
            Ok(DmType::Timestamp(precision as u8))
        }
        odbc_api::DataType::Time { precision } => {
            assert!(precision >= 0);
            assert!(precision <= 6);
            Ok(DmType::Time(precision as u8))
        }
        odbc_api::DataType::Date => Ok(DmType::Date),
        _ => Err(DataFusionError::Execution(format!(
            "Unsupported DM type: {data_type:?}"
        ))),
    }
}

pub(crate) fn seconds_since_epoch(value: &odbc_api::sys::Timestamp) -> DFResult<i64> {
    let ndt =
        chrono::NaiveDate::from_ymd_opt(value.year as i32, value.month as u32, value.day as u32)
            .ok_or_else(|| DataFusionError::Execution(format!("Invalid timestamp: {value:?}")))?
            .and_hms_opt(value.hour as u32, value.minute as u32, value.second as u32)
            .ok_or_else(|| DataFusionError::Execution(format!("Invalid timestamp: {value:?}")))?;
    Ok::<_, DataFusionError>(ndt.and_utc().timestamp())
}

pub(crate) fn ms_since_epoch(value: &odbc_api::sys::Timestamp) -> DFResult<i64> {
    let ndt =
        chrono::NaiveDate::from_ymd_opt(value.year as i32, value.month as u32, value.day as u32)
            .ok_or_else(|| DataFusionError::Execution(format!("Invalid timestamp: {value:?}")))?
            .and_hms_nano_opt(
                value.hour as u32,
                value.minute as u32,
                value.second as u32,
                value.fraction,
            )
            .ok_or_else(|| DataFusionError::Execution(format!("Invalid timestamp: {value:?}")))?;
    Ok::<_, DataFusionError>(ndt.and_utc().timestamp_millis())
}

pub(crate) fn us_since_epoch(value: &odbc_api::sys::Timestamp) -> DFResult<i64> {
    let ndt =
        chrono::NaiveDate::from_ymd_opt(value.year as i32, value.month as u32, value.day as u32)
            .ok_or_else(|| DataFusionError::Execution(format!("Invalid timestamp: {value:?}")))?
            .and_hms_nano_opt(
                value.hour as u32,
                value.minute as u32,
                value.second as u32,
                value.fraction,
            )
            .ok_or_else(|| DataFusionError::Execution(format!("Invalid timestamp: {value:?}")))?;
    Ok::<_, DataFusionError>(ndt.and_utc().timestamp_micros())
}

pub(crate) fn ns_since_epoch(value: &odbc_api::sys::Timestamp) -> DFResult<i64> {
    let ndt =
        chrono::NaiveDate::from_ymd_opt(value.year as i32, value.month as u32, value.day as u32)
            .ok_or_else(|| DataFusionError::Execution(format!("Invalid timestamp: {value:?}")))?
            .and_hms_nano_opt(
                value.hour as u32,
                value.minute as u32,
                value.second as u32,
                value.fraction,
            )
            .ok_or_else(|| DataFusionError::Execution(format!("Invalid timestamp: {value:?}")))?;

    // The dates that can be represented as nanoseconds are between 1677-09-21T00:12:44.0 and
    // 2262-04-11T23:47:16.854775804
    ndt.and_utc()
        .timestamp_nanos_opt()
        .ok_or_else(|| DataFusionError::Execution(format!("Invalid timestamp: {value:?}")))
}
