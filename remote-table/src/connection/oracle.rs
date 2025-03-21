use crate::connection::{big_decimal_to_i128, projections_contains};
use crate::transform::transform_batch;
use crate::{
    Connection, DFResult, OracleType, Pool, RemoteField, RemoteSchema, RemoteSchemaRef, RemoteType,
    Transform,
};
use bb8_oracle::OracleConnectionManager;
use datafusion::arrow::array::{
    make_builder, ArrayRef, Decimal128Builder, RecordBatch, StringBuilder,
    TimestampNanosecondBuilder, TimestampSecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::common::{project_schema, DataFusionError};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use oracle::sql_type::OracleType as ColumnType;
use oracle::{Connector, Row};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct OracleConnectionOptions {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub service_name: String,
}

impl OracleConnectionOptions {
    pub fn new(
        host: impl Into<String>,
        port: u16,
        username: impl Into<String>,
        password: impl Into<String>,
        service_name: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            username: username.into(),
            password: password.into(),
            service_name: service_name.into(),
        }
    }
}

#[derive(Debug)]
pub struct OraclePool {
    pool: bb8::Pool<OracleConnectionManager>,
}

pub(crate) async fn connect_oracle(options: &OracleConnectionOptions) -> DFResult<OraclePool> {
    let connect_string = format!(
        "//{}:{}/{}",
        options.host, options.port, options.service_name
    );
    let connector = Connector::new(
        options.username.clone(),
        options.password.clone(),
        connect_string,
    );
    let _ = connector
        .connect()
        .map_err(|e| DataFusionError::Internal(format!("Failed to connect to oracle: {e:?}")))?;
    let manager = OracleConnectionManager::from_connector(connector);
    let pool = bb8::Pool::builder()
        .build(manager)
        .await
        .map_err(|e| DataFusionError::Internal(format!("Failed to create oracle pool: {:?}", e)))?;
    Ok(OraclePool { pool })
}

#[async_trait::async_trait]
impl Pool for OraclePool {
    async fn get(&self) -> DFResult<Arc<dyn Connection>> {
        let conn = self.pool.get_owned().await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to get oracle connection due to {e:?}"))
        })?;
        Ok(Arc::new(OracleConnection { conn }))
    }
}

#[derive(Debug)]
pub struct OracleConnection {
    conn: bb8::PooledConnection<'static, OracleConnectionManager>,
}

#[async_trait::async_trait]
impl Connection for OracleConnection {
    async fn infer_schema(
        &self,
        sql: &str,
        transform: Option<Arc<dyn Transform>>,
    ) -> DFResult<(RemoteSchemaRef, SchemaRef)> {
        let row = self.conn.query_row(sql, &[]).map_err(|e| {
            DataFusionError::Execution(format!("Failed to query one row to infer schema: {e:?}"))
        })?;
        let remote_schema = Arc::new(build_remote_schema(&row)?);
        let arrow_schema = Arc::new(remote_schema.to_arrow_schema());
        if let Some(transform) = transform {
            let batch = rows_to_batch(&[row], &arrow_schema, None)?;
            let transformed_batch = transform_batch(
                batch,
                transform.as_ref(),
                &arrow_schema,
                None,
                Some(&remote_schema),
            )?;
            Ok((remote_schema, transformed_batch.schema()))
        } else {
            Ok((remote_schema, arrow_schema))
        }
    }

    async fn query(
        &self,
        sql: String,
        table_schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> DFResult<SendableRecordBatchStream> {
        let projected_schema = project_schema(&table_schema, projection.as_ref())?;
        let result_set = self.conn.query(&sql, &[]).map_err(|e| {
            DataFusionError::Execution(format!("Failed to execute query on oracle: {e:?}"))
        })?;
        let stream = futures::stream::iter(result_set).chunks(2000).boxed();

        let stream = stream.map(move |rows| {
            let rows: Vec<Row> = rows
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to collect rows from oracle due to {e}",
                    ))
                })?;
            rows_to_batch(rows.as_slice(), &table_schema, projection.as_ref())
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            projected_schema,
            stream,
        )))
    }
}

fn oracle_type_to_remote_type(oracle_type: &ColumnType) -> DFResult<RemoteType> {
    match oracle_type {
        ColumnType::Varchar2(size) => Ok(RemoteType::Oracle(OracleType::Varchar2(*size))),
        ColumnType::Char(size) => Ok(RemoteType::Oracle(OracleType::Char(*size))),
        ColumnType::Number(precision, scale) => {
            Ok(RemoteType::Oracle(OracleType::Number(*precision, *scale)))
        }
        ColumnType::Date => Ok(RemoteType::Oracle(OracleType::Date)),
        ColumnType::Timestamp(_) => Ok(RemoteType::Oracle(OracleType::Timestamp)),
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported oracle type: {oracle_type:?}",
        ))),
    }
}

fn build_remote_schema(row: &Row) -> DFResult<RemoteSchema> {
    let mut remote_fields = vec![];
    for col in row.column_info() {
        let remote_type = oracle_type_to_remote_type(col.oracle_type())?;
        remote_fields.push(RemoteField::new(col.name(), remote_type, col.nullable()));
    }
    Ok(RemoteSchema::new(remote_fields))
}

macro_rules! handle_primitive_type {
    ($builder:expr, $field:expr, $builder_ty:ty, $value_ty:ty, $row:expr, $index:expr) => {{
        let builder = $builder
            .as_any_mut()
            .downcast_mut::<$builder_ty>()
            .unwrap_or_else(|| {
                panic!(
                    concat!(
                        "Failed to downcast builder to ",
                        stringify!($builder_ty),
                        " for {:?}"
                    ),
                    $field
                )
            });
        let v = $row
            .get::<usize, Option<$value_ty>>($index)
            .unwrap_or_else(|e| {
                panic!(
                    concat!(
                        "Failed to get ",
                        stringify!($value_ty),
                        " value for {:?}: {:?}"
                    ),
                    $field, e
                )
            });

        match v {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }};
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
            let col = row.column_info().get(idx);
            match field.data_type() {
                DataType::Utf8 => {
                    handle_primitive_type!(builder, col, StringBuilder, String, row, idx);
                }
                DataType::Decimal128(_precision, scale) => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<Decimal128Builder>()
                        .unwrap_or_else(|| {
                            panic!("Failed to downcast builder to Decimal128Builder for {col:?}")
                        });

                    let v = row.get::<usize, Option<String>>(idx).unwrap_or_else(|e| {
                        panic!("Failed to get String value for {col:?}: {e:?}")
                    });

                    match v {
                        Some(v) => {
                            let decimal = v.parse::<bigdecimal::BigDecimal>().map_err(|e| {
                                DataFusionError::Execution(format!(
                                    "Failed to parse BigDecimal from {v:?}: {e:?}",
                                ))
                            })?;
                            let Some(v) = big_decimal_to_i128(&decimal, Some(*scale as u32)) else {
                                return Err(DataFusionError::Execution(format!(
                                    "Failed to convert BigDecimal to i128 for {decimal:?}",
                                )));
                            };
                            builder.append_value(v);
                        }
                        None => builder.append_null(),
                    }
                }
                DataType::Timestamp(TimeUnit::Second, None) => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<TimestampSecondBuilder>()
                        .unwrap_or_else(|| {
                            panic!(
                                "Failed to downcast builder to TimestampSecondBuilder for {col:?}"
                            )
                        });
                    let v = row
                        .get::<usize, Option<chrono::NaiveDateTime>>(idx)
                        .unwrap_or_else(|e| {
                            panic!("Failed to get chrono::NaiveDateTime value for {col:?}: {e:?}")
                        });

                    match v {
                        Some(v) => {
                            let t = v.and_utc().timestamp();
                            builder.append_value(t);
                        }
                        None => builder.append_null(),
                    }
                }
                DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                    let builder = builder
                                .as_any_mut()
                                .downcast_mut::<TimestampNanosecondBuilder>()
                                .unwrap_or_else(|| {
                                    panic!("Failed to downcast builder to TimestampNanosecondBuilder for {col:?}")
                                });
                    let v = row
                        .get::<usize, Option<chrono::NaiveDateTime>>(idx)
                        .unwrap_or_else(|e| {
                            panic!("Failed to get chrono::NaiveDateTime value for {col:?}: {e:?}")
                        });

                    match v {
                        Some(v) => {
                            let t = v.and_utc().timestamp_nanos_opt().ok_or_else(|| {
                                        DataFusionError::Execution(format!(
                                        "Failed to convert chrono::NaiveDateTime {v} to nanos timestamp"
                                    ))
                                    })?;
                            builder.append_value(t);
                        }
                        None => builder.append_null(),
                    }
                }
                _ => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Unsupported data type {:?} for col: {:?}",
                        field.data_type(),
                        col
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
    Ok(RecordBatch::try_new(projected_schema, projected_columns)?)
}
