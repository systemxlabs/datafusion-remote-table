use crate::connection::{RemoteDbType, big_decimal_to_i128, just_return, projections_contains};
use crate::{
    Connection, ConnectionOptions, DFResult, MysqlType, Pool, RemoteField, RemoteSchema,
    RemoteSchemaRef, RemoteType, Unparse,
};
use async_stream::stream;
use bigdecimal::{BigDecimal, num_bigint};
use chrono::Timelike;
use datafusion::arrow::array::{
    ArrayRef, BinaryBuilder, Date32Builder, Decimal128Builder, Decimal256Builder, Float32Builder,
    Float64Builder, Int8Builder, Int16Builder, Int32Builder, Int64Builder, LargeBinaryBuilder,
    LargeStringBuilder, RecordBatch, RecordBatchOptions, StringBuilder, Time32SecondBuilder,
    Time64NanosecondBuilder, TimestampMicrosecondBuilder, UInt8Builder, UInt16Builder,
    UInt32Builder, UInt64Builder, make_builder,
};
use datafusion::arrow::datatypes::{DataType, Date32Type, SchemaRef, TimeUnit, i256};
use datafusion::common::{DataFusionError, project_schema};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use derive_getters::Getters;
use derive_with::With;
use futures::StreamExt;
use futures::lock::Mutex;
use log::debug;
use mysql_async::consts::{ColumnFlags, ColumnType};
use mysql_async::prelude::Queryable;
use mysql_async::{Column, FromValueError, Row, Value};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, Clone, With, Getters)]
pub struct MysqlConnectionOptions {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) database: Option<String>,
    pub(crate) pool_max_size: usize,
    pub(crate) stream_chunk_size: usize,
}

impl MysqlConnectionOptions {
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
            database: None,
            pool_max_size: 10,
            stream_chunk_size: 2048,
        }
    }
}

impl From<MysqlConnectionOptions> for ConnectionOptions {
    fn from(options: MysqlConnectionOptions) -> Self {
        ConnectionOptions::Mysql(options)
    }
}

#[derive(Debug)]
pub struct MysqlPool {
    pool: mysql_async::Pool,
}

pub(crate) fn connect_mysql(options: &MysqlConnectionOptions) -> DFResult<MysqlPool> {
    let pool_opts = mysql_async::PoolOpts::new().with_constraints(
        mysql_async::PoolConstraints::new(0, options.pool_max_size)
            .expect("Failed to create pool constraints"),
    );
    let opts_builder = mysql_async::OptsBuilder::default()
        .ip_or_hostname(options.host.clone())
        .tcp_port(options.port)
        .user(Some(options.username.clone()))
        .pass(Some(options.password.clone()))
        .db_name(options.database.clone())
        .init(vec!["set time_zone='+00:00'".to_string()])
        .pool_opts(pool_opts);
    let pool = mysql_async::Pool::new(opts_builder);
    Ok(MysqlPool { pool })
}

#[async_trait::async_trait]
impl Pool for MysqlPool {
    async fn get(&self) -> DFResult<Arc<dyn Connection>> {
        let conn = self.pool.get_conn().await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to get mysql connection from pool: {e:?}"))
        })?;
        Ok(Arc::new(MysqlConnection {
            conn: Arc::new(Mutex::new(conn)),
        }))
    }
}

#[derive(Debug)]
pub struct MysqlConnection {
    conn: Arc<Mutex<mysql_async::Conn>>,
}

#[async_trait::async_trait]
impl Connection for MysqlConnection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn infer_schema(&self, sql: &str) -> DFResult<RemoteSchemaRef> {
        let sql = RemoteDbType::Mysql.query_limit_1(sql);
        let mut conn = self.conn.lock().await;
        let conn = &mut *conn;
        let stmt = conn.prep(&sql).await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to prepare query {sql} on mysql: {e:?}"))
        })?;
        let remote_schema = Arc::new(build_remote_schema(&stmt)?);
        Ok(remote_schema)
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

        let sql = RemoteDbType::Mysql.rewrite_query(sql, unparsed_filters, limit);
        debug!("[remote-table] executing mysql query: {sql}");

        let projection = projection.cloned();
        let chunk_size = conn_options.stream_chunk_size();
        let conn = Arc::clone(&self.conn);
        let stream = Box::pin(stream! {
            let mut conn = conn.lock().await;
            let mut query_iter = conn
                .query_iter(sql.clone())
                .await
                .map_err(|e| {
                    DataFusionError::Execution(format!("Failed to execute query {sql} on mysql: {e:?}"))
                })?;

            let Some(stream) = query_iter.stream::<Row>().await.map_err(|e| {
                    DataFusionError::Execution(format!("Failed to get stream from mysql: {e:?}"))
                })? else {
                yield Err(DataFusionError::Execution("Get none stream from mysql".to_string()));
                return;
            };

            let mut chunked_stream = stream.chunks(chunk_size).boxed();

            while let Some(chunk) = chunked_stream.next().await {
                let rows = chunk
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to collect rows from mysql due to {e}",
                        ))
                    })?;

                yield Ok::<_, DataFusionError>(rows)
            }
        });

        let stream = stream.map(move |rows| {
            let rows = rows?;
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
        _unparser: Arc<dyn Unparse>,
        _table: &[String],
        _remote_schema: RemoteSchemaRef,
        _input: SendableRecordBatchStream,
    ) -> DFResult<usize> {
        Err(DataFusionError::Execution(
            "Insert operation is not supported for mysql".to_string(),
        ))
    }
}

fn mysql_type_to_remote_type(mysql_col: &Column) -> DFResult<MysqlType> {
    let character_set = mysql_col.character_set();
    let is_utf8_bin_character_set = character_set == 45;
    let is_binary = mysql_col.flags().contains(ColumnFlags::BINARY_FLAG);
    let is_blob = mysql_col.flags().contains(ColumnFlags::BLOB_FLAG);
    let is_unsigned = mysql_col.flags().contains(ColumnFlags::UNSIGNED_FLAG);
    let col_length = mysql_col.column_length();
    match mysql_col.column_type() {
        ColumnType::MYSQL_TYPE_TINY => {
            if is_unsigned {
                Ok(MysqlType::TinyIntUnsigned)
            } else {
                Ok(MysqlType::TinyInt)
            }
        }
        ColumnType::MYSQL_TYPE_SHORT => {
            if is_unsigned {
                Ok(MysqlType::SmallIntUnsigned)
            } else {
                Ok(MysqlType::SmallInt)
            }
        }
        ColumnType::MYSQL_TYPE_INT24 => {
            if is_unsigned {
                Ok(MysqlType::MediumIntUnsigned)
            } else {
                Ok(MysqlType::MediumInt)
            }
        }
        ColumnType::MYSQL_TYPE_LONG => {
            if is_unsigned {
                Ok(MysqlType::IntegerUnsigned)
            } else {
                Ok(MysqlType::Integer)
            }
        }
        ColumnType::MYSQL_TYPE_LONGLONG => {
            if is_unsigned {
                Ok(MysqlType::BigIntUnsigned)
            } else {
                Ok(MysqlType::BigInt)
            }
        }
        ColumnType::MYSQL_TYPE_FLOAT => Ok(MysqlType::Float),
        ColumnType::MYSQL_TYPE_DOUBLE => Ok(MysqlType::Double),
        ColumnType::MYSQL_TYPE_NEWDECIMAL => {
            let precision = (mysql_col.column_length() - 2) as u8;
            let scale = mysql_col.decimals();
            Ok(MysqlType::Decimal(precision, scale))
        }
        ColumnType::MYSQL_TYPE_DATE => Ok(MysqlType::Date),
        ColumnType::MYSQL_TYPE_DATETIME => Ok(MysqlType::Datetime),
        ColumnType::MYSQL_TYPE_TIME => Ok(MysqlType::Time),
        ColumnType::MYSQL_TYPE_TIMESTAMP => Ok(MysqlType::Timestamp),
        ColumnType::MYSQL_TYPE_YEAR => Ok(MysqlType::Year),
        ColumnType::MYSQL_TYPE_STRING if !is_binary => Ok(MysqlType::Char),
        ColumnType::MYSQL_TYPE_STRING if is_binary => {
            if is_utf8_bin_character_set {
                Ok(MysqlType::Char)
            } else {
                Ok(MysqlType::Binary)
            }
        }
        ColumnType::MYSQL_TYPE_VAR_STRING if !is_binary => Ok(MysqlType::Varchar),
        ColumnType::MYSQL_TYPE_VAR_STRING if is_binary => {
            if is_utf8_bin_character_set {
                Ok(MysqlType::Varchar)
            } else {
                Ok(MysqlType::Varbinary)
            }
        }
        ColumnType::MYSQL_TYPE_VARCHAR => Ok(MysqlType::Varchar),
        ColumnType::MYSQL_TYPE_BLOB if is_blob && !is_binary => Ok(MysqlType::Text(col_length)),
        ColumnType::MYSQL_TYPE_BLOB if is_blob && is_binary => {
            if is_utf8_bin_character_set {
                Ok(MysqlType::Text(col_length))
            } else {
                Ok(MysqlType::Blob(col_length))
            }
        }
        ColumnType::MYSQL_TYPE_JSON => Ok(MysqlType::Json),
        ColumnType::MYSQL_TYPE_GEOMETRY => Ok(MysqlType::Geometry),
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported mysql type: {mysql_col:?}",
        ))),
    }
}

fn build_remote_schema(stmt: &mysql_async::Statement) -> DFResult<RemoteSchema> {
    let mut remote_fields = vec![];
    for col in stmt.columns() {
        remote_fields.push(RemoteField::new(
            col.name_str().to_string(),
            RemoteType::Mysql(mysql_type_to_remote_type(col)?),
            true,
        ));
    }
    Ok(RemoteSchema::new(remote_fields))
}

macro_rules! handle_primitive_type {
    ($builder:expr, $field:expr, $col:expr, $builder_ty:ty, $value_ty:ty, $row:expr, $index:expr, $convert:expr) => {{
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
        let v = $row.get_opt::<$value_ty, usize>($index);

        match v {
            None => builder.append_null(),
            Some(Ok(v)) => builder.append_value($convert(v)?),
            Some(Err(FromValueError(Value::NULL))) => builder.append_null(),
            Some(Err(e)) => {
                return Err(DataFusionError::Execution(format!(
                    "Failed to get optional {:?} value for {:?} and {:?}: {e:?}",
                    stringify!($value_ty),
                    $field,
                    $col,
                )));
            }
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
            let col = row.columns_ref().get(idx);
            match field.data_type() {
                DataType::Int8 => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        Int8Builder,
                        i8,
                        row,
                        idx,
                        just_return
                    );
                }
                DataType::Int16 => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        Int16Builder,
                        i16,
                        row,
                        idx,
                        just_return
                    );
                }
                DataType::Int32 => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        Int32Builder,
                        i32,
                        row,
                        idx,
                        just_return
                    );
                }
                DataType::Int64 => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        Int64Builder,
                        i64,
                        row,
                        idx,
                        just_return
                    );
                }
                DataType::UInt8 => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        UInt8Builder,
                        u8,
                        row,
                        idx,
                        just_return
                    );
                }
                DataType::UInt16 => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        UInt16Builder,
                        u16,
                        row,
                        idx,
                        just_return
                    );
                }
                DataType::UInt32 => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        UInt32Builder,
                        u32,
                        row,
                        idx,
                        just_return
                    );
                }
                DataType::UInt64 => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        UInt64Builder,
                        u64,
                        row,
                        idx,
                        just_return
                    );
                }
                DataType::Float32 => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        Float32Builder,
                        f32,
                        row,
                        idx,
                        just_return
                    );
                }
                DataType::Float64 => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        Float64Builder,
                        f64,
                        row,
                        idx,
                        just_return
                    );
                }
                DataType::Decimal128(_precision, scale) => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        Decimal128Builder,
                        BigDecimal,
                        row,
                        idx,
                        |v: BigDecimal| {
                            big_decimal_to_i128(&v, Some(*scale as i32)).ok_or_else(|| {
                                DataFusionError::Execution(format!(
                                    "Failed to convert BigDecimal {v:?} to i128"
                                ))
                            })
                        }
                    );
                }
                DataType::Decimal256(_precision, _scale) => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        Decimal256Builder,
                        BigDecimal,
                        row,
                        idx,
                        |v: BigDecimal| { Ok::<_, DataFusionError>(to_decimal_256(&v)) }
                    );
                }
                DataType::Date32 => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        Date32Builder,
                        chrono::NaiveDate,
                        row,
                        idx,
                        |v: chrono::NaiveDate| {
                            Ok::<_, DataFusionError>(Date32Type::from_naive_date(v))
                        }
                    );
                }
                DataType::Timestamp(TimeUnit::Microsecond, tz_opt) => {
                    match tz_opt {
                        None => {}
                        Some(tz) => {
                            if !tz.eq_ignore_ascii_case("utc") {
                                return Err(DataFusionError::NotImplemented(format!(
                                    "Unsupported data type {:?} for col: {:?}",
                                    field.data_type(),
                                    col
                                )));
                            }
                        }
                    }
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        TimestampMicrosecondBuilder,
                        time::PrimitiveDateTime,
                        row,
                        idx,
                        |v: time::PrimitiveDateTime| {
                            let timestamp_micros =
                                (v.assume_utc().unix_timestamp_nanos() / 1_000) as i64;
                            Ok::<_, DataFusionError>(timestamp_micros)
                        }
                    );
                }
                DataType::Time32(TimeUnit::Second) => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        Time32SecondBuilder,
                        chrono::NaiveTime,
                        row,
                        idx,
                        |v: chrono::NaiveTime| {
                            Ok::<_, DataFusionError>(v.num_seconds_from_midnight() as i32)
                        }
                    );
                }
                DataType::Time64(TimeUnit::Nanosecond) => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        Time64NanosecondBuilder,
                        chrono::NaiveTime,
                        row,
                        idx,
                        |v: chrono::NaiveTime| {
                            let t = i64::from(v.num_seconds_from_midnight()) * 1_000_000_000
                                + i64::from(v.nanosecond());
                            Ok::<_, DataFusionError>(t)
                        }
                    );
                }
                DataType::Utf8 => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        StringBuilder,
                        String,
                        row,
                        idx,
                        just_return
                    );
                }
                DataType::LargeUtf8 => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        LargeStringBuilder,
                        String,
                        row,
                        idx,
                        just_return
                    );
                }
                DataType::Binary => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        BinaryBuilder,
                        Vec<u8>,
                        row,
                        idx,
                        just_return
                    );
                }
                DataType::LargeBinary => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        LargeBinaryBuilder,
                        Vec<u8>,
                        row,
                        idx,
                        just_return
                    );
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
    let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
    Ok(RecordBatch::try_new_with_options(
        projected_schema,
        projected_columns,
        &options,
    )?)
}

fn to_decimal_256(decimal: &BigDecimal) -> i256 {
    let (bigint_value, _) = decimal.as_bigint_and_exponent();
    let mut bigint_bytes = bigint_value.to_signed_bytes_le();

    let is_negative = bigint_value.sign() == num_bigint::Sign::Minus;
    let fill_byte = if is_negative { 0xFF } else { 0x00 };

    if bigint_bytes.len() > 32 {
        bigint_bytes.truncate(32);
    } else {
        bigint_bytes.resize(32, fill_byte);
    };

    let mut array = [0u8; 32];
    array.copy_from_slice(&bigint_bytes);

    i256::from_le_bytes(array)
}
