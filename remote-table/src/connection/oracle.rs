use crate::connection::{RemoteDbType, just_return, projections_contains};
use crate::utils::big_decimal_to_i128;
use crate::{
    Connection, ConnectionOptions, DFResult, Literalize, OracleConnectionOptions, OracleType, Pool,
    PoolState, RemoteField, RemoteSchema, RemoteSchemaRef, RemoteSource, RemoteType,
};
use bb8_oracle::OracleConnectionManager;
use datafusion::arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date64Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, LargeBinaryBuilder,
    LargeStringBuilder, RecordBatch, RecordBatchOptions, StringBuilder, StructBuilder,
    TimestampNanosecondBuilder, TimestampSecondBuilder, make_builder,
};
use datafusion::arrow::datatypes::{DataType, Fields, SchemaRef, TimeUnit};
use datafusion::common::{DataFusionError, project_schema};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use log::debug;
use oracle::sql_type::{Object, OracleType as ColumnType};
use oracle::{Connector, Row};
use std::any::Any;
use std::sync::Arc;

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
        .max_size(options.pool_max_size as u32)
        .min_idle(Some(options.pool_min_idle as u32))
        .idle_timeout(Some(options.pool_idle_timeout))
        .reaper_rate(options.pool_ttl_check_interval)
        .build(manager)
        .await
        .map_err(|e| DataFusionError::Internal(format!("Failed to create oracle pool: {e:?}")))?;
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

    async fn state(&self) -> DFResult<PoolState> {
        let bb8_state = self.pool.state();
        Ok(PoolState {
            connections: bb8_state.connections as usize,
            idle_connections: bb8_state.idle_connections as usize,
        })
    }
}

#[derive(Debug)]
pub struct OracleConnection {
    conn: bb8::PooledConnection<'static, OracleConnectionManager>,
}

#[async_trait::async_trait]
impl Connection for OracleConnection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn infer_schema(&self, source: &RemoteSource) -> DFResult<RemoteSchemaRef> {
        let sql = RemoteDbType::Oracle.limit_1_query_if_possible(source);
        let result_set = self.conn.query(&sql, &[]).map_err(|e| {
            DataFusionError::Plan(format!("Failed to execute query {sql} on oracle: {e:?}"))
        })?;
        let remote_schema = Arc::new(build_remote_schema(&result_set)?);
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

        let sql = RemoteDbType::Oracle.rewrite_query(source, unparsed_filters, limit);
        debug!("[remote-table] executing oracle query: {sql}");

        let projection = projection.cloned();
        let chunk_size = conn_options.stream_chunk_size();
        let result_set = self.conn.query(&sql, &[]).map_err(|e| {
            DataFusionError::Execution(format!("Failed to execute query on oracle: {e:?}"))
        })?;
        let stream = futures::stream::iter(result_set).chunks(chunk_size).boxed();

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

    async fn insert(
        &self,
        _conn_options: &ConnectionOptions,
        _literalizer: Arc<dyn Literalize>,
        _table: &[String],
        _remote_schema: RemoteSchemaRef,
        _input: SendableRecordBatchStream,
    ) -> DFResult<usize> {
        Err(DataFusionError::Execution(
            "Insert operation is not supported for oracle".to_string(),
        ))
    }
}

fn oracle_type_to_remote_type(oracle_type: &ColumnType) -> DFResult<OracleType> {
    match oracle_type {
        ColumnType::Number(precision, scale) => {
            // TODO need more investigation on the precision and scale
            let precision = if *precision == 0 { 38 } else { *precision };
            let scale = if *scale == -127 { 0 } else { *scale };
            Ok(OracleType::Number(precision, scale))
        }
        ColumnType::BinaryFloat => Ok(OracleType::BinaryFloat),
        ColumnType::BinaryDouble => Ok(OracleType::BinaryDouble),
        ColumnType::Float(precision) => Ok(OracleType::Float(*precision)),
        ColumnType::Varchar2(size) => Ok(OracleType::Varchar2(*size)),
        ColumnType::NVarchar2(size) => Ok(OracleType::NVarchar2(*size)),
        ColumnType::Char(size) => Ok(OracleType::Char(*size)),
        ColumnType::NChar(size) => Ok(OracleType::NChar(*size)),
        ColumnType::Long => Ok(OracleType::Long),
        ColumnType::CLOB => Ok(OracleType::Clob),
        ColumnType::NCLOB => Ok(OracleType::NClob),
        ColumnType::Raw(size) => Ok(OracleType::Raw(*size)),
        ColumnType::LongRaw => Ok(OracleType::LongRaw),
        ColumnType::BLOB => Ok(OracleType::Blob),
        ColumnType::Date => Ok(OracleType::Date),
        ColumnType::Timestamp(_) => Ok(OracleType::Timestamp),
        ColumnType::Boolean => Ok(OracleType::Boolean),
        ColumnType::Object(_) => Ok(OracleType::SdeGeometry),
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported oracle type: {oracle_type:?}",
        ))),
    }
}

fn build_remote_schema(result_set: &oracle::ResultSet<Row>) -> DFResult<RemoteSchema> {
    let mut remote_fields = vec![];
    for col in result_set.column_info() {
        let remote_type = RemoteType::Oracle(oracle_type_to_remote_type(col.oracle_type())?);
        remote_fields.push(RemoteField::new(col.name(), remote_type, col.nullable()));
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
        let v = $row.get::<usize, Option<$value_ty>>($index).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to get {} value for {:?} and {:?}: {e:?}",
                stringify!($value_ty),
                $field,
                $col
            ))
        })?;

        match v {
            Some(v) => builder.append_value($convert(v)?),
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
                DataType::Decimal128(_precision, scale) => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        Decimal128Builder,
                        String,
                        row,
                        idx,
                        |v: String| {
                            let decimal = v.parse::<bigdecimal::BigDecimal>().map_err(|e| {
                                DataFusionError::Execution(format!(
                                    "Failed to parse BigDecimal from {v:?}: {e:?}",
                                ))
                            })?;
                            big_decimal_to_i128(&decimal, Some(*scale as i32))
                        }
                    );
                }
                DataType::Timestamp(TimeUnit::Second, None) => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        TimestampSecondBuilder,
                        chrono::NaiveDateTime,
                        row,
                        idx,
                        |v: chrono::NaiveDateTime| {
                            let t = v.and_utc().timestamp();
                            Ok::<_, DataFusionError>(t)
                        }
                    );
                }
                DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        TimestampNanosecondBuilder,
                        chrono::NaiveDateTime,
                        row,
                        idx,
                        |v: chrono::NaiveDateTime| {
                            v.and_utc().timestamp_nanos_opt().ok_or_else(|| {
                                DataFusionError::Execution(format!(
                                    "Failed to convert chrono::NaiveDateTime {v} to nanos timestamp"
                                ))
                            })
                        }
                    );
                }
                DataType::Date64 => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        Date64Builder,
                        chrono::NaiveDateTime,
                        row,
                        idx,
                        |v: chrono::NaiveDateTime| {
                            Ok::<_, DataFusionError>(v.and_utc().timestamp_millis())
                        }
                    );
                }
                DataType::Boolean => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        BooleanBuilder,
                        bool,
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
                DataType::Struct(fields) => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<StructBuilder>()
                        .unwrap_or_else(|| {
                            panic!("Failed to downcast builder to StructBuilder for {field:?} and {col:?}")
                        });
                    let object = row.get::<usize, Option<Object>>(idx).map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to get object for {field:?} and {col:?}: {e:?}"
                        ))
                    })?;
                    append_object_to_struct_builder(builder, fields, object)?;
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

macro_rules! append_object_attr {
    ($struct_builder:expr, $field_builder_type:ty, $field:expr, $field_idx:expr, $object_opt:expr, $field_value_ty:ty, $convert:expr) => {{
        let field_builder = $struct_builder
            .field_builder::<$field_builder_type>($field_idx)
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Failed to get {} field builder for {:?}",
                    stringify!($field_builder_type),
                    $field,
                ))
            })?;
        match &$object_opt {
            Some(object) => {
                let field_name = $field.name();
                let field_value_opt =
                    object
                        .get::<Option<$field_value_ty>>(field_name)
                        .map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to get {} field value for {:?}: {e:?}",
                                stringify!($field_value_ty),
                                $field,
                            ))
                        })?;
                match field_value_opt {
                    Some(field_value) => {
                        field_builder.append_value($convert(field_value)?);
                    }
                    None => {
                        field_builder.append_null();
                    }
                }
            }
            None => {
                field_builder.append_null();
            }
        }
    }};
}

fn append_object_to_struct_builder(
    builder: &mut StructBuilder,
    fields: &Fields,
    object_opt: Option<Object>,
) -> DFResult<()> {
    for (idx, field) in fields.iter().enumerate() {
        match field.data_type() {
            DataType::Int32 => {
                append_object_attr!(
                    builder,
                    Int32Builder,
                    field,
                    idx,
                    object_opt,
                    i32,
                    just_return
                );
            }
            DataType::Int64 => {
                append_object_attr!(
                    builder,
                    Int64Builder,
                    field,
                    idx,
                    object_opt,
                    i64,
                    just_return
                );
            }
            DataType::Float32 => {
                append_object_attr!(
                    builder,
                    Float32Builder,
                    field,
                    idx,
                    object_opt,
                    f32,
                    just_return
                );
            }
            DataType::Float64 => {
                append_object_attr!(
                    builder,
                    Float64Builder,
                    field,
                    idx,
                    object_opt,
                    f64,
                    just_return
                );
            }
            DataType::Decimal128(_precision, scale) => {
                append_object_attr!(
                    builder,
                    Decimal128Builder,
                    field,
                    idx,
                    object_opt,
                    String,
                    |v: String| {
                        let decimal = v.parse::<bigdecimal::BigDecimal>().map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to parse BigDecimal from {v:?}: {e:?}",
                            ))
                        })?;
                        big_decimal_to_i128(&decimal, Some(*scale as i32))
                    }
                );
            }
            DataType::Binary => {
                append_object_attr!(
                    builder,
                    BinaryBuilder,
                    field,
                    idx,
                    object_opt,
                    Vec<u8>,
                    just_return
                );
            }
            DataType::LargeBinary => {
                append_object_attr!(
                    builder,
                    LargeBinaryBuilder,
                    field,
                    idx,
                    object_opt,
                    Vec<u8>,
                    just_return
                );
            }
            DataType::Struct(fields) => {
                let field_builder =
                    builder.field_builder::<StructBuilder>(idx).ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "Failed to get struct field builder for {field:?}"
                        ))
                    })?;
                match &object_opt {
                    Some(object) => {
                        let field_value =
                            object.get::<Option<Object>>(field.name()).map_err(|e| {
                                DataFusionError::Execution(format!(
                                    "Failed to get object value for {field:?}: {e:?}"
                                ))
                            })?;
                        append_object_to_struct_builder(field_builder, fields, field_value)?;
                    }
                    None => {
                        field_builder.append_null();
                    }
                }
            }
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported struct field type {}",
                    field.data_type(),
                )));
            }
        }
    }

    match &object_opt {
        Some(_) => {
            builder.append(true);
        }
        None => {
            builder.append_null();
        }
    }
    Ok(())
}
