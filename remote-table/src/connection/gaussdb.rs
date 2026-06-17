use crate::connection::{RemoteDbType, just_return, projections_contains};
use crate::utils::{big_decimal_to_i128, big_decimal_to_i256};
use crate::{
    Connection, ConnectionOptions, DFResult, GaussDBConnectionOptions, GaussDBType, Literalize, Pool,
    PoolState, RemoteField, RemoteSchema, RemoteSchemaRef, RemoteSource, RemoteType,
    literalize_array,
};
use arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, BinaryViewBuilder, BooleanBuilder, Date32Builder,
    Decimal128Builder, Decimal256Builder, FixedSizeBinaryBuilder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, IntervalMonthDayNanoBuilder, LargeStringBuilder,
    ListBuilder, RecordBatch, RecordBatchOptions, StringBuilder, StringViewBuilder,
    Time64MicrosecondBuilder, Time64NanosecondBuilder, TimestampMicrosecondBuilder,
    TimestampNanosecondBuilder, UInt32Builder, make_builder,
};
use arrow::datatypes::{
    DataType, Date32Type, IntervalMonthDayNanoType, IntervalUnit, SchemaRef, TimeUnit,
};
use bb8_gaussdb::GaussDBConnectionManager;
use bb8_gaussdb::tokio_gaussdb::types::{FromSql, Type};
use bb8_gaussdb::tokio_gaussdb::{NoTls, Row, Statement};
use bigdecimal::BigDecimal;
use chrono::Timelike;
use datafusion_common::DataFusionError;
use datafusion_common::project_schema;
use datafusion_execution::SendableRecordBatchStream;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use log::debug;
use num_bigint::{BigInt, Sign};
use std::string::ToString;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug)]
pub struct GaussDBPool {
    pool: bb8::Pool<GaussDBConnectionManager<NoTls>>,
}

#[async_trait::async_trait]
impl Pool for GaussDBPool {
    async fn get(&self) -> DFResult<Arc<dyn Connection>> {
        let conn = self.pool.get_owned().await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to get gaussdb connection due to {e:?}"))
        })?;
        Ok(Arc::new(GaussDBConnection { conn }))
    }

    async fn state(&self) -> DFResult<PoolState> {
        let bb8_state = self.pool.state();
        Ok(PoolState {
            connections: bb8_state.connections as usize,
            idle_connections: bb8_state.idle_connections as usize,
        })
    }
}

pub async fn connect_gaussdb(options: &GaussDBConnectionOptions) -> DFResult<GaussDBPool> {
    let mut config = bb8_gaussdb::tokio_gaussdb::Config::new();
    config
        .host(&options.host)
        .port(options.port)
        .user(&options.username)
        .password(&options.password);
    if let Some(database) = &options.database {
        config.dbname(database);
    }
    let manager = GaussDBConnectionManager::new(config, NoTls);
    let pool = bb8::Pool::builder()
        .max_size(options.pool_max_size as u32)
        .min_idle(Some(options.pool_min_idle as u32))
        .idle_timeout(Some(options.pool_idle_timeout))
        .reaper_rate(options.pool_ttl_check_interval)
        .build(manager)
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to create gaussdb connection pool due to {e}",
            ))
        })?;

    Ok(GaussDBPool { pool })
}

#[derive(Debug)]
pub struct GaussDBConnection {
    pub(crate) conn: bb8::PooledConnection<'static, GaussDBConnectionManager<NoTls>>,
}

// ---- Macros ----

macro_rules! handle_primitive_type {
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
        let v: Option<$value_ty> = $row.try_get($col_idx).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to get value for {:?}: {e:?}",
                $field
            ))
        })?;
        match v {
            Some(value) => builder.append_value($convert(value)?),
            None => builder.append_null(),
        }
    }};
}

macro_rules! handle_primitive_array_type {
    ($builder:expr, $field:expr, $builder_ty:ty, $row:expr, $col_idx:expr, $value_ty:ty, $convert:expr) => {{
        let builder = $builder
            .as_any_mut()
            .downcast_mut::<ListBuilder<$builder_ty>>()
            .unwrap_or_else(|| {
                panic!(
                    "Failed to downcast builder to ListBuilder for {:?}",
                    $field,
                )
            });
        let v: Option<Vec<Option<$value_ty>>> = $row.try_get($col_idx).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to get array value for {:?}: {e:?}",
                $field
            ))
        })?;
        match v {
            Some(values) => {
                for value in values {
                    match value {
                        Some(v) => builder.values().append_value($convert(v)?),
                        None => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            None => builder.append_null(),
        }
    }};
}

// ---- FromSql implementations ----

struct BigDecimalFromSql(BigDecimal);

impl<'a> FromSql<'a> for BigDecimalFromSql {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        // Numeric is stored as digits in base 10000
        if raw.len() < 8 {
            return Err("numeric too short".into());
        }
        let num_digits = u16::from_be_bytes([raw[0], raw[1]]);
        let _weight = i16::from_be_bytes([raw[2], raw[3]]);
        let sign = u16::from_be_bytes([raw[4], raw[5]]);
        let dscale = u16::from_be_bytes([raw[6], raw[7]]);

        let mut result = BigInt::from(0u8);
        for i in 0..num_digits as usize {
            let start = 8 + i * 2;
            let digit = u16::from_be_bytes([raw[start], raw[start + 1]]);
            result = result * 10000u16 + BigInt::from(digit);
        }
        let sign = match sign {
            0x0000 => Sign::Plus,
            0x4000 => Sign::Minus,
            _ => return Err("invalid numeric sign".into()),
        };

        let big_decimal = BigDecimal::new(
            BigInt::from_bytes_be(sign, &result.to_bytes_be().1),
            dscale as i64,
        );
        Ok(BigDecimalFromSql(big_decimal))
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "numeric"
    }
}

struct XmlFromSql(String);

impl<'a> FromSql<'a> for XmlFromSql {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let s = std::str::from_utf8(raw)?;
        Ok(XmlFromSql(s.to_string()))
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "xml"
    }
}

struct GeometryFromSql(Vec<u8>);

impl<'a> FromSql<'a> for GeometryFromSql {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(GeometryFromSql(raw.to_vec()))
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "geometry"
    }
}

struct IntervalFromSql {
    months: i32,
    days: i32,
    time: i64,
}

impl<'a> FromSql<'a> for IntervalFromSql {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if raw.len() != 16 {
            return Err("interval must be 16 bytes".into());
        }
        let time = i64::from_be_bytes([
            raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
        ]);
        let days = i32::from_be_bytes([raw[8], raw[9], raw[10], raw[11]]);
        let months = i32::from_be_bytes([raw[12], raw[13], raw[14], raw[15]]);
        Ok(IntervalFromSql { months, days, time })
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "interval"
    }
}

// ---- Schema inference ----

fn gdb_type_to_remote_type(data_type: &Type) -> DFResult<GaussDBType> {
    Ok(match *data_type {
        Type::INT2 => GaussDBType::Int2,
        Type::INT4 => GaussDBType::Int4,
        Type::INT8 => GaussDBType::Int8,
        Type::FLOAT4 => GaussDBType::Float4,
        Type::FLOAT8 => GaussDBType::Float8,
        Type::NUMERIC | Type::NUMERIC_ARRAY => {
            unimplemented!("NUMERIC is handled via information_schema")
        }
        Type::OID => GaussDBType::Oid,
        Type::NAME => GaussDBType::Name,
        Type::VARCHAR | Type::VARCHAR_ARRAY => GaussDBType::Varchar,
        Type::BPCHAR | Type::BPCHAR_ARRAY => GaussDBType::Bpchar,
        Type::TEXT | Type::TEXT_ARRAY => GaussDBType::Text,
        Type::BYTEA | Type::BYTEA_ARRAY => GaussDBType::Bytea,
        Type::DATE => GaussDBType::Date,
        Type::TIMESTAMP => GaussDBType::Timestamp,
        Type::TIMESTAMPTZ => GaussDBType::TimestampTz,
        Type::TIME => GaussDBType::Time,
        Type::INTERVAL => GaussDBType::Interval,
        Type::BOOL | Type::BOOL_ARRAY => GaussDBType::Bool,
        Type::JSON => GaussDBType::Json,
        Type::JSONB => GaussDBType::Jsonb,
        Type::INT2_ARRAY => GaussDBType::Int2Array,
        Type::INT4_ARRAY => GaussDBType::Int4Array,
        Type::INT8_ARRAY => GaussDBType::Int8Array,
        Type::FLOAT4_ARRAY => GaussDBType::Float4Array,
        Type::FLOAT8_ARRAY => GaussDBType::Float8Array,
        Type::UUID => GaussDBType::Uuid,
        ref ty
            if ty.name() == "xml" =>
        {
            GaussDBType::Xml
        }
        _ => {
            return Err(DataFusionError::NotImplemented(format!(
                "Unsupported gaussdb type: {data_type:?}"
            )));
        }
    })
}

fn parse_gdb_type(data_type: &str, numeric_precision: Option<u8>, numeric_scale: Option<i8>) -> DFResult<GaussDBType> {
    match data_type {
        "smallint" | "int2" => Ok(GaussDBType::Int2),
        "integer" | "int" | "int4" => Ok(GaussDBType::Int4),
        "bigint" | "int8" => Ok(GaussDBType::Int8),
        "real" | "float4" => Ok(GaussDBType::Float4),
        "double precision" | "float8" => Ok(GaussDBType::Float8),
        "numeric" => Ok(GaussDBType::Numeric(
            numeric_precision.unwrap_or(38),
            numeric_scale.unwrap_or(10),
        )),
        "oid" => Ok(GaussDBType::Oid),
        "name" => Ok(GaussDBType::Name),
        "character varying" | "varchar" => Ok(GaussDBType::Varchar),
        "character" | "char" | "bpchar" => Ok(GaussDBType::Bpchar),
        "text" => Ok(GaussDBType::Text),
        "bytea" => Ok(GaussDBType::Bytea),
        "date" => Ok(GaussDBType::Date),
        "time without time zone" | "time" => Ok(GaussDBType::Time),
        "timestamp without time zone" | "timestamp" => Ok(GaussDBType::Timestamp),
        "timestamp with time zone" | "timestamptz" => Ok(GaussDBType::TimestampTz),
        "interval" => Ok(GaussDBType::Interval),
        "boolean" | "bool" => Ok(GaussDBType::Bool),
        "json" => Ok(GaussDBType::Json),
        "jsonb" => Ok(GaussDBType::Jsonb),
        "xml" => Ok(GaussDBType::Xml),
        "uuid" => Ok(GaussDBType::Uuid),
        "ARRAY" | "ARRAY_int2" | "_int2" => Ok(GaussDBType::Int2Array),
        "ARRAY_int4" | "_int4" => Ok(GaussDBType::Int4Array),
        "ARRAY_int8" | "_int8" => Ok(GaussDBType::Int8Array),
        "ARRAY_float4" | "_float4" => Ok(GaussDBType::Float4Array),
        "ARRAY_float8" | "_float8" => Ok(GaussDBType::Float8Array),
        "ARRAY_varchar" | "_varchar" => Ok(GaussDBType::VarcharArray),
        "ARRAY_bpchar" | "_bpchar" => Ok(GaussDBType::BpcharArray),
        "ARRAY_text" | "_text" => Ok(GaussDBType::TextArray),
        "ARRAY_bytea" | "_bytea" => Ok(GaussDBType::ByteaArray),
        "ARRAY_bool" | "_bool" => Ok(GaussDBType::BoolArray),
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported gaussdb type: {data_type}"
        ))),
    }
}

async fn build_remote_schema_for_query(stmt: Statement) -> DFResult<RemoteSchema> {
    let mut remote_fields = Vec::new();
    for col in stmt.columns().iter() {
        let gdb_type = gdb_type_to_remote_type(col.type_())?;
        remote_fields.push(RemoteField::new(col.name(), RemoteType::GaussDB(gdb_type), true));
    }
    Ok(RemoteSchema::new(remote_fields))
}

fn build_remote_schema_for_table(rows: Vec<Row>) -> DFResult<RemoteSchema> {
    let mut remote_fields = vec![];
    for row in rows {
        let column_name: String = row.try_get(0).map_err(|e| {
            DataFusionError::Plan(format!("Failed to get column name from gaussdb row: {e:?}"))
        })?;
        let data_type: String = row.try_get(1).map_err(|e| {
            DataFusionError::Plan(format!("Failed to get data type from gaussdb row: {e:?}"))
        })?;
        let numeric_precision: Option<i32> = row.try_get(2).unwrap_or(None);
        let numeric_scale: Option<i32> = row.try_get(3).unwrap_or(None);
        let is_nullable: String = row.try_get(4).unwrap_or_else(|_| "YES".to_string());
        let column_default: Option<String> = row.try_get(5).unwrap_or(None);
        let is_identity: String = row.try_get(6).unwrap_or_else(|_| "NO".to_string());
        let gdb_type = parse_gdb_type(
            &data_type,
            numeric_precision.map(|p| p as u8),
            numeric_scale.map(|s| s as i8),
        )?;
        let nullable = is_nullable == "YES";
        let auto_increment =
            is_identity == "YES" || column_default.as_ref().map_or(false, |d| d.contains("nextval("));
        remote_fields.push(
            RemoteField::new(column_name, RemoteType::GaussDB(gdb_type), nullable)
                .with_auto_increment(auto_increment),
        );
    }
    Ok(RemoteSchema::new(remote_fields))
}

// ---- Query ----

#[async_trait::async_trait]
impl Connection for GaussDBConnection {
    async fn infer_schema(&self, source: &RemoteSource) -> DFResult<RemoteSchemaRef> {
        match source {
            RemoteSource::Table(table) => {
                let db_type = RemoteDbType::GaussDB;
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
                    "select column_name, data_type, numeric_precision, numeric_scale, is_nullable, column_default, is_identity
                    from information_schema.columns
                    where {} order by ordinal_position",
                    where_condition
                );
                let rows = self.conn.query(&sql, &[]).await.map_err(|e| {
                    DataFusionError::Plan(format!(
                        "Failed to execute query {sql} on gaussdb: {e:?}",
                    ))
                })?;
                let remote_schema = Arc::new(build_remote_schema_for_table(rows)?);
                Ok(remote_schema)
            }
            RemoteSource::Query(query) => {
                let stmt = self.conn.prepare(query).await.map_err(|e| {
                    DataFusionError::Plan(format!(
                        "Failed to execute query {query} on gaussdb: {e:?}",
                    ))
                })?;
                let remote_schema = Arc::new(build_remote_schema_for_query(stmt).await?);
                Ok(remote_schema)
            }
            RemoteSource::Command(cmd) => Err(DataFusionError::NotImplemented(format!(
                "Command {cmd:?} is not supported for GaussDB"
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

        let sql = RemoteDbType::GaussDB.rewrite_query(source, unparsed_filters, limit)?;
        debug!("[remote-table] executing gaussdb query: {sql}");

        let projection = projection.cloned();
        let chunk_size = conn_options.stream_chunk_size();
        let stream = self
            .conn
            .query_raw(&sql, Vec::<String>::new())
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to execute query {sql} on gaussdb: {e}",
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
                        "Failed to collect rows from gaussdb due to {e}",
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
            col_names.push(RemoteDbType::GaussDB.sql_identifier(&remote_field.name));
        }

        let sql = format!(
            "INSERT INTO {} ({}) VALUES {}",
            RemoteDbType::GaussDB.sql_table_name(table),
            col_names.join(","),
            values.join(",")
        );

        let count = self.conn.execute(&sql, &[]).await.map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to execute insert statement on gaussdb: {e:?}, sql: {sql}"
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

// ---- Row to batch conversion ----

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
                DataType::Boolean => {
                    handle_primitive_type!(
                        builder, field, BooleanBuilder, row, idx, bool, just_return
                    );
                }
                DataType::Int16 => {
                    handle_primitive_type!(
                        builder, field, Int16Builder, row, idx, i16, just_return
                    );
                }
                DataType::Int32 => {
                    if field.name() == "oid" {
                        handle_primitive_type!(
                            builder, field, Int32Builder, row, idx, i32, just_return
                        );
                    } else {
                        handle_primitive_type!(
                            builder, field, Int32Builder, row, idx, i32, just_return
                        );
                    }
                }
                DataType::UInt32 => {
                    handle_primitive_type!(
                        builder, field, UInt32Builder, row, idx, i32, |v: i32| {
                            Ok::<u32, DataFusionError>(v as u32)
                        }
                    );
                }
                DataType::Int64 => {
                    handle_primitive_type!(
                        builder, field, Int64Builder, row, idx, i64, just_return
                    );
                }
                DataType::Float32 => {
                    handle_primitive_type!(
                        builder, field, Float32Builder, row, idx, f32, just_return
                    );
                }
                DataType::Float64 => {
                    handle_primitive_type!(
                        builder, field, Float64Builder, row, idx, f64, just_return
                    );
                }
                DataType::Decimal128(_precision, _scale) => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<Decimal128Builder>()
                        .unwrap_or_else(|| {
                            panic!("Failed to downcast builder to Decimal128Builder for {field:?}")
                        });
                    let v: Option<BigDecimalFromSql> = row.try_get(idx).map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to get decimal value for {:?}: {e:?}", field
                        ))
                    })?;
                    match v {
                        Some(bd) => {
                            let i = big_decimal_to_i128(&bd.0, None)?;
                            builder.append_value(i);
                        }
                        None => builder.append_null(),
                    }
                }
                DataType::Decimal256(_precision, scale) => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<Decimal256Builder>()
                        .unwrap_or_else(|| {
                            panic!("Failed to downcast builder to Decimal256Builder for {field:?}")
                        });
                    let v: Option<BigDecimalFromSql> = row.try_get(idx).map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to get decimal value for {:?}: {e:?}", field
                        ))
                    })?;
                    match v {
                        Some(bd) => {
                            let i = big_decimal_to_i256(&bd.0, Some(*scale as i32))?;
                            builder.append_value(i);
                        }
                        None => builder.append_null(),
                    }
                }
                DataType::Utf8 => {
                    if field.name().contains("xml") {
                        let builder = builder
                            .as_any_mut()
                            .downcast_mut::<StringBuilder>()
                            .unwrap_or_else(|| {
                                panic!("Failed to downcast builder to StringBuilder for {field:?}")
                            });
                        let v: Option<XmlFromSql> = row.try_get(idx).map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to get xml value for {:?}: {e:?}",
                                field
                            ))
                        })?;
                        match v {
                            Some(xml) => builder.append_value(xml.0),
                            None => builder.append_null(),
                        }
                    } else {
                        handle_primitive_type!(
                            builder, field, StringBuilder, row, idx, String, just_return
                        );
                    }
                }
                DataType::LargeUtf8 => {
                    if field.name().contains("json") {
                        let builder = builder
                            .as_any_mut()
                            .downcast_mut::<LargeStringBuilder>()
                            .unwrap_or_else(|| {
                                panic!(
                                    "Failed to downcast builder to LargeStringBuilder for {field:?}"
                                )
                            });
                        let v: Option<serde_json::Value> = row.try_get(idx).map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to get json value for {:?}: {e:?}",
                                field
                            ))
                        })?;
                        match v {
                            Some(json_value) => builder.append_value(json_value.to_string()),
                            None => builder.append_null(),
                        }
                    } else {
                        handle_primitive_type!(
                            builder,
                            field,
                            LargeStringBuilder,
                            row,
                            idx,
                            String,
                            just_return
                        );
                    }
                }
                DataType::Utf8View => {
                    if field.name().contains("json") {
                        let builder = builder
                            .as_any_mut()
                            .downcast_mut::<StringViewBuilder>()
                            .unwrap_or_else(|| {
                                panic!(
                                    "Failed to downcast builder to StringViewBuilder for {field:?}"
                                )
                            });
                        let v: Option<serde_json::Value> = row.try_get(idx).map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to get json value for {:?}: {e:?}",
                                field
                            ))
                        })?;
                        match v {
                            Some(json_value) => builder.append_value(json_value.to_string()),
                            None => builder.append_null(),
                        }
                    } else {
                        handle_primitive_type!(
                            builder, field, StringViewBuilder, row, idx, String, just_return
                        );
                    }
                }
                DataType::Binary => {
                    if field.name().contains("geometry") {
                        let builder = builder
                            .as_any_mut()
                            .downcast_mut::<BinaryBuilder>()
                            .unwrap_or_else(|| {
                                panic!("Failed to downcast builder to BinaryBuilder for {field:?}")
                            });
                        let v: Option<GeometryFromSql> = row.try_get(idx).map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to get geometry value for {:?}: {e:?}",
                                field
                            ))
                        })?;
                        match v {
                            Some(geom) => builder.append_value(geom.0),
                            None => builder.append_null(),
                        }
                    } else if field.name().contains("json") {
                        let builder = builder
                            .as_any_mut()
                            .downcast_mut::<BinaryBuilder>()
                            .unwrap_or_else(|| {
                                panic!("Failed to downcast builder to BinaryBuilder for {field:?}")
                            });
                        let v: Option<serde_json::Value> = row.try_get(idx).map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to get json value for {:?}: {e:?}",
                                field
                            ))
                        })?;
                        match v {
                            Some(json_value) => {
                                builder.append_value(json_value.to_string().into_bytes())
                            }
                            None => builder.append_null(),
                        }
                    } else {
                        handle_primitive_type!(
                            builder, field, BinaryBuilder, row, idx, Vec<u8>, just_return
                        );
                    }
                }
                DataType::BinaryView => {
                    if field.name().contains("json") {
                        let builder = builder
                            .as_any_mut()
                            .downcast_mut::<BinaryViewBuilder>()
                            .unwrap_or_else(|| {
                                panic!(
                                    "Failed to downcast builder to BinaryViewBuilder for {field:?}"
                                )
                            });
                        let v: Option<serde_json::Value> = row.try_get(idx).map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to get json value for {:?}: {e:?}",
                                field
                            ))
                        })?;
                        match v {
                            Some(json_value) => {
                                builder.append_value(json_value.to_string().into_bytes())
                            }
                            None => builder.append_null(),
                        }
                    } else {
                        handle_primitive_type!(
                            builder, field, BinaryViewBuilder, row, idx, Vec<u8>, just_return
                        );
                    }
                }
                DataType::FixedSizeBinary(_) => {
                    if field.name().contains("uuid") {
                        let builder = builder
                            .as_any_mut()
                            .downcast_mut::<FixedSizeBinaryBuilder>()
                            .unwrap_or_else(|| {
                                panic!(
                                    "Failed to downcast builder to FixedSizeBinaryBuilder for {field:?}"
                                )
                            });
                        let v: Option<Uuid> = row.try_get(idx).map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to get uuid value for {:?}: {e:?}",
                                field
                            ))
                        })?;
                        match v {
                            Some(uuid) => {
                                builder.append_value(uuid.as_bytes())?;
                            }
                            None => builder.append_null(),
                        }
                    } else {
                        let builder = builder
                            .as_any_mut()
                            .downcast_mut::<FixedSizeBinaryBuilder>()
                            .unwrap_or_else(|| {
                                panic!(
                                    "Failed to downcast builder to FixedSizeBinaryBuilder for {field:?}"
                                )
                            });
                        let v: Option<Vec<u8>> = row.try_get(idx).map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to get binary value for {:?}: {e:?}",
                                field
                            ))
                        })?;
                        match v {
                            Some(bytes) => {
                                builder.append_value(&bytes)?;
                            }
                            None => builder.append_null(),
                        }
                    }
                }
                DataType::Timestamp(TimeUnit::Microsecond, None) => {
                    handle_primitive_type!(
                        builder,
                        field,
                        TimestampMicrosecondBuilder,
                        row,
                        idx,
                        chrono::NaiveDateTime,
                        |v: chrono::NaiveDateTime| {
                            Ok::<_, DataFusionError>(v.and_utc().timestamp_micros())
                        }
                    );
                }
                DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
                    handle_primitive_type!(
                        builder,
                        field,
                        TimestampMicrosecondBuilder,
                        row,
                        idx,
                        chrono::DateTime<chrono::Utc>,
                        |v: chrono::DateTime<chrono::Utc>| {
                            Ok::<_, DataFusionError>(v.timestamp_micros())
                        }
                    );
                }
                DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                    handle_primitive_type!(
                        builder,
                        field,
                        TimestampNanosecondBuilder,
                        row,
                        idx,
                        chrono::NaiveDateTime,
                        |v: chrono::NaiveDateTime| {
                            v.and_utc()
                                .timestamp_nanos_opt()
                                .ok_or_else(|| {
                                    DataFusionError::Execution(format!(
                                        "Timestamp out of range for {field:?}"
                                    ))
                                })
                        }
                    );
                }
                DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => {
                    handle_primitive_type!(
                        builder,
                        field,
                        TimestampNanosecondBuilder,
                        row,
                        idx,
                        chrono::DateTime<chrono::Utc>,
                        |v: chrono::DateTime<chrono::Utc>| {
                            v.timestamp_nanos_opt().ok_or_else(|| {
                                DataFusionError::Execution(format!(
                                    "Timestamp out of range for {field:?}"
                                ))
                            })
                        }
                    );
                }
                DataType::Time64(TimeUnit::Microsecond) => {
                    handle_primitive_type!(
                        builder,
                        field,
                        Time64MicrosecondBuilder,
                        row,
                        idx,
                        chrono::NaiveTime,
                        |v: chrono::NaiveTime| {
                            Ok::<_, DataFusionError>(
                                v.num_seconds_from_midnight() as i64 * 1_000_000
                                    + v.nanosecond() as i64 / 1000,
                            )
                        }
                    );
                }
                DataType::Time64(TimeUnit::Nanosecond) => {
                    handle_primitive_type!(
                        builder,
                        field,
                        Time64NanosecondBuilder,
                        row,
                        idx,
                        chrono::NaiveTime,
                        |v: chrono::NaiveTime| {
                            Ok::<_, DataFusionError>(
                                v.num_seconds_from_midnight() as i64 * 1_000_000_000
                                    + v.nanosecond() as i64,
                            )
                        }
                    );
                }
                DataType::Date32 => {
                    handle_primitive_type!(
                        builder,
                        field,
                        Date32Builder,
                        row,
                        idx,
                        chrono::NaiveDate,
                        |v: chrono::NaiveDate| {
                            Ok::<_, DataFusionError>(Date32Type::from_naive_date(v))
                        }
                    );
                }
                DataType::Interval(IntervalUnit::MonthDayNano) => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<IntervalMonthDayNanoBuilder>()
                        .unwrap_or_else(|| {
                            panic!(
                                "Failed to downcast builder to IntervalMonthDayNanoBuilder for {field:?}"
                            )
                        });
                    let v: Option<IntervalFromSql> = row.try_get(idx).map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to get interval value for {:?}: {e:?}",
                            field
                        ))
                    })?;
                    match v {
                        Some(iv) => {
                            builder.append_value(IntervalMonthDayNanoType::make_value(
                                iv.months, iv.days, iv.time,
                            ));
                        }
                        None => builder.append_null(),
                    }
                }
                DataType::List(inner) => match inner.data_type() {
                    DataType::Int16 => {
                        handle_primitive_array_type!(
                            builder, field, Int16Builder, row, idx, i16, just_return
                        );
                    }
                    DataType::Int32 => {
                        handle_primitive_array_type!(
                            builder, field, Int32Builder, row, idx, i32, just_return
                        );
                    }
                    DataType::Int64 => {
                        handle_primitive_array_type!(
                            builder, field, Int64Builder, row, idx, i64, just_return
                        );
                    }
                    DataType::Float32 => {
                        handle_primitive_array_type!(
                            builder, field, Float32Builder, row, idx, f32, just_return
                        );
                    }
                    DataType::Float64 => {
                        handle_primitive_array_type!(
                            builder, field, Float64Builder, row, idx, f64, just_return
                        );
                    }
                    DataType::Utf8 => {
                        handle_primitive_array_type!(
                            builder, field, StringBuilder, row, idx, String, just_return
                        );
                    }
                    DataType::Binary => {
                        handle_primitive_array_type!(
                            builder, field, BinaryBuilder, row, idx, Vec<u8>, just_return
                        );
                    }
                    DataType::Boolean => {
                        handle_primitive_array_type!(
                            builder, field, BooleanBuilder, row, idx, bool, just_return
                        );
                    }
                    _ => {
                        return Err(DataFusionError::NotImplemented(format!(
                            "Unsupported array inner type {:?} for gaussdb",
                            inner.data_type()
                        )));
                    }
                },
                _ => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Unsupported data type {} ({}) for gaussdb",
                        field.data_type(),
                        field.name()
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
