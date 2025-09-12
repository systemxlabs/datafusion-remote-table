use crate::connection::{RemoteDbType, just_return, projections_contains};
use crate::{
    Connection, ConnectionOptions, DFResult, Pool, PostgresType, RemoteField, RemoteSchema,
    RemoteSchemaRef, RemoteSource, RemoteType, Unparse, unparse_array,
};
use bb8_postgres::PostgresConnectionManager;
use bb8_postgres::tokio_postgres::types::{FromSql, Type};
use bb8_postgres::tokio_postgres::{NoTls, Row, RowStream, Statement};
use byteorder::{BigEndian, ReadBytesExt};
use chrono::Timelike;
use datafusion::arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, IntervalMonthDayNanoBuilder, LargeStringBuilder, ListBuilder, RecordBatch,
    RecordBatchOptions, StringBuilder, Time64MicrosecondBuilder, Time64NanosecondBuilder,
    TimestampMicrosecondBuilder, TimestampNanosecondBuilder, UInt32Builder, make_builder,
};
use datafusion::arrow::datatypes::{
    DataType, Date32Type, IntervalMonthDayNanoType, IntervalUnit, SchemaRef, TimeUnit,
};

use datafusion::common::project_schema;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use derive_getters::Getters;
use derive_with::With;
use futures::{StreamExt, pin_mut};
use log::debug;
use std::any::Any;
use std::collections::BTreeMap;
use std::string::ToString;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone, With, Getters)]
pub struct PostgresConnectionOptions {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) database: Option<String>,
    pub(crate) pool_max_size: usize,
    pub(crate) stream_chunk_size: usize,
}

impl PostgresConnectionOptions {
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

impl From<PostgresConnectionOptions> for ConnectionOptions {
    fn from(options: PostgresConnectionOptions) -> Self {
        ConnectionOptions::Postgres(options)
    }
}

#[derive(Debug)]
pub struct PostgresPool {
    pool: bb8::Pool<PostgresConnectionManager<NoTls>>,
}

#[async_trait::async_trait]
impl Pool for PostgresPool {
    async fn get(&self) -> DFResult<Arc<dyn Connection>> {
        let conn = self.pool.get_owned().await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to get postgres connection due to {e:?}"))
        })?;
        Ok(Arc::new(PostgresConnection { conn }))
    }
}

pub(crate) async fn connect_postgres(
    options: &PostgresConnectionOptions,
) -> DFResult<PostgresPool> {
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
        .build(manager)
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to create postgres connection pool due to {e}",
            ))
        })?;

    Ok(PostgresPool { pool })
}

#[derive(Debug)]
pub(crate) struct PostgresConnection {
    conn: bb8::PooledConnection<'static, PostgresConnectionManager<NoTls>>,
}

#[async_trait::async_trait]
impl Connection for PostgresConnection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn infer_schema(&self, source: &RemoteSource) -> DFResult<RemoteSchemaRef> {
        match source {
            RemoteSource::Table(table) => {
                let db_type = RemoteDbType::Postgres;
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
                    "
select
	column_name,
	case
        when data_type = 'ARRAY'
        		then data_type || udt_name
        when data_type = 'USER-DEFINED'
         		then udt_schema || '.' || udt_name
		else
                data_type
	end as column_type,
	numeric_precision,
	numeric_scale,
	is_nullable
from information_schema.columns
where {}
order by ordinal_position",
                    where_condition
                );
                let rows = self.conn.query(&sql, &[]).await.map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to execute query {sql} on postgres: {e:?}",
                    ))
                })?;
                let remote_schema = Arc::new(build_remote_schema_for_table(rows)?);
                Ok(remote_schema)
            }
            RemoteSource::Query(_query) => {
                let sql = source.query(RemoteDbType::Postgres);
                let stmt = self.conn.prepare(&sql).await.map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to execute query {sql} on postgres: {e:?}",
                    ))
                })?;
                let remote_schema = Arc::new(build_remote_schema_for_query(self, stmt).await?);
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
        let projected_schema = project_schema(&table_schema, projection)?;

        let sql = RemoteDbType::Postgres.rewrite_query(source, unparsed_filters, limit);
        debug!("[remote-table] executing postgres query: {sql}");

        let projection = projection.cloned();
        let chunk_size = conn_options.stream_chunk_size();
        let stream = self
            .conn
            .query_raw(&sql, Vec::<String>::new())
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to execute query {sql} on postgres: {e}",
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
                        "Failed to collect rows from postgres due to {e}",
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
        unparser: Arc<dyn Unparse>,
        table: &[String],
        remote_schema: RemoteSchemaRef,
        mut input: SendableRecordBatchStream,
    ) -> DFResult<usize> {
        let input_schema = input.schema();

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
                col_names.push(RemoteDbType::Postgres.sql_identifier(&remote_field.name));
            }

            let sql = format!(
                "INSERT INTO {} ({}) VALUES {}",
                RemoteDbType::Postgres.sql_table_name(table),
                col_names.join(","),
                values.join(",")
            );

            let count = self.conn.execute(&sql, &[]).await.map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to execute insert statement on postgres: {e:?}, sql: {sql}"
                ))
            })?;
            total_count += count as usize;
        }

        Ok(total_count)
    }
}

async fn build_remote_schema_for_query(
    conn: &PostgresConnection,
    stmt: Statement,
) -> DFResult<RemoteSchema> {
    let mut remote_field_map = BTreeMap::new();
    let mut numeric_field_map = BTreeMap::new();
    for (col_idx, col) in stmt.columns().iter().enumerate() {
        let pg_type = col.type_();
        match col.type_() {
            &Type::NUMERIC => {
                numeric_field_map.insert(col_idx, col.name());
            }
            _ => {
                let remote_type = pg_type_to_remote_type(pg_type)?;
                remote_field_map.insert(
                    col_idx,
                    RemoteField::new(col.name(), RemoteType::Postgres(remote_type), true),
                );
            }
        }
    }
    if !numeric_field_map.is_empty() {
        let row_stream = conn
            .conn
            .query_raw(&stmt, Vec::<String>::new())
            .await
            .map_err(|e| DataFusionError::Execution(format!("Failed to exec stmt: {e:?}")))?;
        let numeric_types = infer_pg_numeric_type(
            row_stream,
            &numeric_field_map.keys().copied().collect::<Vec<_>>(),
        )
        .await?;
        assert_eq!(numeric_field_map.len(), numeric_types.len());

        for ((col_idx, name), remote_type) in
            numeric_field_map.into_iter().zip(numeric_types.into_iter())
        {
            remote_field_map.insert(
                col_idx,
                RemoteField::new(name, RemoteType::Postgres(remote_type), true),
            );
        }
    }
    Ok(RemoteSchema::new(remote_field_map.into_values().collect()))
}

fn pg_type_to_remote_type(pg_type: &Type) -> DFResult<PostgresType> {
    match pg_type {
        &Type::INT2 => Ok(PostgresType::Int2),
        &Type::INT4 => Ok(PostgresType::Int4),
        &Type::INT8 => Ok(PostgresType::Int8),
        &Type::FLOAT4 => Ok(PostgresType::Float4),
        &Type::FLOAT8 => Ok(PostgresType::Float8),
        &Type::OID => Ok(PostgresType::Oid),
        &Type::NAME => Ok(PostgresType::Name),
        &Type::VARCHAR => Ok(PostgresType::Varchar),
        &Type::BPCHAR => Ok(PostgresType::Bpchar),
        &Type::TEXT => Ok(PostgresType::Text),
        &Type::BYTEA => Ok(PostgresType::Bytea),
        &Type::DATE => Ok(PostgresType::Date),
        &Type::TIMESTAMP => Ok(PostgresType::Timestamp),
        &Type::TIMESTAMPTZ => Ok(PostgresType::TimestampTz),
        &Type::TIME => Ok(PostgresType::Time),
        &Type::INTERVAL => Ok(PostgresType::Interval),
        &Type::BOOL => Ok(PostgresType::Bool),
        &Type::JSON => Ok(PostgresType::Json),
        &Type::JSONB => Ok(PostgresType::Jsonb),
        &Type::INT2_ARRAY => Ok(PostgresType::Int2Array),
        &Type::INT4_ARRAY => Ok(PostgresType::Int4Array),
        &Type::INT8_ARRAY => Ok(PostgresType::Int8Array),
        &Type::FLOAT4_ARRAY => Ok(PostgresType::Float4Array),
        &Type::FLOAT8_ARRAY => Ok(PostgresType::Float8Array),
        &Type::VARCHAR_ARRAY => Ok(PostgresType::VarcharArray),
        &Type::BPCHAR_ARRAY => Ok(PostgresType::BpcharArray),
        &Type::TEXT_ARRAY => Ok(PostgresType::TextArray),
        &Type::BYTEA_ARRAY => Ok(PostgresType::ByteaArray),
        &Type::BOOL_ARRAY => Ok(PostgresType::BoolArray),
        &Type::XML => Ok(PostgresType::Xml),
        &Type::UUID => Ok(PostgresType::Uuid),
        other if other.name().eq_ignore_ascii_case("geometry") => Ok(PostgresType::PostGisGeometry),
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported postgres type {pg_type:?}",
        ))),
    }
}

async fn infer_pg_numeric_type(
    row_stream: RowStream,
    col_indexes: &[usize],
) -> DFResult<Vec<PostgresType>> {
    pin_mut!(row_stream);

    let mut remote_type_map = BTreeMap::new();
    while let Some(row) = row_stream.next().await {
        let row = row.map_err(|e| {
            DataFusionError::Execution(format!("Failed to get row from stream: {e:?}"))
        })?;
        for col_idx in col_indexes {
            if remote_type_map.contains_key(col_idx) {
                continue;
            }
            let v: Option<rust_decimal::Decimal> = row.try_get(*col_idx).map_err(|e| {
                DataFusionError::Execution(format!("Failed to get decimal value: {e:?}"))
            })?;
            if let Some(v) = v {
                let scale = v.scale();
                remote_type_map.insert(*col_idx, PostgresType::Numeric(scale as i8));
            }
        }
        if remote_type_map.len() == col_indexes.len() {
            break;
        }
    }

    if remote_type_map.len() != col_indexes.len() {
        let missing_cols = col_indexes
            .iter()
            .filter(|col_idx| !remote_type_map.contains_key(col_idx))
            .collect::<Vec<_>>();
        return Err(DataFusionError::Execution(format!(
            "Failed to infer numeric type for col indexes: {missing_cols:?} as these columns have no non-null values",
        )));
    }
    Ok(remote_type_map.into_values().collect())
}

fn build_remote_schema_for_table(rows: Vec<Row>) -> DFResult<RemoteSchema> {
    let mut remote_fields = vec![];
    for row in rows {
        let columa_name = row.try_get::<_, String>(0).map_err(|e| {
            DataFusionError::Execution(format!("Failed to get col name from postgres row: {e:?}"))
        })?;
        let column_type = row.try_get::<_, String>(1).map_err(|e| {
            DataFusionError::Execution(format!("Failed to get col type from postgres row: {e:?}"))
        })?;
        let numeric_precision = row.try_get::<_, Option<i32>>(2).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to get numeric precision from postgres row: {e:?}"
            ))
        })?;
        let numeric_scale = row.try_get::<_, Option<i32>>(3).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to get numeric scale from postgres row: {e:?}"
            ))
        })?;
        let pg_type = parse_pg_type(&column_type, numeric_precision, numeric_scale)?;
        let is_nullable = row.try_get::<_, String>(4).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to get is_nullable from postgres row: {e:?}"
            ))
        })?;
        let nullable = match is_nullable.as_str() {
            "YES" => true,
            "NO" => false,
            _ => {
                return Err(DataFusionError::Execution(format!(
                    "Unsupported postgres is_nullable value {is_nullable}"
                )));
            }
        };
        remote_fields.push(RemoteField::new(
            columa_name,
            RemoteType::Postgres(pg_type),
            nullable,
        ));
    }
    Ok(RemoteSchema::new(remote_fields))
}

fn parse_pg_type(
    pg_type: &str,
    _numeric_precision: Option<i32>,
    numeric_scale: Option<i32>,
) -> DFResult<PostgresType> {
    if pg_type.starts_with("numeric") {
        let Some(scale) = numeric_scale else {
            return Err(DataFusionError::Execution(
                "Failed to get numeric scale from postgres row".to_string(),
            ));
        };
        return Ok(PostgresType::Numeric(scale as i8));
    }
    match pg_type {
        "smallint" => Ok(PostgresType::Int2),
        "integer" => Ok(PostgresType::Int4),
        "bigint" => Ok(PostgresType::Int8),
        "real" => Ok(PostgresType::Float4),
        "double precision" => Ok(PostgresType::Float8),
        "character varying" => Ok(PostgresType::Varchar),
        "character" => Ok(PostgresType::Bpchar),
        "text" => Ok(PostgresType::Text),
        "bytea" => Ok(PostgresType::Bytea),
        "date" => Ok(PostgresType::Date),
        "time without time zone" => Ok(PostgresType::Time),
        "timestamp without time zone" => Ok(PostgresType::Timestamp),
        "timestamp with time zone" => Ok(PostgresType::TimestampTz),
        "interval" => Ok(PostgresType::Interval),
        "boolean" => Ok(PostgresType::Bool),
        "json" => Ok(PostgresType::Json),
        "jsonb" => Ok(PostgresType::Jsonb),
        "public.geometry" => Ok(PostgresType::PostGisGeometry),
        "ARRAY_int2" => Ok(PostgresType::Int2Array),
        "ARRAY_int4" => Ok(PostgresType::Int4Array),
        "ARRAY_int8" => Ok(PostgresType::Int8Array),
        "ARRAY_float4" => Ok(PostgresType::Float4Array),
        "ARRAY_float8" => Ok(PostgresType::Float8Array),
        "ARRAY_varchar" => Ok(PostgresType::VarcharArray),
        "ARRAY_bpchar" => Ok(PostgresType::BpcharArray),
        "ARRAY_text" => Ok(PostgresType::TextArray),
        "ARRAY_bytea" => Ok(PostgresType::ByteaArray),
        "ARRAY_bool" => Ok(PostgresType::BoolArray),
        "xml" => Ok(PostgresType::Xml),
        "uuid" => Ok(PostgresType::Uuid),
        "oid" => Ok(PostgresType::Oid),
        "name" => Ok(PostgresType::Name),
        _ => Err(DataFusionError::Execution(format!(
            "Unsupported postgres type {pg_type}"
        ))),
    }
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
        let v: Option<$value_ty> = $row.try_get($index).map_err(|e| {
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

macro_rules! handle_primitive_array_type {
    ($builder:expr, $field:expr, $col:expr, $values_builder_ty:ty, $primitive_value_ty:ty, $row:expr, $index:expr) => {{
        let builder = $builder
            .as_any_mut()
            .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
            .unwrap_or_else(|| {
                panic!(
                    "Failed to downcast builder to ListBuilder<Box<dyn ArrayBuilder>> for {:?} and {:?}",
                    $field, $col
                )
            });
        let values_builder = builder
            .values()
            .as_any_mut()
            .downcast_mut::<$values_builder_ty>()
            .unwrap_or_else(|| {
                panic!(
                    "Failed to downcast values builder to {} for {:?} and {:?}",
                    stringify!($builder_ty),
                    $field,
                    $col,
                )
            });
        let v: Option<Vec<$primitive_value_ty>> = $row.try_get($index).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to get {} array value for {:?} and {:?}: {e:?}",
                stringify!($value_ty),
                $field,
                $col,
            ))
        })?;

        match v {
            Some(v) => {
                let v = v.into_iter().map(Some);
                values_builder.extend(v);
                builder.append(true);
            }
            None => builder.append_null(),
        }
    }};
}

// interval_send - Postgres C (https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/timestamp.c#L1032)
// interval values are internally stored as three integral fields: months, days, and microseconds
#[derive(Debug)]
struct IntervalFromSql {
    time: i64,
    day: i32,
    month: i32,
}

impl<'a> FromSql<'a> for IntervalFromSql {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let mut cursor = std::io::Cursor::new(raw);

        let time = cursor.read_i64::<BigEndian>()?;
        let day = cursor.read_i32::<BigEndian>()?;
        let month = cursor.read_i32::<BigEndian>()?;

        Ok(IntervalFromSql { time, day, month })
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::INTERVAL)
    }
}

struct GeometryFromSql<'a> {
    wkb: &'a [u8],
}

impl<'a> FromSql<'a> for GeometryFromSql<'a> {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(GeometryFromSql { wkb: raw })
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty.name(), "geometry")
    }
}

struct XmlFromSql<'a> {
    xml: &'a str,
}

impl<'a> FromSql<'a> for XmlFromSql<'a> {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let xml = str::from_utf8(raw)?;
        Ok(XmlFromSql { xml })
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::XML)
    }
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
            let col = row.columns().get(idx);
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
                DataType::Decimal128(_precision, _scale) => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        Decimal128Builder,
                        rust_decimal::Decimal,
                        row,
                        idx,
                        |v: rust_decimal::Decimal| { Ok::<_, DataFusionError>(v.mantissa()) }
                    );
                }
                DataType::Utf8 => {
                    if col.is_some() && col.unwrap().type_().name().eq_ignore_ascii_case("xml") {
                        let convert: for<'a> fn(XmlFromSql<'a>) -> DFResult<&'a str> =
                            |v| Ok(v.xml);
                        handle_primitive_type!(
                            builder,
                            field,
                            col,
                            StringBuilder,
                            XmlFromSql,
                            row,
                            idx,
                            convert
                        );
                    } else {
                        handle_primitive_type!(
                            builder,
                            field,
                            col,
                            StringBuilder,
                            &str,
                            row,
                            idx,
                            just_return
                        );
                    }
                }
                DataType::LargeUtf8 => {
                    if col.is_some() && matches!(col.unwrap().type_(), &Type::JSON | &Type::JSONB) {
                        handle_primitive_type!(
                            builder,
                            field,
                            col,
                            LargeStringBuilder,
                            serde_json::value::Value,
                            row,
                            idx,
                            |v: serde_json::value::Value| {
                                Ok::<_, DataFusionError>(v.to_string())
                            }
                        );
                    } else {
                        handle_primitive_type!(
                            builder,
                            field,
                            col,
                            LargeStringBuilder,
                            &str,
                            row,
                            idx,
                            just_return
                        );
                    }
                }
                DataType::Binary => {
                    if col.is_some() && col.unwrap().type_().name().eq_ignore_ascii_case("geometry")
                    {
                        let convert: for<'a> fn(GeometryFromSql<'a>) -> DFResult<&'a [u8]> =
                            |v| Ok(v.wkb);
                        handle_primitive_type!(
                            builder,
                            field,
                            col,
                            BinaryBuilder,
                            GeometryFromSql,
                            row,
                            idx,
                            convert
                        );
                    } else if col.is_some()
                        && matches!(col.unwrap().type_(), &Type::JSON | &Type::JSONB)
                    {
                        handle_primitive_type!(
                            builder,
                            field,
                            col,
                            BinaryBuilder,
                            serde_json::value::Value,
                            row,
                            idx,
                            |v: serde_json::value::Value| {
                                Ok::<_, DataFusionError>(v.to_string().into_bytes())
                            }
                        );
                    } else {
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
                }
                DataType::FixedSizeBinary(_) => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<FixedSizeBinaryBuilder>()
                        .unwrap_or_else(|| {
                            panic!(
                                "Failed to downcast builder to FixedSizeBinaryBuilder for {field:?}"
                            )
                        });
                    let v = if col.is_some()
                        && col.unwrap().type_().name().eq_ignore_ascii_case("uuid")
                    {
                        let v: Option<Uuid> = row.try_get(idx).map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to get Uuid value for field {:?}: {e:?}",
                                field
                            ))
                        })?;
                        v.map(|v| v.as_bytes().to_vec())
                    } else {
                        let v: Option<Vec<u8>> = row.try_get(idx).map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to get FixedSizeBinary value for field {:?}: {e:?}",
                                field
                            ))
                        })?;
                        v
                    };

                    match v {
                        Some(v) => builder.append_value(v)?,
                        None => builder.append_null(),
                    }
                }
                DataType::Timestamp(TimeUnit::Microsecond, None) => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        TimestampMicrosecondBuilder,
                        chrono::NaiveDateTime,
                        row,
                        idx,
                        |v: chrono::NaiveDateTime| {
                            let timestamp: i64 = v.and_utc().timestamp_micros();

                            Ok::<i64, DataFusionError>(timestamp)
                        }
                    );
                }
                DataType::Timestamp(TimeUnit::Microsecond, Some(_tz)) => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        TimestampMicrosecondBuilder,
                        chrono::DateTime<chrono::Utc>,
                        row,
                        idx,
                        |v: chrono::DateTime<chrono::Utc>| {
                            let timestamp: i64 = v.timestamp_micros();
                            Ok::<_, DataFusionError>(timestamp)
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
                            let timestamp: i64 = v.and_utc().timestamp_nanos_opt().unwrap_or_else(|| panic!("Failed to get timestamp in nanoseconds from {v} for {field:?} and {col:?}"));
                            Ok::<i64, DataFusionError>(timestamp)
                        }
                    );
                }
                DataType::Timestamp(TimeUnit::Nanosecond, Some(_tz)) => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        TimestampNanosecondBuilder,
                        chrono::DateTime<chrono::Utc>,
                        row,
                        idx,
                        |v: chrono::DateTime<chrono::Utc>| {
                            let timestamp: i64 = v.timestamp_nanos_opt().unwrap_or_else(|| panic!("Failed to get timestamp in nanoseconds from {v} for {field:?} and {col:?}"));
                            Ok::<_, DataFusionError>(timestamp)
                        }
                    );
                }
                DataType::Time64(TimeUnit::Microsecond) => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        Time64MicrosecondBuilder,
                        chrono::NaiveTime,
                        row,
                        idx,
                        |v: chrono::NaiveTime| {
                            let seconds = i64::from(v.num_seconds_from_midnight());
                            let microseconds = i64::from(v.nanosecond()) / 1000;
                            Ok::<_, DataFusionError>(seconds * 1_000_000 + microseconds)
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
                            let timestamp: i64 = i64::from(v.num_seconds_from_midnight())
                                * 1_000_000_000
                                + i64::from(v.nanosecond());
                            Ok::<_, DataFusionError>(timestamp)
                        }
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
                        |v| { Ok::<_, DataFusionError>(Date32Type::from_naive_date(v)) }
                    );
                }
                DataType::Interval(IntervalUnit::MonthDayNano) => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        IntervalMonthDayNanoBuilder,
                        IntervalFromSql,
                        row,
                        idx,
                        |v: IntervalFromSql| {
                            let interval_month_day_nano = IntervalMonthDayNanoType::make_value(
                                v.month,
                                v.day,
                                v.time * 1_000,
                            );
                            Ok::<_, DataFusionError>(interval_month_day_nano)
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
                DataType::List(inner) => match inner.data_type() {
                    DataType::Int16 => {
                        handle_primitive_array_type!(
                            builder,
                            field,
                            col,
                            Int16Builder,
                            i16,
                            row,
                            idx
                        );
                    }
                    DataType::Int32 => {
                        handle_primitive_array_type!(
                            builder,
                            field,
                            col,
                            Int32Builder,
                            i32,
                            row,
                            idx
                        );
                    }
                    DataType::Int64 => {
                        handle_primitive_array_type!(
                            builder,
                            field,
                            col,
                            Int64Builder,
                            i64,
                            row,
                            idx
                        );
                    }
                    DataType::Float32 => {
                        handle_primitive_array_type!(
                            builder,
                            field,
                            col,
                            Float32Builder,
                            f32,
                            row,
                            idx
                        );
                    }
                    DataType::Float64 => {
                        handle_primitive_array_type!(
                            builder,
                            field,
                            col,
                            Float64Builder,
                            f64,
                            row,
                            idx
                        );
                    }
                    DataType::Utf8 => {
                        handle_primitive_array_type!(
                            builder,
                            field,
                            col,
                            StringBuilder,
                            &str,
                            row,
                            idx
                        );
                    }
                    DataType::Binary => {
                        handle_primitive_array_type!(
                            builder,
                            field,
                            col,
                            BinaryBuilder,
                            Vec<u8>,
                            row,
                            idx
                        );
                    }
                    DataType::Boolean => {
                        handle_primitive_array_type!(
                            builder,
                            field,
                            col,
                            BooleanBuilder,
                            bool,
                            row,
                            idx
                        );
                    }
                    _ => {
                        return Err(DataFusionError::NotImplemented(format!(
                            "Unsupported list data type {} for col: {:?}",
                            field.data_type(),
                            col
                        )));
                    }
                },
                _ => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Unsupported data type {} for col: {:?}",
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
