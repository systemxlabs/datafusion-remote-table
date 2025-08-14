use crate::connection::{RemoteDbType, big_decimal_to_i128, just_return, projections_contains};
use crate::{
    Connection, ConnectionOptions, DFResult, Pool, PostgresType, RemoteField, RemoteSchema,
    RemoteSchemaRef, RemoteType,
};
use bb8_postgres::PostgresConnectionManager;
use bb8_postgres::tokio_postgres::types::{FromSql, Type};
use bb8_postgres::tokio_postgres::{NoTls, Row};
use bigdecimal::BigDecimal;
use byteorder::{BigEndian, ReadBytesExt};
use chrono::Timelike;
use datafusion::arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder,
    IntervalMonthDayNanoBuilder, LargeStringBuilder, ListBuilder, RecordBatch, RecordBatchOptions,
    StringBuilder, Time64MicrosecondBuilder, Time64NanosecondBuilder, TimestampMicrosecondBuilder,
    TimestampNanosecondBuilder, UInt32Builder, make_builder,
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
use futures::StreamExt;
use log::debug;
use num_bigint::{BigInt, Sign};
use std::any::Any;
use std::string::ToString;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

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

    async fn infer_schema(&self, sql: &str) -> DFResult<RemoteSchemaRef> {
        let sql = RemoteDbType::Postgres.query_limit_1(sql);
        let row = self.conn.query_one(&sql, &[]).await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to execute query {sql} on postgres: {e:?}",))
        })?;
        let remote_schema = Arc::new(build_remote_schema(&row)?);
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

        let sql = RemoteDbType::Postgres.rewrite_query(sql, unparsed_filters, limit);
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
}

fn pg_type_to_remote_type(pg_type: &Type, row: &Row, idx: usize) -> DFResult<PostgresType> {
    match pg_type {
        &Type::INT2 => Ok(PostgresType::Int2),
        &Type::INT4 => Ok(PostgresType::Int4),
        &Type::INT8 => Ok(PostgresType::Int8),
        &Type::FLOAT4 => Ok(PostgresType::Float4),
        &Type::FLOAT8 => Ok(PostgresType::Float8),
        &Type::NUMERIC => {
            let v: Option<BigDecimalFromSql> = row.try_get(idx).map_err(|e| {
                DataFusionError::Execution(format!("Failed to get BigDecimal value: {e:?}"))
            })?;
            let scale = match v {
                Some(v) => v.scale,
                None => 0,
            };
            assert!((scale as u32) <= (i8::MAX as u32));
            Ok(PostgresType::Numeric(scale.try_into().unwrap_or_default()))
        }
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
        other if other.name().eq_ignore_ascii_case("geometry") => Ok(PostgresType::PostGisGeometry),
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported postgres type {pg_type:?}",
        ))),
    }
}

fn build_remote_schema(row: &Row) -> DFResult<RemoteSchema> {
    let mut remote_fields = vec![];
    for (idx, col) in row.columns().iter().enumerate() {
        remote_fields.push(RemoteField::new(
            col.name(),
            RemoteType::Postgres(pg_type_to_remote_type(col.type_(), row, idx)?),
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

#[derive(Debug)]
struct BigDecimalFromSql {
    inner: BigDecimal,
    scale: u16,
}

impl BigDecimalFromSql {
    fn to_decimal_128(&self) -> Option<i128> {
        big_decimal_to_i128(&self.inner, Some(self.scale as i32))
    }
}

#[allow(clippy::cast_sign_loss)]
#[allow(clippy::cast_possible_wrap)]
#[allow(clippy::cast_possible_truncation)]
impl<'a> FromSql<'a> for BigDecimalFromSql {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let raw_u16: Vec<u16> = raw
            .chunks(2)
            .map(|chunk| {
                if chunk.len() == 2 {
                    u16::from_be_bytes([chunk[0], chunk[1]])
                } else {
                    u16::from_be_bytes([chunk[0], 0])
                }
            })
            .collect();

        let base_10_000_digit_count = raw_u16[0];
        let weight = raw_u16[1] as i16;
        let sign = raw_u16[2];
        let scale = raw_u16[3];

        let mut base_10_000_digits = Vec::new();
        for i in 4..4 + base_10_000_digit_count {
            base_10_000_digits.push(raw_u16[i as usize]);
        }

        let mut u8_digits = Vec::new();
        for &base_10_000_digit in base_10_000_digits.iter().rev() {
            let mut base_10_000_digit = base_10_000_digit;
            let mut temp_result = Vec::new();
            while base_10_000_digit > 0 {
                temp_result.push((base_10_000_digit % 10) as u8);
                base_10_000_digit /= 10;
            }
            while temp_result.len() < 4 {
                temp_result.push(0);
            }
            u8_digits.extend(temp_result);
        }
        u8_digits.reverse();

        let value_scale = 4 * (i64::from(base_10_000_digit_count) - i64::from(weight) - 1);
        let size = i64::try_from(u8_digits.len())? + i64::from(scale) - value_scale;
        u8_digits.resize(size as usize, 0);

        let sign = match sign {
            0x4000 => Sign::Minus,
            0x0000 => Sign::Plus,
            _ => {
                return Err(Box::new(DataFusionError::Execution(
                    "Failed to parse big decimal from postgres numeric value".to_string(),
                )));
            }
        };

        let Some(digits) = BigInt::from_radix_be(sign, u8_digits.as_slice(), 10) else {
            return Err(Box::new(DataFusionError::Execution(
                "Failed to parse big decimal from postgres numeric value".to_string(),
            )));
        };
        Ok(BigDecimalFromSql {
            inner: BigDecimal::new(digits, i64::from(scale)),
            scale,
        })
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::NUMERIC)
    }
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
                        BigDecimalFromSql,
                        row,
                        idx,
                        |v: BigDecimalFromSql| {
                            v.to_decimal_128().ok_or_else(|| {
                                DataFusionError::Execution(format!(
                                    "Failed to convert BigDecimal {v:?} to i128",
                                ))
                            })
                        }
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
                DataType::Timestamp(TimeUnit::Microsecond, None) => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        TimestampMicrosecondBuilder,
                        SystemTime,
                        row,
                        idx,
                        |v: SystemTime| {
                            if let Ok(v) = v.duration_since(UNIX_EPOCH) {
                                let timestamp: i64 = v
                                    .as_micros()
                                    .try_into()
                                    .expect("Failed to convert SystemTime to i64");
                                Ok(timestamp)
                            } else {
                                Err(DataFusionError::Execution(format!(
                                    "Failed to convert SystemTime {v:?} to i64 for {field:?} and {col:?}"
                                )))
                            }
                        }
                    );
                }
                DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        TimestampNanosecondBuilder,
                        SystemTime,
                        row,
                        idx,
                        |v: SystemTime| {
                            if let Ok(v) = v.duration_since(UNIX_EPOCH) {
                                let timestamp: i64 = v
                                    .as_nanos()
                                    .try_into()
                                    .expect("Failed to convert SystemTime to i64");
                                Ok(timestamp)
                            } else {
                                Err(DataFusionError::Execution(format!(
                                    "Failed to convert SystemTime {v:?} to i64 for {field:?} and {col:?}"
                                )))
                            }
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
