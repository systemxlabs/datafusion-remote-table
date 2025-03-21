use crate::generated::prost as protobuf;
use crate::{
    connect, ConnectionOptions, DFResult, MysqlConnectionOptions, MysqlType,
    OracleConnectionOptions, OracleType, PostgresConnectionOptions, PostgresType, RemoteField,
    RemoteSchema, RemoteSchemaRef, RemoteTableExec, RemoteType, SqliteType, Transform,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::execution::FunctionRegistry;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::convert_required;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::protobuf::proto_error;
use prost::Message;
use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;

pub trait TransformCodec: Debug + Send + Sync {
    fn try_encode(&self, value: &dyn Transform) -> DFResult<Vec<u8>>;
    fn try_decode(&self, value: &[u8]) -> DFResult<Arc<dyn Transform>>;
}

#[derive(Debug)]
pub struct RemotePhysicalCodec {
    transform_codec: Option<Arc<dyn TransformCodec>>,
}

impl RemotePhysicalCodec {
    pub fn new(transform_codec: Option<Arc<dyn TransformCodec>>) -> Self {
        Self { transform_codec }
    }
}

impl PhysicalExtensionCodec for RemotePhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _registry: &dyn FunctionRegistry,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let proto = protobuf::RemoteTableExec::decode(buf).map_err(|e| {
            DataFusionError::Internal(format!(
                "Failed to decode remote table execution plan: {e:?}"
            ))
        })?;

        let transform = if let Some(bytes) = proto.transform {
            let Some(transform_codec) = self.transform_codec.as_ref() else {
                return Err(DataFusionError::Execution(
                    "No transform codec found".to_string(),
                ));
            };
            Some(transform_codec.try_decode(&bytes)?)
        } else {
            None
        };

        let table_schema: SchemaRef = Arc::new(convert_required!(&proto.table_schema)?);
        let remote_schema = proto
            .remote_schema
            .map(|schema| Arc::new(parse_remote_schema(&schema)));

        let projection: Option<Vec<usize>> = proto
            .projection
            .map(|p| p.projection.iter().map(|n| *n as usize).collect());

        let conn_options = parse_connection_options(proto.conn_options.unwrap());
        let conn = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let pool = connect(&conn_options).await?;
                let conn = pool.get().await?;
                Ok::<_, DataFusionError>(conn)
            })
        })?;

        Ok(Arc::new(RemoteTableExec::try_new(
            conn_options,
            proto.sql,
            table_schema,
            remote_schema,
            projection,
            transform,
            conn,
        )?))
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> DFResult<()> {
        if let Some(exec) = node.as_any().downcast_ref::<RemoteTableExec>() {
            let serialized_transform = if let Some(transform) = exec.transform.as_ref() {
                let Some(transform_codec) = self.transform_codec.as_ref() else {
                    return Err(DataFusionError::Execution(
                        "No transform codec found".to_string(),
                    ));
                };
                let bytes = transform_codec.try_encode(transform.as_ref())?;
                Some(bytes)
            } else {
                None
            };

            let serialized_connection_options = serialize_connection_options(&exec.conn_options);
            let remote_schema = exec.remote_schema.as_ref().map(serialize_remote_schema);

            let proto = protobuf::RemoteTableExec {
                conn_options: Some(serialized_connection_options),
                sql: exec.sql.clone(),
                table_schema: Some(exec.table_schema.as_ref().try_into()?),
                remote_schema,
                projection: exec
                    .projection
                    .as_ref()
                    .map(|p| serialize_projection(p.as_slice())),
                transform: serialized_transform,
            };

            proto.encode(buf).map_err(|e| {
                DataFusionError::Internal(format!(
                    "Failed to encode remote table execution plan: {e:?}"
                ))
            })?;
            Ok(())
        } else {
            Err(DataFusionError::Execution(format!(
                "Failed to encode {}",
                RemoteTableExec::static_name()
            )))
        }
    }
}

fn serialize_connection_options(options: &ConnectionOptions) -> protobuf::ConnectionOptions {
    match options {
        ConnectionOptions::Postgres(options) => protobuf::ConnectionOptions {
            connection_options: Some(protobuf::connection_options::ConnectionOptions::Postgres(
                protobuf::PostgresConnectionOptions {
                    host: options.host.clone(),
                    port: options.port as u32,
                    username: options.username.clone(),
                    password: options.password.clone(),
                    database: options.database.clone(),
                },
            )),
        },
        ConnectionOptions::Mysql(options) => protobuf::ConnectionOptions {
            connection_options: Some(protobuf::connection_options::ConnectionOptions::Mysql(
                protobuf::MysqlConnectionOptions {
                    host: options.host.clone(),
                    port: options.port as u32,
                    username: options.username.clone(),
                    password: options.password.clone(),
                    database: options.database.clone(),
                },
            )),
        },
        ConnectionOptions::Oracle(options) => protobuf::ConnectionOptions {
            connection_options: Some(protobuf::connection_options::ConnectionOptions::Oracle(
                protobuf::OracleConnectionOptions {
                    host: options.host.clone(),
                    port: options.port as u32,
                    username: options.username.clone(),
                    password: options.password.clone(),
                    service_name: options.service_name.clone(),
                },
            )),
        },
        ConnectionOptions::Sqlite(path) => protobuf::ConnectionOptions {
            connection_options: Some(protobuf::connection_options::ConnectionOptions::Sqlite(
                protobuf::SqliteConnectionOptions {
                    path: path.to_str().unwrap().to_string(),
                },
            )),
        },
    }
}

fn parse_connection_options(options: protobuf::ConnectionOptions) -> ConnectionOptions {
    match options.connection_options {
        Some(protobuf::connection_options::ConnectionOptions::Postgres(options)) => {
            ConnectionOptions::Postgres(PostgresConnectionOptions {
                host: options.host,
                port: options.port as u16,
                username: options.username,
                password: options.password,
                database: options.database,
            })
        }
        Some(protobuf::connection_options::ConnectionOptions::Mysql(options)) => {
            ConnectionOptions::Mysql(MysqlConnectionOptions {
                host: options.host,
                port: options.port as u16,
                username: options.username,
                password: options.password,
                database: options.database,
            })
        }
        Some(protobuf::connection_options::ConnectionOptions::Oracle(options)) => {
            ConnectionOptions::Oracle(OracleConnectionOptions {
                host: options.host,
                port: options.port as u16,
                username: options.username,
                password: options.password,
                service_name: options.service_name,
            })
        }
        Some(protobuf::connection_options::ConnectionOptions::Sqlite(options)) => {
            ConnectionOptions::Sqlite(Path::new(&options.path).to_path_buf())
        }
        _ => panic!("Failed to parse connection options: {options:?}"),
    }
}

fn serialize_projection(projection: &[usize]) -> protobuf::Projection {
    protobuf::Projection {
        projection: projection.iter().map(|n| *n as u32).collect(),
    }
}

fn serialize_remote_schema(remote_schema: &RemoteSchemaRef) -> protobuf::RemoteSchema {
    let fields = remote_schema
        .fields
        .iter()
        .map(serialize_remote_field)
        .collect::<Vec<_>>();

    protobuf::RemoteSchema { fields }
}

fn serialize_remote_field(remote_field: &RemoteField) -> protobuf::RemoteField {
    protobuf::RemoteField {
        name: remote_field.name.clone(),
        remote_type: Some(serialize_remote_type(&remote_field.remote_type)),
        nullable: remote_field.nullable,
    }
}

fn serialize_remote_type(remote_type: &RemoteType) -> protobuf::RemoteType {
    match remote_type {
        RemoteType::Postgres(PostgresType::Int2) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::PostgresInt2(
                protobuf::PostgresInt2 {},
            )),
        },
        RemoteType::Postgres(PostgresType::Int4) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::PostgresInt4(
                protobuf::PostgresInt4 {},
            )),
        },
        RemoteType::Postgres(PostgresType::Int8) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::PostgresInt8(
                protobuf::PostgresInt8 {},
            )),
        },
        RemoteType::Postgres(PostgresType::Float4) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::PostgresFloat4(
                protobuf::PostgresFloat4 {},
            )),
        },
        RemoteType::Postgres(PostgresType::Float8) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::PostgresFloat8(
                protobuf::PostgresFloat8 {},
            )),
        },
        RemoteType::Postgres(PostgresType::Numeric(scale)) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::PostgresNumeric(
                protobuf::PostgresNumeric {
                    scale: *scale as i32,
                },
            )),
        },
        RemoteType::Postgres(PostgresType::Name) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::PostgresName(
                protobuf::PostgresName {},
            )),
        },
        RemoteType::Postgres(PostgresType::Varchar) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::PostgresVarchar(
                protobuf::PostgresVarchar {},
            )),
        },
        RemoteType::Postgres(PostgresType::Bpchar) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::PostgresBpchar(
                protobuf::PostgresBpchar {},
            )),
        },
        RemoteType::Postgres(PostgresType::Text) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::PostgresText(
                protobuf::PostgresText {},
            )),
        },
        RemoteType::Postgres(PostgresType::Bytea) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::PostgresBytea(
                protobuf::PostgresBytea {},
            )),
        },
        RemoteType::Postgres(PostgresType::Date) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::PostgresDate(
                protobuf::PostgresDate {},
            )),
        },
        RemoteType::Postgres(PostgresType::Timestamp) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::PostgresTimestamp(
                protobuf::PostgresTimestamp {},
            )),
        },
        RemoteType::Postgres(PostgresType::TimestampTz) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::PostgresTimestampTz(
                protobuf::PostgresTimestampTz {},
            )),
        },
        RemoteType::Postgres(PostgresType::Time) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::PostgresTime(
                protobuf::PostgresTime {},
            )),
        },
        RemoteType::Postgres(PostgresType::Interval) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::PostgresInterval(
                protobuf::PostgresInterval {},
            )),
        },
        RemoteType::Postgres(PostgresType::Bool) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::PostgresBool(
                protobuf::PostgresBool {},
            )),
        },
        RemoteType::Postgres(PostgresType::Json) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::PostgresJson(
                protobuf::PostgresJson {},
            )),
        },
        RemoteType::Postgres(PostgresType::Jsonb) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::PostgresJsonb(
                protobuf::PostgresJsonb {},
            )),
        },
        RemoteType::Postgres(PostgresType::Int2Array) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::PostgresInt2Array(
                protobuf::PostgresInt2Array {},
            )),
        },
        RemoteType::Postgres(PostgresType::Int4Array) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::PostgresInt4Array(
                protobuf::PostgresInt4Array {},
            )),
        },
        RemoteType::Postgres(PostgresType::Int8Array) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::PostgresInt8Array(
                protobuf::PostgresInt8Array {},
            )),
        },
        RemoteType::Postgres(PostgresType::Float4Array) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::PostgresFloat4Array(
                protobuf::PostgresFloat4Array {},
            )),
        },
        RemoteType::Postgres(PostgresType::Float8Array) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::PostgresFloat8Array(
                protobuf::PostgresFloat8Array {},
            )),
        },
        RemoteType::Postgres(PostgresType::VarcharArray) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::PostgresVarcharArray(
                protobuf::PostgresVarcharArray {},
            )),
        },
        RemoteType::Postgres(PostgresType::BpcharArray) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::PostgresBpcharArray(
                protobuf::PostgresBpcharArray {},
            )),
        },
        RemoteType::Postgres(PostgresType::TextArray) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::PostgresTextArray(
                protobuf::PostgresTextArray {},
            )),
        },
        RemoteType::Postgres(PostgresType::ByteaArray) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::PostgresByteaArray(
                protobuf::PostgresByteaArray {},
            )),
        },
        RemoteType::Postgres(PostgresType::BoolArray) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::PostgresBoolArray(
                protobuf::PostgresBoolArray {},
            )),
        },
        RemoteType::Postgres(PostgresType::PostGisGeometry) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::PostgresPostgisGeometry(
                protobuf::PostgresPostGisGeometry {},
            )),
        },

        RemoteType::Mysql(MysqlType::TinyInt) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::MysqlTinyInt(
                protobuf::MysqlTinyInt {},
            )),
        },
        RemoteType::Mysql(MysqlType::TinyIntUnsigned) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::MysqlTinyIntUnsigned(
                protobuf::MysqlTinyIntUnsigned {},
            )),
        },
        RemoteType::Mysql(MysqlType::SmallInt) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::MysqlSmallInt(
                protobuf::MysqlSmallInt {},
            )),
        },
        RemoteType::Mysql(MysqlType::SmallIntUnsigned) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::MysqlSmallIntUnsigned(
                protobuf::MysqlSmallIntUnsigned {},
            )),
        },
        RemoteType::Mysql(MysqlType::MediumInt) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::MysqlMediumInt(
                protobuf::MysqlMediumInt {},
            )),
        },
        RemoteType::Mysql(MysqlType::MediumIntUnsigned) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::MysqlMediumIntUnsigned(
                protobuf::MysqlMediumIntUnsigned {},
            )),
        },
        RemoteType::Mysql(MysqlType::Integer) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::MysqlInteger(
                protobuf::MysqlInteger {},
            )),
        },
        RemoteType::Mysql(MysqlType::IntegerUnsigned) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::MysqlIntegerUnsigned(
                protobuf::MysqlIntegerUnsigned {},
            )),
        },
        RemoteType::Mysql(MysqlType::BigInt) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::MysqlBigInt(
                protobuf::MysqlBigInt {},
            )),
        },
        RemoteType::Mysql(MysqlType::BigIntUnsigned) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::MysqlBigIntUnsigned(
                protobuf::MysqlBigIntUnsigned {},
            )),
        },
        RemoteType::Mysql(MysqlType::Float) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::MysqlFloat(
                protobuf::MysqlFloat {},
            )),
        },
        RemoteType::Mysql(MysqlType::Double) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::MysqlDouble(
                protobuf::MysqlDouble {},
            )),
        },
        RemoteType::Mysql(MysqlType::Decimal(precision, scale)) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::MysqlDecimal(
                protobuf::MysqlDecimal {
                    precision: *precision as u32,
                    scale: *scale as u32,
                },
            )),
        },
        RemoteType::Mysql(MysqlType::Date) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::MysqlDate(
                protobuf::MysqlDate {},
            )),
        },
        RemoteType::Mysql(MysqlType::Datetime) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::MysqlDateTime(
                protobuf::MysqlDateTime {},
            )),
        },
        RemoteType::Mysql(MysqlType::Time) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::MysqlTime(
                protobuf::MysqlTime {},
            )),
        },
        RemoteType::Mysql(MysqlType::Timestamp) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::MysqlTimestamp(
                protobuf::MysqlTimestamp {},
            )),
        },
        RemoteType::Mysql(MysqlType::Year) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::MysqlYear(
                protobuf::MysqlYear {},
            )),
        },
        RemoteType::Mysql(MysqlType::Char) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::MysqlChar(
                protobuf::MysqlChar {},
            )),
        },
        RemoteType::Mysql(MysqlType::Varchar) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::MysqlVarchar(
                protobuf::MysqlVarchar {},
            )),
        },
        RemoteType::Mysql(MysqlType::Binary) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::MysqlBinary(
                protobuf::MysqlBinary {},
            )),
        },
        RemoteType::Mysql(MysqlType::Varbinary) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::MysqlVarbinary(
                protobuf::MysqlVarbinary {},
            )),
        },
        RemoteType::Mysql(MysqlType::Text(len)) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::MysqlText(
                protobuf::MysqlText { length: *len },
            )),
        },
        RemoteType::Mysql(MysqlType::Blob(len)) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::MysqlBlob(
                protobuf::MysqlBlob { length: *len },
            )),
        },
        RemoteType::Mysql(MysqlType::Json) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::MysqlJson(
                protobuf::MysqlJson {},
            )),
        },
        RemoteType::Mysql(MysqlType::Geometry) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::MysqlGeometry(
                protobuf::MysqlGeometry {},
            )),
        },

        RemoteType::Oracle(OracleType::Varchar2(len)) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::OracleVarchar2(
                protobuf::OracleVarchar2 { length: *len },
            )),
        },
        RemoteType::Oracle(OracleType::Char(len)) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::OracleChar(
                protobuf::OracleChar { length: *len },
            )),
        },
        RemoteType::Oracle(OracleType::Number(precision, scale)) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::OracleNumber(
                protobuf::OracleNumber {
                    precision: *precision as u32,
                    scale: *scale as i32,
                },
            )),
        },
        RemoteType::Oracle(OracleType::Date) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::OracleDate(
                protobuf::OracleDate {},
            )),
        },
        RemoteType::Oracle(OracleType::Timestamp) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::OracleTimestamp(
                protobuf::OracleTimestamp {},
            )),
        },
        RemoteType::Sqlite(SqliteType::Null) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::SqliteNull(
                protobuf::SqliteNull {},
            )),
        },
        RemoteType::Sqlite(SqliteType::Integer) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::SqliteInteger(
                protobuf::SqliteInteger {},
            )),
        },
        RemoteType::Sqlite(SqliteType::Real) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::SqliteReal(
                protobuf::SqliteReal {},
            )),
        },
        RemoteType::Sqlite(SqliteType::Text) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::SqliteText(
                protobuf::SqliteText {},
            )),
        },
        RemoteType::Sqlite(SqliteType::Blob) => protobuf::RemoteType {
            r#type: Some(protobuf::remote_type::Type::SqliteBlob(
                protobuf::SqliteBlob {},
            )),
        },
    }
}

fn parse_remote_schema(remote_schema: &protobuf::RemoteSchema) -> RemoteSchema {
    let fields = remote_schema
        .fields
        .iter()
        .map(parse_remote_field)
        .collect::<Vec<_>>();

    RemoteSchema { fields }
}

fn parse_remote_field(field: &protobuf::RemoteField) -> RemoteField {
    RemoteField {
        name: field.name.clone(),
        remote_type: parse_remote_type(field.remote_type.as_ref().unwrap()),
        nullable: field.nullable,
    }
}

fn parse_remote_type(remote_type: &protobuf::RemoteType) -> RemoteType {
    match remote_type.r#type.as_ref().unwrap() {
        protobuf::remote_type::Type::PostgresInt2(_) => RemoteType::Postgres(PostgresType::Int2),
        protobuf::remote_type::Type::PostgresInt4(_) => RemoteType::Postgres(PostgresType::Int4),
        protobuf::remote_type::Type::PostgresInt8(_) => RemoteType::Postgres(PostgresType::Int8),
        protobuf::remote_type::Type::PostgresFloat4(_) => {
            RemoteType::Postgres(PostgresType::Float4)
        }
        protobuf::remote_type::Type::PostgresFloat8(_) => {
            RemoteType::Postgres(PostgresType::Float8)
        }
        protobuf::remote_type::Type::PostgresNumeric(numeric) => {
            RemoteType::Postgres(PostgresType::Numeric(numeric.scale as i8))
        }
        protobuf::remote_type::Type::PostgresName(_) => RemoteType::Postgres(PostgresType::Name),
        protobuf::remote_type::Type::PostgresVarchar(_) => {
            RemoteType::Postgres(PostgresType::Varchar)
        }
        protobuf::remote_type::Type::PostgresBpchar(_) => {
            RemoteType::Postgres(PostgresType::Bpchar)
        }
        protobuf::remote_type::Type::PostgresText(_) => RemoteType::Postgres(PostgresType::Text),
        protobuf::remote_type::Type::PostgresBytea(_) => RemoteType::Postgres(PostgresType::Bytea),
        protobuf::remote_type::Type::PostgresDate(_) => RemoteType::Postgres(PostgresType::Date),
        protobuf::remote_type::Type::PostgresTimestamp(_) => {
            RemoteType::Postgres(PostgresType::Timestamp)
        }
        protobuf::remote_type::Type::PostgresTimestampTz(_) => {
            RemoteType::Postgres(PostgresType::TimestampTz)
        }
        protobuf::remote_type::Type::PostgresTime(_) => RemoteType::Postgres(PostgresType::Time),
        protobuf::remote_type::Type::PostgresInterval(_) => {
            RemoteType::Postgres(PostgresType::Interval)
        }
        protobuf::remote_type::Type::PostgresBool(_) => RemoteType::Postgres(PostgresType::Bool),
        protobuf::remote_type::Type::PostgresJson(_) => RemoteType::Postgres(PostgresType::Json),
        protobuf::remote_type::Type::PostgresJsonb(_) => RemoteType::Postgres(PostgresType::Jsonb),
        protobuf::remote_type::Type::PostgresInt2Array(_) => {
            RemoteType::Postgres(PostgresType::Int2Array)
        }
        protobuf::remote_type::Type::PostgresInt4Array(_) => {
            RemoteType::Postgres(PostgresType::Int4Array)
        }
        protobuf::remote_type::Type::PostgresInt8Array(_) => {
            RemoteType::Postgres(PostgresType::Int8Array)
        }
        protobuf::remote_type::Type::PostgresFloat4Array(_) => {
            RemoteType::Postgres(PostgresType::Float4Array)
        }
        protobuf::remote_type::Type::PostgresFloat8Array(_) => {
            RemoteType::Postgres(PostgresType::Float8Array)
        }
        protobuf::remote_type::Type::PostgresVarcharArray(_) => {
            RemoteType::Postgres(PostgresType::VarcharArray)
        }
        protobuf::remote_type::Type::PostgresBpcharArray(_) => {
            RemoteType::Postgres(PostgresType::BpcharArray)
        }
        protobuf::remote_type::Type::PostgresTextArray(_) => {
            RemoteType::Postgres(PostgresType::TextArray)
        }
        protobuf::remote_type::Type::PostgresByteaArray(_) => {
            RemoteType::Postgres(PostgresType::ByteaArray)
        }
        protobuf::remote_type::Type::PostgresBoolArray(_) => {
            RemoteType::Postgres(PostgresType::BoolArray)
        }
        protobuf::remote_type::Type::PostgresPostgisGeometry(_) => {
            RemoteType::Postgres(PostgresType::PostGisGeometry)
        }
        protobuf::remote_type::Type::MysqlTinyInt(_) => RemoteType::Mysql(MysqlType::TinyInt),
        protobuf::remote_type::Type::MysqlTinyIntUnsigned(_) => {
            RemoteType::Mysql(MysqlType::TinyIntUnsigned)
        }
        protobuf::remote_type::Type::MysqlSmallInt(_) => RemoteType::Mysql(MysqlType::SmallInt),
        protobuf::remote_type::Type::MysqlSmallIntUnsigned(_) => {
            RemoteType::Mysql(MysqlType::SmallIntUnsigned)
        }
        protobuf::remote_type::Type::MysqlMediumInt(_) => RemoteType::Mysql(MysqlType::MediumInt),
        protobuf::remote_type::Type::MysqlMediumIntUnsigned(_) => {
            RemoteType::Mysql(MysqlType::MediumIntUnsigned)
        }
        protobuf::remote_type::Type::MysqlInteger(_) => RemoteType::Mysql(MysqlType::Integer),
        protobuf::remote_type::Type::MysqlIntegerUnsigned(_) => {
            RemoteType::Mysql(MysqlType::IntegerUnsigned)
        }
        protobuf::remote_type::Type::MysqlBigInt(_) => RemoteType::Mysql(MysqlType::BigInt),
        protobuf::remote_type::Type::MysqlBigIntUnsigned(_) => {
            RemoteType::Mysql(MysqlType::BigIntUnsigned)
        }
        protobuf::remote_type::Type::MysqlFloat(_) => RemoteType::Mysql(MysqlType::Float),
        protobuf::remote_type::Type::MysqlDouble(_) => RemoteType::Mysql(MysqlType::Double),
        protobuf::remote_type::Type::MysqlDecimal(decimal) => RemoteType::Mysql(
            MysqlType::Decimal(decimal.precision as u8, decimal.scale as u8),
        ),
        protobuf::remote_type::Type::MysqlDate(_) => RemoteType::Mysql(MysqlType::Date),
        protobuf::remote_type::Type::MysqlDateTime(_) => RemoteType::Mysql(MysqlType::Datetime),
        protobuf::remote_type::Type::MysqlTime(_) => RemoteType::Mysql(MysqlType::Time),
        protobuf::remote_type::Type::MysqlTimestamp(_) => RemoteType::Mysql(MysqlType::Timestamp),
        protobuf::remote_type::Type::MysqlYear(_) => RemoteType::Mysql(MysqlType::Year),
        protobuf::remote_type::Type::MysqlChar(_) => RemoteType::Mysql(MysqlType::Char),
        protobuf::remote_type::Type::MysqlVarchar(_) => RemoteType::Mysql(MysqlType::Varchar),
        protobuf::remote_type::Type::MysqlBinary(_) => RemoteType::Mysql(MysqlType::Binary),
        protobuf::remote_type::Type::MysqlVarbinary(_) => RemoteType::Mysql(MysqlType::Varbinary),
        protobuf::remote_type::Type::MysqlText(text) => {
            RemoteType::Mysql(MysqlType::Text(text.length))
        }
        protobuf::remote_type::Type::MysqlBlob(blob) => {
            RemoteType::Mysql(MysqlType::Blob(blob.length))
        }
        protobuf::remote_type::Type::MysqlJson(_) => RemoteType::Mysql(MysqlType::Json),
        protobuf::remote_type::Type::MysqlGeometry(_) => RemoteType::Mysql(MysqlType::Geometry),
        protobuf::remote_type::Type::OracleVarchar2(varchar) => {
            RemoteType::Oracle(OracleType::Varchar2(varchar.length))
        }
        protobuf::remote_type::Type::OracleChar(char) => {
            RemoteType::Oracle(OracleType::Char(char.length))
        }
        protobuf::remote_type::Type::OracleNumber(number) => RemoteType::Oracle(
            OracleType::Number(number.precision as u8, number.scale as i8),
        ),
        protobuf::remote_type::Type::OracleDate(_) => RemoteType::Oracle(OracleType::Date),
        protobuf::remote_type::Type::OracleTimestamp(_) => {
            RemoteType::Oracle(OracleType::Timestamp)
        }
        protobuf::remote_type::Type::SqliteNull(_) => RemoteType::Sqlite(SqliteType::Null),
        protobuf::remote_type::Type::SqliteInteger(_) => RemoteType::Sqlite(SqliteType::Integer),
        protobuf::remote_type::Type::SqliteReal(_) => RemoteType::Sqlite(SqliteType::Real),
        protobuf::remote_type::Type::SqliteText(_) => RemoteType::Sqlite(SqliteType::Text),
        protobuf::remote_type::Type::SqliteBlob(_) => RemoteType::Sqlite(SqliteType::Blob),
    }
}
