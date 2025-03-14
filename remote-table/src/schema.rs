use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum RemoteType {
    Postgres(PostgresType),
    Mysql(MysqlType),
    Oracle(OracleType),
    Sqlite(SqliteType),
}

impl RemoteType {
    pub fn to_arrow_type(&self) -> DataType {
        match self {
            RemoteType::Postgres(postgres_type) => postgres_type.to_arrow_type(),
            RemoteType::Mysql(mysql_type) => mysql_type.to_arrow_type(),
            RemoteType::Oracle(oracle_type) => oracle_type.to_arrow_type(),
            RemoteType::Sqlite(sqlite_type) => sqlite_type.to_arrow_type(),
        }
    }
}

/// https://www.postgresql.org/docs/current/datatype.html
#[derive(Debug, Clone)]
pub enum PostgresType {
    Bool,
    Char,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Text,
    Varchar,
    Bpchar,
    Bytea,
    Date,
    Timestamp,
    TimestampTz,
    Time,
    Int2Array,
    Int4Array,
    Int8Array,
    Float4Array,
    Float8Array,
    TextArray,
    VarcharArray,
    ByteaArray,
    PostGisGeometry,
}

impl PostgresType {
    pub fn to_arrow_type(&self) -> DataType {
        match self {
            PostgresType::Bool => DataType::Boolean,
            PostgresType::Char => DataType::Utf8,
            PostgresType::Int2 => DataType::Int16,
            PostgresType::Int4 => DataType::Int32,
            PostgresType::Int8 => DataType::Int64,
            PostgresType::Float4 => DataType::Float32,
            PostgresType::Float8 => DataType::Float64,
            PostgresType::Text | PostgresType::Varchar | PostgresType::Bpchar => DataType::Utf8,
            PostgresType::Bytea => DataType::Binary,
            PostgresType::Date => DataType::Date32,
            PostgresType::Timestamp => DataType::Timestamp(TimeUnit::Nanosecond, None),
            PostgresType::TimestampTz => {
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
            }
            PostgresType::Time => DataType::Time64(TimeUnit::Nanosecond),
            PostgresType::Int2Array => {
                DataType::List(Arc::new(Field::new("", DataType::Int16, true)))
            }
            PostgresType::Int4Array => {
                DataType::List(Arc::new(Field::new("", DataType::Int32, true)))
            }
            PostgresType::Int8Array => {
                DataType::List(Arc::new(Field::new("", DataType::Int64, true)))
            }
            PostgresType::Float4Array => {
                DataType::List(Arc::new(Field::new("", DataType::Float32, true)))
            }
            PostgresType::Float8Array => {
                DataType::List(Arc::new(Field::new("", DataType::Float64, true)))
            }
            PostgresType::TextArray | PostgresType::VarcharArray => {
                DataType::List(Arc::new(Field::new("", DataType::Utf8, true)))
            }
            PostgresType::ByteaArray => {
                DataType::List(Arc::new(Field::new("", DataType::Binary, true)))
            }
            PostgresType::PostGisGeometry => DataType::Binary,
        }
    }
}

// https://dev.mysql.com/doc/refman/8.4/en/data-types.html
#[derive(Debug, Clone)]
pub enum MysqlType {
    TinyInt,
    SmallInt,
    MediumInt,
    Integer,
    BigInt,
    Float,
    Double,
    Date,
    Datetime,
    Time,
    Timestamp,
    Char,
    Varchar,
    Binary,
    Varbinary,
    TinyText,
    Text,
    MediumText,
    LongText,
    TinyBlob,
    Blob,
    MediumBlob,
    LongBlob,
    Json,
    Geometry,
}

impl MysqlType {
    pub fn to_arrow_type(&self) -> DataType {
        match self {
            MysqlType::TinyInt => DataType::Int8,
            MysqlType::SmallInt => DataType::Int16,
            MysqlType::MediumInt => DataType::Int32,
            MysqlType::Integer => DataType::Int32,
            MysqlType::BigInt => DataType::Int64,
            MysqlType::Float => DataType::Float32,
            MysqlType::Double => DataType::Float64,
            MysqlType::Date => DataType::Date32,
            MysqlType::Datetime => DataType::Timestamp(TimeUnit::Microsecond, None),
            MysqlType::Time => DataType::Time64(TimeUnit::Nanosecond),
            MysqlType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            MysqlType::Char => DataType::Utf8,
            MysqlType::Varchar => DataType::Utf8,
            MysqlType::Binary => DataType::Binary,
            MysqlType::Varbinary => DataType::Binary,
            MysqlType::TinyText => DataType::Utf8,
            MysqlType::Text => DataType::Utf8,
            MysqlType::MediumText => DataType::Utf8,
            MysqlType::LongText => DataType::LargeUtf8,
            MysqlType::TinyBlob => DataType::Binary,
            MysqlType::Blob => DataType::Binary,
            MysqlType::MediumBlob => DataType::Binary,
            MysqlType::LongBlob => DataType::LargeBinary,
            MysqlType::Json => DataType::LargeUtf8,
            MysqlType::Geometry => DataType::LargeBinary,
        }
    }
}

// https://docs.oracle.com/cd/B28359_01/server.111/b28286/sql_elements001.htm#i54330
#[derive(Debug, Clone)]
pub enum OracleType {
    Varchar2(u32),
}

impl OracleType {
    pub fn to_arrow_type(&self) -> DataType {
        match self {
            OracleType::Varchar2(_) => DataType::Utf8,
        }
    }
}

// https://www.sqlite.org/datatype3.html
#[derive(Debug, Clone)]
pub enum SqliteType {
    Null,
    Integer,
    Real,
    Text,
    Blob,
}

impl SqliteType {
    pub fn to_arrow_type(&self) -> DataType {
        match self {
            SqliteType::Null => DataType::Null,
            SqliteType::Integer => DataType::Int64,
            SqliteType::Real => DataType::Float64,
            SqliteType::Text => DataType::Utf8,
            SqliteType::Blob => DataType::Binary,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RemoteField {
    pub name: String,
    pub remote_type: RemoteType,
    pub nullable: bool,
}

impl RemoteField {
    pub fn new(name: impl Into<String>, remote_type: RemoteType, nullable: bool) -> Self {
        RemoteField {
            name: name.into(),
            remote_type,
            nullable,
        }
    }

    pub fn to_arrow_field(&self) -> Field {
        Field::new(
            self.name.clone(),
            self.remote_type.to_arrow_type(),
            self.nullable,
        )
    }
}

#[derive(Debug, Clone)]
pub struct RemoteSchema {
    pub fields: Vec<RemoteField>,
}

impl RemoteSchema {
    pub fn empty() -> Self {
        RemoteSchema { fields: vec![] }
    }
    pub fn new(fields: Vec<RemoteField>) -> Self {
        RemoteSchema { fields }
    }

    pub fn to_arrow_schema(&self) -> Schema {
        let mut fields = vec![];
        for remote_field in self.fields.iter() {
            fields.push(remote_field.to_arrow_field());
        }
        Schema::new(fields)
    }
}

pub fn project_remote_schema(
    schema: &RemoteSchema,
    projection: Option<&Vec<usize>>,
) -> RemoteSchema {
    match projection {
        Some(projection) => {
            let fields = projection
                .iter()
                .map(|i| schema.fields[*i].clone())
                .collect::<Vec<_>>();
            RemoteSchema::new(fields)
        }
        None => schema.clone(),
    }
}
