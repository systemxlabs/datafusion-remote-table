use datafusion::arrow::datatypes::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use std::sync::Arc;

use crate::RemoteDbType;

#[derive(Debug, Clone)]
pub enum RemoteType {
    Postgres(PostgresType),
    Mysql(MysqlType),
    Oracle(OracleType),
    Sqlite(SqliteType),
    Dm(DmType),
}

impl RemoteType {
    pub fn to_arrow_type(&self) -> DataType {
        match self {
            RemoteType::Postgres(postgres_type) => postgres_type.to_arrow_type(),
            RemoteType::Mysql(mysql_type) => mysql_type.to_arrow_type(),
            RemoteType::Oracle(oracle_type) => oracle_type.to_arrow_type(),
            RemoteType::Sqlite(sqlite_type) => sqlite_type.to_arrow_type(),
            RemoteType::Dm(dm_type) => dm_type.to_arrow_type(),
        }
    }

    pub fn db_type(&self) -> RemoteDbType {
        match self {
            RemoteType::Postgres(_) => RemoteDbType::Postgres,
            RemoteType::Mysql(_) => RemoteDbType::Mysql,
            RemoteType::Oracle(_) => RemoteDbType::Oracle,
            RemoteType::Sqlite(_) => RemoteDbType::Sqlite,
            RemoteType::Dm(_) => RemoteDbType::Dm,
        }
    }
}

/// https://www.postgresql.org/docs/current/datatype.html
#[derive(Debug, Clone)]
pub enum PostgresType {
    // smallint
    Int2,
    // integer
    Int4,
    // bigint
    Int8,
    // real
    Float4,
    // double precision
    Float8,
    // numeric(p, s), decimal(p, s)
    // precision is a fixed value(38)
    Numeric(i8),
    Oid,
    Name,
    // varchar(n)
    Varchar,
    // char, char(n), bpchar(n), bpchar
    Bpchar,
    Text,
    Bytea,
    Date,
    Timestamp,
    TimestampTz,
    Time,
    Interval,
    Bool,
    Json,
    Jsonb,
    Int2Array,
    Int4Array,
    Int8Array,
    Float4Array,
    Float8Array,
    VarcharArray,
    BpcharArray,
    TextArray,
    ByteaArray,
    BoolArray,
    PostGisGeometry,
    Xml,
    Uuid,
}

impl PostgresType {
    pub fn to_arrow_type(&self) -> DataType {
        match self {
            PostgresType::Int2 => DataType::Int16,
            PostgresType::Int4 => DataType::Int32,
            PostgresType::Int8 => DataType::Int64,
            PostgresType::Float4 => DataType::Float32,
            PostgresType::Float8 => DataType::Float64,
            PostgresType::Numeric(scale) => DataType::Decimal128(38, *scale),
            PostgresType::Oid => DataType::UInt32,
            PostgresType::Name
            | PostgresType::Text
            | PostgresType::Varchar
            | PostgresType::Bpchar
            | PostgresType::Xml => DataType::Utf8,
            PostgresType::Bytea => DataType::Binary,
            PostgresType::Date => DataType::Date32,
            PostgresType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            PostgresType::TimestampTz => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            PostgresType::Time => DataType::Time64(TimeUnit::Nanosecond),
            PostgresType::Interval => DataType::Interval(IntervalUnit::MonthDayNano),
            PostgresType::Bool => DataType::Boolean,
            PostgresType::Json | PostgresType::Jsonb => DataType::LargeUtf8,
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
            PostgresType::VarcharArray | PostgresType::BpcharArray | PostgresType::TextArray => {
                DataType::List(Arc::new(Field::new("", DataType::Utf8, true)))
            }
            PostgresType::ByteaArray => {
                DataType::List(Arc::new(Field::new("", DataType::Binary, true)))
            }
            PostgresType::BoolArray => {
                DataType::List(Arc::new(Field::new("", DataType::Boolean, true)))
            }
            PostgresType::PostGisGeometry => DataType::Binary,
            PostgresType::Uuid => DataType::FixedSizeBinary(16),
        }
    }
}

// https://dev.mysql.com/doc/refman/8.4/en/data-types.html
#[derive(Debug, Clone)]
pub enum MysqlType {
    TinyInt,
    TinyIntUnsigned,
    SmallInt,
    SmallIntUnsigned,
    MediumInt,
    MediumIntUnsigned,
    Integer,
    IntegerUnsigned,
    BigInt,
    BigIntUnsigned,
    Float,
    Double,
    Decimal(u8, u8),
    Date,
    Datetime,
    Time,
    Timestamp,
    Year,
    Char,
    Varchar,
    Binary,
    Varbinary,
    // TinyText, Text, MediumText, LongText
    Text(u32),
    // TinyBlob, Blob, MediumBlob, LongBlob
    Blob(u32),
    Json,
    Geometry,
}

impl MysqlType {
    pub fn to_arrow_type(&self) -> DataType {
        match self {
            MysqlType::TinyInt => DataType::Int8,
            MysqlType::TinyIntUnsigned => DataType::UInt8,
            MysqlType::SmallInt => DataType::Int16,
            MysqlType::SmallIntUnsigned => DataType::UInt16,
            MysqlType::MediumInt => DataType::Int32,
            MysqlType::MediumIntUnsigned => DataType::UInt32,
            MysqlType::Integer => DataType::Int32,
            MysqlType::IntegerUnsigned => DataType::UInt32,
            MysqlType::BigInt => DataType::Int64,
            MysqlType::BigIntUnsigned => DataType::UInt64,
            MysqlType::Float => DataType::Float32,
            MysqlType::Double => DataType::Float64,
            MysqlType::Decimal(precision, scale) => {
                assert!(*scale <= (i8::MAX as u8));
                if *precision > 38 {
                    DataType::Decimal256(*precision, *scale as i8)
                } else {
                    DataType::Decimal128(*precision, *scale as i8)
                }
            }
            MysqlType::Date => DataType::Date32,
            MysqlType::Datetime => DataType::Timestamp(TimeUnit::Microsecond, None),
            MysqlType::Time => DataType::Time64(TimeUnit::Nanosecond),
            MysqlType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            MysqlType::Year => DataType::Int16,
            MysqlType::Char => DataType::Utf8,
            MysqlType::Varchar => DataType::Utf8,
            MysqlType::Binary => DataType::Binary,
            MysqlType::Varbinary => DataType::Binary,
            MysqlType::Text(col_len) => {
                if (*col_len as usize) > (i32::MAX as usize) {
                    DataType::LargeUtf8
                } else {
                    DataType::Utf8
                }
            }
            MysqlType::Blob(col_len) => {
                if (*col_len as usize) > (i32::MAX as usize) {
                    DataType::LargeBinary
                } else {
                    DataType::Binary
                }
            }
            MysqlType::Json => DataType::LargeUtf8,
            MysqlType::Geometry => DataType::LargeBinary,
        }
    }
}

// https://docs.oracle.com/cd/B28359_01/server.111/b28286/sql_elements001.htm#i54330
// https://docs.oracle.com/en/database/oracle/oracle-database/21/lnoci/data-types.html
#[derive(Debug, Clone)]
pub enum OracleType {
    BinaryFloat,
    BinaryDouble,
    Number(u8, i8),
    Float(u8),
    Varchar2(u32),
    NVarchar2(u32),
    Char(u32),
    NChar(u32),
    Long,
    Clob,
    NClob,
    Raw(u32),
    LongRaw,
    Blob,
    Date,
    Timestamp,
    Boolean,
}

impl OracleType {
    pub fn to_arrow_type(&self) -> DataType {
        match self {
            OracleType::BinaryFloat => DataType::Float32,
            OracleType::BinaryDouble => DataType::Float64,
            OracleType::Number(precision, scale) => DataType::Decimal128(*precision, *scale),
            OracleType::Float(_precision) => DataType::Float64,
            OracleType::Varchar2(_) => DataType::Utf8,
            OracleType::NVarchar2(_) => DataType::Utf8,
            OracleType::Char(_) => DataType::Utf8,
            OracleType::NChar(_) => DataType::Utf8,
            OracleType::Long => DataType::Utf8,
            OracleType::Clob => DataType::LargeUtf8,
            OracleType::NClob => DataType::LargeUtf8,
            OracleType::Raw(_) => DataType::Binary,
            OracleType::LongRaw => DataType::Binary,
            OracleType::Blob => DataType::LargeBinary,
            OracleType::Date => DataType::Timestamp(TimeUnit::Second, None),
            OracleType::Timestamp => DataType::Timestamp(TimeUnit::Nanosecond, None),
            OracleType::Boolean => DataType::Boolean,
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

// https://eco.dameng.com/document/dm/zh-cn/pm/odbc-rogramming-guide.html
// https://eco.dameng.com/document/dm/zh-cn/pm/dm8_sql-data-types-operators.html
#[derive(Debug, Clone)]
pub enum DmType {
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    Real,
    Double,
    Numeric(u8, i8),
    Decimal(u8, i8),
    Char(Option<u16>),
    Varchar(Option<u16>),
    Text,
    Binary(u16),
    Varbinary(Option<u16>),
    Image,
    Bit,
    Timestamp(u8),
    Time(u8),
    Date,
}

impl DmType {
    pub fn to_arrow_type(&self) -> DataType {
        match self {
            DmType::TinyInt => DataType::Int8,
            DmType::SmallInt => DataType::Int16,
            DmType::Integer => DataType::Int32,
            DmType::BigInt => DataType::Int64,
            DmType::Real => DataType::Float32,
            DmType::Double => DataType::Float64,
            DmType::Numeric(precision, scale) => DataType::Decimal128(*precision, *scale),
            DmType::Decimal(precision, scale) => DataType::Decimal128(*precision, *scale),
            DmType::Char(_) => DataType::Utf8,
            DmType::Varchar(_) => DataType::Utf8,
            DmType::Text => DataType::Utf8,
            DmType::Binary(len) => DataType::FixedSizeBinary(*len as i32),
            DmType::Varbinary(_) => DataType::Binary,
            DmType::Image => DataType::Binary,
            DmType::Bit => DataType::Boolean,
            DmType::Timestamp(precision) => {
                if *precision == 0 {
                    DataType::Timestamp(TimeUnit::Second, None)
                } else if *precision <= 3 {
                    DataType::Timestamp(TimeUnit::Millisecond, None)
                } else if *precision <= 6 {
                    DataType::Timestamp(TimeUnit::Microsecond, None)
                } else {
                    DataType::Timestamp(TimeUnit::Nanosecond, None)
                }
            }
            DmType::Time(precision) => {
                if *precision == 0 {
                    DataType::Time32(TimeUnit::Second)
                } else if *precision <= 3 {
                    DataType::Time32(TimeUnit::Millisecond)
                } else {
                    DataType::Time64(TimeUnit::Microsecond)
                }
            }
            DmType::Date => DataType::Date32,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RemoteField {
    pub name: String,
    pub remote_type: RemoteType,
    pub nullable: bool,
    pub auto_increment: bool,
}

impl RemoteField {
    pub fn new(name: impl Into<String>, remote_type: RemoteType, nullable: bool) -> Self {
        RemoteField {
            name: name.into(),
            remote_type,
            nullable,
            auto_increment: false,
        }
    }

    pub fn with_auto_increment(mut self, auto_increment: bool) -> Self {
        self.auto_increment = auto_increment;
        self
    }

    pub fn to_arrow_field(&self) -> Field {
        Field::new(
            self.name.clone(),
            self.remote_type.to_arrow_type(),
            self.nullable,
        )
    }
}

pub type RemoteSchemaRef = Arc<RemoteSchema>;

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
