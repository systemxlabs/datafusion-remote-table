// This file is @generated by prost-build.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoteTableExec {
    #[prost(message, optional, tag = "1")]
    pub conn_options: ::core::option::Option<ConnectionOptions>,
    #[prost(string, tag = "2")]
    pub sql: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub table_schema: ::core::option::Option<::datafusion_proto::protobuf::Schema>,
    #[prost(message, optional, tag = "4")]
    pub remote_schema: ::core::option::Option<RemoteSchema>,
    #[prost(message, optional, tag = "5")]
    pub projection: ::core::option::Option<Projection>,
    #[prost(string, repeated, tag = "6")]
    pub unparsed_filters: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(uint32, optional, tag = "7")]
    pub limit: ::core::option::Option<u32>,
    #[prost(bytes = "vec", tag = "8")]
    pub transform: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConnectionOptions {
    #[prost(
        oneof = "connection_options::ConnectionOptions",
        tags = "1, 2, 3, 4, 5"
    )]
    pub connection_options: ::core::option::Option<connection_options::ConnectionOptions>,
}
/// Nested message and enum types in `ConnectionOptions`.
pub mod connection_options {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum ConnectionOptions {
        #[prost(message, tag = "1")]
        Postgres(super::PostgresConnectionOptions),
        #[prost(message, tag = "2")]
        Mysql(super::MysqlConnectionOptions),
        #[prost(message, tag = "3")]
        Oracle(super::OracleConnectionOptions),
        #[prost(message, tag = "4")]
        Sqlite(super::SqliteConnectionOptions),
        #[prost(message, tag = "5")]
        Dm(super::DmConnectionOptions),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PostgresConnectionOptions {
    #[prost(string, tag = "1")]
    pub host: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub port: u32,
    #[prost(string, tag = "3")]
    pub username: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub password: ::prost::alloc::string::String,
    #[prost(string, optional, tag = "5")]
    pub database: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(uint32, tag = "6")]
    pub pool_max_size: u32,
    #[prost(uint32, tag = "7")]
    pub stream_chunk_size: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MysqlConnectionOptions {
    #[prost(string, tag = "1")]
    pub host: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub port: u32,
    #[prost(string, tag = "3")]
    pub username: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub password: ::prost::alloc::string::String,
    #[prost(string, optional, tag = "5")]
    pub database: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(uint32, tag = "6")]
    pub pool_max_size: u32,
    #[prost(uint32, tag = "7")]
    pub stream_chunk_size: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OracleConnectionOptions {
    #[prost(string, tag = "1")]
    pub host: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub port: u32,
    #[prost(string, tag = "3")]
    pub username: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub password: ::prost::alloc::string::String,
    #[prost(string, tag = "5")]
    pub service_name: ::prost::alloc::string::String,
    #[prost(uint32, tag = "6")]
    pub pool_max_size: u32,
    #[prost(uint32, tag = "7")]
    pub stream_chunk_size: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SqliteConnectionOptions {
    #[prost(string, tag = "1")]
    pub path: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub stream_chunk_size: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DmConnectionOptions {
    #[prost(string, tag = "1")]
    pub host: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub port: u32,
    #[prost(string, tag = "3")]
    pub username: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub password: ::prost::alloc::string::String,
    #[prost(string, optional, tag = "5")]
    pub schema: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(uint32, tag = "6")]
    pub stream_chunk_size: u32,
    #[prost(string, tag = "7")]
    pub driver: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Projection {
    #[prost(uint32, repeated, tag = "1")]
    pub projection: ::prost::alloc::vec::Vec<u32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoteSchema {
    #[prost(message, repeated, tag = "1")]
    pub fields: ::prost::alloc::vec::Vec<RemoteField>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoteField {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub remote_type: ::core::option::Option<RemoteType>,
    #[prost(bool, tag = "3")]
    pub nullable: bool,
}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct RemoteType {
    #[prost(
        oneof = "remote_type::Type",
        tags = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 301, 302, 303, 304, 305, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418"
    )]
    pub r#type: ::core::option::Option<remote_type::Type>,
}
/// Nested message and enum types in `RemoteType`.
pub mod remote_type {
    #[derive(Clone, Copy, PartialEq, ::prost::Oneof)]
    pub enum Type {
        #[prost(message, tag = "1")]
        PostgresInt2(super::PostgresInt2),
        #[prost(message, tag = "2")]
        PostgresInt4(super::PostgresInt4),
        #[prost(message, tag = "3")]
        PostgresInt8(super::PostgresInt8),
        #[prost(message, tag = "4")]
        PostgresFloat4(super::PostgresFloat4),
        #[prost(message, tag = "5")]
        PostgresFloat8(super::PostgresFloat8),
        #[prost(message, tag = "6")]
        PostgresNumeric(super::PostgresNumeric),
        #[prost(message, tag = "7")]
        PostgresName(super::PostgresName),
        #[prost(message, tag = "8")]
        PostgresVarchar(super::PostgresVarchar),
        #[prost(message, tag = "9")]
        PostgresBpchar(super::PostgresBpchar),
        #[prost(message, tag = "10")]
        PostgresText(super::PostgresText),
        #[prost(message, tag = "11")]
        PostgresBytea(super::PostgresBytea),
        #[prost(message, tag = "12")]
        PostgresDate(super::PostgresDate),
        #[prost(message, tag = "13")]
        PostgresTime(super::PostgresTime),
        #[prost(message, tag = "14")]
        PostgresTimestamp(super::PostgresTimestamp),
        #[prost(message, tag = "15")]
        PostgresTimestampTz(super::PostgresTimestampTz),
        #[prost(message, tag = "16")]
        PostgresInterval(super::PostgresInterval),
        #[prost(message, tag = "17")]
        PostgresBool(super::PostgresBool),
        #[prost(message, tag = "18")]
        PostgresJson(super::PostgresJson),
        #[prost(message, tag = "19")]
        PostgresJsonb(super::PostgresJsonb),
        #[prost(message, tag = "20")]
        PostgresInt2Array(super::PostgresInt2Array),
        #[prost(message, tag = "21")]
        PostgresInt4Array(super::PostgresInt4Array),
        #[prost(message, tag = "22")]
        PostgresInt8Array(super::PostgresInt8Array),
        #[prost(message, tag = "23")]
        PostgresFloat4Array(super::PostgresFloat4Array),
        #[prost(message, tag = "24")]
        PostgresFloat8Array(super::PostgresFloat8Array),
        #[prost(message, tag = "25")]
        PostgresVarcharArray(super::PostgresVarcharArray),
        #[prost(message, tag = "26")]
        PostgresBpcharArray(super::PostgresBpcharArray),
        #[prost(message, tag = "27")]
        PostgresTextArray(super::PostgresTextArray),
        #[prost(message, tag = "28")]
        PostgresByteaArray(super::PostgresByteaArray),
        #[prost(message, tag = "29")]
        PostgresBoolArray(super::PostgresBoolArray),
        #[prost(message, tag = "30")]
        PostgresPostgisGeometry(super::PostgresPostGisGeometry),
        #[prost(message, tag = "31")]
        PostgresOid(super::PostgresOid),
        #[prost(message, tag = "101")]
        MysqlTinyInt(super::MysqlTinyInt),
        #[prost(message, tag = "102")]
        MysqlTinyIntUnsigned(super::MysqlTinyIntUnsigned),
        #[prost(message, tag = "103")]
        MysqlSmallInt(super::MysqlSmallInt),
        #[prost(message, tag = "104")]
        MysqlSmallIntUnsigned(super::MysqlSmallIntUnsigned),
        #[prost(message, tag = "105")]
        MysqlMediumInt(super::MysqlMediumInt),
        #[prost(message, tag = "106")]
        MysqlMediumIntUnsigned(super::MysqlMediumIntUnsigned),
        #[prost(message, tag = "107")]
        MysqlInteger(super::MysqlInteger),
        #[prost(message, tag = "108")]
        MysqlIntegerUnsigned(super::MysqlIntegerUnsigned),
        #[prost(message, tag = "109")]
        MysqlBigInt(super::MysqlBigInt),
        #[prost(message, tag = "110")]
        MysqlBigIntUnsigned(super::MysqlBigIntUnsigned),
        #[prost(message, tag = "111")]
        MysqlFloat(super::MysqlFloat),
        #[prost(message, tag = "112")]
        MysqlDouble(super::MysqlDouble),
        #[prost(message, tag = "113")]
        MysqlDecimal(super::MysqlDecimal),
        #[prost(message, tag = "114")]
        MysqlDate(super::MysqlDate),
        #[prost(message, tag = "115")]
        MysqlDateTime(super::MysqlDateTime),
        #[prost(message, tag = "116")]
        MysqlTime(super::MysqlTime),
        #[prost(message, tag = "117")]
        MysqlTimestamp(super::MysqlTimestamp),
        #[prost(message, tag = "118")]
        MysqlYear(super::MysqlYear),
        #[prost(message, tag = "119")]
        MysqlChar(super::MysqlChar),
        #[prost(message, tag = "120")]
        MysqlVarchar(super::MysqlVarchar),
        #[prost(message, tag = "121")]
        MysqlBinary(super::MysqlBinary),
        #[prost(message, tag = "122")]
        MysqlVarbinary(super::MysqlVarbinary),
        #[prost(message, tag = "123")]
        MysqlText(super::MysqlText),
        #[prost(message, tag = "124")]
        MysqlBlob(super::MysqlBlob),
        #[prost(message, tag = "125")]
        MysqlJson(super::MysqlJson),
        #[prost(message, tag = "126")]
        MysqlGeometry(super::MysqlGeometry),
        #[prost(message, tag = "201")]
        OracleVarchar2(super::OracleVarchar2),
        #[prost(message, tag = "202")]
        OracleChar(super::OracleChar),
        #[prost(message, tag = "203")]
        OracleNumber(super::OracleNumber),
        #[prost(message, tag = "204")]
        OracleDate(super::OracleDate),
        #[prost(message, tag = "205")]
        OracleTimestamp(super::OracleTimestamp),
        #[prost(message, tag = "206")]
        OracleBoolean(super::OracleBoolean),
        #[prost(message, tag = "207")]
        OracleBinaryFloat(super::OracleBinaryFloat),
        #[prost(message, tag = "208")]
        OracleBinaryDouble(super::OracleBinaryDouble),
        #[prost(message, tag = "209")]
        OracleBlob(super::OracleBlob),
        #[prost(message, tag = "210")]
        OracleFloat(super::OracleFloat),
        #[prost(message, tag = "211")]
        OracleNchar(super::OracleNChar),
        #[prost(message, tag = "212")]
        OracleNvarchar2(super::OracleNVarchar2),
        #[prost(message, tag = "213")]
        OracleRaw(super::OracleRaw),
        #[prost(message, tag = "214")]
        OracleLongRaw(super::OracleLongRaw),
        #[prost(message, tag = "215")]
        OracleLong(super::OracleLong),
        #[prost(message, tag = "216")]
        OracleClob(super::OracleClob),
        #[prost(message, tag = "217")]
        OracleNclob(super::OracleNClob),
        #[prost(message, tag = "301")]
        SqliteNull(super::SqliteNull),
        #[prost(message, tag = "302")]
        SqliteInteger(super::SqliteInteger),
        #[prost(message, tag = "303")]
        SqliteReal(super::SqliteReal),
        #[prost(message, tag = "304")]
        SqliteText(super::SqliteText),
        #[prost(message, tag = "305")]
        SqliteBlob(super::SqliteBlob),
        #[prost(message, tag = "401")]
        DmTinyInt(super::DmTinyInt),
        #[prost(message, tag = "402")]
        DmSmallInt(super::DmSmallInt),
        #[prost(message, tag = "403")]
        DmInteger(super::DmInteger),
        #[prost(message, tag = "404")]
        DmBigInt(super::DmBigInt),
        #[prost(message, tag = "405")]
        DmReal(super::DmReal),
        #[prost(message, tag = "406")]
        DmDouble(super::DmDouble),
        #[prost(message, tag = "407")]
        DmNumeric(super::DmNumeric),
        #[prost(message, tag = "408")]
        DmDecimal(super::DmDecimal),
        #[prost(message, tag = "409")]
        DmChar(super::DmChar),
        #[prost(message, tag = "410")]
        DmVarchar(super::DmVarchar),
        #[prost(message, tag = "411")]
        DmText(super::DmText),
        #[prost(message, tag = "412")]
        DmBinary(super::DmBinary),
        #[prost(message, tag = "413")]
        DmVarbinary(super::DmVarbinary),
        #[prost(message, tag = "414")]
        DmImage(super::DmImage),
        #[prost(message, tag = "415")]
        DmBit(super::DmBit),
        #[prost(message, tag = "416")]
        DmTimestamp(super::DmTimestamp),
        #[prost(message, tag = "417")]
        DmTime(super::DmTime),
        #[prost(message, tag = "418")]
        DmDate(super::DmDate),
    }
}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresInt2 {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresInt4 {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresInt8 {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresFloat4 {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresFloat8 {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresNumeric {
    #[prost(int32, tag = "1")]
    pub scale: i32,
}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresName {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresVarchar {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresBpchar {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresText {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresBytea {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresDate {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresTime {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresTimestamp {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresTimestampTz {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresInterval {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresBool {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresJson {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresJsonb {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresInt2Array {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresInt4Array {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresInt8Array {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresFloat4Array {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresFloat8Array {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresVarcharArray {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresBpcharArray {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresTextArray {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresByteaArray {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresBoolArray {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresPostGisGeometry {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PostgresOid {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct MysqlTinyInt {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct MysqlTinyIntUnsigned {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct MysqlSmallInt {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct MysqlSmallIntUnsigned {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct MysqlMediumInt {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct MysqlMediumIntUnsigned {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct MysqlInteger {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct MysqlIntegerUnsigned {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct MysqlBigInt {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct MysqlBigIntUnsigned {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct MysqlFloat {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct MysqlDouble {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct MysqlDecimal {
    #[prost(uint32, tag = "1")]
    pub precision: u32,
    #[prost(uint32, tag = "2")]
    pub scale: u32,
}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct MysqlDate {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct MysqlDateTime {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct MysqlTime {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct MysqlTimestamp {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct MysqlYear {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct MysqlChar {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct MysqlVarchar {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct MysqlBinary {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct MysqlVarbinary {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct MysqlText {
    #[prost(uint32, tag = "1")]
    pub length: u32,
}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct MysqlBlob {
    #[prost(uint32, tag = "1")]
    pub length: u32,
}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct MysqlJson {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct MysqlGeometry {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct OracleVarchar2 {
    #[prost(uint32, tag = "1")]
    pub length: u32,
}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct OracleChar {
    #[prost(uint32, tag = "1")]
    pub length: u32,
}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct OracleNumber {
    #[prost(uint32, tag = "1")]
    pub precision: u32,
    #[prost(int32, tag = "2")]
    pub scale: i32,
}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct OracleDate {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct OracleTimestamp {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct OracleBoolean {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct OracleBinaryFloat {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct OracleBinaryDouble {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct OracleBlob {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct OracleFloat {
    #[prost(uint32, tag = "1")]
    pub precision: u32,
}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct OracleNChar {
    #[prost(uint32, tag = "1")]
    pub length: u32,
}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct OracleNVarchar2 {
    #[prost(uint32, tag = "1")]
    pub length: u32,
}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct OracleRaw {
    #[prost(uint32, tag = "1")]
    pub length: u32,
}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct OracleLongRaw {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct OracleLong {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct OracleClob {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct OracleNClob {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct SqliteNull {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct SqliteInteger {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct SqliteReal {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct SqliteText {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct SqliteBlob {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct DmTinyInt {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct DmSmallInt {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct DmInteger {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct DmBigInt {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct DmReal {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct DmDouble {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct DmNumeric {
    #[prost(uint32, tag = "1")]
    pub precision: u32,
    #[prost(int32, tag = "2")]
    pub scale: i32,
}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct DmDecimal {
    #[prost(uint32, tag = "1")]
    pub precision: u32,
    #[prost(int32, tag = "2")]
    pub scale: i32,
}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct DmChar {
    #[prost(uint32, optional, tag = "1")]
    pub length: ::core::option::Option<u32>,
}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct DmVarchar {
    #[prost(uint32, optional, tag = "1")]
    pub length: ::core::option::Option<u32>,
}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct DmText {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct DmBinary {
    #[prost(uint32, tag = "1")]
    pub length: u32,
}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct DmVarbinary {
    #[prost(uint32, optional, tag = "1")]
    pub length: ::core::option::Option<u32>,
}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct DmImage {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct DmBit {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct DmTimestamp {
    #[prost(uint32, tag = "1")]
    pub precision: u32,
}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct DmTime {
    #[prost(uint32, tag = "1")]
    pub precision: u32,
}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct DmDate {}
