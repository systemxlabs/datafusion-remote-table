# datafusion-remote-table
![License](https://img.shields.io/badge/license-MIT-blue.svg)
[![Crates.io](https://img.shields.io/crates/v/datafusion-remote-table.svg)](https://crates.io/crates/datafusion-remote-table)
[![Docs](https://docs.rs/datafusion-remote-table/badge.svg)](https://docs.rs/datafusion-remote-table/latest/datafusion_remote_table/)

## Features
1. Execute SQL queries on remote databases and stream results as datafusion table provider
2. Insert data into remote databases
3. Support inferring schema or user specified schema
4. Support pushing down filters and limit to remote databases
5. Execution plan can be serialized for distributed execution
6. Record batches can be transformed before outputting to next plan node

## Usage
1. Execute SQL queries on remote database
```rust
#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = PostgresConnectionOptions::new("localhost", 5432, "user", "password");
    let remote_table = RemoteTable::try_new(options, "select * from supported_data_types").await?;

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(remote_table))?;

    ctx.sql("select * from remote_table").await?.show().await?;

    Ok(())
}
```

2. Insert data into remote database
```rust
#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = PostgresConnectionOptions::new("localhost", 5432, "user", "password");
    let remote_table = RemoteTable::try_new(options, vec!["public", "test_table"]).await?;

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(remote_table))?;

    ctx.sql("insert into remote_table values (1, 'Tom')").await?.show().await?;

    Ok(())
}
```

## Supported databases
- [x] Postgres
  - [x] Int2 / Int4 / Int8
  - [x] Float4 / Float8 / Numeric
  - [x] Char / Varchar / Text / Bpchar / Bytea
  - [x] Date / Time / Timestamp / Timestamptz / Interval
  - [x] Bool / Oid / Name / Json / Jsonb / Geometry(PostGIS) / Xml / Uuid
  - [x] Int2[] / Int4[] / Int8[]
  - [x] Float4[] / Float8[]
  - [x] Char[] / Varchar[] / Bpchar[] / Text[] / Bytea[]
- [x] MySQL
  - [x] TinyInt (Unsigned) / Smallint (Unsigned) / MediumInt (Unsigned) / Int (Unsigned) / Bigint (Unsigned)
  - [x] Float / Double / Decimal
  - [x] Date / DateTime / Time / Timestamp / Year
  - [x] Char / Varchar / Binary / Varbinary
  - [x] TinyText / Text / MediumText / LongText
  - [x] TinyBlob / Blob / MediumBlob / LongBlob
  - [x] Json / Geometry
- [x] Oracle
  - [x] Number / BinaryFloat / BinaryDouble / Float
  - [x] Varchar2 / NVarchar2 / Char / NChar / Long / Clob / NClob
  - [x] Raw / Long Raw / Blob
  - [x] Date / Timestamp
  - [x] Boolean / SDE.ST_GEOMETRY
- [x] SQLite
  - [x] Null / Integer / Real / Text / Blob
- [x] DM (达梦数据库)
  - [x] TinyInt / Smallint / Int / Bigint
  - [x] Real / Float / Double / Numeric / Decimal
  - [x] Char / Varchar / Text
  - [x] Binary / Varbinary / Image
  - [x] Bit / Timestamp / Time / Date
- [x] GaussDB / OpenGauss
  - [x] Int2 / Int4 / Int8
  - [x] Float4 / Float8 / Numeric
  - [x] Char / Varchar / Text / Bpchar / Bytea
  - [x] Date / Time / Timestamp / Timestamptz / Interval
  - [x] Bool / Oid / Name / Json / Jsonb / Xml / Uuid
  - [x] Int2[] / Int4[] / Int8[]
  - [x] Float4[] / Float8[]
  - [x] Varchar[] / Text[] / Bool[]
- [x] MDB (Microsoft Access `.mdb`, Jet engine, via the MDBTools ODBC driver)
  - [x] Byte / Small Integer / Long Integer / Bit
  - [x] Real / Double / Currency
  - [x] Text / Memo / Binary / OLE / Guid
  - [x] Date / Time / DateTime
- [x] Access (Microsoft Access `.accdb`, ACE engine, via the MDBTools ODBC driver)
  - [x] Byte / Small Integer / Long Integer / Bit
  - [x] Real / Double / Currency
  - [x] Text / Memo / Binary / OLE / Guid
  - [x] Date / Time / DateTime
  - [x] Own type enum, options, pool and connection handling; tables are listed through the `MSysObjects` catalog table
- [x] MongoDB
  - [x] Double / Int32 / Int64
  - [x] String / Boolean
  - [x] Date / ObjectId / Binary
  - [x] Embedded document / Array (as JSON text)
  - [x] Decimal128 / BSON timestamp / Regular expression (as text)
  - [x] Null

## MongoDB

MongoDB has no SQL dialect and no query string, so only whole collections are
supported: a `RemoteSource` must be a `RemoteSource::Table`. The collection is
resolved against `MongoDBConnectionOptions::database`, or use
`[database, collection]` to override it per table.

```rust
let options = MongoDBConnectionOptions::new("mongodb://localhost:27017", "test");

let remote_table = RemoteTable::try_new(options, vec!["restaurants"]).await?;
// Or an explicit database:
let remote_table = RemoteTable::try_new(options, vec!["test", "restaurants"]).await?;
```

MongoDB collections are schemaless: the schema is inferred from a sample of
documents (`sample_size`, 100 by default) and is the union of their fields.
Filters are not pushed down — a SQL predicate cannot be unparsed into a BSON
filter — so DataFusion evaluates them locally. Limit pushdown, `count()`
(via `count_documents`) and inserts are supported.

## Thanks
- [datafusion-table-providers](https://crates.io/crates/datafusion-table-providers)
