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
  - [x] `_id` (typed from the stored key, including ObjectId)
  - [x] Whole document as a Parquet Variant

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

A MongoDB collection is schemaless and can be arbitrarily nested, so it is
exposed as exactly two columns:

| Column | Type | Contents |
|---|---|---|
| `_id` | the Arrow type of the stored key | the document key; `ObjectId` becomes its 24 character hex string |
| `document` | a Parquet [Variant](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md) | the whole document, self-describing |

Only `_id` needs looking at (a sample of `sample_size` documents, 100 by
default) and the rest of the document is stored as a Variant, which is
per-value typed and nested natively. Compared with inferring one column per
field, this keeps the collection faithful — nested documents and arrays are not
flattened, types are not widened and no value is turned into text — and the
schema is stable no matter what the documents contain or how the collection
evolves. A collection is queryable even when it is empty, in which case `_id`
reports MongoDB's default key type (`ObjectId`).

The Variant column is an Arrow [canonical extension type][ext]: its Arrow type
is `Struct<metadata: BinaryView, value: BinaryView>` carrying the
`arrow.parquet.variant` extension name. Everything works at the Arrow level
today, but DataFusion 55 has no SQL functions for Variant yet, so reaching into
a document from SQL needs a UDF or a custom `Transform`; the
`parquet-variant-compute` crate provides the `variant_get` and
`variant_to_json` kernels to build on.

[ext]: https://arrow.apache.org/docs/format/CanonicalExtensions.html#parquet-variant

Every BSON type Variant can express is stored natively: numbers keep their
width (`int32` stays `Int32`, `int64` stays `Int64`), dates become UTC
timestamps, binary values are stored as bytes. The MongoDB specific types —
ObjectId, regular expressions, the internal BSON timestamp, min/max keys,
JavaScript, symbols and `decimal128` — are stored as their canonical extended
JSON object (for example `{"$oid": "..."}`), which is also how they are read
back. Two things do not survive, because Variant cannot express them:

- the subtype of a binary value (a UUID is stored as plain bytes);
- BSON field order, since a Variant object's fields are stored sorted by name.

Inserts take the `document` column as it is; a non-null `_id` overrides the key
inside the document, and a null `_id` leaves it to the server to generate.

Filters are not pushed down — a SQL predicate cannot be unparsed into a BSON
filter — so DataFusion evaluates them locally, and predicates can only reference
`_id` because the document itself is opaque to SQL. Limit pushdown and `count()`
(via `count_documents`) are supported.

## Thanks
- [datafusion-table-providers](https://crates.io/crates/datafusion-table-providers)
