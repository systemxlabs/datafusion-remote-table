use std::{sync::Arc, time::Instant};

use datafusion::arrow::{array::*, datatypes::SchemaRef};
use datafusion_remote_table::{
    DefaultLiteralizer, PostgresConnection, PostgresType, RemoteDbType, RemoteField, RemoteSchema,
    RemoteSchemaRef, RemoteType, connect,
};
use futures::StreamExt;
use integration_tests::{setup_postgres_db, utils::build_conn_options};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_postgres_db().await;

    let options = build_conn_options(RemoteDbType::Postgres);
    let pool = connect(&options).await?;
    let conn = pool.get().await?;

    let table_name = "pg_bench";
    let pg_conn = conn.as_any().downcast_ref::<PostgresConnection>().unwrap();
    pg_conn
        .conn
        .batch_execute(&create_table_sql(table_name))
        .await?;

    let insert_batch_size = 1000;

    let mut insert_count = 0;
    let literalizer = Arc::new(DefaultLiteralizer {});
    let start = Instant::now();
    for _ in 0..1000 {
        let batch = make_record_batch(insert_batch_size);
        let count = conn
            .insert(
                &options,
                literalizer.clone(),
                &[table_name.to_string()],
                remote_schema(),
                batch,
            )
            .await?;
        insert_count += count;
    }
    println!(
        "Postgres: insert {insert_count} rows cost {}ms",
        start.elapsed().as_millis()
    );

    let start = Instant::now();
    let mut stream = conn
        .query(
            &options,
            &format!("select * from {table_name}").into(),
            table_schema(),
            None,
            &[],
            None,
        )
        .await?;
    let mut query_count = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        query_count += batch.num_rows();
    }
    assert_eq!(query_count, insert_count);

    println!(
        "Postgres: query {query_count} rows cost {}ms",
        start.elapsed().as_millis()
    );

    Ok(())
}

fn create_table_sql(table_name: &str) -> String {
    format!(
        "CREATE TABLE {table_name} (int_col INT, float8_col FLOAT8, bytea_col BYTEA, text_col TEXT, json_col JSON, timestamp_col TIMESTAMP)"
    )
}

fn table_schema() -> SchemaRef {
    let schema = remote_schema().to_arrow_schema();
    Arc::new(schema)
}

fn remote_schema() -> RemoteSchemaRef {
    let schema = RemoteSchema::new(vec![
        RemoteField::new("int_col", RemoteType::Postgres(PostgresType::Int4), true),
        RemoteField::new(
            "float8_col",
            RemoteType::Postgres(PostgresType::Float8),
            true,
        ),
        RemoteField::new("bytea_col", RemoteType::Postgres(PostgresType::Bytea), true),
        RemoteField::new("text_col", RemoteType::Postgres(PostgresType::Text), true),
        RemoteField::new("json_col", RemoteType::Postgres(PostgresType::Json), true),
        RemoteField::new(
            "timestamp_col",
            RemoteType::Postgres(PostgresType::Timestamp),
            true,
        ),
    ]);
    Arc::new(schema)
}

fn make_record_batch(size: usize) -> RecordBatch {
    let int_col = Int32Array::from_iter_values(0..size as i32);
    let float8_col = Float64Array::from_iter_values(vec![1.23456789; size]);
    let bytea_col = BinaryArray::from_iter_values(vec![
        "this is loooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooog bytes".as_bytes();
        size
    ]);

    let text_col = StringArray::from_iter_values(vec![
        "This is a loooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooog text";
        size
    ]);
    let json_col = LargeStringArray::from_iter_values(vec![
        r#"{"key": "value", "key2": [1, 2, 3], "key3": {"nested": "value"}}"#;
        size
    ]);
    let timestamp_col = TimestampMicrosecondArray::from_iter_values(vec![1764124713000000; size]);

    RecordBatch::try_new(
        table_schema(),
        vec![
            Arc::new(int_col),
            Arc::new(float8_col),
            Arc::new(bytea_col),
            Arc::new(text_col),
            Arc::new(json_col),
            Arc::new(timestamp_col),
        ],
    )
    .unwrap()
}
