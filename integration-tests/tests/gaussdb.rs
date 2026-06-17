use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::SessionContext;
use datafusion_remote_table::{RemoteDbType, RemoteSource, RemoteTable};
use integration_tests::setup_gaussdb_db;
use integration_tests::utils::{assert_plan_and_result, assert_sqls, build_conn_options};
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread")]
pub async fn gaussdb_simple_query() {
    setup_gaussdb_db().await;

    let options = build_conn_options(RemoteDbType::GaussDB);
    let table = RemoteTable::try_new(options, "select 1 as id")
        .await
        .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table)).unwrap();

    let df = ctx.sql("SELECT * FROM remote_table").await.unwrap();
    let batches = df.collect().await.unwrap();
    let table_str = pretty_format_batches(&batches).unwrap().to_string();
    println!("{table_str}");

    assert_eq!(
        table_str,
        r#"+----+
| id |
+----+
| 1  |
+----+"#,
    );
}

#[tokio::test(flavor = "multi_thread")]
pub async fn supported_gaussdb_types() {
    setup_gaussdb_db().await;

    assert_sqls(
        RemoteDbType::GaussDB,
        vec![
            RemoteSource::Table(vec![
                "public".to_string(),
                "supported_data_types".to_string(),
            ]),
            RemoteSource::Query("SELECT * FROM supported_data_types".to_string()),
        ],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
pub async fn supported_gaussdb_timestamp_type() {
    setup_gaussdb_db().await;

    assert_sqls(
        RemoteDbType::GaussDB,
        vec![RemoteSource::Table(vec![
            "public".to_string(),
            "timestamp_test".to_string(),
        ])],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
pub async fn pushdown_limit() {
    setup_gaussdb_db().await;

    assert_plan_and_result(
        RemoteDbType::GaussDB,
        RemoteSource::Table(vec!["public".to_string(), "simple_table".to_string()]),
        "SELECT id FROM remote_table ORDER BY id LIMIT 2",
        vec!["RemoteTableScanExec: projection=[id]"],
        r#"+----+
| id |
+----+
| 1  |
| 2  |
+----+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
pub async fn pushdown_filters() {
    setup_gaussdb_db().await;

    assert_plan_and_result(
        RemoteDbType::GaussDB,
        RemoteSource::Table(vec!["public".to_string(), "simple_table".to_string()]),
        "SELECT id, name FROM remote_table WHERE id > 1",
        vec!["RemoteTableScanExec: projection=[id, name]"],
        r#"+----+-------+
| id | name  |
+----+-------+
| 2  | Jerry |
| 3  | Spike |
+----+-------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
pub async fn table_projection() {
    setup_gaussdb_db().await;

    assert_plan_and_result(
        RemoteDbType::GaussDB,
        RemoteSource::Table(vec!["public".to_string(), "simple_table".to_string()]),
        "SELECT name FROM remote_table",
        vec!["RemoteTableScanExec: projection=[name]"],
        r#"+-------+
| name  |
+-------+
| Tom   |
| Jerry |
| Spike |
+-------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
pub async fn empty_projection() {
    setup_gaussdb_db().await;

    let options = build_conn_options(RemoteDbType::GaussDB);
    let table = RemoteTable::try_new(
        options,
        RemoteSource::Table(vec!["public".to_string(), "simple_table".to_string()]),
    )
    .await
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table)).unwrap();

    let result = ctx
        .sql("SELECT count(1) FROM remote_table")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let expected = r#"+----------+
| count(1) |
+----------+
| 3        |
+----------+"#;
    assert_eq!(
        pretty_format_batches(&result).unwrap().to_string(),
        expected
    );
}
