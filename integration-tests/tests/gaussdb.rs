use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::SessionContext;
use datafusion_remote_table::{RemoteDbType, RemoteTable};
use integration_tests::setup_gaussdb_db;
use integration_tests::utils::build_conn_options;
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
