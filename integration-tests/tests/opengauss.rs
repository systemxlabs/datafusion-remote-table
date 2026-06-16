#[cfg(feature = "opengauss")]
use datafusion::arrow::util::pretty::pretty_format_batches;
#[cfg(feature = "opengauss")]
use datafusion::prelude::SessionContext;
#[cfg(feature = "opengauss")]
use datafusion_remote_table::{ConnectionOptions, RemoteDbType, RemoteTable};
#[cfg(feature = "opengauss")]
use integration_tests::setup_opengauss_db;
#[cfg(feature = "opengauss")]
use integration_tests::utils::build_conn_options;
#[cfg(feature = "opengauss")]
use std::sync::Arc;

#[cfg(feature = "opengauss")]
#[tokio::test(flavor = "multi_thread")]
pub async fn opengauss_simple_query() {
    setup_opengauss_db().await;

    let options = build_conn_options(RemoteDbType::OpenGauss);
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
