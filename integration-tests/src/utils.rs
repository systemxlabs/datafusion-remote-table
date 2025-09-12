use crate::setup_sqlite_db;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_remote_table::{
    ConnectionOptions, MysqlConnectionOptions, OracleConnectionOptions, PostgresConnectionOptions,
    RemoteDbType, RemoteSource, RemoteTable, SqliteConnectionOptions,
};
use std::sync::Arc;

pub async fn assert_result(
    database: RemoteDbType,
    source: impl Into<RemoteSource>,
    df_sql: &str,
    expected_result: &str,
) {
    let options = build_conn_options(database);
    let table = RemoteTable::try_new(options, source.into()).await.unwrap();
    println!("remote schema: {:#?}", table.remote_schema());

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table)).unwrap();

    let df = ctx.sql(df_sql).await.unwrap();
    let exec_plan = df.create_physical_plan().await.unwrap();
    println!(
        "{}",
        DisplayableExecutionPlan::new(exec_plan.as_ref()).indent(true)
    );

    let result = collect(exec_plan, ctx.task_ctx()).await.unwrap();
    println!("{}", pretty_format_batches(&result).unwrap());

    assert_eq!(
        pretty_format_batches(&result).unwrap().to_string(),
        expected_result
    );
}

pub async fn assert_plan_and_result(
    database: RemoteDbType,
    source: impl Into<RemoteSource>,
    df_sql: &str,
    expected_plans: Vec<&str>,
    expected_result: &str,
) {
    let options = build_conn_options(database);
    let table = RemoteTable::try_new(options, source).await.unwrap();
    println!("remote schema: {:#?}", table.remote_schema());

    let config = SessionConfig::new().with_target_partitions(12);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_table("remote_table", Arc::new(table)).unwrap();

    let df = ctx.sql(df_sql).await.unwrap();
    let exec_plan = df.create_physical_plan().await.unwrap();
    let exec_plan_str = DisplayableExecutionPlan::new(exec_plan.as_ref())
        .indent(true)
        .to_string();
    println!("{exec_plan_str}");
    assert!(expected_plans.contains(&exec_plan_str.as_str()));

    let result = collect(exec_plan, ctx.task_ctx()).await.unwrap();
    println!("{}", pretty_format_batches(&result).unwrap());

    assert_eq!(
        pretty_format_batches(&result).unwrap().to_string(),
        expected_result
    );
}

pub async fn assert_sqls(database: RemoteDbType, remote_sources: Vec<RemoteSource>) {
    let options = build_conn_options(database);

    for source in remote_sources.into_iter() {
        println!("Testing source: {source}");

        let table = RemoteTable::try_new(options.clone(), source.clone())
            .await
            .unwrap();
        println!("remote schema: {:#?}", table.remote_schema());

        let ctx = SessionContext::new();
        ctx.register_table("remote_table", Arc::new(table)).unwrap();
        ctx.sql("select * from remote_table")
            .await
            .unwrap()
            .show()
            .await
            .unwrap();
    }
}

pub async fn wait_container_ready(database: RemoteDbType) {
    let conn = build_conn_options(database);

    let mut retry = 0;
    loop {
        if let Ok(_table) = RemoteTable::try_new(conn.clone(), "select 1").await {
            break;
        };
        retry += 1;
        if retry > 20 {
            panic!("container still not ready after 200 seconds");
        }
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    }
}

pub fn build_conn_options(database: RemoteDbType) -> ConnectionOptions {
    match database {
        RemoteDbType::Mysql => ConnectionOptions::Mysql(
            MysqlConnectionOptions::new("127.0.0.1", 3306, "root", "password")
                .with_database(Some("test".to_string())),
        ),
        RemoteDbType::Postgres => ConnectionOptions::Postgres(
            PostgresConnectionOptions::new("localhost", 5432, "postgres", "password")
                .with_database(Some("postgres".to_string())),
        ),
        RemoteDbType::Oracle => ConnectionOptions::Oracle(OracleConnectionOptions::new(
            "127.0.0.1",
            49161,
            "system",
            "oracle",
            "free",
        )),
        RemoteDbType::Sqlite => {
            let db_path = setup_sqlite_db();
            ConnectionOptions::Sqlite(SqliteConnectionOptions::new(db_path.clone()))
        }
        RemoteDbType::Dm => todo!(),
    }
}
