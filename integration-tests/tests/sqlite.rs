use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_remote_table::{
    ConnectionOptions, RemoteDbType, RemoteField, RemoteSchema, RemoteSource, RemoteTable,
    RemoteType, SqliteConnectionOptions, SqliteType, connect,
};
use integration_tests::setup_sqlite_db;
use integration_tests::utils::{assert_plan_and_result, assert_result, build_conn_options};
use std::sync::Arc;

#[rstest::rstest]
#[case("SELECT * from supported_data_types".into())]
#[case(vec!["supported_data_types"].into())]
#[tokio::test(flavor = "multi_thread")]
pub async fn supported_sqlite_types(#[case] source: RemoteSource) {
    assert_result(
        RemoteDbType::Sqlite,
        source,
        "select * from remote_table",
        r#"+-------------+--------------+---------+------------+----------+----------+----------+-----------+------------+----------+--------------------+--------------------------+-------------+-----------------------+-----------------------------+----------+--------------+-------------+-----------------+----------+--------------+------------+----------------+---------------+-------------------+----------+
| tinyint_col | smallint_col | int_col | bigint_col | int2_col | int4_col | int8_col | float_col | double_col | real_col | real_precision_col | real_precision_scale_col | numeric_col | numeric_precision_col | numeric_precision_scale_col | char_col | char_len_col | varchar_col | varchar_len_col | text_col | text_len_col | binary_col | binary_len_col | varbinary_col | varbinary_len_col | blob_col |
+-------------+--------------+---------+------------+----------+----------+----------+-----------+------------+----------+--------------------+--------------------------+-------------+-----------------------+-----------------------------+----------+--------------+-------------+-----------------+----------+--------------+------------+----------------+---------------+-------------------+----------+
| 1           | 2            | 3       | 4          | 5        | 6        | 7        | 1.1       | 2.2        | 3.3      | 4.4                | 5.5                      | 6.6         | 7.7                   | 8.8                         | char     | char(10)     | varchar     | varchar(120)    | text     | text(200)    | 01         | 02             | 03            | 04                | 05       |
|             |              |         |            |          |          |          |           |            |          |                    |                          |             |                       |                             |          |              |             |                 |          |              |            |                |               |                   |          |
+-------------+--------------+---------+------------+----------+----------+----------+-----------+------------+----------+--------------------+--------------------------+-------------+-----------------------+-----------------------------+----------+--------------+-------------+-----------------+----------+--------------+------------+----------------+---------------+-------------------+----------+"#,
    )
    .await;

    assert_result(
        RemoteDbType::Sqlite,
        "select count(1) from supported_data_types",
        "select * from remote_table",
        r#"+----------+
| count(1) |
+----------+
| 2        |
+----------+"#,
    )
    .await;
}

#[rstest::rstest]
#[case("SELECT * from simple_table".into())]
#[case(vec!["simple_table"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn streaming_execution(#[case] source: RemoteSource) {
    let db_path = setup_sqlite_db();
    let options = ConnectionOptions::Sqlite(
        SqliteConnectionOptions::new(db_path.clone()).with_stream_chunk_size(1usize),
    );
    let table = RemoteTable::try_new(options, source).await.unwrap();
    println!("remote schema: {:#?}", table.remote_schema());

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table)).unwrap();

    let df = ctx.sql("select * from remote_table").await.unwrap();
    let exec_plan = df.create_physical_plan().await.unwrap();
    println!(
        "{}",
        DisplayableExecutionPlan::new(exec_plan.as_ref()).indent(true)
    );

    let result = collect(exec_plan, ctx.task_ctx()).await.unwrap();
    println!("{}", pretty_format_batches(&result).unwrap());

    assert_eq!(
        pretty_format_batches(&result).unwrap().to_string(),
        r#"+----+-------+
| id | name  |
+----+-------+
| 1  | Tom   |
| 2  | Jerry |
| 3  | Spike |
+----+-------+"#,
    );
}

#[rstest::rstest]
#[case("SELECT * from simple_table".into())]
#[case(vec!["simple_table"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn pushdown_limit(#[case] source: RemoteSource) {
    assert_plan_and_result(
        RemoteDbType::Sqlite,
        source,
        "select * from remote_table limit 1",
        vec![
            "CooperativeExec\n  RemoteTableExec: source=query, limit=1\n",
            "CooperativeExec\n  RemoteTableExec: source=simple_table, limit=1\n",
        ],
        r#"+----+------+
| id | name |
+----+------+
| 1  | Tom  |
+----+------+"#,
    )
    .await;
}

#[rstest::rstest]
#[case("SELECT * from simple_table".into())]
#[case(vec!["simple_table"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn pushdown_filters(#[case] source: RemoteSource) {
    assert_plan_and_result(
        RemoteDbType::Sqlite,
        source,
        "select * from remote_table where id = 1",
        vec![
            "CooperativeExec\n  RemoteTableExec: source=query, filters=[(`id` = 1)]\n",
            "CooperativeExec\n  RemoteTableExec: source=simple_table, filters=[(`id` = 1)]\n",
        ],
        r#"+----+------+
| id | name |
+----+------+
| 1  | Tom  |
+----+------+"#,
    )
    .await;
}

#[rstest::rstest]
#[case("SELECT * from simple_table".into())]
#[case(vec!["simple_table"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn count1_agg(#[case] source: RemoteSource) {
    assert_plan_and_result(
        RemoteDbType::Sqlite,
        source.clone(),
        "select count(*) from remote_table",
        vec!["ProjectionExec: expr=[3 as count(*)]\n  PlaceholderRowExec\n"],
        r#"+----------+
| count(*) |
+----------+
| 3        |
+----------+"#,
    )
    .await;

    assert_plan_and_result(
        RemoteDbType::Sqlite,
        source.clone(),
        "select count(*) from remote_table where id > 1",
        vec!["ProjectionExec: expr=[2 as count(*)]\n  PlaceholderRowExec\n"],
        r#"+----------+
| count(*) |
+----------+
| 2        |
+----------+"#,
    )
    .await;

    assert_plan_and_result(
        RemoteDbType::Sqlite,
        source.clone(),
        "select count(*) from (select * from remote_table where id > 1 limit 1)",
        vec!["ProjectionExec: expr=[1 as count(*)]\n  PlaceholderRowExec\n"],
        r#"+----------+
| count(*) |
+----------+
| 1        |
+----------+"#,
    )
    .await;
}

#[rstest::rstest]
#[case("SELECT * from simple_table".into())]
#[case(vec!["simple_table"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn empty_projection(#[case] source: RemoteSource) {
    let options = build_conn_options(RemoteDbType::Sqlite);
    let table = RemoteTable::try_new(options, source).await.unwrap();

    let config = SessionConfig::new().with_target_partitions(12);
    let ctx = SessionContext::new_with_config(config);

    let df = ctx.read_table(Arc::new(table)).unwrap();
    let df = df.select_columns(&[]).unwrap();

    let exec_plan = df.create_physical_plan().await.unwrap();
    let plan_display = DisplayableExecutionPlan::new(exec_plan.as_ref())
        .indent(true)
        .to_string();
    println!("{plan_display}");
    assert!(
        [
            "CooperativeExec\n  RemoteTableExec: source=query, projection=[]\n",
            "CooperativeExec\n  RemoteTableExec: source=simple_table, projection=[]\n"
        ]
        .contains(&plan_display.as_str())
    );

    let result = collect(exec_plan, ctx.task_ctx()).await.unwrap();
    assert_eq!(result.len(), 1);
    let batch = &result[0];
    assert_eq!(batch.num_columns(), 0);
    assert_eq!(batch.num_rows(), 3);
}

#[tokio::test(flavor = "multi_thread")]
pub async fn insert_supported_sqlite_types() {
    assert_result(
        RemoteDbType::Sqlite,
        vec!["insert_supported_data_types"],
        "insert into remote_table values
        (1, 2, 3, 4, 5, 6, 7, 1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 'char', 'char(10)', 'varchar', 'varchar(120)', 'text', 'text(200)', X'01', X'02', X'03', X'04', X'05'),
        (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
        r#"+-------+
| count |
+-------+
| 2     |
+-------+"#,
    )
    .await;

    assert_result(
        RemoteDbType::Sqlite,
        vec!["insert_supported_data_types"],
        "select * from remote_table",
        r#"+-------------+--------------+---------+------------+----------+----------+----------+-----------+------------+----------+--------------------+--------------------------+-------------+-----------------------+-----------------------------+----------+--------------+-------------+-----------------+----------+--------------+------------+----------------+---------------+-------------------+----------+
| tinyint_col | smallint_col | int_col | bigint_col | int2_col | int4_col | int8_col | float_col | double_col | real_col | real_precision_col | real_precision_scale_col | numeric_col | numeric_precision_col | numeric_precision_scale_col | char_col | char_len_col | varchar_col | varchar_len_col | text_col | text_len_col | binary_col | binary_len_col | varbinary_col | varbinary_len_col | blob_col |
+-------------+--------------+---------+------------+----------+----------+----------+-----------+------------+----------+--------------------+--------------------------+-------------+-----------------------+-----------------------------+----------+--------------+-------------+-----------------+----------+--------------+------------+----------------+---------------+-------------------+----------+
| 1           | 2            | 3       | 4          | 5        | 6        | 7        | 1.1       | 2.2        | 3.3      | 4.4                | 5.5                      | 6.6         | 7.7                   | 8.8                         | char     | char(10)     | varchar     | varchar(120)    | text     | text(200)    | 01         | 02             | 03            | 04                | 05       |
|             |              |         |            |          |          |          |           |            |          |                    |                          |             |                       |                             |          |              |             |                 |          |              |            |                |               |                   |          |
+-------------+--------------+---------+------------+----------+----------+----------+-----------+------------+----------+--------------------+--------------------------+-------------+-----------------------+-----------------------------+----------+--------------+-------------+-----------------+----------+--------------+------------+----------------+---------------+-------------------+----------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
pub async fn insert_table_with_primary_key() {
    let db_path = setup_sqlite_db();
    let options = ConnectionOptions::Sqlite(SqliteConnectionOptions::new(db_path.clone()));
    let remote_schema = Arc::new(RemoteSchema::new(vec![
        RemoteField::new("id", RemoteType::Sqlite(SqliteType::Integer), false)
            .with_auto_increment(true),
        RemoteField::new("name", RemoteType::Sqlite(SqliteType::Text), true),
    ]));
    let table = RemoteTable::try_new_with_remote_schema(
        options,
        vec!["insert_table_with_primary_key"],
        remote_schema,
    )
    .await
    .unwrap();
    println!("remote schema: {:#?}", table.remote_schema());

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table)).unwrap();

    let df = ctx
        .sql("insert into remote_table (name) values ('Tom')")
        .await
        .unwrap();
    let exec_plan = df.create_physical_plan().await.unwrap();
    println!(
        "{}",
        DisplayableExecutionPlan::new(exec_plan.as_ref()).indent(true)
    );

    let result = collect(exec_plan, ctx.task_ctx()).await.unwrap();
    println!("{}", pretty_format_batches(&result).unwrap());

    assert_eq!(
        pretty_format_batches(&result).unwrap().to_string(),
        r#"+-------+
| count |
+-------+
| 1     |
+-------+"#,
    );

    assert_result(
        RemoteDbType::Sqlite,
        vec!["insert_table_with_primary_key"],
        "SELECT * FROM remote_table",
        r#"+----+------+
| id | name |
+----+------+
| 1  | Tom  |
+----+------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
pub async fn pool_state() {
    let db_path = setup_sqlite_db();
    let options = ConnectionOptions::Sqlite(SqliteConnectionOptions::new(db_path.clone()));
    let pool = connect(&options).await.unwrap();

    let conn = pool.get().await.unwrap();
    assert_eq!(pool.state().await.unwrap().connections, 1);
    drop(conn);
    assert_eq!(pool.state().await.unwrap().connections, 0);
}
