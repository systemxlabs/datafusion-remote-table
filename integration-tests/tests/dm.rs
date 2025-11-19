use datafusion::physical_plan::collect;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_remote_table::{RemoteDbType, RemoteSource, RemoteTable, connect};
use integration_tests::setup_dm_db;
use integration_tests::utils::{assert_plan_and_result, assert_result, build_conn_options};
use std::sync::Arc;

#[rstest::rstest]
#[case("SELECT * from \"supported_data_types\"".into())]
#[case(vec!["supported_data_types"].into())]
#[tokio::test(flavor = "multi_thread")]
pub async fn supported_dm_types(#[case] source: RemoteSource) {
    setup_dm_db().await;
    assert_result(RemoteDbType::Dm, source, "SELECT * FROM remote_table", 
        r#"+---------+-------------+--------------+-------------+------------+----------+-----------+------------+-------------+-------------+------------+-------------+----------+------------+---------------+------------+---------------------+--------------+------------+
| BIT_COL | TINYINT_COL | SMALLINT_COL | INTEGER_COL | BIGINT_COL | REAL_COL | FLOAT_COL | DOUBLE_COL | NUMERIC_COL | DECIMAL_COL | CHAR_COL   | VARCHAR_COL | TEXT_COL | BINARY_COL | VARBINARY_COL | IMAGE_COL  | TIMESTAMP_COL       | TIME_COL     | DATE_COL   |
+---------+-------------+--------------+-------------+------------+----------+-----------+------------+-------------+-------------+------------+-------------+----------+------------+---------------+------------+---------------------+--------------+------------+
| true    | 1           | 2            | 3           | 4          | 1.1      | 2.2       | 3.3        | 4.40        | 5.50        | char       | varchar     | text     | 01         | 00000002      | 696d616765 | 2002-12-12T09:10:21 | 09:10:21.200 | 2023-10-01 |
|         |             |              |             |            |          |           |            |             |             |            |             |          |            |               |            |                     |              |            |
+---------+-------------+--------------+-------------+------------+----------+-----------+------------+-------------+-------------+------------+-------------+----------+------------+---------------+------------+---------------------+--------------+------------+"#).await;
}

#[rstest::rstest]
#[case("SELECT * from \"simple_table\"".into())]
#[case(vec!["simple_table"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn pushdown_limit(#[case] source: RemoteSource) {
    setup_dm_db().await;
    assert_plan_and_result(
        RemoteDbType::Dm,
        source,
        "select * from remote_table limit 1",
        vec![
            "CooperativeExec\n  RemoteTableExec: source=query, limit=1\n",
            "CooperativeExec\n  RemoteTableExec: source=simple_table, limit=1\n",
        ],
        r#"+----+------+
| ID | NAME |
+----+------+
| 1  | Tom  |
+----+------+"#,
    )
    .await;
}

#[rstest::rstest]
#[case("SELECT * from \"simple_table\"".into())]
#[case(vec!["simple_table"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn pushdown_filters(#[case] source: RemoteSource) {
    setup_dm_db().await;
    assert_plan_and_result(
        RemoteDbType::Dm,
        source,
        r#"select * from remote_table where "ID" = 1"#,
        vec![
            r#"CoalesceBatchesExec: target_batch_size=8192
  FilterExec: ID@0 = 1
    CooperativeExec
      RemoteTableExec: source=query
"#,
            r#"CoalesceBatchesExec: target_batch_size=8192
  FilterExec: ID@0 = 1
    CooperativeExec
      RemoteTableExec: source=simple_table
"#,
        ],
        r#"+----+------+
| ID | NAME |
+----+------+
| 1  | Tom  |
+----+------+"#,
    )
    .await;
}

#[rstest::rstest]
#[case("SELECT * from \"simple_table\"".into())]
#[case(vec!["simple_table"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn empty_projection(#[case] source: RemoteSource) {
    setup_dm_db().await;

    let options = build_conn_options(RemoteDbType::Dm);
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

#[rstest::rstest]
#[case("SELECT * from \"empty_table\"".into())]
#[case(vec!["empty_table"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn empty_table(#[case] source: RemoteSource) {
    setup_dm_db().await;

    assert_result(
        RemoteDbType::Dm,
        source,
        "SELECT * FROM remote_table",
        "++\n++",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
pub async fn pool_state() {
    setup_dm_db().await;

    let options = build_conn_options(RemoteDbType::Dm);
    let pool = connect(&options).await.unwrap();

    let conn = pool.get().await.unwrap();
    assert_eq!(pool.state().await.unwrap().connections, 1);
    drop(conn);
    assert_eq!(pool.state().await.unwrap().connections, 0);
}
