use datafusion::physical_plan::collect;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_remote_table::{ConnectionOptions, RemoteDbType, RemoteSource, RemoteTable};
use integration_tests::setup_oracle_db;
use integration_tests::utils::{
    assert_plan_and_result, assert_result, assert_sqls, build_conn_options,
};
use std::sync::Arc;
use std::time::Duration;

#[rstest::rstest]
#[case("SELECT * from SYS.\"supported_data_types\"".into())]
#[case(vec!["SYS", "supported_data_types"].into())]
#[tokio::test(flavor = "multi_thread")]
pub async fn supported_oracle_types(#[case] source: RemoteSource) {
    setup_oracle_db().await;
    assert_result(
        RemoteDbType::Oracle,
        source,
        "SELECT * FROM remote_table",
        r#"+-------------+--------------+-------------+------------------+-------------------+------------+----------+-----------+--------------+---------------+------------+------------+----------+-----------+---------+------------------+----------+---------------------+----------------------------+
| BOOLEAN_COL | SMALLINT_COL | INTEGER_COL | BINARY_FLOAT_COL | BINARY_DOUBLE_COL | NUMBER_COL | REAL_COL | FLOAT_COL | VARCHAR2_COL | NVARCHAR2_COL | CHAR_COL   | NCHAR_COL  | CLOB_COL | NCLOB_COL | RAW_COL | LONG_RAW_COL     | BLOB_COL | DATE_COL            | TIMESTAMP_COL              |
+-------------+--------------+-------------+------------------+-------------------+------------+----------+-----------+--------------+---------------+------------+------------+----------+-----------+---------+------------------+----------+---------------------+----------------------------+
| true        | 1            | 2           | 1.1              | 2.2               | 3.30       | 4.4      | 5.5       | varchar2     | nvarchar2     | char       | nchar      | clob     | nclob     | 726177  | 6c6f6e6720726177 | 626c6f62 | 2003-05-03T21:02:44 | 2023-10-01T14:30:45.123456 |
|             |              |             |                  |                   |            |          |           |              |               |            |            |          |           |         |                  |          |                     |                            |
+-------------+--------------+-------------+------------------+-------------------+------------+----------+-----------+--------------+---------------+------------+------------+----------+-----------+---------+------------------+----------+---------------------+----------------------------+"#,
    ).await;
}

// ORA-01754: a table may contain only one column of type LONG
#[rstest::rstest]
#[case("SELECT * from SYS.\"supported_data_types2\"".into())]
#[case(vec!["SYS", "supported_data_types2"].into())]
#[tokio::test(flavor = "multi_thread")]
pub async fn supported_oracle_types2(#[case] source: RemoteSource) {
    setup_oracle_db().await;
    assert_result(
        RemoteDbType::Oracle,
        source,
        "SELECT * FROM remote_table",
        r#"+----------+
| LONG_COL |
+----------+
| long     |
|          |
+----------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
pub async fn various_sqls() {
    setup_oracle_db().await;

    assert_sqls(
        RemoteDbType::Oracle,
        vec![
            "select * from USER_TABLES".into(),
            vec!["USER_TABLES"].into(),
        ],
    )
    .await;
}

#[rstest::rstest]
#[case("SELECT * from SYS.\"simple_table\"".into())]
#[case(vec!["SYS", "simple_table"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn pushdown_limit(#[case] source: RemoteSource) {
    setup_oracle_db().await;
    assert_plan_and_result(
        RemoteDbType::Oracle,
        source,
        "select * from remote_table limit 1",
        vec![
            "CooperativeExec\n  RemoteTableExec: source=query, limit=1\n",
            "CooperativeExec\n  RemoteTableExec: source=SYS.simple_table, limit=1\n",
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
#[case("SELECT * from SYS.\"simple_table\"".into())]
#[case(vec!["SYS", "simple_table"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn pushdown_filters(#[case] source: RemoteSource) {
    setup_oracle_db().await;
    assert_plan_and_result(
        RemoteDbType::Oracle,
        source,
        r#"select * from remote_table where "ID" = 1"#,
        vec![
            r#"CoalesceBatchesExec: target_batch_size=8192
  FilterExec: ID@0 = Some(1),38,0
    CooperativeExec
      RemoteTableExec: source=query
"#,
            r#"CoalesceBatchesExec: target_batch_size=8192
  FilterExec: ID@0 = Some(1),38,0
    CooperativeExec
      RemoteTableExec: source=SYS.simple_table
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
#[case("SELECT * from SYS.\"simple_table\"".into())]
#[case(vec!["SYS", "simple_table"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn count1_agg(#[case] source: RemoteSource) {
    setup_oracle_db().await;

    assert_plan_and_result(
        RemoteDbType::Oracle,
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
        RemoteDbType::Oracle,
        source.clone(),
        r#"select count(*) from remote_table where "ID" > 1"#,
        vec![
            r#"ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
  AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
    CoalescePartitionsExec
      AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
        RepartitionExec: partitioning=RoundRobinBatch(12), input_partitions=1
          ProjectionExec: expr=[]
            CoalesceBatchesExec: target_batch_size=8192
              FilterExec: ID@0 > Some(1),38,0
                CooperativeExec
                  RemoteTableExec: source=query, projection=[ID]
"#,
            r#"ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
  AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
    CoalescePartitionsExec
      AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
        RepartitionExec: partitioning=RoundRobinBatch(12), input_partitions=1
          ProjectionExec: expr=[]
            CoalesceBatchesExec: target_batch_size=8192
              FilterExec: ID@0 > Some(1),38,0
                CooperativeExec
                  RemoteTableExec: source=SYS.simple_table, projection=[ID]
"#,
        ],
        r#"+----------+
| count(*) |
+----------+
| 2        |
+----------+"#,
    )
    .await;

    assert_plan_and_result(
        RemoteDbType::Oracle,
        source.clone(),
        r#"select count(*) from (select * from remote_table where "ID" > 1 limit 1)"#,
        vec![
            r#"ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
  AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
    CoalescePartitionsExec
      AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
        RepartitionExec: partitioning=RoundRobinBatch(12), input_partitions=1
          ProjectionExec: expr=[]
            CoalesceBatchesExec: target_batch_size=8192, fetch=1
              FilterExec: ID@0 > Some(1),38,0
                CooperativeExec
                  RemoteTableExec: source=query, projection=[ID]
"#,
            r#"ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
  AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
    CoalescePartitionsExec
      AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
        RepartitionExec: partitioning=RoundRobinBatch(12), input_partitions=1
          ProjectionExec: expr=[]
            CoalesceBatchesExec: target_batch_size=8192, fetch=1
              FilterExec: ID@0 > Some(1),38,0
                CooperativeExec
                  RemoteTableExec: source=SYS.simple_table, projection=[ID]
"#,
        ],
        r#"+----------+
| count(*) |
+----------+
| 1        |
+----------+"#,
    )
    .await;
}

#[rstest::rstest]
#[case("SELECT * from SYS.\"simple_table\"".into())]
#[case(vec!["SYS", "simple_table"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn empty_projection(#[case] source: RemoteSource) {
    setup_oracle_db().await;

    let options = build_conn_options(RemoteDbType::Oracle);
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
            "CooperativeExec\n  RemoteTableExec: source=SYS.simple_table, projection=[]\n"
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
#[case("SELECT * from SYS.\"empty_table\"".into())]
#[case(vec!["SYS", "empty_table"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn empty_table(#[case] source: RemoteSource) {
    setup_oracle_db().await;

    assert_result(
        RemoteDbType::Oracle,
        source,
        "SELECT * FROM remote_table",
        "++\n++",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
pub async fn disable_pooled_connections() {
    setup_oracle_db().await;

    let options = build_conn_options(RemoteDbType::Oracle);
    let ConnectionOptions::Oracle(options) = options else {
        unreachable!()
    };
    let options = options
        .with_pool_max_size(100usize)
        .with_pool_min_idle(0usize)
        .with_pool_idle_timeout(Duration::from_micros(1))
        .with_pool_ttl_check_interval(Duration::from_secs(3));
    let table = RemoteTable::try_new(options, "select * from SYS.\"simple_table\"")
        .await
        .unwrap();
    let pool = table.pool().clone();
    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table)).unwrap();

    let mut handles = Vec::new();
    for _ in 0..50 {
        let ctx = ctx.clone();
        let handle = tokio::spawn(async move {
            let df = ctx.sql("select * from remote_table").await.unwrap();
            let _batches = df.collect().await.unwrap();
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.await.unwrap();
    }

    tokio::time::sleep(Duration::from_secs(5)).await;
    let pool_state = pool.state().await.unwrap();
    assert_eq!(pool_state.connections, 0);
    assert_eq!(pool_state.idle_connections, 0);
}
