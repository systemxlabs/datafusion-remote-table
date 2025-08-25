use datafusion::physical_plan::collect;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_remote_table::{RemoteDbType, RemoteTable};
use integration_tests::setup_oracle_db;
use integration_tests::utils::{
    assert_plan_and_result, assert_result, assert_sqls, build_conn_options,
};
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
pub async fn supported_oracle_types() {
    setup_oracle_db().await;
    assert_result(
        RemoteDbType::Oracle,
        "SELECT * from SYS.supported_data_types",
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
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
pub async fn supported_oracle_types2() {
    setup_oracle_db().await;
    assert_result(
        RemoteDbType::Oracle,
        "SELECT * from SYS.supported_data_types2",
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

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
pub async fn various_sqls() {
    setup_oracle_db().await;

    assert_sqls(RemoteDbType::Oracle, vec!["select * from USER_TABLES"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn pushdown_limit() {
    setup_oracle_db().await;
    assert_plan_and_result(
        RemoteDbType::Oracle,
        "select * from SYS.simple_table",
        "select * from remote_table limit 1",
        "RemoteTableExec: limit=Some(1), filters=[]\n",
        r#"+----+------+
| ID | NAME |
+----+------+
| 1  | Tom  |
+----+------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn pushdown_filters() {
    setup_oracle_db().await;
    assert_plan_and_result(
        RemoteDbType::Oracle,
        "select * from SYS.simple_table",
        r#"select * from remote_table where "ID" = 1"#,
        "CoalesceBatchesExec: target_batch_size=8192\n  FilterExec: ID@0 = Some(1),38,0\n    RemoteTableExec: limit=None, filters=[]\n",
        r#"+----+------+
| ID | NAME |
+----+------+
| 1  | Tom  |
+----+------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn count1_agg() {
    setup_oracle_db().await;

    assert_plan_and_result(
        RemoteDbType::Oracle,
        "select * from SYS.simple_table",
        "select count(*) from remote_table",
        "ProjectionExec: expr=[3 as count(*)]\n  PlaceholderRowExec\n",
        r#"+----------+
| count(*) |
+----------+
| 3        |
+----------+"#,
    )
    .await;

    assert_plan_and_result(
        RemoteDbType::Oracle,
        "select * from SYS.simple_table",
        r#"select count(*) from remote_table where "ID" > 1"#,
        r#"ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
  AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
    CoalescePartitionsExec
      AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
        RepartitionExec: partitioning=RoundRobinBatch(12), input_partitions=1
          ProjectionExec: expr=[]
            CoalesceBatchesExec: target_batch_size=8192
              FilterExec: ID@0 > Some(1),38,0
                RemoteTableExec: limit=None, filters=[]
"#,
        r#"+----------+
| count(*) |
+----------+
| 2        |
+----------+"#,
    )
    .await;

    assert_plan_and_result(
        RemoteDbType::Oracle,
        "select * from SYS.simple_table",
        r#"select count(*) from (select * from remote_table where "ID" > 1 limit 1)"#,
        r#"ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
  AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
    CoalescePartitionsExec
      AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
        RepartitionExec: partitioning=RoundRobinBatch(12), input_partitions=1
          ProjectionExec: expr=[]
            CoalesceBatchesExec: target_batch_size=8192, fetch=1
              FilterExec: ID@0 > Some(1),38,0
                RemoteTableExec: limit=None, filters=[]
"#,
        r#"+----------+
| count(*) |
+----------+
| 1        |
+----------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn empty_projection() {
    setup_oracle_db().await;

    let options = build_conn_options(RemoteDbType::Oracle);
    let table = RemoteTable::try_new(options, "select * from SYS.simple_table")
        .await
        .unwrap();

    let config = SessionConfig::new().with_target_partitions(12);
    let ctx = SessionContext::new_with_config(config);

    let df = ctx.read_table(Arc::new(table)).unwrap();
    let df = df.select_columns(&[]).unwrap();

    let exec_plan = df.create_physical_plan().await.unwrap();
    let plan_display = DisplayableExecutionPlan::new(exec_plan.as_ref())
        .indent(true)
        .to_string();
    println!("{plan_display}");
    assert_eq!(plan_display, "RemoteTableExec: limit=None, filters=[]\n");

    let result = collect(exec_plan, ctx.task_ctx()).await.unwrap();
    assert_eq!(result.len(), 1);
    let batch = &result[0];
    assert_eq!(batch.num_columns(), 0);
    assert_eq!(batch.num_rows(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn empty_table() {
    setup_oracle_db().await;

    assert_result(
        RemoteDbType::Oracle,
        "select * from SYS.empty_table",
        "SELECT * FROM remote_table",
        "++\n++",
    )
    .await;
}
