use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use datafusion_remote_table::{
    ConnectionOptions, MongoDBConnectionOptions, MongoDBType, RemoteDbType, RemoteField,
    RemotePhysicalCodec, RemoteSchema, RemoteTable, RemoteType, connect,
};
use integration_tests::utils::{assert_plan_and_result, assert_result, build_conn_options};
use integration_tests::{MONGODB_DATABASE, MONGODB_URI, setup_mongodb_db};
use std::sync::Arc;

const SIMPLE_TABLE: &str = r#"+-----+----+-------+
| _id | id | name  |
+-----+----+-------+
| 1   | 1  | Tom   |
| 2   | 2  | Jerry |
| 3   | 3  | Spike |
+-----+----+-------+"#;

#[tokio::test(flavor = "multi_thread")]
pub async fn supported_mongodb_types() {
    setup_mongodb_db().await;

    assert_result(
        RemoteDbType::MongoDB,
        vec!["supported_data_types"],
        "select * from remote_table order by _id",
        r#"+-----+------------+-----------+-----------+------------+----------+----------------------+--------------------------+------------+-------------+------------+-----------+----------+
| _id | double_col | int32_col | int64_col | string_col | bool_col | date_col             | object_id_col            | binary_col | decimal_col | object_col | array_col | null_col |
+-----+------------+-----------+-----------+------------+----------+----------------------+--------------------------+------------+-------------+------------+-----------+----------+
| 1   | 1.5        | 2         | 3         | text       | true     | 2024-01-02T03:04:05Z | 507f1f77bcf86cd799439011 | 0102       | 1.23        | { "a": 1 } | [1, 2]    |          |
| 2   |            |           |           |            |          |                      |                          |            |             |            |           |          |
+-----+------------+-----------+-----------+------------+----------+----------------------+--------------------------+------------+-------------+------------+-----------+----------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn streaming_execution() {
    setup_mongodb_db().await;

    // A chunk size of one must produce one record batch per document.
    let options = ConnectionOptions::MongoDB(
        MongoDBConnectionOptions::new(MONGODB_URI, MONGODB_DATABASE).with_stream_chunk_size(1usize),
    );
    let table = RemoteTable::try_new(options, vec!["simple_table"])
        .await
        .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table)).unwrap();

    let result = ctx
        .sql("select * from remote_table")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let table_str = pretty_format_batches(&result).unwrap().to_string();
    println!("{table_str}");

    assert_eq!(table_str, SIMPLE_TABLE);
    assert_eq!(result.len(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn pushdown_limit() {
    setup_mongodb_db().await;

    assert_plan_and_result(
        RemoteDbType::MongoDB,
        vec!["simple_table"],
        "select * from remote_table limit 1",
        vec!["CooperativeExec\n  RemoteTableScanExec: source=simple_table, limit=1\n"],
        r#"+-----+----+------+
| _id | id | name |
+-----+----+------+
| 1   | 1  | Tom  |
+-----+----+------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn filters_are_applied_locally() {
    setup_mongodb_db().await;

    // MongoDB has no SQL unparser, so DataFusion keeps the filter above the scan.
    assert_plan_and_result(
        RemoteDbType::MongoDB,
        vec!["simple_table"],
        "select * from remote_table where id = 1",
        vec!["FilterExec: id@1 = 1\n  CooperativeExec\n    RemoteTableScanExec: source=simple_table\n"],
        r#"+-----+----+------+
| _id | id | name |
+-----+----+------+
| 1   | 1  | Tom  |
+-----+----+------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn count_documents() {
    setup_mongodb_db().await;

    // `count()` uses `count_documents`, so DataFusion collapses the aggregate.
    assert_plan_and_result(
        RemoteDbType::MongoDB,
        vec!["simple_table"],
        "select count(*) from remote_table",
        vec!["ProjectionExec: expr=[3 as count(*)]\n  PlaceholderRowExec\n"],
        r#"+----------+
| count(*) |
+----------+
| 3        |
+----------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_projection() {
    setup_mongodb_db().await;

    let options = build_conn_options(RemoteDbType::MongoDB);
    let table = RemoteTable::try_new(options, vec!["simple_table"])
        .await
        .unwrap();

    let config = SessionConfig::new().with_target_partitions(12);
    let ctx = SessionContext::new_with_config(config);

    let df = ctx.read_table(Arc::new(table)).unwrap();
    let df = df.select_columns(&[]).unwrap();

    let exec_plan = df.create_physical_plan().await.unwrap();
    println!(
        "{}",
        DisplayableExecutionPlan::new(exec_plan.as_ref()).indent(true)
    );

    let result = collect(exec_plan, ctx.task_ctx()).await.unwrap();
    assert_eq!(result.len(), 1);
    let batch = &result[0];
    assert_eq!(batch.num_columns(), 0);
    assert_eq!(batch.num_rows(), 3);
}

#[tokio::test(flavor = "multi_thread")]
pub async fn insert_supported_mongodb_types() {
    setup_mongodb_db().await;

    // The collection starts empty, so the insert tests declare the schema explicitly.
    let options =
        ConnectionOptions::MongoDB(MongoDBConnectionOptions::new(MONGODB_URI, MONGODB_DATABASE));
    let remote_schema = Arc::new(RemoteSchema::new(vec![
        RemoteField::new("name", RemoteType::MongoDB(MongoDBType::String), true),
        RemoteField::new("score", RemoteType::MongoDB(MongoDBType::Double), true),
        RemoteField::new("active", RemoteType::MongoDB(MongoDBType::Boolean), true),
        RemoteField::new("id", RemoteType::MongoDB(MongoDBType::Int32), true),
    ]));
    let table = RemoteTable::try_new_with_remote_schema(
        options,
        vec!["insert_supported_data_types"],
        remote_schema,
    )
    .await
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table)).unwrap();

    let df = ctx
        .sql("insert into remote_table (name, score, active, id) values ('Eve', 1.5, true, 7)")
        .await
        .unwrap();
    let exec_plan = df.create_physical_plan().await.unwrap();
    println!(
        "{}",
        DisplayableExecutionPlan::new(exec_plan.as_ref()).indent(true)
    );

    let result = collect(exec_plan, ctx.task_ctx()).await.unwrap();
    assert_eq!(
        pretty_format_batches(&result).unwrap().to_string(),
        r#"+-------+
| count |
+-------+
| 1     |
+-------+"#
    );

    let result = ctx
        .sql("select * from remote_table")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let table_str = pretty_format_batches(&result).unwrap().to_string();
    println!("{table_str}");
    assert_eq!(
        table_str,
        r#"+------+-------+--------+----+
| name | score | active | id |
+------+-------+--------+----+
| Eve  | 1.5   | true   | 7  |
+------+-------+--------+----+"#,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn physical_plan_serialization() {
    setup_mongodb_db().await;

    let options = build_conn_options(RemoteDbType::MongoDB);
    let table = RemoteTable::try_new(options, vec!["simple_table"])
        .await
        .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table)).unwrap();
    let exec_plan = ctx
        .sql("select * from remote_table limit 1")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let expected = collect(exec_plan.clone(), ctx.task_ctx()).await.unwrap();

    let codec = RemotePhysicalCodec::new();
    let mut plan_buf: Vec<u8> = vec![];
    PhysicalPlanNode::try_from_physical_plan(exec_plan, &codec)
        .unwrap()
        .try_encode(&mut plan_buf)
        .unwrap();
    let new_plan: Arc<dyn ExecutionPlan> = PhysicalPlanNode::try_decode(&plan_buf)
        .and_then(|proto| proto.try_into_physical_plan(&ctx.task_ctx(), &codec))
        .unwrap();
    println!(
        "deserialized plan: {}",
        DisplayableExecutionPlan::new(new_plan.as_ref()).indent(true)
    );

    let actual = collect(new_plan, ctx.task_ctx()).await.unwrap();
    assert_eq!(
        pretty_format_batches(&expected).unwrap().to_string(),
        pretty_format_batches(&actual).unwrap().to_string()
    );
}

/// MongoDB has no query string, so anything other than a collection is refused.
#[tokio::test(flavor = "multi_thread")]
async fn query_source_is_rejected() {
    setup_mongodb_db().await;

    let options = build_conn_options(RemoteDbType::MongoDB);
    for source in [
        r#"{"find": "simple_table"}"#,
        r#"{"aggregate": "simple_table", "pipeline": []}"#,
        "select 1",
    ] {
        let err = RemoteTable::try_new(options.clone(), source.to_string())
            .await
            .expect_err("MongoDB only supports RemoteSource::Table");
        let message = err.to_string();
        assert!(
            message.contains("only supports RemoteSource::Table"),
            "unexpected error for {source}: {message}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
pub async fn pool_state() {
    setup_mongodb_db().await;

    let options = build_conn_options(RemoteDbType::MongoDB);
    let pool = connect(&options).await.unwrap();

    let conn = pool.get().await.unwrap();
    assert_eq!(pool.state().await.unwrap().connections, 1);
    drop(conn);
    assert_eq!(pool.state().await.unwrap().connections, 0);
}
