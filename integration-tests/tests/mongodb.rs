use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::catalog::TableProvider;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use datafusion_remote_table::{
    ConnectionOptions, MongoDBConnectionOptions, RemoteDbType, RemotePhysicalCodec, RemoteTable,
    connect,
};
use integration_tests::utils::{assert_plan_and_result, build_conn_options};
use integration_tests::{MONGODB_DATABASE, MONGODB_URI, setup_mongodb_db};
use mongodb::bson::{Bson, Document, doc, oid::ObjectId};
use std::sync::Arc;

fn schema_of(schema: &SchemaRef) -> Vec<(String, DataType, bool)> {
    schema
        .fields()
        .iter()
        .map(|field| {
            (
                field.name().clone(),
                field.data_type().clone(),
                field.metadata().contains_key("ARROW:extension:name"),
            )
        })
        .collect()
}

/// The Arrow type of a Variant column: a plain struct that the
/// `arrow.parquet.variant` extension name is attached to.
fn variant_type() -> DataType {
    DataType::Struct(
        vec![
            Field::new("metadata", DataType::BinaryView, false),
            Field::new("value", DataType::BinaryView, false),
        ]
        .into(),
    )
}

async fn raw_client() -> mongodb::Client {
    mongodb::Client::with_uri_str(MONGODB_URI).await.unwrap()
}

/// Run a query and report both the physical plan and the number of rows, which
/// is all a single opaque column allows a test to check directly.
async fn plan_and_rows(source: Vec<&str>, sql: &str) -> (String, usize) {
    let options = build_conn_options(RemoteDbType::MongoDB);
    let table = RemoteTable::try_new(options, source).await.unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table)).unwrap();

    let exec_plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let plan = format!(
        "{}",
        DisplayableExecutionPlan::new(exec_plan.as_ref()).indent(true)
    );
    let batches = collect(exec_plan, ctx.task_ctx()).await.unwrap();
    (plan, batches.iter().map(|batch| batch.num_rows()).sum())
}

async fn plan_ref(ctx: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
    ctx.sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap()
}

/// A collection is exposed as exactly one column: the whole document.
#[tokio::test(flavor = "multi_thread")]
async fn schema_is_a_single_variant_column() {
    setup_mongodb_db().await;

    let options = build_conn_options(RemoteDbType::MongoDB);
    for collection in [
        "simple_table",
        "object_id_table",
        "supported_data_types",
        "empty_collection",
    ] {
        let table = RemoteTable::try_new(options.clone(), vec![collection])
            .await
            .unwrap();
        assert_eq!(
            schema_of(&table.schema()),
            vec![("document".to_string(), variant_type(), true)],
            "unexpected schema for {collection}"
        );
    }
}

/// The Variant column has to survive a full round trip: reading a collection
/// and inserting it elsewhere must reproduce the original BSON documents. The
/// driver is the reference, so this fails if any BSON type is mangled.
#[tokio::test(flavor = "multi_thread")]
async fn documents_round_trip_through_variant() {
    setup_mongodb_db().await;

    let keys: Vec<ObjectId> = ["507f1f77bcf86cd799439021", "507f1f77bcf86cd799439022"]
        .iter()
        .map(|key| ObjectId::parse_str(key).unwrap())
        .collect();

    let client = raw_client().await;
    let database = client.database(MONGODB_DATABASE);
    let source = database.collection::<Document>("supported_data_types");
    let target = database.collection::<Document>("round_trip_target");

    let mut before = Vec::new();
    for key in &keys {
        before.push(
            source
                .find_one(doc! { "_id": *key })
                .await
                .unwrap()
                .unwrap(),
        );
    }
    // Every BSON type the fixture uses must actually be present.
    assert!(matches!(
        before[0].get("object_id_col"),
        Some(Bson::ObjectId(_))
    ));
    assert!(matches!(
        before[0].get("decimal_col"),
        Some(Bson::Decimal128(_))
    ));
    assert!(matches!(before[0].get("date_col"), Some(Bson::DateTime(_))));
    assert!(matches!(before[0].get("binary_col"), Some(Bson::Binary(_))));

    // Copy through the provider: read as Variant, write back as BSON.
    let options = build_conn_options(RemoteDbType::MongoDB);
    let source_table = RemoteTable::try_new(options.clone(), vec!["supported_data_types"])
        .await
        .unwrap();
    let target_table = RemoteTable::try_new(options, vec!["round_trip_target"])
        .await
        .unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("source", Arc::new(source_table))
        .unwrap();
    ctx.register_table("target", Arc::new(target_table))
        .unwrap();

    let inserted = ctx
        .sql("insert into target select document from source")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        pretty_format_batches(&inserted).unwrap().to_string(),
        r#"+-------+
| count |
+-------+
| 2     |
+-------+"#
    );

    let mut after = Vec::new();
    for key in &keys {
        after.push(
            target
                .find_one(doc! { "_id": *key })
                .await
                .unwrap()
                .unwrap(),
        );
    }
    assert_eq!(before, after);
}

/// An empty collection is queryable: the schema does not depend on the data.
#[tokio::test(flavor = "multi_thread")]
async fn empty_collection_is_queryable() {
    setup_mongodb_db().await;

    let options = build_conn_options(RemoteDbType::MongoDB);
    let table = RemoteTable::try_new(options, vec!["empty_collection"])
        .await
        .unwrap();
    assert_eq!(
        schema_of(&table.schema()),
        vec![("document".to_string(), variant_type(), true)]
    );

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table)).unwrap();
    let batches = ctx
        .sql("select * from remote_table")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(batches.is_empty() || batches.iter().all(|batch| batch.num_rows() == 0));
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
    assert_eq!(result.len(), 3);
    assert_eq!(
        result.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        3
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn pushdown_limit() {
    setup_mongodb_db().await;

    let (plan, rows) =
        plan_and_rows(vec!["simple_table"], "select * from remote_table limit 1").await;
    assert_eq!(
        plan,
        "CooperativeExec\n  RemoteTableScanExec: source=simple_table, limit=1\n"
    );
    assert_eq!(rows, 1);
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
async fn physical_plan_serialization() {
    setup_mongodb_db().await;

    let options = build_conn_options(RemoteDbType::MongoDB);
    let table = RemoteTable::try_new(options, vec!["simple_table"])
        .await
        .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table)).unwrap();
    let exec_plan = plan_ref(&ctx, "select * from remote_table limit 1").await;
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
