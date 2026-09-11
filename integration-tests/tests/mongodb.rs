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
use integration_tests::{
    MONGODB_DATABASE, MONGODB_HOST, MONGODB_PASSWORD, MONGODB_PORT, MONGODB_USERNAME,
    setup_mongodb_db,
};
use parquet_variant::Variant;
use parquet_variant_compute::{VariantArray, VariantArrayBuilder};
use std::sync::Arc;

/// The canonical Variant field, taken from the library itself: if the type the
/// provider declares for its `document` column ever drifts from this, the
/// schema assertions below fail.
fn variant_field() -> Field {
    VariantArrayBuilder::new(0).build().field("document")
}

fn schema_of(schema: &SchemaRef) -> Vec<(String, DataType, bool)> {
    let canonical = variant_field();
    schema
        .fields()
        .iter()
        .map(|field| {
            (
                field.name().clone(),
                field.data_type().clone(),
                field.metadata() == canonical.metadata(),
            )
        })
        .collect()
}

/// Read the `document` column of a collection as a Variant array.
async fn documents_of(ctx: &SessionContext, table: &str) -> VariantArray {
    let batches = ctx
        .sql(&format!("select document from {table}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut arrays: Vec<VariantArray> = batches
        .iter()
        .map(|batch| VariantArray::try_new(batch.column(0).as_ref()).unwrap())
        .collect();
    assert_eq!(arrays.len(), 1, "expected a single batch");
    arrays.pop().unwrap()
}

/// Run a query and report both the physical plan and the number of rows, which
/// is all that a single opaque column lets a test check directly.
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

async fn register(ctx: &SessionContext, name: &str, collection: &str) {
    let options = build_conn_options(RemoteDbType::MongoDB);
    let table = RemoteTable::try_new(options, vec![MONGODB_DATABASE, collection])
        .await
        .unwrap();
    ctx.register_table(name, Arc::new(table)).unwrap();
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
        let table = RemoteTable::try_new(options.clone(), vec![MONGODB_DATABASE, collection])
            .await
            .unwrap();
        assert_eq!(
            schema_of(&table.schema()),
            vec![(
                "document".to_string(),
                variant_field().data_type().clone(),
                true
            )],
            "unexpected schema for {collection}"
        );
        assert!(!table.schema().field(0).is_nullable());
    }
}

/// BSON types are exposed as the Variant types that can hold them.
///
/// The assertions pin down the conversion: the types Variant can express are
/// stored natively with their BSON widths, and the ones it cannot stay
/// recognisable in their canonical extended JSON form.
#[tokio::test(flavor = "multi_thread")]
async fn bson_types_are_exposed_as_variant() {
    setup_mongodb_db().await;

    let ctx = SessionContext::new();
    register(&ctx, "source", "supported_data_types").await;

    let documents = documents_of(&ctx, "source").await;
    assert_eq!(documents.len(), 2);
    let Variant::Object(document) = documents.value(0) else {
        panic!("a document must be an object");
    };

    // Numbers keep the width they were stored with.
    assert!(matches!(document.get("int32_col"), Some(Variant::Int32(2))));
    assert!(matches!(document.get("int64_col"), Some(Variant::Int64(3))));
    assert!(matches!(
        document.get("double_col"),
        Some(Variant::Double(_))
    ));
    assert!(matches!(
        document.get("bool_col"),
        Some(Variant::BooleanTrue)
    ));
    assert!(matches!(
        document.get("string_col"),
        Some(Variant::ShortString(_) | Variant::String(_))
    ));
    // Dates are timestamps and binary values stay bytes.
    assert!(matches!(
        document.get("date_col"),
        Some(Variant::TimestampMicros(_) | Variant::TimestampNtzMicros(_))
    ));
    assert!(matches!(
        document.get("binary_col"),
        Some(Variant::Binary(_))
    ));
    // Nested documents and arrays are native, not text.
    assert!(matches!(
        document.get("object_col"),
        Some(Variant::Object(_))
    ));
    assert!(matches!(document.get("array_col"), Some(Variant::List(_))));
    assert!(matches!(document.get("null_col"), Some(Variant::Null)));
    // The types Variant cannot express use their canonical extended JSON form,
    // which is where the document key lives too.
    for wrapper in ["_id", "object_id_col"] {
        let Some(Variant::Object(value)) = document.get(wrapper) else {
            panic!("{wrapper} must be wrapped as an object");
        };
        assert!(matches!(
            value.get("$oid"),
            Some(Variant::ShortString(_) | Variant::String(_))
        ));
    }
    let Some(Variant::Object(decimal)) = document.get("decimal_col") else {
        panic!("decimal must be wrapped as an object");
    };
    assert!(matches!(
        decimal.get("$numberDecimal"),
        Some(Variant::ShortString(_) | Variant::String(_))
    ));
}

/// An empty collection is queryable: the schema does not depend on the data.
#[tokio::test(flavor = "multi_thread")]
async fn empty_collection_is_queryable() {
    setup_mongodb_db().await;

    let options = build_conn_options(RemoteDbType::MongoDB);
    let table = RemoteTable::try_new(options, vec![MONGODB_DATABASE, "empty_collection"])
        .await
        .unwrap();
    assert_eq!(
        schema_of(&table.schema()),
        vec![(
            "document".to_string(),
            variant_field().data_type().clone(),
            true
        )]
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
        MongoDBConnectionOptions::new(
            MONGODB_HOST,
            MONGODB_PORT,
            MONGODB_USERNAME,
            MONGODB_PASSWORD,
        )
        .with_stream_chunk_size(1usize),
    );
    let table = RemoteTable::try_new(options, vec![MONGODB_DATABASE, "simple_table"])
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

    let (plan, rows) = plan_and_rows(
        vec![MONGODB_DATABASE, "simple_table"],
        "select * from remote_table limit 1",
    )
    .await;
    assert_eq!(
        plan,
        "CooperativeExec\n  RemoteTableScanExec: source=test.simple_table, limit=1\n"
    );
    assert_eq!(rows, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn count_documents() {
    setup_mongodb_db().await;

    // `count()` uses `count_documents`, so DataFusion collapses the aggregate.
    assert_plan_and_result(
        RemoteDbType::MongoDB,
        vec![MONGODB_DATABASE, "simple_table"],
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
    let table = RemoteTable::try_new(options, vec![MONGODB_DATABASE, "simple_table"])
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
    let table = RemoteTable::try_new(options, vec![MONGODB_DATABASE, "simple_table"])
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

/// Insert is not supported: the plan is accepted, but running it reports the
/// gap rather than writing a document that lost the BSON types Variant cannot
/// carry.
#[tokio::test(flavor = "multi_thread")]
async fn insert_is_rejected() {
    setup_mongodb_db().await;

    let ctx = SessionContext::new();
    register(&ctx, "source", "simple_table").await;
    register(&ctx, "target", "empty_collection").await;

    let error = ctx
        .sql("insert into target select document from source")
        .await
        .unwrap()
        .collect()
        .await
        .expect_err("MongoDB does not support insert");
    let message = error.to_string();
    assert!(
        message.contains("MongoDB does not support insert"),
        "unexpected error: {message}"
    );
}

/// A client is not bound to a database, so a collection identifier has to name
/// one; a bare collection name has nowhere to look.
#[tokio::test(flavor = "multi_thread")]
async fn bare_collection_is_rejected() {
    setup_mongodb_db().await;

    let options = build_conn_options(RemoteDbType::MongoDB);
    let err = RemoteTable::try_new(options, vec!["simple_table"])
        .await
        .expect_err("a collection alone does not name a database");
    let message = err.to_string();
    assert!(
        message.contains("must be [database, collection]"),
        "unexpected error: {message}"
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

/// Pool settings have to reach the driver. It validates them when it builds a
/// client and rejects a zero with a typed error, which is the cheapest proof
/// that the value made it all the way; whether every value survives the plan
/// codec is checked by a unit test on the codec itself, since a plan can only
/// be serialized once the driver has accepted its options.
#[tokio::test(flavor = "multi_thread")]
async fn pool_options_reach_the_driver() {
    setup_mongodb_db().await;

    // Inferring the schema connects, so an invalid value is rejected here.
    let err = RemoteTable::try_new(
        ConnectionOptions::MongoDB(
            MongoDBConnectionOptions::new(
                MONGODB_HOST,
                MONGODB_PORT,
                MONGODB_USERNAME,
                MONGODB_PASSWORD,
            )
            .with_pool_max_size(Some(0)),
        ),
        vec![MONGODB_DATABASE, "simple_table"],
    )
    .await
    .expect_err("the driver rejects maxPoolSize=0");
    assert!(
        err.to_string().contains("maxPoolSize"),
        "unexpected error: {err}"
    );
}

/// Valid pool settings must not get in the way of querying.
#[tokio::test(flavor = "multi_thread")]
async fn pool_options_are_accepted() {
    setup_mongodb_db().await;

    let options = ConnectionOptions::MongoDB(
        MongoDBConnectionOptions::new(
            MONGODB_HOST,
            MONGODB_PORT,
            MONGODB_USERNAME,
            MONGODB_PASSWORD,
        )
        .with_pool_max_size(Some(4))
        .with_pool_min_idle(Some(1))
        .with_pool_max_connecting(Some(2))
        .with_pool_idle_timeout(Some(std::time::Duration::from_secs(60))),
    );
    let table = RemoteTable::try_new(options, vec![MONGODB_DATABASE, "simple_table"])
        .await
        .unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table)).unwrap();

    let batches = ctx
        .sql("select count(*) from remote_table")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        pretty_format_batches(&batches).unwrap().to_string(),
        r#"+----------+
| count(*) |
+----------+
| 3        |
+----------+"#
    );
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
