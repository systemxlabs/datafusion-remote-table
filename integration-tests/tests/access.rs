use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use datafusion_remote_table::{
    AccessConnectionOptions, ConnectionOptions, RemoteDbType, RemotePhysicalCodec, RemoteSource,
    RemoteTable,
};
use integration_tests::setup_accdb;
use integration_tests::utils::{assert_plan_and_result, assert_result, build_conn_options};
use std::sync::Arc;

/// The Access fixture (`ASampleDatabase.accdb`) uses the ACE engine, so these
/// tests cover `.accdb` files through `RemoteDbType::Access`. `.mdb` files are
/// covered by `mdb.rs`; the two backends are independent, so the tests mirror
/// each other rather than share fixtures.

#[rstest::rstest]
#[case("SELECT * FROM \"Asset Items\"".into())]
#[case(vec!["Asset Items"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn test_basic_query(#[case] source: RemoteSource) {
    assert_plan_and_result(
        RemoteDbType::Access,
        source,
        "select \"Asset No\", \"Make\", \"Cost\" from remote_table limit 3",
        vec![
            "CooperativeExec\n  RemoteTableScanExec: source=query, projection=[Asset No, Make, Cost], limit=3\n",
            "CooperativeExec\n  RemoteTableScanExec: source=Asset Items, projection=[Asset No, Make, Cost], limit=3\n",
        ],
        r#"+----------+------------+-----------+
| Asset No | Make       | Cost      |
+----------+------------+-----------+
| 30050    | GEO Rocket | 1995.5000 |
| 30051    | GEO Blast  | 2450.0000 |
| 30052    | FurnTown   | 244.0000  |
+----------+------------+-----------+"#,
    )
    .await;
}

#[rstest::rstest]
#[case("SELECT * FROM \"Asset Items\"".into())]
#[case(vec!["Asset Items"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn filter_on_currency_column(#[case] source: RemoteSource) {
    // Cost is a Currency column, which mdbtools' mdb_test_sarg() cannot compare,
    // so the filter is classified as inexact and DataFusion re-checks it locally.
    assert_plan_and_result(
        RemoteDbType::Access,
        source,
        "select \"Asset No\", \"Cost\" from remote_table \
         where \"Cost\" > 1000 order by \"Asset No\" limit 5",
        vec![
            "SortPreservingMergeExec: [Asset No@0 ASC NULLS LAST], fetch=5\n  SortExec: TopK(fetch=5), expr=[Asset No@0 ASC NULLS LAST], preserve_partitioning=[true]\n    FilterExec: Cost@1 > 1000.0000\n      RepartitionExec: partitioning=RoundRobinBatch(12), input_partitions=1\n        RemoteTableScanExec: source=query, projection=[Asset No, Cost]\n",
            "SortPreservingMergeExec: [Asset No@0 ASC NULLS LAST], fetch=5\n  SortExec: TopK(fetch=5), expr=[Asset No@0 ASC NULLS LAST], preserve_partitioning=[true]\n    FilterExec: Cost@1 > 1000.0000\n      RepartitionExec: partitioning=RoundRobinBatch(12), input_partitions=1\n        RemoteTableScanExec: source=Asset Items, projection=[Asset No, Cost], filters=[(\"Cost\" > 1000.0000)]\n",
        ],
        r#"+----------+-----------+
| Asset No | Cost      |
+----------+-----------+
| 30050    | 1995.5000 |
| 30051    | 2450.0000 |
| 30054    | 2560.0000 |
| 30055    | 5433.0000 |
| 30058    | 1255.0000 |
+----------+-----------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn list_tables_through_msys_objects() {
    // `.accdb` sources have no list-tables command: an ACE file has a real
    // catalog table, so the tables and stored queries are read from it like any
    // other source. Type 1 = local table, 5 = stored query.
    assert_result(
        RemoteDbType::Access,
        vec!["MSysObjects"],
        r#"select "Name", "Type" from remote_table where "Type" = 5 and "Name" like 'qry%' order by "Name""#,
        r#"+---------------------------------+------+
| Name                            | Type |
+---------------------------------+------+
| qryComputerHardwareInOwnerOrder | 5    |
| qryCostsSummedByOwner           | 5    |
| qryGSTCalculations              | 5    |
+---------------------------------+------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
pub async fn pool_state() {
    let options = build_conn_options(RemoteDbType::Access);
    let pool = datafusion_remote_table::connect(&options).await.unwrap();

    let conn = pool.get().await.unwrap();
    assert_eq!(pool.state().await.unwrap().connections, 1);
    drop(conn);
    assert_eq!(pool.state().await.unwrap().connections, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn serialization_round_trip() {
    let options = build_conn_options(RemoteDbType::Access);
    let table = RemoteTable::try_new(options, vec!["Asset Items"])
        .await
        .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table)).unwrap();

    let df = ctx
        .sql("select \"Asset No\", \"Cost\" from remote_table where \"Cost\" > 4000 order by \"Asset No\"")
        .await
        .unwrap();
    let plan = df.create_physical_plan().await.unwrap();

    // The Access connection options travel through the plan as their own
    // oneof field, so a round trip has to keep the database type intact.
    let codec = RemotePhysicalCodec::new();
    let mut plan_buf: Vec<u8> = vec![];
    let plan_proto = PhysicalPlanNode::try_from_physical_plan(plan, &codec).unwrap();
    plan_proto.try_encode(&mut plan_buf).unwrap();
    let new_plan = PhysicalPlanNode::try_decode(&plan_buf)
        .and_then(|proto| proto.try_into_physical_plan(&ctx.task_ctx(), &codec))
        .unwrap();
    println!(
        "deserialized plan: {}",
        DisplayableExecutionPlan::new(new_plan.as_ref()).indent(true)
    );

    let batches = collect(new_plan, ctx.task_ctx()).await.unwrap();
    assert_eq!(
        datafusion::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string(),
        r#"+----------+-----------+
| Asset No | Cost      |
+----------+-----------+
| 30055    | 5433.0000 |
| 30071    | 6799.0000 |
| 30090    | 5433.0000 |
| 30110    | 5999.0000 |
+----------+-----------+"#
    );
}

#[rstest::rstest]
#[case("SELECT * FROM \"Asset Items\"".into())]
#[case(vec!["Asset Items"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn pushdown_limit(#[case] source: RemoteSource) {
    assert_plan_and_result(
        RemoteDbType::Access,
        source,
        "select \"Asset No\", \"Make\", \"Cost\" from remote_table limit 2",
        vec![
            "CooperativeExec\n  RemoteTableScanExec: source=query, projection=[Asset No, Make, Cost], limit=2\n",
            "CooperativeExec\n  RemoteTableScanExec: source=Asset Items, projection=[Asset No, Make, Cost], limit=2\n",
        ],
        r#"+----------+------------+-----------+
| Asset No | Make       | Cost      |
+----------+------------+-----------+
| 30050    | GEO Rocket | 1995.5000 |
| 30051    | GEO Blast  | 2450.0000 |
+----------+------------+-----------+"#,
    )
    .await;
}

#[rstest::rstest]
#[case("SELECT * FROM \"Asset Items\"".into())]
#[case(vec!["Asset Items"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn count1_agg(#[case] source: RemoteSource) {
    // Table source: COUNT pushdown via the Access row-count fast path
    // Query source: COUNT via DataFusion aggregate (no pushdown)
    let query_plan = "ProjectionExec: expr=[count(Int64(1))@0 as count(*)]\n  \
         AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]\n    \
         CoalescePartitionsExec\n      \
         AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]\n        \
         RepartitionExec: partitioning=RoundRobinBatch(12), input_partitions=1\n          \
         RemoteTableScanExec: source=query, projection=[]\n"
        .to_string();
    let expected_plans: Vec<&str> = match &source {
        RemoteSource::Table(_) => {
            vec!["ProjectionExec: expr=[65 as count(*)]\n  PlaceholderRowExec\n"]
        }
        RemoteSource::Query(_) => vec![&query_plan],
        RemoteSource::Command(_) => unreachable!("Command not used in this test"),
    };
    assert_plan_and_result(
        RemoteDbType::Access,
        source,
        "select count(*) from remote_table",
        expected_plans,
        r#"+----------+
| count(*) |
+----------+
| 65       |
+----------+"#,
    )
    .await;
}

#[rstest::rstest]
#[case("SELECT * FROM \"Asset Items\"".into())]
#[case(vec!["Asset Items"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn empty_projection(#[case] source: RemoteSource) {
    let options = build_conn_options(RemoteDbType::Access);
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
            "CooperativeExec\n  RemoteTableScanExec: source=query, projection=[]\n",
            "CooperativeExec\n  RemoteTableScanExec: source=Asset Items, projection=[]\n",
        ]
        .contains(&plan_display.as_str())
    );

    let result = collect(exec_plan, ctx.task_ctx()).await.unwrap();
    assert_eq!(result.len(), 1);
    let batch = &result[0];
    assert_eq!(batch.num_columns(), 0);
    assert_eq!(batch.num_rows(), 65);
}

#[tokio::test(flavor = "multi_thread")]
pub async fn asset_items_table_with_various_types() {
    assert_result(
        RemoteDbType::Access,
        vec!["Asset Items"],
        "select * from remote_table limit 2",
        r#"+----------+-------------------+------------+------------+-------------+-------+-----------+---------------------+-----------+----------+-----------+-------------+----------+
| Asset No | Asset Category    | Make       | Model      | Description | Owner | Serial No | Acquired            | Cost      | Warranty | Tax Scale | Supplier No | Comments |
+----------+-------------------+------------+------------+-------------+-------+-----------+---------------------+-----------+----------+-----------+-------------+----------+
| 30050    | Computer Hardware | GEO Rocket | 220ZX      | Computer    | Sales | 344-667   | 1997-09-02T00:00:00 | 1995.5000 | 12       | A         | 44577       |          |
| 30051    | Computer Hardware | GEO Blast  | Surger 350 | Computer    | Sales | 345-556   | 1997-09-02T00:00:00 | 2450.0000 | 12       | A         | 44577       |          |
+----------+-------------------+------------+------------+-------------+-------+-----------+---------------------+-----------+----------+-----------+-------------+----------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
pub async fn streaming_execution() {
    let options = ConnectionOptions::Access(
        AccessConnectionOptions::new(setup_accdb().to_path_buf()).with_stream_chunk_size(1usize),
    );
    let table = RemoteTable::try_new(options, RemoteSource::from(vec!["Asset Items"]))
        .await
        .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table)).unwrap();

    let df = ctx
        .sql("select \"Asset No\", \"Make\" from remote_table where \"Asset No\" < '30053' order by \"Asset No\"")
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
        r#"+----------+------------+
| Asset No | Make       |
+----------+------------+
| 30050    | GEO Rocket |
| 30051    | GEO Blast  |
| 30052    | FurnTown   |
+----------+------------+"#
    );
}

#[rstest::rstest]
#[case("SELECT * FROM \"Asset Items\" WHERE \"Asset No\" = '30050'".into())]
#[case(vec!["Asset Items"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn pushdown_filters(#[case] source: RemoteSource) {
    // Table sources: filters pushed via rewrite_query (no unparser needed)
    // Query sources: filters applied via DataFusion FilterExec (unparser unsupported)
    assert_plan_and_result(
        RemoteDbType::Access,
        source,
        "select \"Asset No\", \"Make\", \"Cost\" from remote_table where \"Asset No\" = '30050'",
        vec![
            "FilterExec: Asset No@0 = 30050\n  RepartitionExec: partitioning=RoundRobinBatch(12), input_partitions=1\n    RemoteTableScanExec: source=query, projection=[Asset No, Make, Cost]\n",
            "CooperativeExec\n  RemoteTableScanExec: source=Asset Items, projection=[Asset No, Make, Cost], filters=[(\"Asset No\" = '30050')]\n",
        ],
        r#"+----------+------------+-----------+
| Asset No | Make       | Cost      |
+----------+------------+-----------+
| 30050    | GEO Rocket | 1995.5000 |
+----------+------------+-----------+"#,
    )
    .await;
}

#[rstest::rstest]
#[case("SELECT * FROM \"Asset Items\" WHERE \"Make\" LIKE 'GEO%'".into())]
#[case(vec!["Asset Items"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn filter_on_text_column(#[case] source: RemoteSource) {
    // Text comparisons are supported by mdb_test_sarg(), so the filter stays
    // pushed down on a table source and results are still correct.
    assert_result(
        RemoteDbType::Access,
        source,
        "select \"Asset No\", \"Make\" from remote_table where \"Make\" like 'GEO%' order by \"Asset No\"",
        r#"+----------+------------+
| Asset No | Make       |
+----------+------------+
| 30050    | GEO Rocket |
| 30051    | GEO Blast  |
| 30054    | GEO Blast  |
| 30055    | GEO Blast  |
| 30066    | GEO Rocket |
| 30067    | GEO Rocket |
| 30089    | GEO Blast  |
| 30090    | GEO Blast  |
| 30102    | GEO Rocket |
| 30104    | GEO Rocket |
| 30105    | GEO Rocket |
| 30110    | GEO Blast  |
+----------+------------+"#,
    )
    .await;
}

#[rstest::rstest]
#[case("SELECT * FROM \"Asset Items\" WHERE \"Warranty\" > 12".into())]
#[case(vec!["Asset Items"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn filter_on_integer_column(#[case] source: RemoteSource) {
    // Integer comparisons are supported by mdb_test_sarg().
    assert_result(
        RemoteDbType::Access,
        source,
        "select \"Asset No\", \"Warranty\" from remote_table where \"Warranty\" > 12 order by \"Asset No\"",
        r#"+----------+----------+
| Asset No | Warranty |
+----------+----------+
| 30055    | 24       |
| 30071    | 24       |
| 30072    | 24       |
| 30090    | 24       |
| 30108    | 24       |
| 30110    | 24       |
+----------+----------+"#,
    )
    .await;
}
