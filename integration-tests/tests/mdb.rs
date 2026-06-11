use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_remote_table::{
    ConnectionOptions, MdbConnectionOptions, RemoteDbType, RemoteSource, RemoteTable,
};
use integration_tests::setup_mdb;
use integration_tests::utils::{assert_plan_and_result, assert_result, build_conn_options};
use std::sync::Arc;

#[rstest::rstest]
#[case("SELECT * FROM Shippers".into())]
#[case(vec!["Shippers"].into())]
#[tokio::test(flavor = "multi_thread")]
pub async fn test_basic_query(#[case] source: RemoteSource) {
    assert_result(
        RemoteDbType::Mdb,
        source,
        "select * from remote_table",
        r#"+-----------+------------------+----------------+
| ShipperID | CompanyName      | Phone          |
+-----------+------------------+----------------+
| 1         | Speedy Express   | (503) 555-9831 |
| 2         | United Package   | (503) 555-3199 |
| 3         | Federal Shipping | (503) 555-9931 |
+-----------+------------------+----------------+"#,
    )
    .await;
}

#[rstest::rstest]
#[case("SELECT * FROM Shippers".into())]
#[case(vec!["Shippers"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn pushdown_limit(#[case] source: RemoteSource) {
    assert_plan_and_result(
        RemoteDbType::Mdb,
        source,
        "select * from remote_table limit 2",
        vec![
            "CooperativeExec\n  RemoteTableScanExec: source=query, limit=2\n",
            "CooperativeExec\n  RemoteTableScanExec: source=Shippers, limit=2\n",
        ],
        r#"+-----------+----------------+----------------+
| ShipperID | CompanyName    | Phone          |
+-----------+----------------+----------------+
| 1         | Speedy Express | (503) 555-9831 |
| 2         | United Package | (503) 555-3199 |
+-----------+----------------+----------------+"#,
    )
    .await;
}

#[rstest::rstest]
#[case("SELECT * FROM Shippers".into(), "query")]
#[case(vec!["Shippers"].into(), "Shippers")]
#[tokio::test(flavor = "multi_thread")]
async fn count1_agg(#[case] source: RemoteSource, #[case] source_label: &str) {
    // Table source: COUNT pushdown via ODBC SELECT COUNT(*) works
    // Query source: COUNT pushdown disabled (subquery syntax not supported)
    let query_plan = format!(
        "ProjectionExec: expr=[count(Int64(1))@0 as count(*)]\n  \
         AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]\n    \
         CoalescePartitionsExec\n      \
         AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]\n        \
         RepartitionExec: partitioning=RoundRobinBatch(12), input_partitions=1\n          \
         RemoteTableScanExec: source={source_label}, projection=[]\n"
    );
    let expected_plans: Vec<&str> = match source {
        RemoteSource::Table(_) => vec!["ProjectionExec: expr=[0 as count(*)]"],
        RemoteSource::Query(_) => vec![&query_plan],
    };
    assert_plan_and_result(
        RemoteDbType::Mdb,
        source,
        "select count(*) from remote_table",
        expected_plans,
        r#"+----------+
| count(*) |
+----------+
| 3        |
+----------+"#,
    )
    .await;
}

#[rstest::rstest]
#[case("SELECT * FROM Shippers".into())]
#[case(vec!["Shippers"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn empty_projection(#[case] source: RemoteSource) {
    let options = build_conn_options(RemoteDbType::Mdb);
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
            "CooperativeExec\n  RemoteTableScanExec: source=Shippers, projection=[]\n",
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
pub async fn products_table_with_various_types() {
    assert_result(
        RemoteDbType::Mdb,
        vec!["Products"],
        "select * from remote_table limit 2",
        r#"+-----------+-------------+------------+------------+--------------------+-----------+--------------+--------------+--------------+--------------+
| ProductID | ProductName | SupplierID | CategoryID | QuantityPerUnit    | UnitPrice | UnitsInStock | UnitsOnOrder | ReorderLevel | Discontinued |
+-----------+-------------+------------+------------+--------------------+-----------+--------------+--------------+--------------+--------------+
| 1         | Chai        | 1          | 1          | 10 boxes x 20 bags | 18.0000   | 39           | 0            | 10           | false        |
| 2         | Chang       | 1          | 1          | 24 - 12 oz bottles | 19.0000   | 17           | 40           | 25           | false        |
+-----------+-------------+------------+------------+--------------------+-----------+--------------+--------------+--------------+--------------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
pub async fn pool_state() {
    let options = build_conn_options(RemoteDbType::Mdb);
    let pool = datafusion_remote_table::connect(&options).await.unwrap();

    let conn = pool.get().await.unwrap();
    assert_eq!(pool.state().await.unwrap().connections, 1);
    drop(conn);
    assert_eq!(pool.state().await.unwrap().connections, 0);
}

#[tokio::test(flavor = "multi_thread")]
pub async fn streaming_execution() {
    let options = ConnectionOptions::Mdb(
        MdbConnectionOptions::new(setup_mdb().to_path_buf()).with_stream_chunk_size(1usize),
    );
    let table = RemoteTable::try_new(options, RemoteSource::from(vec!["Shippers"]))
        .await
        .unwrap();
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
        r#"+-----------+------------------+----------------+
| ShipperID | CompanyName      | Phone          |
+-----------+------------------+----------------+
| 1         | Speedy Express   | (503) 555-9831 |
| 2         | United Package   | (503) 555-3199 |
| 3         | Federal Shipping | (503) 555-9931 |
+-----------+------------------+----------------+"#,
    );
}

#[rstest::rstest]
#[case("SELECT * FROM Shippers WHERE ShipperID = 1".into())]
#[case(vec!["Shippers"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn pushdown_filters(#[case] source: RemoteSource) {
    // Table sources: filters pushed via rewrite_query (no unparser needed)
    // Query sources: filters applied via DataFusion FilterExec (unparser unsupported)
    assert_plan_and_result(
        RemoteDbType::Mdb,
        source,
        "select * from remote_table where \"ShipperID\" = 1",
        vec![
            "FilterExec: ShipperID@0 = 1\n  RepartitionExec: partitioning=RoundRobinBatch(12), input_partitions=1\n    RemoteTableScanExec: source=query\n",
            "CooperativeExec\n  RemoteTableScanExec: source=Shippers, filters=[(\"ShipperID\" = 1)]\n",
        ],
        r#"+-----------+----------------+----------------+
| ShipperID | CompanyName    | Phone          |
+-----------+----------------+----------------+
| 1         | Speedy Express | (503) 555-9831 |
+-----------+----------------+----------------+"#,
    )
    .await;
}
