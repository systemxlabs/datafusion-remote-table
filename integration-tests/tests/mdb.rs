use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_remote_table::{
    ConnectionOptions, MdbConnectionOptions, RemoteDbType, RemoteSource, RemoteTable, SourceCommand,
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
#[case("SELECT * FROM Shippers".into())]
#[case(vec!["Shippers"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn count1_agg(#[case] source: RemoteSource) {
    // Table source: COUNT pushdown via MDB row-count fast path
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
            vec!["ProjectionExec: expr=[3 as count(*)]\n  PlaceholderRowExec\n"]
        }
        RemoteSource::Query(_) => vec![&query_plan],
        RemoteSource::Command(_) => unreachable!("Command not used in this test"),
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

#[tokio::test(flavor = "multi_thread")]
async fn list_tables_basic() {
    assert_result(
        RemoteDbType::Mdb,
        RemoteSource::Command(SourceCommand::ListMdbTables),
        "select * from remote_table order by table_name limit 3",
        r#"+-------------------------------+------------+
| table_name                    | table_type |
+-------------------------------+------------+
| Alphabetical List of Products | View       |
| Catalog                       | View       |
| Categories                    | Table      |
+-------------------------------+------------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn list_tables_projection_and_limit() {
    let options = build_conn_options(RemoteDbType::Mdb);
    let table = RemoteTable::try_new(options, RemoteSource::Command(SourceCommand::ListMdbTables))
        .await
        .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table)).unwrap();

    // Projection: select only table_name column with a limit
    let df = ctx
        .sql("select table_name from remote_table order by table_name limit 2")
        .await
        .unwrap();
    let result = collect(df.create_physical_plan().await.unwrap(), ctx.task_ctx())
        .await
        .unwrap();
    let formatted = pretty_format_batches(&result).unwrap().to_string();
    assert_eq!(
        formatted,
        r#"+-------------------------------+
| table_name                    |
+-------------------------------+
| Alphabetical List of Products |
| Catalog                       |
+-------------------------------+"#
    );
}

#[rstest::rstest]
#[case("SELECT * FROM Products WHERE UnitPrice > 20".into())]
#[case(vec!["Products"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn filter_on_currency_column(#[case] source: RemoteSource) {
    // UnitPrice is a Jet Currency column. mdbtools' mdb_test_sarg() has no case
    // for MONEY, so the pushed predicate filters nothing remotely; the filter is
    // classified as inexact and DataFusion must apply it locally.
    assert_plan_and_result(
        RemoteDbType::Mdb,
        source,
        "select \"ProductID\", \"ProductName\", \"UnitPrice\" from remote_table \
         where \"UnitPrice\" > 20 order by \"ProductID\" limit 6",
        vec![
            "SortPreservingMergeExec: [ProductID@0 ASC NULLS LAST], fetch=6\n  SortExec: TopK(fetch=6), expr=[ProductID@0 ASC NULLS LAST], preserve_partitioning=[true]\n    FilterExec: UnitPrice@2 > 20.0000\n      RepartitionExec: partitioning=RoundRobinBatch(12), input_partitions=1\n        RemoteTableScanExec: source=query, projection=[ProductID, ProductName, UnitPrice]\n",
            "SortPreservingMergeExec: [ProductID@0 ASC NULLS LAST], fetch=6\n  SortExec: TopK(fetch=6), expr=[ProductID@0 ASC NULLS LAST], preserve_partitioning=[true]\n    FilterExec: UnitPrice@2 > 20.0000\n      RepartitionExec: partitioning=RoundRobinBatch(12), input_partitions=1\n        RemoteTableScanExec: source=Products, projection=[ProductID, ProductName, UnitPrice], filters=[(\"UnitPrice\" > 20.0000)]\n",
        ],
        r#"+-----------+---------------------------------+-----------+
| ProductID | ProductName                     | UnitPrice |
+-----------+---------------------------------+-----------+
| 4         | Chef Anton's Cajun Seasoning    | 22.0000   |
| 5         | Chef Anton's Gumbo Mix          | 21.3500   |
| 6         | Grandma's Boysenberry Spread    | 25.0000   |
| 7         | Uncle Bob's Organic Dried Pears | 30.0000   |
| 8         | Northwoods Cranberry Sauce      | 40.0000   |
| 9         | Mishi Kobe Niku                 | 97.0000   |
+-----------+---------------------------------+-----------+"#,
    )
    .await;
}

#[rstest::rstest]
#[case("SELECT * FROM Products WHERE UnitPrice > 20".into())]
#[case(vec!["Products"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn filter_on_currency_column_with_limit(#[case] source: RemoteSource) {
    // The predicate cannot be evaluated by the driver, so the limit must not be
    // pushed into the scan either: the scan would otherwise truncate the table
    // before the local filter removes the non-matching rows.
    assert_plan_and_result(
        RemoteDbType::Mdb,
        source,
        "select \"ProductID\", \"UnitPrice\" from remote_table \
         where \"UnitPrice\" > 20 order by \"ProductID\" limit 2",
        vec![
            "SortPreservingMergeExec: [ProductID@0 ASC NULLS LAST], fetch=2\n  SortExec: TopK(fetch=2), expr=[ProductID@0 ASC NULLS LAST], preserve_partitioning=[true]\n    FilterExec: UnitPrice@1 > 20.0000\n      RepartitionExec: partitioning=RoundRobinBatch(12), input_partitions=1\n        RemoteTableScanExec: source=query, projection=[ProductID, UnitPrice]\n",
            "SortPreservingMergeExec: [ProductID@0 ASC NULLS LAST], fetch=2\n  SortExec: TopK(fetch=2), expr=[ProductID@0 ASC NULLS LAST], preserve_partitioning=[true]\n    FilterExec: UnitPrice@1 > 20.0000\n      RepartitionExec: partitioning=RoundRobinBatch(12), input_partitions=1\n        RemoteTableScanExec: source=Products, projection=[ProductID, UnitPrice], filters=[(\"UnitPrice\" > 20.0000)]\n",
        ],
        r#"+-----------+-----------+
| ProductID | UnitPrice |
+-----------+-----------+
| 4         | 22.0000   |
| 5         | 21.3500   |
+-----------+-----------+"#,
    )
    .await;
}

#[rstest::rstest]
#[case("SELECT * FROM Products WHERE ProductName LIKE 'Ch%'".into())]
#[case(vec!["Products"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn filter_on_text_column(#[case] source: RemoteSource) {
    // Text comparisons are supported by mdb_test_sarg(), so the filter stays
    // pushed down (no local FilterExec) and results are still correct.
    assert_plan_and_result(
        RemoteDbType::Mdb,
        source,
        "select \"ProductID\", \"ProductName\" from remote_table \
         where \"ProductName\" like 'Ch%' order by \"ProductID\"",
        vec![
            "SortPreservingMergeExec: [ProductID@0 ASC NULLS LAST]\n  SortExec: expr=[ProductID@0 ASC NULLS LAST], preserve_partitioning=[true]\n    FilterExec: ProductName@1 LIKE Ch%\n      RepartitionExec: partitioning=RoundRobinBatch(12), input_partitions=1\n        RemoteTableScanExec: source=query, projection=[ProductID, ProductName]\n",
            "SortExec: expr=[ProductID@0 ASC NULLS LAST], preserve_partitioning=[false]\n  CooperativeExec\n    RemoteTableScanExec: source=Products, projection=[ProductID, ProductName], filters=[\"ProductName\" LIKE 'Ch%']\n",
        ],
        r#"+-----------+------------------------------+
| ProductID | ProductName                  |
+-----------+------------------------------+
| 1         | Chai                         |
| 2         | Chang                        |
| 4         | Chef Anton's Cajun Seasoning |
| 5         | Chef Anton's Gumbo Mix       |
| 39        | Chartreuse verte             |
| 48        | Chocolade                    |
+-----------+------------------------------+"#,
    )
    .await;
}

#[rstest::rstest]
#[case("SELECT * FROM Orders WHERE OrderID > 11070".into())]
#[case(vec!["Orders"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn filter_on_integer_column(#[case] source: RemoteSource) {
    // Integer comparisons are supported by mdb_test_sarg().
    assert_plan_and_result(
        RemoteDbType::Mdb,
        source,
        "select \"OrderID\", \"CustomerID\", \"ShipVia\" from remote_table \
         where \"OrderID\" > 11070 order by \"OrderID\"",
        vec![
            "SortPreservingMergeExec: [OrderID@0 ASC NULLS LAST]\n  SortExec: expr=[OrderID@0 ASC NULLS LAST], preserve_partitioning=[true]\n    FilterExec: OrderID@0 > 11070\n      RepartitionExec: partitioning=RoundRobinBatch(12), input_partitions=1\n        RemoteTableScanExec: source=query, projection=[OrderID, CustomerID, ShipVia]\n",
            "SortExec: expr=[OrderID@0 ASC NULLS LAST], preserve_partitioning=[false]\n  CooperativeExec\n    RemoteTableScanExec: source=Orders, projection=[OrderID, CustomerID, ShipVia], filters=[(\"OrderID\" > 11070)]\n",
        ],
        r#"+---------+------------+---------+
| OrderID | CustomerID | ShipVia |
+---------+------------+---------+
| 11071   | LILAS      | 1       |
| 11072   | ERNSH      | 2       |
| 11073   | PERIC      | 2       |
| 11074   | SIMOB      | 2       |
| 11075   | RICSU      | 2       |
| 11076   | BONAP      | 2       |
| 11077   | RATTC      | 2       |
+---------+------------+---------+"#,
    )
    .await;
}
