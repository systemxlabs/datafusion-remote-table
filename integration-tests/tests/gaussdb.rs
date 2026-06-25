use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_remote_table::{
    ConnectionOptions, GaussDBType, RemoteDbType, RemoteField, RemoteSchema, RemoteSource,
    RemoteTable, RemoteType,
};
use integration_tests::setup_gaussdb_db;
use integration_tests::utils::{
    assert_plan_and_result, assert_result, assert_sqls, build_conn_options,
};
use std::sync::Arc;
use std::time::Duration;

fn public_table(name: &str) -> RemoteSource {
    RemoteSource::Table(vec!["public".to_string(), name.to_string()])
}

#[tokio::test(flavor = "multi_thread")]
pub async fn gaussdb_simple_query() {
    setup_gaussdb_db().await;

    let options = build_conn_options(RemoteDbType::GaussDB);
    let table = RemoteTable::try_new(options, "select 1 as id")
        .await
        .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table)).unwrap();

    let df = ctx.sql("SELECT * FROM remote_table").await.unwrap();
    let batches = df.collect().await.unwrap();
    let table_str = pretty_format_batches(&batches).unwrap().to_string();

    assert_eq!(
        table_str,
        r#"+----+
| id |
+----+
| 1  |
+----+"#,
    );
}

#[tokio::test(flavor = "multi_thread")]
pub async fn supported_gaussdb_types() {
    setup_gaussdb_db().await;

    assert_sqls(
        RemoteDbType::GaussDB,
        vec![
            public_table("supported_data_types"),
            RemoteSource::Query("SELECT * FROM supported_data_types".to_string()),
        ],
    )
    .await;
}

#[rstest::rstest]
#[case("SELECT * FROM timestamp_test".into())]
#[case(public_table("timestamp_test"))]
#[tokio::test(flavor = "multi_thread")]
pub async fn supported_gaussdb_timestamp_type(#[case] source: RemoteSource) {
    setup_gaussdb_db().await;

    assert_result(
        RemoteDbType::GaussDB,
        source,
        "SELECT * FROM remote_table",
        r#"+----------------------------+-----------------------------+---------------------+-------------------------+----------------------------+
| timestamp_col              | timestamptz_col             | timestamp_0         | timestamp_3             | timestamp_6                |
+----------------------------+-----------------------------+---------------------+-------------------------+----------------------------+
| 2023-10-27T12:34:56        | 2023-10-27T10:34:56Z        | 2023-10-27T12:34:56 | 2023-10-27T12:34:56.123 | 2023-10-27T12:34:56.123456 |
| 1969-07-20T20:17:40        | 1969-07-20T20:17:40Z        | 1969-07-20T20:17:40 | 1969-07-20T20:17:40.123 | 1969-07-20T20:17:40.123456 |
| 2030-12-31T23:59:59        | 2030-12-31T23:59:59Z        | 2030-12-31T23:59:59 | 2030-12-31T23:59:59.999 | 2030-12-31T23:59:59.999999 |
| 2023-10-27T12:34:56.876543 | 2023-10-27T12:34:56.876543Z | 2023-10-27T12:34:57 | 2023-10-27T12:34:56.877 | 2023-10-27T12:34:56.876543 |
| 0001-01-01T00:00:00        | 0001-01-01T00:00:00Z        | 0001-01-01T00:00:00 | 0001-01-01T00:00:00     | 0001-01-01T00:00:00        |
| 9999-12-31T23:59:59        | 9999-12-31T23:59:59Z        | 9999-12-31T23:59:59 | 9999-12-31T23:59:59.999 | 9999-12-31T23:59:59.999999 |
|                            |                             |                     |                         |                            |
+----------------------------+-----------------------------+---------------------+-------------------------+----------------------------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
pub async fn table_columns() {
    setup_gaussdb_db().await;

    let sql = format!(
        r#"
        SELECT
    a.attname AS column_name,
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
    CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable,
    t.typname AS udt_name
FROM
    pg_catalog.pg_attribute a
JOIN
    pg_catalog.pg_class c ON a.attrelid = c.oid
JOIN
    pg_catalog.pg_type t ON a.atttypid = t.oid
WHERE
    c.relname = '{}'
    AND c.relkind IN ('r', 'v', 'm')
    AND a.attnum > 0
    AND NOT a.attisdropped
ORDER BY
    a.attnum
        "#,
        "simple_table"
    );

    assert_result(
        RemoteDbType::GaussDB,
        &sql,
        "SELECT * FROM remote_table",
        r#"+-------------+------------------------+-------------+----------+
| column_name | data_type              | is_nullable | udt_name |
+-------------+------------------------+-------------+----------+
| id          | integer                | NO          | int4     |
| name        | character varying(255) | NO          | varchar  |
+-------------+------------------------+-------------+----------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
pub async fn various_sqls() {
    setup_gaussdb_db().await;

    assert_sqls(
        RemoteDbType::GaussDB,
        vec![
            "select * from pg_catalog.pg_stat_all_tables".into(),
            vec!["pg_catalog", "pg_stat_all_tables"].into(),
        ],
    )
    .await;
}

#[rstest::rstest]
#[case("SELECT * from simple_table".into())]
#[case(public_table("simple_table"))]
#[tokio::test(flavor = "multi_thread")]
async fn pushdown_limit(#[case] source: RemoteSource) {
    setup_gaussdb_db().await;

    assert_plan_and_result(
        RemoteDbType::GaussDB,
        source,
        "select * from remote_table limit 1",
        vec![
            "CooperativeExec\n  RemoteTableScanExec: source=query, limit=1\n",
            "CooperativeExec\n  RemoteTableScanExec: source=public.simple_table, limit=1\n",
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
#[case(public_table("simple_table"))]
#[tokio::test(flavor = "multi_thread")]
async fn pushdown_filters(#[case] source: RemoteSource) {
    setup_gaussdb_db().await;

    assert_plan_and_result(
        RemoteDbType::GaussDB,
        source,
        "select * from remote_table where id = 1",
        vec![
            "CooperativeExec\n  RemoteTableScanExec: source=query, filters=[(\"id\" = 1)]\n",
            "CooperativeExec\n  RemoteTableScanExec: source=public.simple_table, filters=[(\"id\" = 1)]\n",
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
#[case(public_table("simple_table"))]
#[tokio::test(flavor = "multi_thread")]
async fn count1_agg(#[case] source: RemoteSource) {
    setup_gaussdb_db().await;

    assert_plan_and_result(
        RemoteDbType::GaussDB,
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
        RemoteDbType::GaussDB,
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
        RemoteDbType::GaussDB,
        source,
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
#[case("SELECT * FROM supported_data_types".into())]
#[case(public_table("supported_data_types"))]
#[tokio::test(flavor = "multi_thread")]
pub async fn table_projection(#[case] source: RemoteSource) {
    setup_gaussdb_db().await;

    assert_result(
        RemoteDbType::GaussDB,
        source,
        "SELECT integer_col, varchar_col FROM remote_table",
        r#"+-------------+-------------+
| integer_col | varchar_col |
+-------------+-------------+
| 2           | varchar     |
|             |             |
+-------------+-------------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
pub async fn empty_projection() {
    setup_gaussdb_db().await;

    let options = build_conn_options(RemoteDbType::GaussDB);
    let table = RemoteTable::try_new(options, public_table("simple_table"))
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
    assert_eq!(
        plan_display,
        "CooperativeExec\n  RemoteTableScanExec: source=public.simple_table, projection=[]\n"
    );

    let result = collect(exec_plan, ctx.task_ctx()).await.unwrap();
    assert_eq!(result.len(), 1);
    let batch = &result[0];
    assert_eq!(batch.num_columns(), 0);
    assert_eq!(batch.num_rows(), 3);
}

#[rstest::rstest]
#[case("SELECT * from empty_table".into())]
#[case(public_table("empty_table"))]
#[tokio::test(flavor = "multi_thread")]
async fn empty_table(#[case] source: RemoteSource) {
    setup_gaussdb_db().await;

    assert_result(
        RemoteDbType::GaussDB,
        source,
        "SELECT * FROM remote_table",
        "++\n++",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
pub async fn insert_supported_gaussdb_types() {
    setup_gaussdb_db().await;

    assert_result(
        RemoteDbType::GaussDB,
        public_table("insert_supported_data_types"),
        "insert into remote_table values
        (1, 2, 3, 1.1, 2.2, 3.3, 'char', 'varchar', 'bpchar', 'text', X'01', '2023-10-01', '12:34:56', '2023-10-01 12:34:56', '2023-10-01 12:34:56+00', '3 months 2 weeks', true, '{\"key1\":\"value1\"}', '{\"key2\":\"value2\"}', [1, 2], [3, 4], [5, 6], [1.1, 2.2], [3.3, 4.4], ['varchar0', 'varchar1'], ['text0', 'text1'], [true, false], X'a0eebc999c0b4ef8bb6d6bb9bd380a11'),
        (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
        r#"+-------+
| count |
+-------+
| 2     |
+-------+"#,
    )
    .await;

    assert_result(
        RemoteDbType::GaussDB,
        public_table("insert_supported_data_types"),
        "SELECT * FROM remote_table",
        r#"+--------------+-------------+------------+----------+------------+-------------+------------+-------------+------------+----------+-----------+------------+----------+---------------------+----------------------+----------------+-------------+-------------------+-------------------+--------------------+-------------------+------------------+----------------+------------------+----------------------+----------------+----------------+----------------------------------+
| smallint_col | integer_col | bigint_col | real_col | double_col | numeric_col | char_col   | varchar_col | bpchar_col | text_col | bytea_col | date_col   | time_col | timestamp_col       | timestamptz_col      | interval_col   | boolean_col | json_col          | jsonb_col         | smallint_array_col | integer_array_col | bigint_array_col | real_array_col | double_array_col | varchar_array_col    | text_array_col | bool_array_col | uuid_col                         |
+--------------+-------------+------------+----------+------------+-------------+------------+-------------+------------+----------+-----------+------------+----------+---------------------+----------------------+----------------+-------------+-------------------+-------------------+--------------------+-------------------+------------------+----------------+------------------+----------------------+----------------+----------------+----------------------------------+
| 1            | 2           | 3          | 1.1      | 2.2        | 3.30        | char       | varchar     | bpchar     | text     | 01        | 2023-10-01 | 12:34:56 | 2023-10-01T12:34:56 | 2023-10-01T12:34:56Z | 3 mons 14 days | true        | {"key1":"value1"} | {"key2":"value2"} | [1, 2]             | [3, 4]            | [5, 6]           | [1.1, 2.2]     | [3.3, 4.4]       | [varchar0, varchar1] | [text0, text1] | [true, false]  | a0eebc999c0b4ef8bb6d6bb9bd380a11 |
|              |             |            |          |            |             |            |             |            |          |           |            |          |                     |                      |                |             |                   |                   |                    |                   |                  |                |                  |                      |                |                |                                  |
+--------------+-------------+------------+----------+------------+-------------+------------+-------------+------------+----------+-----------+------------+----------+---------------------+----------------------+----------------+-------------+-------------------+-------------------+--------------------+-------------------+------------------+----------------+------------------+----------------------+----------------+----------------+----------------------------------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
pub async fn insert_table_with_primary_key() {
    setup_gaussdb_db().await;

    let options = build_conn_options(RemoteDbType::GaussDB);
    let remote_schema = Arc::new(RemoteSchema::new(vec![
        RemoteField::new("id", RemoteType::GaussDB(GaussDBType::Int4), false)
            .with_auto_increment(true),
        RemoteField::new("name", RemoteType::GaussDB(GaussDBType::Varchar), true),
    ]));
    let table = RemoteTable::try_new_with_remote_schema(
        options,
        public_table("insert_table_with_primary_key"),
        remote_schema,
    )
    .await
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table)).unwrap();

    let df = ctx
        .sql("insert into remote_table (name) values ('John')")
        .await
        .unwrap();
    let exec_plan = df.create_physical_plan().await.unwrap();

    let result = collect(exec_plan, ctx.task_ctx()).await.unwrap();
    assert_eq!(
        pretty_format_batches(&result).unwrap().to_string(),
        r#"+-------+
| count |
+-------+
| 1     |
+-------+"#,
    );

    assert_result(
        RemoteDbType::GaussDB,
        public_table("insert_table_with_primary_key"),
        "SELECT * FROM remote_table",
        r#"+----+------+
| id | name |
+----+------+
| 1  | John |
+----+------+"#,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
pub async fn disable_pooled_connections() {
    setup_gaussdb_db().await;

    let options = build_conn_options(RemoteDbType::GaussDB);
    let ConnectionOptions::GaussDB(options) = options else {
        unreachable!()
    };
    let options = options
        .with_pool_max_size(100)
        .with_pool_min_idle(0)
        .with_pool_idle_timeout(Duration::from_micros(1))
        .with_pool_ttl_check_interval(Duration::from_secs(3));
    let table = RemoteTable::try_new(options, "select * from simple_table")
        .await
        .unwrap();
    let pool = table.pool().await.unwrap().clone();
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
