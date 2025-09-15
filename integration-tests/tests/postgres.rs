use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_remote_table::{
    PostgresType, RemoteDbType, RemoteField, RemoteSchema, RemoteSource, RemoteTable, RemoteType,
};
use integration_tests::setup_postgres_db;
use integration_tests::utils::{
    assert_plan_and_result, assert_result, assert_sqls, build_conn_options,
};
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread")]
pub async fn supported_postgres_types() {
    setup_postgres_db().await;
    assert_result(
        RemoteDbType::Postgres,
        "SELECT * FROM supported_data_types",
        "SELECT * FROM remote_table",
        r#"+--------------+-------------+------------+----------+------------+--------------+------------+-------------+------------+----------+-----------+------------+----------+----------------+-------------+-------------------+-------------------+----------------------------------------------------+--------------------+-------------------+------------------+----------------+------------------+--------------------------+----------------------+--------------------+----------------+----------------+----------------+----------------------------------+
| smallint_col | integer_col | bigint_col | real_col | double_col | numeric_col  | char_col   | varchar_col | bpchar_col | text_col | bytea_col | date_col   | time_col | interval_col   | boolean_col | json_col          | jsonb_col         | geometry_col                                       | smallint_array_col | integer_array_col | bigint_array_col | real_array_col | double_array_col | char_array_col           | varchar_array_col    | bpchar_array_col   | text_array_col | bool_array_col | xml_col        | uuid_col                         |
+--------------+-------------+------------+----------+------------+--------------+------------+-------------+------------+----------+-----------+------------+----------+----------------+-------------+-------------------+-------------------+----------------------------------------------------+--------------------+-------------------+------------------+----------------+------------------+--------------------------+----------------------+--------------------+----------------+----------------+----------------+----------------------------------+
| 1            | 2           | 3          | 1.1      | 2.2        | 3.3000000000 | char       | varchar     | bpchar     | text     | deadbeef  | 2023-10-01 | 12:34:56 | 3 mons 14 days | true        | {"key1":"value1"} | {"key2":"value2"} | 010100002038010000000000000000f03f000000000000f03f | [1, 2]             | [3, 4]            | [5, 6]           | [1.1, 2.2]     | [3.3, 4.4]       | [char0     , char1     ] | [varchar0, varchar1] | [bpchar0, bpchar1] | [text0, text1] | [true, false]  | <item>1</item> | a0eebc999c0b4ef8bb6d6bb9bd380a11 |
|              |             |            |          |            |              |            |             |            |          |           |            |          |                |             |                   |                   |                                                    |                    |                   |                  |                |                  |                          |                      |                    |                |                |                |                                  |
+--------------+-------------+------------+----------+------------+--------------+------------+-------------+------------+----------+-----------+------------+----------+----------------+-------------+-------------------+-------------------+----------------------------------------------------+--------------------+-------------------+------------------+----------------+------------------+--------------------------+----------------------+--------------------+----------------+----------------+----------------+----------------------------------+"#,
   ).await;

    assert_result(
        RemoteDbType::Postgres,
        vec!["supported_data_types"],
        "SELECT * FROM remote_table",
        r#"+--------------+-------------+------------+----------+------------+-------------+------------+-------------+------------+----------+-----------+------------+----------+----------------+-------------+-------------------+-------------------+----------------------------------------------------+--------------------+-------------------+------------------+----------------+------------------+--------------------------+----------------------+--------------------+----------------+----------------+----------------+----------------------------------+
| smallint_col | integer_col | bigint_col | real_col | double_col | numeric_col | char_col   | varchar_col | bpchar_col | text_col | bytea_col | date_col   | time_col | interval_col   | boolean_col | json_col          | jsonb_col         | geometry_col                                       | smallint_array_col | integer_array_col | bigint_array_col | real_array_col | double_array_col | char_array_col           | varchar_array_col    | bpchar_array_col   | text_array_col | bool_array_col | xml_col        | uuid_col                         |
+--------------+-------------+------------+----------+------------+-------------+------------+-------------+------------+----------+-----------+------------+----------+----------------+-------------+-------------------+-------------------+----------------------------------------------------+--------------------+-------------------+------------------+----------------+------------------+--------------------------+----------------------+--------------------+----------------+----------------+----------------+----------------------------------+
| 1            | 2           | 3          | 1.1      | 2.2        | 3.30        | char       | varchar     | bpchar     | text     | deadbeef  | 2023-10-01 | 12:34:56 | 3 mons 14 days | true        | {"key1":"value1"} | {"key2":"value2"} | 010100002038010000000000000000f03f000000000000f03f | [1, 2]             | [3, 4]            | [5, 6]           | [1.1, 2.2]     | [3.3, 4.4]       | [char0     , char1     ] | [varchar0, varchar1] | [bpchar0, bpchar1] | [text0, text1] | [true, false]  | <item>1</item> | a0eebc999c0b4ef8bb6d6bb9bd380a11 |
|              |             |            |          |            |             |            |             |            |          |           |            |          |                |             |                   |                   |                                                    |                    |                   |                  |                |                  |                          |                      |                    |                |                |                |                                  |
+--------------+-------------+------------+----------+------------+-------------+------------+-------------+------------+----------+-----------+------------+----------+----------------+-------------+-------------------+-------------------+----------------------------------------------------+--------------------+-------------------+------------------+----------------+------------------+--------------------------+----------------------+--------------------+----------------+----------------+----------------+----------------------------------+"#,
   ).await;
}

#[rstest::rstest]
#[case("SELECT * FROM timestamp_test".into())]
#[case(vec!["timestamp_test"].into())]
#[tokio::test(flavor = "multi_thread")]
pub async fn supported_postgres_timestamp_type(#[case] source: RemoteSource) {
    setup_postgres_db().await;
    assert_result(
        RemoteDbType::Postgres,
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
    setup_postgres_db().await;
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
        RemoteDbType::Postgres,
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
    setup_postgres_db().await;

    assert_sqls(
        RemoteDbType::Postgres,
        vec![
            "select * from pg_catalog.pg_stat_all_tables".into(),
            vec!["pg_catalog", "pg_stat_all_tables"].into(),
        ],
    )
    .await;
}

#[rstest::rstest]
#[case("SELECT * from simple_table".into())]
#[case(vec!["simple_table"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn pushdown_limit(#[case] source: RemoteSource) {
    setup_postgres_db().await;
    assert_plan_and_result(
        RemoteDbType::Postgres,
        source,
        "select * from remote_table limit 1",
        vec![
            "RemoteTableExec: source=query, limit=1\n",
            "RemoteTableExec: source=simple_table, limit=1\n",
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
#[case(vec!["simple_table"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn pushdown_filters(#[case] source: RemoteSource) {
    setup_postgres_db().await;
    assert_plan_and_result(
        RemoteDbType::Postgres,
        source,
        "select * from remote_table where id = 1",
        vec![
            "RemoteTableExec: source=query, filters=[(\"id\" = 1)]\n",
            "RemoteTableExec: source=simple_table, filters=[(\"id\" = 1)]\n",
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
#[case(vec!["simple_table"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn count1_agg(#[case] source: RemoteSource) {
    setup_postgres_db().await;
    assert_plan_and_result(
        RemoteDbType::Postgres,
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
        RemoteDbType::Postgres,
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
        RemoteDbType::Postgres,
        source.clone(),
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
#[case("SELECT * from supported_data_types".into())]
#[case(vec!["supported_data_types"].into())]
#[tokio::test(flavor = "multi_thread")]
pub async fn table_projection(#[case] source: RemoteSource) {
    setup_postgres_db().await;
    assert_result(
        RemoteDbType::Postgres,
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
async fn empty_projection() {
    setup_postgres_db().await;

    let options = build_conn_options(RemoteDbType::Postgres);
    let table = RemoteTable::try_new(options, "select * from simple_table")
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
    assert_eq!(
        plan_display,
        "RemoteTableExec: source=query, projection=[]\n"
    );

    let result = collect(exec_plan, ctx.task_ctx()).await.unwrap();
    assert_eq!(result.len(), 1);
    let batch = &result[0];
    assert_eq!(batch.num_columns(), 0);
    assert_eq!(batch.num_rows(), 3);
}

#[rstest::rstest]
#[case("SELECT * FROM unconstrained_numeric".into())]
#[case(vec!["unconstrained_numeric"].into())]
#[tokio::test(flavor = "multi_thread")]
pub async fn unconstrained_numeric(#[case] source: RemoteSource) {
    setup_postgres_db().await;

    assert_result(
        RemoteDbType::Postgres,
        source.clone(),
        "SELECT * FROM remote_table",
        r#"+------------------------+
| numeric_col            |
+------------------------+
| 1.1000000000           |
| 12.1200000000          |
| 123.1230000000         |
| 12345678901.1234567890 |
+------------------------+"#,
    )
    .await;

    let options = build_conn_options(RemoteDbType::Postgres);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "numeric_col",
        DataType::Decimal128(38, 5),
        true,
    )]));
    let table = RemoteTable::try_new_with_schema(options, source, schema)
        .await
        .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table)).unwrap();
    let df = ctx.sql("SELECT * FROM remote_table").await.unwrap();
    let batches = df.collect().await.unwrap();
    let table_str = pretty_format_batches(&batches).unwrap().to_string();
    println!("{}", table_str);
    assert_eq!(
        table_str,
        r#"+-------------------+
| numeric_col       |
+-------------------+
| 1.10000           |
| 12.12000          |
| 123.12300         |
| 12345678901.12345 |
+-------------------+"#,
    );
}

#[tokio::test(flavor = "multi_thread")]
pub async fn insert_supported_postgres_types() {
    setup_postgres_db().await;
    assert_result(RemoteDbType::Postgres, vec!["insert_supported_data_types"],
    "insert into remote_table values
        (1, 2, 3, 1.1, 2.2, 3.3, 'char', 'varchar', 'bpchar', 'text', X'01', '2023-10-01', '12:34:56', '2023-10-01 12:34:56', '2023-10-01 12:34:56+00', '3 months 2 weeks', true, '{\"key1\":\"value1\"}', '{\"key2\":\"value2\"}', X'010100002038010000000000000000f03f000000000000f03f', [1, 2], [3, 4], [5, 6], [1.1, 2.2], [3.3, 4.4], ['char0', 'char1'], ['varchar0', 'varchar1'], ['bpchar0', 'bpchar1'], ['text0', 'text1'], [true, false], '<item>1</item>', X'a0eebc999c0b4ef8bb6d6bb9bd380a11'),
        (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
        r#"+-------+
| count |
+-------+
| 2     |
+-------+"#,
).await;

    assert_result(RemoteDbType::Postgres, vec!["insert_supported_data_types"], "SELECT * FROM remote_table",
    r#"+--------------+-------------+------------+----------+------------+-------------+------------+-------------+------------+----------+-----------+------------+----------+---------------------+----------------------+----------------+-------------+-------------------+-------------------+----------------------------------------------------+--------------------+-------------------+------------------+----------------+------------------+--------------------------+----------------------+--------------------+----------------+----------------+----------------+----------------------------------+
| smallint_col | integer_col | bigint_col | real_col | double_col | numeric_col | char_col   | varchar_col | bpchar_col | text_col | bytea_col | date_col   | time_col | timestamp_col       | timestamptz_col      | interval_col   | boolean_col | json_col          | jsonb_col         | geometry_col                                       | smallint_array_col | integer_array_col | bigint_array_col | real_array_col | double_array_col | char_array_col           | varchar_array_col    | bpchar_array_col   | text_array_col | bool_array_col | xml_col        | uuid_col                         |
+--------------+-------------+------------+----------+------------+-------------+------------+-------------+------------+----------+-----------+------------+----------+---------------------+----------------------+----------------+-------------+-------------------+-------------------+----------------------------------------------------+--------------------+-------------------+------------------+----------------+------------------+--------------------------+----------------------+--------------------+----------------+----------------+----------------+----------------------------------+
| 1            | 2           | 3          | 1.1      | 2.2        | 3.30        | char       | varchar     | bpchar     | text     | 01        | 2023-10-01 | 12:34:56 | 2023-10-01T12:34:56 | 2023-10-01T12:34:56Z | 3 mons 14 days | true        | {"key1":"value1"} | {"key2":"value2"} | 010100002038010000000000000000f03f000000000000f03f | [1, 2]             | [3, 4]            | [5, 6]           | [1.1, 2.2]     | [3.3, 4.4]       | [char0     , char1     ] | [varchar0, varchar1] | [bpchar0, bpchar1] | [text0, text1] | [true, false]  | <item>1</item> | a0eebc999c0b4ef8bb6d6bb9bd380a11 |
|              |             |            |          |            |             |            |             |            |          |           |            |          |                     |                      |                |             |                   |                   |                                                    |                    |                   |                  |                |                  |                          |                      |                    |                |                |                |                                  |
+--------------+-------------+------------+----------+------------+-------------+------------+-------------+------------+----------+-----------+------------+----------+---------------------+----------------------+----------------+-------------+-------------------+-------------------+----------------------------------------------------+--------------------+-------------------+------------------+----------------+------------------+--------------------------+----------------------+--------------------+----------------+----------------+----------------+----------------------------------+"#,
    ).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
pub async fn insert_table_with_primary_key() {
    setup_postgres_db().await;

    let options = build_conn_options(RemoteDbType::Postgres);
    let remote_schema = Arc::new(RemoteSchema::new(vec![
        RemoteField::new("id", RemoteType::Postgres(PostgresType::Int4), false)
            .with_auto_increment(true),
        RemoteField::new("name", RemoteType::Postgres(PostgresType::Varchar), true),
    ]));
    let table = RemoteTable::try_new_with_remote_schema(
        options,
        vec!["insert_table_with_primary_key"],
        remote_schema,
    )
    .await
    .unwrap();
    println!("remote schema: {:#?}", table.remote_schema());

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table)).unwrap();

    let df = ctx
        .sql("insert into remote_table (name) values ('John')")
        .await
        .unwrap();
    let exec_plan = df.create_physical_plan().await.unwrap();
    println!(
        "{}",
        DisplayableExecutionPlan::new(exec_plan.as_ref()).indent(true)
    );

    let result = collect(exec_plan, ctx.task_ctx()).await.unwrap();
    println!("{}", pretty_format_batches(&result).unwrap());

    assert_eq!(
        pretty_format_batches(&result).unwrap().to_string(),
        r#"+-------+
| count |
+-------+
| 1     |
+-------+"#,
    );

    assert_result(
        RemoteDbType::Postgres,
        vec!["insert_table_with_primary_key"],
        "SELECT * FROM remote_table",
        r#"+----+------+
| id | name |
+----+------+
| 1  | John |
+----+------+"#,
    )
    .await;
}
