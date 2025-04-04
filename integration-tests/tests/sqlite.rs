use integration_tests::utils::{assert_plan_and_result, assert_result};

#[tokio::test]
pub async fn supported_sqlite_types() {
    assert_result(
        "sqlite",
        "select * from supported_data_types",
        "select * from remote_table",
        r#"+----------+-------------+--------------+---------+------------+-----------+------------+----------+----------+-------------+----------+------------+---------------+----------+
| null_col | tinyint_col | smallint_col | int_col | bigint_col | float_col | double_col | real_col | char_col | varchar_col | text_col | binary_col | varbinary_col | blob_col |
+----------+-------------+--------------+---------+------------+-----------+------------+----------+----------+-------------+----------+------------+---------------+----------+
|          | 1           | 2            | 3       | 4          | 1.1       | 2.2        | 3.3      | char     | varchar     | text     | 01         | 02            | 03       |
|          |             |              |         |            |           |            |          |          |             |          |            |               |          |
+----------+-------------+--------------+---------+------------+-----------+------------+----------+----------+-------------+----------+------------+---------------+----------+"#,
    )
    .await;
}

#[tokio::test]
async fn pushdown_limit() {
    assert_plan_and_result(
        "sqlite",
        "select * from simple_table",
        "select * from remote_table limit 1",
        "RemoteTableExec: limit=Some(1), filters=[]\n",
        r#"+----+------+
| id | name |
+----+------+
| 1  | Tom  |
+----+------+"#,
    )
    .await;
}

#[tokio::test]
async fn pushdown_filters() {
    assert_plan_and_result(
        "sqlite",
        "select * from simple_table",
        "select * from remote_table where id = 1",
        "RemoteTableExec: limit=None, filters=[id = Int64(1)]\n",
        r#"+----+------+
| id | name |
+----+------+
| 1  | Tom  |
+----+------+"#,
    )
    .await;
}
