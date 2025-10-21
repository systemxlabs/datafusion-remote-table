use datafusion_remote_table::{RemoteDbType, RemoteSource};
use integration_tests::setup_dm_db;
use integration_tests::utils::assert_result;

#[rstest::rstest]
#[case("SELECT * from supported_data_types".into())]
#[case(vec!["supported_data_types"].into())]
#[tokio::test(flavor = "multi_thread")]
pub async fn supported_dm_types(#[case] source: RemoteSource) {
    setup_dm_db().await;
    assert_result(RemoteDbType::Dm, source, "SELECT * FROM remote_table", 
        r#"+---------+-------------+--------------+-------------+------------+----------+-----------+------------+-------------+-------------+------------+-------------+----------+------------+---------------+------------+---------------------+--------------+------------+
| BIT_COL | TINYINT_COL | SMALLINT_COL | INTEGER_COL | BIGINT_COL | REAL_COL | FLOAT_COL | DOUBLE_COL | NUMERIC_COL | DECIMAL_COL | CHAR_COL   | VARCHAR_COL | TEXT_COL | BINARY_COL | VARBINARY_COL | IMAGE_COL  | TIMESTAMP_COL       | TIME_COL     | DATE_COL   |
+---------+-------------+--------------+-------------+------------+----------+-----------+------------+-------------+-------------+------------+-------------+----------+------------+---------------+------------+---------------------+--------------+------------+
| true    | 1           | 2            | 3           | 4          | 1.1      | 2.2       | 3.3        | 4.40        | 5.50        | char       | varchar     | text     | 01         | 00000002      | 696d616765 | 2002-12-12T09:10:21 | 09:10:21.200 | 2023-10-01 |
|         |             |              |             |            |          |           |            |             |             |            |             |          |            |               |            |                     |              |            |
+---------+-------------+--------------+-------------+------------+----------+-----------+------------+-------------+-------------+------------+-------------+----------+------------+---------------+------------+---------------------+--------------+------------+"#).await;
}
