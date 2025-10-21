use datafusion_remote_table::RemoteSource;

#[rstest::rstest]
#[case("SELECT * from supported_data_types".into())]
#[case(vec!["supported_data_types"].into())]
#[tokio::test(flavor = "multi_thread")]
pub async fn supported_mysql_types(#[case] source: RemoteSource) {}
