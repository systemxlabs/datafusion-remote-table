use datafusion::prelude::SessionContext;
use datafusion_remote_table::{PostgresConnectionOptions, RemoteTable};
use std::sync::Arc;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = PostgresConnectionOptions::new("localhost", 5432, "user", "password");
    let remote_table = RemoteTable::try_new(options, "select * from supported_data_types").await?;

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(remote_table))?;

    ctx.sql("select * from remote_table").await?.show().await?;

    Ok(())
}
