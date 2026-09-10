//! Concurrent reads over the bundled ESRI personal geodatabase fixture.
//!
//! mdbtools' `libmdbodbc.so` keeps process-global state and the crate funnels
//! every `MdbPool` sharing a connection identity through one cached ODBC
//! connection, so these tests read several layers at once to pin that down.
//! `testdata/mdb/esri_layers.mdb` holds `公路编号` (47 rows) and `境界线`
//! (242 rows).

use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_remote_table::{ConnectionOptions, MdbConnectionOptions, RemoteSource, RemoteTable};
use integration_tests::setup_mdb_layers;
use std::sync::Arc;

const ROAD_NUMBERS: &str = "公路编号";
const ROAD_NUMBERS_ROWS: usize = 47;
const BOUNDARIES: &str = "境界线";
const BOUNDARIES_ROWS: usize = 242;

/// A connection identity distinct per `client`, so callers below that need the
/// driver to connect more than once miss the process-global connection cache.
fn options(client: usize) -> MdbConnectionOptions {
    MdbConnectionOptions::new(setup_mdb_layers().to_path_buf())
        .with_extra_params(vec![("Client".to_string(), client.to_string())])
}

async fn query_layer(layer: &str, client: usize) -> Result<usize, String> {
    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);
    let remote_table = RemoteTable::try_new(options(client), RemoteSource::from(vec![layer]))
        .await
        .map_err(|e| format!("try_new({layer}): {e}"))?;
    ctx.register_table("remote_table", Arc::new(remote_table))
        .map_err(|e| format!("register({layer}): {e}"))?;
    let df = ctx
        .sql("select * from remote_table")
        .await
        .map_err(|e| format!("sql({layer}): {e}"))?;
    let batches = df
        .collect()
        .await
        .map_err(|e| format!("collect({layer}): {e}"))?;
    Ok(batches.iter().map(|b| b.num_rows()).sum())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn sequential_layers() {
    for (layer, expected) in [
        (ROAD_NUMBERS, ROAD_NUMBERS_ROWS),
        (BOUNDARIES, BOUNDARIES_ROWS),
    ] {
        assert_eq!(
            query_layer(layer, 0).await.unwrap(),
            expected,
            "layer {layer}"
        );
    }
}

/// Each task uses its own connection identity, so this drives several
/// `SQLDriverConnect` calls at once rather than sharing one cached connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_layers() {
    let expected = [
        (ROAD_NUMBERS, ROAD_NUMBERS_ROWS),
        (BOUNDARIES, BOUNDARIES_ROWS),
    ];
    let mut handles = vec![];
    for client in 1..=12 {
        let (layer, rows) = expected[client % expected.len()];
        handles.push(tokio::spawn(async move {
            (layer, rows, query_layer(layer, client).await)
        }));
    }
    for handle in handles {
        let (layer, expected_rows, result) = handle.await.unwrap();
        assert_eq!(result.unwrap(), expected_rows, "layer {layer}");
    }
}

/// Several tasks reading one shared `RemoteTable`. `RemoteTableScanExec` is
/// single-partition, so the concurrency here is across the `collect` calls
/// contending for the one cached connection, not between scan partitions.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_same_layer() {
    let remote_table = RemoteTable::try_new(options(0), RemoteSource::from(vec![BOUNDARIES]))
        .await
        .unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(remote_table))
        .unwrap();
    let ctx = Arc::new(ctx);

    let mut handles = vec![];
    for _ in 0..8 {
        let ctx = ctx.clone();
        handles.push(tokio::spawn(async move {
            let df = ctx
                .sql(r#"select "OBJECTID" from remote_table"#)
                .await
                .unwrap();
            let batches = collect(df.create_physical_plan().await.unwrap(), ctx.task_ctx())
                .await
                .unwrap();
            batches.iter().map(|b| b.num_rows()).sum::<usize>()
        }));
    }
    for handle in handles {
        assert_eq!(handle.await.unwrap(), BOUNDARIES_ROWS);
    }
}

/// One `Pool::get` per distinct connection identity, so the driver connects 40
/// times instead of hitting the cache after the first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_pool_get() {
    for client in 0..40 {
        let options = ConnectionOptions::Mdb(options(client));
        let pool = datafusion_remote_table::connect(&options).await.unwrap();
        pool.get()
            .await
            .unwrap_or_else(|e| panic!("get#{client}: {e}"));
    }
}
