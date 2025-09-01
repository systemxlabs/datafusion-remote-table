pub mod cmd;
pub mod docker;
pub mod utils;

use datafusion_remote_table::RemoteDbType;

use crate::docker::DockerCompose;
use crate::utils::wait_container_ready;
use std::path::PathBuf;
use std::sync::OnceLock;

static POSTGRES_DB: OnceLock<DockerCompose> = OnceLock::new();
pub async fn setup_postgres_db() {
    let _ = POSTGRES_DB.get_or_init(|| {
        let compose = DockerCompose::new(
            "postgres",
            format!("{}/testdata/postgres", env!("CARGO_MANIFEST_DIR")),
        );
        compose.down();
        compose.up();
        compose
    });
    wait_container_ready(RemoteDbType::Postgres).await;
}

static MYSQL_DB: OnceLock<DockerCompose> = OnceLock::new();
pub async fn setup_mysql_db() {
    let _ = MYSQL_DB.get_or_init(|| {
        let compose = DockerCompose::new(
            "mysql",
            format!("{}/testdata/mysql", env!("CARGO_MANIFEST_DIR")),
        );
        compose.down();
        compose.up();
        compose
    });
    wait_container_ready(RemoteDbType::Mysql).await;
}

static ORACLE_DB: OnceLock<DockerCompose> = OnceLock::new();
pub async fn setup_oracle_db() {
    let _ = ORACLE_DB.get_or_init(|| {
        let compose = DockerCompose::new(
            "oracle",
            format!("{}/testdata/oracle", env!("CARGO_MANIFEST_DIR")),
        );
        compose.down();
        compose.up();
        compose
    });
    wait_container_ready(RemoteDbType::Oracle).await;
}

static SQLITE_DB: OnceLock<PathBuf> = OnceLock::new();
pub fn setup_sqlite_db() -> &'static PathBuf {
    SQLITE_DB.get_or_init(|| {
        let tmpdir = std::env::temp_dir();
        let db_path = tmpdir.join(uuid::Uuid::new_v4().to_string());
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(include_str!("../testdata/sqlite_init.sql"))
            .unwrap();
        db_path
    })
}
