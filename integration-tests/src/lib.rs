pub mod cmd;
pub mod docker;
pub mod utils;

use datafusion_remote_table::RemoteDbType;

use crate::docker::DockerCompose;
use crate::utils::wait_container_ready;
use odbc_api::Environment;
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

static DM_DB: OnceLock<DockerCompose> = OnceLock::new();
pub async fn setup_dm_db() {
    let _ = DM_DB.get_or_init(|| {
        let compose =
            DockerCompose::new("dm", format!("{}/testdata/dm", env!("CARGO_MANIFEST_DIR")));
        compose.down();
        compose.up();
        compose
    });
    wait_container_ready(RemoteDbType::Dm).await;

    static DM_INIT: OnceLock<()> = OnceLock::new();
    let _ = DM_INIT.get_or_init(|| {
        let env = datafusion_remote_table::ODBC_ENV
            .get_or_init(|| Environment::new().expect("failed to create ODBC env"));
        let connection_str =
            "Driver={DM8 ODBC DRIVER};Server=localhost;TCP_Port=25236;UID=SYSDBA;PWD=Password123";
        let connection = env
            .connect_with_connection_string(connection_str, odbc_api::ConnectionOptions::default())
            .unwrap();
        connection.set_autocommit(true).unwrap();

        let sqls = include_str!("../testdata/dm/dm_init.sql").split(";");
        for sql in sqls {
            if let Err(e) = connection.execute(sql, (), None) {
                println!("Failed to exec sql {sql}, e: {e:?}")
            }
        }
    });
}
