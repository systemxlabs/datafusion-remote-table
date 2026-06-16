pub mod cmd;
pub mod docker;
pub mod utils;

use datafusion_remote_table::RemoteDbType;

use crate::docker::DockerCompose;
use crate::utils::wait_container_ready;
use odbc_api::Environment;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

static ENV_LOGGER: OnceLock<()> = OnceLock::new();
pub fn init_env_logger() {
    unsafe {
        std::env::set_var("RUST_LOG", "info,datafusion_remote_table=debug");
    }
    ENV_LOGGER.get_or_init(|| {
        env_logger::init();
    });
}

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

static MDB_DB: OnceLock<PathBuf> = OnceLock::new();

/// Returns the path to the MDB test database file.
///
/// Downloads `data/nwind.mdb` (3,002,368 bytes — the canonical Microsoft
/// Northwind sample database) from the mdbtools/mdbtestdata repo at a pinned
/// commit. The file is cached at `target/nwind.mdb`; subsequent invocations
/// reuse the cached copy when its size matches `EXPECTED_SIZE`.
///
/// mdbtools' own test suite runs against this exact fixture, so compatibility
/// with the `MDBTools` ODBC driver is guaranteed.
pub fn setup_mdb() -> &'static Path {
    const URL: &str = "https://raw.githubusercontent.com/mdbtools/mdbtestdata/\
        5ebf2d685ec628df72f4774b78abee96a866b837/data/nwind.mdb";
    const EXPECTED_SIZE: u64 = 3_002_368;

    MDB_DB.get_or_init(|| {
        let db_path = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../target/nwind.mdb"));
        let needs_download = std::fs::metadata(&db_path)
            .map(|m| m.len() != EXPECTED_SIZE)
            .unwrap_or(true);
        if needs_download {
            let status = std::process::Command::new("curl")
                .args(["-fsSL", "--retry", "3", "-o"])
                .arg(&db_path)
                .arg(URL)
                .status()
                .expect("failed to invoke curl to fetch nwind.mdb (is curl installed?)");
            if !status.success() {
                panic!("Failed to download MDB fixture from {URL}");
            }
        }
        db_path
    })
}

#[cfg(feature = "opengauss")]
static OPENGAUSS_DB: OnceLock<DockerCompose> = OnceLock::new();
#[cfg(feature = "opengauss")]
pub async fn setup_opengauss_db() {
    let _ = OPENGAUSS_DB.get_or_init(|| {
        let compose = DockerCompose::new(
            "opengauss",
            format!("{}/testdata/opengauss", env!("CARGO_MANIFEST_DIR")),
        );
        compose.down();
        compose.up();
        compose
    });
    wait_container_ready(RemoteDbType::OpenGauss).await;
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
