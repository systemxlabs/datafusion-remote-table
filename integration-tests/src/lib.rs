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

static ACCDB_DB: OnceLock<PathBuf> = OnceLock::new();

/// Returns the path to the ACCDB (Access 2007+ / ACE engine) test database file.
///
/// Downloads `data/ASampleDatabase.accdb` (544,768 bytes) from the same pinned
/// commit of the mdbtools/mdbtestdata repo used by [`setup_mdb`]. The file is
/// cached at `target/ASampleDatabase.accdb`; subsequent invocations reuse the
/// cached copy when its size matches `EXPECTED_SIZE`.
///
/// mdbtools' own test suite exercises this file through the `MDBTools` ODBC
/// driver, so it covers the ACE code path that `nwind.mdb` (JET3) does not.
pub fn setup_accdb() -> &'static Path {
    const URL: &str = "https://raw.githubusercontent.com/mdbtools/mdbtestdata/\
        5ebf2d685ec628df72f4774b78abee96a866b837/data/ASampleDatabase.accdb";
    const EXPECTED_SIZE: u64 = 544_768;

    ACCDB_DB.get_or_init(|| {
        let db_path = PathBuf::from(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../target/ASampleDatabase.accdb"
        ));
        let needs_download = std::fs::metadata(&db_path)
            .map(|m| m.len() != EXPECTED_SIZE)
            .unwrap_or(true);
        if needs_download {
            let status = std::process::Command::new("curl")
                .args(["-fsSL", "--retry", "3", "-o"])
                .arg(&db_path)
                .arg(URL)
                .status()
                .expect(
                    "failed to invoke curl to fetch ASampleDatabase.accdb (is curl installed?)",
                );
            if !status.success() {
                panic!("Failed to download ACCDB fixture from {URL}");
            }
        }
        db_path
    })
}

static GAUSSDB_DB: OnceLock<DockerCompose> = OnceLock::new();
pub async fn setup_gaussdb_db() {
    let _ = GAUSSDB_DB.get_or_init(|| {
        let compose = DockerCompose::new(
            "gaussdb",
            format!("{}/testdata/gaussdb", env!("CARGO_MANIFEST_DIR")),
        );
        compose.down();
        compose.up();
        compose
    });
    wait_container_ready(RemoteDbType::GaussDB).await;
    wait_gaussdb_init_done().await;
}

/// openGauss accepts connections while the init scripts are still running, so
/// `select 1` is not enough to know that the test tables exist yet. Wait until
/// the last table created by `opengauss_init.sql` is visible.
async fn wait_gaussdb_init_done() {
    let conn = utils::build_conn_options(RemoteDbType::GaussDB);
    let mut retry = 0;
    loop {
        match datafusion_remote_table::RemoteTable::try_new(
            conn.clone(),
            vec!["unconstrained_numeric"],
        )
        .await
        {
            Ok(table)
                if table
                    .remote_schema()
                    .is_some_and(|schema| !schema.fields.is_empty()) =>
            {
                break;
            }
            Ok(_) => eprintln!("gaussdb init not done: test tables not created yet"),
            Err(err) => eprintln!("gaussdb init check error: {err:?}"),
        }
        retry += 1;
        if retry > 60 {
            panic!("gaussdb test tables are still not available after 300 seconds");
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
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

/// MongoDB test connection string. The compose file creates the root user and
/// the `test` database, seeds it from `testdata/mongodb/mongodb_init.js`, and
/// its healthcheck makes `docker compose up --wait` block until the server
/// answers.
pub const MONGODB_URI: &str = "mongodb://root:password@127.0.0.1:27017/?authSource=admin";
pub const MONGODB_DATABASE: &str = "test";
/// Must match the published port in `testdata/mongodb/docker-compose.yaml`.
const MONGODB_ADDR: &str = "127.0.0.1:27017";

static MONGODB_DB: OnceLock<DockerCompose> = OnceLock::new();

pub async fn setup_mongodb_db() {
    let _ = MONGODB_DB.get_or_init(|| {
        let compose = DockerCompose::new(
            "mongodb",
            format!("{}/testdata/mongodb", env!("CARGO_MANIFEST_DIR")),
        );
        compose.down();
        compose.up();
        compose
    });
    wait_mongodb_listening().await;
}

/// The entrypoint answers its healthcheck from a temporary server that it later
/// replaces with the real one, so `up --wait` can return before anything is
/// listening for the tests. Only the real server publishes the port, so wait
/// for that instead of trusting "healthy" on its own.
async fn wait_mongodb_listening() {
    for _ in 0..60 {
        if std::net::TcpStream::connect(MONGODB_ADDR).is_ok() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    panic!("mongodb never started listening on {MONGODB_ADDR}");
}
