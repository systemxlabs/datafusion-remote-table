use crate::RemoteDbType;
use derive_getters::Getters;
use derive_with::With;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub enum ConnectionOptions {
    Postgres(PostgresConnectionOptions),
    Oracle(OracleConnectionOptions),
    Mysql(MysqlConnectionOptions),
    Sqlite(SqliteConnectionOptions),
    Dm(DmConnectionOptions),
}

impl ConnectionOptions {
    pub(crate) fn stream_chunk_size(&self) -> usize {
        match self {
            ConnectionOptions::Postgres(options) => options.stream_chunk_size,
            ConnectionOptions::Oracle(options) => options.stream_chunk_size,
            ConnectionOptions::Mysql(options) => options.stream_chunk_size,
            ConnectionOptions::Sqlite(options) => options.stream_chunk_size,
            ConnectionOptions::Dm(options) => options.stream_chunk_size,
        }
    }

    pub(crate) fn db_type(&self) -> RemoteDbType {
        match self {
            ConnectionOptions::Postgres(_) => RemoteDbType::Postgres,
            ConnectionOptions::Oracle(_) => RemoteDbType::Oracle,
            ConnectionOptions::Mysql(_) => RemoteDbType::Mysql,
            ConnectionOptions::Sqlite(_) => RemoteDbType::Sqlite,
            ConnectionOptions::Dm(_) => RemoteDbType::Dm,
        }
    }

    pub fn with_pool_max_size(self, pool_max_size: usize) -> Self {
        match self {
            ConnectionOptions::Postgres(options) => {
                ConnectionOptions::Postgres(options.with_pool_max_size(pool_max_size))
            }
            ConnectionOptions::Oracle(options) => {
                ConnectionOptions::Oracle(options.with_pool_max_size(pool_max_size))
            }
            ConnectionOptions::Mysql(options) => {
                ConnectionOptions::Mysql(options.with_pool_max_size(pool_max_size))
            }
            ConnectionOptions::Sqlite(options) => ConnectionOptions::Sqlite(options),
            ConnectionOptions::Dm(options) => ConnectionOptions::Dm(options),
        }
    }
}

#[derive(Debug, Clone, With, Getters)]
pub struct PostgresConnectionOptions {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) database: Option<String>,
    pub(crate) pool_max_size: usize,
    pub(crate) stream_chunk_size: usize,
    pub(crate) default_numeric_scale: i8,
}

impl PostgresConnectionOptions {
    pub fn new(
        host: impl Into<String>,
        port: u16,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            username: username.into(),
            password: password.into(),
            database: None,
            pool_max_size: 10,
            stream_chunk_size: 2048,
            default_numeric_scale: 10,
        }
    }
}

impl From<PostgresConnectionOptions> for ConnectionOptions {
    fn from(options: PostgresConnectionOptions) -> Self {
        ConnectionOptions::Postgres(options)
    }
}

#[derive(Debug, Clone, With, Getters)]
pub struct MysqlConnectionOptions {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) database: Option<String>,
    pub(crate) pool_max_size: usize,
    pub(crate) stream_chunk_size: usize,
}

impl MysqlConnectionOptions {
    pub fn new(
        host: impl Into<String>,
        port: u16,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            username: username.into(),
            password: password.into(),
            database: None,
            pool_max_size: 10,
            stream_chunk_size: 2048,
        }
    }
}

impl From<MysqlConnectionOptions> for ConnectionOptions {
    fn from(options: MysqlConnectionOptions) -> Self {
        ConnectionOptions::Mysql(options)
    }
}

#[derive(Debug, Clone, With, Getters)]
pub struct OracleConnectionOptions {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) service_name: String,
    pub(crate) pool_max_size: usize,
    pub(crate) stream_chunk_size: usize,
}

impl OracleConnectionOptions {
    pub fn new(
        host: impl Into<String>,
        port: u16,
        username: impl Into<String>,
        password: impl Into<String>,
        service_name: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            username: username.into(),
            password: password.into(),
            service_name: service_name.into(),
            pool_max_size: 10,
            stream_chunk_size: 2048,
        }
    }
}

impl From<OracleConnectionOptions> for ConnectionOptions {
    fn from(options: OracleConnectionOptions) -> Self {
        ConnectionOptions::Oracle(options)
    }
}

#[derive(Debug, Clone, With, Getters)]
pub struct SqliteConnectionOptions {
    pub path: PathBuf,
    pub stream_chunk_size: usize,
}

impl SqliteConnectionOptions {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            stream_chunk_size: 2048,
        }
    }
}

impl From<SqliteConnectionOptions> for ConnectionOptions {
    fn from(options: SqliteConnectionOptions) -> Self {
        ConnectionOptions::Sqlite(options)
    }
}

#[derive(Debug, Clone, With, Getters)]
pub struct DmConnectionOptions {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) schema: Option<String>,
    pub(crate) stream_chunk_size: usize,
    pub(crate) driver: String,
}

impl DmConnectionOptions {
    pub fn new(
        host: impl Into<String>,
        port: u16,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            username: username.into(),
            password: password.into(),
            schema: None,
            stream_chunk_size: 1024,
            driver: "DM8 ODBC DRIVER".to_string(),
        }
    }
}

impl From<DmConnectionOptions> for ConnectionOptions {
    fn from(options: DmConnectionOptions) -> Self {
        ConnectionOptions::Dm(options)
    }
}
