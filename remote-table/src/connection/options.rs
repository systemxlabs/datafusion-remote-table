use crate::RemoteDbType;
use derive_getters::Getters;
use derive_with::With;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone)]
pub enum ConnectionOptions {
    Postgres(PostgresConnectionOptions),
    Oracle(OracleConnectionOptions),
    Mysql(MysqlConnectionOptions),
    Sqlite(SqliteConnectionOptions),
    Dm(DmConnectionOptions),
    /// Microsoft Access `.mdb` files (Jet engine).
    Mdb(MdbConnectionOptions),
    /// Microsoft Access `.accdb` files (ACE engine).
    Access(AccessConnectionOptions),
    GaussDB(GaussDBConnectionOptions),
    MongoDB(MongoDBConnectionOptions),
}

impl ConnectionOptions {
    pub(crate) fn stream_chunk_size(&self) -> usize {
        match self {
            ConnectionOptions::Postgres(options) => options.stream_chunk_size,
            ConnectionOptions::Oracle(options) => options.stream_chunk_size,
            ConnectionOptions::Mysql(options) => options.stream_chunk_size,
            ConnectionOptions::Sqlite(options) => options.stream_chunk_size,
            ConnectionOptions::Dm(options) => options.stream_chunk_size,
            ConnectionOptions::Mdb(options) => options.stream_chunk_size,
            ConnectionOptions::Access(options) => options.stream_chunk_size,
            ConnectionOptions::GaussDB(options) => options.stream_chunk_size,
            ConnectionOptions::MongoDB(options) => options.stream_chunk_size,
        }
    }

    pub(crate) fn db_type(&self) -> RemoteDbType {
        match self {
            ConnectionOptions::Postgres(_) => RemoteDbType::Postgres,
            ConnectionOptions::Oracle(_) => RemoteDbType::Oracle,
            ConnectionOptions::Mysql(_) => RemoteDbType::Mysql,
            ConnectionOptions::Sqlite(_) => RemoteDbType::Sqlite,
            ConnectionOptions::Dm(_) => RemoteDbType::Dm,
            ConnectionOptions::Mdb(_) => RemoteDbType::Mdb,
            ConnectionOptions::Access(_) => RemoteDbType::Access,
            ConnectionOptions::GaussDB(_) => RemoteDbType::GaussDB,
            ConnectionOptions::MongoDB(_) => RemoteDbType::MongoDB,
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
            ConnectionOptions::Mdb(options) => ConnectionOptions::Mdb(options),
            ConnectionOptions::Access(options) => ConnectionOptions::Access(options),
            ConnectionOptions::GaussDB(options) => ConnectionOptions::GaussDB(options),
            ConnectionOptions::MongoDB(options) => ConnectionOptions::MongoDB(options),
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
    pub(crate) pool_min_idle: usize,
    pub(crate) pool_idle_timeout: Duration,
    pub(crate) pool_ttl_check_interval: Duration,
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
            pool_min_idle: 0,
            pool_idle_timeout: Duration::from_secs(10 * 60),
            pool_ttl_check_interval: Duration::from_secs(30),
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
    pub(crate) pool_min_idle: usize,
    pub(crate) pool_idle_timeout: Duration,
    pub(crate) pool_ttl_check_interval: Duration,
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
            pool_min_idle: 0,
            pool_idle_timeout: Duration::from_secs(10 * 60),
            pool_ttl_check_interval: Duration::from_secs(30),
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
    pub(crate) pool_min_idle: usize,
    pub(crate) pool_idle_timeout: Duration,
    pub(crate) pool_ttl_check_interval: Duration,
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
            pool_min_idle: 0,
            pool_idle_timeout: Duration::from_secs(10 * 60),
            pool_ttl_check_interval: Duration::from_secs(30),
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

#[derive(Debug, Clone, With, Getters)]
pub struct MdbConnectionOptions {
    pub path: PathBuf,
    pub driver: String,
    pub stream_chunk_size: usize,
    pub uid: Option<String>,
    pub pwd: Option<String>,
    /// ODBC connection attributes, appended as `;key=value`. Use this for
    /// driver-specific parameters not covered by the typed fields (e.g.
    /// `SystemDB`, `Exclusive=1`, `IMEX=1` for the Microsoft Access ODBC driver
    /// on Windows). A map, because a connection string is a set of attributes:
    /// the order of different keywords is not significant, and repeating a
    /// keyword is driver-defined behaviour that this type does not express.
    pub extra_params: HashMap<String, String>,
}

impl MdbConnectionOptions {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            driver: "MDBTools".to_string(),
            stream_chunk_size: 2048,
            uid: None,
            pwd: None,
            extra_params: HashMap::new(),
        }
    }

    pub fn new_with_driver(path: PathBuf, driver: impl Into<String>) -> Self {
        Self {
            path,
            driver: driver.into(),
            stream_chunk_size: 2048,
            uid: None,
            pwd: None,
            extra_params: HashMap::new(),
        }
    }

    /// Build the ODBC connection string. The default `Driver={...};DBQ=...` form
    /// works for the mdbtools (Linux) driver. For the Microsoft Access ODBC driver
    /// on Windows, set `uid`/`pwd` or add entries to `extra_params` (e.g.
    /// `SystemDB`, `Exclusive`, `IMEX`).
    pub fn connection_string(&self) -> String {
        let mut s = format!("Driver={{{}}};DBQ={}", self.driver, self.path.display());
        if let Some(uid) = &self.uid {
            s.push_str(&format!(";UID={uid}"));
        }
        if let Some(pwd) = &self.pwd {
            s.push_str(&format!(";PWD={pwd}"));
        }
        for (k, v) in &self.extra_params {
            s.push_str(&format!(";{k}={v}"));
        }
        s
    }
}

impl From<MdbConnectionOptions> for ConnectionOptions {
    fn from(options: MdbConnectionOptions) -> Self {
        ConnectionOptions::Mdb(options)
    }
}

/// Options for a Microsoft Access `.accdb` (ACE engine) source.
///
/// This is its own type, with its own fields, pool and connection handling;
/// what it shares with [`MdbConnectionOptions`] is only the shape of the
/// connection parameters, because both formats are read through the same ODBC
/// driver.
#[derive(Debug, Clone, With, Getters)]
pub struct AccessConnectionOptions {
    pub path: PathBuf,
    pub driver: String,
    pub stream_chunk_size: usize,
    pub uid: Option<String>,
    pub pwd: Option<String>,
    /// ODBC connection attributes, appended as `;key=value`. Use this for
    /// driver-specific parameters not covered by the typed fields (e.g.
    /// `SystemDB`, `Exclusive=1`, `IMEX=1` for the Microsoft Access ODBC driver
    /// on Windows). A map, because a connection string is a set of attributes:
    /// the order of different keywords is not significant, and repeating a
    /// keyword is driver-defined behaviour that this type does not express.
    pub extra_params: HashMap<String, String>,
}

impl AccessConnectionOptions {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            driver: "MDBTools".to_string(),
            stream_chunk_size: 2048,
            uid: None,
            pwd: None,
            extra_params: HashMap::new(),
        }
    }

    pub fn new_with_driver(path: PathBuf, driver: impl Into<String>) -> Self {
        Self {
            path,
            driver: driver.into(),
            stream_chunk_size: 2048,
            uid: None,
            pwd: None,
            extra_params: HashMap::new(),
        }
    }

    /// Build the ODBC connection string. The default `Driver={...};DBQ=...` form
    /// works for the mdbtools (Linux) driver. For the Microsoft Access ODBC driver
    /// on Windows, set `uid`/`pwd` or add entries to `extra_params` (e.g.
    /// `SystemDB`, `Exclusive`, `IMEX`).
    pub fn connection_string(&self) -> String {
        let mut s = format!("Driver={{{}}};DBQ={}", self.driver, self.path.display());
        if let Some(uid) = &self.uid {
            s.push_str(&format!(";UID={uid}"));
        }
        if let Some(pwd) = &self.pwd {
            s.push_str(&format!(";PWD={pwd}"));
        }
        for (k, v) in &self.extra_params {
            s.push_str(&format!(";{k}={v}"));
        }
        s
    }
}

impl From<AccessConnectionOptions> for ConnectionOptions {
    fn from(options: AccessConnectionOptions) -> Self {
        ConnectionOptions::Access(options)
    }
}

#[derive(Debug, Clone, With, Getters)]
pub struct GaussDBConnectionOptions {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) database: Option<String>,
    pub(crate) pool_max_size: usize,
    pub(crate) pool_min_idle: usize,
    pub(crate) pool_idle_timeout: Duration,
    pub(crate) pool_ttl_check_interval: Duration,
    pub(crate) stream_chunk_size: usize,
}

impl GaussDBConnectionOptions {
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
            pool_min_idle: 0,
            pool_idle_timeout: Duration::from_secs(10 * 60),
            pool_ttl_check_interval: Duration::from_secs(30),
            stream_chunk_size: 2048,
        }
    }
}

impl From<GaussDBConnectionOptions> for ConnectionOptions {
    fn from(options: GaussDBConnectionOptions) -> Self {
        ConnectionOptions::GaussDB(options)
    }
}

#[derive(Debug, Clone, With, Getters)]
pub struct MongoDBConnectionOptions {
    /// Connection string, e.g. `mongodb://user:password@localhost:27017`.
    /// All driver options understood by the official Rust driver can be passed
    /// here (e.g. `?authSource=admin`).
    pub(crate) uri: String,
    /// Database the collections are read from. For `RemoteSource::Table` with a
    /// single identifier this is the database that holds the collection; a two
    /// part identifier (`[database, collection]`) overrides it per table.
    pub(crate) database: String,
    pub(crate) stream_chunk_size: usize,
    /// Maximum number of connections the driver pools per server. `None` keeps
    /// whatever the connection string (or the driver default, 10) specifies.
    pub(crate) pool_max_size: Option<u32>,
    /// Connections the driver keeps warm per server.
    pub(crate) pool_min_idle: Option<u32>,
    /// Maximum number of connections established concurrently.
    pub(crate) pool_max_connecting: Option<u32>,
    /// How long an idle pooled connection is kept before it is closed.
    pub(crate) pool_idle_timeout: Option<Duration>,
}

impl MongoDBConnectionOptions {
    pub fn new(uri: impl Into<String>, database: impl Into<String>) -> Self {
        Self {
            uri: uri.into(),
            database: database.into(),
            stream_chunk_size: 2048,
            pool_max_size: None,
            pool_min_idle: None,
            pool_max_connecting: None,
            pool_idle_timeout: None,
        }
    }
}

impl From<MongoDBConnectionOptions> for ConnectionOptions {
    fn from(options: MongoDBConnectionOptions) -> Self {
        ConnectionOptions::MongoDB(options)
    }
}
