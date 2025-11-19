#[cfg(feature = "dm")]
mod dm;
#[cfg(feature = "mysql")]
mod mysql;
mod options;
#[cfg(feature = "oracle")]
mod oracle;
#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

#[cfg(feature = "dm")]
pub use dm::*;
#[cfg(feature = "mysql")]
pub use mysql::*;
pub use options::*;
#[cfg(feature = "oracle")]
pub use oracle::*;
#[cfg(feature = "postgres")]
pub use postgres::*;
#[cfg(feature = "sqlite")]
pub use sqlite::*;

use std::any::Any;

use crate::{DFResult, Literalize, RemoteSchemaRef, RemoteSource, extract_primitive_array};
use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef};
use datafusion::common::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::common::collect;
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::{MySqlDialect, PostgreSqlDialect, SqliteDialect};
use std::fmt::Debug;
use std::sync::Arc;

#[cfg(feature = "dm")]
pub static ODBC_ENV: std::sync::OnceLock<odbc_api::Environment> = std::sync::OnceLock::new();

#[async_trait::async_trait]
pub trait Pool: Debug + Send + Sync {
    async fn get(&self) -> DFResult<Arc<dyn Connection>>;
    async fn state(&self) -> DFResult<PoolState>;
}

#[derive(Debug, Clone)]
pub struct PoolState {
    pub connections: usize,
    pub idle_connections: usize,
}

#[async_trait::async_trait]
pub trait Connection: Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;

    async fn infer_schema(&self, source: &RemoteSource) -> DFResult<RemoteSchemaRef>;

    async fn query(
        &self,
        conn_options: &ConnectionOptions,
        source: &RemoteSource,
        table_schema: SchemaRef,
        projection: Option<&Vec<usize>>,
        unparsed_filters: &[String],
        limit: Option<usize>,
    ) -> DFResult<SendableRecordBatchStream>;

    async fn insert(
        &self,
        conn_options: &ConnectionOptions,
        literalizer: Arc<dyn Literalize>,
        table: &[String],
        remote_schema: RemoteSchemaRef,
        input: SendableRecordBatchStream,
    ) -> DFResult<usize>;
}

#[allow(unused_variables)]
pub async fn connect(options: &ConnectionOptions) -> DFResult<Arc<dyn Pool>> {
    match options {
        ConnectionOptions::Postgres(options) => {
            #[cfg(feature = "postgres")]
            {
                let pool = connect_postgres(options).await?;
                Ok(Arc::new(pool))
            }
            #[cfg(not(feature = "postgres"))]
            {
                Err(DataFusionError::Internal(
                    "Please enable the postgres feature".to_string(),
                ))
            }
        }
        ConnectionOptions::Mysql(options) => {
            #[cfg(feature = "mysql")]
            {
                let pool = connect_mysql(options)?;
                Ok(Arc::new(pool))
            }
            #[cfg(not(feature = "mysql"))]
            {
                Err(DataFusionError::Internal(
                    "Please enable the mysql feature".to_string(),
                ))
            }
        }
        ConnectionOptions::Oracle(options) => {
            #[cfg(feature = "oracle")]
            {
                let pool = connect_oracle(options).await?;
                Ok(Arc::new(pool))
            }
            #[cfg(not(feature = "oracle"))]
            {
                Err(DataFusionError::Internal(
                    "Please enable the oracle feature".to_string(),
                ))
            }
        }
        ConnectionOptions::Sqlite(options) => {
            #[cfg(feature = "sqlite")]
            {
                let pool = connect_sqlite(options).await?;
                Ok(Arc::new(pool))
            }
            #[cfg(not(feature = "sqlite"))]
            {
                Err(DataFusionError::Internal(
                    "Please enable the sqlite feature".to_string(),
                ))
            }
        }
        ConnectionOptions::Dm(options) => {
            #[cfg(feature = "dm")]
            {
                let pool = connect_dm(options)?;
                Ok(Arc::new(pool))
            }
            #[cfg(not(feature = "dm"))]
            {
                Err(DataFusionError::Internal(
                    "Please enable the dm feature".to_string(),
                ))
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum RemoteDbType {
    Postgres,
    Mysql,
    Oracle,
    Sqlite,
    Dm,
}

impl RemoteDbType {
    pub(crate) fn support_rewrite_with_filters_limit(&self, source: &RemoteSource) -> bool {
        match source {
            RemoteSource::Table(_) => true,
            RemoteSource::Query(query) => query.trim()[0..6].eq_ignore_ascii_case("select"),
        }
    }

    pub(crate) fn create_unparser(&self) -> DFResult<Unparser<'_>> {
        match self {
            RemoteDbType::Postgres => Ok(Unparser::new(&PostgreSqlDialect {})),
            RemoteDbType::Mysql => Ok(Unparser::new(&MySqlDialect {})),
            RemoteDbType::Sqlite => Ok(Unparser::new(&SqliteDialect {})),
            RemoteDbType::Oracle => Err(DataFusionError::NotImplemented(
                "Oracle unparser not implemented".to_string(),
            )),
            RemoteDbType::Dm => Err(DataFusionError::NotImplemented(
                "Dm unparser not implemented".to_string(),
            )),
        }
    }

    pub(crate) fn rewrite_query(
        &self,
        source: &RemoteSource,
        unparsed_filters: &[String],
        limit: Option<usize>,
    ) -> String {
        match source {
            RemoteSource::Table(table) => match self {
                RemoteDbType::Postgres
                | RemoteDbType::Mysql
                | RemoteDbType::Sqlite
                | RemoteDbType::Dm => {
                    let where_clause = if unparsed_filters.is_empty() {
                        "".to_string()
                    } else {
                        format!(" WHERE {}", unparsed_filters.join(" AND "))
                    };
                    let limit_clause = if let Some(limit) = limit {
                        format!(" LIMIT {limit}")
                    } else {
                        "".to_string()
                    };

                    format!(
                        "{}{where_clause}{limit_clause}",
                        self.select_all_query(table)
                    )
                }
                RemoteDbType::Oracle => {
                    let mut all_filters: Vec<String> = vec![];
                    all_filters.extend_from_slice(unparsed_filters);
                    if let Some(limit) = limit {
                        all_filters.push(format!("ROWNUM <= {limit}"))
                    }

                    let where_clause = if all_filters.is_empty() {
                        "".to_string()
                    } else {
                        format!(" WHERE {}", all_filters.join(" AND "))
                    };
                    format!("{}{where_clause}", self.select_all_query(table))
                }
            },
            RemoteSource::Query(query) => match self {
                RemoteDbType::Postgres
                | RemoteDbType::Mysql
                | RemoteDbType::Sqlite
                | RemoteDbType::Dm => {
                    let where_clause = if unparsed_filters.is_empty() {
                        "".to_string()
                    } else {
                        format!(" WHERE {}", unparsed_filters.join(" AND "))
                    };
                    let limit_clause = if let Some(limit) = limit {
                        format!(" LIMIT {limit}")
                    } else {
                        "".to_string()
                    };

                    if where_clause.is_empty() && limit_clause.is_empty() {
                        query.clone()
                    } else {
                        format!("SELECT * FROM ({query}) as __subquery{where_clause}{limit_clause}")
                    }
                }
                RemoteDbType::Oracle => {
                    let mut all_filters: Vec<String> = vec![];
                    all_filters.extend_from_slice(unparsed_filters);
                    if let Some(limit) = limit {
                        all_filters.push(format!("ROWNUM <= {limit}"))
                    }

                    let where_clause = if all_filters.is_empty() {
                        "".to_string()
                    } else {
                        format!(" WHERE {}", all_filters.join(" AND "))
                    };
                    if where_clause.is_empty() {
                        query.clone()
                    } else {
                        format!("SELECT * FROM ({query}){where_clause}")
                    }
                }
            },
        }
    }

    pub(crate) fn sql_identifier(&self, identifier: &str) -> String {
        match self {
            RemoteDbType::Postgres
            | RemoteDbType::Oracle
            | RemoteDbType::Sqlite
            | RemoteDbType::Dm => {
                format!("\"{identifier}\"")
            }
            RemoteDbType::Mysql => {
                format!("`{identifier}`")
            }
        }
    }

    pub(crate) fn sql_table_name(&self, indentifiers: &[String]) -> String {
        indentifiers
            .iter()
            .map(|identifier| self.sql_identifier(identifier))
            .collect::<Vec<String>>()
            .join(".")
    }

    pub(crate) fn sql_string_literal(&self, value: &str) -> String {
        let value = value.replace("'", "''");
        format!("'{value}'")
    }

    pub(crate) fn sql_binary_literal(&self, value: &[u8]) -> String {
        match self {
            RemoteDbType::Postgres => format!("E'\\\\x{}'", hex::encode(value)),
            RemoteDbType::Mysql | RemoteDbType::Sqlite => format!("X'{}'", hex::encode(value)),
            RemoteDbType::Oracle | RemoteDbType::Dm => todo!(),
        }
    }

    pub(crate) fn select_all_query(&self, table_identifiers: &[String]) -> String {
        match self {
            RemoteDbType::Postgres
            | RemoteDbType::Mysql
            | RemoteDbType::Oracle
            | RemoteDbType::Sqlite
            | RemoteDbType::Dm => {
                format!("SELECT * FROM {}", self.sql_table_name(table_identifiers))
            }
        }
    }

    pub(crate) fn limit_1_query_if_possible(&self, source: &RemoteSource) -> String {
        if !self.support_rewrite_with_filters_limit(source) {
            return source.query(*self);
        }
        self.rewrite_query(source, &[], Some(1))
    }

    pub(crate) fn try_count1_query(&self, source: &RemoteSource) -> Option<String> {
        if !self.support_rewrite_with_filters_limit(source) {
            return None;
        }
        match source {
            RemoteSource::Table(table) => Some(self.select_all_query(table)),
            RemoteSource::Query(query) => match self {
                RemoteDbType::Postgres
                | RemoteDbType::Mysql
                | RemoteDbType::Sqlite
                | RemoteDbType::Dm => Some(format!("SELECT COUNT(1) FROM ({query}) AS __subquery")),
                RemoteDbType::Oracle => Some(format!("SELECT COUNT(1) FROM ({query})")),
            },
        }
    }

    pub(crate) async fn fetch_count(
        &self,
        conn: Arc<dyn Connection>,
        conn_options: &ConnectionOptions,
        count1_query: &str,
    ) -> DFResult<usize> {
        let count1_schema = Arc::new(Schema::new(vec![Field::new(
            "count(1)",
            DataType::Int64,
            false,
        )]));
        let stream = conn
            .query(
                conn_options,
                &RemoteSource::Query(count1_query.to_string()),
                count1_schema,
                None,
                &[],
                None,
            )
            .await?;
        let batches = collect(stream).await?;
        let count_vec = extract_primitive_array::<Int64Type>(&batches, 0)?;
        if count_vec.len() != 1 {
            return Err(DataFusionError::Execution(format!(
                "Count query did not return exactly one row: {count_vec:?}",
            )));
        }
        count_vec[0]
            .map(|count| count as usize)
            .ok_or_else(|| DataFusionError::Execution("Count query returned null".to_string()))
    }
}

pub(crate) fn projections_contains(projection: Option<&Vec<usize>>, col_idx: usize) -> bool {
    match projection {
        Some(p) => p.contains(&col_idx),
        None => true,
    }
}

#[allow(unused)]
fn just_return<T>(v: T) -> DFResult<T> {
    Ok(v)
}

#[allow(unused)]
fn just_deref<T: Copy>(t: &T) -> DFResult<T> {
    Ok(*t)
}

#[test]
fn tst_f32() {
    println!("{}", 10f32.powi(40));
}
