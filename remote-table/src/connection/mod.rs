#[cfg(feature = "dm")]
mod dm;
#[cfg(feature = "mysql")]
mod mysql;
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
#[cfg(feature = "oracle")]
pub use oracle::*;
#[cfg(feature = "postgres")]
pub use postgres::*;
#[cfg(feature = "sqlite")]
pub use sqlite::*;
use std::any::Any;

use crate::{DFResult, RemoteSchemaRef, extract_primitive_array};
use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef};
use datafusion::common::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::common::collect;
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::{MySqlDialect, PostgreSqlDialect, SqliteDialect};
use std::fmt::Debug;
use std::sync::Arc;

#[cfg(feature = "dm")]
pub(crate) static ODBC_ENV: std::sync::OnceLock<odbc_api::Environment> = std::sync::OnceLock::new();

#[async_trait::async_trait]
pub trait Pool: Debug + Send + Sync {
    async fn get(&self) -> DFResult<Arc<dyn Connection>>;
}

#[async_trait::async_trait]
pub trait Connection: Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;

    async fn infer_schema(&self, sql: &str) -> DFResult<RemoteSchemaRef>;

    async fn query(
        &self,
        conn_options: &ConnectionOptions,
        sql: &str,
        table_schema: SchemaRef,
        projection: Option<&Vec<usize>>,
        unparsed_filters: &[String],
        limit: Option<usize>,
    ) -> DFResult<SendableRecordBatchStream>;
}

pub async fn connect(options: &ConnectionOptions) -> DFResult<Arc<dyn Pool>> {
    match options {
        #[cfg(feature = "postgres")]
        ConnectionOptions::Postgres(options) => {
            let pool = connect_postgres(options).await?;
            Ok(Arc::new(pool))
        }
        #[cfg(feature = "mysql")]
        ConnectionOptions::Mysql(options) => {
            let pool = connect_mysql(options)?;
            Ok(Arc::new(pool))
        }
        #[cfg(feature = "oracle")]
        ConnectionOptions::Oracle(options) => {
            let pool = connect_oracle(options).await?;
            Ok(Arc::new(pool))
        }
        #[cfg(feature = "sqlite")]
        ConnectionOptions::Sqlite(options) => {
            let pool = connect_sqlite(options).await?;
            Ok(Arc::new(pool))
        }
        #[cfg(feature = "dm")]
        ConnectionOptions::Dm(options) => {
            let pool = connect_dm(options)?;
            Ok(Arc::new(pool))
        }
    }
}

#[derive(Debug, Clone)]
pub enum ConnectionOptions {
    #[cfg(feature = "postgres")]
    Postgres(PostgresConnectionOptions),
    #[cfg(feature = "oracle")]
    Oracle(OracleConnectionOptions),
    #[cfg(feature = "mysql")]
    Mysql(MysqlConnectionOptions),
    #[cfg(feature = "sqlite")]
    Sqlite(SqliteConnectionOptions),
    #[cfg(feature = "dm")]
    Dm(DmConnectionOptions),
}

impl ConnectionOptions {
    pub(crate) fn stream_chunk_size(&self) -> usize {
        match self {
            #[cfg(feature = "postgres")]
            ConnectionOptions::Postgres(options) => options.stream_chunk_size,
            #[cfg(feature = "oracle")]
            ConnectionOptions::Oracle(options) => options.stream_chunk_size,
            #[cfg(feature = "mysql")]
            ConnectionOptions::Mysql(options) => options.stream_chunk_size,
            #[cfg(feature = "sqlite")]
            ConnectionOptions::Sqlite(options) => options.stream_chunk_size,
            #[cfg(feature = "dm")]
            ConnectionOptions::Dm(options) => options.stream_chunk_size,
        }
    }

    pub(crate) fn db_type(&self) -> RemoteDbType {
        match self {
            #[cfg(feature = "postgres")]
            ConnectionOptions::Postgres(_) => RemoteDbType::Postgres,
            #[cfg(feature = "oracle")]
            ConnectionOptions::Oracle(_) => RemoteDbType::Oracle,
            #[cfg(feature = "mysql")]
            ConnectionOptions::Mysql(_) => RemoteDbType::Mysql,
            #[cfg(feature = "sqlite")]
            ConnectionOptions::Sqlite(_) => RemoteDbType::Sqlite,
            #[cfg(feature = "dm")]
            ConnectionOptions::Dm(_) => RemoteDbType::Dm,
        }
    }
}

pub enum RemoteDbType {
    Postgres,
    Mysql,
    Oracle,
    Sqlite,
    Dm,
}

impl RemoteDbType {
    pub(crate) fn support_rewrite_with_filters_limit(&self, sql: &str) -> bool {
        sql.trim()[0..6].eq_ignore_ascii_case("select")
    }

    pub(crate) fn create_unparser(&self) -> DFResult<Unparser> {
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
        sql: &str,
        unparsed_filters: &[String],
        limit: Option<usize>,
    ) -> String {
        match self {
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
                    sql.to_string()
                } else {
                    format!("SELECT * FROM ({sql}) as __subquery{where_clause}{limit_clause}")
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
                    sql.to_string()
                } else {
                    format!("SELECT * FROM ({sql}){where_clause}")
                }
            }
        }
    }

    pub(crate) fn query_limit_1(&self, sql: &str) -> String {
        if !self.support_rewrite_with_filters_limit(sql) {
            return sql.to_string();
        }
        self.rewrite_query(sql, &[], Some(1))
    }

    pub(crate) fn try_count1_query(&self, sql: &str) -> Option<String> {
        if !self.support_rewrite_with_filters_limit(sql) {
            return None;
        }
        match self {
            RemoteDbType::Postgres
            | RemoteDbType::Mysql
            | RemoteDbType::Sqlite
            | RemoteDbType::Dm => Some(format!("SELECT COUNT(1) FROM ({sql}) AS __subquery")),
            RemoteDbType::Oracle => Some(format!("SELECT COUNT(1) FROM ({sql})")),
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
            .query(conn_options, count1_query, count1_schema, None, &[], None)
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

#[cfg(any(feature = "mysql", feature = "postgres", feature = "oracle"))]
fn big_decimal_to_i128(decimal: &bigdecimal::BigDecimal, scale: Option<i32>) -> Option<i128> {
    use bigdecimal::{FromPrimitive, ToPrimitive};
    let scale = scale.unwrap_or_else(|| {
        decimal
            .fractional_digit_count()
            .try_into()
            .unwrap_or_default()
    });
    let scale_decimal = bigdecimal::BigDecimal::from_f32(10f32.powi(scale))?;
    (decimal * scale_decimal).to_i128()
}

#[allow(unused)]
fn just_return<T>(v: T) -> DFResult<T> {
    Ok(v)
}

#[allow(unused)]
fn just_deref<T: Copy>(t: &T) -> DFResult<T> {
    Ok(*t)
}
