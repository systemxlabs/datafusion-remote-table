use crate::{
    ConnectionOptions, DFResult, DefaultTransform, DefaultUnparser, Pool, RemoteDbType,
    RemoteSchemaRef, RemoteTableInsertExec, RemoteTableScanExec, Transform, Unparse, connect,
    transform_schema,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, Statistics};
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use log::{debug, warn};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum TableSource {
    Query(String),
    Table(Vec<String>),
}

impl TableSource {
    pub fn query(&self, db_type: RemoteDbType) -> String {
        match self {
            TableSource::Query(query) => query.clone(),
            TableSource::Table(table_identifiers) => db_type.select_all_query(table_identifiers),
        }
    }
}

impl std::fmt::Display for TableSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableSource::Query(query) => write!(f, "{query}"),
            TableSource::Table(table) => write!(f, "{}", table.join(".")),
        }
    }
}

impl From<String> for TableSource {
    fn from(query: String) -> Self {
        TableSource::Query(query)
    }
}

impl From<&str> for TableSource {
    fn from(query: &str) -> Self {
        TableSource::Query(query.to_string())
    }
}

impl From<Vec<String>> for TableSource {
    fn from(table_identifiers: Vec<String>) -> Self {
        TableSource::Table(table_identifiers)
    }
}

impl From<Vec<&str>> for TableSource {
    fn from(table_identifiers: Vec<&str>) -> Self {
        TableSource::Table(
            table_identifiers
                .into_iter()
                .map(|s| s.to_string())
                .collect(),
        )
    }
}

#[derive(Debug)]
pub struct RemoteTable {
    pub(crate) conn_options: ConnectionOptions,
    pub(crate) source: TableSource,
    pub(crate) table_schema: SchemaRef,
    pub(crate) transformed_table_schema: SchemaRef,
    pub(crate) remote_schema: Option<RemoteSchemaRef>,
    pub(crate) transform: Arc<dyn Transform>,
    pub(crate) unparser: Arc<dyn Unparse>,
    pub(crate) pool: Arc<dyn Pool>,
}

impl RemoteTable {
    pub async fn try_new(
        conn_options: impl Into<ConnectionOptions>,
        source: impl Into<TableSource>,
    ) -> DFResult<Self> {
        Self::try_new_with_schema_transform_unparser(
            conn_options,
            source,
            None,
            Arc::new(DefaultTransform {}),
            Arc::new(DefaultUnparser {}),
        )
        .await
    }

    pub async fn try_new_with_schema(
        conn_options: impl Into<ConnectionOptions>,
        source: impl Into<TableSource>,
        table_schema: SchemaRef,
    ) -> DFResult<Self> {
        Self::try_new_with_schema_transform_unparser(
            conn_options,
            source,
            Some(table_schema),
            Arc::new(DefaultTransform {}),
            Arc::new(DefaultUnparser {}),
        )
        .await
    }

    pub async fn try_new_with_transform(
        conn_options: impl Into<ConnectionOptions>,
        source: impl Into<TableSource>,
        transform: Arc<dyn Transform>,
    ) -> DFResult<Self> {
        Self::try_new_with_schema_transform_unparser(
            conn_options,
            source,
            None,
            transform,
            Arc::new(DefaultUnparser {}),
        )
        .await
    }

    pub async fn try_new_with_schema_transform_unparser(
        conn_options: impl Into<ConnectionOptions>,
        source: impl Into<TableSource>,
        table_schema: Option<SchemaRef>,
        transform: Arc<dyn Transform>,
        unparser: Arc<dyn Unparse>,
    ) -> DFResult<Self> {
        let conn_options = conn_options.into();
        let source = source.into();

        let now = std::time::Instant::now();
        let pool = connect(&conn_options).await?;
        debug!(
            "[remote-table] Creating connection pool cost: {}ms",
            now.elapsed().as_millis()
        );

        let (table_schema, remote_schema) = if let Some(table_schema) = table_schema {
            let remote_schema = if transform.as_any().is::<DefaultTransform>() {
                None
            } else {
                // Infer remote schema
                let now = std::time::Instant::now();
                let conn = pool.get().await?;
                let remote_schema_opt = conn.infer_schema(&source).await.ok();
                debug!(
                    "[remote-table] Inferring remote schema cost: {}ms",
                    now.elapsed().as_millis()
                );
                remote_schema_opt
            };
            (table_schema, remote_schema)
        } else {
            // Infer table schema
            let now = std::time::Instant::now();
            let conn = pool.get().await?;
            match conn.infer_schema(&source).await {
                Ok(remote_schema) => {
                    debug!(
                        "[remote-table] Inferring table schema cost: {}ms",
                        now.elapsed().as_millis()
                    );
                    let inferred_table_schema = Arc::new(remote_schema.to_arrow_schema());
                    (inferred_table_schema, Some(remote_schema))
                }
                Err(e) => {
                    return Err(DataFusionError::Execution(format!(
                        "Failed to infer schema: {e}"
                    )));
                }
            }
        };

        let transformed_table_schema = transform_schema(
            table_schema.clone(),
            transform.as_ref(),
            remote_schema.as_ref(),
        )?;

        Ok(RemoteTable {
            conn_options,
            source,
            table_schema,
            transformed_table_schema,
            remote_schema,
            transform,
            unparser,
            pool,
        })
    }

    pub fn remote_schema(&self) -> Option<RemoteSchemaRef> {
        self.remote_schema.clone()
    }
}

#[async_trait::async_trait]
impl TableProvider for RemoteTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.transformed_table_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let transformed_table_schema = transform_schema(
            self.table_schema.clone(),
            self.transform.as_ref(),
            self.remote_schema.as_ref(),
        )?;
        let rewritten_filters = rewrite_filters_column(
            filters.to_vec(),
            &self.table_schema,
            &transformed_table_schema,
        )?;
        let mut unparsed_filters = vec![];
        for filter in rewritten_filters {
            unparsed_filters.push(
                self.unparser
                    .unparse_filter(&filter, self.conn_options.db_type())?,
            );
        }

        let now = std::time::Instant::now();
        let conn = self.pool.get().await?;
        debug!(
            "[remote-table] Getting connection from pool cost: {}ms",
            now.elapsed().as_millis()
        );

        Ok(Arc::new(RemoteTableScanExec::try_new(
            self.conn_options.clone(),
            self.source.clone(),
            self.table_schema.clone(),
            self.remote_schema.clone(),
            projection.cloned(),
            unparsed_filters,
            limit,
            self.transform.clone(),
            conn,
        )?))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        let db_type = self.conn_options.db_type();
        if !db_type.support_rewrite_with_filters_limit(&self.source) {
            return Ok(vec![
                TableProviderFilterPushDown::Unsupported;
                filters.len()
            ]);
        }
        let mut pushdown = vec![];
        for filter in filters {
            pushdown.push(
                self.unparser
                    .support_filter_pushdown(filter, self.conn_options.db_type())?,
            );
        }
        Ok(pushdown)
    }

    fn statistics(&self) -> Option<Statistics> {
        let db_type = self.conn_options.db_type();
        if let Some(count1_query) = db_type.try_count1_query(&self.source) {
            let conn_options = self.conn_options.clone();
            let row_count_result = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let pool = connect(&conn_options).await?;
                    let conn = pool.get().await?;
                    conn_options
                        .db_type()
                        .fetch_count(conn, &conn_options, &count1_query)
                        .await
                })
            });

            match row_count_result {
                Ok(row_count) => {
                    let column_stat =
                        Statistics::unknown_column(self.transformed_table_schema.as_ref());
                    Some(Statistics {
                        num_rows: Precision::Exact(row_count),
                        total_byte_size: Precision::Absent,
                        column_statistics: column_stat,
                    })
                }
                Err(e) => {
                    warn!("[remote-table] Failed to fetch table statistics: {e}");
                    None
                }
            }
        } else {
            debug!(
                "[remote-table] Query can not be rewritten as count1 query: {}",
                self.source
            );
            None
        }
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        match insert_op {
            InsertOp::Append => {}
            InsertOp::Overwrite | InsertOp::Replace => {
                return Err(DataFusionError::Execution(
                    "Only support append insert operation".to_string(),
                ));
            }
        }

        let remote_schema = self
            .remote_schema
            .as_ref()
            .ok_or(DataFusionError::Execution(
                "Remote schema is not available".to_string(),
            ))?
            .clone();

        let TableSource::Table(table) = &self.source else {
            return Err(DataFusionError::Execution(
                "Only support insert operation for table".to_string(),
            ));
        };

        let now = std::time::Instant::now();
        let conn = self.pool.get().await?;
        debug!(
            "[remote-table] Getting connection from pool cost: {}ms",
            now.elapsed().as_millis()
        );

        let exec = RemoteTableInsertExec::new(
            input,
            self.conn_options.clone(),
            self.unparser.clone(),
            table.clone(),
            remote_schema,
            conn,
        );
        Ok(Arc::new(exec))
    }
}

pub(crate) fn rewrite_filters_column(
    filters: Vec<Expr>,
    table_schema: &SchemaRef,
    transformed_table_schema: &SchemaRef,
) -> DFResult<Vec<Expr>> {
    filters
        .into_iter()
        .map(|f| {
            f.transform_down(|e| {
                if let Expr::Column(col) = e {
                    let col_idx = transformed_table_schema.index_of(col.name())?;
                    let row_name = table_schema.field(col_idx).name().to_string();
                    Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                        row_name,
                    ))))
                } else {
                    Ok(Transformed::no(e))
                }
            })
            .map(|trans| trans.data)
        })
        .collect::<DFResult<Vec<_>>>()
}
