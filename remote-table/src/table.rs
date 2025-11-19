use crate::{
    ConnectionOptions, DFResult, DefaultLiteralizer, DefaultTransform, Literalize, Pool,
    RemoteDbType, RemoteSchema, RemoteSchemaRef, RemoteTableInsertExec, RemoteTableScanExec,
    Transform, TransformArgs, connect, transform_schema,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Statistics;
use datafusion::common::stats::Precision;
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use log::{debug, warn};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum RemoteSource {
    Query(String),
    Table(Vec<String>),
}

impl RemoteSource {
    pub fn query(&self, db_type: RemoteDbType) -> String {
        match self {
            RemoteSource::Query(query) => query.clone(),
            RemoteSource::Table(table_identifiers) => db_type.select_all_query(table_identifiers),
        }
    }
}

impl std::fmt::Display for RemoteSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RemoteSource::Query(query) => write!(f, "{query}"),
            RemoteSource::Table(table) => write!(f, "{}", table.join(".")),
        }
    }
}

impl From<String> for RemoteSource {
    fn from(query: String) -> Self {
        RemoteSource::Query(query)
    }
}

impl From<&String> for RemoteSource {
    fn from(query: &String) -> Self {
        RemoteSource::Query(query.clone())
    }
}

impl From<&str> for RemoteSource {
    fn from(query: &str) -> Self {
        RemoteSource::Query(query.to_string())
    }
}

impl From<Vec<String>> for RemoteSource {
    fn from(table_identifiers: Vec<String>) -> Self {
        RemoteSource::Table(table_identifiers)
    }
}

impl From<Vec<&str>> for RemoteSource {
    fn from(table_identifiers: Vec<&str>) -> Self {
        RemoteSource::Table(
            table_identifiers
                .into_iter()
                .map(|s| s.to_string())
                .collect(),
        )
    }
}

impl From<Vec<&String>> for RemoteSource {
    fn from(table_identifiers: Vec<&String>) -> Self {
        RemoteSource::Table(table_identifiers.into_iter().cloned().collect())
    }
}

#[derive(Debug)]
pub struct RemoteTable {
    pub(crate) conn_options: Arc<ConnectionOptions>,
    pub(crate) source: RemoteSource,
    pub(crate) table_schema: SchemaRef,
    pub(crate) transformed_table_schema: SchemaRef,
    pub(crate) remote_schema: Option<RemoteSchemaRef>,
    pub(crate) transform: Arc<dyn Transform>,
    pub(crate) literalizer: Arc<dyn Literalize>,
    pub(crate) pool: Arc<dyn Pool>,
}

impl RemoteTable {
    pub async fn try_new(
        conn_options: impl Into<ConnectionOptions>,
        source: impl Into<RemoteSource>,
    ) -> DFResult<Self> {
        Self::try_new_with_schema_transform_literalizer(
            conn_options,
            source,
            None,
            None,
            Arc::new(DefaultTransform {}),
            Arc::new(DefaultLiteralizer {}),
        )
        .await
    }

    pub async fn try_new_with_schema(
        conn_options: impl Into<ConnectionOptions>,
        source: impl Into<RemoteSource>,
        table_schema: SchemaRef,
    ) -> DFResult<Self> {
        Self::try_new_with_schema_transform_literalizer(
            conn_options,
            source,
            Some(table_schema),
            None,
            Arc::new(DefaultTransform {}),
            Arc::new(DefaultLiteralizer {}),
        )
        .await
    }

    pub async fn try_new_with_remote_schema(
        conn_options: impl Into<ConnectionOptions>,
        source: impl Into<RemoteSource>,
        remote_schema: RemoteSchemaRef,
    ) -> DFResult<Self> {
        Self::try_new_with_schema_transform_literalizer(
            conn_options,
            source,
            None,
            Some(remote_schema),
            Arc::new(DefaultTransform {}),
            Arc::new(DefaultLiteralizer {}),
        )
        .await
    }

    pub async fn try_new_with_transform(
        conn_options: impl Into<ConnectionOptions>,
        source: impl Into<RemoteSource>,
        transform: Arc<dyn Transform>,
    ) -> DFResult<Self> {
        Self::try_new_with_schema_transform_literalizer(
            conn_options,
            source,
            None,
            None,
            transform,
            Arc::new(DefaultLiteralizer {}),
        )
        .await
    }

    pub async fn try_new_with_schema_transform(
        conn_options: impl Into<ConnectionOptions>,
        source: impl Into<RemoteSource>,
        table_schema: SchemaRef,
        transform: Arc<dyn Transform>,
    ) -> DFResult<Self> {
        Self::try_new_with_schema_transform_literalizer(
            conn_options,
            source,
            Some(table_schema),
            None,
            transform,
            Arc::new(DefaultLiteralizer {}),
        )
        .await
    }

    pub async fn try_new_with_remote_schema_transform(
        conn_options: impl Into<ConnectionOptions>,
        source: impl Into<RemoteSource>,
        remote_schema: RemoteSchemaRef,
        transform: Arc<dyn Transform>,
    ) -> DFResult<Self> {
        Self::try_new_with_schema_transform_literalizer(
            conn_options,
            source,
            None,
            Some(remote_schema),
            transform,
            Arc::new(DefaultLiteralizer {}),
        )
        .await
    }

    pub async fn try_new_with_schema_transform_literalizer(
        conn_options: impl Into<ConnectionOptions>,
        source: impl Into<RemoteSource>,
        table_schema: Option<SchemaRef>,
        remote_schema: Option<RemoteSchemaRef>,
        transform: Arc<dyn Transform>,
        literalizer: Arc<dyn Literalize>,
    ) -> DFResult<Self> {
        let conn_options = conn_options.into();
        let source = source.into();

        if let RemoteSource::Table(table) = &source
            && table.is_empty()
        {
            return Err(DataFusionError::Plan(
                "Table source is empty vec".to_string(),
            ));
        }

        let now = std::time::Instant::now();
        let pool = connect(&conn_options).await?;
        debug!(
            "[remote-table] Creating connection pool cost: {}ms",
            now.elapsed().as_millis()
        );

        let infer_schema_fn =
            async |pool: &Arc<dyn Pool>, source: &RemoteSource| -> DFResult<RemoteSchemaRef> {
                let now = std::time::Instant::now();
                let conn = pool.get().await?;
                let remote_schema = conn.infer_schema(source).await?;
                debug!(
                    "[remote-table] Inferring remote schema cost: {}ms",
                    now.elapsed().as_millis()
                );
                Ok(remote_schema)
            };

        let (table_schema, remote_schema_opt): (SchemaRef, Option<RemoteSchemaRef>) =
            match (table_schema, remote_schema) {
                (Some(table_schema), Some(remote_schema)) => (table_schema, Some(remote_schema)),
                (Some(table_schema), None) => {
                    let remote_schema = if transform.as_any().is::<DefaultTransform>()
                        && matches!(source, RemoteSource::Query(_))
                    {
                        None
                    } else {
                        // Infer remote schema
                        let remote_schema = infer_schema_fn(&pool, &source).await?;
                        Some(remote_schema)
                    };
                    (table_schema, remote_schema)
                }
                (None, Some(remote_schema)) => (
                    Arc::new(remote_schema.to_arrow_schema()),
                    Some(remote_schema),
                ),
                (None, None) => {
                    // Infer table schema
                    let remote_schema = infer_schema_fn(&pool, &source).await?;
                    let inferred_table_schema = Arc::new(remote_schema.to_arrow_schema());
                    (inferred_table_schema, Some(remote_schema))
                }
            };

        if let Some(remote_schema) = &remote_schema_opt
            && table_schema.fields.len() != remote_schema.fields.len()
        {
            return Err(DataFusionError::Plan(format!(
                "fields length of table schema is not matched with remote schema. table schema: {table_schema}, remote schema: {remote_schema:?}"
            )));
        }

        let transformed_table_schema = transform_schema(
            transform.as_ref(),
            table_schema.clone(),
            remote_schema_opt.as_ref(),
            conn_options.db_type(),
        )?;

        Ok(RemoteTable {
            conn_options: Arc::new(conn_options),
            source,
            table_schema,
            transformed_table_schema,
            remote_schema: remote_schema_opt,
            transform,
            literalizer,
            pool,
        })
    }

    pub fn remote_schema(&self) -> Option<RemoteSchemaRef> {
        self.remote_schema.clone()
    }

    pub fn pool(&self) -> &Arc<dyn Pool> {
        &self.pool
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
        let remote_schema = if self.transform.as_any().is::<DefaultTransform>() {
            Arc::new(RemoteSchema::empty())
        } else {
            let Some(remote_schema) = &self.remote_schema else {
                return Err(DataFusionError::Plan(
                    "remote schema is none but transform is not DefaultTransform".to_string(),
                ));
            };
            remote_schema.clone()
        };
        let mut unparsed_filters = vec![];
        for filter in filters {
            let args = TransformArgs {
                db_type: self.conn_options.db_type(),
                table_schema: &self.table_schema,
                remote_schema: &remote_schema,
            };
            unparsed_filters.push(self.transform.unparse_filter(filter, args)?);
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

        let remote_schema = if self.transform.as_any().is::<DefaultTransform>() {
            Arc::new(RemoteSchema::empty())
        } else {
            let Some(remote_schema) = &self.remote_schema else {
                return Err(DataFusionError::Plan(
                    "remote schema is none but transform is not DefaultTransform".to_string(),
                ));
            };
            remote_schema.clone()
        };

        let mut pushdown = vec![];
        for filter in filters {
            let args = TransformArgs {
                db_type: self.conn_options.db_type(),
                table_schema: &self.table_schema,
                remote_schema: &remote_schema,
            };
            pushdown.push(self.transform.support_filter_pushdown(filter, args)?);
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

        let RemoteSource::Table(table) = &self.source else {
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
            self.literalizer.clone(),
            table.clone(),
            remote_schema,
            conn,
        );
        Ok(Arc::new(exec))
    }
}
