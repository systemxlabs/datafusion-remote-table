use crate::{
    Connection, ConnectionOptions, DFResult, RemoteSchemaRef, TableSource, Transform,
    TransformStream, transform_schema,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Statistics;
use datafusion::common::stats::Precision;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::display::ProjectSchemaDisplay;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, project_schema,
};
use futures::TryStreamExt;
use log::{debug, warn};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct RemoteTableScanExec {
    pub(crate) conn_options: ConnectionOptions,
    pub(crate) source: TableSource,
    pub(crate) table_schema: SchemaRef,
    pub(crate) remote_schema: Option<RemoteSchemaRef>,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) unparsed_filters: Vec<String>,
    pub(crate) limit: Option<usize>,
    pub(crate) transform: Arc<dyn Transform>,
    pub(crate) conn: Arc<dyn Connection>,
    plan_properties: PlanProperties,
}

impl RemoteTableScanExec {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        conn_options: ConnectionOptions,
        source: TableSource,
        table_schema: SchemaRef,
        remote_schema: Option<RemoteSchemaRef>,
        projection: Option<Vec<usize>>,
        unparsed_filters: Vec<String>,
        limit: Option<usize>,
        transform: Arc<dyn Transform>,
        conn: Arc<dyn Connection>,
    ) -> DFResult<Self> {
        let transformed_table_schema = transform_schema(
            table_schema.clone(),
            transform.as_ref(),
            remote_schema.as_ref(),
        )?;
        let projected_schema = project_schema(&transformed_table_schema, projection.as_ref())?;
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Ok(Self {
            conn_options,
            source,
            table_schema,
            remote_schema,
            projection,
            unparsed_filters,
            limit,
            transform,
            conn,
            plan_properties,
        })
    }
}

impl ExecutionPlan for RemoteTableScanExec {
    fn name(&self) -> &str {
        "RemoteTableExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0);
        let schema = self.schema();
        let fut = build_and_transform_stream(
            self.conn.clone(),
            self.conn_options.clone(),
            self.source.clone(),
            self.table_schema.clone(),
            self.remote_schema.clone(),
            self.projection.clone(),
            self.unparsed_filters.clone(),
            self.limit,
            self.transform.clone(),
        );
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DFResult<Statistics> {
        if let Some(partition) = partition
            && partition != 0
        {
            return Err(DataFusionError::Plan(format!(
                "Invalid partition index: {partition}"
            )));
        }
        let db_type = self.conn_options.db_type();
        let limit = if db_type.support_rewrite_with_filters_limit(&self.source) {
            self.limit
        } else {
            None
        };
        let real_sql = db_type.rewrite_query(&self.source, &self.unparsed_filters, limit);

        if let Some(count1_query) = db_type.try_count1_query(&TableSource::Query(real_sql)) {
            let conn = self.conn.clone();
            let conn_options = self.conn_options.clone();
            let row_count_result = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    db_type
                        .fetch_count(conn, &conn_options, &count1_query)
                        .await
                })
            });

            match row_count_result {
                Ok(row_count) => {
                    let column_stat = Statistics::unknown_column(self.schema().as_ref());
                    Ok(Statistics {
                        num_rows: Precision::Exact(row_count),
                        total_byte_size: Precision::Absent,
                        column_statistics: column_stat,
                    })
                }
                Err(e) => {
                    warn!("[remote-table] Failed to fetch exec statistics: {e}");
                    Err(e)
                }
            }
        } else {
            debug!(
                "[remote-table] Query can not be rewritten as count1 query: {}",
                self.source
            );
            Ok(Statistics::new_unknown(self.schema().as_ref()))
        }
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        let db_type = self.conn_options.db_type();
        if db_type.support_rewrite_with_filters_limit(&self.source) {
            Some(Arc::new(Self {
                conn_options: self.conn_options.clone(),
                source: self.source.clone(),
                table_schema: self.table_schema.clone(),
                remote_schema: self.remote_schema.clone(),
                projection: self.projection.clone(),
                unparsed_filters: self.unparsed_filters.clone(),
                limit,
                transform: self.transform.clone(),
                conn: self.conn.clone(),
                plan_properties: self.plan_properties.clone(),
            }))
        } else {
            None
        }
    }

    fn fetch(&self) -> Option<usize> {
        self.limit
    }
}

#[allow(clippy::too_many_arguments)]
async fn build_and_transform_stream(
    conn: Arc<dyn Connection>,
    conn_options: ConnectionOptions,
    source: TableSource,
    table_schema: SchemaRef,
    remote_schema: Option<RemoteSchemaRef>,
    projection: Option<Vec<usize>>,
    unparsed_filters: Vec<String>,
    limit: Option<usize>,
    transform: Arc<dyn Transform>,
) -> DFResult<SendableRecordBatchStream> {
    let limit = if conn_options
        .db_type()
        .support_rewrite_with_filters_limit(&source)
    {
        limit
    } else {
        None
    };

    let stream = conn
        .query(
            &conn_options,
            &source,
            table_schema.clone(),
            projection.as_ref(),
            unparsed_filters.as_slice(),
            limit,
        )
        .await?;

    Ok(Box::pin(TransformStream::try_new(
        stream,
        transform.clone(),
        table_schema,
        projection,
        remote_schema,
    )?))
}

impl DisplayAs for RemoteTableScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "RemoteTableExec: source={}",
            match &self.source {
                TableSource::Query(_query) => "query".to_string(),
                TableSource::Table(table) => table.join("."),
            }
        )?;
        let projected_schema = self.schema();
        if has_projection(self.projection.as_ref(), self.table_schema.fields().len()) {
            write!(
                f,
                ", projection={}",
                ProjectSchemaDisplay(&projected_schema)
            )?;
        }
        if let Some(limit) = self.limit {
            write!(f, ", limit={limit}")?;
        }
        if !self.unparsed_filters.is_empty() {
            write!(f, ", filters=[{}]", self.unparsed_filters.join(", "))?;
        }
        Ok(())
    }
}

fn has_projection(projection: Option<&Vec<usize>>, table_columns: usize) -> bool {
    if let Some(projection) = projection {
        if projection.len() != table_columns {
            return true;
        }
        for (i, index) in projection.iter().enumerate() {
            if *index != i {
                return true;
            }
        }
    }
    false
}
