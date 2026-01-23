use crate::{
    ConnectionOptions, DFResult, DefaultTransform, LazyPool, RemoteSchemaRef, RemoteSource,
    Transform, TransformStream, transform_schema,
};
use arrow::datatypes::SchemaRef;
use datafusion_common::DataFusionError;
use datafusion_common::Statistics;
use datafusion_common::stats::Precision;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_plan::display::ProjectSchemaDisplay;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, project_schema,
};
use futures::TryStreamExt;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct RemoteTableScanExec {
    pub(crate) conn_options: Arc<ConnectionOptions>,
    pub(crate) pool: LazyPool,
    pub(crate) source: RemoteSource,
    pub(crate) table_schema: SchemaRef,
    pub(crate) remote_schema: Option<RemoteSchemaRef>,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) unparsed_filters: Vec<String>,
    pub(crate) limit: Option<usize>,
    pub(crate) transform: Arc<dyn Transform>,
    pub(crate) row_count: Option<usize>,
    plan_properties: PlanProperties,
}

impl RemoteTableScanExec {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        conn_options: Arc<ConnectionOptions>,
        pool: LazyPool,
        source: RemoteSource,
        table_schema: SchemaRef,
        remote_schema: Option<RemoteSchemaRef>,
        projection: Option<Vec<usize>>,
        unparsed_filters: Vec<String>,
        limit: Option<usize>,
        transform: Arc<dyn Transform>,
        row_count: Option<usize>,
    ) -> DFResult<Self> {
        let transformed_table_schema = transform_schema(
            transform.as_ref(),
            table_schema.clone(),
            remote_schema.as_ref(),
            conn_options.db_type(),
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
            pool,
            source,
            table_schema,
            remote_schema,
            projection,
            unparsed_filters,
            limit,
            transform,
            row_count,
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
            self.pool.clone(),
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

        if let Some(count) = self.row_count {
            let column_stat = Statistics::unknown_column(self.schema().as_ref());
            let row_count_after_limit = if let Some(limit) = self.limit {
                std::cmp::min(count, limit)
            } else {
                count
            };
            Ok(Statistics {
                num_rows: Precision::Exact(row_count_after_limit),
                total_byte_size: Precision::Absent,
                column_statistics: column_stat,
            })
        } else {
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
                pool: self.pool.clone(),
                row_count: self.row_count,
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
    pool: LazyPool,
    conn_options: Arc<ConnectionOptions>,
    source: RemoteSource,
    table_schema: SchemaRef,
    remote_schema: Option<RemoteSchemaRef>,
    projection: Option<Vec<usize>>,
    unparsed_filters: Vec<String>,
    limit: Option<usize>,
    transform: Arc<dyn Transform>,
) -> DFResult<SendableRecordBatchStream> {
    let db_type = conn_options.db_type();
    let limit = if db_type.support_rewrite_with_filters_limit(&source) {
        limit
    } else {
        None
    };

    let conn = pool.get().await?;

    if transform.as_any().is::<DefaultTransform>() {
        conn.query(
            &conn_options,
            &source,
            table_schema.clone(),
            projection.as_ref(),
            unparsed_filters.as_slice(),
            limit,
        )
        .await
    } else {
        let Some(remote_schema) = remote_schema else {
            return Err(DataFusionError::Execution(
                "remote_schema is required for non-default transform".to_string(),
            ));
        };
        let stream = conn
            .query(
                &conn_options,
                &source,
                table_schema.clone(),
                None,
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
            db_type,
        )?))
    }
}

impl DisplayAs for RemoteTableScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "RemoteTableExec: source={}",
            match &self.source {
                RemoteSource::Query(_query) => "query".to_string(),
                RemoteSource::Table(table) => table.join("."),
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
