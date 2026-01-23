use crate::{ConnectionOptions, DFResult, LazyPool, Literalize RemoteSchemaRef};
use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::stats::Precision;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties,
};
use futures::StreamExt;
use std::sync::Arc;

#[derive(Debug)]
pub struct RemoteTableInsertExec {
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) conn_options: Arc<ConnectionOptions>,
    pub(crate) pool: LazyPool,
    pub(crate) literalizer: Arc<dyn Literalize>,
    pub(crate) table: Vec<String>,
    pub(crate) remote_schema: RemoteSchemaRef,
    plan_properties: PlanProperties,
}

impl RemoteTableInsertExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        conn_options: Arc<ConnectionOptions>,
        pool: LazyPool,
        literalizer: Arc<dyn Literalize>,
        table: Vec<String>,
        remote_schema: RemoteSchemaRef,
    ) -> Self {
        // TODO sqlite does not support parallel insert
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(make_count_schema()),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            input.pipeline_behavior(),
            input.boundedness(),
        );
        Self {
            input,
            conn_options,
            pool,
            literalizer,
            table,
            remote_schema,
            plan_properties,
        }
    }
}

impl ExecutionPlan for RemoteTableInsertExec {
    fn name(&self) -> &str {
        "RemoteTableInsertExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let input = children[0].clone();
        let exec = Self::new(
            input,
            self.conn_options.clone(),
            self.pool.clone(),
            self.literalizer.clone(),
            self.table.clone(),
            self.remote_schema.clone(),
        );
        Ok(Arc::new(exec))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let mut input_stream = self.input.execute(partition, context)?;
        let conn_options = self.conn_options.clone();
        let literalizer = self.literalizer.clone();
        let table = self.table.clone();
        let remote_schema = self.remote_schema.clone();
        let pool = self.pool.clone();

        let stream = futures::stream::once(async move {
            let conn = pool.get().await?;

            let mut total_count = 0;
            while let Some(batch) = input_stream.next().await {
                let batch = batch?;
                let count = conn
                    .insert(
                        &conn_options,
                        literalizer.clone(),
                        &table,
                        remote_schema.clone(),
                        batch,
                    )
                    .await?;
                total_count += count;
            }
            make_result_batch(total_count as i64)
        })
        .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            make_count_schema(),
            stream,
        )))
    }
}

impl DisplayAs for RemoteTableInsertExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "RemoteTableInsertExec: table={}", self.table.join("."))?;
        if let Ok(stats) = self.input.partition_statistics(None) {
            match stats.num_rows {
                Precision::Exact(rows) => write!(f, ", rows={rows}")?,
                Precision::Inexact(rows) => write!(f, ", rows~={rows}")?,
                Precision::Absent => {}
            }
        }
        Ok(())
    }
}

fn make_result_batch(count: i64) -> DFResult<RecordBatch> {
    let schema = make_count_schema();
    let array = Arc::new(Int64Array::from(vec![count])) as ArrayRef;
    let batch = RecordBatch::try_new(schema, vec![array])?;
    Ok(batch)
}

fn make_count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::Int64,
        false,
    )]))
}
