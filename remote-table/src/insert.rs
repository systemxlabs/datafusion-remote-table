use crate::{ConnectionOptions, DFResult, Literalize, Pool, RemoteSchemaRef, get_or_create_pool};
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
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct RemoteTableInsertExec {
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) conn_options: Arc<ConnectionOptions>,
    pub(crate) literalizer: Arc<dyn Literalize>,
    pub(crate) table: Vec<String>,
    pub(crate) remote_schema: RemoteSchemaRef,
    pub(crate) pool: Arc<Mutex<Option<Arc<dyn Pool>>>>,
    plan_properties: PlanProperties,
}

impl RemoteTableInsertExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        conn_options: Arc<ConnectionOptions>,
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
            literalizer,
            table,
            remote_schema,
            pool: Arc::new(Mutex::new(None)),
            plan_properties,
        }
    }

    pub fn with_pool(mut self, pool: Option<Arc<dyn Pool>>) -> Self {
        self.pool = Arc::new(Mutex::new(pool));
        self
    }

    pub fn with_mutex_pool(mut self, pool: Arc<Mutex<Option<Arc<dyn Pool>>>>) -> Self {
        self.pool = pool;
        self
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
            self.literalizer.clone(),
            self.table.clone(),
            self.remote_schema.clone(),
        )
        .with_mutex_pool(self.pool.clone());
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
        let pool_mutex = self.pool.clone();

        let stream = futures::stream::once(async move {
            let pool = get_or_create_pool(&pool_mutex, &conn_options).await?;
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
