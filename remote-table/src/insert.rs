use crate::{Connection, ConnectionOptions, DFResult, RemoteSchemaRef, Unparse};
use datafusion::arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties,
};
use futures::StreamExt;
use std::sync::Arc;

#[derive(Debug)]
pub struct RemoteTableInsertExec {
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) conn_options: ConnectionOptions,
    pub(crate) unparser: Arc<dyn Unparse>,
    pub(crate) table: Vec<String>,
    pub(crate) remote_schema: RemoteSchemaRef,
    pub(crate) conn: Arc<dyn Connection>,
    plan_properties: PlanProperties,
}

impl RemoteTableInsertExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        conn_options: ConnectionOptions,
        unparser: Arc<dyn Unparse>,
        table: Vec<String>,
        remote_schema: RemoteSchemaRef,
        conn: Arc<dyn Connection>,
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
            unparser,
            table,
            remote_schema,
            conn,
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
            self.unparser.clone(),
            self.table.clone(),
            self.remote_schema.clone(),
            self.conn.clone(),
        );
        Ok(Arc::new(exec))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let conn_options = self.conn_options.clone();
        let unparser = self.unparser.clone();
        let table = self.table.clone();
        let remote_schema = self.remote_schema.clone();
        let conn = self.conn.clone();

        let stream = futures::stream::once(async move {
            let count = conn
                .insert(
                    &conn_options,
                    unparser.clone(),
                    &table,
                    remote_schema.clone(),
                    input_stream,
                )
                .await?;
            make_result_batch(count as u64)
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
        write!(f, "RemoteTableInsertExec: table={}", self.table.join("."))
    }
}

fn make_result_batch(count: u64) -> DFResult<RecordBatch> {
    let schema = make_count_schema();
    let array = Arc::new(UInt64Array::from(vec![count])) as ArrayRef;
    let batch = RecordBatch::try_new(schema, vec![array])?;
    Ok(batch)
}

fn make_count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}
