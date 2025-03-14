use crate::transform::transform_batch;
use crate::{Connection, ConnectionOptions, DFResult, RemoteSchema, Transform};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{Stream, StreamExt, TryStreamExt};
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug)]
pub struct RemoteTableExec {
    pub(crate) conn_options: ConnectionOptions,
    pub(crate) sql: String,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) transform: Option<Arc<dyn Transform>>,
    conn: Arc<dyn Connection>,
    plan_properties: PlanProperties,
}

impl RemoteTableExec {
    pub fn new(
        conn_options: ConnectionOptions,
        projected_schema: SchemaRef,
        sql: String,
        projection: Option<Vec<usize>>,
        transform: Option<Arc<dyn Transform>>,
        conn: Arc<dyn Connection>,
    ) -> Self {
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            conn_options,
            sql,
            projection,
            transform,
            conn,
            plan_properties,
        }
    }
}

impl ExecutionPlan for RemoteTableExec {
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
            self.sql.clone(),
            self.projection.clone(),
            self.transform.clone(),
            schema.clone(),
        );
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

async fn build_and_transform_stream(
    conn: Arc<dyn Connection>,
    sql: String,
    projection: Option<Vec<usize>>,
    transform: Option<Arc<dyn Transform>>,
    projected_schema: SchemaRef,
) -> DFResult<SendableRecordBatchStream> {
    let (stream, remote_schema) = conn.query(sql, projection).await?;
    assert_eq!(projected_schema.fields().len(), remote_schema.fields.len());
    if let Some(transform) = transform.as_ref() {
        Ok(Box::pin(TransformStream {
            input: stream,
            transform: transform.clone(),
            schema: projected_schema,
            remote_schema,
        }))
    } else {
        Ok(stream)
    }
}

pub(crate) struct TransformStream {
    input: SendableRecordBatchStream,
    transform: Arc<dyn Transform>,
    schema: SchemaRef,
    remote_schema: RemoteSchema,
}

impl Stream for TransformStream {
    type Item = DFResult<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                match transform_batch(batch, self.transform.as_ref(), &self.remote_schema) {
                    Ok(result) => Poll::Ready(Some(Ok(result))),
                    Err(e) => Poll::Ready(Some(Err(e))),
                }
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for TransformStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl DisplayAs for RemoteTableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "RemoteTableExec")
    }
}
