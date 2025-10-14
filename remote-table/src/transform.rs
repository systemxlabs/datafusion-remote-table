use crate::{DFResult, RemoteSchemaRef};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, project_schema};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use futures::{Stream, StreamExt};
use std::any::Any;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct TransformArgs<'a> {
    pub table_schema: &'a SchemaRef,
    pub remote_schema: &'a RemoteSchemaRef,
}

pub trait Transform: Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;

    fn transform(&self, batch: RecordBatch, args: TransformArgs) -> DFResult<RecordBatch>;
}

#[derive(Debug)]
pub struct DefaultTransform {}

impl Transform for DefaultTransform {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn transform(&self, batch: RecordBatch, _args: TransformArgs) -> DFResult<RecordBatch> {
        Ok(batch)
    }
}

pub(crate) struct TransformStream {
    input: SendableRecordBatchStream,
    transform: Arc<dyn Transform>,
    table_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    projected_transformed_schema: SchemaRef,
    remote_schema: RemoteSchemaRef,
}

impl TransformStream {
    pub fn try_new(
        input: SendableRecordBatchStream,
        transform: Arc<dyn Transform>,
        table_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        remote_schema: RemoteSchemaRef,
    ) -> DFResult<Self> {
        let input_schema = input.schema();
        if input.schema() != table_schema {
            return Err(DataFusionError::Execution(format!(
                "Transform stream input schema is not equals to table schema, input schema: {input_schema:?}, table schema: {table_schema:?}"
            )));
        }
        let transformed_table_schema = transform_schema(
            table_schema.clone(),
            transform.as_ref(),
            Some(&remote_schema),
        )?;
        let projected_transformed_schema =
            project_schema(&transformed_table_schema, projection.as_ref())?;
        Ok(Self {
            input,
            transform,
            table_schema,
            projection,
            projected_transformed_schema,
            remote_schema,
        })
    }
}

impl Stream for TransformStream {
    type Item = DFResult<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let args = TransformArgs {
                    table_schema: &self.table_schema,
                    remote_schema: &self.remote_schema,
                };
                match self.transform.transform(batch, args) {
                    Ok(transformed_batch) => {
                        let projected_batch = if let Some(projection) = &self.projection {
                            match transformed_batch.project(projection) {
                                Ok(batch) => batch,
                                Err(e) => return Poll::Ready(Some(Err(DataFusionError::from(e)))),
                            }
                        } else {
                            transformed_batch
                        };
                        Poll::Ready(Some(Ok(projected_batch)))
                    }
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
        self.projected_transformed_schema.clone()
    }
}

pub(crate) fn transform_schema(
    schema: SchemaRef,
    transform: &dyn Transform,
    remote_schema: Option<&RemoteSchemaRef>,
) -> DFResult<SchemaRef> {
    if transform.as_any().is::<DefaultTransform>() {
        Ok(schema)
    } else {
        let Some(remote_schema) = remote_schema else {
            return Err(DataFusionError::Execution(
                "remote_schema is required for non-default transform".to_string(),
            ));
        };
        let empty_batch = RecordBatch::new_empty(schema.clone());
        let args = TransformArgs {
            table_schema: &schema,
            remote_schema,
        };
        let transformed_batch = transform.transform(empty_batch, args)?;
        Ok(transformed_batch.schema())
    }
}
