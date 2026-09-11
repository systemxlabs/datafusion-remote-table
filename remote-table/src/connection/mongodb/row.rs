use super::variant;
use crate::DFResult;
use arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow::datatypes::SchemaRef;
use datafusion_common::project_schema;
use mongodb::bson::Document;

/// Convert a chunk of documents into one record batch holding the Variant
/// document column, honouring the projection (which is either all of it or, for
/// a count, none of it).
pub(crate) fn documents_to_batch(
    documents: &[Document],
    table_schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> DFResult<RecordBatch> {
    let projected_schema = project_schema(table_schema, projection)?;
    let indices: Vec<usize> = match projection {
        Some(projection) => projection.clone(),
        None => (0..table_schema.fields().len()).collect(),
    };

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(indices.len());
    for _ in indices {
        // The document column is the only column there is.
        columns.push(variant::documents_to_variant_array(documents)?);
    }

    let options = RecordBatchOptions::new().with_row_count(Some(documents.len()));
    RecordBatch::try_new_with_options(projected_schema, columns, &options).map_err(Into::into)
}
