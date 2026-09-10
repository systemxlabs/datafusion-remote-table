use super::schema::DOCUMENT_COLUMN;
use super::variant;
use crate::DFResult;
use arrow::array::{Array, ArrayRef, AsArray, RecordBatch, RecordBatchOptions};
use arrow::datatypes::SchemaRef;
use datafusion_common::{DataFusionError, project_schema};
use mongodb::bson::Document;
use parquet_variant::Variant;

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

/// Convert a record batch of documents back into BSON, one document per row.
pub(crate) fn batch_to_documents(batch: &RecordBatch) -> DFResult<Vec<Document>> {
    let index = batch.schema().index_of(DOCUMENT_COLUMN).map_err(|_| {
        DataFusionError::Execution(format!(
            "MongoDB insert requires a {DOCUMENT_COLUMN:?} column"
        ))
    })?;

    // The document column is a Variant: a struct of a shared metadata buffer
    // and one value buffer per row.
    let struct_array = batch.column(index).as_struct();
    let metadata = struct_array.column(0).as_binary_view();
    let values = struct_array.column(1).as_binary_view();

    let mut documents = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if struct_array.is_null(row) {
            return Err(DataFusionError::Execution(format!(
                "MongoDB insert requires a non-null {DOCUMENT_COLUMN:?} value, got null in row {row}"
            )));
        }
        let variant = Variant::try_new(metadata.value(row), values.value(row)).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to decode the {DOCUMENT_COLUMN:?} column as a Variant: {e:?}"
            ))
        })?;
        documents.push(variant::variant_to_document(&variant)?);
    }
    Ok(documents)
}
