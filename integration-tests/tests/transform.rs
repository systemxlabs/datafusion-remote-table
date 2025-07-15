use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, Float64Array, Int64Array, NullArray, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use datafusion_remote_table::{
    RemotePhysicalCodec, RemoteTable, SqliteConnectionOptions, Transform, TransformArgs,
    TransformCodec,
};
use integration_tests::shared_containers::setup_sqlite_db;
use std::any::Any;
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn transform() {
    let db_path = setup_sqlite_db();
    let options = SqliteConnectionOptions::new(db_path);

    let table = RemoteTable::try_new_with_transform(
        options,
        "SELECT * from supported_data_types",
        Arc::new(MyTransform {}),
    )
    .await
    .unwrap();
    println!("remote schema: {:#?}", table.remote_schema());

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table)).unwrap();

    let result = ctx
        .sql("select * from remote_table")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    println!("{}", pretty_format_batches(&result).unwrap());

    assert_eq!(
        pretty_format_batches(&result).unwrap().to_string(),
        r#"+-------------------------------------------------+-------------------------------------------------+-------------------------------------------------+-------------------------------------------------+-------------------------------------------------+-------------------------------------------------+-------------------------------------------------+----------------------------------------------------+----------------------------------------------------+----------------------------------------------------+-----------------------------------------------------+-----------------------------------------------------+-----------------------------------------------------+-----------------------------------------------------+-----------------------------------------------------+--------------------------------------------------+------------------------------------------------------+-----------------------------------------------------+----------------------------------------------------------+--------------------------------------------------+-------------------------------------------------------+---------------------------------------------------+---------------------------------------------------+---------------------------------------------------+---------------------------------------------------+---------------------------------------------------+
| transformed_int64-tinyint_col                   | transformed_int64-smallint_col                  | transformed_int64-int_col                       | transformed_int64-bigint_col                    | transformed_int64-int2_col                      | transformed_int64-int4_col                      | transformed_int64-int8_col                      | transformed_float64-float_col                      | transformed_float64-double_col                     | transformed_float64-real_col                       | transformed_float64-real_precision_col              | transformed_float64-real_precision_scale_col        | transformed_float64-numeric_col                     | transformed_float64-numeric_precision_col           | transformed_float64-numeric_precision_scale_col     | transformed_utf8-char_col                        | transformed_utf8-char_len_col                        | transformed_utf8-varchar_col                        | transformed_utf8-varchar_len_col                         | transformed_utf8-text_col                        | transformed_utf8-text_len_col                         | transformed_binary-binary_col                     | transformed_binary-binary_len_col                 | transformed_binary-varbinary_col                  | transformed_binary-varbinary_len_col              | transformed_binary-blob_col                       |
+-------------------------------------------------+-------------------------------------------------+-------------------------------------------------+-------------------------------------------------+-------------------------------------------------+-------------------------------------------------+-------------------------------------------------+----------------------------------------------------+----------------------------------------------------+----------------------------------------------------+-----------------------------------------------------+-----------------------------------------------------+-----------------------------------------------------+-----------------------------------------------------+-----------------------------------------------------+--------------------------------------------------+------------------------------------------------------+-----------------------------------------------------+----------------------------------------------------------+--------------------------------------------------+-------------------------------------------------------+---------------------------------------------------+---------------------------------------------------+---------------------------------------------------+---------------------------------------------------+---------------------------------------------------+
| transform_int64-0-Int64-Sqlite(Integer)-Some(1) | transform_int64-1-Int64-Sqlite(Integer)-Some(2) | transform_int64-2-Int64-Sqlite(Integer)-Some(3) | transform_int64-3-Int64-Sqlite(Integer)-Some(4) | transform_int64-4-Int64-Sqlite(Integer)-Some(5) | transform_int64-5-Int64-Sqlite(Integer)-Some(6) | transform_int64-6-Int64-Sqlite(Integer)-Some(7) | transform_float64-7-Float64-Sqlite(Real)-Some(1.1) | transform_float64-8-Float64-Sqlite(Real)-Some(2.2) | transform_float64-9-Float64-Sqlite(Real)-Some(3.3) | transform_float64-10-Float64-Sqlite(Real)-Some(4.4) | transform_float64-11-Float64-Sqlite(Real)-Some(5.5) | transform_float64-12-Float64-Sqlite(Real)-Some(6.6) | transform_float64-13-Float64-Sqlite(Real)-Some(7.7) | transform_float64-14-Float64-Sqlite(Real)-Some(8.8) | transform_utf8-15-Utf8-Sqlite(Text)-Some("char") | transform_utf8-16-Utf8-Sqlite(Text)-Some("char(10)") | transform_utf8-17-Utf8-Sqlite(Text)-Some("varchar") | transform_utf8-18-Utf8-Sqlite(Text)-Some("varchar(120)") | transform_utf8-19-Utf8-Sqlite(Text)-Some("text") | transform_utf8-20-Utf8-Sqlite(Text)-Some("text(200)") | transform_binary-21-Binary-Sqlite(Blob)-Some([1]) | transform_binary-22-Binary-Sqlite(Blob)-Some([2]) | transform_binary-23-Binary-Sqlite(Blob)-Some([3]) | transform_binary-24-Binary-Sqlite(Blob)-Some([4]) | transform_binary-25-Binary-Sqlite(Blob)-Some([5]) |
| transform_int64-0-Int64-Sqlite(Integer)-None    | transform_int64-1-Int64-Sqlite(Integer)-None    | transform_int64-2-Int64-Sqlite(Integer)-None    | transform_int64-3-Int64-Sqlite(Integer)-None    | transform_int64-4-Int64-Sqlite(Integer)-None    | transform_int64-5-Int64-Sqlite(Integer)-None    | transform_int64-6-Int64-Sqlite(Integer)-None    | transform_float64-7-Float64-Sqlite(Real)-None      | transform_float64-8-Float64-Sqlite(Real)-None      | transform_float64-9-Float64-Sqlite(Real)-None      | transform_float64-10-Float64-Sqlite(Real)-None      | transform_float64-11-Float64-Sqlite(Real)-None      | transform_float64-12-Float64-Sqlite(Real)-None      | transform_float64-13-Float64-Sqlite(Real)-None      | transform_float64-14-Float64-Sqlite(Real)-None      | transform_utf8-15-Utf8-Sqlite(Text)-None         | transform_utf8-16-Utf8-Sqlite(Text)-None             | transform_utf8-17-Utf8-Sqlite(Text)-None            | transform_utf8-18-Utf8-Sqlite(Text)-None                 | transform_utf8-19-Utf8-Sqlite(Text)-None         | transform_utf8-20-Utf8-Sqlite(Text)-None              | transform_binary-21-Binary-Sqlite(Blob)-None      | transform_binary-22-Binary-Sqlite(Blob)-None      | transform_binary-23-Binary-Sqlite(Blob)-None      | transform_binary-24-Binary-Sqlite(Blob)-None      | transform_binary-25-Binary-Sqlite(Blob)-None      |
+-------------------------------------------------+-------------------------------------------------+-------------------------------------------------+-------------------------------------------------+-------------------------------------------------+-------------------------------------------------+-------------------------------------------------+----------------------------------------------------+----------------------------------------------------+----------------------------------------------------+-----------------------------------------------------+-----------------------------------------------------+-----------------------------------------------------+-----------------------------------------------------+-----------------------------------------------------+--------------------------------------------------+------------------------------------------------------+-----------------------------------------------------+----------------------------------------------------------+--------------------------------------------------+-------------------------------------------------------+---------------------------------------------------+---------------------------------------------------+---------------------------------------------------+---------------------------------------------------+---------------------------------------------------+"#,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn transform_serialization() {
    let db_path = setup_sqlite_db();
    let options = SqliteConnectionOptions::new(db_path);

    let table = RemoteTable::try_new_with_transform(
        options,
        "select * from supported_data_types",
        Arc::new(MyTransform {}),
    )
    .await
    .unwrap();
    println!("remote schema: {:#?}", table.remote_schema());

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table)).unwrap();
    let plan = ctx
        .sql(r#"select * from remote_table where "transformed_int64-tinyint_col" = 1 limit 1"#)
        .await
        .unwrap();
    let exec_plan = plan.create_physical_plan().await.unwrap();
    println!(
        "plan: {}",
        DisplayableExecutionPlan::new(exec_plan.as_ref()).indent(true)
    );
    let result = collect(exec_plan.clone(), ctx.task_ctx()).await.unwrap();
    println!("{}", pretty_format_batches(&result).unwrap());

    let codec = RemotePhysicalCodec::new()
        .with_transform_codec(Arc::new(MyTransformCodec {}) as Arc<dyn TransformCodec>);
    let mut plan_buf: Vec<u8> = vec![];
    let plan_proto = PhysicalPlanNode::try_from_physical_plan(exec_plan, &codec).unwrap();
    plan_proto.try_encode(&mut plan_buf).unwrap();

    let new_plan: Arc<dyn ExecutionPlan> = PhysicalPlanNode::try_decode(&plan_buf)
        .and_then(|proto| proto.try_into_physical_plan(&ctx, &ctx.runtime_env(), &codec))
        .unwrap();
    println!(
        "plan: {}",
        DisplayableExecutionPlan::new(new_plan.as_ref()).indent(true)
    );

    let serde_result = collect(new_plan, ctx.task_ctx()).await.unwrap();
    println!("{}", pretty_format_batches(&serde_result).unwrap());

    assert_eq!(
        pretty_format_batches(&result).unwrap().to_string(),
        pretty_format_batches(&serde_result).unwrap().to_string()
    );
}

#[derive(Debug)]
pub struct MyTransformCodec {}

impl TransformCodec for MyTransformCodec {
    fn try_encode(&self, value: &dyn Transform) -> Result<Vec<u8>, DataFusionError> {
        if value.as_any().downcast_ref::<MyTransform>().is_some() {
            Ok("MyTransform".as_bytes().to_vec())
        } else {
            Err(DataFusionError::Internal(
                "Unexpected transform type".to_string(),
            ))
        }
    }

    fn try_decode(&self, value: &[u8]) -> Result<Arc<dyn Transform>, DataFusionError> {
        if value == "MyTransform".as_bytes() {
            Ok(Arc::new(MyTransform {}))
        } else {
            Err(DataFusionError::Internal(
                "Unexpected transform type".to_string(),
            ))
        }
    }
}

#[derive(Debug)]
pub struct MyTransform {}

impl Transform for MyTransform {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn transform_null(
        &self,
        array: &NullArray,
        args: TransformArgs,
    ) -> Result<(ArrayRef, Field), DataFusionError> {
        let mut data = Vec::with_capacity(array.len());
        for _ in 0..array.len() {
            data.push(format!(
                "transform_null-{}-{}-{:?}-NULL",
                args.col_index,
                args.field.data_type(),
                args.remote_field.as_ref().unwrap().remote_type
            ))
        }
        Ok((
            Arc::new(StringArray::from(data)),
            Field::new(
                format!("transformed_null-{}", args.field.name()),
                DataType::Utf8,
                false,
            ),
        ))
    }

    fn transform_int64(
        &self,
        array: &Int64Array,
        args: TransformArgs,
    ) -> Result<(ArrayRef, Field), DataFusionError> {
        let mut data = Vec::with_capacity(array.len());
        for row in array.iter() {
            data.push(format!(
                "transform_int64-{}-{}-{:?}-{row:?}",
                args.col_index,
                args.field.data_type(),
                args.remote_field.as_ref().unwrap().remote_type
            ))
        }
        Ok((
            Arc::new(StringArray::from(data)),
            Field::new(
                format!("transformed_int64-{}", args.field.name()),
                DataType::Utf8,
                false,
            ),
        ))
    }

    fn transform_float64(
        &self,
        array: &Float64Array,
        args: TransformArgs,
    ) -> Result<(ArrayRef, Field), DataFusionError> {
        let mut data = Vec::with_capacity(array.len());
        for row in array.iter() {
            data.push(format!(
                "transform_float64-{}-{}-{:?}-{row:?}",
                args.col_index,
                args.field.data_type(),
                args.remote_field.as_ref().unwrap().remote_type
            ))
        }
        Ok((
            Arc::new(StringArray::from(data)),
            Field::new(
                format!("transformed_float64-{}", args.field.name()),
                DataType::Utf8,
                false,
            ),
        ))
    }

    fn transform_utf8(
        &self,
        array: &StringArray,
        args: TransformArgs,
    ) -> Result<(ArrayRef, Field), DataFusionError> {
        let mut data = Vec::with_capacity(array.len());
        for row in array.iter() {
            data.push(format!(
                "transform_utf8-{}-{}-{:?}-{row:?}",
                args.col_index,
                args.field.data_type(),
                args.remote_field.as_ref().unwrap().remote_type
            ))
        }
        Ok((
            Arc::new(StringArray::from(data)),
            Field::new(
                format!("transformed_utf8-{}", args.field.name()),
                DataType::Utf8,
                false,
            ),
        ))
    }

    fn transform_binary(
        &self,
        array: &BinaryArray,
        args: TransformArgs,
    ) -> Result<(ArrayRef, Field), DataFusionError> {
        let mut data = Vec::with_capacity(array.len());
        for row in array.iter() {
            data.push(format!(
                "transform_binary-{}-{}-{:?}-{row:?}",
                args.col_index,
                args.field.data_type(),
                args.remote_field.as_ref().unwrap().remote_type
            ))
        }
        Ok((
            Arc::new(StringArray::from(data)),
            Field::new(
                format!("transformed_binary-{}", args.field.name()),
                DataType::Utf8,
                false,
            ),
        ))
    }
}
