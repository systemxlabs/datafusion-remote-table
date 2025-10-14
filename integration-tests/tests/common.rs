use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, Float64Array, Int64Array, RecordBatch, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::Column;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{Expr, SessionContext};
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use datafusion_remote_table::{
    DefaultTransform, RemotePhysicalCodec, RemoteSource, RemoteTable, SqliteConnectionOptions,
    Transform, TransformArgs, TransformCodec, transform_schema,
};
use integration_tests::setup_sqlite_db;
use std::any::Any;
use std::sync::Arc;

#[rstest::rstest]
#[case("SELECT * from supported_data_types".into())]
#[case(vec!["supported_data_types"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn transform_changing_field(#[case] source: RemoteSource) {
    let db_path = setup_sqlite_db();
    let options = SqliteConnectionOptions::new(db_path.clone());

    let table = RemoteTable::try_new_with_transform(options, source, Arc::new(MyTransform {}))
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

#[rstest::rstest]
#[case("SELECT * from supported_data_types".into())]
#[case(vec!["supported_data_types"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn transform_serialization(#[case] source: RemoteSource) {
    let db_path = setup_sqlite_db();
    let options = SqliteConnectionOptions::new(db_path.clone());

    let table = RemoteTable::try_new_with_transform(options, source, Arc::new(MyTransform {}))
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

    fn transform(
        &self,
        batch: RecordBatch,
        args: TransformArgs,
    ) -> Result<RecordBatch, DataFusionError> {
        let fields_len = args.table_schema.fields.len();
        let mut new_fields = Vec::with_capacity(fields_len);
        let mut new_arrays: Vec<ArrayRef> = Vec::with_capacity(fields_len);

        for i in 0..fields_len {
            let field = args.table_schema.field(i);
            let array = batch.column(i);
            let field_name = field.name();
            let field_type = field.data_type();
            let remote_type = &args.remote_schema.fields[i].remote_type;
            match field_type {
                DataType::Null => {
                    new_fields.push(Field::new(
                        format!("transformed_null-{field_name}"),
                        DataType::Utf8,
                        false,
                    ));

                    let mut data = Vec::with_capacity(array.len());
                    for _ in 0..array.len() {
                        data.push(format!(
                            "transform_null-{i}-{field_type}-{remote_type:?}-NULL",
                        ))
                    }
                    new_arrays.push(Arc::new(StringArray::from(data)));
                }
                DataType::Int64 => {
                    new_fields.push(Field::new(
                        format!("transformed_int64-{field_name}"),
                        DataType::Utf8,
                        false,
                    ));

                    let int64_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                    let mut data = Vec::with_capacity(array.len());
                    for row in int64_array.iter() {
                        data.push(format!(
                            "transform_int64-{i}-{field_type}-{remote_type:?}-{row:?}",
                        ))
                    }
                    new_arrays.push(Arc::new(StringArray::from(data)));
                }
                DataType::Float64 => {
                    new_fields.push(Field::new(
                        format!("transformed_float64-{field_name}"),
                        DataType::Utf8,
                        false,
                    ));

                    let float64_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                    let mut data = Vec::with_capacity(array.len());
                    for row in float64_array.iter() {
                        data.push(format!(
                            "transform_float64-{i}-{field_type}-{remote_type:?}-{row:?}",
                        ))
                    }
                    new_arrays.push(Arc::new(StringArray::from(data)));
                }
                DataType::Utf8 => {
                    new_fields.push(Field::new(
                        format!("transformed_utf8-{field_name}"),
                        DataType::Utf8,
                        false,
                    ));

                    let utf8_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                    let mut data = Vec::with_capacity(array.len());
                    for row in utf8_array.iter() {
                        data.push(format!(
                            "transform_utf8-{i}-{field_type}-{remote_type:?}-{row:?}",
                        ))
                    }
                    new_arrays.push(Arc::new(StringArray::from(data)));
                }
                DataType::Binary => {
                    new_fields.push(Field::new(
                        format!("transformed_binary-{field_name}"),
                        DataType::Utf8,
                        false,
                    ));

                    let binary_array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
                    let mut data = Vec::with_capacity(array.len());
                    for row in binary_array.iter() {
                        data.push(format!(
                            "transform_binary-{i}-{field_type}-{remote_type:?}-{row:?}",
                        ))
                    }
                    new_arrays.push(Arc::new(StringArray::from(data)));
                }
                _ => unreachable!(),
            }
        }

        let schema = Arc::new(Schema::new(new_fields));
        let batch = RecordBatch::try_new(schema, new_arrays)?;
        Ok(batch)
    }

    fn support_filter_pushdown(
        &self,
        filter: &Expr,
        args: TransformArgs,
    ) -> Result<TableProviderFilterPushDown, DataFusionError> {
        DefaultTransform {}.support_filter_pushdown(filter, args)
    }

    fn unparse_filter(
        &self,
        filter: &Expr,
        args: TransformArgs,
    ) -> Result<String, DataFusionError> {
        let transformed_table_schema = transform_schema(
            self,
            args.table_schema.clone(),
            Some(&args.remote_schema),
            args.db_type,
        )?;
        let rewritten_filter = filter
            .clone()
            .transform_down(|e| {
                if let Expr::Column(col) = e {
                    let col_idx = transformed_table_schema.index_of(col.name())?;
                    let row_name = args.table_schema.field(col_idx).name().to_string();
                    Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                        row_name,
                    ))))
                } else {
                    Ok(Transformed::no(e))
                }
            })?
            .data;
        DefaultTransform {}.unparse_filter(&rewritten_filter, args)
    }
}

#[rstest::rstest]
#[case("SELECT * from simple_table".into())]
#[case(vec!["simple_table"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn transform_adding_field(
    #[case] source: RemoteSource,
) -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Debug)]
    struct MyTransform;

    impl Transform for MyTransform {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn transform(
            &self,
            batch: RecordBatch,
            _args: TransformArgs,
        ) -> Result<RecordBatch, DataFusionError> {
            let mut fields = batch.schema().fields.to_vec();
            fields.push(Arc::new(Field::new("new_field", DataType::Utf8, true)));

            let mut arrays = batch.columns().to_vec();
            let new_array = StringArray::from_iter_values(vec!["new"; batch.num_rows()]);
            arrays.push(Arc::new(new_array));

            let new_schema = Arc::new(Schema::new(fields));
            let new_batch = RecordBatch::try_new(new_schema, arrays)?;
            Ok(new_batch)
        }

        fn support_filter_pushdown(
            &self,
            filter: &Expr,
            args: TransformArgs,
        ) -> Result<TableProviderFilterPushDown, DataFusionError> {
            DefaultTransform {}.support_filter_pushdown(filter, args)
        }

        fn unparse_filter(
            &self,
            filter: &Expr,
            args: TransformArgs,
        ) -> Result<String, DataFusionError> {
            DefaultTransform {}.unparse_filter(filter, args)
        }
    }

    let db_path = setup_sqlite_db();
    let options = SqliteConnectionOptions::new(db_path.clone());

    let table =
        RemoteTable::try_new_with_transform(options, source, Arc::new(MyTransform {})).await?;
    println!("remote schema: {:#?}", table.remote_schema());

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table))?;

    let result = ctx
        .sql("select * from remote_table")
        .await?
        .collect()
        .await?;
    let table_str = pretty_format_batches(&result)?.to_string();
    println!("{table_str}");

    assert_eq!(
        table_str,
        r#"+----+-------+-----------+
| id | name  | new_field |
+----+-------+-----------+
| 1  | Tom   | new       |
| 2  | Jerry | new       |
| 3  | Spike | new       |
+----+-------+-----------+"#,
    );

    Ok(())
}

#[rstest::rstest]
#[case("SELECT * from simple_table".into())]
#[case(vec!["simple_table"].into())]
#[tokio::test(flavor = "multi_thread")]
async fn transform_removing_field(
    #[case] source: RemoteSource,
) -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Debug)]
    struct MyTransform;

    impl Transform for MyTransform {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn transform(
            &self,
            batch: RecordBatch,
            _args: TransformArgs,
        ) -> Result<RecordBatch, DataFusionError> {
            let new_batch = batch.project(&[0])?;
            Ok(new_batch)
        }

        fn support_filter_pushdown(
            &self,
            filter: &Expr,
            args: TransformArgs,
        ) -> Result<TableProviderFilterPushDown, DataFusionError> {
            DefaultTransform {}.support_filter_pushdown(filter, args)
        }

        fn unparse_filter(
            &self,
            filter: &Expr,
            args: TransformArgs,
        ) -> Result<String, DataFusionError> {
            DefaultTransform {}.unparse_filter(filter, args)
        }
    }

    let db_path = setup_sqlite_db();
    let options = SqliteConnectionOptions::new(db_path.clone());

    let table =
        RemoteTable::try_new_with_transform(options, source, Arc::new(MyTransform {})).await?;
    println!("remote schema: {:#?}", table.remote_schema());

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table))?;

    let result = ctx
        .sql("select * from remote_table")
        .await?
        .collect()
        .await?;
    let table_str = pretty_format_batches(&result)?.to_string();
    println!("{table_str}");

    assert_eq!(
        table_str,
        r#"+----+
| id |
+----+
| 1  |
| 2  |
| 3  |
+----+"#,
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn insert_serialization() -> Result<(), DataFusionError> {
    let db_path = setup_sqlite_db();
    let options = SqliteConnectionOptions::new(db_path.clone());

    let table = RemoteTable::try_new(options, vec!["insert_exec_serialization_table"]).await?;
    println!("remote schema: {:#?}", table.remote_schema());

    let ctx = SessionContext::new();
    ctx.register_table("remote_table", Arc::new(table))?;

    let df = ctx
        .sql("INSERT INTO remote_table (id, name) VALUES (1, 'Eve')")
        .await?;
    let plan = df.create_physical_plan().await?;
    println!(
        "plan: {}",
        DisplayableExecutionPlan::new(plan.as_ref()).indent(true)
    );

    let codec = RemotePhysicalCodec::new();
    let mut plan_buf: Vec<u8> = vec![];
    let plan_proto = PhysicalPlanNode::try_from_physical_plan(plan, &codec)?;
    plan_proto.try_encode(&mut plan_buf)?;
    let new_plan: Arc<dyn ExecutionPlan> = PhysicalPlanNode::try_decode(&plan_buf)
        .and_then(|proto| proto.try_into_physical_plan(&ctx, &ctx.runtime_env(), &codec))?;
    println!(
        "deserialized plan: {}",
        DisplayableExecutionPlan::new(new_plan.as_ref()).indent(true)
    );

    let batches = collect(new_plan, ctx.task_ctx()).await?;
    let table_str = pretty_format_batches(&batches)?.to_string();
    println!("{}", table_str);
    assert_eq!(
        table_str,
        r#"+-------+
| count |
+-------+
| 1     |
+-------+"#,
    );

    let df = ctx.sql("SELECT * FROM remote_table").await?;
    let batches = df.collect().await?;
    let table_str = pretty_format_batches(&batches)?.to_string();
    println!("{}", table_str);
    assert_eq!(
        table_str,
        r#"+----+------+
| id | name |
+----+------+
| 1  | Eve  |
+----+------+"#,
    );

    Ok(())
}
