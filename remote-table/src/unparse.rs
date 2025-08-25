use crate::{DFResult, RemoteDbType, RemoteType};
use datafusion::arrow::array::{Array, BooleanArray, Int8Array, Int16Array, NullArray};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use std::fmt::Debug;

pub trait Unparse: Debug + Send + Sync {
    fn support_filter_pushdown(
        &self,
        filter: &Expr,
        db_type: RemoteDbType,
    ) -> DFResult<TableProviderFilterPushDown>;

    fn unparse_filter(&self, filter: &Expr, db_type: RemoteDbType) -> DFResult<String>;

    fn unparse_null_array(
        &self,
        array: &NullArray,
        _remote_type: RemoteType,
    ) -> DFResult<Vec<String>> {
        Ok(vec!["NULL".to_string(); array.len()])
    }

    fn unparse_boolean_array(
        &self,
        _array: &BooleanArray,
        _remote_type: RemoteType,
    ) -> DFResult<Vec<String>> {
        todo!()
    }

    fn unparse_int8_array(
        &self,
        array: &Int8Array,
        _remote_type: RemoteType,
    ) -> DFResult<Vec<String>> {
        let mut sqls = Vec::with_capacity(array.len());
        for i in 0..array.len() {
            sqls.push(array.value(i).to_string());
        }
        Ok(sqls)
    }

    fn unparse_int16_array(
        &self,
        array: &Int16Array,
        _remote_type: RemoteType,
    ) -> DFResult<Vec<String>> {
        let mut sqls = Vec::with_capacity(array.len());
        for i in 0..array.len() {
            sqls.push(array.value(i).to_string());
        }
        Ok(sqls)
    }
}

#[derive(Debug)]
pub struct DefaultUnparser {}

impl Unparse for DefaultUnparser {
    fn support_filter_pushdown(
        &self,
        filter: &Expr,
        db_type: RemoteDbType,
    ) -> DFResult<TableProviderFilterPushDown> {
        let unparser = match db_type.create_unparser() {
            Ok(unparser) => unparser,
            Err(_) => return Ok(TableProviderFilterPushDown::Unsupported),
        };
        if unparser.expr_to_sql(filter).is_err() {
            return Ok(TableProviderFilterPushDown::Unsupported);
        }

        let mut pushdown = TableProviderFilterPushDown::Exact;
        filter
            .apply(|e| {
                if matches!(e, Expr::ScalarFunction(_)) {
                    pushdown = TableProviderFilterPushDown::Unsupported;
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .expect("won't fail");

        Ok(pushdown)
    }

    fn unparse_filter(&self, filter: &Expr, db_type: RemoteDbType) -> DFResult<String> {
        let unparser = db_type.create_unparser()?;
        let ast = unparser.expr_to_sql(filter)?;
        Ok(format!("{ast}"))
    }
}
