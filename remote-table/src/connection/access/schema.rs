use crate::AccessType;
use crate::DFResult;
use crate::RemoteField;
use crate::RemoteSchema;
use crate::RemoteType;
use datafusion_common::DataFusionError;
use odbc_api::CursorImpl;
use odbc_api::ResultSetMetadata;
use odbc_api::handles::{AsStatementRef, ColumnDescription, Statement, StatementImpl};

pub(super) fn build_remote_schema(mut cursor: CursorImpl<StatementImpl>) -> DFResult<RemoteSchema> {
    let col_count = cursor
        .num_result_cols()
        .map_err(|e| DataFusionError::External(Box::new(e)))? as u16;
    let mut remote_fields = vec![];
    for i in 1..=col_count {
        // mdbtools' libmdbodbc.so doesn't fully support the higher-level
        // SQLColAttribute wrappers (cursor.col_name / col_data_type /
        // col_nullability return NoDiagnostics), so we go through the
        // low-level SQLDescribeCol path.
        let mut col_desc = ColumnDescription::default();
        let describe_result = cursor.as_stmt_ref().describe_col(i, &mut col_desc);
        if describe_result.is_err() {
            return Err(DataFusionError::Plan(format!(
                "describe_col failed for column {i} on access"
            )));
        }
        // mdbtools is the only driver this backend talks to and it reports
        // column names in the file's code page (CP1252/CP936/etc.), not UTF-8.
        // from_utf8_lossy keeps the printable bytes and substitutes U+FFFD for
        // the rest, so non-ASCII names don't abort a batch.
        let col_name = String::from_utf8_lossy(&col_desc.name).into_owned();
        let col_nullable = col_desc.nullability.could_be_nullable();

        let remote_type = RemoteType::Access(access_type_to_remote_type(col_desc.data_type)?);
        remote_fields.push(RemoteField::new(col_name, remote_type, col_nullable));
    }

    Ok(RemoteSchema::new(remote_fields))
}

fn access_type_to_remote_type(data_type: odbc_api::DataType) -> DFResult<AccessType> {
    match data_type {
        odbc_api::DataType::Bit => Ok(AccessType::Bit),
        odbc_api::DataType::TinyInt => Ok(AccessType::TinyInt),
        odbc_api::DataType::SmallInt => Ok(AccessType::SmallInt),
        odbc_api::DataType::Integer => Ok(AccessType::Integer),
        odbc_api::DataType::Real => Ok(AccessType::Real),
        odbc_api::DataType::Double => Ok(AccessType::Double),
        odbc_api::DataType::Numeric { .. } | odbc_api::DataType::Decimal { .. } => {
            Ok(AccessType::Currency)
        }
        odbc_api::DataType::Char { length } | odbc_api::DataType::WVarchar { length } => {
            Ok(AccessType::Text(length.map(|l| l.get() as u16)))
        }
        odbc_api::DataType::Varchar { length } => {
            Ok(AccessType::Text(length.map(|l| l.get() as u16)))
        }
        odbc_api::DataType::LongVarchar { .. } | odbc_api::DataType::WLongVarchar { .. } => {
            Ok(AccessType::Memo)
        }
        odbc_api::DataType::Binary { length } | odbc_api::DataType::Varbinary { length } => {
            Ok(AccessType::Binary(length.map(|l| l.get() as u16)))
        }
        odbc_api::DataType::LongVarbinary { .. } => Ok(AccessType::OleObject),
        odbc_api::DataType::Timestamp { .. } => Ok(AccessType::DateTime),
        odbc_api::DataType::Date => Ok(AccessType::Date),
        odbc_api::DataType::Time { .. } => Ok(AccessType::Time),
        odbc_api::DataType::WChar { length } => {
            Ok(AccessType::Text(length.map(|l| l.get() as u16)))
        }
        odbc_api::DataType::Other { data_type, .. }
            if data_type == odbc_api::sys::SqlDataType::EXT_GUID =>
        {
            Ok(AccessType::Guid)
        }
        _ => Err(DataFusionError::Execution(format!(
            "Unsupported Access type: {data_type:?}"
        ))),
    }
}
