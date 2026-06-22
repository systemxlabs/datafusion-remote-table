use crate::DFResult;
use crate::MdbType;
use crate::RemoteField;
use crate::RemoteSchema;
use crate::RemoteType;
use datafusion_common::DataFusionError;
use odbc_api::Prepared;
use odbc_api::ResultSetMetadata;
use odbc_api::handles::{AsStatementRef, ColumnDescription, Statement, StatementImpl};

/// Lossy decode of an ODBC column name. `ColumnDescription.name` is
/// `Vec<SqlChar>` where `SqlChar` is `u8` on non-Windows (odbc-api default)
/// and `u16` on Windows (odbc-api default). Both stdlib helpers substitute
/// U+FFFD for invalid input; mdbtools is the only driver this crate uses that
/// emits non-UTF8 names (CP1252/CP936), so the lossy path is the right
/// behaviour on both sides.
///
/// If you enable odbc-api's `wide` or `narrow` feature, you must also declare
/// the corresponding feature in this crate so this dispatch stays in sync.
#[cfg(not(target_os = "windows"))]
fn sql_chars_to_string_lossy(name: &[u8]) -> String {
    String::from_utf8_lossy(name).into_owned()
}
#[cfg(target_os = "windows")]
fn sql_chars_to_string_lossy(name: &[u16]) -> String {
    String::from_utf16_lossy(name)
}

pub(super) fn build_remote_schema(
    prepared: &mut Prepared<StatementImpl<'_>>,
) -> DFResult<RemoteSchema> {
    let col_count = prepared
        .num_result_cols()
        .map_err(|e| DataFusionError::External(Box::new(e)))? as u16;
    let mut remote_fields = vec![];
    for i in 1..=col_count {
        // mdbtools' libmdbodbc.so doesn't fully support the higher-level
        // SQLColAttribute wrappers (cursor.col_name / col_data_type /
        // col_nullability return NoDiagnostics), so we go through the
        // low-level SQLDescribeCol path.
        let mut col_desc = ColumnDescription::default();
        let describe_result = prepared.as_stmt_ref().describe_col(i, &mut col_desc);
        if describe_result.is_err() {
            return Err(DataFusionError::Plan(format!(
                "describe_col failed for column {i} on mdb"
            )));
        }
        // mdbtools reports column names in the MDB file's code page
        // (CP1252/CP936/etc.), not UTF-8. sql_chars_to_string_lossy substitutes
        // U+FFFD for invalid input. It also handles the wide branch (Windows /
        // odbc-api `wide` feature) where col_desc.name is Vec<u16>.
        let col_name = sql_chars_to_string_lossy(&col_desc.name);
        let col_nullable = col_desc.nullability.could_be_nullable();

        let remote_type = RemoteType::Mdb(mdb_type_to_remote_type(col_desc.data_type)?);
        remote_fields.push(RemoteField::new(col_name, remote_type, col_nullable));
    }

    Ok(RemoteSchema::new(remote_fields))
}

fn mdb_type_to_remote_type(data_type: odbc_api::DataType) -> DFResult<MdbType> {
    match data_type {
        odbc_api::DataType::Bit => Ok(MdbType::Bit),
        odbc_api::DataType::TinyInt => Ok(MdbType::TinyInt),
        odbc_api::DataType::SmallInt => Ok(MdbType::SmallInt),
        odbc_api::DataType::Integer => Ok(MdbType::Integer),
        odbc_api::DataType::Real => Ok(MdbType::Real),
        odbc_api::DataType::Double => Ok(MdbType::Double),
        odbc_api::DataType::Numeric { .. } | odbc_api::DataType::Decimal { .. } => {
            Ok(MdbType::Currency)
        }
        odbc_api::DataType::Char { length } | odbc_api::DataType::WVarchar { length } => {
            Ok(MdbType::Text(length.map(|l| l.get() as u16)))
        }
        odbc_api::DataType::Varchar { length } => Ok(MdbType::Text(length.map(|l| l.get() as u16))),
        odbc_api::DataType::LongVarchar { .. } | odbc_api::DataType::WLongVarchar { .. } => {
            Ok(MdbType::Memo)
        }
        odbc_api::DataType::Binary { length } | odbc_api::DataType::Varbinary { length } => {
            Ok(MdbType::Binary(length.map(|l| l.get() as u16)))
        }
        odbc_api::DataType::LongVarbinary { .. } => Ok(MdbType::OleObject),
        odbc_api::DataType::Timestamp { .. } => Ok(MdbType::DateTime),
        odbc_api::DataType::Date => Ok(MdbType::Date),
        odbc_api::DataType::Time { .. } => Ok(MdbType::Time),
        odbc_api::DataType::WChar { length } => Ok(MdbType::Text(length.map(|l| l.get() as u16))),
        odbc_api::DataType::Other { data_type, .. }
            if data_type == odbc_api::sys::SqlDataType::EXT_GUID =>
        {
            Ok(MdbType::Guid)
        }
        _ => Err(DataFusionError::Execution(format!(
            "Unsupported MDB type: {data_type:?}"
        ))),
    }
}
