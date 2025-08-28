use crate::PostgresType;
use crate::{DFResult, RemoteDbType, RemoteType};
use chrono::{TimeZone, Utc};
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion::arrow::temporal_conversions::{
    date32_to_datetime, time64ns_to_time, time64us_to_time, timestamp_ns_to_datetime,
    timestamp_us_to_datetime,
};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use std::any::Any;
use std::fmt::Debug;

macro_rules! unparse_array {
    ($array:ident) => {{
        let mut sqls: Vec<String> = Vec::with_capacity($array.len());
        for v in $array.iter() {
            match v {
                Some(v) => {
                    sqls.push(format!("{v}"));
                }
                None => {
                    sqls.push("NULL".to_string());
                }
            }
        }
        Ok::<_, DataFusionError>(sqls)
    }};
    ($array:ident, $convert:expr) => {{
        let mut sqls: Vec<String> = Vec::with_capacity($array.len());
        for v in $array.iter() {
            match v {
                Some(v) => {
                    sqls.push($convert(v)?);
                }
                None => {
                    sqls.push("NULL".to_string());
                }
            }
        }
        Ok::<_, DataFusionError>(sqls)
    }};
}

pub trait Unparse: Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;

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

    fn unparse_null_array(
        &self,
        array: &NullArray,
        _remote_type: RemoteType,
    ) -> DFResult<Vec<String>> {
        Ok(vec!["NULL".to_string(); array.len()])
    }

    fn unparse_boolean_array(
        &self,
        array: &BooleanArray,
        _remote_type: RemoteType,
    ) -> DFResult<Vec<String>> {
        unparse_array!(array)
    }

    fn unparse_int8_array(
        &self,
        array: &Int8Array,
        _remote_type: RemoteType,
    ) -> DFResult<Vec<String>> {
        unparse_array!(array)
    }

    fn unparse_int16_array(
        &self,
        array: &Int16Array,
        _remote_type: RemoteType,
    ) -> DFResult<Vec<String>> {
        unparse_array!(array)
    }

    fn unparse_int32_array(
        &self,
        array: &Int32Array,
        _remote_type: RemoteType,
    ) -> DFResult<Vec<String>> {
        unparse_array!(array)
    }

    fn unparse_int64_array(
        &self,
        array: &Int64Array,
        _remote_type: RemoteType,
    ) -> DFResult<Vec<String>> {
        unparse_array!(array)
    }

    fn unparse_uint8_array(
        &self,
        array: &UInt8Array,
        _remote_type: RemoteType,
    ) -> DFResult<Vec<String>> {
        unparse_array!(array)
    }

    fn unparse_uint16_array(
        &self,
        array: &UInt16Array,
        _remote_type: RemoteType,
    ) -> DFResult<Vec<String>> {
        unparse_array!(array)
    }

    fn unparse_uint32_array(
        &self,
        array: &UInt32Array,
        _remote_type: RemoteType,
    ) -> DFResult<Vec<String>> {
        unparse_array!(array)
    }

    fn unparse_uint64_array(
        &self,
        array: &UInt64Array,
        _remote_type: RemoteType,
    ) -> DFResult<Vec<String>> {
        unparse_array!(array)
    }

    fn unparse_float16_array(
        &self,
        array: &Float16Array,
        _remote_type: RemoteType,
    ) -> DFResult<Vec<String>> {
        unparse_array!(array)
    }

    fn unparse_float32_array(
        &self,
        array: &Float32Array,
        _remote_type: RemoteType,
    ) -> DFResult<Vec<String>> {
        unparse_array!(array)
    }

    fn unparse_timestamp_microsecond_array(
        &self,
        array: &TimestampMicrosecondArray,
        remote_type: RemoteType,
    ) -> DFResult<Vec<String>> {
        let db_type = remote_type.db_type();
        let tz = match array.timezone() {
            Some(tz) => Some(
                tz.parse::<Tz>()
                    .map_err(|e| DataFusionError::Internal(e.to_string()))?,
            ),
            None => None,
        };

        unparse_array!(array, |v| {
            let Some(naive) = timestamp_us_to_datetime(v) else {
                return Err(DataFusionError::Internal(format!(
                    "invalid timestamp microsecond value: {v}"
                )));
            };
            let format = match tz {
                Some(tz) => {
                    let date = Utc.from_utc_datetime(&naive).with_timezone(&tz);
                    date.format("%Y-%m-%d %H:%M:%S.%f").to_string()
                }
                None => naive.format("%Y-%m-%d %H:%M:%S.%f").to_string(),
            };
            Ok::<_, DataFusionError>(db_type.sql_string_literal(&format))
        })
    }

    fn unparse_timestamp_nanosecond_array(
        &self,
        array: &TimestampNanosecondArray,
        remote_type: RemoteType,
    ) -> DFResult<Vec<String>> {
        let db_type = remote_type.db_type();
        let tz = match array.timezone() {
            Some(tz) => Some(
                tz.parse::<Tz>()
                    .map_err(|e| DataFusionError::Internal(e.to_string()))?,
            ),
            None => None,
        };

        unparse_array!(array, |v| {
            let Some(naive) = timestamp_ns_to_datetime(v) else {
                return Err(DataFusionError::Internal(format!(
                    "invalid timestamp nanosecond value: {v}"
                )));
            };
            let format = match tz {
                Some(tz) => {
                    let date = Utc.from_utc_datetime(&naive).with_timezone(&tz);
                    date.format("%Y-%m-%d %H:%M:%S.%f").to_string()
                }
                None => naive.format("%Y-%m-%d %H:%M:%S.%f").to_string(),
            };
            Ok::<_, DataFusionError>(db_type.sql_string_literal(&format))
        })
    }

    fn unparse_float64_array(
        &self,
        array: &Float64Array,
        _remote_type: RemoteType,
    ) -> DFResult<Vec<String>> {
        unparse_array!(array)
    }

    fn unparse_date32_array(
        &self,
        array: &Date32Array,
        remote_type: RemoteType,
    ) -> DFResult<Vec<String>> {
        let db_type = remote_type.db_type();
        unparse_array!(array, |v| {
            let Some(date) = date32_to_datetime(v) else {
                return Err(DataFusionError::Internal(format!(
                    "invalid date32 value: {v}"
                )));
            };
            Ok::<_, DataFusionError>(
                db_type.sql_string_literal(&date.format("%Y-%m-%d").to_string()),
            )
        })
    }

    fn unparse_time64_microsecond_array(
        &self,
        array: &Time64MicrosecondArray,
        remote_type: RemoteType,
    ) -> DFResult<Vec<String>> {
        let db_type = remote_type.db_type();
        unparse_array!(array, |v| {
            let Some(time) = time64us_to_time(v) else {
                return Err(DataFusionError::Internal(format!(
                    "invalid time64 microsecond value: {v}"
                )));
            };
            Ok::<_, DataFusionError>(
                db_type.sql_string_literal(&time.format("%H:%M:%S.%f").to_string()),
            )
        })
    }

    fn unparse_time64_nanosecond_array(
        &self,
        array: &Time64NanosecondArray,
        remote_type: RemoteType,
    ) -> DFResult<Vec<String>> {
        let db_type = remote_type.db_type();
        unparse_array!(array, |v| {
            let Some(time) = time64ns_to_time(v) else {
                return Err(DataFusionError::Internal(format!(
                    "invalid time64 nanosecond value: {v}"
                )));
            };
            Ok::<_, DataFusionError>(
                db_type.sql_string_literal(&time.format("%H:%M:%S.%f").to_string()),
            )
        })
    }

    fn unparse_interval_month_day_nano_array(
        &self,
        array: &IntervalMonthDayNanoArray,
        remote_type: RemoteType,
    ) -> DFResult<Vec<String>> {
        let db_type = remote_type.db_type();
        unparse_array!(array, |v: IntervalMonthDayNano| {
            let mut s = String::new();
            let mut prefix = "";

            if v.months != 0 {
                s.push_str(&format!("{prefix}{} mons", v.months));
                prefix = " ";
            }

            if v.days != 0 {
                s.push_str(&format!("{prefix}{} days", v.days));
                prefix = " ";
            }

            if v.nanoseconds != 0 {
                let secs = v.nanoseconds / 1_000_000_000;
                let mins = secs / 60;
                let hours = mins / 60;

                let secs = secs - (mins * 60);
                let mins = mins - (hours * 60);

                let nanoseconds = v.nanoseconds % 1_000_000_000;

                if hours != 0 {
                    s.push_str(&format!("{prefix}{} hours", hours));
                    prefix = " ";
                }

                if mins != 0 {
                    s.push_str(&format!("{prefix}{} mins", mins));
                    prefix = " ";
                }

                if secs != 0 || nanoseconds != 0 {
                    let secs_sign = if secs < 0 || nanoseconds < 0 { "-" } else { "" };
                    s.push_str(&format!(
                        "{prefix}{}{}.{:09} secs",
                        secs_sign,
                        secs.abs(),
                        nanoseconds.abs()
                    ));
                }
            }

            Ok::<_, DataFusionError>(db_type.sql_string_literal(&s))
        })
    }

    fn unparse_string_array(
        &self,
        array: &StringArray,
        remote_type: RemoteType,
    ) -> DFResult<Vec<String>> {
        let db_type = remote_type.db_type();
        unparse_array!(array, |v| Ok::<_, DataFusionError>(
            db_type.sql_string_literal(v)
        ))
    }

    fn unparse_large_string_array(
        &self,
        array: &LargeStringArray,
        remote_type: RemoteType,
    ) -> DFResult<Vec<String>> {
        let db_type = remote_type.db_type();
        unparse_array!(array, |v| Ok::<_, DataFusionError>(
            db_type.sql_string_literal(v)
        ))
    }

    fn unparse_binary_array(
        &self,
        array: &BinaryArray,
        remote_type: RemoteType,
    ) -> DFResult<Vec<String>> {
        let db_type = remote_type.db_type();
        match remote_type {
            RemoteType::Postgres(PostgresType::PostGisGeometry) => {
                unparse_array!(array, |v| {
                    let s = db_type.sql_binary_literal(v);
                    Ok::<_, DataFusionError>(format!("ST_GeomFromWKB({s})"))
                })
            }
            _ => unparse_array!(array, |v| Ok::<_, DataFusionError>(
                db_type.sql_binary_literal(v)
            )),
        }
    }

    fn unparse_fixed_size_binary_array(
        &self,
        array: &FixedSizeBinaryArray,
        remote_type: RemoteType,
    ) -> DFResult<Vec<String>> {
        let db_type = remote_type.db_type();
        match remote_type {
            RemoteType::Postgres(PostgresType::Uuid) => {
                unparse_array!(array, |v| Ok::<_, DataFusionError>(format!(
                    "'{}'",
                    hex::encode(v)
                )))
            }
            _ => unparse_array!(array, |v| Ok::<_, DataFusionError>(
                db_type.sql_binary_literal(v)
            )),
        }
    }

    fn unparse_list_array(
        &self,
        array: &ListArray,
        remote_type: RemoteType,
    ) -> DFResult<Vec<String>> {
        let db_type = remote_type.db_type();
        let data_type = array.data_type();
        let DataType::List(field) = data_type else {
            return Err(DataFusionError::Internal(format!(
                "expect list array, but got {data_type}"
            )));
        };

        let inner_type = field.data_type();

        match inner_type {
            DataType::Boolean => {
                unparse_array!(array, |v: ArrayRef| {
                    let array = v.as_boolean();
                    let sqls = unparse_array!(array)?;
                    Ok::<_, DataFusionError>(format!("ARRAY[{}]", sqls.join(",")))
                })
            }
            DataType::Int16 => {
                unparse_array!(array, |v: ArrayRef| {
                    let array = v.as_primitive::<Int16Type>();
                    let sqls = unparse_array!(array)?;
                    Ok::<_, DataFusionError>(format!("ARRAY[{}]", sqls.join(",")))
                })
            }
            DataType::Int32 => {
                unparse_array!(array, |v: ArrayRef| {
                    let array = v.as_primitive::<Int32Type>();
                    let sqls = unparse_array!(array)?;
                    Ok::<_, DataFusionError>(format!("ARRAY[{}]", sqls.join(",")))
                })
            }
            DataType::Int64 => {
                unparse_array!(array, |v: ArrayRef| {
                    let array = v.as_primitive::<Int64Type>();
                    let sqls = unparse_array!(array)?;
                    Ok::<_, DataFusionError>(format!("ARRAY[{}]", sqls.join(",")))
                })
            }
            DataType::Float32 => {
                unparse_array!(array, |v: ArrayRef| {
                    let array = v.as_primitive::<Float32Type>();
                    let sqls = unparse_array!(array)?;
                    Ok::<_, DataFusionError>(format!("ARRAY[{}]", sqls.join(",")))
                })
            }
            DataType::Float64 => {
                unparse_array!(array, |v: ArrayRef| {
                    let array = v.as_primitive::<Float64Type>();
                    let sqls = unparse_array!(array)?;
                    Ok::<_, DataFusionError>(format!("ARRAY[{}]", sqls.join(",")))
                })
            }
            DataType::Utf8 => {
                unparse_array!(array, |v: ArrayRef| {
                    let array = v.as_string::<i32>();
                    let sqls = unparse_array!(array, |v| Ok::<_, DataFusionError>(
                        db_type.sql_string_literal(v)
                    ))?;
                    Ok::<_, DataFusionError>(format!("ARRAY[{}]", sqls.join(",")))
                })
            }
            DataType::LargeUtf8 => {
                unparse_array!(array, |v: ArrayRef| {
                    let array = v.as_string::<i64>();
                    let sqls = unparse_array!(array, |v| Ok::<_, DataFusionError>(
                        db_type.sql_string_literal(v)
                    ))?;
                    Ok::<_, DataFusionError>(format!("ARRAY[{}]", sqls.join(",")))
                })
            }
            _ => Err(DataFusionError::NotImplemented(format!(
                "Not supported unparsing list array: {data_type}"
            ))),
        }
    }

    fn unparse_decimal128_array(
        &self,
        array: &Decimal128Array,
        remote_type: RemoteType,
    ) -> DFResult<Vec<String>> {
        let precision = array.precision();
        let scale = array.scale();
        let db_type = remote_type.db_type();
        unparse_array!(array, |v| Ok::<_, DataFusionError>(
            db_type.sql_string_literal(&Decimal128Type::format_decimal(v, precision, scale))
        ))
    }

    fn unparse_decimal256_array(
        &self,
        array: &Decimal256Array,
        remote_type: RemoteType,
    ) -> DFResult<Vec<String>> {
        let precision = array.precision();
        let scale = array.scale();
        let db_type = remote_type.db_type();
        unparse_array!(array, |v| Ok::<_, DataFusionError>(
            db_type.sql_string_literal(&Decimal256Type::format_decimal(v, precision, scale))
        ))
    }
}

pub fn unparse_array(
    unparser: &dyn Unparse,
    array: &ArrayRef,
    remote_type: RemoteType,
) -> DFResult<Vec<String>> {
    match array.data_type() {
        DataType::Null => {
            let array = array
                .as_any()
                .downcast_ref::<NullArray>()
                .expect("expect null array");
            unparser.unparse_null_array(array, remote_type)
        }
        DataType::Boolean => {
            let array = array.as_boolean();
            unparser.unparse_boolean_array(array, remote_type)
        }
        DataType::Int8 => {
            let array = array.as_primitive::<Int8Type>();
            unparser.unparse_int8_array(array, remote_type)
        }
        DataType::Int16 => {
            let array = array.as_primitive::<Int16Type>();
            unparser.unparse_int16_array(array, remote_type)
        }
        DataType::Int32 => {
            let array = array.as_primitive::<Int32Type>();
            unparser.unparse_int32_array(array, remote_type)
        }
        DataType::Int64 => {
            let array = array.as_primitive::<Int64Type>();
            unparser.unparse_int64_array(array, remote_type)
        }
        DataType::UInt8 => {
            let array = array.as_primitive::<UInt8Type>();
            unparser.unparse_uint8_array(array, remote_type)
        }
        DataType::UInt16 => {
            let array = array.as_primitive::<UInt16Type>();
            unparser.unparse_uint16_array(array, remote_type)
        }
        DataType::UInt32 => {
            let array = array.as_primitive::<UInt32Type>();
            unparser.unparse_uint32_array(array, remote_type)
        }
        DataType::UInt64 => {
            let array = array.as_primitive::<UInt64Type>();
            unparser.unparse_uint64_array(array, remote_type)
        }
        DataType::Float16 => {
            let array = array.as_primitive::<Float16Type>();
            unparser.unparse_float16_array(array, remote_type)
        }
        DataType::Float32 => {
            let array = array.as_primitive::<Float32Type>();
            unparser.unparse_float32_array(array, remote_type)
        }
        DataType::Float64 => {
            let array = array.as_primitive::<Float64Type>();
            unparser.unparse_float64_array(array, remote_type)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let array = array.as_primitive::<TimestampMicrosecondType>();
            unparser.unparse_timestamp_microsecond_array(array, remote_type)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let array = array.as_primitive::<TimestampNanosecondType>();
            unparser.unparse_timestamp_nanosecond_array(array, remote_type)
        }
        DataType::Date32 => {
            let array = array.as_primitive::<Date32Type>();
            unparser.unparse_date32_array(array, remote_type)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let array = array.as_primitive::<Time64MicrosecondType>();
            unparser.unparse_time64_microsecond_array(array, remote_type)
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let array = array.as_primitive::<Time64NanosecondType>();
            unparser.unparse_time64_nanosecond_array(array, remote_type)
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let array = array.as_primitive::<IntervalMonthDayNanoType>();
            unparser.unparse_interval_month_day_nano_array(array, remote_type)
        }
        DataType::Utf8 => {
            let array = array.as_string();
            unparser.unparse_string_array(array, remote_type)
        }
        DataType::LargeUtf8 => {
            let array = array.as_string::<i64>();
            unparser.unparse_large_string_array(array, remote_type)
        }
        DataType::Binary => {
            let array = array.as_binary::<i32>();
            unparser.unparse_binary_array(array, remote_type)
        }
        DataType::FixedSizeBinary(_) => {
            let array = array.as_fixed_size_binary();
            unparser.unparse_fixed_size_binary_array(array, remote_type)
        }
        DataType::List(_) => {
            let array = array.as_list::<i32>();
            unparser.unparse_list_array(array, remote_type)
        }
        DataType::Decimal128(_, _) => {
            let array = array.as_primitive::<Decimal128Type>();
            unparser.unparse_decimal128_array(array, remote_type)
        }
        DataType::Decimal256(_, _) => {
            let array = array.as_primitive::<Decimal256Type>();
            unparser.unparse_decimal256_array(array, remote_type)
        }
        _ => Err(DataFusionError::NotImplemented(format!(
            "Not supported unparsing array: {}",
            array.data_type()
        ))),
    }
}

#[derive(Debug)]
pub struct DefaultUnparser {}

impl Unparse for DefaultUnparser {
    fn as_any(&self) -> &dyn Any {
        self
    }
}
