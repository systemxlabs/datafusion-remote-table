//! Shared ODBC helpers used by both the dm and mdb backends.
//!
//! Anything ODBC-specific that is generic across backends lives here so the
//! dm and mdb modules can share rather than duplicate.

use crate::DFResult;
use datafusion_common::DataFusionError;

fn build_naive_datetime(value: &odbc_api::sys::Timestamp) -> DFResult<chrono::NaiveDateTime> {
    chrono::NaiveDate::from_ymd_opt(value.year as i32, value.month as u32, value.day as u32)
        .ok_or_else(|| DataFusionError::Execution(format!("Invalid timestamp: {value:?}")))?
        .and_hms_nano_opt(
            value.hour as u32,
            value.minute as u32,
            value.second as u32,
            value.fraction,
        )
        .ok_or_else(|| DataFusionError::Execution(format!("Invalid timestamp: {value:?}")))
}

#[allow(dead_code)] // only used by dm backend; suppress warning when only mdb is enabled
pub(crate) fn seconds_since_epoch(value: &odbc_api::sys::Timestamp) -> DFResult<i64> {
    chrono::NaiveDate::from_ymd_opt(value.year as i32, value.month as u32, value.day as u32)
        .ok_or_else(|| DataFusionError::Execution(format!("Invalid timestamp: {value:?}")))?
        .and_hms_opt(value.hour as u32, value.minute as u32, value.second as u32)
        .ok_or_else(|| DataFusionError::Execution(format!("Invalid timestamp: {value:?}")))
        .map(|ndt| ndt.and_utc().timestamp())
}

#[allow(dead_code)] // only used by dm backend; suppress warning when only mdb is enabled
pub(crate) fn ms_since_epoch(value: &odbc_api::sys::Timestamp) -> DFResult<i64> {
    Ok(build_naive_datetime(value)?.and_utc().timestamp_millis())
}

pub(crate) fn us_since_epoch(value: &odbc_api::sys::Timestamp) -> DFResult<i64> {
    Ok(build_naive_datetime(value)?.and_utc().timestamp_micros())
}

#[allow(dead_code)] // only used by dm backend; suppress warning when only mdb is enabled
pub(crate) fn ns_since_epoch(value: &odbc_api::sys::Timestamp) -> DFResult<i64> {
    // The dates that can be represented as nanoseconds are between
    // 1677-09-21T00:12:44.0 and 2262-04-11T23:47:16.854775804
    build_naive_datetime(value)?
        .and_utc()
        .timestamp_nanos_opt()
        .ok_or_else(|| DataFusionError::Execution(format!("Invalid timestamp: {value:?}")))
}
