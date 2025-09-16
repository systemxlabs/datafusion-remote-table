use crate::{ConnectionOptions, DFResult, RemoteSource, RemoteTable};
use bigdecimal::BigDecimal;
use bigdecimal::ToPrimitive;
use datafusion::arrow::array::{
    Array, BooleanArray, GenericByteArray, PrimitiveArray, RecordBatch,
};
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, BinaryType, BooleanType, ByteArrayType, LargeBinaryType, LargeUtf8Type,
    Utf8Type, i256,
};
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use std::str::FromStr;
use std::sync::Arc;

pub async fn remote_collect(
    options: ConnectionOptions,
    sql: impl Into<String>,
) -> DFResult<Vec<RecordBatch>> {
    let table = RemoteTable::try_new(options, RemoteSource::Query(sql.into())).await?;
    let ctx = SessionContext::new();
    ctx.read_table(Arc::new(table))?.collect().await
}

pub async fn remote_collect_primitive_column<T: ArrowPrimitiveType>(
    options: ConnectionOptions,
    sql: impl Into<String>,
    col_idx: usize,
) -> DFResult<Vec<Option<T::Native>>> {
    let batches = remote_collect(options, sql).await?;
    extract_primitive_array::<T>(&batches, col_idx)
}

pub async fn remote_collect_utf8_column(
    options: ConnectionOptions,
    sql: impl Into<String>,
    col_idx: usize,
) -> DFResult<Vec<Option<String>>> {
    let batches = remote_collect(options, sql).await?;
    let vec = extract_byte_array::<Utf8Type>(&batches, col_idx)?;
    Ok(vec.into_iter().map(|s| s.map(|s| s.to_string())).collect())
}

pub async fn remote_collect_large_utf8_column(
    options: ConnectionOptions,
    sql: impl Into<String>,
    col_idx: usize,
) -> DFResult<Vec<Option<String>>> {
    let batches = remote_collect(options, sql).await?;
    let vec = extract_byte_array::<LargeUtf8Type>(&batches, col_idx)?;
    Ok(vec.into_iter().map(|s| s.map(|s| s.to_string())).collect())
}

pub async fn remote_collect_binary_column(
    options: ConnectionOptions,
    sql: impl Into<String>,
    col_idx: usize,
) -> DFResult<Vec<Option<Vec<u8>>>> {
    let batches = remote_collect(options, sql).await?;
    let vec = extract_byte_array::<BinaryType>(&batches, col_idx)?;
    Ok(vec.into_iter().map(|s| s.map(|s| s.to_vec())).collect())
}

pub async fn remote_collect_large_binary_column(
    options: ConnectionOptions,
    sql: impl Into<String>,
    col_idx: usize,
) -> DFResult<Vec<Option<Vec<u8>>>> {
    let batches = remote_collect(options, sql).await?;
    let vec = extract_byte_array::<LargeBinaryType>(&batches, col_idx)?;
    Ok(vec.into_iter().map(|s| s.map(|s| s.to_vec())).collect())
}

pub fn extract_primitive_array<T: ArrowPrimitiveType>(
    batches: &[RecordBatch],
    col_idx: usize,
) -> DFResult<Vec<Option<T::Native>>> {
    let mut result = Vec::new();
    for batch in batches {
        let column = batch.column(col_idx);
        if let Some(array) = column.as_any().downcast_ref::<PrimitiveArray<T>>() {
            result.extend(array.iter().collect::<Vec<_>>())
        } else {
            return Err(DataFusionError::Execution(format!(
                "Column at index {col_idx} is not {} instead of {}",
                T::DATA_TYPE,
                column.data_type(),
            )));
        }
    }
    Ok(result)
}

pub fn extract_boolean_array(
    batches: &[RecordBatch],
    col_idx: usize,
) -> DFResult<Vec<Option<bool>>> {
    let mut result = Vec::new();
    for batch in batches {
        let column = batch.column(col_idx);
        if let Some(array) = column.as_any().downcast_ref::<BooleanArray>() {
            result.extend(array.iter().collect::<Vec<_>>())
        } else {
            return Err(DataFusionError::Execution(format!(
                "Column at index {col_idx} is not {} instead of {}",
                BooleanType::DATA_TYPE,
                column.data_type(),
            )));
        }
    }
    Ok(result)
}

pub fn extract_byte_array<T: ByteArrayType>(
    batches: &[RecordBatch],
    col_idx: usize,
) -> DFResult<Vec<Option<&T::Native>>> {
    let mut result = Vec::new();
    for batch in batches {
        let column = batch.column(col_idx);
        if let Some(array) = column.as_any().downcast_ref::<GenericByteArray<T>>() {
            result.extend(array.iter().collect::<Vec<_>>())
        } else {
            return Err(DataFusionError::Execution(format!(
                "Column at index {col_idx} is not {} instead of {}",
                T::DATA_TYPE,
                column.data_type(),
            )));
        }
    }
    Ok(result)
}

pub fn gen_tenfold_scaling_factor(scale: i32) -> String {
    if scale >= 0 {
        format!("1{}", "0".repeat(scale as usize))
    } else {
        format!("0.{}{}", "0".repeat((-scale - 1) as usize), "1")
    }
}

pub fn big_decimal_to_i128(decimal: &BigDecimal, scale: Option<i32>) -> DFResult<i128> {
    let scale = scale.unwrap_or_else(|| {
        decimal
            .fractional_digit_count()
            .try_into()
            .unwrap_or_default()
    });
    let scale_str = gen_tenfold_scaling_factor(scale);
    let scale_decimal = BigDecimal::from_str(&scale_str).map_err(|e| {
        DataFusionError::Execution(format!(
            "Failed to parse str {scale_str} to BigDecimal: {e:?}",
        ))
    })?;
    (decimal * scale_decimal).to_i128().ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Failed to convert BigDecimal to i128 for {decimal:?}",
        ))
    })
}

pub fn big_decimal_to_i256(decimal: &BigDecimal, scale: Option<i32>) -> DFResult<i256> {
    let scale = scale.unwrap_or_else(|| {
        decimal
            .fractional_digit_count()
            .try_into()
            .unwrap_or_default()
    });
    let scale_str = gen_tenfold_scaling_factor(scale);
    let scale_decimal = BigDecimal::from_str(&scale_str).map_err(|e| {
        DataFusionError::Execution(format!(
            "Failed to parse str {scale_str} to BigDecimal: {e:?}",
        ))
    })?;
    let scaled_decimal = decimal * scale_decimal;

    // remove the fractional part, only keep the integer part
    let integer_part = scaled_decimal.with_scale(0);

    // Convert to string and then parse as i256
    integer_part.to_string().parse::<i256>().map_err(|e| {
        DataFusionError::Execution(format!("Failed to parse str {integer_part} to i256: {e:?}",))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{BooleanArray, Int32Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema, Utf8Type};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_extract_primitive_array() {
        let expected = vec![Some(1), Some(2), None];
        let batches = vec![
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)])),
                vec![Arc::new(Int32Array::from(expected.clone()))],
            )
            .unwrap(),
        ];
        let result: Vec<Option<i32>> = extract_primitive_array::<Int32Type>(&batches, 0).unwrap();
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_extract_bool_array() {
        let expected = vec![Some(true), Some(false), None];
        let batches = vec![
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("a", DataType::Boolean, true)])),
                vec![Arc::new(BooleanArray::from(expected.clone()))],
            )
            .unwrap(),
        ];
        let result: Vec<Option<bool>> = extract_boolean_array(&batches, 0).unwrap();
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_extract_byte_array() {
        let expected = vec![Some("abc"), Some("def"), None];
        let batches = vec![
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)])),
                vec![Arc::new(StringArray::from(expected.clone()))],
            )
            .unwrap(),
        ];
        let result: Vec<Option<&str>> = extract_byte_array::<Utf8Type>(&batches, 0).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_gen_tenfold_scaling_factor() {
        assert_eq!(gen_tenfold_scaling_factor(0), "1");
        assert_eq!(gen_tenfold_scaling_factor(1), "10");
        assert_eq!(gen_tenfold_scaling_factor(2), "100");
        assert_eq!(gen_tenfold_scaling_factor(-1), "0.1");
        assert_eq!(gen_tenfold_scaling_factor(-2), "0.01");
    }
}
