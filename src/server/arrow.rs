use datafusion::{self, *};
use datafusion::datasource::TableProvider;
use datafusion::datasource::csv::{CsvFile, CsvReadOptions};
use datafusion::execution::physical_plan::udf::ScalarFunction;
use arrow::datatypes::*;
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::ExecutionContext;
use datafusion::error::ExecutionError;
use std::sync::Arc;
use std::error::Error;
use datafusion::logicalplan::Expr;
use datafusion::logicalplan::aggregate_expr;
use arrow::datatypes::{ArrowPrimitiveType,ArrowNativeType,ArrowNumericType};
use crate::tables::{table::Table, column::*, nullable_column::*};

/*
Perhaps we can implement TableProvider for a foreign PostgreSQL table,
and let the user manipulate the data in-memory?
*/

/// Copies into a column from the Array type A into Rust native type N
fn primitive_to_column<A, N>(arr : &Arc<dyn Array>) -> Result<Column, String>
where
    N : Copy + ArrowNativeType,
    A : ArrowNumericType<Native=N>,
    Column : From<Vec<N>>
{
    let prim_arr = arr.as_any()
        .downcast_ref::<PrimitiveArray<A>>()
        .ok_or(format!("Error downcasting column"))?;
    let mut v = Vec::new();
    v.extend_from_slice(prim_arr.value_slice(0, prim_arr.len()));
    Ok(Column::from(v))
}

pub fn table_from_batch(results : &RecordBatch) -> Result<Table, String> {
    let schema = results.schema();
    let fields = schema.fields();
    let mut names = Vec::new();
    let mut cols = Vec::new();
    for (i, arr) in results.columns().iter().enumerate() {
        let col = match fields[i].data_type() {
            DataType::Int8 => primitive_to_column::<Int8Type, i8>(arr)?,
            DataType::Int16 => primitive_to_column::<Int16Type, i16>(arr)?,
            DataType::Int32 => primitive_to_column::<Int32Type, i32>(arr)?,
            DataType::UInt32 => primitive_to_column::<UInt32Type, u32>(arr)?,
            DataType::Int64 => primitive_to_column::<Int64Type, i64>(arr)?,
            DataType::Float32 => primitive_to_column::<Float32Type, f32>(arr)?,
            DataType::Float64 => primitive_to_column::<Float64Type, f64>(arr)?,
            DataType::Utf8 => unimplemented!(), // StringArray
            DataType::Binary => unimplemented!(), // BinaryArray
            _ => return Err(format!("Invalid datatype for column {}", i))
        };
        cols.push(col);
        names.push(fields[i].name().clone());
    }
    Table::new(names, cols).map_err(|e| format!("{}", e))
}

/*fn create_context() {

    ctx.register_csv(
        "my_data",
        "test.csv",
        CsvReadOptions::new(),
    )?;
}

fn execute() {
    let results = ctx.sql(sql, 10)?;
    pretty::print_batches(&results)?;
}

fn register_function() {
    let f1 = Field::new("data1", DataType::Int64, true);
    let f2 = Field::new("data2", DataType::Int64, true);
    let sf = ScalarFunction::new("mysf", vec![f1, f2], DataType::Int64, Arc::new(sum));

    let f1 = Field::new("data1", DataType::Utf8, true);
    let f2 = Field::new("data2", DataType::Float64, true);
    let sf2 = ScalarFunction::new("model_info", vec![f1, f2], DataType::Utf8, Arc::new(model_info));
    ctx.register_udf(sf);
}*/

#[cfg(feature="arrowext")]
fn query_arrow(ctx : &mut ExecutionContext, q : &str) -> QueryResult {
    match ctx.sql(q, 10000) {
        Ok(results) => {
            if results.len() == 0 {
                return QueryResult::Statement(String::from("0 Row(s) modified"));
            } else {
                match super::arrow::table_from_batch(&results[0]) {
                    Ok(tbl) => QueryResult::Valid(q.to_string(), tbl),
                    Err(e) => QueryResult::Invalid(format!("{}", e), true)
                }
            }
        },
        Err(e) => {
            QueryResult::Invalid(format!("{}", e), true )
        }
    }
}

#[cfg(feature="arrowext")]
fn exec_arrow(ctx : &mut ExecutionContext, q : &str) -> QueryResult {
    match ctx.sql(q, 10000) {
        Ok(results) => {
            if results.len() == 0 {
                return QueryResult::Statement(String::from("0 Row(s) modified"));
            } else {
                match super::arrow::table_from_batch(&results[0]) {
                    Ok(tbl) => QueryResult::Valid(q.to_string(), tbl),
                    Err(e) => QueryResult::Invalid(format!("{}", e), true)
                }
            }
        },
        Err(e) => {
            QueryResult::Invalid(format!("{}", e), true )
        }
    }
}

