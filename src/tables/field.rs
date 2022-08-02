use rust_decimal::Decimal;
use serde_json::Value;
use std::fmt;

#[derive(Debug, Clone)]
pub enum Field {
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    U32(u32),
    I64(i64),
    F32(f32),
    F64(f64),
    Numeric(Decimal),
    Str(String),
    Json(Value),
    Bytes(Vec<u8>)
}

impl Field {

    pub fn display_content<'a>(&'a self) -> String {
        match self {
            Field::Bool(f) => f.to_string(),
            Field::I8(f) => f.to_string(),
            Field::I16(f) => f.to_string(),
            Field::I32(f) => f.to_string(),
            Field::U32(f) => f.to_string(),
            Field::I64(f) => f.to_string(),
            Field::F32(f) => f.to_string(),
            Field::F64(f) => f.to_string(),
            Field::Numeric(f) => f.to_string(),
            Field::Str(f) => f.clone(),
            Field::Json(f) => f.to_string(),
            Field::Bytes(f) => format!("(Binary)")
        }
    }

}
