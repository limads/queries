use postgres::types::ToSql;
use std::marker::Sync;
use rust_decimal::Decimal;
use super::nullable_column::*;
use num_traits::ToPrimitive;
use super::field::Field;
use serde_json::Value;
use std::borrow::Cow;
use serde_json;

// TODO create Array<Column> for N-D Postgre arrays, that carries a vector of Columns
// and a dimensionality metadata.

/// Densely packed column, where each variant is a vector of some
/// element that implements postgres::types::ToSql.
#[derive(Debug, Clone)]
pub enum Column {
    Bool(Vec<bool>),
    I8(Vec<i8>),
    I16(Vec<i16>),
    I32(Vec<i32>),
    U32(Vec<u32>),
    I64(Vec<i64>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    Numeric(Vec<Decimal>),
    Str(Vec<String>),
    Bytes(Vec<Vec<u8>>),
    Json(Vec<Value>),
    Nullable(Box<NullableColumn>)
}

impl<'a> Column {

    pub fn new_empty<T>() -> Self
        where Column : From<Vec<T>>
    {
        let vec = Vec::<bool>::new();
        vec.into()
    }

    /// If this column contains a single JSON element, return it.
    pub fn single_json_row(&self) -> Option<serde_json::Value> {
        match self {
            Column::Json(vals) => {
                match vals.len() {
                    1 => Some(vals[0].clone()),
                    _ => None
                }
            },
            _ => None
        }
    }

    /*pub fn get_integer(&self, ix : usize) -> Option<i64> {
        match &self {
            Column::I64(v) => v.get(ix),
            Column::I8(v) => v.get(ix).map(|v| v as i64 ),
            Column::I16(v) => v.get(ix).map(|v| v as i64 ),
            Column::I32(v) => v.get(ix).map(|v| v as i64 ),
            Column::U32(v) => v.get(ix).map(|v| v as i64 ),
            _ => None
        }
    }

    pub fn get_real(&self, ix : usize) -> Option<f64> {
        match &self {
            Column::F32(v) => v.get(ix),
            Column::F64(v) => v.get(ix).map(|v| v as f64 ),
            _ => None
        }
    }

    pub fn get_text(&self, ix : usize) -> Option<String> {
        match &self {
            Column::Text(v) => v.get(ix).map(|v| v.clone() ),
            _ => None
        }
    }

    pub fn get_bytes(&self, ix : usize) -> Option<Vec<u8>> {
        match &self {
            Column::Bytes(b) => v.get(ix).map(|b| b.clone() ),
            _ => None
        }
    }*/

    /*pub fn try_slice_bool(&'a self) -> Option<&'a [bool]> {
        match self {
            Column::Bool(b) => Some(&b[..]),
            _ => None
        }
    }

    pub fn try_slice_i8(&'a self) -> Option<&'a [i8]> {
        match self {
            Column::I8(i) => Some(&i[..]),
            _ => None
        }
    }*/

    fn to_ref_dyn<'b, T>(v : &'b Vec<T>) -> Vec<&'b (dyn ToSql + Sync)>
    where T : ToSql + Sync
    {
        v.iter().map(|e| e as &'b (dyn ToSql + Sync)).collect()
    }

    pub fn at(&self, ix : usize) -> Option<Field> {
        match self {
            Column::Bool(v) => v.get(ix).map(|f| Field::Bool(*f) ),
            Column::I8(v) => v.get(ix).map(|f| Field::I8(*f) ),
            Column::I16(v) => v.get(ix).map(|f| Field::I16(*f) ),
            Column::I32(v) => v.get(ix).map(|f| Field::I32(*f) ),
            Column::U32(v) => v.get(ix).map(|f| Field::U32(*f) ),
            Column::I64(v) => v.get(ix).map(|f| Field::I64(*f) ),
            Column::F32(v) => v.get(ix).map(|f| Field::F32(*f) ),
            Column::F64(v) => v.get(ix).map(|f| Field::F64(*f) ),
            Column::Numeric(v) => v.get(ix).map(|f| Field::Numeric(f.clone()) ),
            Column::Str(v) => v.get(ix).map(|f| Field::Str(f.clone()) ),
            Column::Json(v) => v.get(ix).map(|f| Field::Json(f.clone()) ),
            Column::Bytes(v) => v.get(ix).map(|f| Field::Bytes(f.clone()) ),

            // TODO implement NullableColumn::at
            Column::Nullable(col) => None
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Column::Bool(v) => v.len(),
            Column::I8(v) => v.len(),
            Column::I16(v) => v.len(),
            Column::I32(v) => v.len(),
            Column::U32(v) => v.len(),
            Column::I64(v) => v.len(),
            Column::F32(v) => v.len(),
            Column::F64(v) => v.len(),
            Column::Numeric(v) => v.len(),
            Column::Str(v) => v.len(),
            Column::Bytes(v) => v.len(),
            Column::Json(v) => v.len(),
            Column::Nullable(col) => col.len()
        }
    }

    pub fn ref_content(&'a self) -> Vec<&(dyn ToSql + Sync)> {
        match self {
            Column::Bool(v) => Self::to_ref_dyn(v),
            Column::I8(v) => Self::to_ref_dyn(v),
            Column::I16(v) => Self::to_ref_dyn(v),
            Column::I32(v) => Self::to_ref_dyn(v),
            Column::U32(v) => Self::to_ref_dyn(v),
            Column::I64(v) => Self::to_ref_dyn(v),
            Column::F32(v) => Self::to_ref_dyn(v),
            Column::F64(v) => Self::to_ref_dyn(v),
            Column::Numeric(v) => Self::to_ref_dyn(v),
            Column::Str(v) => Self::to_ref_dyn(v),
            Column::Json(v) => Self::to_ref_dyn(v),
            Column::Bytes(v) => Self::to_ref_dyn(v),
            Column::Nullable(col) => col.ref_content()
        }
    }

    fn display_with_precision(value : f64, prec : usize) -> String {
        match prec {
            1 => format!("{:.1}", value),
            2 => format!("{:.2}", value),
            3 => format!("{:.3}", value),
            4 => format!("{:.4}", value),
            5 => format!("{:.5}", value),
            6 => format!("{:.6}", value),
            7 => format!("{:.7}", value),
            8 => format!("{:.8}", value),
            _ => format!("{}", value)
        }
    }
    
    pub fn display_content_at_index(&'a self, row_ix : usize, prec : usize) -> Cow<'a, str> {
        match &self {
            Column::Str(v) => Cow::Borrowed(&v[row_ix]),
            Column::Bool(v) => Cow::Owned(v[row_ix].to_string()),
            Column::I8(v) => Cow::Owned(v[row_ix].to_string()),
            Column::I16(v) => Cow::Owned(v[row_ix].to_string()),
            Column::I32(v) => Cow::Owned(v[row_ix].to_string()),
            Column::U32(v) => Cow::Owned(v[row_ix].to_string()),
            Column::I64(v) => Cow::Owned(v[row_ix].to_string()),

            // TODO panic
            Column::F32(v) => Cow::Owned(Self::display_with_precision(v[row_ix] as f64, prec)),
            Column::F64(v) => Cow::Owned(Self::display_with_precision(v[row_ix] as f64, prec)),
            Column::Numeric(v) => Cow::Owned(v[row_ix].to_string()),
            Column::Json(v) => Cow::Owned(v[row_ix].to_string()),
            Column::Bytes(v) => Cow::Owned(format!("Binary ({} bytes)", v[row_ix].len())),
            Column::Nullable(col) => col.display_content_at_index(row_ix, prec)
        }
    }

    pub fn display_content(&'a self, prec : usize) -> Vec<String> {
        match self {
            Column::Bool(v) => v.iter().map(|e| e.to_string() ).collect(),
            Column::I8(v) => v.iter().map(|e| e.to_string() ).collect(),
            Column::I16(v) => v.iter().map(|e| e.to_string() ).collect(),
            Column::I32(v) => v.iter().map(|e| e.to_string() ).collect(),
            Column::U32(v) => v.iter().map(|e| e.to_string() ).collect(),
            Column::I64(v) => v.iter().map(|e| e.to_string() ).collect(),
            Column::F32(v) => v.iter().map(|e| Self::display_with_precision(*e as f64, prec) ).collect(),
            Column::F64(v) => v.iter().map(|e| Self::display_with_precision(*e as f64, prec) ).collect(),
            Column::Numeric(v) => v.iter().map(|d| d.to_string() ).collect(),
            Column::Str(v) => v.clone(),
            Column::Json(v) => v.iter().map(|e| e.to_string() ).collect(),
            Column::Bytes(v) => v.iter().map(|e| format!("Binary ({} bytes)", e.len()) ).collect(),
            Column::Nullable(col) => col.display_content(prec)
        }
    }

    pub fn sqlite3_type(&self) -> String {
        match self {
            Column::I32(_) | Column::I64(_) => String::from("INT"),
            Column::F32(_) | Column::F64(_) => String::from("REAL"),
            Column::Bytes(_) => String::from("BLOB"),
            _ => String::from("TEXT"),
        }
    }

    pub fn truncate(&mut self, n : usize) {
        match self {
            Column::Bool(v) => v.truncate(n),
            Column::I8(v) => v.truncate(n),
            Column::I16(v) => v.truncate(n),
            Column::I32(v) => v.truncate(n),
            Column::U32(v) => v.truncate(n),
            Column::I64(v) => v.truncate(n),
            Column::F32(v) => v.truncate(n),
            Column::F64(v) => v.truncate(n),
            Column::Numeric(v) => v.truncate(n),
            Column::Str(v) => v.truncate(n),
            Column::Json(v) => v.truncate(n),
            Column::Bytes(v) => v.truncate(n),
            Column::Nullable(col) => col.truncate(n)
        }
    }

}

pub mod from {

    use super::*;
    use std::convert::{ From, TryFrom };

    impl Into<()> for Column {
        fn into(self) -> () {
            unimplemented!()
        }
    }

    impl From<()> for Column {
        fn from(value: ()) -> Self {
            unimplemented!()
        }
    }

    impl From<Vec<()>> for Column {
        fn from(value: Vec<()>) -> Self {
            unimplemented!()
        }
    }

    impl From<Vec<bool>> for Column {
        fn from(value: Vec<bool>) -> Self {
            Self::Bool(value)
        }
    }

    impl From<Vec<i8>> for Column {

        fn from(value: Vec<i8>) -> Self {
            Self::I8(value)
        }
    }

    impl From<Vec<i16>> for Column {
        fn from(value: Vec<i16>) -> Self {
            Self::I16(value)
        }
    }

    impl From<Vec<i32>> for Column {
        fn from(value: Vec<i32>) -> Self {
            Self::I32(value)
        }
    }

    impl From<Vec<u32>> for Column {
        fn from(value: Vec<u32>) -> Self {
            Self::U32(value)
        }
    }

    impl From<Vec<i64>> for Column {
        fn from(value: Vec<i64>) -> Self {
            Self::I64(value)
        }
    }

    impl From<Vec<f32>> for Column {
        fn from(value: Vec<f32>) -> Self {
            Self::F32(value)
        }
    }

    impl From<Vec<f64>> for Column {
        fn from(value: Vec<f64>) -> Self {
            Self::F64(value)
        }
    }

    impl From<Vec<Decimal>> for Column {
        fn from(value: Vec<Decimal>) -> Self {
            Self::Numeric(value)
        }
    }

    impl From<Vec<String>> for Column {
        fn from(value: Vec<String>) -> Self {
            Self::Str(value)
        }
    }

    impl From<Vec<Value>> for Column {
        fn from(value: Vec<Value>) -> Self {
            Self::Json(value)
        }
    }

    impl From<Vec<Vec<u8>>> for Column {
        fn from(value: Vec<Vec<u8>>) -> Self {
            Self::Bytes(value)
        }
    }

    /*impl<T> From<Vec<Vec<T>>> for Column
    where
        Column : From<Vec<T>>
    {

        //type Error = ();

        fn from(value: Vec<Vec<T>>) -> Self {
            let mut columns = Vec::new();
            for v in value {
                columns.push(v.into());
            }
            columns
        }

    }*/

    /*impl<T> From<Vec<Option<T>>> for Column
        where
            //Option<T> : ToSql + Sync + Clone,
            Column : From<Vec<T>>,
            NullableColumn : From<Vec<Option<T>>>
    {
        fn from(value: Vec<Option<T>>) -> Self {
            let null_col : NullableColumn = value.into();
            Self::Nullable(Box::new(null_col))
        }
    }*/

}

pub mod try_into {

    use std::convert::{ TryInto, TryFrom};
    use super::*;

    impl TryFrom<Column> for Vec<bool> {

        type Error = &'static str;

        fn try_from(col : Column) -> Result<Self, Self::Error> {
            match col {
                Column::Bool(v) => Ok(v),
                _ => Err("Invalid column type")
            }
        }

    }

    impl TryFrom<Column> for Vec<i8> {

        type Error = &'static str;

        fn try_from(col : Column) -> Result<Self, Self::Error> {
            match col {
                Column::I8(v) => Ok(v),
                _ => Err("Invalid column type")
            }
        }

    }

    impl TryFrom<Column> for Vec<i16> {

        type Error = &'static str;

        fn try_from(col : Column) -> Result<Self, Self::Error> {
            match col {
                Column::I16(v) => Ok(v),
                _ => Err("Invalid column type")
            }
        }

    }

    impl TryFrom<Column> for Vec<Value> {

        type Error = &'static str;

        fn try_from(col : Column) -> Result<Self, Self::Error> {
            match col {
                Column::Json(v) => Ok(v),
                _ => Err("Invalid column type")
            }
        }

    }

    impl TryFrom<Column> for Vec<i32> {

        type Error = &'static str;

        fn try_from(col : Column) -> Result<Self, Self::Error> {
            match col {
                Column::I32(v) => Ok(v),
                _ => Err("Invalid column type")
            }
        }

    }

    impl TryFrom<Column> for Vec<u32> {

        type Error = &'static str;

        fn try_from(col : Column) -> Result<Self, Self::Error> {
            match col {
                Column::U32(v) => Ok(v),
                _ => Err("Invalid column type")
            }
        }

    }

    impl TryFrom<Column> for Vec<i64> {

        type Error = &'static str;

        fn try_from(col : Column) -> Result<Self, Self::Error> {
            match col {
                Column::I64(v) => Ok(v),
                _ => Err("Invalid column type")
            }
        }

    }

    impl TryFrom<Column> for Vec<f32> {

        type Error = &'static str;

        fn try_from(col : Column) -> Result<Self, Self::Error> {
            match col {
                Column::F32(v) => Ok(v),
                _ => Err("Invalid column type")
            }
        }

    }

    impl TryFrom<Column> for Vec<f64> {

        type Error = &'static str;

        fn try_from(col : Column) -> Result<Self, Self::Error> {
            match col {
                Column::F64(v) => Ok(v),
                _ => Err("Invalid column type")
            }
        }

    }

    impl TryFrom<Column> for Vec<Decimal> {

        type Error = &'static str;

        fn try_from(col : Column) -> Result<Self, Self::Error> {
            match col {
                Column::Numeric(v) => Ok(v),
                _ => Err("Invalid column type")
            }
        }

    }

    impl TryFrom<Column> for Vec<String> {

        type Error = &'static str;

        fn try_from(col : Column) -> Result<Self, Self::Error> {
            match col {
                Column::Str(v) => Ok(v),
                _ => Err("Invalid column type")
            }
        }

    }

    impl TryFrom<Column> for Vec<Vec<u8>> {

        type Error = &'static str;

        fn try_from(col : Column) -> Result<Self, Self::Error> {
            match col {
                Column::Bytes(v) => Ok(v),
                _ => Err("Invalid column type")
            }
        }

    }
    
    impl<T> TryFrom<Column> for Vec<Option<T>>
    where
        Vec<Option<T>> : TryFrom<NullableColumn>,
        NullableColumn : From<Vec<Option<bool>>>,
        NullableColumn : From<Vec<Option<i16>>>,
        NullableColumn : From<Vec<Option<i32>>>,
        NullableColumn : From<Vec<Option<u32>>>,
        NullableColumn : From<Vec<Option<i64>>>,
        NullableColumn : From<Vec<Option<f32>>>,
        NullableColumn : From<Vec<Option<f64>>>,
        NullableColumn : From<Vec<Option<Decimal>>>,
        NullableColumn : From<Vec<Option<String>>>,
        NullableColumn : From<Vec<Option<serde_json::Value>>>,
        NullableColumn : From<Vec<Option<Vec<u8>>>>
    {
    
        type Error = &'static str;
        
        fn try_from(col : Column) -> Result<Self, Self::Error> {
            match col {
                Column::Nullable(c) => {
                    if let Ok(v) = Vec::<Option<T>>::try_from(*c.clone()) {
                        Ok(v)
                    } else {
                        Err("")
                    }
                },
                Column::Bool(mut v) => Vec::<Option<T>>::try_from(NullableColumn::from(v.drain(..).map(|v| Some(v) ).collect::<Vec<_>>())).map_err(|_| "" ),
                Column::I8(mut v) => Vec::<Option<T>>::try_from(NullableColumn::from(v.drain(..).map(|v| Some(v) ).collect::<Vec<_>>())).map_err(|_| "" ),
                Column::I16(mut v) => Vec::<Option<T>>::try_from(NullableColumn::from(v.drain(..).map(|v| Some(v) ).collect::<Vec<_>>())).map_err(|_| "" ),
                Column::I32(mut v) => Vec::<Option<T>>::try_from(NullableColumn::from(v.drain(..).map(|v| Some(v) ).collect::<Vec<_>>())).map_err(|_| "" ),
                Column::U32(mut v) => Vec::<Option<T>>::try_from(NullableColumn::from(v.drain(..).map(|v| Some(v) ).collect::<Vec<_>>())).map_err(|_| "" ),
                Column::I64(mut v) => Vec::<Option<T>>::try_from(NullableColumn::from(v.drain(..).map(|v| Some(v) ).collect::<Vec<_>>())).map_err(|_| "" ),
                Column::F32(mut v) => Vec::<Option<T>>::try_from(NullableColumn::from(v.drain(..).map(|v| Some(v) ).collect::<Vec<_>>())).map_err(|_| "" ),
                Column::F64(mut v) => Vec::<Option<T>>::try_from(NullableColumn::from(v.drain(..).map(|v| Some(v) ).collect::<Vec<_>>())).map_err(|_| "" ),
                Column::Numeric(mut v) => Vec::<Option<T>>::try_from(NullableColumn::from(v.drain(..).map(|v| Some(v) ).collect::<Vec<_>>())).map_err(|_| "" ),
                Column::Str(mut v) => Vec::<Option<T>>::try_from(NullableColumn::from(v.drain(..).map(|v| Some(v) ).collect::<Vec<_>>())).map_err(|_| "" ),
                Column::Json(mut v) => Vec::<Option<T>>::try_from(NullableColumn::from(v.drain(..).map(|v| Some(v) ).collect::<Vec<_>>())).map_err(|_| "" ),
                Column::Bytes(mut v) => Vec::<Option<T>>::try_from(NullableColumn::from(v.drain(..).map(|v| Some(v) ).collect::<Vec<_>>())).map_err(|_| "" ),
            }
        }
    }

}


