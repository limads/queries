use super::column::*;
use tokio_postgres::types::{ToSql   };
use std::marker::Sync;
use std::convert::{TryFrom, TryInto};
use std::borrow::Cow;
use std::collections::BTreeMap;
use crate::tables::field::Field;
use rust_decimal::Decimal;
use serde_json::Value;

#[derive(Debug, Clone)]
pub enum NullableColumn {
    Bool(Vec<Option<bool>>),
    I8(Vec<Option<i8>>),
    I16(Vec<Option<i16>>),
    I32(Vec<Option<i32>>),
    U32(Vec<Option<u32>>),
    I64(Vec<Option<i64>>),
    F32(Vec<Option<f32>>),
    F64(Vec<Option<f64>>),
    Numeric(Vec<Option<Decimal>>),
    Str(Vec<Option<String>>),
    Bytes(Vec<Option<Vec<u8>>>),
    Json(Vec<Option<Value>>)
}

impl<'a> NullableColumn {

    const NULL : &'a str = "NULL";

    pub fn rearranged(&self, ixs : &[usize]) -> Self {
        match self {
            NullableColumn::Bool(vs) => {
                rearrange(&vs[..], ixs)
            },
            NullableColumn::I8(vs) => {
                rearrange(&vs[..], ixs)
            },
            NullableColumn::I16(vs) => {
                rearrange(&vs[..], ixs)
            },
            NullableColumn::I32(vs) => {
                rearrange(&vs[..], ixs)
            },
            NullableColumn::U32(vs) => {
                rearrange(&vs[..], ixs)
            },
            NullableColumn::I64(vs) => {
                rearrange(&vs[..], ixs)
            },
            NullableColumn::F32(vs) => {
                rearrange(&vs[..], ixs)
            },
            NullableColumn::F64(vs) => {
                rearrange(&vs[..], ixs)
            },
            NullableColumn::Numeric(vs) => {
                rearrange(&vs[..], ixs)
            },
            NullableColumn::Str(vs) => {
                rearrange(&vs[..], ixs)
            },
            NullableColumn::Bytes(vs) => {
                NullableColumn::from(self.display_opt_content(None)).rearranged(ixs)
            },
            NullableColumn::Json(vs) => {
                NullableColumn::from(self.display_opt_content(None)).rearranged(ixs)
            }
        }
    }

    pub fn sorted(&self, ascending : bool) -> (Vec<usize>, Self) {
        match self {
            NullableColumn::Bool(vs) => {
                sorted(&vs[..], ascending)
            },
            NullableColumn::I8(vs) => {
                sorted(&vs[..], ascending)
            },
            NullableColumn::I16(vs) => {
                sorted(&vs[..], ascending)
            },
            NullableColumn::I32(vs) => {
                sorted(&vs[..], ascending)
            },
            NullableColumn::U32(vs) => {
                sorted(&vs[..], ascending)
            },
            NullableColumn::I64(vs) => {
                sorted(&vs[..], ascending)
            },
            NullableColumn::F32(vs) => {
                sorted(&vs[..], ascending)
            },
            NullableColumn::F64(vs) => {
                sorted(&vs[..], ascending)
            },
            NullableColumn::Numeric(vs) => {
                sorted(&vs[..], ascending)
            },
            NullableColumn::Str(vs) => {
                sorted(&vs[..], ascending)
            },
            NullableColumn::Bytes(vs) => {
                NullableColumn::from(self.display_opt_content(None)).sorted(ascending)
            },
            NullableColumn::Json(vs) => {
                NullableColumn::from(self.display_opt_content(None)).sorted(ascending)
            }
        }
    }

    pub fn pack(&self) -> Column {
        match self {
            NullableColumn::Bool(v) => Column::from(v.iter().filter_map(|v| v.clone() ).collect::<Vec<_>>()),
            NullableColumn::I8(v) => Column::from(v.iter().filter_map(|v| v.clone() ).collect::<Vec<_>>()),
            NullableColumn::I16(v) => Column::from(v.iter().filter_map(|v| v.clone() ).collect::<Vec<_>>()),
            NullableColumn::I32(v) => Column::from(v.iter().filter_map(|v| v.clone() ).collect::<Vec<_>>()),
            NullableColumn::U32(v) => Column::from(v.iter().filter_map(|v| v.clone() ).collect::<Vec<_>>()),
            NullableColumn::I64(v) => Column::from(v.iter().filter_map(|v| v.clone() ).collect::<Vec<_>>()),
            NullableColumn::F32(v) => Column::from(v.iter().filter_map(|v| v.clone() ).collect::<Vec<_>>()),
            NullableColumn::F64(v) => Column::from(v.iter().filter_map(|v| v.clone() ).collect::<Vec<_>>()),
            NullableColumn::Numeric(v) => Column::from(v.iter().filter_map(|v| v.clone() ).collect::<Vec<_>>()),
            NullableColumn::Str(v) => Column::from(v.iter().filter_map(|v| v.clone() ).collect::<Vec<_>>()),
            NullableColumn::Bytes(v) => Column::from(v.iter().filter_map(|v| v.clone() ).collect::<Vec<_>>()),
            NullableColumn::Json(v) => Column::from(v.iter().filter_map(|v| v.clone() ).collect::<Vec<_>>())
        }
    }

    pub fn count_valid(&self) -> usize {
        match self {
            NullableColumn::Bool(v) => v.iter().filter(|v| v.is_some()).count(),
            NullableColumn::I8(v) => v.iter().filter(|v| v.is_some()).count(),
            NullableColumn::I16(v) => v.iter().filter(|v| v.is_some()).count(),
            NullableColumn::I32(v) => v.iter().filter(|v| v.is_some()).count(),
            NullableColumn::U32(v) => v.iter().filter(|v| v.is_some()).count(),
            NullableColumn::I64(v) => v.iter().filter(|v| v.is_some()).count(),
            NullableColumn::F32(v) => v.iter().filter(|v| v.is_some()).count(),
            NullableColumn::F64(v) => v.iter().filter(|v| v.is_some()).count(),
            NullableColumn::Numeric(v) => v.iter().filter(|v| v.is_some()).count(),
            NullableColumn::Str(v) => v.iter().filter(|v| v.is_some()).count(),
            NullableColumn::Bytes(v) => v.iter().filter(|v| v.is_some()).count(),
            NullableColumn::Json(v) => v.iter().filter(|v| v.is_some()).count()
        }
    }

    /// Returns a string field carrying null if value is not present.
    pub fn at(&self, ix : usize, missing : Option<&str>) -> Option<Field> {
        match self {
            NullableColumn::Bool(v) => v.get(ix).cloned().map(|f| f.map(|f| Field::Bool(f)  ).unwrap_or(Field::Str(String::from(Self::NULL))) ),
            NullableColumn::I8(v) => v.get(ix).cloned().map(|f| f.map(|f| Field::I8(f) ).unwrap_or(Field::Str(String::from(Self::NULL))) ),
            NullableColumn::I16(v) => v.get(ix).cloned().map(|f| f.map(|f| Field::I16(f) ).unwrap_or(Field::Str(String::from(Self::NULL))) ),
            NullableColumn::I32(v) => v.get(ix).cloned().map(|f| f.map(|f| Field::I32(f) ).unwrap_or(Field::Str(String::from(Self::NULL))) ),
            NullableColumn::U32(v) => v.get(ix).cloned().map(|f| f.map(|f| Field::U32(f) ).unwrap_or(Field::Str(String::from(Self::NULL))) ),
            NullableColumn::I64(v) => v.get(ix).cloned().map(|f| f.map(|f| Field::I64(f) ).unwrap_or(Field::Str(String::from(Self::NULL))) ),
            NullableColumn::F32(v) => v.get(ix).cloned().map(|f| f.map(|f| Field::F32(f) ).unwrap_or(Field::Str(String::from(Self::NULL))) ),
            NullableColumn::F64(v) => v.get(ix).cloned().map(|f| f.map(|f| Field::F64(f) ).unwrap_or(Field::Str(String::from(Self::NULL))) ),
            NullableColumn::Numeric(v) => v.get(ix).cloned().map(|f| f.map(|f| Field::Numeric(f.clone()) ).unwrap_or(Field::Str(String::from(Self::NULL))) ),
            NullableColumn::Str(v) => v.get(ix).cloned().map(|f| f.map(|f| Field::Str(f.clone()) ).unwrap_or(Field::Str(String::from(Self::NULL))) ),
            NullableColumn::Json(v) => v.get(ix).cloned().map(|f| f.map(|f| Field::Json(f.clone()) ).unwrap_or(Field::Str(String::from(Self::NULL))) ),
            NullableColumn::Bytes(v) => v.get(ix).cloned().map(|f| f.map(|f| Field::Bytes(f.clone()) ).unwrap_or(Field::Str(String::from(Self::NULL))) )
        }
    }

    pub fn len(&self) -> usize {
        match self {
            NullableColumn::Bool(v) => v.len(),
            NullableColumn::I8(v) => v.len(),
            NullableColumn::I16(v) => v.len(),
            NullableColumn::I32(v) => v.len(),
            NullableColumn::U32(v) => v.len(),
            NullableColumn::I64(v) => v.len(),
            NullableColumn::F32(v) => v.len(),
            NullableColumn::F64(v) => v.len(),
            NullableColumn::Numeric(v) => v.len(),
            NullableColumn::Str(v) => v.len(),
            NullableColumn::Bytes(v) => v.len(),
            NullableColumn::Json(v) => v.len()
        }
    }

    pub fn display_opt_content(&'a self, prec : Option<usize>) -> Vec<Option<String>> {
        match self {
            NullableColumn::Bool(v) => v.iter().map(|e| e.as_ref().map(|e| e.to_string() )).collect(),
            NullableColumn::I8(v) => v.iter().map(|e| e.as_ref().map(|e| e.to_string() ) ).collect(),
            NullableColumn::I16(v) => v.iter().map(|e| e.as_ref().map(|e| e.to_string() ) ).collect(),
            NullableColumn::I32(v) => v.iter().map(|e| e.as_ref().map(|e| e.to_string() ) ).collect(),
            NullableColumn::U32(v) => v.iter().map(|e| e.as_ref().map(|e| e.to_string() )).collect(),
            NullableColumn::I64(v) => v.iter().map(|e| e.as_ref().map(|e| e.to_string() )).collect(),
            NullableColumn::F32(v) => v.iter().map(|e| e.as_ref().map(|e| Column::display_with_precision(*e as f64, prec) )).collect(),
            NullableColumn::F64(v) => v.iter().map(|e| e.as_ref().map(|e| Column::display_with_precision(*e as f64, prec) )).collect(),
            NullableColumn::Numeric(v) => v.iter().map(|d| d.as_ref().map(|d| d.to_string() )).collect(),
            NullableColumn::Str(v) => v.iter().map(|e| e.as_ref().map(|e| e.to_string() )).collect(),
            NullableColumn::Json(v) => v.iter().map(|e| e.as_ref().map(|e| json_to_string(&e) )).collect(),
            NullableColumn::Bytes(v) => v.iter().map(|e| e.as_ref().map(|e| display_binary(&e) )).collect(),
        }
    }

    pub fn display_content(&'a self, prec : Option<usize>) -> Vec<String> {
        match self {
            NullableColumn::Bool(v) => v.iter().map(|e| e.as_ref().map(|e| e.to_string() ).unwrap_or(String::from(Self::NULL)) ).collect(),
            NullableColumn::I8(v) => v.iter().map(|e| e.as_ref().map(|e| e.to_string() ).unwrap_or(String::from(Self::NULL) )).collect(),
            NullableColumn::I16(v) => v.iter().map(|e| e.as_ref().map(|e| e.to_string() ).unwrap_or(String::from(Self::NULL) ) ).collect(),
            NullableColumn::I32(v) => v.iter().map(|e| e.as_ref().map(|e| e.to_string() ).unwrap_or(String::from(Self::NULL) )).collect(),
            NullableColumn::U32(v) => v.iter().map(|e| e.as_ref().map(|e| e.to_string() ).unwrap_or(String::from(Self::NULL) )).collect(),
            NullableColumn::I64(v) => v.iter().map(|e| e.as_ref().map(|e| e.to_string() ).unwrap_or(String::from(Self::NULL) )).collect(),
            NullableColumn::F32(v) => v.iter().map(|e| e.as_ref().map(|e| Column::display_with_precision(*e as f64, prec) ).unwrap_or(String::from(Self::NULL) )).collect(),
            NullableColumn::F64(v) => v.iter().map(|e| e.as_ref().map(|e| Column::display_with_precision(*e as f64, prec) ).unwrap_or(String::from(Self::NULL) )).collect(),
            NullableColumn::Numeric(v) => v.iter().map(|d| d.as_ref().map(|d| d.to_string() ).unwrap_or(String::from(Self::NULL) )).collect(),
            NullableColumn::Str(v) => v.iter().map(|e| e.as_ref().map(|e| e.to_string() ).unwrap_or(String::from(Self::NULL) )).collect(),
            NullableColumn::Json(v) => v.iter().map(|e| e.as_ref().map(|e| json_to_string(&e) ).unwrap_or(String::from(Self::NULL) )).collect(),
            NullableColumn::Bytes(v) => v.iter().map(|e| e.as_ref().map(|e| display_binary(&e) ).unwrap_or(String::from(Self::NULL) )).collect(),
        }
    }

    pub fn display_content_at_index(&'a self, row_ix : usize, prec : Option<usize>) -> Cow<'a, str> {
        match &self {
            NullableColumn::Str(ref v) => v[row_ix].as_ref().map(|v| Cow::Borrowed(&v[..]) ).unwrap_or(Cow::Borrowed(Self::NULL)),
            NullableColumn::Bool(ref v) => v[row_ix].as_ref().map(|v| Cow::Owned(v.to_string()) ).unwrap_or(Cow::Borrowed(Self::NULL)),
            NullableColumn::I8(ref v) => v[row_ix].as_ref().map(|v| Cow::Owned(v.to_string()) ).unwrap_or(Cow::Borrowed(Self::NULL)),
            NullableColumn::I16(ref v) => v[row_ix].as_ref().map(|v| Cow::Owned(v.to_string()) ).unwrap_or(Cow::Borrowed(Self::NULL)),
            NullableColumn::I32(ref v) => v[row_ix].as_ref().map(|v| Cow::Owned(v.to_string()) ).unwrap_or(Cow::Borrowed(Self::NULL)),
            NullableColumn::U32(ref v) => v[row_ix].as_ref().map(|v| Cow::Owned(v.to_string()) ).unwrap_or(Cow::Borrowed(Self::NULL)),
            NullableColumn::I64(ref v) => v[row_ix].as_ref().map(|v| Cow::Owned(v.to_string()) ).unwrap_or(Cow::Borrowed(Self::NULL)),
            NullableColumn::F32(ref v) => v[row_ix].as_ref().map(|v| Cow::Owned(Column::display_with_precision(*v as f64, prec)) )
                .unwrap_or(Cow::Borrowed(Self::NULL)),
            NullableColumn::F64(ref v) => v[row_ix].as_ref().map(|v| Cow::Owned(Column::display_with_precision(*v as f64, prec)) )
                .unwrap_or(Cow::Borrowed(Self::NULL)),
            NullableColumn::Numeric(ref v) => v[row_ix].as_ref().map(|v| Cow::Owned(v.to_string()) ).unwrap_or(Cow::Borrowed(Self::NULL)),
            NullableColumn::Json(ref v) => {
                v[row_ix].as_ref().map(|v| Cow::Owned(json_to_string(&v)) ).unwrap_or(Cow::Borrowed(Self::NULL))
            },
            NullableColumn::Bytes(ref v) => {
                v[row_ix]
                    .as_ref()
                    .map(|v| Cow::Owned(display_binary(&v)) )
                    .unwrap_or(Cow::Borrowed(Self::NULL))
            },
        }
    }

}

pub mod from {

    use super::*;
    use std::convert::{ From };

    impl From<Vec<Option<bool>>> for NullableColumn {
        fn from(value: Vec<Option<bool>>) -> Self {
            Self::Bool(value)
        }
    }

    impl From<Vec<Option<i8>>> for NullableColumn {

        fn from(value: Vec<Option<i8>>) -> Self {
            Self::I8(value)
        }
    }

    impl From<Vec<Option<i16>>> for NullableColumn {
        fn from(value: Vec<Option<i16>>) -> Self {
            Self::I16(value)
        }
    }

    impl From<Vec<Option<i32>>> for NullableColumn {
        fn from(value: Vec<Option<i32>>) -> Self {
            Self::I32(value)
        }
    }

    impl From<Vec<Option<u32>>> for NullableColumn {
        fn from(value: Vec<Option<u32>>) -> Self {
            Self::U32(value)
        }
    }

    impl From<Vec<Option<i64>>> for NullableColumn {
        fn from(value: Vec<Option<i64>>) -> Self {
            Self::I64(value)
        }
    }

    impl From<Vec<Option<f32>>> for NullableColumn {
        fn from(value: Vec<Option<f32>>) -> Self {
            Self::F32(value)
        }
    }

    impl From<Vec<Option<f64>>> for NullableColumn {
        fn from(value: Vec<Option<f64>>) -> Self {
            Self::F64(value)
        }
    }

    impl From<Vec<Option<Decimal>>> for NullableColumn {
        fn from(value: Vec<Option<Decimal>>) -> Self {
            Self::Numeric(value)
        }
    }

    impl From<Vec<Option<String>>> for NullableColumn {
        fn from(value: Vec<Option<String>>) -> Self {
            Self::Str(value)
        }
    }

    impl From<Vec<Option<Value>>> for NullableColumn {
        fn from(value: Vec<Option<Value>>) -> Self {
            Self::Json(value)
        }
    }

    impl From<Vec<Option<Vec<u8>>>> for NullableColumn {
        fn from(value: Vec<Option<Vec<u8>>>) -> Self {
            Self::Bytes(value)
        }
    }

}

pub mod try_into {

    use std::convert::{TryFrom};
    use super::*;

    impl TryFrom<NullableColumn> for Vec<Option<bool>> {

        type Error = &'static str;

        fn try_from(col : NullableColumn) -> Result<Self, Self::Error> {
            match col {
                NullableColumn::Bool(v) => Ok(v),
                _ => Err("Invalid column type")
            }
        }

    }

    impl TryFrom<NullableColumn> for Vec<Option<i8>> {

        type Error = &'static str;

        fn try_from(col : NullableColumn) -> Result<Self, Self::Error> {
            match col {
                NullableColumn::I8(v) => Ok(v),
                _ => Err("Invalid column type")
            }
        }

    }

    impl TryFrom<NullableColumn> for Vec<Option<i16>> {

        type Error = &'static str;

        fn try_from(col : NullableColumn) -> Result<Self, Self::Error> {
            match col {
                NullableColumn::I16(v) => Ok(v),
                _ => Err("Invalid column type")
            }
        }

    }

    impl TryFrom<NullableColumn> for Vec<Option<Value>> {

        type Error = &'static str;

        fn try_from(col : NullableColumn) -> Result<Self, Self::Error> {
            match col {
                NullableColumn::Json(v) => Ok(v),
                _ => Err("Invalid column type")
            }
        }

    }

    impl TryFrom<NullableColumn> for Vec<Option<i32>> {

        type Error = &'static str;

        fn try_from(col : NullableColumn) -> Result<Self, Self::Error> {
            match col {
                NullableColumn::I32(v) => Ok(v),
                _ => Err("Invalid column type")
            }
        }

    }

    impl TryFrom<NullableColumn> for Vec<Option<u32>> {

        type Error = &'static str;

        fn try_from(col : NullableColumn) -> Result<Self, Self::Error> {
            match col {
                NullableColumn::U32(v) => Ok(v),
                _ => Err("Invalid column type")
            }
        }

    }

    impl TryFrom<NullableColumn> for Vec<Option<i64>> {

        type Error = &'static str;

        fn try_from(col : NullableColumn) -> Result<Self, Self::Error> {
            match col {
                NullableColumn::I64(v) => Ok(v),
                _ => Err("Invalid column type")
            }
        }

    }

    impl TryFrom<NullableColumn> for Vec<Option<f32>> {

        type Error = &'static str;

        fn try_from(col : NullableColumn) -> Result<Self, Self::Error> {
            match col {
                NullableColumn::F32(v) => Ok(v),
                _ => Err("Invalid column type")
            }
        }

    }

    impl TryFrom<NullableColumn> for Vec<Option<f64>> {

        type Error = &'static str;

        fn try_from(col : NullableColumn) -> Result<Self, Self::Error> {
            match col {
                NullableColumn::F64(v) => Ok(v),
                _ => Err("Invalid column type")
            }
        }

    }

    impl TryFrom<NullableColumn> for Vec<Option<Decimal>> {

        type Error = &'static str;

        fn try_from(col : NullableColumn) -> Result<Self, Self::Error> {
            match col {
                NullableColumn::Numeric(v) => Ok(v),
                _ => Err("Invalid column type")
            }
        }

    }

    impl TryFrom<NullableColumn> for Vec<Option<String>> {

        type Error = &'static str;

        fn try_from(col : NullableColumn) -> Result<Self, Self::Error> {
            match col {
                NullableColumn::Str(v) => Ok(v),
                _ => Err("Invalid column type")
            }
        }

    }

    impl TryFrom<NullableColumn> for Vec<Option<Vec<u8>>> {

        type Error = &'static str;

        fn try_from(col : NullableColumn) -> Result<Self, Self::Error> {
            match col {
                NullableColumn::Bytes(v) => Ok(v),
                _ => Err("Invalid column type")
            }
        }

    }
}
