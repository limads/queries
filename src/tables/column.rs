/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use rust_decimal::Decimal;
use serde_json::Value;
use super::nullable::*;
use super::field::Field;
use std::borrow::Cow;
use serde_json;
use itertools::Itertools;
use std::cmp::{PartialOrd, PartialEq, Ordering};

use std::str::FromStr;
use std::fmt::Write;

/// Densely packed column, where each variant is a vector of some
/// element that implements postgres::types::ToSql.
#[derive(Debug, Clone, PartialEq)]
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
    Nullable(NullableColumn)
}

pub fn rearrange<T, U>(vs : &[T], ixs : &[usize]) -> U
where
    T : Clone,
    U : From<Vec<T>>
{
    assert!(ixs.len() <= vs.len());
    let mut dst = Vec::with_capacity(ixs.len());
    for i in 0..ixs.len() {
        dst.push(vs[ixs[i]].clone());
    }
    U::from(dst)
}

pub fn filtered<T, U>(vs : &[T], v : T) -> (Vec<usize>, U)
where
    T : PartialEq + Clone,
    U : From<Vec<T>>
{
    let (ixs, vec) : (Vec<usize>, Vec<T>) = vs.iter().cloned()
        .enumerate()
        .filter(|(_, a)| *a == v )
        .unzip();
    (ixs, U::from(vec))
}

pub fn sorted<T, U>(vs : &[T], ascending : bool) -> (Vec<usize>, U)
where
    T : PartialOrd + Clone,
    U : From<Vec<T>>
{
    let (ixs, vec) : (Vec<usize>, Vec<T>) = if ascending {
        vs.iter().cloned()
            .enumerate()
            .sorted_by(|(_, a), (_, b)| a.partial_cmp(&b).unwrap_or(Ordering::Equal) )
            .unzip()
    } else {
        vs.iter().cloned()
            .enumerate()
            .sorted_by(|(_, a), (_, b)| b.partial_cmp(&a).unwrap_or(Ordering::Equal) )
            .unzip()
    };
    (ixs, U::from(vec))
}

pub fn write_binary(buffer : &mut String, s : &[u8]) {
    s.iter().take(MAX_WIDTH_CHARS / 2).for_each(|b| writeln!(buffer, "{:x}", b).unwrap() );
    if s.len() > MAX_WIDTH_CHARS / 2 {
        write!(buffer, "...").unwrap();
    }
}

pub fn display_binary(s : &[u8]) -> String {
    s.iter().map(|b| format!("{:x}", b) ).join("")
}

impl<'a> Column {

    pub fn filtered(&self, val : &str) -> Option<(Vec<usize>, Column)> {
        match self {
            Column::Bool(vs) => {
                let key = if val == "t" || val == "true" {
                    true
                } else if val == "f" || val == "false" {
                    false
                } else {
                    return None;
                };
                Some(filtered(&vs[..], key))
            },
            Column::I8(vs) => {
                Some(filtered(&vs[..], i8::from_str(val).ok()?))
            },
            Column::I16(vs) => {
                Some(filtered(&vs[..], i16::from_str(val).ok()?))
            },
            Column::I32(vs) => {
                Some(filtered(&vs[..], i32::from_str(val).ok()?))
            },
            Column::U32(vs) => {
                Some(filtered(&vs[..], u32::from_str(val).ok()?))
            },
            Column::I64(vs) => {
                Some(filtered(&vs[..], i64::from_str(val).ok()?))
            },
            Column::F32(vs) => {
                Some(filtered(&vs[..], f32::from_str(val).ok()?))
            },
            Column::F64(vs) => {
                Some(filtered(&vs[..], f64::from_str(val).ok()?))
            },
            Column::Numeric(vs) => {
                Some(filtered(&vs[..], Decimal::from_str(val).ok()?))
            },
            Column::Str(vs) => {
                let (ixs, vec) : (Vec<usize>, Vec<String>) = vs.iter().cloned()
                    .enumerate()
                    .filter(|(_, a)| a.matches(val).next().is_some() )
                    .unzip();
                Some((ixs, Column::from(vec)))
            },
            Column::Bytes(_vs) => {
                None
            },
            Column::Json(_vs) => {
                None
            },
            Column::Nullable(_vs) => {
                None
            }
        }
    }

    pub fn rearranged(&self, ixs : &[usize]) -> Column {
        match self {
            Column::Bool(vs) => {
                rearrange(&vs[..], ixs)
            },
            Column::I8(vs) => {
                rearrange(&vs[..], ixs)
            },
            Column::I16(vs) => {
                rearrange(&vs[..], ixs)
            },
            Column::I32(vs) => {
                rearrange(&vs[..], ixs)
            },
            Column::U32(vs) => {
                rearrange(&vs[..], ixs)
            },
            Column::I64(vs) => {
                rearrange(&vs[..], ixs)
            },
            Column::F32(vs) => {
                rearrange(&vs[..], ixs)
            },
            Column::F64(vs) => {
                rearrange(&vs[..], ixs)
            },
            Column::Numeric(vs) => {
                rearrange(&vs[..], ixs)
            },
            Column::Str(vs) => {
                rearrange(&vs[..], ixs)
            },
            Column::Bytes(_vs) => {
                Column::from(self.display_content(None)).rearranged(ixs)
            },
            Column::Json(_vs) => {
                Column::from(self.display_content(None)).rearranged(ixs)
            },
            Column::Nullable(vs) => {
                let vals : NullableColumn = vs.rearranged(ixs);
                Column::Nullable(vals)
            }
        }
    }

    // Returns a sorted clone of the column, with the new index order.
    pub fn sorted(&self, ascending : bool) -> (Vec<usize>, Column) {
        match self {
            Column::Bool(vs) => {
                sorted(&vs[..], ascending)
            },
            Column::I8(vs) => {
                sorted(&vs[..], ascending)
            },
            Column::I16(vs) => {
                sorted(&vs[..], ascending)
            },
            Column::I32(vs) => {
                sorted(&vs[..], ascending)
            },
            Column::U32(vs) => {
                sorted(&vs[..], ascending)
            },
            Column::I64(vs) => {
                sorted(&vs[..], ascending)
            },
            Column::F32(vs) => {
                sorted(&vs[..], ascending)
            },
            Column::F64(vs) => {
                sorted(&vs[..], ascending)
            },
            Column::Numeric(vs) => {
                sorted(&vs[..], ascending)
            },
            Column::Str(vs) => {
                sorted(&vs[..], ascending)
            },
            Column::Bytes(_vs) => {
                Column::from(self.display_content(None)).sorted(ascending)
            },
            Column::Json(_vs) => {
                Column::from(self.display_content(None)).sorted(ascending)
            },
            Column::Nullable(vs) => {
                let (ixs, vals) : (_, NullableColumn) = vs.sorted(ascending);
                (ixs, Column::Nullable(vals))
            }
        }
    }

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

    /*fn to_ref_dyn<'b, T>(v : &'b Vec<T>) -> Vec<&'b (dyn ToSql + Sync)>
    where T : ToSql + Sync
    {
        v.iter().map(|e| e as &'b (dyn ToSql + Sync)).collect()
    }*/

    /// Returns a string field carrying Field::String(missing.unwrap_or("null")) if value is not present;
    /// or None if ix is outside the column range.
    pub fn at(&self, ix : usize, missing : Option<&str>) -> Option<Field> {
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
            Column::Nullable(col) => col.at(ix, missing)
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

    /*pub fn ref_content(&'a self) -> Vec<&(dyn ToSql + Sync)> {
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
    }*/

    pub fn display_with_precision(value : f64, prec : Option<usize>) -> String {
        match prec {
            Some(prec) => match prec {
                1 => format!("{:.1}", value),
                2 => format!("{:.2}", value),
                3 => format!("{:.3}", value),
                4 => format!("{:.4}", value),
                5 => format!("{:.5}", value),
                6 => format!("{:.6}", value),
                7 => format!("{:.7}", value),
                8 => format!("{:.8}", value),
                9 => format!("{:.9}", value),
                10 => format!("{:.10}", value),
                11 => format!("{:.11}", value),
                12 => format!("{:.12}", value),
                13 => format!("{:.13}", value),
                14 => format!("{:.14}", value),
                15 => format!("{:.15}", value),
                16 => format!("{:.16}", value),
                17 => format!("{:.17}", value),
                18 => format!("{:.18}", value),
                19 => format!("{:.19}", value),
                20 => format!("{:.20}", value),
                21 => format!("{:.21}", value),
                22 => format!("{:.22}", value),
                23 => format!("{:.23}", value),
                24 => format!("{:.24}", value),
                25 => format!("{:.25}", value),
                26 => format!("{:.26}", value),
                27 => format!("{:.27}", value),
                28 => format!("{:.28}", value),
                29 => format!("{:.29}", value),
                30 => format!("{:.30}", value),
                31 => format!("{:.31}", value),
                32 => format!("{:.32}", value),
                _ => format!("{}", value)
            },
            None => {
                format!("{}", value)
            }
        }
    }
    
    pub fn write_with_precision(s : &mut String, value : f64, prec : Option<usize>) {
        match prec {
            Some(prec) => match prec {
                1 => writeln!(s, "{:.1}", value).unwrap(),
                2 => writeln!(s, "{:.2}", value).unwrap(),
                3 => writeln!(s, "{:.3}", value).unwrap(),
                4 => writeln!(s, "{:.4}", value).unwrap(),
                5 => writeln!(s, "{:.5}", value).unwrap(),
                6 => writeln!(s, "{:.6}", value).unwrap(),
                7 => writeln!(s, "{:.7}", value).unwrap(),
                8 => writeln!(s, "{:.8}", value).unwrap(),
                9 => writeln!(s, "{:.9}", value).unwrap(),
                10 => writeln!(s, "{:.10}", value).unwrap(),
                11 => writeln!(s, "{:.11}", value).unwrap(),
                12 => writeln!(s, "{:.12}", value).unwrap(),
                13 => writeln!(s, "{:.13}", value).unwrap(),
                14 => writeln!(s, "{:.14}", value).unwrap(),
                15 => writeln!(s, "{:.15}", value).unwrap(),
                16 => writeln!(s, "{:.16}", value).unwrap(),
                17 => writeln!(s, "{:.17}", value).unwrap(),
                18 => writeln!(s, "{:.18}", value).unwrap(),
                19 => writeln!(s, "{:.19}", value).unwrap(),
                20 => writeln!(s, "{:.20}", value).unwrap(),
                21 => writeln!(s, "{:.21}", value).unwrap(),
                22 => writeln!(s, "{:.22}", value).unwrap(),
                23 => writeln!(s, "{:.23}", value).unwrap(),
                24 => writeln!(s, "{:.24}", value).unwrap(),
                25 => writeln!(s, "{:.25}", value).unwrap(),
                26 => writeln!(s, "{:.26}", value).unwrap(),
                27 => writeln!(s, "{:.27}", value).unwrap(),
                28 => writeln!(s, "{:.28}", value).unwrap(),
                29 => writeln!(s, "{:.29}", value).unwrap(),
                30 => writeln!(s, "{:.30}", value).unwrap(),
                31 => writeln!(s, "{:.31}", value).unwrap(),
                32 => writeln!(s, "{:.32}", value).unwrap(),
                _ => writeln!(s, "{}", value).unwrap()
            },
            None => {
                writeln!(s, "{}", value).unwrap()
            }
        }
    }

    pub fn display_content_at_index(&'a self, row_ix : usize, prec : Option<usize>) -> Cow<'a, str> {
        match &self {
            Column::Str(v) => Cow::Borrowed(&v[row_ix]),
            Column::Bool(v) => Cow::Owned(v[row_ix].to_string()),
            Column::I8(v) => Cow::Owned(v[row_ix].to_string()),
            Column::I16(v) => Cow::Owned(v[row_ix].to_string()),
            Column::I32(v) => Cow::Owned(v[row_ix].to_string()),
            Column::U32(v) => Cow::Owned(v[row_ix].to_string()),
            Column::I64(v) => Cow::Owned(v[row_ix].to_string()),
            Column::F32(v) => Cow::Owned(Self::display_with_precision(v[row_ix] as f64, prec)),
            Column::F64(v) => Cow::Owned(Self::display_with_precision(v[row_ix] as f64, prec)),
            Column::Numeric(v) => Cow::Owned(v[row_ix].to_string()),
            Column::Json(v) => {
                Cow::Owned(json_to_string(&v[row_ix]))
            },
            Column::Bytes(v) => Cow::Owned(display_binary(&v[row_ix])),
            Column::Nullable(col) => col.display_content_at_index(row_ix, prec)
        }
    }

    pub fn display_lines(&'a self, prec : Option<usize>, fst_row : Option<usize>, max_rows : Option<usize>) -> String {
        let mut buffer = String::new();
        let max_rows = max_rows.unwrap_or(usize::MAX).max(1);
        let fst_row = fst_row.unwrap_or(1).max(1);
        let fst_row_ix = fst_row - 1;
        match self {
            Column::Bool(v) => v.iter().skip(fst_row_ix).take(max_rows).for_each(|e| writeln!(&mut buffer, "{}", e).unwrap() ),
            Column::I8(v) => v.iter().skip(fst_row_ix).take(max_rows).for_each(|e| writeln!(&mut buffer, "{}", e).unwrap() ),
            Column::I16(v) => v.iter().skip(fst_row_ix).take(max_rows).for_each(|e| writeln!(&mut buffer, "{}", e).unwrap() ),
            Column::I32(v) => v.iter().skip(fst_row_ix).take(max_rows).for_each(|e| writeln!(&mut buffer, "{}", e).unwrap() ),
            Column::U32(v) => v.iter().skip(fst_row_ix).take(max_rows).for_each(|e| writeln!(&mut buffer, "{}", e).unwrap() ),
            Column::I64(v) => v.iter().skip(fst_row_ix).take(max_rows).for_each(|e| writeln!(&mut buffer, "{}", e).unwrap() ),
            Column::F32(v) => v.iter().skip(fst_row_ix).take(max_rows).for_each(|e| Self::write_with_precision(&mut buffer, *e as f64, prec) ),
            Column::F64(v) => v.iter().skip(fst_row_ix).take(max_rows).for_each(|e| Self::write_with_precision(&mut buffer, *e as f64, prec) ),
            Column::Numeric(v) => v.iter().skip(fst_row_ix).take(max_rows).for_each(|e| writeln!(&mut buffer, "{}", e).unwrap() ),
            Column::Str(v) => v.iter().skip(fst_row_ix).take(max_rows).for_each(|e| write_str(&mut buffer, &e[..]) ),
            Column::Json(v) => v.iter().skip(fst_row_ix).take(max_rows).for_each(|e| write_str(&mut buffer, &json_to_string(&e) ) ),
            Column::Bytes(v) => v.iter().skip(fst_row_ix).take(max_rows).for_each(|e| write_binary(&mut buffer, &e[..]) ),
            Column::Nullable(col) => { buffer = col.display_lines(prec, Some(fst_row), Some(max_rows)); }
        }

        // Ignore last line break
        if buffer.ends_with("\n") {
            buffer.truncate(buffer.len()-1);
        }

        buffer
    }

    pub fn display_content(&'a self, prec : Option<usize>) -> Vec<String> {
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
            Column::Json(v) => v.iter().map(|e| json_to_string(e) ).collect(),
            Column::Bytes(v) => v.iter().map(|e| display_binary(&e) ).collect(),
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

    /*pub fn truncate(&mut self, n : usize) {
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
    }*/

}

pub fn json_to_string(v : &serde_json::Value) -> String {

    let mut v_str = v.to_string();
    
    // Iterpret top-level JSON arrays as following postgres array syntax (since this
    // is how they are represeted in the client tables).
    match v {
        serde_json::Value::Array(_) => {
            v_str = format!("{{{}}}", v_str.trim_start_matches("[").trim_end_matches("]"));
        },
        _ => { }
    }
    
    v_str
}

const MAX_WIDTH_CHARS : usize = 140;

pub fn write_str(buffer : &mut String, s : &str) {
    if s.contains("\n") {
        write_str(buffer, &s.replace("\n", "\\n"));
    } else {
        let extrapolated = if s.len() <= MAX_WIDTH_CHARS {
            writeln!(buffer, "{}", s).unwrap();
            false
        } else {
            if let Some((ix, _)) = s.char_indices().nth(MAX_WIDTH_CHARS+1) {
                write!(buffer, "{}", &s[..ix]).unwrap();
                true
            } else {
                write!(buffer, "{}", s).unwrap();
                false
            }
        };
        if extrapolated {
            write!(buffer, "...").unwrap()
        }
    }
}

pub mod from {

    use super::*;
    use std::convert::{ From };

    impl From<NullableColumn> for Column {

        fn from(nc : NullableColumn) -> Self {
            if nc.count_valid() == nc.len() {
                nc.pack()
            } else {
                Column::Nullable(nc)
            }
        }

    }

    impl Into<()> for Column {
        fn into(self) -> () {
            unimplemented!()
        }
    }

    impl From<()> for Column {
        fn from(_value: ()) -> Self {
            unimplemented!()
        }
    }

    impl From<Vec<()>> for Column {
        fn from(_value: Vec<()>) -> Self {
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

    use std::convert::{ TryFrom};
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
                    if let Ok(v) = Vec::<Option<T>>::try_from(c.clone()) {
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


