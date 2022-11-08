/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use super::column::*;
use tokio_postgres::types::{ToSql   };
use std::marker::Sync;
use std::convert::{TryFrom, TryInto};
use std::borrow::Cow;
use std::collections::BTreeMap;
use crate::tables::field::Field;

/// Represents an incomplete column of information, holding
/// the indices from which the valid column entries refer to,
/// and the total column size. This representation is best if
/// there are few null values, since we do not need to store
/// the Option discriminant for all entries. If there are many
/// NULL entries, it is best to store the valid indices on a
/// dense column, and the corresponding valid indices in a BTreeMap<usize, usize>.
#[derive(Debug, Clone)]
pub struct NullableColumn {

    // Contiguous values. Contains default on NULL entries.
    col : Column,

    // The keys are which indices over 0..n are non-NULL; The values are indices for the col field.
    valid_ixs : BTreeMap<usize, usize>,

    n : usize
}

impl<'a> NullableColumn {

    const NULL : &'a str = "NULL";

    /// Returns a string field carrying null if value is not present.
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
        self.n
    }

    pub fn from_col(col : Column) -> Self {
        let n = col.len();
        let mut valid_ixs = BTreeMap::new();
        for ix in 0..n {
            valid_ixs.insert(ix, ix);
        }
        Self{ col, valid_ixs, n }
    }

    pub fn display_content(&self, prec : Option<usize>) -> Vec<String> {
        let mut content = Vec::new();
        for ix in 0..self.n {
            content.push(self.display_content_at_index(ix, prec).to_string());
        }
        content
    }

    /// Tries to convert to a complete representation, or just return a nullable
    /// variant otherwise.
    pub fn to_column(self) -> Column {
        if let Column::Nullable(_) = self.col {
            eprintln!("Recursive nullable column identified");
        }
        if self.valid_ixs.len() == self.n {
            self.col
        } else {
            Column::Nullable(Box::new(self))
        }
    }

    /*pub fn ref_content(&'a self) -> Vec<&'a (dyn ToSql + Sync)>
        where &'a str : FromSql<'a>
    {
        /*if let Column::Nullable(_) = self.col {
            return Vec::new()
        }
        let valid_refs = self.col.ref_content();
        let mut full_refs = Vec::new();
        let mut n_ix = 0;
        for i in 0..self.n {
            if n_ix < self.null_ix.len() && i == self.null_ix[n_ix] {
                full_refs.push(&Self::NULL as &'a (dyn ToSql + Sync));
                n_ix += 1;
            } else {
                full_refs.push(valid_refs[i - n_ix]);
            }
        }
        full_refs*/
        unimplemented!()
    }

    pub fn truncate(&mut self, _n : usize) {
        //self.col.truncate(n);
        unimplemented!()
    }*/

}

impl<T> From<Vec<Option<T>>> for NullableColumn
    where
        T : ToSql + Sync + Clone,
        Column : From<Vec<T>>
{

    fn from(mut opt_vals: Vec<Option<T>>) -> Self {
        let n = opt_vals.len();
        let mut valid_ixs : BTreeMap<usize, usize> = BTreeMap::new();
        let mut data : Vec<T> = Vec::new();
        for (i, opt_value) in opt_vals.drain(0..n).enumerate() {
            if let Some(v) = opt_value {
                data.push(v);
                valid_ixs.insert(i, data.len() - 1);
            } //else {

            // }
        }
        Self { col : data.into(), valid_ixs, n }
    }

}

impl<T> TryInto<Vec<Option<T>>> for NullableColumn
    where
        T : ToSql + Sync + Clone,
        Vec<T> : TryFrom<Column, Error = &'static str>
{
    type Error = &'static str;

    fn try_into(self) -> Result<Vec<Option<T>>, Self::Error> {
        /*let n = self.n;
        let mut null_ix = Vec::new();
        mem::swap(&mut null_ix, &mut self.null_ix);
        let mut valid_vals : Vec<T> = self.col.try_into()
            .map_err(|_| "Error performing conversion")?;
        let mut opt_cols : Vec<Option<T>> = Vec::new();
        let mut n_ix = 0;
        for i  in 0..n {
            if n_ix < self.null_ix.len() && i == self.null_ix[n_ix] {
                opt_cols.push(None);
                n_ix += 1;
            } else {
                opt_cols.push(Some(valid_vals.remove(0)));
            }
        }
        if valid_vals.len() > 0 {
            return Err("Data vector not cleared");
        }
        Ok(opt_cols)*/
        let dense_col = Vec::<T>::try_from(self.col.clone())?;
        let mut opt_col : Vec<Option<T>> = Vec::new();
        for ix in 0..self.n {
            if let Some(dense_ix) = self.valid_ixs.get(&ix) {
                opt_col.push(Some(dense_col[*dense_ix].clone()));
            } else {
                opt_col.push(None);
            }
        }
        Ok(opt_col)
    }

}
