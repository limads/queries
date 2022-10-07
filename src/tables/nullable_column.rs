/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use super::column::*;
use tokio_postgres::types::{ToSql, FromSql};
use std::marker::Sync;
use std::convert::{TryFrom, TryInto};
use std::borrow::Cow;
use std::collections::HashMap;
use crate::tables::field::Field;

/// Represents an incomplete column of information, holding
/// the indices from which the valid column entries refer to,
/// and the total column size. This representation is best if
/// there are few null values, since we do not need to store
/// the Option discriminant for all entries. If there are many
/// NULL entries, it is best to store the valid indices on a
/// dense column, and the corresponding valid indices in a HashMap<usize, usize>.
#[derive(Debug, Clone)]
pub struct NullableColumn {

    // Contiguous values. Contains default on NULL entries.
    col : Column,

    // The keys are which indices over 0..n are non-NULL; The values are indices for the col field.
    valid_ixs : HashMap<usize, usize>,

    n : usize
}

impl<'a> NullableColumn {

    const NULL : &'a str = "NULL";

    /// Returns a string field carrying null if value is not present.
    pub fn at(&self, row_ix : usize, missing : Option<&str>) -> Option<Field> {
        if let Some(dense_ix) = self.valid_ixs.get(&row_ix) {
            self.col.at(*dense_ix, missing)
        } else {
            Some(Field::Str(String::from(missing.unwrap_or(Self::NULL))))
        }
    }

    pub fn len(&self) -> usize {
        self.n
    }

    pub fn from_col(col : Column) -> Self {
        let n = col.ref_content().len();
        // let mut valid_ix = Vec::new();
        // valid_ix.extend((0..n).map(|i| i ));
        let mut valid_ixs = HashMap::new();
        for ix in 0..n {
            valid_ixs.insert(ix, ix);
        }
        Self{ col, valid_ixs, n }
    }

    pub fn display_content_at_index(&'a self, row_ix : usize, prec : usize) -> Cow<'a, str> {
        /*let mut n_ix = 0;
        let mut i = 0;
        while i < row_ix {
            // TODO thread 'main' panicked at 'index out of bounds: the len is 8 but the index is 8', src/tables/nullable_column.rs:37:26
            if let Some(null_ix) = self.null_ix.get(n_ix).map(|ix| ix == row_ix).unwrap_or(false) {
                return Cow::Borrowed(Self::NULL);
            }
            if n_ix < self.null_ix.len() && i == self.null_ix[n_ix] {
                n_ix += 1;
            }
            i+=1;
        }

        self.col.display_content_at_index(i - n_ix, prec)*/
        if let Some(dense_ix) = self.valid_ixs.get(&row_ix) {
            self.col.display_content_at_index(*dense_ix, prec)
        } else {
            Cow::Borrowed(Self::NULL)
        }
    }

    pub fn display_content(&self, prec : usize) -> Vec<String> {
        /*if let Column::Nullable(_) = self.col {
            println!("Recursive nullable column identified");
            return Vec::new()
        }
        let valid_content = self.col.display_content(prec);
        let mut content = Vec::new();
        let mut n_ix = 0;
        for i in 0..self.n {
            if n_ix < self.null_ix.len() && i == self.null_ix[n_ix] {
                content.push(String::from(Self::NULL));
                n_ix += 1;
            } else {
                // TODO thread 'main' panicked at 'index out of bounds: the len is 200 but
                // the index is 200', src/tables/nullable_column.rs:46:30
                content.push(valid_content[i - n_ix].clone());
            }
        }
        content*/
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
            println!("Recursive nullable column identified");
        }
        if self.valid_ixs.len() == self.n {
            self.col
        } else {
            Column::Nullable(Box::new(self))
        }
    }

    pub fn ref_content(&'a self) -> Vec<&'a (dyn ToSql + Sync)>
        where &'a str : FromSql<'a>
    {
        /*if let Column::Nullable(_) = self.col {
            println!("Recursive nullable column identified");
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
    }

}

impl<T> From<Vec<Option<T>>> for NullableColumn
    where
        T : ToSql + Sync + Clone,
        Column : From<Vec<T>>
{

    fn from(mut opt_vals: Vec<Option<T>>) -> Self {
        let n = opt_vals.len();
        let mut valid_ixs : HashMap<usize, usize> = HashMap::new();
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
