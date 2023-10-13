/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use tokio_postgres::{self, types::ToSql };
use tokio_postgres::types::{self, Type, FromSql};
use tokio_postgres::row;
use std::convert::{TryInto, TryFrom};
use rust_decimal::Decimal;
use super::column::*;
use std::fmt::{self, Display};
use std::string::ToString;
use num_traits::cast::ToPrimitive;
use std::str::FromStr;
use std::default::Default;
use serde_json::Value;
use std::error::Error;
use itertools::Itertools;
use std::borrow::Cow;
use std::iter::ExactSizeIterator;
use std::cmp::{Eq, PartialEq};
use quick_xml::Reader;
use quick_xml::events::{Event };
use crate::tables::nullable::NullableColumn;
use std::ops::Index;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct TableSource {

    pub name : Option<String>,

    pub relation : Option<String>

}

/// Data-owning structure that encapsulate named columns.
/// Implementation guarantees all columns are of the same size.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Table {

    name : Option<String>,

    relation : Option<String>,

    names : Vec<String>,

    cols : Vec<Column>,

    nrows : usize,

    format : TableSettings,

}

pub trait RowIterator<'a>
where
    Self : Iterator<Item=Cow<'a, str>> + ExactSizeIterator
{

}

impl<'a, T> RowIterator<'a> for T
where
    T : Iterator<Item=Cow<'a, str>> + ExactSizeIterator
{

}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuoteType {
    Single,
    Double
}

#[test]
fn tbl_from_html() {
    let s = r#"<table><thead><tr><th>Header1</th><th>Header2</th></tr></thead><tbody><tr>
    <td>Value1</td><td>Value2</td></tr><tr><td>Value3</td><td>Value4</td></tr></tbody></table>"#;
    println!("{:?}", Table::from_html(s));
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum HTMLTag {
    Table,
    THead,
    TR,
    TH,
    TBody,
    TD,
}

impl<'a> Index<&'a str> for Table {

    type Output=Column;

    fn index(&self, ix : &'a str) -> &Column {
        &self.get_column_by_name(ix).unwrap()
    }

}

impl Index<usize> for Table {

    type Output=Column;

    fn index(&self, ix : usize) -> &Column {
        &self.cols[ix]
    }

}

impl Table {

    pub fn as_json_values(&self) -> Option<serde_json::Map<String, serde_json::Value>> {
        let mut map = serde_json::Map::new();
        for (n, c) in self.names.iter().zip(self.cols.iter()) {
            map.insert(n.to_string(), c.as_json_values()?);
        }
        Some(map)
    }

    pub fn filtered_by(&self, col_ix : usize, val : &str) -> Option<Table> {
        let (ixs, filtered_col) = self.cols[col_ix].filtered(val)?;
        let mut cols = Vec::new();
        for i in 0..self.cols.len() {
            if i == col_ix {
                cols.push(filtered_col.clone());
            } else {
                let filtered = self.cols[i].rearranged(&ixs[..]);
                cols.push(filtered);
            }
        }
        Some(Table::new(None, self.names.clone(), cols).ok()?)
    }

    pub fn sorted_by(&self, col_ix : usize, ascending : bool) -> Option<Table> {
        // let col_ix = self.names.iter().position(|c| c == col )?;
        let (ixs, sorted_col) = self.cols[col_ix].sorted(ascending);
        let mut cols = Vec::new();
        for i in 0..self.cols.len() {
            if i == col_ix {
                cols.push(sorted_col.clone());
            } else {
                let rearranged = self.cols[i].rearranged(&ixs);
                cols.push(rearranged);
            }
        }
        Some(Table::new(None, self.names.clone(), cols).ok()?)
    }

    pub fn ncols(&self) -> usize {
        self.cols.len()
    }
    
    pub fn nrows(&self) -> usize {
        self.cols.get(0).map(|c| c.len() ).unwrap_or(0)
    }
    
    // Transpose this table, setting all columns to type text.
    pub fn transpose(&self) -> Table {
        let mut cols = Vec::new();
        for _ in 0..(self.cols.len()+1) {
            cols.push(Vec::new());
        }
        let txt_rows = self.text_rows(None, None, true, 0);
        
        for n in &self.names {
            cols[0].push(n.to_string());
        }
        
        for r in txt_rows {
            for (i, txt) in r.enumerate() {
                cols[i+1].push(txt.to_string());
            }
        }
        
        Self {
            name : None,
            relation : None,
            names : (0..(self.cols.len()+1)).map(|_| String::new() ).collect(),
            cols : cols.drain(..).map(|c| Column::from(c) ).collect(),
            nrows : self.nrows,
            format : self.format.clone()
        }
    }
    
    pub fn display_content_at<'a>(&'a self, row_ix : usize, col_ix : usize, precision : Option<usize>) -> Option<Cow<'a, str>> {
        Some(self.cols.get(col_ix)?.display_content_at_index(row_ix, precision))
    }
    
    /// Joints two tables, as long as they have the same number of rows and column names are unique.
    pub fn join(mut self, other : &Table) -> Result<Table, String> {
        if self.shape().0 != other.shape().0 {
            return Err(format!("Table shape mismatch"));
        }
        self.names.extend(other.names.clone());
        self.cols.extend(other.cols.clone());
        if self.names.iter().unique().count() != self.names.iter().count() {
            return Err(format!("Cannot join tables with duplicated column names"));
        }
        Ok(self)
    }

    pub fn is_empty(&self) -> bool {
        self.cols.len() == 0 || self.cols[0].len() == 0
    }
    
    pub fn empty(names : Vec<String>) -> Self {
        let cols : Vec<_> = (0..names.len()).map(|_| Column::new_empty::<bool>() ).collect();
        Table {
            name : None,
            relation : None,
            names,
            cols,
            nrows : 0,
            format : TableSettings::default()
        }
    }
    
    pub fn from_sqlite_rows(
        names : Vec<String>,
        col_tys : &[String],
        mut rows : rusqlite::Rows
    ) -> Result<Table, &'static str>
    where
        NullableColumn : From<Vec<Option<i64>>>,
        NullableColumn : From<Vec<Option<f64>>>,
        NullableColumn : From<Vec<Option<String>>>,
        NullableColumn : From<Vec<Option<Vec<u8>>>>
    {

        use crate::server::SqliteColumn;
        let empty_cols : Result<Vec<Column>, &'static str> = col_tys.iter().map(|c| {
            let sq_c = SqliteColumn::new(c).map_err(|_| "Unknown column type" )?;
            let nc : NullableColumn = sq_c.into();
            Ok(Column::from(nc))
        }).collect();
        let empty_cols : Vec<_> = empty_cols?;
        if names.len() == 0 {
            return Err("No columns available");
        }
        let mut sqlite_cols : Vec<SqliteColumn> = Vec::new();
        let mut curr_row = 0;
        while let Ok(row) = rows.next() {
            match row {
                Some(r) => {
                    if curr_row == 0 {
                        for c_ix in 0..names.len() {
                            sqlite_cols.push(SqliteColumn::new_from_first_value(&r, c_ix)?);
                        }
                    } else {
                        for (i, col) in sqlite_cols.iter_mut().enumerate() {
                            // let value = r.get::<usize, rusqlite::types::Value>(i)
                            //    .unwrap_or(rusqlite::types::Value::Null);
                            // TODO panicking here when using a sqlite subtraction.
                            // sqlite_cols[i].try_append(value)?;
                            col.append_from_row(r, i);
                        }
                    }
                    curr_row += 1;
                },
                None => { break; }
            }
        }
        if curr_row == 0 {
            Ok(Table::new(None, names, empty_cols)?)
        } else {
            let mut null_cols : Vec<NullableColumn> = sqlite_cols
                .drain(0..sqlite_cols.len())
                .map(|c| c.into() ).collect();
            if null_cols.len() == 0 {
                return Err("Too few columns");
            }
            let cols : Vec<Column> = null_cols.drain(0..null_cols.len())
                .map(|nc| Column::from(nc) )
                .collect();
            Ok(Table::new(None, names, cols)?)
        }
    }

    pub fn from_rows(rows : &[row::Row]) -> Result<Table, &'static str> {
        let mut names : Vec<String> = rows.get(0)
            .map(|r| r.columns().iter().map(|c| c.name().to_string()).collect() )
            .ok_or("No rows available")?;
        let mut n_unnamed = 1;
        for (_ix, name) in names.iter_mut().enumerate() {
            if &name[..] == "?column?" {
                *name = format!("(Unnamed {})", n_unnamed);
                n_unnamed += 1;
            }
        }
        let row1 = rows.iter().next().ok_or("No first row available")?;
        let cols = row1.columns();
        let col_types : Vec<_> = cols.iter().map(|c| c.type_()).collect();
        if names.len() == 0 {
            return Err("No columns available");
        }
        let ncols = names.len();
        let mut null_cols : Vec<NullableColumn> = Vec::new();
        for i in 0..ncols {
            let is_bool = col_types[i] == &Type::BOOL;
            let is_bytea = col_types[i] == &Type::BYTEA;
            let is_text = col_types[i] == &Type::TEXT ||
                col_types[i] == &Type::VARCHAR ||
                col_types[i] == &Type::BPCHAR ||
                col_types[i] == &Type::NAME;
            let is_char = col_types[i] == &Type::CHAR;
            let is_double = col_types[i] == &Type::FLOAT8;
            let is_float = col_types[i] == &Type::FLOAT4;
            let is_int = col_types[i] == &Type::INT4;
            let is_long = col_types[i] == &Type::INT8;
            let is_oid = col_types[i] == &Type::OID;
            let is_smallint = col_types[i] == &Type::INT2;
            let is_timestamp = col_types[i] == &Type::TIMESTAMP;
            let is_timestamp_tz = col_types[i] == &Type::TIMESTAMPTZ;
            let is_date = col_types[i] == &Type::DATE;
            let is_time = col_types[i] == &Type::TIME;
            let is_numeric = col_types[i] == &Type::NUMERIC;
            // let is_money = col_types[i] == &Type::MONEY;
            let is_uuid = col_types[i] == &Type::UUID;
            let is_json = col_types[i] == &Type::JSON || col_types[i] == &Type::JSONB;
            let is_point = col_types[i] == &Type::POINT;
            // let is_line = col_types[i] == &Type::LINE;
            let is_box = col_types[i] == &Type::BOX;
            let is_path = col_types[i] == &Type::PATH;
            let is_text_arr = col_types[i] == &Type::TEXT_ARRAY;
            let is_real_arr = col_types[i] == &Type::FLOAT4_ARRAY;
            let is_dp_arr = col_types[i] == &Type::FLOAT8_ARRAY;
            let is_smallint_arr = col_types[i] == &Type::INT2_ARRAY;
            let is_int_arr = col_types[i] == &Type::INT4_ARRAY;
            let is_decimal_arr = col_types[i] == &Type::NUMERIC_ARRAY;
            let is_bigint_arr = col_types[i] == &Type::INT8_ARRAY;
            let _is_xml = col_types[i] == &Type::XML;
            let is_json_arr = col_types[i] == &Type::JSON_ARRAY;
            let array_ty = if is_text_arr {
                Some(ArrayType::Text)
            } else if is_real_arr {
                Some(ArrayType::Float4)
            } else if is_dp_arr {
                Some(ArrayType::Float8)
            } else if is_smallint_arr {
                Some(ArrayType::Int2)
            } else if is_int_arr {
                Some(ArrayType::Int4)
            } else if is_bigint_arr {
                Some(ArrayType::Int8)
            } else if is_json_arr {
                Some(ArrayType::Json)
            } else if is_decimal_arr {
                Some(ArrayType::Decimal)
            } else {
                None
            };
            if is_bool {
                null_cols.push(nullable_from_rows::<bool>(rows, i)?);
            } else if is_bytea {
                null_cols.push(nullable_from_rows::<Vec<u8>>(rows, i)?);
            } else if is_text {
                null_cols.push(nullable_from_rows::<String>(rows, i)?);
            } else if is_char {
                null_cols.push(nullable_from_rows::<i8>(rows, i)?);
            } else if is_double {
                null_cols.push(nullable_from_rows::<f64>(rows, i)?);
            } else if is_float {
                null_cols.push(nullable_from_rows::<f32>(rows, i)?);
            } else if is_int {
                null_cols.push(nullable_from_rows::<i32>(rows, i)?);
            } else if is_smallint {
                null_cols.push(nullable_from_rows::<i16>(rows, i)?);
            } else if is_long {
                null_cols.push(nullable_from_rows::<i64>(rows, i)?);
            } else if is_oid {
                null_cols.push(nullable_from_rows::<u32>(rows, i)?);
            } else if is_timestamp {
                null_cols.push(as_nullable_text::<chrono::NaiveDateTime>(rows, i)?);
            } else if is_timestamp_tz {
                null_cols.push(as_nullable_text::<chrono::DateTime<chrono::Utc>>(rows, i)?);
            } else if is_date {
                null_cols.push(as_nullable_text::<chrono::NaiveDate>(rows, i)?);
            } else if is_time {
                null_cols.push(as_nullable_text::<chrono::NaiveTime>(rows, i)?);
            } else if is_uuid {
                null_cols.push(as_nullable_text::<uuid::Uuid>(rows, i)?);
            } else if is_point {
                null_cols.push(as_nullable_json_value::<geo_types::Point<f64>>(rows, i)?);
            } else if is_path {
               null_cols.push(as_nullable_json_value::<geo_types::LineString<f64>>(rows, i)?);
            } else if is_box {
                null_cols.push(as_nullable_json_value::<geo_types::Rect<f64>>(rows, i)?);
            } else if is_numeric {
                null_cols.push(nullable_from_rows::<Decimal>(rows, i)?);
            } else if is_json {
                null_cols.push(nullable_from_rows::<Value>(rows, i)?);
            } else if let Some(ty) = array_ty {
                null_cols.push(nullable_from_arr(rows, i, ty)?);
            } else {
                null_cols.push(nullable_unable_to_parse(rows, col_types[i]));
            }
        }

        // TODO missing hstore identifier
        // let is_hstore = col_types[i] == &Type::HSTORE;
        // if is_hstore {
        //    null_cols.push(hstore_to_value(rows, i)?);

        let cols : Vec<Column> = null_cols.drain(0..names.len())
            .map(|nc| Column::from(nc) ).collect();
        Ok(Table::new(None, names, cols)?)
    }

    pub fn single_json_field(&self) -> Option<serde_json::Value> {
        match self.cols.len() {
            1 => self.cols[0].single_json_row(),
            _ => None
        }
    }

    pub fn from_html(html : &str) -> Option<Self> {
        let mut reader = Reader::from_str(html);
	    reader.trim_text(true);
	    let mut buf = Vec::new();
        let mut tags = Vec::new();
        let mut header = Vec::new();
        let mut cols = Vec::new();
        let mut col_ix = 0;
	    loop {
	        let event = reader.read_event(&mut buf).ok()?;
		    match &event {
		        Event::Start(ref e) => {
		            match e.name() {
		                b"table" => {
                            tags.push(HTMLTag::Table);
		                },
		                b"thead" => {
                            tags.push(HTMLTag::THead);
                            col_ix = 0;
		                },
		                b"tr" => {
                            tags.push(HTMLTag::TR);
                            col_ix = 0;
		                },
		                b"th" => {
                            tags.push(HTMLTag::TH);
		                },
		                b"tbody" => {
                            tags.push(HTMLTag::TBody);
                            col_ix = 0;
		                },
		                b"td" => {
                            tags.push(HTMLTag::TD);
		                },
		                _ => {
		                    return None;
		                }
		            }
		        },
		        Event::Text(ref ev) => {
		            let n = tags.len();
		            let txt = ev.unescape_and_decode(&reader).unwrap();
                    if n == 4 {
                        match &tags[n-4..] {
                            [HTMLTag::Table, HTMLTag::THead, HTMLTag::TR, HTMLTag::TH] => {
                                header.push(txt);
                                col_ix += 1;
                            },
                            [HTMLTag::Table, HTMLTag::TBody, HTMLTag::TR, HTMLTag::TD] => {
                                if cols.len() == col_ix {
                                    cols.push(Vec::new());
                                }
                                cols[col_ix].push(txt);
                                col_ix += 1;
                            },
                            _ => {
                                return None;
                            }
                        }
                    }
		        },
		        Event::End(ref _e) => {
		        	if tags.len() > 0 {
		        	    tags.pop();
		        	} else {
		        	    return None;
		        	}
		        },
		        Event::Eof => {
		            break;
		        },
		        _ => {

		        },
		    }
	    }

	    // TODO attempt field conversion

        Some(Table::new(None, header, cols.drain(..).map(|c| Column::from(c) ).collect()).ok()?)
    }

    pub fn new(name : Option<String>, names : Vec<String>, cols : Vec<Column>) -> Result<Self, &'static str> {
        if names.len() != cols.len() {
            return Err("Differing number of names and columns");
        }
        let nrows = if let Some(col0) = cols.get(0) {
            col0.len()
        } else {
            return Err("No column zero");
        };
        for c in cols.iter().skip(1) {
            if c.len() != nrows {
                return Err("Number of rows mismatch at table creation");
            }
        }
        
        /*if names.iter().unique().count() == names.iter().count() {
            Ok(Self { name, relation : None, names, cols, nrows, format : Default::default(), })
        } else {
            Err("Column names are not unique")
        }*/
        
        Ok(Self { name, relation : None, names, cols, nrows, format : Default::default(), })
    }

    /// Returns (name, relation) pair
    pub fn source(&self) -> TableSource {
        TableSource { name : self.name.clone(), relation : self.relation.clone() }
    }

    pub fn set_name(&mut self, name : Option<String>) {
        self.name = name;
    }

    pub fn set_relation(&mut self, relation : Option<String>) {
        self.relation = relation;
    }

    pub fn new_from_text(
        source : String
    ) -> Result<Self, &'static str> {
        match csv::parse_csv_as_text_cols(&source.clone()) {
            Ok(mut cols) => {
                let mut parsed_cols = Vec::new();
                let mut names = Vec::new();
                for (name, values) in cols.drain(0..) {
                    let mut parsed_int = Vec::new();
                    let mut parsed_float = Vec::new();
                    let mut parsed_json = Vec::new();
                    let mut all_int = true;
                    let mut all_float = true;
                    let mut all_json = true;
                    for s in values.iter() {
                        if all_int {
                            if let Ok(int) = s.parse::<i64>() {
                                parsed_int.push(int);
                            } else {
                                all_int = false;
                            }
                        }
                        if all_float {
                            if let Ok(float) = s.parse::<f64>() {
                                parsed_float.push(float);
                            } else {
                                all_float = false;
                            }
                        }
                        if all_json {
                            if let Ok(json) = s.parse::<Value>() {
                                parsed_json.push(json);
                            } else {
                                all_json = false;
                            }
                        }
                    }
                    match (all_int, all_float, all_json) {
                        (true, _, _) => parsed_cols.push(Column::I64(parsed_int)),
                        (false, true, _) => parsed_cols.push(Column::F64(parsed_float)),
                        (false, false, true) => parsed_cols.push(Column::Json(parsed_json)),
                        _ => parsed_cols.push(Column::Str(values))
                    }
                    names.push(name);
                }
                Ok(Table::new(None, names, parsed_cols)?)
            },
            Err(_e) => {
                Err("Could not parse CSV content")
            }
        }
    }

    /*pub fn flatten<'a>(&'a self) -> Result<Vec<Vec<&'a (dyn ToSql+Sync)>>, &'static str> {
        let dyn_cols : Vec<_> = self.cols.iter().map(|c| c.ref_content()).collect();
        if dyn_cols.len() == 0 {
            return Err("Query result is empty");
        }
        let n = dyn_cols[0].len();
        let mut dyn_rows = Vec::new();
        for r in 0..n {
            let mut dyn_r = Vec::new();
            for c in dyn_cols.iter() {
                dyn_r.push(c[r]);
            }
            dyn_rows.push(dyn_r);
        }
        Ok(dyn_rows)
    }*/

    /// Show all content as text (including column header)
    pub fn text_rows<'a>(
        &'a self,
        max_nrows : Option<usize>, 
        max_ncols : Option<usize>,
        include_header : bool,
        fst_row : usize
    ) -> Vec<std::boxed::Box<dyn RowIterator + 'a>> {
        let sz = self.nrows + 1;
        let mut rows : Vec<std::boxed::Box<dyn RowIterator>> = Vec::with_capacity(sz);

        let nrows = max_nrows.unwrap_or(self.nrows).min(self.nrows);
        if fst_row >= nrows {
            return Vec::new();
        }

        let ncols = max_ncols.unwrap_or(self.cols.len());

        if include_header {
            rows.push(Box::new(self.names.iter().take(ncols).map(|n| Cow::Borrowed(&n[..]) )) as Box<dyn RowIterator + 'a> );
        }

        for row_ix in fst_row..nrows {
            rows.push(
                Box::new(
                    self.cols.iter()
                        .take(ncols)
                        .map(move |col| col.display_content_at_index(row_ix, self.format.prec) )
                ) as Box<dyn RowIterator + 'a>
            );
        }
        rows
    }

    /// Show sequence of column data (omiting column headers).
    pub fn text_cols<'a>(&'a self) -> impl ExactSizeIterator<Item=Cow<'a, [String]>> {
        self.cols.iter().map(move |col| {
            match col {
                Column::Str(ref s) => Cow::Borrowed(&s[..]),
                _ => Cow::Owned(col.display_content(self.format.prec).into())
            }
        })
    }

    /// Returns a SQL string (valid for SQlite3/PostgreSQL subset)
    /// which will contain both the table creation and data insertion
    /// commands. Binary columns are created but will hold NULL. Fails
    /// if table is not named.
    /// TODO check if SQL is valid (maybe external to the struct). SQL can
    /// be invalid if there are reserved keywords as column names.
    pub fn sql_string(&self, name : &str) -> Result<String, String> {
        if let Some(mut creation) = self.sql_table_creation(name, &[]) {
            creation += &self.sql_table_insertion(name, &[])?;
            /*match crate::sql::parsing::parse_sql(&creation[..], &HashMap::new()) {
                Ok(_) => Ok(creation),
                Err(e) => Err(format!("{}", e))
            }*/
            Ok(creation)
        } else {
            Err(format!("Unable to form create table statement"))
        }
    }

    pub fn sql_types(&self) -> Vec<String> {
        self.cols.iter().map(|c| c.sqlite3_type().to_string()).collect()
    }

    pub fn display_lines(&self, col_ix : usize, fst_row : Option<usize>, max_rows : Option<usize>) -> String {
        self.cols[col_ix].display_lines(self.format.prec, fst_row, max_rows)
    }

    pub fn sql_table_creation(&self, name : &str, _cols : &[String]) -> Option<String> {
        let mut query = format!("CREATE TABLE {}(", name);
        for (i, (name, col)) in self.names.iter().zip(self.cols.iter()).enumerate() {
            let name = match name.chars().find(|c| *c == ' ') {
                Some(_) => String::from("\"") + &name[..] + "\"",
                None => name.clone()
            };
            query += &format!("{} {}", name, col.sqlite3_type());
            if i < self.cols.len() - 1 {
                query += ","
            } else {
                query += ");\n"
            }
        }
        Some(query)
    }

    /// Always successful, but query might be empty if there is no data on the columns.
    pub fn sql_table_insertion(&self, name : &str, cols : &[String]) -> Result<String, String> {
        let mut stmt = String::new();
        let nrows = self.nrows();
        
        for i in 0..(cols.len()-1) {
            for j in (i+1)..cols.len() {
                if &cols[i][..] == &cols[j][..] {
                    return Err(String::from("Duplicated columns"));
                }
            }
        }
        
        if cols.len() == 0 {
            stmt += &format!("insert into {} values ", name)[..];
        } else {
            let tuple = insertion_tuple(&cols);
            stmt += &format!("insert into {} {} values ", name, tuple)[..];
        }
        
        let types = self.sql_types();
        let order : Vec<usize> = if cols.len() == 0 {
            (0..self.names.len()).collect()
        } else {
            cols.iter().filter_map(|c| self.names.iter().position(|n| &n[..] == &c[..] ) ).collect()
        };

        let mut curr_row = Vec::new();
        let mut content = self.text_rows(None, None, true, 0);
        let ncol = order.len();
        for (line_n, line) in content.iter_mut().skip(1).enumerate() {
            stmt += "(";
            curr_row.clear();
            curr_row.extend(line);
            for i in 0..order.len() {
                let f = &curr_row[order[i]];
                let t = &types[order[i]];
                append_field(&mut stmt, f, t, QuoteType::Single);
                if i < ncol - 1 {
                    stmt += ","
                } else {
                    if line_n < nrows - 1 {
                        stmt += "),";
                    } else {
                        stmt += ");\n";
                    }
                }
            }
        }
        Ok(stmt)
    }

    /// Decide if column at ix should be displayed, according to the current display rules.
    fn show_column(&self, ix : usize) -> bool {
        if let Some(show) = self.format.show_only.as_ref() {
            show.iter()
                .find(|s| &s[..] == &self.names[ix][..] )
                .is_some()
        } else {
            true
        }
    }

    pub fn to_csv(&self) -> String {
        let mut content = String::new();
        let mut text_rows = self.text_rows(None, None, true, 0);
        let n = text_rows.len();
        let types = self.sql_types();
        for (row_ix, row) in text_rows.iter_mut().enumerate() {
            for (i, field) in row.enumerate() {
                // Skip columns that should not be shown
                if self.show_column(i) {
                    if i >= 1 {
                        content += ",";
                    }
                    // Assume type is text when appending the first (header) field.
                    // Verify the type for the remaining (non-header) fields.
                    if row_ix == 0 {
                        append_field(&mut content, &field, "text", QuoteType::Double);
                    } else {
                        append_field(&mut content, &field, &types[i], QuoteType::Double);
                    }
                }
            }
            if row_ix < n - 1 {
                content += "\n";
            }
        }
        content
    }

    pub fn to_tex(&self) -> String {
        let mut rows = self.text_rows(None, None, true, 0);
        let mut tex = String::new();
        tex += r"\begin{tabular}";
        let ncol = self.cols.len();
        for (i, row) in rows.iter_mut().enumerate() {
            if i == 0 {
                tex += "{ ";
                for c in 0..ncol {
                    if self.show_column(c) {
                        tex += "c ";
                    }
                }
                tex += "}\n";
            }

            for (j, field) in row.enumerate() {
                if self.show_column(j) {
                    tex += &format!("{} ", field);
                    if j < ncol - 1 {
                        tex += "& ";
                    } else {
                        tex += r"\\";
                        tex += "\n";
                    }
                }
            }
        }
        tex += r"\end{tabular}";
        tex += "\n";
        tex
    }

    pub fn to_markdown(&self) -> String {
        let mut rows = self.text_rows(None, None, true, 0);
        let mut md = String::new();
        for (i, row) in rows.iter_mut().enumerate() {
            for (j, field) in row.enumerate() {
                if self.show_column(j) {
                    md += &format!("|{}", field);
                }
            }
            md += &format!("|\n");
            if i == 0 {
                for j in 0..row.len() {
                    if self.show_column(j) {
                        let header_sep = match self.format.align {
                            Align::Left => "|:---",
                            Align::Center => "|:---:",
                            Align::Right => "|---:",
                        };
                        md += header_sep;
                    }
                }
                md += "|\n";
            }
        }
        md
    }

    pub fn to_html(&self) -> String {
        let mut html = String::new();

        html += "<table>\n";

        html += "<thead>\n";
        html += "<tr>\n";
        for ref name in &self.names {
            html += "<th>";
            html += &name[..];
            html += "</th>\n";
        }
        html += "</tr>\n";
        html += "</thead>\n";

        html += "<tbody>\n";
        for ref mut row in self.text_rows(None, None, true, 0).iter_mut().skip(1) {
            html += "<tr>\n";
            for cell in row {
                html += "<td>\n";
                html += &cell[..];
                html += "</td>\n";
            }
            html += "</tr>\n";
        }
        html += "</tbody>\n";

        html += "</table>";

        html
    }

    pub fn to_ooxml(&self, name : Option<String>, style : Option<String>) -> String {
        let mut ooxml = String::new();
        let name = name.unwrap_or(String::from("Table1"));
        let style = style.unwrap_or(String::from("Table1"));
        ooxml += &format!("<table:table table:name=\"{}\" table:style-name={}\"Table1\", table:template-name=\"Academic\">", name, style);
        let snames : Vec<char> = (65..90u32).map(|i| char::try_from(i).unwrap() ).collect();

        for ix in 0..self.cols.len() {
            ooxml += &format!("<table:table-column table:style-name=\"{}.{}\" />", style, snames[ix]);
        }

        for r in 0..self.cols[0].len() {

            ooxml += "<table:table-row table:style-name=\"Table1.1\">";

            for c in 0..self.cols.len() {
                ooxml += &format!("<table:table-cell table:style-name=\"Table1.{}{}\" office:value-type=\"string\">", snames[c], r);
                ooxml += &format!("<text:p text:style-name=\"P2\">{}</text:p>", self.cols[c].at(r, None).unwrap().display_content() );
                ooxml += "</table:table-cell>";
            }

            ooxml += "</table:table-row>";
        }
        ooxml += "</table:table>";

        ooxml
    }

    pub fn size(&self) -> (usize, usize) {
        (self.nrows, self.cols.len())
    }

    pub fn shape(&self) -> (usize, usize) {
        (self.nrows, self.cols.len())
    }

    pub fn get_columns<'a>(&'a self, ixs : &[usize]) -> Columns<'a> {
        let mut cols = Columns::new();
        for ix in ixs.iter() {
            match (self.names.get(*ix), self.cols.get(*ix)) {
                (Some(name), Some(col)) => { cols = cols.take_and_push(name, col, *ix); },
                _ => { }
            }
        }
        cols
    }

    pub fn get_column<'a>(&'a self, ix : usize) -> Option<&'a Column> {
        self.cols.get(ix)
    }

    pub fn get_column_by_name<'a>(&'a self, name : &str) -> Option<&'a Column> {
        let pos = self.names.iter().position(|col_name| &col_name[..] == &name[..] )?;
        self.get_column(pos)
    }

    pub fn names(&self) -> Vec<String> {
        self.names.clone()
    }

    pub fn take_columns(self) -> Vec<Column> {
        self.cols
    }

    /*/// If self has more rows than n, trim it. Pass self unchanged otherwise
    pub fn truncate(mut self, n : usize) -> Self {
        for col in self.cols.iter_mut() {
            col.truncate(n);
        }
        self
    }*/

    pub fn update_format(&mut self, settings : TableSettings) {
        self.format = settings;
    }

}

fn append_field(buffer : &mut String, field : &str, ty : &str, quote : QuoteType) {
    let requires_quote = ["text", "TEXT", "char", "CHAR"].iter()
        .any(|tname| ty.contains(tname) );
    let is_quoted = match quote {
        QuoteType::Single => field.starts_with("'") && field.ends_with("'"),
        QuoteType::Double => field.starts_with("\"") && field.ends_with("\"")
    };
    let should_quote = requires_quote && !is_quoted;
    if should_quote {
        match quote {
            QuoteType::Single => {
                *buffer += "'";
                *buffer += field.replace("'", "''").as_ref();
                *buffer += "'";
            },
            QuoteType::Double => {
                *buffer += "\"";
                *buffer += field.replace("\"", "\"\"").as_ref();
                *buffer += "\"";
            }
        }
    } else {
        *buffer +=&field;
    }
}

impl Display for Table {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let content = match self.format.format {
            Format::Csv => self.to_csv(),
            Format::Markdown => self.to_markdown(),
            Format::Html => self.to_html()
        };
        write!(f, "{}", content)
    }

}

pub fn insertion_tuple(cols : &[String]) -> String {
    let mut tuple = String::new();
    tuple += "(";
    for name in cols.iter().take(cols.len().saturating_sub(1)) {
        tuple += &name[..];
        tuple += ",";
    }
    if let Some(lst) = cols.last() {
        tuple += &lst[..];
        tuple += ")";
    }
    tuple
}

#[derive(Debug)]
pub enum NotNumericErr {
    HasNull,
    IsNot,
    DecConversion,
    InvalidIndex
}

impl Display for NotNumericErr {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let msg = match self {
            Self::HasNull => "Null fields",
            Self::IsNot => "Column is not numeric",
            Self::DecConversion => "Invalid decimal conversion",
            Self::InvalidIndex => "Invalid index"
        };
        write!(f, "{}", msg)
    }

}

impl Error for NotNumericErr { }

/// Referential structure that encapsulate iteration over named columns.
/// Since columns might have different tables as their source,
/// there is no guarantee columns will have the same size.
#[derive(Clone, Debug)]
pub struct Columns<'a> {
    names : Vec<&'a str>,
    cols : Vec<&'a Column>,
    ixs : Vec<usize>
}

impl<'a> Columns<'a> {

    pub fn new() -> Self {
        Self{ names : Vec::new(), cols: Vec::new(), ixs : Vec::new() }
    }

    pub fn take_and_push(mut self, name : &'a str, col : &'a Column, ix : usize) -> Self {
        self.names.push(name);
        self.cols.push(col);
        self.ixs.push(ix);
        self
    }

    pub fn take_and_extend(mut self, cols : Columns<'a>) -> Self {
        self.names.extend(cols.names);
        self.cols.extend(cols.cols);
        self.ixs.extend(cols.ixs);
        self
    }

    pub fn names(&'a self) -> &'a [&'a str] {
        &self.names[..]
    }

    pub fn indices(&'a self) -> &'a [usize] {
        &self.ixs[..]
    }

    pub fn get(&'a self, ix : usize) -> Option<&'a Column> {
        self.cols.get(ix).map(|c| *c)
    }

    // TODO move this to the implementation of try_into(.)
    /// Tries to retrieve a cloned copy from a column, performing any valid
    /// upcasts required to retrieve a f64 numeric type.
    pub fn try_numeric(&'a self, ix : usize) -> Result<Vec<f64>, NotNumericErr>
    where
        Column : TryInto<Vec<f64>,Error=&'static str>
    {
        if let Some(dbl) = self.try_access::<f64>(ix) {
            return Ok(dbl);
        }
        if let Some(float) = self.try_access::<f32>(ix) {
            let cvt : Vec<f64> = float.iter().map(|f| *f as f64).collect();
            return Ok(cvt);
        }
        if let Some(short) = self.try_access::<i16>(ix) {
            let cvt : Vec<f64> = short.iter().map(|s| *s as f64).collect();
            return Ok(cvt);
        }
        if let Some(int) = self.try_access::<i32>(ix) {
            let cvt : Vec<f64> = int.iter().map(|i| *i as f64).collect();
            return Ok(cvt);
        }
        if let Some(int) = self.try_access::<i32>(ix) {
            let cvt : Vec<f64> = int.iter().map(|i| *i as f64).collect();
            return Ok(cvt);
        }
        if let Some(uint) = self.try_access::<u32>(ix) {
            let cvt : Vec<f64> = uint.iter().map(|u| *u as f64).collect();
            return Ok(cvt);
        }
        if let Some(long) = self.try_access::<i64>(ix) {
            let cvt : Vec<f64> = long.iter().map(|l| *l as f64).collect();
            return Ok(cvt);
        }
        if let Some(dec) = self.try_access::<Decimal>(ix) {
            let mut cvt : Vec<f64> = Vec::new();
            for d in dec.iter() {
                if let Some(f) = d.to_f64() {
                    cvt.push(f);
                } else {
                    return Err(NotNumericErr::DecConversion);
                }
            }
            return Ok(cvt);
        }
        match self.cols.get(ix) {
            Some(Column::Nullable(_)) => Err(NotNumericErr::HasNull),
            Some(_) => Err(NotNumericErr::IsNot),
            None => Err(NotNumericErr::InvalidIndex)
        }
    }

    pub fn try_access<T>(&'a self, ix : usize) -> Option<Vec<T>>
        where
            Column : TryInto<Vec<T>, Error=&'static str>
    {
        if let Some(c) = self.get(ix) {
            let v : Result<Vec<T>,_> = c.clone().try_into();
            match v {
                Ok(c) => { Some(c) },
                Err(_) => { None }
            }
        } else {
            None
        }
    }

}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Format {
    Csv,
    Markdown,
    Html
}

impl FromStr for Format {

    type Err = ();

    fn from_str(s: &str) -> Result<Self, ()> {
        match s {
            "CSV" => Ok(Format::Csv),
            "HTML" => Ok(Format::Html),
            "Markdown" => Ok(Format::Markdown),
            _ => Err(())
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Align {
    Left,
    Center,
    Right
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoolField {
    Char,
    CharUpper,
    Word,
    WordUpper,
    Integer
}

impl FromStr for BoolField {

    type Err = ();

    fn from_str(s: &str) -> Result<Self, ()> {
        match s {
            "'t' or 'f'" => Ok(Self::Char),
            "'T' or 'F'" => Ok(Self::CharUpper),
            "'true' or 'False'" => Ok(Self::Word),
            "'TRUE' or 'FALSE'" => Ok(Self::WordUpper),
            "'1' or '0'" => Ok(Self::WordUpper),
            _ => Err(())
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum NullField {
    Word,
    WordUpper,
    Omit
}

impl FromStr for NullField {

    type Err = ();

    fn from_str(s: &str) -> Result<Self, ()> {
        match s {
            "null" => Ok(Self::Word),
            "NULL" => Ok(Self::WordUpper),
            "Omit'" => Ok(Self::Omit),
            _ => Err(())
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableSettings {
    pub format : Format,
    pub align : Align,
    pub bool_field : BoolField,
    pub null_field : NullField,
    pub prec : Option<usize>,
    pub show_only : Option<Vec<String>>
}

impl Default for TableSettings {

    fn default() -> Self {
        Self {
            format : Format::Csv,
            align : Align::Left,
            bool_field : BoolField::Word,
            null_field : NullField::Omit,
            prec : Some(8),
            show_only : None
        }
    }

}

pub fn full_csv_display(tbl : &mut Table, cols : Vec<String>) -> String {
    let show = if cols.len() == 0 {
        None
    } else {
        Some(cols)
    };
    let fmt = TableSettings {
        format : Format::Csv,
        align : Align::Left,
        bool_field : BoolField::Char,
        null_field : NullField::WordUpper,
        prec : None,
        show_only : show
    };
    tbl.update_format(fmt);
    let csv_tbl = format!("{}", tbl);
    csv_tbl
}

mod csv {

    use ::csv;

    fn parse_header(
        csv_reader : &mut csv::Reader<&[u8]>
    ) -> Option<Vec<String>> {
        let mut header_entries = Vec::new();
        if let Ok(header) = csv_reader.headers() {
            for entry in header.iter() {
                let e = entry.to_string();
                header_entries.push(e);
            }
            Some(header_entries)
        } else {
            None
        }
    }

    /// CSV files might have unnamed columns. In this case,
    /// attribute arbirtrary names "Column {i}" for i in 1..k
    /// to the columns, and return them as the first tuple element.
    /// Consider the first line as actual data and return them
    /// in the second tuple element. If the first line has
    /// valid names, return None. The csv crate considers
    /// the first row as a header by default, so we should check that
    /// we don't have a "pure" data file.
    fn _try_convert_header_to_data(header : &[String]) -> Option<(Vec<String>, Vec<String>)> {
        let mut new_header = Vec::new();
        let mut first_line = Vec::new();
        for (i, e) in header.iter().enumerate() {
            match e.parse::<f64>() {
                Ok(f) => {
                    new_header.push(String::from("(Column ") + &i.to_string() + ")");
                    first_line.push(f.to_string());
                },
                Err(_) => { }
            }
        }
        if new_header.len() == header.len() {
            Some((new_header, first_line))
        } else {
            None
        }
    }

    /// Given a textual content as CSV, return a HashMap of its columns as strings.
    pub fn parse_csv_as_text_cols(
        content : &String
    ) -> Result<Vec<(String, Vec<String>)>, String> {
        let mut csv_reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(false)
            .trim(csv::Trim::All)
            .quote(b'"')
            .quoting(true)
            .from_reader(content.as_bytes());
        let header : Vec<String> = parse_header(&mut csv_reader)
            .ok_or("No CSV header at informed file".to_string())?;
        
        let data_keys = header.clone();
        let mut data_vec : Vec<(String, Vec<String>)> = Vec::new();
        for d in data_keys.iter() {
            data_vec.push( (d.clone(), Vec::new()) );
        }
        
        let mut n_records = 0;
        for (ix_rec, row_record) in csv_reader.records().enumerate() {
            match row_record {
                Ok(ref row) => {
                    n_records += 1;
                    let mut n_fields = 0;
                    for (i, entry) in row.iter().enumerate() {
                        if let Some((_,v)) = data_vec.get_mut(i) {
                            v.push(entry.to_string());
                            n_fields += 1;
                        } else {
                            return Err("Unable to get mutable reference to data vector".into())
                        }
                    }
                    match n_fields {
                        0 => { return Err(format!("Record {:?} (Line {}) had zero fields", row, ix_rec)); },
                        _ => { }
                    }
                },
                Err(e) => {
                    return Err(format!("Error parsing CSV record (Line {}): {}", ix_rec, e));
                }
            }
        }

        match n_records {
            0 => Err("No records available.".to_string()),
            _ => Ok(data_vec)
        }
    }

}

// Used to unroll JSON and build data tables to generate ooxml/html reports.
impl TryFrom<serde_json::Value> for Table {

    type Error = String;

    fn try_from(value : serde_json::Value) -> Result<Self, String> {

        match value {
            Value::Object(obj) => {
                let mut names = Vec::new();
                let mut cols = Vec::new();
                let mut n = 0;
                for (col_ix, (k, v)) in obj.iter().enumerate() {
                    match v {
                        Value::Array(arr) => {
                            if col_ix == 0 {
                                n = arr.len();
                            } else {
                                if arr.len() != n {
                                    return Err(format!("Field {} has {} values, but previous column(s) have {} values", k, arr.len(), n));
                                }
                            }

                            if arr.len() == 0 {
                                names.push(k.clone());
                                cols.push(Column::from(Vec::<String>::new()));
                                continue;
                            }

                            let mut col = match &arr[0] {
                                Value::Bool(b) => {
                                    Column::from(vec![*b])
                                },
                                Value::Number(n) => {
                                    if n.is_f64() {
                                        Column::from(vec![n.as_f64().unwrap()])
                                    } else {
                                        if n.is_i64() {
                                            Column::from(vec![n.as_i64().unwrap()])
                                        } else {
                                            return Err(format!("Array column elements for {} required to be numeric", k));
                                        }
                                    }
                                },
                                Value::String(txt) => {
                                    Column::from(vec![txt.clone()])
                                },
                                _ => { return Err(format!("Array column elements for {} required to be boolean, number or string", k)); }
                            };
                            for ix in 1..n {
                                match col {
                                    Column::Bool(ref mut bs) => {
                                        bs.push(arr[ix].as_bool().ok_or(format!("Value for {} should be bool", k))?);
                                    },
                                    Column::F64(ref mut fs) => {
                                        fs.push(arr[ix].as_f64().ok_or(format!("Value for {} should be f64", k))?);
                                    },
                                    Column::I64(ref mut is) => {
                                        is.push(arr[ix].as_i64().ok_or(format!("Value for {} should be i64", k))?);
                                    },
                                    Column::Str(ref mut ts) => {
                                        ts.push(arr[ix].as_str().ok_or(format!("Value for {} should be str", k))?.to_string());
                                    },
                                    _ => {
                                        return Err(format!("Invalid column format for {}", k))
                                    }
                                }
                            }
                            names.push(k.clone());
                            cols.push(col);
                        },
                        _ => return Err(format!("Value at field {} required to be JSON array", k))
                    }
                }
                Table::new(None, names, cols).map_err(|e| format!("{}", e) )
            },
            _ => Err(format!("Root JSON object should be a key:value map"))
        }
    }

}

pub fn col_as_vec<'a, T>(
    rows : &'a [row::Row],
    ix : usize
) -> Result<Vec<T>, &'static str>
    where
        T : FromSql<'a> + ToSql + Sync,
{
    let mut data = Vec::new();
    for r in rows.iter() {
        let datum = r.try_get::<usize, T>(ix)
            .map_err(|_e| { "Unable to parse column" })?;
        data.push(datum);
    }
    Ok(data)
}

pub fn col_as_opt_vec<'a, T>(
    rows : &'a [row::Row],
    ix : usize
) -> Result<Vec<Option<T>>, &'static str>
    where
        T : FromSql<'a> + ToSql + Sync,
{
    let mut opt_data = Vec::new();
    for r in rows.iter() {
        let opt_datum = r.try_get::<usize, Option<T>>(ix)
            .map_err(|_e| { "Unable to parse column" })?;
        opt_data.push(opt_datum);
    }
    Ok(opt_data)
}

pub fn hstore_to_value(
    rows : &[row::Row],
    ix : usize
) -> Result<NullableColumn, &'static str> {
    let mut opt_data = col_as_opt_vec::<HashMap<String, Option<String>>>(rows, ix)?;
    let vals : Vec<Option<Value>> = opt_data.drain(..)
        .map(|opt_hash| opt_hash.and_then(|hash| serde_json::to_value(hash).ok() ))
        .collect();
    Ok(NullableColumn::from(vals))
}

pub fn nullable_from_rows<'a, T>(
    rows : &'a [row::Row],
    ix : usize
) -> Result<NullableColumn, &'static str>
    where
        T : FromSql<'a> + ToSql + Sync,
        NullableColumn : From<Vec<Option<T>>>
{
    let opt_data = col_as_opt_vec::<T>(rows, ix)?;
    Ok(NullableColumn::from(opt_data))
}

pub fn as_nullable_text<'a, T>(
    rows : &'a [row::Row],
    ix : usize
) -> Result<NullableColumn, &'static str>
    where
        T : FromSql<'a> + ToSql + Sync + ToString,
        NullableColumn : From<Vec<Option<String>>>
{
    let opt_data = col_as_opt_vec::<T>(rows, ix)?;
    let str_data : Vec<Option<String>> = opt_data.iter()
        .map(|opt| opt.as_ref().map(|o| o.to_string()) ).collect();
    Ok(NullableColumn::from(str_data))
}

pub fn as_nullable_json_value<'a, T>(
    rows : &'a [row::Row],
    ix : usize
) -> Result<NullableColumn, &'static str>
    where
        T : FromSql<'a> + ToSql + Sync + serde::Serialize,
        NullableColumn : From<Vec<Option<String>>>
{
    let mut opt_data = col_as_opt_vec::<T>(rows, ix)?;
    let val_data : Result<Vec<Option<Value>>, &'static str> = opt_data.drain(..)
        .map(|opt| match opt {
            Some(t) => {
                if let Ok(val) = serde_json::to_value(t) {
                    Ok(Some(val))
                } else {
                    Err("Unable to serialize")
                }
            },
            None => Ok(None)
        }).collect();
    Ok(NullableColumn::from(val_data?))
}

pub enum ArrayType {
    Float4,
    Float8,
    Text,
    Int2,
    Int4,
    Int8,
    Decimal,
    Json
}

pub fn nullable_unable_to_parse<'a>(rows : &'a [row::Row], ty_name : &types::Type) -> NullableColumn {
    let unable_to_parse : Vec<Option<String>> = rows.iter()
        .map(|_| Some(format!("Unable to parse ({})", ty_name)))
        .collect();
    NullableColumn::from(unable_to_parse)
}

pub fn json_value_or_null<T>(v : Option<T>) -> Option<serde_json::Value>
where
    serde_json::Value : From<T>
{
    if let Some(v) = v {
        Some(serde_json::Value::from(v))
    } else {
        None
    }
}

pub fn nullable_from_arr<'a>(
    rows : &'a [row::Row],
    ix : usize,
    ty : ArrayType
) -> Result<NullableColumn, &'static str> {
    let data : Vec<Option<serde_json::Value>> = match ty {
        ArrayType::Float4 => {
            col_as_opt_vec::<Vec<f32>>(rows, ix)?.drain(..).map(|v| json_value_or_null(v) ).collect()
        },
        ArrayType::Float8 => {
            col_as_opt_vec::<Vec<f64>>(rows, ix)?.drain(..).map(|v| json_value_or_null(v) ).collect()
        },
        ArrayType::Int2 => {
            col_as_opt_vec::<Vec<i16>>(rows, ix)?.drain(..).map(|v| json_value_or_null(v) ).collect()
        },
        ArrayType::Int4 => {
            col_as_opt_vec::<Vec<i32>>(rows, ix)?.drain(..).map(|v| json_value_or_null(v) ).collect()
        },
        ArrayType::Int8 => {
            col_as_opt_vec::<Vec<i64>>(rows, ix)?.drain(..).map(|v| json_value_or_null(v) ).collect()
        },
        ArrayType::Text => {
            col_as_opt_vec::<Vec<String>>(rows, ix)?.drain(..).map(|v| json_value_or_null(v) ).collect()
        },
        ArrayType::Json => {
            col_as_opt_vec::<Vec<serde_json::Value>>(rows, ix)?.drain(..).map(|v| json_value_or_null(v) ).collect()
        },
        ArrayType::Decimal => {
            let mut col = col_as_opt_vec::<Vec<Decimal>>(rows, ix)?;
            let mut fcol : Vec<Option<Vec<f64>>> = col.drain(..).map(|v| match v {
                Some(v) => Some(v.clone().drain(..).map(|v| v.to_f64().unwrap_or(std::f64::NAN) ).collect::<Vec<_>>()),
                None => None
            }).collect();
            fcol.drain(..).map(|v| json_value_or_null(v) ).collect()
        }
    };
    Ok(NullableColumn::from(data))
}

// cargo test -- sorted_table --nocapture
#[test]
fn sorted_table() {
    let seq = 0..10i32;
    let tbl = Table::new(
        None,
        vec![
            format!("i8"),
            format!("i16"),
            format!("i32"),
            format!("u32"),
            format!("i64"),
            format!("f32"),
            format!("f64"),
            format!("numeric"),
            format!("str")
        ],
        vec![
            Column::I8(seq.clone().map(|i| i as i8).collect()),
            Column::I16(seq.clone().map(|i| i as i16).collect()),
            Column::I32(seq.clone().map(|i| i as i32).collect()),
            Column::U32(seq.clone().map(|i| i as u32).collect()),
            Column::I64(seq.clone().map(|i| i as i64).collect()),
            Column::F32(seq.clone().map(|i| i as f32).collect()),
            Column::F64(seq.clone().map(|i| i as f64).collect()),
            Column::Numeric(seq.clone().map(|i| Decimal::new(i as i64, 2) ).collect()),
            Column::Str(seq.clone().map(|i| format!("{}", char::from(i as u8 + 100)) ).collect())
        ]
    ).unwrap();
    for i in 0..8 {
        for j in (i+1)..9 {
            assert!(tbl.sorted_by(i, true) == tbl.sorted_by(j, true));
            assert!(tbl.sorted_by(i, false) == tbl.sorted_by(j, false));
        }
    }
}


