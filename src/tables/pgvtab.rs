/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use rusqlite::vtab::{self, *};
use std::default::Default;
use rusqlite::{self, Error};
use std::ffi::c_int;
use postgres::{Client, Row};
use super::postgre;
use super::table::Table;

#[repr(C)]
struct PGVTab {

   base : sqlite3_vtab,

   cli : Client,

   query : String,

   tbl : Table
}

unsafe impl<'vtab> VTab<'vtab> for PGVTab {

    type Aux = ();

    type Cursor = VTabCursor<'vtab>;

    fn connect(
        db: &mut VTabConnection,
        aux: Option<&Self::Aux>,
        args: &[&[u8]]
    ) -> rusqlite::Result<(String, Self)> {
        let conn = String::from_utf8(args[0])
            .map_err(|e| Error::ModuleError(format!("Connection string error: {}", e)))?;
        // let query = String::from_utf8(args[1])
        //    .map_err(|e| Error::ModuleError(format!("Query string error")))?;
        let tbl_name = String::from_utf8(args[1])
            .map_err(|e| Error::ModuleError(format!("Table name error")))?;
        println!("conn: {}; query: {}", conn, query );
        match Client::connect(&conn) {
            Ok(cli) => {
                let query = format!("select * from {};", tbl_name);
                match cli.query(&query) {
                    Ok(rows) => if let Some(r0) = rows.get(0) {
                        let tbl = postgre::build_table_from_postgre(&rows[..])
                            .map_err(|e| Error::ModuleError(format!("{}", e)) )?;
                        let mut create_stmt = format!("create table {}(", tbl_name);
                        let cnames = r0.column_names();
                        for (i, name) in cnames.enumerate() {
                            vals += name;
                            if i < cnames.len() - 1 {
                                create_stmt += ", ";
                            } else {
                                create_stmt += ")";
                            }
                        }
                        let vtab = PGVTab {
                            base: sqlite3_vtab::default(),
                            cli,
                            query,
                            rows
                        };
                        Ok((create_stmt, vtab))
                    } else {
                        Error::ModuleError(format!("No rows returned from query"))
                    },
                    Err(e) => Error::ModuleError(format!("{}", e))
                }
            },
            Err(e) => Error::ModuleError(format!("Postgres connection error: {}", e))
        }
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        Ok(())
    }

    /// Pass the Vec<Row> queries from the connect method.
    fn open(&'vtab self) -> Result<Self::Cursor> {

        Ok(PGVTabCursor{row_id : 0, vtab_ref : &self })
    }

}

/// Cursor will hold the Vec<Row> resulting from a query
#[derive(Default)]
#[repr(C)]
struct PGVTabCursor<'vtab> {
    row_id: i64,
    vtab_ref : &'vtab PGVTab
}

unsafe impl VTabCursor for PGVTabCursor<'_> {

    // Unwinds cursor back to first row
    fn filter(
        &mut self,
        _idx_num: c_int,
        _idx_str: Option<&str>,
        _args: &Values<'_>,
    ) -> Result<()> {
        self.row_id = 0;
        Ok(())
    }

    /// Increment the current row
    fn next(&mut self) -> Result<()> {
        self.row_id += 1;
        Ok(())
    }

    /// Returns false when all rows have been read
    fn eof(&self) -> bool {
        self.row_id < self.vtab_ref.ans.len()
    }

    /// Sets the result given the current column of the virtual table. Use set_result
    /// by dispatching from the given entry of the postgres return set value.
    fn column(&self, ctx: &mut Context, col_ix : c_int) -> Result<()> {
        let col = self.tbl.get_column(col_ix as usize);
        match col.get_integer(self.row_id) {
            Ok(int) => ctx.set_result(int),
            Err(_) => match col.get_real(self.row_id) {
                Ok(real) => ctx.set_result(real),
                Err(_) => match col.get_text(self.row_id) {
                    Some(txt) => ctx.set_result(txt),
                    Err(_) => match col.get_byes(self.row_id) {
                        Some(bytes) => ctx.set_result(bytes),
                        Err(_) => Error::ModuleError(format!("Incompatible type at column {} and row {}", col_ix, self.row_id))
                    }
                }
            }
        }
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.row_id)
    }
}

// planned usage create virtual table patients('https::/pgconn', 'select * from my_tbl');
// Then queries to patients will be forwarded to the query informed at the vtab creation.
pub fn register_pgvtab(conn : &Connection, conn_str : &str, tbl : &str) {

    // Eponymous vtabs can only be used as part of a from SQL clause.
    // let module = vtab::eponymous_only_module::<PGVTab>();

    // Non-eponymous vtabs can be used as create virtual table(...) statements.
    let module = vtab::read_only_module::<PGVTab>();

    // let aux = (conn_str.to_string(), tbl.to_string());
    if let Err(e) = conn.create_module("pgvtab", module, None /*Some(aux)*/) {
        println!("{}", e);
    }

}
