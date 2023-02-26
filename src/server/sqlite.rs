/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use rusqlite;
use std::path::PathBuf;
use super::*;
use crate::tables::column::*;
use crate::tables::nullable::*;
use crate::tables::table::*;
use rusqlite::Row;
use std::fmt::{self, Display};
use crate::sql::{*, object::*, parsing::*};
use rusqlite::types::Value;
use itertools::Itertools;
use std::convert::{TryInto};
use crate::client::ConnectionInfo;
use crate::client::ConnConfig;
use std::error::Error;
use crate::client::ConnURI;

pub struct SqliteConnection {

    path : Option<PathBuf>,

    conn : rusqlite::Connection,

    info : ConnectionInfo

}

const EXTENSION_ERR : &'static str = "Invalid extension for SQLite database\n(expected 'db' or 'sqlite')";

impl SqliteConnection {

    pub fn try_new(uri : ConnURI) -> Result<Self, String> {
        if !uri.uri.as_ref().starts_with("file://") {
            return Err(format!("Invalid database path URI"));
        }
        let path = PathBuf::from(uri.uri.as_ref().trim_start_matches("file://").to_string());
        if let Some(ext) = path.extension() {
            if let Some(ext) = ext.to_str() {
                if !(["sqlite", "sqlite3", "db", "db3"].iter().any(|e| &e[..] == &ext[..] )) {
                    Err(EXTENSION_ERR)?;
                }
            } else {
                Err(EXTENSION_ERR)?;
            }
        } else {
            Err(EXTENSION_ERR)?;
        };

        if path.exists() && path.is_dir() {
            return Err("Path is directory".to_string());
        }
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                return Err("Parent directory does not exist".to_string());
            }
            if !parent.is_dir() {
                return Err("Parent path should be a directory".to_string());
            }
        }
        let res_conn = rusqlite::Connection::open(&path);
        match res_conn {
            Ok(conn) => {
                Ok(Self{
                    path : Some(path),
                    conn,
                    info : uri.info.clone()
                })
            },
            Err(e) => Err(format!("{}", e))
        }
    }

    /*pub fn try_new_local(_content : String) -> Result<Self, String> {
        let conn = rusqlite::Connection::open_in_memory()
            .map_err(|e| format!("{}", e))?;
        // let guard = rusqlite::LoadExtensionGuard::new(&conn)
        //    .map_err(|e| format!("{}", e))?;
        // conn.load_extension(Path::new("csv"), None);
        Ok(Self { conn, path : None })
    }*/

}

impl Connection for SqliteConnection {

    fn configure(&mut self, cfg : ConnConfig) {

    }

    fn listen_at_channel(&mut self, channel : String) {

    }

    fn import(
        &mut self,
        tbl : &mut Table,
        dst : &str,
        cols : &[String],
    ) -> Result<usize, String> {
        let client = &mut self.conn;

        // Auto table creation
        /*if !crate::sql::object::schema_has_table(dst, schema) {
            let create = tbl.sql_table_creation(dst, cols).unwrap();
            println!("{}", create);
            let mut create_stmt = client.prepare(&create).map_err(|e| format!("{}", e) )?;
            create_stmt.execute(rusqlite::NO_PARAMS).map_err(|e| format!("{}", e) )?;
        }*/

        let insert = tbl.sql_table_insertion(dst, cols).map_err(|e| format!("Invalid SQL: {}",e) )?;
        let mut insert_stmt = client.prepare(&insert).map_err(|e| format!("{}", e) )?;
        insert_stmt.execute([]).map_err(|e| format!("{}", e) )?;
        Ok(tbl.shape().0)
    }

    fn query(&mut self, query : &str) -> StatementOutput {
        // let query = substitute_if_required(q, subs);

        match self.conn.prepare(&query[..]) {
            Ok(mut prep_stmt) => {
                let col_names : Vec<String> = prep_stmt.column_names().iter().map(|cn| cn.to_string() ).collect();
                let mut col_tys = Vec::new();
                for col in prep_stmt.columns() {
                    if let Some(ty) = col.decl_type() {
                        col_tys.push(ty.to_string());
                    } else {
                        col_tys.push("unknown".to_string());
                    }
                }
                if col_names.len() != col_tys.len() {
                    return StatementOutput::Invalid("Invalid column set".to_string(), false);
                }
                match prep_stmt.query([]) {
                    Ok(rows) => {
                        match Table::from_sqlite_rows(col_names, &col_tys, rows) {
                            Ok(mut tbl) => {
                                if let Some((name, relation)) = crate::sql::table_name_from_sql(query) {
                                    tbl.set_name(Some(name));
                                    if !relation.is_empty() {
                                        tbl.set_relation(Some(relation));
                                    }
                                }
                                if tbl.names().iter().unique().count() == tbl.names().len() {
                                    StatementOutput::Valid(query.to_string(), tbl)
                                } else {
                                    StatementOutput::Invalid(crate::sql::build_error_with_stmt("Non-unique column names", &query), false)
                                }
                            },
                            Err(e) => {
                                StatementOutput::Invalid(crate::sql::build_error_with_stmt(&e, &query), false)
                            }
                        }
                    },
                    Err(e) => {
                        StatementOutput::Invalid(crate::sql::build_error_with_stmt(&format!("{}", e), &query), true)
                    }
                }
            },
            Err(e) => {
                StatementOutput::Invalid(crate::sql::build_error_with_stmt(&format!("{}", e), &query), true)
            }
        }
    }

    fn exec_transaction(&mut self, _stmt : &AnyStatement) -> StatementOutput {
        StatementOutput::Invalid("Transactions are unsupported in the SQLite backend".to_string(), false)
    }
    
    fn query_async(&mut self, _stmts : &[AnyStatement]) -> Vec<StatementOutput> {
        vec![StatementOutput::Invalid("Asynchronous queries are unsupported in the SQLite backend".to_string(), false)]
    }
    
    fn exec(&mut self, stmt : &AnyStatement, /*subs : &HashMap<String, String>*/) -> StatementOutput {
        let ans = match stmt {
            AnyStatement::Parsed(_, raw) | AnyStatement::ParsedTransaction{ raw, .. } => {
                self.conn.execute(&raw, [])
            },
            AnyStatement::Raw(_, s, _) => self.conn.execute(&s, []),
            AnyStatement::Local(_) => panic!("Tried to execute local statement remotely")
        };
        match ans {
            Ok(n) => crate::sql::build_statement_result(&stmt, n),
            Err(e) => StatementOutput::Invalid(e.to_string(), true)
        }
    }

    fn conn_info(&self) -> ConnectionInfo {
        self.info.clone()
    }

    fn db_info(&mut self) -> Result<DBInfo, Box<dyn Error>> {
        let mut top_objs = Vec::new();
        let names = get_sqlite_tbl_names(self)?;
        for name in names {
            if let Some(obj) = get_sqlite_columns(self, &name) {
                top_objs.push(obj);
            } else {
                Err("Failed to retrieve columns for table {}")?;
            }
        }

        use std::os::unix::fs::MetadataExt;
        let size = if let Ok(sz) = std::fs::metadata(&self.path.as_ref().unwrap()).map(|meta| meta.size() ) {
            if sz < 1_000 {
                format!("{} bytes", sz)
            } else if sz >= 1_000 && sz < 1_000_000 {
                format!("{:.2} kb", sz as f32 / 1.0e3)
            } else if sz >= 1_000_000 && sz < 1_000_000_000 {
                format!("{:.2} mb", sz as f32 / 1.0e6)
            } else {
                format!("{:.2} gb", sz as f32 / 1.0e9)
            }
        } else {
            "Unknown".to_string()
        };
        let details = DBDetails {
            uptime : "Unknown".to_string(),
            server : "SQLite 3".to_string(),
            size,
            locale : "Unknown".to_string()
        };
        Ok(DBInfo { schema : top_objs, details : Some(details) })
    }

}

/*fn attach_functions(conn : &rusqlite::Connection) {
        // generate N ordered real elements from a memory-contiguous
        // byte array decodable as f64 (double precision)
        let create_scalar_ok = conn.create_scalar_function("jdecode", 1, false, move |ctx| {
            if ctx.len() != 1 {
                println!("Function receives single argument");
                return Err(rusqlite::Error::UserFunctionError(
                    DecodingError::new("Function receives single argument")
                ));
            }

            let res_buf = ctx.get::<Vec<u8>>(0);
            match res_buf {
                Ok(buf) => {
                    match decoding::decode_bytes(&buf[..]) {
                        Some(data) => {
                            if data.len() >= 1 {
                                let mut json = String::from("{");
                                //println!("{:?}", data);
                                for (i, d) in data.iter().enumerate() {
                                    json += &format!("{:.8}", d)[..];
                                    if i < data.len()-1 {
                                        json += ","
                                    } else {
                                        json += "}"
                                    }
                                    if i < 10 {
                                        println!("{}", d);
                                    }
                                }
                                Ok(json)
                            } else {
                                println!("Empty buffer");
                                Err(rusqlite::Error::UserFunctionError(
                                    DecodingError::new("Empty buffer")
                                ))
                            }
                        },
                        None => {
                            println!("Could not decode data");
                            Err(rusqlite::Error::UserFunctionError(
                                    DecodingError::new("Could not decode data")
                                ))
                        }
                    }
                },
                Err(e) => {
                    println!("{}", e);
                    Err(rusqlite::Error::UserFunctionError(
                        DecodingError::new("Field is not a blob")
                    ))
                }
            }
        });

        let my_fn = move |_ : Table| { String::from("Hello") };
        let agg = TableAggregate::<String>{
            ans : String::new(),
            f : &my_fn
        };
        let create_agg_ok = conn.create_aggregate_function("multi",
            2,
            false,
            agg
        );
        match create_agg_ok {
            Ok(_) => { },
            Err(e) => { println!("{}", e); }
        }
        match create_scalar_ok {
            Ok(_) => { },
            Err(e) => { println!("{}", e); }
        }
    }

    /*fn load_extension(
        conn : &rusqlite::Connection,
        path : &str
    ) {
        match conn.load_extension(path, None) {
            Ok(_) => { },
            Err(e) => { println!("{}", e); }
        }
    }*/

    /// Given a vector of paths to be loaded,
    fn load_extensions(
        conn : &rusqlite::Connection,
        paths : Vec<String>
    ) {
        for p in paths.iter() {
            Self::load_extension(conn, &p[..]);
        }
    }*/

    /*pub fn try_new_postgre(conn_str : String) -> Result<Self, String> {
        let tls_mode = NoTls{ };
        //println!("{}", conn_str);
        match Client::connect(&conn_str[..], tls_mode) {
            Ok(conn) => Ok(SqlEngine::PostgreSql{
                conn_str,
                conn,
                exec : Arc::new(Mutex::new((Executor::new(), String::new()))) ,
                channel : None
            }),
            Err(e) => {
                let mut e = e.to_string();
                Self::format_pg_string(&mut e);
                Err(e)
            }
        }
    }*/

    /*pub fn remove_sqlite3_udfs(&self, loader : &FunctionLoader, lib_name : &str) {
        match self {
            SqlEngine::Sqlite3{ conn, .. } => {
                for f in loader.fn_list_for_lib(lib_name) {
                    if let Err(e) = conn.remove_function(&f.name, f.args.len() as i32) {
                        println!("{}", e);
                    }
                }
            },
            _ => println!("No UDFs can be registered with the current engine")
        }
    }

    // Since we are handing over control of the function to the C
    // SQLite API, we can't track the lifetime anymore. raw_fn is now
    // assumed to stay alive while the last shared reference to the
    // function loader is alive and the library has not been cleared
    // from the "libs" array of loader. Two things mut happen to guarantee this:
    // (1) The function is always removed when the library is removed, so this branch is
    // not accessed;
    // (2) The function is removed from the Sqlite connection via conn.remove_function(.)
    // any time the library is de-activated.
    // (3) No call to raw_fn must happen outside the TableEnvironment public API,
    // (since TableEnvironment holds an Arc copy to FunctionLoader).
    // Libraries that are not active but are loaded stay on main memory, but will not
    // be registered by this function because load_functions return only active libraries.
    // Perhaps only let the user add/remove/active libraries when there is no connection open
    // for safety.
    fn bind_sqlite3_udfs(conn : &rusqlite::Connection, loader : &FunctionLoader) {
        // println!("Function loader state (New Sqlite3 conn): {:?}", loader);
        match loader.load_functions() {
            Ok(funcs) => {
                for (func, load_func) in funcs {
                    let n_arg = if func.var_arg {
                        -1
                    } else {
                        func.args.len() as i32
                    };
                    let created = match load_func {
                        LoadedFunc::I32(f) => {
                            let raw_fn = unsafe { f.into_raw() };
                            conn.create_scalar_function(
                                &func.name,
                                n_arg,
                                FunctionFlags::empty(),
                                move |ctx| { unsafe{ raw_fn(ctx) } }
                            )
                        },
                        LoadedFunc::F64(f) => {
                            let raw_fn = unsafe { f.into_raw() };
                            conn.create_scalar_function(
                                &func.name,
                                n_arg,
                                FunctionFlags::empty(),
                                move |ctx| { unsafe{ raw_fn(ctx) } }
                            )
                        },
                        LoadedFunc::Text(f) => {
                            let raw_fn = unsafe { f.into_raw() };
                            conn.create_scalar_function(
                                &func.name,
                                n_arg,
                                FunctionFlags::empty(),
                                move |ctx| { unsafe{ raw_fn(ctx) } }
                            )
                        },
                        LoadedFunc::Bytes(f) => {
                            let raw_fn = unsafe { f.into_raw() };
                            conn.create_scalar_function(
                                &func.name,
                                n_arg,
                                FunctionFlags::empty(),
                                move |ctx| { unsafe{ raw_fn(ctx) } }
                            )
                        }
                    };
                    if let Err(e) = created {
                        println!("{:?}", e);
                    } else {
                        println!("User defined function {:?} registered", func);
                    }
                }
            },
            Err(e) => {
                println!("{:?}", e);
            }
        }
    }*/


    /*/// Inserts a table, but only if using in-memory SQLite3 database
    pub fn insert_external_table(&mut self, tbl : &Table) {
        match &self {
            SqlEngine::Sqlite3{path, conn : _} => {
                match &path {
                    None => {
                        if let Ok(q) = tbl.sql_string("transf_table") {
                            // println!("{}", q);
                            if let Err(e) = self.try_run(q, &HashMap::new(), true,/*None*/ ) {
                                println!("{}", e);
                            }
                        } else {
                            println!("Tried to generate SQL for unnamed table");
                        }
                    },
                    Some(_) => {
                        println!("Can only insert tables to in-memory SQLite3 databases");
                    }
                }
            },
            _ => {
                println!("Tried to insert table to Non-sqlite3 database");
            }
        }
    }*/


    /*/// Table is an expesive data structure, so we pass ownership to the function call
    /// because it may be disassembled if the function is found, but we return it back to
    /// the user on an not-found error, since the caller will want to re-use it.
    fn try_client_function(sub : Substitution, tbl : Table, loader : &FunctionLoader) -> StatementOutput {
        match loader.try_exec_fn(sub.func_name, sub.func_args, tbl) {
            Ok(tbl) => StatementOutput::Valid(String::new(), tbl),
            Err(FunctionErr::UserErr(msg)) | Err(FunctionErr::TableAgg(msg)) => {
                StatementOutput::Invalid(msg)
            },
            Err(FunctionErr::TypeMismatch(ix)) => {
                StatementOutput::Invalid(format!("Type mismatch at column {}", ix))
            },
            Err(FunctionErr::NotFound(tbl)) => {
                StatementOutput::Valid(String::new(), tbl)
            }
        }
    }*/

    /// After the statement execution status returned from the SQL engine,
    /// build a message to display to the user.

/// Get all SQLite table names.
/// TODO This will break if there is a table under the temp schema with the same name
/// as a table under the global schema.
fn get_sqlite_tbl_names(conn : &mut SqliteConnection) -> Result<Vec<String>, String> {
    let tbl_query = String::from("SELECT name from sqlite_master WHERE type = 'table' UNION \
        SELECT name from temp.sqlite_master WHERE type = 'table';");
    let ans = conn.query(&tbl_query);
    match ans {
        StatementOutput::Valid(_, names) => {
            let col = names.get_column(0).and_then(|c| {
                let s : Option<Vec<String>> = c.clone().try_into().ok();
                s
            });
            match col {
                Some(s) => Ok(s),
                None => Err("Missing name column".to_string())
            }
        },
        StatementOutput::Invalid(msg, _) => { Err(format!("{}", msg)) },
        _ => Err("Invalid statement output".to_string())
    }
}

fn get_sqlite_columns(conn : &mut SqliteConnection, tbl_name : &str) -> Option<DBObject> {
    let col_query = format!("PRAGMA table_info({});", tbl_name);
    let ans = conn.query(&col_query);
    match ans {
        StatementOutput::Valid(_, col_info) => {
            let names = col_info.get_column(1)
                .and_then(|c| { let s : Option<Vec<String>> = c.clone().try_into().ok(); s })?;
            let col_types = col_info.get_column(2)
                .and_then(|c| match c {
                    Column::Nullable(n) => {
                        let opt_v : Option<Vec<Option<String>>> = n.clone().try_into().ok();
                        match opt_v {
                            Some(v) => {
                                let v_flat = v.iter()
                                    .map(|s| s.clone().unwrap_or(String::new()))
                                    .collect::<Vec<String>>();
                                Some(v_flat)
                            },
                            None => None
                        }
                    },
                    _ => {
                        let s : Option<Vec<String>> = c.clone().try_into().ok();
                        s
                    }
                })?;
            let pks = Vec::new();
            let cols = pack_column_types(names, col_types, pks).ok()?;  

            // TODO pass empty schema. Treat empty schema as non-namespace qualified at query/insert/fncall commands.
            let obj = DBObject::Table{
                schema : format!("public"),
                name : tbl_name.to_string(),
                cols, rels : Vec::new()
            };
            Some(obj)
        },
        StatementOutput::Invalid(_msg, _) => { None },
        _ => None
    }
}

#[derive(Debug, Clone)]
pub enum SqliteColumn {
    I64(Vec<Option<i64>>),
    F64(Vec<Option<f64>>),
    Str(Vec<Option<String>>),
    Bytes(Vec<Option<Vec<u8>>>)
}

impl Display for SqliteColumn {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let t = match self {
            SqliteColumn::I64(_) => "Integer",
            SqliteColumn::F64(_) => "Real",
            SqliteColumn::Str(_) => "String",
            SqliteColumn::Bytes(_) => "Bytes",
        };
        write!(f, "{}", t)
    }
}

impl SqliteColumn {

    pub fn new(decl_type : &str) -> Result<Self, &'static str> {
        match decl_type {
            "integer" | "int" | "INTEGER" | "INT" => Ok(SqliteColumn::I64(Vec::new())),
            "real" | "REAL" => Ok(SqliteColumn::F64(Vec::new())),
            "text" | "TEXT" => Ok(SqliteColumn::Str(Vec::new())),
            "blob" | "BLOB" => Ok(SqliteColumn::Bytes(Vec::new())),

            // Used by the Queries application itself, when column.decl_type()
            // does not have a declared type.
            "unknown" | "Unknown" => Ok(SqliteColumn::Bytes(Vec::new())),

            _ => { Err("Invalid column type") }
        }
    }

    pub fn new_from_first_value(row : &Row, ix : usize) -> Result<Self, &'static str> {
        if let Ok(opt_value) = row.get::<usize, Option<i64>>(ix) {
            return Ok(SqliteColumn::I64(vec![opt_value]));
        } else {
            if let Ok(opt_value) = row.get::<usize, Option<f64>>(ix) {
                return Ok(SqliteColumn::F64(vec![opt_value]));
            } else {
                if let Ok(opt_value) = row.get::<usize, Option<String>>(ix) {
                    return Ok(SqliteColumn::Str(vec![opt_value]));
                } else {
                    if let Ok(opt_value) = row.get::<usize, Option<Vec<u8>>>(ix) {
                        return Ok(SqliteColumn::Bytes(vec![opt_value]));
                    } else {
                        Err("Could not parse value")
                    }
                }
            }
        }
    }

    pub fn append_from_row(&mut self, row : &Row, ix : usize) -> Result<(), &'static str> {
        if let Ok(opt_value) = row.get::<usize, Option<i64>>(ix) {
            if let SqliteColumn::I64(ref mut v) = self {
                v.push(opt_value);
                return Ok(());
            }
        } else {
             if let Ok(opt_value) = row.get::<usize, Option<f64>>(ix) {
                if let SqliteColumn::F64(ref mut v) = self {
                    v.push(opt_value);
                    return Ok(());
                }
             } else {
                if let Ok(opt_value) = row.get::<usize, Option<String>>(ix) {
                    if let SqliteColumn::Str(ref mut v) = self {
                        v.push(opt_value);
                        return Ok(());
                    }
                } else {
                    if let Ok(opt_value) = row.get::<usize, Option<Vec<u8>>>(ix) {
                        if let SqliteColumn::Bytes(ref mut v) = self {
                            v.push(opt_value);
                            return Ok(());
                        }
                    }
                }
             }
        }
        Err("Unable to parse value")
    }

    fn try_append(&mut self, value : Value) -> Result<(), &'static str> {
        match self {
            Self::I64(ref mut v) => {
                match value {
                    Value::Integer(i) => v.push(Some(i)),
                    Value::Null => v.push(None),
                    _ => {
                        return Err("Invalid type");
                    }
                }
            },
            Self::F64(ref mut v) => {
                match value {
                    Value::Real(r) => v.push(Some(r)),
                    Value::Null => v.push(None),
                    _ => {
                        return Err("Invalid type");
                    }
                }
            },
            Self::Str(ref mut v) => {
                match value {
                    Value::Text(t) => v.push(Some(t)),
                    Value::Null => v.push(None),
                    _ => {
                        return Err("Invalid type");
                    }
                }
            },
            Self::Bytes(ref mut v) => {
                match value {
                    Value::Blob(b) => v.push(Some(b)),
                    Value::Null => v.push(None),
                    _ => {
                        return Err("Invalid type");
                    }
                }
            }
        }
        Ok(())
    }

}

impl From<SqliteColumn> for NullableColumn
    where
        NullableColumn : From<Vec<Option<i64>>>,
        NullableColumn : From<Vec<Option<f64>>>,
        NullableColumn : From<Vec<Option<String>>>,
        NullableColumn : From<Vec<Option<Vec<u8>>>>,
{
    fn from(col: SqliteColumn) -> Self {
        match col {
            SqliteColumn::I64(v) => v.into(),
            SqliteColumn::F64(v) => v.into(),
            SqliteColumn::Str(v) => v.into(),
            SqliteColumn::Bytes(v) => v.into()
        }
    }
}

pub fn copy_table_to_sqlite(
    client : &mut rusqlite::Connection,
    tbl : &mut Table,
    dst : &str,
    cols : &[String],
    schema : &[DBObject]
) -> Result<(), String> {

    // TODO filter cols

    if !crate::sql::object::schema_has_table(dst, schema) {
        let create = tbl.sql_table_creation(dst, cols).ok_or(String::from("Invalid SQL"))?;
        let mut create_stmt = client.prepare(&create).map_err(|e| format!("{}", e) )?;
        create_stmt.execute([]).map_err(|e| format!("{}", e) )?;
    }

    let insert = tbl.sql_table_insertion(dst, cols)?;
    let mut insert_stmt = client.prepare(&insert).map_err(|e| format!("{}", e) )?;
    insert_stmt.execute([]).map_err(|e| format!("{}", e) )?;
    Ok(())
}

/*mod functions {

    use rusqlite::{self, ToSql};
    use rusqlite::functions::{Aggregate, Context};
    use std::panic::{RefUnwindSafe, UnwindSafe};

    pub struct ToSqlAgg<T,F>
    where
        T : ToSql,
        F : ToSql
    {
        data : T,

        init_func : Box<dyn Fn()->T>,

        /// This function can be read as a dynamic external symbol
        state_func : Box<dyn Fn(T)->T>,

        /// This function also can be read as a dynamic external symbol
        final_func : Box<dyn Fn(T)->F>
    }

    impl<T, F> Aggregate<T, F> for ToSqlAgg<T, F>
    where
        T : ToSql + RefUnwindSafe + UnwindSafe,
        F : ToSql + RefUnwindSafe + UnwindSafe
    {
        fn init(&self) -> T {
            unimplemented!()
        }

        fn step(&self, ctx : &mut Context, t : &mut T) ->rusqlite::Result<()> {
            unimplemented!()
        }

        fn finalize(&self, t : Option<T>) -> rusqlite::Result<F> {
            unimplemented!()
        }

    }

}*/

/*pub fn backup_if_sqlite(conn : &mut SqliteConnection, path : PathBuf) {
    if let Err(e) = conn.conn.backup(rusqlite::DatabaseName::Main, path, None) {
        println!("{}", e);
    }
}*/

/*
pub fn remove_udfs(&self, lib_name : &str) {
        if let (Ok(engine), Ok(loader)) = (self.listener.engine.lock(), self.loader.lock()) {
            engine.remove_sqlite3_udfs(&loader, lib_name);
        } else {
            println!("Failed acquiring lock over sql engine or function loader to remove UDFs");
        }
    }
*/

