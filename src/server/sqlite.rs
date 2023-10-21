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
use crate::ui::apply::Modules;
use rusqlite::functions::{FunctionFlags, Aggregate, Context};
use std::sync::Mutex;
use std::sync::Arc;
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::sync::atomic::AtomicU64;
use rusqlite::OpenFlags;

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
        let res_conn = if uri.info.readonly == Some(true) {
            let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI | OpenFlags::SQLITE_OPEN_NO_MUTEX;
            rusqlite::Connection::open_with_flags(&path, flags)
        } else {
            rusqlite::Connection::open(&path)
        };
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

    fn bind_functions(&self, modules : &crate::ui::apply::Modules) {
        attach_functions(&self.conn, modules);
    }

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

        let insert = tbl.sql_table_insertion(dst, cols, false)
            .map_err(|e| format!("Invalid SQL: {}",e) )?
            .ok_or(String::from("Empty table"))?;
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
            uptime : "N/A".to_string(),
            server : "SQLite 3".to_string(),
            size,
            locale : "N/A".to_string()
        };
        Ok(DBInfo { schema : top_objs, details : Some(details) })
    }

}

type BoxedAgg = Box<dyn Fn(serde_json::Value)->Result<serde_json::Value, Box<dyn std::error::Error>> + Send + 'static>;

pub struct JsonAgg(BoxedAgg);

impl Aggregate<Vec<Vec<serde_json::Value>>, serde_json::Value> for JsonAgg {

    fn init(&self, ctx : &mut Context<'_>) -> rusqlite::Result<Vec<Vec<serde_json::Value>>> {
        let mut vals = Vec::new();
        for i in 0..ctx.len() {
            vals.push(Vec::new());
        }
        Ok(vals)
    }

    fn step(&self, ctx: &mut Context<'_>, state : &mut Vec<Vec<serde_json::Value>>) -> rusqlite::Result<()> {
        for i in 0..ctx.len() {
            if let Ok(s) = ctx.get::<String>(i) {
                state[i].push(serde_json::Value::String(s));
            } else if let Ok(int) = ctx.get::<i64>(i) {
                state[i].push(serde_json::Value::from(int));
            } else if let Ok(f) = ctx.get::<f64>(i) {
                state[i].push(serde_json::Value::from(f));
            } else if let Ok(b) = ctx.get::<bool>(i) {
                state[i].push(serde_json::Value::from(b));
            } else {
                return Err(rusqlite::Error::UserFunctionError(format!("Invalid type").into()));
            }
        }
        Ok(())
    }

    fn finalize(&self, _: &mut Context<'_>, mut state: Option<Vec<Vec<serde_json::Value>>>) -> rusqlite::Result<serde_json::Value> {
        let val = if let Some(mut state) = state {
           serde_json::Value::Array(state.drain(..).map(|s| serde_json::Value::Array(s) ).collect())
        } else {
            serde_json::Value::Null
        };
        (self.0)(val).map_err(|e| rusqlite::Error::UserFunctionError(format!("{}",e).into()) )
    }

}

fn process_request(
    this_mod : &mut crate::ui::apply::Module,
    func_name : &str,
    val : &serde_json::Value
) -> Result<serde_json::Value, String> {
    let input = serde_json::to_string(&val).map_err(|e| format!("{}",e) )?;
    let bytes = this_mod.plugin.call(&func_name, &input[..]).map_err(|e| format!("{}",e) )?;
    Ok(serde_json::from_reader::<_, serde_json::Value>(bytes).map_err(|e| format!("{}",e) )?)
}

fn attach_functions(conn : &rusqlite::Connection, mods : &Modules) {
    use gtk4::glib;

    /* We need to process the data at the main thread, because the Plugin held
    by the module is neither Send nor UnwindSafe, which are requirements for
    the closures bound by sqlite (and the database operations live in another thread).
    The solution is to process the data in the glib main loop, blocking the function call
    briefly until the result is ready. An improved solution would be to re-load the modules
    at the database thread. But in this case each function would have its plugin instance,
    since Plugin cannot be Send. */
    let (tx, rx) = glib::MainContext::channel::<(u64, String, String, serde_json::Value)>(glib::source::Priority::DEFAULT);
    let (ans_tx, ans_rx) = crossbeam::channel::unbounded::<Result<(u64, String, String, serde_json::Value), String>>();

    rx.attach(None, {
        let mods = mods.clone();
        move |(call_id, mod_name, func_name, val)| {
            let mut mods = mods.borrow_mut();
            let this_mod = mods.get_mut(&mod_name).unwrap();
            let res = process_request(this_mod, &func_name, &val);
            ans_tx.send(res.map(|res| (call_id, mod_name, func_name, res)));
            glib::ControlFlow::Continue
        }
    });
    let ans_rx = Arc::new(ans_rx);
    let mods = mods.clone();
    let call_id = Arc::new(AtomicU64::new(0));
    for (mod_name, module) in mods.borrow().iter() {
        for f in &module.funcs {
            let call_id = call_id.clone();
            let func_name = f.name.clone();
            let mod_name = mod_name.clone();
            let tx = tx.clone();
            let ans_rx = ans_rx.clone();
            if f.aggregate {
                let agg = JsonAgg(Box::new(move |v| {
                    call_id.fetch_add(1, Ordering::Relaxed);
                    let id = call_id.load(Ordering::Relaxed);
                    tx.send((id, mod_name.clone(), func_name.clone(), v));
                    match ans_rx.recv() {
                        Ok(Ok(ans)) => {
                            if ans.0 == id && &ans.1[..] == &mod_name[..] && &ans.2[..] == &func_name[..] {
                                Ok(ans.3)
                            } else {
                                Err(Box::new(rusqlite::Error::UserFunctionError("Synchronization error".into())))
                            }
                        },
                        Ok(Err(e)) => {
                            Err(Box::new(rusqlite::Error::UserFunctionError(e.into())))
                        },
                        Err(_) => {
                            Err(Box::new(rusqlite::Error::UserFunctionError("Disconnected".into())))
                        }
                    }
                }));
                if let Err(e) = conn.create_aggregate_function(&f.name, -1, FunctionFlags::empty(), agg) {
                    println!("{}",e);
                }
            } else {
                if let Err(e) = conn.create_scalar_function(&f.name, -1, FunctionFlags::empty(), move |ctx| {
                    call_id.fetch_add(1, Ordering::Relaxed);
                    let id = call_id.load(Ordering::Relaxed);
                    let mut args = Vec::new();
                    for i in 0..ctx.len() {
                        if let Ok(s) = ctx.get::<String>(i) {
                            args.push(serde_json::Value::String(s));
                        } else if let Ok(int) = ctx.get::<i64>(i) {
                            args.push(serde_json::Value::from(int));
                        } else if let Ok(f) = ctx.get::<f64>(i) {
                            args.push(serde_json::Value::from(f));
                        } else if let Ok(b) = ctx.get::<bool>(i) {
                            args.push(serde_json::Value::from(b));
                        } else {
                            return Err(rusqlite::Error::UserFunctionError(format!("Invalid type").into()));
                        }
                    }
                    tx.send((id, mod_name.clone(), func_name.clone(), serde_json::Value::Array(args)));
                    match ans_rx.recv() {
                        Ok(Ok(ans)) => {
                            if ans.0 == id && &ans.1[..] == &mod_name[..] && &ans.2[..] == &func_name[..] {
                                Ok(ans.3)
                            } else {
                                Err(rusqlite::Error::UserFunctionError("Synchronization error".into()))
                            }
                        },
                        Ok(Err(e)) => {
                            Err(rusqlite::Error::UserFunctionError(e.into()))
                        },
                        Err(_) => {
                            Err(rusqlite::Error::UserFunctionError("Disconnected".into()))
                        }
                    }
                }) {
                    println!("{}",e);
                }
            }
        }
    }
}

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
                schema : String::new(),
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

    let insert = tbl.sql_table_insertion(dst, cols, false)?.ok_or(String::from("Empty table"))?;
    let mut insert_stmt = client.prepare(&insert).map_err(|e| format!("{}", e) )?;
    insert_stmt.execute([]).map_err(|e| format!("{}", e) )?;
    Ok(())
}

