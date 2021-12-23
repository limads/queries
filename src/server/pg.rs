use postgres;
use crate::sql::{*, object::*};
use rust_decimal::Decimal;
use crate::tables::column::*;
use crate::tables::nullable_column::*;
use crate::tables::table::*;
use postgres::types::Type;
use std::io::Write;
use std::error::Error;
use crate::tables::table::{self, Table, Align, Format, TableSettings, BoolField, NullField};
// use crate::utils;
use serde_json::Value;
use crate::sql::object::{DBObject, DBType, DBInfo};
use crate::sql::parsing::AnyStatement;
use crate::sql::copy::*;
use super::Connection;
use std::sync::{Arc, Mutex};
use crate::command::Executor;
use std::collections::HashMap;
use std::fs::File;
use std::ffi::OsStr;
use postgres::types::{FromSql, ToSql};
use std::convert::{TryFrom, TryInto};
use std::str::FromStr;
use std::io::Read;
use std::mem;
use postgres::NoTls;
use postgres::Client;
use itertools::Itertools;
use std::path::Path;
use crate::tables::field::Field;

pub struct PostgresConnection {

    conn_str : String,

    conn : postgres::Client,

    exec : Arc<Mutex<(Executor, String)>>,

    channel : Option<(String, String, bool)>

}

impl PostgresConnection {

    pub fn try_new(conn_str : String) -> Result<Self, String> {
        let tls_mode = NoTls{ };
        //println!("{}", conn_str);
        match Client::connect(&conn_str[..], tls_mode) {
            Ok(conn) => Ok(Self{
                conn_str,
                conn,
                exec : Arc::new(Mutex::new((Executor::new(), String::new()))) ,
                channel : None
            }),
            Err(e) => {
                let mut e = e.to_string();
                format_pg_string(&mut e);
                Err(e)
            }
        }
    }

}

impl Connection for PostgresConnection {

    fn listen_at_channel(&mut self, channel : String) {

    }

    fn query(&mut self, q : &str, subs : &HashMap<String, String>) -> QueryResult {
        let query = substitute_if_required(q, subs);

        // println!("Final query: {}", query);
        // println!("Executing: {}", query);
        match self.conn.query(&query[..], &[]) {
            Ok(rows) => {
                match build_table_from_postgre(&rows[..]) {
                    Ok(mut tbl) => {
                        if let Some((name, relation)) = crate::sql::table_name_from_sql(q) {
                            tbl.set_name(Some(name));
                            if !relation.is_empty() {
                                tbl.set_relation(Some(relation));
                            }
                        }
                        if tbl.names().iter().unique().count() == tbl.names().len() {
                            QueryResult::Valid(q.to_string(), tbl)
                        } else {
                            QueryResult::Invalid(crate::sql::build_error_with_stmt("Non-unique column names", &query[..]), false)
                        }
                    },
                    Err(e) => QueryResult::Invalid(crate::sql::build_error_with_stmt(&e, &query[..]), false)
                }
            },
            Err(e) => {
                let mut e = e.to_string();
                format_pg_string(&mut e);
                QueryResult::Invalid(e, true)
            }
        }
    }

    fn exec(&mut self, stmt : &AnyStatement, subs : &HashMap<String, String>) -> QueryResult {
        // let final_stmt = substitute_if_required(&s, subs);
        let ans = match stmt {
            AnyStatement::Parsed(stmt, s) => {
                let s = format!("{}", stmt);
                let final_statement = substitute_if_required(&s, subs);
                // println!("Final statement: {}", final_statement);
                self.conn.execute(&final_statement[..], &[])
            },
            AnyStatement::Raw(_, s, _) => {
                let final_statement = substitute_if_required(&s, subs);
                self.conn.execute(&final_statement[..], &[])
            },
            AnyStatement::Local(_) => {
                panic!("Tried to execute local statement remotely")
            }
        };
        match ans {
            Ok(n) => crate::sql::build_statement_result(&stmt, n as usize),
            Err(e) => {
                let mut e = e.to_string();
                format_pg_string(&mut e);
                QueryResult::Invalid(e, true)
            }
        }
    }

    fn info(&mut self) -> Option<DBInfo> {
        let mut top_objs = Vec::new();
        if let Some(schemata) = get_postgre_schemata(self) {
            println!("Obtained schemata: {:?}", schemata);
            for (schema, tbls) in schemata.iter() {
                let mut tbl_objs = Vec::new();
                for t in tbls.iter() {
                    if let Some(tbl) = get_postgre_columns(self, &schema[..], &t[..]) {
                        tbl_objs.push(tbl);
                    } else {
                        println!("Failed getting columns for {}.{}", schema, t);
                        return None;
                    }
                }

                let func_objs = get_postgres_functions(self, &schema[..]).unwrap_or(Vec::new());
                let view_objs = get_postgres_views(self, &schema[..]).unwrap_or(Vec::new());
                let mut children = tbl_objs;
                if view_objs.len() > 0 {
                    children.push(DBObject::Schema { name : format!("views"), children : view_objs } );
                }
                if func_objs.len() > 0 {
                    children.push(DBObject::Schema { name : format!("functions"), children : func_objs } );
                }

                let obj = DBObject::Schema{ name : schema.to_string(), children };
                top_objs.push(obj);
            }

            // Also include catalog functions?
            // let catalog_funcs = self.get_postgres_functions("pg_catalog").unwrap_or(Vec::new());
            // top_objs.push(DBObject::Schema { name : format!("catalog"), children : catalog_funcs });

            Some(DBInfo { schema : top_objs, ..Default::default() })

        } else {
            println!("Failed retrieving database schemata");
            let mut empty = Vec::new();
            empty.push(DBObject::Schema{ name : "public".to_string(), children : Vec::new() });
            Some(DBInfo { schema : empty, ..Default::default() })
            // None
        }
    }

}

/*fn get_postgre_extensions(&mut self) {
    // First, check all available extensions.
    let ext_query = "select extname::text, extversion from pg_available_extensions;";

    // Then, add (installed) tag to those that also appear here:
    let used_query = "select extname::text from pg_extension;";
}*/

// fn get_postgre_roles(&mut self) {
    // let role_query = "select rolinherit, rolcanlogin, rolsuper, from pg_catalog.pg_roles";
// }

// pg_proc.prokind codes: f = function; p = procedure; a = aggregate; w = window
// To retrieve source: case when pg_language.lanname = 'internal' then pg_proc.prosrc else pg_get_functiondef(pg_proc.oid) end as source
// -- pg_language.lanname not like 'internal' and
// TODO arguments are not correctly ordered.
// Alternative query:
// select cast(cast(pg_proc.oid as regprocedure) as text) from pg_proc left join pg_namespace on
// pg_proc.pronamespace = pg_namespace.oid where pg_namespace.nspname like 'public';
// Then parse the resulting text (but it won't give the return type).
fn get_postgres_functions(conn : &mut PostgresConnection, schema : &str) -> Option<Vec<DBObject>> {
    let fn_query = format!(r#"
    with arguments as (
        with arg_types as (select pg_proc.oid as proc_oid,
            unnest(proargtypes) as arg_oid
            from pg_catalog.pg_proc
        ) select arg_types.proc_oid as proc_id,
            array_agg(cast(typname as text)) as arg_typename
            from pg_catalog.pg_type inner join arg_types on pg_type.oid = arg_types.arg_oid
            group by arg_types.proc_oid
            order by arg_types.proc_oid
    ) select pg_proc.oid,
        pg_proc.prokind,
        proname::text,
        arguments.arg_typename,
        cast(typname as text) as ret_typename,
        pg_language.lanname as lang,
        pg_namespace.nspname
    from pg_catalog.pg_proc left join pg_catalog.pg_type on pg_proc.prorettype = pg_type.oid
        left join arguments on pg_proc.oid = arguments.proc_id
        left join pg_language on pg_proc.prolang = pg_language.oid
        left join pg_namespace on pg_proc.pronamespace = pg_namespace.oid
    where
        pg_namespace.nspname like '{}' and
        pg_proc.proname not like 'ts_debug' and
        pg_proc.proname not like '_pg%'
    order by pg_proc.oid;"#, schema);

    let ans = conn.try_run(fn_query, &HashMap::new(), false).map_err(|e| println!("{}", e) ).ok()?;
    match ans.get(0)? {
        QueryResult::Valid(_, fn_info) => {
            let mut fns = Vec::new();
            let names = Vec::<String>::try_from(fn_info.get_column(2).unwrap().clone()).ok()?;
            let args = (0..names.len()).map(|ix| fn_info.get_column(3).unwrap().at(ix) ).collect::<Vec<_>>();
            println!("Retrieved args = {:?}", args);
            let ret = Vec::<String>::try_from(fn_info.get_column(4).unwrap().clone()).ok()?;
            for (name, (arg_vals, ret)) in names.iter().zip(args.iter().zip(ret.iter())) {
                let args = match arg_vals {
                    Some(Field::Json(serde_json::Value::Array(arg_names))) => {
                        arg_names.iter().map(|arg| match arg {
                            serde_json::Value::String(s) => DBType::from_str(&s[..]).unwrap_or(DBType::Unknown),
                            _ => DBType::Unknown
                        }).collect()
                    },
                    _ => {
                        Vec::new()
                    }
                };
                let ret = DBType::from_str(ret).unwrap_or(DBType::Unknown);
                fns.push(DBObject::Function { name : name.clone(), args, ret });
            }
            Some(fns)
        },
        QueryResult::Invalid(msg, _) => { println!("{}", msg); None },
        _ => None
    }
}

/// Return HashMap of Schema->Tables
fn get_postgre_schemata(conn : &mut PostgresConnection) -> Option<HashMap<String, Vec<String>>> {
    let tbl_query = String::from("select schemaname::text, tablename::text \
        from pg_catalog.pg_tables \
        where schemaname != 'pg_catalog' and schemaname != 'information_schema';");
    let ans = conn.try_run(tbl_query, &HashMap::new(), false)
        .map_err(|e| println!("{}", e) ).ok()?;
    let q_res = ans.get(0)?;
    match q_res {
        QueryResult::Valid(_, table) => {
            if table.shape().0 == 0 {
                let mut empty = HashMap::new();
                empty.insert(String::from("public"), Vec::new());
                return Some(empty);
            }
            let schemata = table.get_column(0).and_then(|c| {
                let s : Option<Vec<String>> = c.clone().try_into().ok();
                s
            });
            let names = table.get_column(1).and_then(|c| {
                let s : Option<Vec<String>> = c.clone().try_into().ok();
                s
            });
            if let Some(schemata) = schemata {
                if let Some(names) = names {
                    let mut schem_hash = HashMap::new();
                    for (schema, table) in schemata.iter().zip(names.iter()) {
                        let tables = schem_hash.entry(schema.clone()).or_insert(Vec::new());
                        tables.push(table.clone());
                    }
                    Some(schem_hash)
                } else {
                    println!("Could not load table names to String vector");
                    None
                }
            } else {
                println!("Could not load schema column to String vector");
                None
            }
        },
        QueryResult::Invalid(msg, _) => { println!("{}", msg); None },
        _ => None
    }
}

fn get_postgres_views(conn : &mut PostgresConnection, schema : &str) -> Option<Vec<DBObject>> {
    let view_query = format!(r#"
    select cast(table_schema as text) as schema_name,
           cast(table_name as text) as view_name
    from information_schema.views
    where table_schema like '{}' and table_schema not in ('information_schema', 'pg_catalog')
    order by schema_name, view_name;"#, schema);
    let ans = conn.try_run(view_query, &HashMap::new(), false).map_err(|e| println!("{}", e) ).ok()?;
    match ans.get(0)? {
        QueryResult::Valid(_, view_info) => {
            let mut views = Vec::new();
            let info = Vec::<String>::try_from(view_info.get_column(1).unwrap().clone()).ok()?;
            for name in info.iter() {
                // let name = row.get::<String>(1);
                views.push(DBObject::View { name : name.clone() });
            }
            Some(views)
        },
        QueryResult::Invalid(msg, _) => { println!("{}", msg); None },
        _ => None
    }
}

fn get_postgres_pks(conn : &mut PostgresConnection, schema_name : &str, tbl_name : &str) -> Option<Vec<String>> {
    let pk_query = format!("select
            cast(tc.table_schema as text) as table_schema,
            cast(tc.constraint_name as text) as constraint_name,
            cast(tc.table_name as text) as table_name,
            cast(kcu.column_name as text) as column_name
        FROM
            information_schema.table_constraints as tc
            join information_schema.key_column_usage as kcu
              on tc.constraint_name = kcu.constraint_name
              and tc.table_schema = kcu.table_schema
            join information_schema.constraint_column_usage AS ccu
              on ccu.constraint_name = tc.constraint_name
              and ccu.table_schema = tc.table_schema
        where tc.constraint_type = 'PRIMARY KEY' and tc.table_name='{}' and tc.table_schema='{}';",
        tbl_name,
        schema_name
    );
    let ans = conn.try_run(pk_query, &HashMap::new(), false).map_err(|e| println!("{}", e) ).ok()?;
    if let Some(q_res) = ans.get(0) {
        match q_res {
            QueryResult::Valid(_, col_info) => {
                let cols = col_info.get_column(3)
                    .and_then(|c| { let s : Option<Vec<String>> = c.clone().try_into().ok(); s })?;
                Some(cols)
            },
            QueryResult::Invalid(msg, _) => { println!("{}", msg); None },
            _ => None
        }
    } else {
        println!("Database info query did not return any results");
        None
    }
}

/// Get foreign key relations for a given table.
fn get_postgres_relations(conn : &mut PostgresConnection, schema_name : &str, tbl_name : &str) -> Option<Vec<Relation>> {
    let rel_query = format!("select
            cast(tc.table_schema as text) as table_schema,
            cast(tc.constraint_name as text) as constraint_name,
            cast(tc.table_name as text) as table_name,
            cast(kcu.column_name as text) as column_name,
            cast(ccu.table_schema  as text) as foreign_table_schema,
            cast(ccu.table_name  as text) as foreign_table_name,
            cast(ccu.column_name  as text) as foreign_column_name
        FROM
            information_schema.table_constraints as tc
            join information_schema.key_column_usage as kcu
              on tc.constraint_name = kcu.constraint_name
              and tc.table_schema = kcu.table_schema
            join information_schema.constraint_column_usage AS ccu
              on ccu.constraint_name = tc.constraint_name
              and ccu.table_schema = tc.table_schema
        where tc.constraint_type = 'FOREIGN KEY' and tc.table_name='{}' and tc.table_schema='{}';",
        tbl_name,
        schema_name
    );
    let ans = conn.try_run(rel_query, &HashMap::new(), false).map_err(|e| println!("{}", e) ).ok()?;
    if let Some(q_res) = ans.get(0) {
        match q_res {
            QueryResult::Valid(_, col_info) => {
                let tgt_schemas = col_info.get_column(4)
                    .and_then(|c| { let s : Option<Vec<String>> = c.clone().try_into().ok(); s })?;
                let tgt_tbls = col_info.get_column(5)
                    .and_then(|c| { let s : Option<Vec<String>> = c.clone().try_into().ok(); s })?;
                let src_cols = col_info.get_column(3)
                    .and_then(|c| { let s : Option<Vec<String>> = c.clone().try_into().ok(); s })?;
                let tgt_cols = col_info.get_column(6)
                    .and_then(|c| { let s : Option<Vec<String>> = c.clone().try_into().ok(); s })?;
                let mut rels = Vec::new();
                for i in 0..tgt_schemas.len() {
                    rels.push(Relation{
                        tgt_schema : tgt_schemas[i].clone(),
                        tgt_tbl : tgt_tbls[i].clone(),
                        src_col : src_cols[i].clone(),
                        tgt_col : tgt_cols[i].clone()
                    });
                }
                println!("Relation vector: {:?}", rels);
                Some(rels)
            },
            QueryResult::Invalid(msg, _) => { println!("{}", msg); None },
            _ => None
        }
    } else {
        println!("Database info query did not return any results");
        None
    }
}

fn get_postgre_columns(conn : &mut PostgresConnection, schema_name : &str, tbl_name : &str) -> Option<DBObject> {
    let col_query = format!("select column_name::text, data_type::text \
        from information_schema.columns where table_name = '{}' and table_schema='{}';", tbl_name, schema_name);
    let ans = conn.try_run(col_query, &HashMap::new(), false)
        .map_err(|e| println!("{}", e) ).ok()?;
    if let Some(q_res) = ans.get(0) {
        match q_res {
            QueryResult::Valid(_, col_info) => {
                let names = col_info.get_column(0)
                    .and_then(|c| { let s : Option<Vec<String>> = c.clone().try_into().ok(); s })?;
                let col_types = col_info.get_column(1)
                    .and_then(|c| { let s : Option<Vec<String>> = c.clone().try_into().ok(); s })?;
                let pks = get_postgres_pks(conn, schema_name, tbl_name).unwrap_or(Vec::new());
                let cols = crate::sql::pack_column_types(names, col_types, pks)?;
                let rels = get_postgres_relations(conn, schema_name, tbl_name).unwrap_or(Vec::new());

                let obj = DBObject::Table{ name : tbl_name.to_string(), cols, rels };
                Some(obj)
            },
            QueryResult::Invalid(msg, _) => { println!("{}", msg); None },
            _ => None
        }
    } else {
        println!("Database info query did not return any results");
        None
    }
}

/// Copies from the PostgreSQL server into a client
fn copy_pg_to(client : &mut postgres::Client, action : &Copy) -> Result<String, String> {
    let copy_call = action.to_string();
    // println!("Transformed copy call: {}", copy_call);
    let mut reader = client.copy_out(&copy_call[..])
        .map_err(|e| format!("{}", e) )?;
    let mut data = String::new();
    reader.read_to_string(&mut data).map_err(|e| format!("{}", e))?;
    Ok(data)
}

/// Copies from a client into the PostgreSQL server
fn copy_pg_from(client : &mut postgres::Client, action : &Copy, data : &str) -> Result<u64, String> {
    let copy_call = action.to_string();
    // println!("Transformed copy call: {}", copy_call);
    let mut writer = client.copy_in(&copy_call[..])
        .map_err(|e| format!("{}", e) )?;
    writer.write_all(data.as_bytes()).map_err(|e| format!("{}", e))?;
    let n = writer.finish().map_err(|e| format!("{}", e) )?;
    Ok(n)
}

pub fn copy(
    conn : &mut postgres::Client,
    action : &Copy,
    exec : &Arc<Mutex<(Executor, String)>>
) -> Result<u64, String> {
    // println!("Executing copy {:?}", action);
    match action.target {
        CopyTarget::From => {
            let csv_input = match &action.client {
                CopyClient::Stdio => {
                    let mut executor = exec.lock().map_err(|e| format!("{}", e))?;
                    if executor.1.len() > 0 {
                        mem::take(&mut executor.1)
                    } else {
                        return Err(format!("No data cached in stdin"));
                    }
                },
                CopyClient::File(path) => {
                    let mut f = File::open(path).map_err(|e| format!("{}", e))?;
                    let mut content = String::new();
                    f.read_to_string(&mut content).map_err(|e| format!("{}", e))?;
                    if content.len() == 0 {
                        return Err(format!("File is empty"));
                    }
                    content
                },
                CopyClient::Program(call) => {
                    wait_command_execution(&call, &exec)?
                },
                CopyClient::Variable(v) => {
                    let mut executor = exec.lock().map_err(|e| format!("{}", e))?;
                    if let Some(s) = executor.0.get_var(&v) {
                        if s.is_empty() {
                            return Err(format!("Variable {} does not have any content", v));
                        } else {
                            s
                        }
                    } else {
                        return Err(format!("Variable {} not declared", v));
                    }
                }
            };
            copy_pg_from(conn, &action, &csv_input)
        },
        CopyTarget::To => {
            let csv_out = copy_pg_to(conn, &action)?;
            println!("Received data from copy: {}", csv_out);
            if csv_out.len() == 0 {
                return Err(format!("'COPY TO' returned no data"));
            }
            match &action.client {
                CopyClient::Stdio => {
                    let mut executor = exec.lock().map_err(|e| format!("{}", e))?;
                    if executor.1.len() > 0 {
                        // println!("Clearing previous data cache");
                        executor.1.clear();
                    }
                    executor.1 = csv_out.clone();
                },
                CopyClient::File(path) => {
                    if Path::new(path).extension() != Some(OsStr::new("csv")) {
                        return Err(format!("Path must point to csv file"));
                    }
                    let mut f = File::create(path).map_err(|e| format!("{}", e))?;
                    f.write_all(csv_out.as_bytes()).map_err(|e| format!("{}", e))?;
                },
                CopyClient::Program(p) => {
                    let mut cmd_out = String::new();
                    let mut executor = exec.lock().map_err(|e| format!("{}", e))?;
                    executor.0.queue_command(p.clone(), Some(csv_out.clone()));
                    executor.0.on_command_result(|out| {
                        if out.status {
                            if out.txt.len() > 0 {
                                cmd_out = out.txt;
                            }
                            Ok(())
                        } else {
                            Err(format!("Command execution failed: {}", out.txt))
                        }
                    })?;
                    if cmd_out.len() > 0 {
                        if executor.1.len() > 0 {
                            // println!("Clearing previous data cache");
                            executor.1.clear();
                        }
                        executor.1 = cmd_out;
                    }
                },
                CopyClient::Variable(v) => {
                    let executor = exec.lock().map_err(|e| format!("{}", e))?;
                    if executor.0.has_var(&v) {
                        executor.0.set_var(v, csv_out);
                    } else {
                        return Err(format!("Variable {} not declared", v));
                    }
                }
            }
            Ok(0)
        }
    }
}

pub fn col_as_vec<'a, T>(
    rows : &'a [postgres::row::Row],
    ix : usize
) -> Result<Vec<T>, &'static str>
    where
        T : FromSql<'a> + ToSql + Sync,
{
    let mut data = Vec::new();
    for r in rows.iter() {
        let datum = r.try_get::<usize, T>(ix)
            .map_err(|e| { println!("{}", e); "Unable to parse column" })?;
        data.push(datum);
    }
    Ok(data)
}

pub fn col_as_opt_vec<'a, T>(
    rows : &'a [postgres::row::Row],
    ix : usize
) -> Result<Vec<Option<T>>, &'static str>
    where
        T : FromSql<'a> + ToSql + Sync,
{
    let mut opt_data = Vec::new();
    for r in rows.iter() {
        let opt_datum = r.try_get::<usize, Option<T>>(ix)
            .map_err(|e| { println!("{}", e); "Unable to parse column" })?;
        opt_data.push(opt_datum);
    }
    Ok(opt_data)
}

pub fn nullable_from_rows<'a, T>(
    rows : &'a [postgres::row::Row],
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
    rows : &'a [postgres::row::Row],
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

/*pub fn try_any_integer(rows : &[postgres::row::Row], ix : usize) -> Result<NullableColumn, String> {
    match nullable_from_rows::<i8>(rows, ix) {
        Ok(col) => Ok(col),
        Err(_) => match nullable_from_rows::<i16>(rows, ix) {
            Ok(col) => Ok(col),
            Err(_) => match nullable_from_rows::<i32>(rows, ix) {
                Ok(col) => Ok(col),
                Err(_) => match nullable_from_rows::<u32>(rows, ix) {
                    Ok(col) => Ok(col),
                    Err(_) => nullable_from_rows::<i64>(rows, ix)
                }
            }
        }
    }
}

pub fn try_any_float(rows : &[postgres::row::Row], ix : usize) -> Result<NullableColumn, String> {
    match nullable_from_rows::<i8>(rows, ix) {
        Ok(col) => Ok(col),
        Err(_) => match nullable_from_rows::<f32>(rows, ix) {
            Ok(col) => Ok(col),
            Err(_) => nullable_from_rows::<f64>(rows, ix)
        }
    }
}*/

pub fn copy_table_to_postgres(
    client : &mut Client,
    tbl : &mut Table,
    dst : &str,
    cols : &[String],
    schema : &[DBObject]
) -> Result<(), String> {
    let copy_stmt = match cols.len() {
        0 => format!("COPY {} FROM stdin with csv header quote '\"';", dst),
        n => {
            let mut cols_agg = String::new();
            for i in 0..n {
                cols_agg += &cols[n];
                if i <= n-1 {
                    cols_agg += ",";
                }
            }
            format!("COPY {} ({}) FROM stdin with csv header quote '\"';", dst, cols_agg)
        }
    };

    // TODO filter cols

    if !crate::sql::object::schema_has_table(dst, schema) {
        let create = tbl.sql_table_creation(dst, cols).unwrap();
        println!("Creating new table with {}", create);
        client.execute(&create[..], &[])
            .map_err(|e| format!("{}", e) )?;
    } else {
        println!("Uploading to existing table");
    }

    let mut writer = client.copy_in(&copy_stmt[..])
        .map_err(|e| format!("{}", e) )?;
    let tbl_content = table::full_csv_display(tbl, cols.into());
    writer.write_all(tbl_content.as_bytes())
        .map_err(|e| format!("Copy from stdin error: {}", e) )?;
    writer.finish()
        .map_err(|e| format!("Copy from stdin error: {}", e) )?;
    Ok(())
}

pub enum ArrayType {
    Float4,
    Float8,
    Text,
    Int2,
    Int4,
    Int8,
    Json
}

pub fn nullable_unable_to_parse<'a>(rows : &'a [postgres::row::Row], ty_name : &postgres::types::Type) -> NullableColumn {
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
        Some(serde_json::Value::String(String::from("NULL")))
    }
}

pub fn nullable_from_arr<'a>(
    rows : &'a [postgres::row::Row],
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
        }
    };
    Ok(NullableColumn::from(data))
}

pub fn build_table_from_postgre(rows : &[postgres::row::Row]) -> Result<Table, &'static str> {
    let mut names : Vec<String> = rows.get(0)
        .map(|r| r.columns().iter().map(|c| c.name().to_string()).collect() )
        .ok_or("No rows available")?;
    let mut n_unnamed = 1;
    for (ix, name) in names.iter_mut().enumerate() {
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
    // println!("Column types");
    for i in 0..ncols {
        // println!("{:?}", col_types[i]);
        let is_bool = col_types[i] == &Type::BOOL;
        let is_bytea = col_types[i] == &Type::BYTEA;
        let is_text = col_types[i] == &Type::TEXT || col_types[i] == &Type::VARCHAR;
        let is_double = col_types[i] == &Type::FLOAT8;
        let is_float = col_types[i] == &Type::FLOAT4;
        let is_int = col_types[i] == &Type::INT4;
        let is_long = col_types[i] == &Type::INT8;
        let is_smallint = col_types[i] == &Type::INT2;
        let is_timestamp = col_types[i] == &Type::TIMESTAMP;
        let is_date = col_types[i] == &Type::DATE;
        let is_time = col_types[i] == &Type::TIME;
        let is_numeric = col_types[i] == &Type::NUMERIC;
        let is_json = col_types[i] == &Type::JSON || col_types[i] == &Type::JSONB;
        let is_text_arr = col_types[i] == &Type::TEXT_ARRAY;
        let is_real_arr = col_types[i] == &Type::FLOAT4_ARRAY;
        let is_dp_arr = col_types[i] == &Type::FLOAT8_ARRAY;
        let is_smallint_arr = col_types[i] == &Type::INT2_ARRAY;
        let is_int_arr = col_types[i] == &Type::INT4_ARRAY;
        let is_bigint_arr = col_types[i] == &Type::INT8_ARRAY;
        let is_xml = col_types[i] == &Type::XML;
        let is_json_arr = col_types[i] == &Type::JSON_ARRAY;
        let array_ty = match (is_text_arr, is_real_arr, is_dp_arr, is_smallint_arr, is_int_arr, is_bigint_arr, is_json_arr) {
            (true, _, _, _, _, _, _) => Some(ArrayType::Text),
            (_, true, _, _, _, _, _) => Some(ArrayType::Float4),
            (_, _, true, _, _, _, _) => Some(ArrayType::Float8),
            (_, _, _, true, _, _, _) => Some(ArrayType::Int2),
            (_, _, _, _, true, _, _) => Some(ArrayType::Int4),
            (_, _, _, _, _, _, true) => Some(ArrayType::Json),
            _ => None
        };

        // Postgres Interval type is unsupported by the client driver
        // let is_interval = col_types[i] == &Type::INTERVAL;

        if is_bool {
            null_cols.push(nullable_from_rows::<bool>(rows, i)?);
        } else {
            if is_bytea {
                null_cols.push(nullable_from_rows::<Vec<u8>>(rows, i)?);
            } else {
                if is_text {
                    null_cols.push(nullable_from_rows::<String>(rows, i)?);
                } else {
                    if is_double {
                        null_cols.push(nullable_from_rows::<f64>(rows, i)?);
                    } else {
                        if is_float {
                            null_cols.push(nullable_from_rows::<f32>(rows, i)?);
                        } else {
                            if is_int {
                                null_cols.push(nullable_from_rows::<i32>(rows, i)?);
                            } else {
                                if is_smallint {
                                    null_cols.push(nullable_from_rows::<i16>(rows, i)?);
                                } else {
                                    if is_long {
                                        null_cols.push(nullable_from_rows::<i64>(rows, i)?);
                                    } else {
                                        if is_timestamp {
                                            null_cols.push(as_nullable_text::<chrono::NaiveDateTime>(rows, i)?);
                                        } else {
                                            if is_date {
                                                null_cols.push(as_nullable_text::<chrono::NaiveDate>(rows, i)?);
                                            } else {
                                                if is_time {
                                                    null_cols.push(as_nullable_text::<chrono::NaiveTime>(rows, i)?);
                                                } else {
                                                    if is_numeric {
                                                        null_cols.push(nullable_from_rows::<Decimal>(rows, i)?);
                                                    } else {
                                                        if is_json {
                                                            null_cols.push(nullable_from_rows::<Value>(rows, i)?);
                                                        } else {
                                                            if let Some(ty) = array_ty {
                                                                null_cols.push(nullable_from_arr(rows, i, ty)?);
                                                            } else {
                                                                null_cols.push(nullable_unable_to_parse(rows, col_types[i]));
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    let cols : Vec<Column> = null_cols.drain(0..names.len())
        .map(|nc| nc.to_column()).collect();
    Ok(Table::new(None, names, cols)?)
}

fn run_local_statement(
    local : &LocalStatement,
    conn : &mut Client,
    exec : &Arc<Mutex<(Executor, String)>>,
    results : &mut Vec<QueryResult>
) -> Result<(), String> {
    match local {
        LocalStatement::Copy(c) => {
            // println!("Found copy: {:?}", c);
            match copy(conn, &c, &*exec) {
                Ok(n) => match (c.target, n) {
                    (CopyTarget::From, 0) => {
                        results.push(QueryResult::Invalid(format!("No rows copied to server"), false));
                    },
                    (CopyTarget::From, n) => {
                        results.push(QueryResult::Statement(format!("Copied {} row(s)", n)));
                    },
                    (CopyTarget::To, _) => {
                        results.push(QueryResult::Statement(format!("Copy to client successful")));
                    }
                },
                Err(e) => {
                    results.push(QueryResult::Invalid(e, false));
                }
            }
        },
        LocalStatement::Decl(decl) => {
            let exec = exec.lock().map_err(|e| format!("{}", e))?;
            for name in decl.names.iter() {
                if exec.0.has_var(name) {
                    let msg = format!("Variable {} already declared", name);
                    results.push(QueryResult::Invalid(msg.clone(), false));
                    return Err(msg);
                }
            }
            for name in &decl.names {
                exec.0.set_var(&name[..], String::new());
            }
            results.push(QueryResult::Empty);
        },
        LocalStatement::Exec(run) => {

            let using_ans = if let Ok(mut exec) = exec.lock() {
                if let Some(using) = run.using.as_ref() {
                    if let Some(val) = exec.0.get_var(using) {
                        if val.len() >= 1 {
                            exec.1 = val;
                            Ok(())
                        } else {
                            Err(format!("No content for variable {}", using))
                        }
                    } else {
                        Err(format!("No variable {} declared", using))
                    }
                } else {
                    exec.1 = String::new();
                    Ok(())
                }
            } else {
                Err(format!("Unable to borrow executor"))
            };

            if let Err(e) = using_ans {
                results.push(QueryResult::Invalid(e.clone(), false));
                return Err(e);
            }

            let into_ans = if let Ok(exec) = exec.lock() {
                if let Some(into) = run.into.as_ref() {
                    if exec.0.has_var(into) {
                        Ok(Some(into.clone()))
                    } else {
                        Err(format!("Variable {} not declared", into))
                    }
                } else {
                    Ok(None)
                }
            } else {
                Err(format!("Unable to borrow executor"))
            };

            let opt_into : Option<String> = match into_ans {
                Ok(into) => into,
                Err(e) => {
                    results.push(QueryResult::Invalid(e.clone(), false));
                    return Err(e);
                }
            };

            match wait_command_execution(&run.call, &exec) {
                Ok(out) => {
                    if let Some(into) = opt_into {
                        if out.len() > 0 {
                            if let Ok(exec) = exec.lock() {
                                exec.0.set_var(&into, out);
                            } else {
                                return Err(format!("Unable to borrow executor"));
                            }
                        } else {
                            let msg = format!("Command {} did not yield any output for variable {}", run.call, into);
                            results.push(QueryResult::Invalid(msg.clone(), false));
                            return Err(msg);
                        }
                    } else {
                        if out.len() > 0 {
                            match Table::new_from_text(out.clone()) {
                                Ok(tbl) => {
                                    results.push(QueryResult::Valid(out, tbl));
                                },
                                Err(e) => {
                                    let msg = format!("Unable to parse table from output: {}", e);
                                    results.push(QueryResult::Invalid(msg.clone(), false));
                                    return Err(msg);
                                }
                            }
                        } else {
                            results.push(QueryResult::Empty);
                        }
                    }
                },
                Err(e) => {
                    results.push(QueryResult::Invalid(e.clone(), false));
                    return Err(e);
                }
            }
        }
    }
    Ok(())
}

fn format_pg_string(e : &mut String) {
    if e.starts_with("db error: ERROR:") || e.starts_with("db error: FATAL:") {
        *e = e.clone().chars().skip(16).collect::<String>();
    } else {
        if e.starts_with("db error:") {
            *e = e.clone().chars().skip(9).collect::<String>();
        }
    }
    *e = e.clone().trim().to_string();
    let fst_char = e[0..1].to_uppercase().to_string();
    e.replace_range(0..1, &fst_char);
}

