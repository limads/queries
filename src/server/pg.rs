use postgres;
use crate::sql::{*, object::*};
use rust_decimal::Decimal;
use monday::tables::column::*;
use monday::tables::nullable_column::*;
use monday::tables::table::*;
use postgres::types::Type;
use std::io::Write;
use std::error::Error;
use monday::tables::table::{self, Table, Align, Format, TableSettings, BoolField, NullField};
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
use monday::tables::field::Field;
use crate::client::ConnectionInfo;
use std::time::SystemTime;

pub struct PostgresConnection {

    conn_str : String,

    info : ConnectionInfo,

    conn : postgres::Client,

    exec : Arc<Mutex<(Executor, String)>>,

    channel : Option<(String, String, bool)>

}

impl PostgresConnection {

    pub fn try_new(conn_str : String, info : ConnectionInfo) -> Result<Self, String> {
        let tls_mode = NoTls{ };
        //println!("{}", conn_str);
        match Client::connect(&conn_str[..], tls_mode) {
            Ok(conn) => Ok(Self{
                conn_str,
                conn,
                exec : Arc::new(Mutex::new((Executor::new(), String::new()))) ,
                channel : None,
                info
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

    fn query(&mut self, query : &str, subs : &HashMap<String, String>) -> StatementOutput {
        // let query = substitute_if_required(q, subs);
        // println!("Final query: {}", query);
        // println!("Executing: {}", query);
        match self.conn.query(&query[..], &[]) {
            Ok(rows) => {
                match Table::from_rows(&rows[..]) {
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

                            // The reporing feature relies on unique column names. Perhaps move
                            // this error to the reporting validation.
                            StatementOutput::Invalid(crate::sql::build_error_with_stmt("Non-unique column names", &query[..]), false)
                        }
                    },
                    Err(e) => StatementOutput::Invalid(crate::sql::build_error_with_stmt(&e, &query[..]), false)
                }
            },
            Err(e) => {
                let mut e = e.to_string();
                format_pg_string(&mut e);
                StatementOutput::Invalid(e, true)
            }
        }
    }

    fn exec(&mut self, stmt : &AnyStatement, subs : &HashMap<String, String>) -> StatementOutput {
        // let final_stmt = substitute_if_required(&s, subs);

        // TODO The postgres driver panics when the number of arguments differ from the number of required
        // substitutions with $. Must reject any query/statements containing those outside literal strings,
        // which can be verified with the sqlparse tokenizer. This will happen, e.g. when the user attempts
        // to create a function with non-named arguments, which are refered in the body with $1, $2, etc.
        // Perhaps we can use the client.simple_query or client.batch_execute in those cases, but we should
        // parse away create function statements to do this call instead of query and execute. But sqlparser
        // does not recognize create function for now.

        let ans = match stmt {
            AnyStatement::Parsed(stmt, s) => {
                let s = format!("{}", stmt);
                // let final_statement = substitute_if_required(&s, subs);
                // println!("Final statement: {}", final_statement);
                self.conn.execute(&s[..], &[])
            },
            AnyStatement::Raw(_, s, _) => {
                // let final_statement = substitute_if_required(&s, subs);
                self.conn.execute(&s[..], &[])
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
                StatementOutput::Invalid(e, true)
            }
        }
    }

    fn info(&mut self) -> Option<DBInfo> {
        let mut top_objs = Vec::new();
        if let Some(schemata) = get_postgres_schemata(self) {
            // println!("Obtained schemata: {:?}", schemata);
            for (schema, tbls) in schemata.iter() {
                let mut tbl_objs = Vec::new();
                for t in tbls.iter() {
                    if let Some(tbl) = get_postgres_columns(self, &schema[..], &t[..]) {
                        tbl_objs.push(tbl);
                    } else {
                        // println!("Failed getting columns for {}.{}", schema, t);
                        return None;
                    }
                }
                tbl_objs.sort_by(|a, b| {
                    a.obj_name().chars().next().unwrap().cmp(&b.obj_name().chars().next().unwrap())
                });

                let t = SystemTime::now();
                println!("Getting functions");
                let func_objs = get_postgres_functions(self, &schema[..]).unwrap_or(Vec::new());
                println!("Got functions in {} ms", SystemTime::now().duration_since(t).unwrap().as_millis());

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

            let mut details = DBDetails::default();
            let version = self.conn.query_one("show server_version", &[]).unwrap().get::<_, String>(0);
            let version_number = version.split(" ").next().unwrap();
            details.server = format!("Postgres {}", version_number);
            // details.encoding = self.conn.query_one("show server_encoding", &[]).unwrap().get::<_, String>(0);
            details.locale = self.conn.query_one("show lc_collate", &[]).unwrap().get::<_, String>(0);
            details.size = self.conn.query_one("select pg_size_pretty(pg_database_size($1));", &[&self.info.database]).unwrap().get::<_, String>(0);

            // Can also use extract (days from interval) or extract(hour from interval)
            details.uptime = self.conn.query_one(UPTIME_QUERY, &[]).unwrap().get::<_, String>(0);

            Some(DBInfo { schema : top_objs, details : Some(details) })

        } else {
            // println!("Failed retrieving database schemata");
            let mut empty = Vec::new();
            empty.push(DBObject::Schema{ name : "public".to_string(), children : Vec::new() });
            Some(DBInfo { schema : empty, ..Default::default() })
            // None
        }
    }

    fn import(
        &mut self,
        tbl : &mut Table,
        dst : &str,
        cols : &[String],
        // schema : &[DBObject]
    ) -> Result<usize, String> {
        let client = &mut self.conn;
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

        /*if !crate::sql::object::schema_has_table(dst, schema) {
            let create = tbl.sql_table_creation(dst, cols).unwrap();
            println!("Creating new table with {}", create);
            client.execute(&create[..], &[])
                .map_err(|e| format!("{}", e) )?;
        } else {
            println!("Uploading to existing table");
        }*/

        let mut writer = client.copy_in(&copy_stmt[..])
            .map_err(|e| format!("{}", e) )?;
        let tbl_content = table::full_csv_display(tbl, cols.into());
        writer.write_all(tbl_content.as_bytes())
            .map_err(|e| format!("Copy from stdin error: {}", e) )?;
        writer.finish()
            .map_err(|e| format!("Copy from stdin error: {}", e) )?;
        Ok(tbl.shape().0)
    }


}

const UPTIME_QUERY : &'static str = r#"
with uptime as (select current_timestamp - pg_postmaster_start_time() as uptime)
select cast(extract(days from uptime) as integer) || 'd ' ||
    cast(extract(hours from uptime) as integer) || 'h ' ||
    cast(extract(minutes from uptime) as integer) || 'm'
from uptime;
"#;

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
// Alternative query:
// select cast(cast(pg_proc.oid as regprocedure) as text) from pg_proc left join pg_namespace on
// pg_proc.pronamespace = pg_namespace.oid where pg_namespace.nspname like 'public';
// Then parse the resulting text (but it won't give the return type).
// TODO the generate_series seems to be slowing the query down, making the connection startup
// unreasonably slow. But removing it makes the arguments be unnested at the incorrect order.
fn get_postgres_functions(conn : &mut PostgresConnection, schema : &str) -> Option<Vec<DBObject>> {
    let fn_query = format!(r#"
    with arguments as (
        with arg_types as (select pg_proc.oid as proc_oid,
            unnest(proargtypes) as arg_oid,
            generate_series(1, cardinality(proargtypes)) as arg_order
            from pg_catalog.pg_proc
        ) select arg_types.proc_oid as proc_id,
            array_agg(cast(typname as text) order by arg_order) as arg_typename
            from pg_catalog.pg_type inner join arg_types on pg_type.oid = arg_types.arg_oid
            group by arg_types.proc_oid
            order by arg_types.proc_oid
    ) select pg_proc.oid,
        pg_proc.prokind,
        proname::text,
        arguments.arg_typename,
        cast(typname as text) as ret_typename,
        pg_proc.proargnames as arg_names,
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

    let ans = conn.try_run(fn_query, &HashMap::new(), false) /*.map_err(|e| println!("{}", e) )*/ .ok()?;
    match ans.get(0)? {
        StatementOutput::Valid(_, fn_info) => {
            let mut fns = Vec::new();
            let names = Vec::<String>::try_from(fn_info.get_column(2).unwrap().clone()).ok()?;
            let arg_types = (0..names.len()).map(|ix| fn_info.get_column(3).unwrap().at(ix) ).collect::<Vec<_>>();
            // println!("Retrieved args = {:?}", args);
            let ret = Vec::<String>::try_from(fn_info.get_column(4).unwrap().clone()).ok()?;
            let arg_names = (0..names.len()).map(|ix| fn_info.get_column(5).unwrap().at(ix) ).collect::<Vec<_>>();
            let fn_iter = names.iter().zip(arg_names.iter().zip(arg_types.iter().zip(ret.iter())));
            for (name, (arg_ns, (arg_tys, ret))) in fn_iter {
                let args = match arg_tys {
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
                let ret = match &ret[..] {
                    "VOID" | "void" => None,
                    _ => Some(DBType::from_str(ret).unwrap_or(DBType::Unknown))
                };
                let mut func_arg_names = Vec::new();
                if let Some(ns) = arg_ns {
                    match ns {
                        Field::Json(Value::Array(arr)) => {
                            for name in arr.iter() {
                                let content = name.to_string();
                                let trim_content = content.trim_start_matches("\"").trim_end_matches("\"");
                                if &trim_content[..] != "null" && &trim_content[..] != "NULL" {
                                    func_arg_names.push(trim_content.to_string());
                                }
                            }
                        },
                        _ => { }
                    }
                }
                let opt_func_arg_names = if func_arg_names.len() > 0 && func_arg_names.len() == args.len() {
                    Some(func_arg_names)
                } else {
                    None
                };
                fns.push(DBObject::Function { name : name.clone(), args, arg_names : opt_func_arg_names, ret });
            }

            fns.sort_by(|a, b| {
                a.obj_name().chars().next().unwrap().cmp(&b.obj_name().chars().next().unwrap())
            });
            Some(fns)
        },
        StatementOutput::Invalid(msg, _) => { /*println!("{}", msg);*/ None },
        _ => None
    }
}

/// Return HashMap of Schema->Tables
fn get_postgres_schemata(conn : &mut PostgresConnection) -> Option<HashMap<String, Vec<String>>> {
    let tbl_query = String::from("select schemaname::text, tablename::text \
        from pg_catalog.pg_tables \
        where schemaname != 'pg_catalog' and schemaname != 'information_schema';");
    let ans = conn.try_run(tbl_query, &HashMap::new(), false)
        /*.map_err(|e| println!("{}", e) )*/ .ok()?;
    let q_res = ans.get(0)?;
    match q_res {
        StatementOutput::Valid(_, table) => {
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
        StatementOutput::Invalid(msg, _) => { println!("{}", msg); None },
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
    let ans = conn.try_run(view_query, &HashMap::new(), false) /*.map_err(|e| println!("{}", e) )*/ .ok()?;
    match ans.get(0)? {
        StatementOutput::Valid(_, view_info) => {
            let mut views = Vec::new();
            let info = Vec::<String>::try_from(view_info.get_column(1).unwrap().clone()).ok()?;
            for name in info.iter() {
                // let name = row.get::<String>(1);
                views.push(DBObject::View { name : name.clone() });
            }
            Some(views)
        },
        StatementOutput::Invalid(msg, _) => { /*println!("{}", msg);*/ None },
        _ => None
    }
}

fn get_postgres_pks(
    conn : &mut PostgresConnection,
    schema_name :
    &str,
    tbl_name : &str
) -> Option<Vec<String>> {
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
    let ans = conn.try_run(pk_query, &HashMap::new(), false) /*.map_err(|e| println!("{}", e) )*/ .ok()?;
    if let Some(q_res) = ans.get(0) {
        match q_res {
            StatementOutput::Valid(_, col_info) => {
                let cols = col_info.get_column(3)
                    .and_then(|c| { let s : Option<Vec<String>> = c.clone().try_into().ok(); s })?;
                Some(cols)
            },

            // Will throw an error when there are no relations.
            StatementOutput::Invalid(msg, _) => { /*println!("{}", msg);*/ None },
            _ => None
        }
    } else {
        // println!("Database info query did not return any results");
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
    let ans = conn.try_run(rel_query, &HashMap::new(), false) /*().map_err(|e| println!("{}", e) ) */ .ok()?;
    if let Some(q_res) = ans.get(0) {
        match q_res {
            StatementOutput::Valid(_, col_info) => {
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
                // println!("Relation vector: {:?}", rels);
                Some(rels)
            },
            StatementOutput::Invalid(msg, _) => { /*println!("{}", msg);*/ None },
            _ => None
        }
    } else {
        // println!("Database info query did not return any results");
        None
    }
}

fn get_postgres_columns(conn : &mut PostgresConnection, schema_name : &str, tbl_name : &str) -> Option<DBObject> {
    let col_query = format!("select column_name::text, data_type::text \
        from information_schema.columns where table_name = '{}' and table_schema='{}';", tbl_name, schema_name);
    let ans = conn.try_run(col_query, &HashMap::new(), false)
        /*.map_err(|e| println!("{}", e) )*/ .ok()?;
    if let Some(q_res) = ans.get(0) {
        match q_res {
            StatementOutput::Valid(_, col_info) => {
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
            StatementOutput::Invalid(msg, _) => { /*println!("{}", msg);*/ None },
            _ => None
        }
    } else {
        // println!("Database info query did not return any results");
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
            // println!("Received data from copy: {}", csv_out);
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

/*pub fn col_as_vec<'a, T>(
    rows : &'a [postgres::row::Row],
    ix : usize
) -> Result<Vec<T>, &'static str>
    where
        T : FromSql<'a> + ToSql + Sync,
{
    let mut data = Vec::new();
    for r in rows.iter() {
        let datum = r.try_get::<usize, T>(ix)
            .map_err(|e| { /*println!("{}", e);*/ "Unable to parse column" })?;
        data.push(datum);
    }
    Ok(data)
}*/

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

fn run_local_statement(
    local : &LocalStatement,
    conn : &mut Client,
    exec : &Arc<Mutex<(Executor, String)>>,
    results : &mut Vec<StatementOutput>
) -> Result<(), String> {
    match local {
        LocalStatement::Copy(c) => {
            // println!("Found copy: {:?}", c);
            match copy(conn, &c, &*exec) {
                Ok(n) => match (c.target, n) {
                    (CopyTarget::From, 0) => {
                        results.push(StatementOutput::Invalid(format!("No rows copied to server"), false));
                    },
                    (CopyTarget::From, n) => {
                        results.push(StatementOutput::Statement(format!("Copied {} row(s)", n)));
                    },
                    (CopyTarget::To, _) => {
                        results.push(StatementOutput::Statement(format!("Copy to client successful")));
                    }
                },
                Err(e) => {
                    results.push(StatementOutput::Invalid(e, false));
                }
            }
        },
        LocalStatement::Decl(decl) => {
            let exec = exec.lock().map_err(|e| format!("{}", e))?;
            for name in decl.names.iter() {
                if exec.0.has_var(name) {
                    let msg = format!("Variable {} already declared", name);
                    results.push(StatementOutput::Invalid(msg.clone(), false));
                    return Err(msg);
                }
            }
            for name in &decl.names {
                exec.0.set_var(&name[..], String::new());
            }
            results.push(StatementOutput::Empty);
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
                results.push(StatementOutput::Invalid(e.clone(), false));
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
                    results.push(StatementOutput::Invalid(e.clone(), false));
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
                            results.push(StatementOutput::Invalid(msg.clone(), false));
                            return Err(msg);
                        }
                    } else {
                        if out.len() > 0 {
                            match Table::new_from_text(out.clone()) {
                                Ok(tbl) => {
                                    results.push(StatementOutput::Valid(out, tbl));
                                },
                                Err(e) => {
                                    let msg = format!("Unable to parse table from output: {}", e);
                                    results.push(StatementOutput::Invalid(msg.clone(), false));
                                    return Err(msg);
                                }
                            }
                        } else {
                            results.push(StatementOutput::Empty);
                        }
                    }
                },
                Err(e) => {
                    results.push(StatementOutput::Invalid(e.clone(), false));
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

