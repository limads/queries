/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

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
use serde_json::Value;
use crate::sql::object::{DBObject, DBType, DBInfo};
use crate::sql::parsing::AnyStatement;
use crate::sql::copy::*;
use super::Connection;
use std::sync::{Arc, Mutex};
use crate::command::Executor;
use std::collections::HashMap;
use std::fs::{self, File};
use std::ffi::OsStr;
use postgres::types::{FromSql, ToSql};
use std::convert::{TryFrom, TryInto};
use std::str::FromStr;
use std::io::Read;
use std::mem;
use postgres::NoTls;
use tokio_postgres::Client;
use itertools::Itertools;
use std::path::Path;
use crate::tables::field::Field;
use crate::client::ConnectionInfo;
use std::time::SystemTime;
use crate::client::{ConnURI, ConnConfig};
use sqlparser::ast::Statement;
use crate::client::Security;
use futures::future;
use std::ops::Range;

/*
client::batch_execute can be used to run a series of statements (useful at transaction blocks)
but do not return results per statement.
*/

pub struct PostgresConnection {

    info : ConnectionInfo,

    // conn : postgres::Client,
    
    client : tokio_postgres::Client,
    
    // conn : tokio_postgres::Connection,
    
    rt : Option<tokio::runtime::Runtime>,
    
    // rt_guard : tokio::runtime::EnterGuard,

    exec : Arc<Mutex<(Executor, String)>>,

    channel : Option<(String, String, bool)>

}

const CERT_ERR : &'static str = r#"
"Remote connections without an associated TLS/SSL certificate are unsupported.
Inform a certificate file path for this host at the security settings."
"#;

async fn connect(
    rt : &tokio::runtime::Runtime, 
    uri : &ConnURI
) -> Result<tokio_postgres::Client, String> {

    // If a TLS certificate is configured, assume the client wants to connect
    // via TLS.
    if let Some(cert) = uri.info.cert.as_ref() {

        match uri.info.is_tls {
            Some(true) => {
            
                use native_tls::{Certificate, TlsConnector};
                use postgres_native_tls::MakeTlsConnector;

                let cert_content = fs::read(cert).map_err(|e| format!("{}", e) )?;
                let cert = Certificate::from_pem(&cert_content)
                    .map_err(|e| format!("{}", e) )?;
                let connector = TlsConnector::builder()
                    .add_root_certificate(cert)
                    .build().map_err(|e| format!("{}", e) )?;
                
                let connector = MakeTlsConnector::new(connector);
                match tokio_postgres::connect(&uri.uri[..], connector).await {
                    Ok((cli, conn)) => {
                        /*Ok(Self{
                            conn,
                            exec : Arc::new(Mutex::new((Executor::new(), String::new()))),
                            channel : None,
                            info : uri.info
                        })*/
                        rt.spawn(conn);
                        Ok(cli)
                    },
                    Err(e) => {
                        let mut e = e.to_string();
                        format_pg_string(&mut e);
                        Err(e)
                    }
                }
            },
            Some(false) => {
            
                use openssl::ssl::{SslConnector, SslMethod};
                use postgres_openssl::MakeTlsConnector;
                
                let mut builder = SslConnector::builder(SslMethod::tls())
                    .map_err(|e| format!("{}", e) )?;
                builder.set_ca_file(&cert)
                    .map_err(|e| format!("{}", e) )?;
                let connector = MakeTlsConnector::new(builder.build());
                match tokio_postgres::connect(&uri.uri[..], connector).await {
                    Ok((cli, conn)) => {
                        /*Ok(Self{
                            conn,
                            exec : Arc::new(Mutex::new((Executor::new(), String::new()))),
                            channel : None,
                            info : uri.info
                        })*/
                        rt.spawn(conn);
                        Ok(cli)
                    },
                    Err(e) => {
                        let mut e = e.to_string();
                        format_pg_string(&mut e);
                        Err(e)
                    }
                }
            },
            None => {
                Err(format!("Secure connection modality unspecified"))
            }
            // Security::None => {
            //    Err(format!("Certificate is configured for host, but security setting is None"))
            // }
        }
    } else {
        if crate::client::is_local(&uri.info)  == Some(true) {
            // Only connect without SSL/TLS when the client is local.
            match tokio_postgres::connect(&uri.uri[..], NoTls{ }).await {
                Ok((cli, conn)) => {
                    /*Ok(Self{
                        // conn_str : uri.uri,
                        conn,
                        exec : Arc::new(Mutex::new((Executor::new(), String::new()))) ,
                        channel : None,
                        info : uri.info
                    })*/
                    rt.spawn(conn);
                    Ok(cli)
                },
                Err(e) => {
                    let mut e = e.to_string();
                    format_pg_string(&mut e);
                    Err(e)
                }
            }
        } else {
            Err(format!("{}", CERT_ERR))
        }
    }
}

impl PostgresConnection {

    /* Tries to build a new connection from a ConnURI. Takes the URI
    by value, guaranteeing that after this point, the queries client state
    does not hold in memory any security-sensitive information. */
    pub fn try_new(uri : ConnURI) -> Result<Self, String> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        // let rt_guard = rt.enter();
        let client = rt.block_on(async {
            connect(&rt, &uri).await
        })?;
        Ok(Self {
            info : uri.info,
            rt : Some(rt),
            // rt_guard,
            client,
            // conn,
            exec : Arc::new(Mutex::new((Executor::new(), String::new()))),
            channel : None,
        })
    }

}

fn build_table(rows : &[postgres::Row], query : &str) -> StatementOutput {
    if rows.len() == 0 {
        if let Ok(cols) = crate::sql::parsing::parse_query_cols(query) {
            return StatementOutput::Valid(query.to_string(), Table::empty(cols));
        }
    }
    match Table::from_rows(rows) {
        Ok(mut tbl) => {
            if let Some((name, relation)) = crate::sql::table_name_from_sql(query) {
                tbl.set_name(Some(name));
                if !relation.is_empty() {
                    tbl.set_relation(Some(relation));
                }
            }
            
            /*if tbl.names().iter().unique().count() == tbl.names().len() {
                StatementOutput::Valid(query.to_string(), tbl)
            } else {

                // The reporing feature relies on unique column names. Perhaps move
                // this error to the reporting validation.
                StatementOutput::Invalid(crate::sql::build_error_with_stmt("Non-unique column names", &query[..]), false)
            }*/
            StatementOutput::Valid(query.to_string(), tbl)
            
        },
        Err(e) => StatementOutput::Invalid(crate::sql::build_error_with_stmt(&e, &query[..]), false)
    }
}

/* Asynchronous query executions cannot cancel the whole chain once a first error query is found */
async fn query_async(stmt : &AnyStatement, client : &mut tokio_postgres::Client) -> Result<StatementOutput, String> {
    match stmt {
        AnyStatement::Parsed(Statement::Query(_), sql) => {
            match client.query(sql, &[]).await {
                Ok(rows) => {
                    Ok(build_table(&rows[..], sql))
                },
                Err(e) => {
                    let mut e = e.to_string();
                    format_pg_string(&mut e);
                    Ok(StatementOutput::Invalid(e, true))
                }
            }
        },
        _ => {
            Err(format!("Only parsed queries can be executed in async mode"))
        }
    }
}

/*async fn query_async_recursive(
    stmts : &[AnyStatement], 
    client : &mut tokio_postgres::Client, 
    mut v : Vec<StatementOutput>
) -> Result<Vec<StatementOutput>, String> {
    match stmts.get(0) {
        Some(AnyStatement::Parsed(Statement::Query(_), sql)) => {
            match client.query(sql, &[]).await {
                Ok(rows) => {
                    v.push(build_table(&rows[..], sql));
                    if stmt.len() > 1 {
                        query_async_recursive(stmts[1..], client, v)
                    } else {
                        v
                    }
                },
                Err(e) => {
                    let mut e = e.to_string();
                    format_pg_string(&mut e);
                    Ok(StatementOutput::Invalid(e, true))
                }
            }
        },
        None => {
            v
        },
        _ => {
            Err(format!("Only parsed queries can be executed in async mode"))
        }
    }
}*/

async fn query_multiple(
    client : &mut Client, 
    stmts : &[AnyStatement]
) -> Result<Vec<Vec<tokio_postgres::Row>>, tokio_postgres::Error> {
    let mut query_futures = Vec::new();
    for s in stmts {
        match s {
            AnyStatement::Parsed(stmt, sql) => {
                if crate::sql::is_like_query(&stmt) {
                    query_futures.push(client.query(sql, &[]));
                } else {
                    panic!("Only queries can be executed asynchronously")
                }
            },
            other => {
                panic!("Only queries can be executed asynchronously")
            }
        }
    }
    future::try_join_all(query_futures).await
}

impl Connection for PostgresConnection {

    fn configure(&mut self, cfg : ConnConfig) {
        let cfg_stmt = format!("set session statement_timeout to {};", cfg.timeout);
        self.rt.as_ref().unwrap().block_on(async {
            match self.client.execute(&cfg_stmt[..], &[]).await {
                Ok(_) => { },
                Err(e) => {
                    eprintln!("{}", e);
                }
            }
        });
    }

    fn listen_at_channel(&mut self, channel : String) {

    }

    fn query(&mut self, query : &str, subs : &HashMap<String, String>) -> StatementOutput {
        self.rt.as_ref().unwrap().block_on(async {
            match self.client.query(&query[..], &[]).await {
                Ok(rows) => {
                    build_table(&rows[..], query)
                },
                Err(e) => {
                    let mut e = e.to_string();
                    format_pg_string(&mut e);
                    StatementOutput::Invalid(e, true)
                }
            }
        })
    }

    fn exec_transaction(&mut self, any_stmt : &AnyStatement) -> StatementOutput {
        let rt = self.rt.take().unwrap();
        let out = rt.block_on(async {
            match any_stmt {
                AnyStatement::ParsedTransaction(stmts, _) => {
                    match self.client.transaction().await {
                        Ok(mut tr) => {
                            let mut total_changed = 0;
                            for stmt in stmts {
                                match &stmt {
                                    Statement::Commit{ .. } => {
                                        match tr.commit().await {
                                            Ok(_) => {
                                                return crate::sql::build_statement_result(any_stmt, total_changed as usize);
                                            },
                                            Err(e) => {
                                                let mut e = e.to_string();
                                                format_pg_string(&mut e);
                                                return StatementOutput::Invalid(e, true);
                                            }
                                        }
                                    },
                                    Statement::Query(_) => {
                                        match tr.query(&format!("{}", stmt), &[]).await {
                                            Ok(_) => {
                                                // Queries inside transactions are not shown for now.
                                            },
                                            Err(e) => {
                                                let mut e = e.to_string();
                                                format_pg_string(&mut e);
                                                return StatementOutput::Invalid(e, true);
                                            }
                                        }
                                    },
                                    other_stmt => {
                                        match tr.execute(&format!("{}", stmt), &[]).await {
                                            Ok(n) => {
                                                total_changed += n;
                                            },
                                            Err(e) => {
                                                let mut e = e.to_string();
                                                format_pg_string(&mut e);
                                                return StatementOutput::Invalid(e, true);
                                            }
                                        }
                                    }
                                }
                            }
                            
                            // Won't really be called, if the statement set returns early on a commit statement
                            // (guaranteed at the parsing stage). But this guarantees that commit isn't called automatically
                            // when tr goes out of scope and its destructor is called.
                            match tr.rollback().await {
                                Ok(_) => {
                                    StatementOutput::Invalid(format!("Transaction wasn't commited"), false)
                                },
                                Err(e) => {
                                    StatementOutput::Invalid(format!("{}",e), false)
                                }
                            }
                        },
                        Err(e) => {
                            StatementOutput::Invalid(format!("{}",e), false)    
                        }
                    }
                },
                _ => {
                    StatementOutput::Invalid(format!("Expected transaction"), false)
                }
            }
        });
        self.rt = Some(rt);
        out
    }
    
    fn query_async(&mut self, stmts : &[AnyStatement]) -> Vec<StatementOutput> {
        let rt = self.rt.take().unwrap();
        let res = rt.block_on(async {
            query_multiple(&mut self.client, stmts).await
        });
        match res {
            Ok(vec_rows) => {
                let mut out = Vec::new();
                assert!(stmts.len() == vec_rows.len());
                for i in 0..stmts.len() {
                    out.push(build_table(&vec_rows[i], stmts[i].sql()));
                }
                self.rt = Some(rt);
                out
            },
            Err(e) => {
                self.rt = Some(rt);
                vec![StatementOutput::Invalid(e.to_string(), false)]
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

        self.rt.as_ref().unwrap().block_on(async {
            let ans = match stmt {
                AnyStatement::Parsed(_, s) | AnyStatement::ParsedTransaction(_, s) => {
                    self.client.execute(&s[..], &[]).await
                },
                AnyStatement::Raw(_, s, _) => {
                    // let final_statement = substitute_if_required(&s, subs);
                    self.client.execute(&s[..], &[]).await
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
        })
    }

    fn conn_info(&self) -> ConnectionInfo {
        self.info.clone()
    }

    fn db_info(&mut self) -> Result<DBInfo, Box<dyn Error>> {
        
        let mut col_queries = Vec::new();
        let mut pk_queries = Vec::new();
        let mut rel_queries = Vec::new();
        let schemata = match get_postgres_schemata(self) {
            Ok(s) => s,
            Err(e) => Err(e)?
        };
        let mut view_queries = Vec::new();
        let mut fn_queries = Vec::new();
        
        for (schema, tbls) in schemata.iter() {
            for tbl in &tbls[..] {
                col_queries.push(AnyStatement::from_sql(&COLUMN_QUERY.replace("$TABLE", &tbl).replace("$SCHEMA", &schema)).unwrap());
                pk_queries.push(AnyStatement::from_sql(&PK_QUERY.replace("$TABLE", &tbl).replace("$SCHEMA", &schema)).unwrap());
                rel_queries.push(AnyStatement::from_sql(&REL_QUERY.replace("$TABLE", &tbl).replace("$SCHEMA", &schema)).unwrap());
            }
            view_queries.push(AnyStatement::from_sql(&VIEW_QUERY.replace("$SCHEMA", schema)).unwrap());
            fn_queries.push(AnyStatement::from_sql(&FN_QUERY.replace("$SCHEMA", schema)).unwrap());
        }
        
        let mut all_queries = Vec::new();
        let col_range = Range { start : 0, end : col_queries.len() };
        let pk_range = Range { start : col_range.end, end : col_range.end + pk_queries.len() };
        let rel_range = Range { start : pk_range.end, end : pk_range.end + rel_queries.len() };
        let view_range = Range { start : rel_range.end, end : rel_range.end + view_queries.len() };
        let fn_range = Range { start : view_range.end, end : view_range.end + fn_queries.len() };
        all_queries.extend(col_queries);
        all_queries.extend(pk_queries);
        all_queries.extend(rel_queries);
        all_queries.extend(view_queries);
        all_queries.extend(fn_queries);
        assert!(fn_range.end == all_queries.len());
        
        let out = self.query_async(&all_queries[..]);
        let col_outs : Vec<&Table> = out[col_range].iter().map(|o| o.table().unwrap() ).collect();
        let pk_outs : Vec<&Table> = out[pk_range].iter().map(|o| o.table().unwrap() ).collect();
        let rel_outs : Vec<&Table> = out[rel_range].iter().map(|o| o.table().unwrap() ).collect();
        let view_outs : Vec<&Table> = out[view_range].iter().map(|o| o.table().unwrap() ).collect();
        let fn_outs : Vec<&Table> = out[fn_range].iter().map(|o| o.table().unwrap() ).collect();
        
        let mut top_objs = Vec::new();
        let mut tbl_ix = 0;
        let mut schema_ix = 0;
        for (schema, tbls) in schemata.iter() {
            let mut tbl_objs = Vec::new();
            for tbl in &tbls[..] {
                let names = col_outs[tbl_ix].get_column(0)
                    .and_then(|c| { let s : Option<Vec<String>> = c.clone().try_into().ok(); s }).unwrap_or(Vec::new());
                let col_types = col_outs[tbl_ix].get_column(1)
                    .and_then(|c| { let s : Option<Vec<String>> = c.clone().try_into().ok(); s }).unwrap_or(Vec::new());
                let pks = retrieve_pks(pk_outs[tbl_ix]).unwrap_or(Vec::new());
                let cols = crate::sql::pack_column_types(names, col_types, pks).ok().unwrap_or(Vec::new());
                let rels = retrieve_relations(&rel_outs[tbl_ix]).unwrap_or(Vec::new());
                let obj = DBObject::Table{ schema : schema.to_string(), name : tbl.to_string(), cols, rels };
                tbl_objs.push(obj);
                tbl_ix += 1;
            }
            let func_objs = retrieve_functions(&fn_outs[schema_ix], &schema).unwrap_or(Vec::new());
            let view_objs = retrieve_views(&view_outs[schema_ix]).unwrap_or(Vec::new());
            schema_ix += 1;
            tbl_objs.sort_by(|a, b| {
                a.obj_name().chars().next().unwrap().cmp(&b.obj_name().chars().next().unwrap())
            });
            if view_objs.len() > 0 {
                tbl_objs.push(DBObject::Schema { name : format!("Views ({})", schema), children : view_objs } );
            }
            if func_objs.len() > 0 {
                tbl_objs.push(DBObject::Schema { name : format!("Functions ({})", schema), children : func_objs } );
            }
            let schema_obj = DBObject::Schema{ name : schema.to_string(), children : tbl_objs };
            top_objs.push(schema_obj);
        }
        
        let details = match query_db_details(self, &self.info.database.clone()[..]) {
            Ok(details) => Some(details),
            Err(e) => {
                eprintln!("{}", e);
                None
            }
        };

        Ok(DBInfo { schema : top_objs, details })

        /*} else {
            // println!("Failed retrieving database schemata");
            let mut empty = Vec::new();
            empty.push(DBObject::Schema{ name : "public".to_string(), children : Vec::new() });
            Some(DBInfo { schema : empty, ..Default::default() })
            // None
        }*/
    }

    fn import(
        &mut self,
        tbl : &mut Table,
        dst : &str,
        cols : &[String],
    ) -> Result<usize, String> {
        /*self.rt.block_on(async {
            let client = &mut self.client;
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

            let mut writer = client.copy_in(&copy_stmt[..]).await
                .map_err(|e| format!("{}", e) )?;
            let tbl_content = table::full_csv_display(tbl, cols.into());
            writer.write_all(tbl_content.as_bytes())
                .map_err(|e| format!("Copy from stdin error: {}", e) )?;
            writer.finish()
                .map_err(|e| format!("Copy from stdin error: {}", e) )?;
            Ok(tbl.shape().0)
        })*/
        unimplemented!()
    }


}

const SERVER_VERSION_QUERY : &'static str = "show server_version";

const COLLATION_QUERY : &'static str = "show lc_collate";

const SIZE_QUERY : &'static str = r#"select pg_size_pretty(pg_database_size('$DBNAME'));"#;

const UPTIME_QUERY : &'static str = r#"
with uptime as (select current_timestamp - pg_postmaster_start_time() as uptime)
select cast(extract(day from uptime) as integer) || 'd ' ||
    cast(extract(hour from uptime) as integer) || 'h ' ||
    cast(extract(minute from uptime) as integer) || 'm'
from uptime;
"#;

/*// cargo test -- uptime_query --nocapture
#[test]
fn uptime_query() {
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let mut ans = sqlparser::parser::Parser::parse_sql(&dialect, &UPTIME_QUERY[..]);
    println!("{:?}", ans);
}*/

fn query_db_details(cli : &mut PostgresConnection, dbname : &str) -> Result<DBDetails, Box<dyn Error>> {
    let mut details = DBDetails::default();
    let out = cli.query_async(&[
        AnyStatement::from_sql(SERVER_VERSION_QUERY).unwrap(),
        AnyStatement::from_sql(COLLATION_QUERY).unwrap(),
        AnyStatement::from_sql(&SIZE_QUERY.replace("$DBNAME", dbname)).unwrap(),
        AnyStatement::from_sql(UPTIME_QUERY).unwrap()
    ]);
    let version = out[0].table_or_error()?.display_content_at(0, 0, 1)
        .ok_or(format!("Missing version"))?;
    let version_number = version.split(" ").next()
        .ok_or(format!("Missing version number"))?;
    details.server = format!("Postgres {}", version_number);
    
    details.locale = out[1].table_or_error()?
        .display_content_at(0, 0, 1)
        .ok_or(format!("Missing locale"))?.to_string();
    details.size = out[2].table_or_error()?
        .display_content_at(0, 0, 1)
        .ok_or(format!("Missing size"))?.to_string();
    
    // details.encoding = self.conn.query_one("show server_encoding", &[]).unwrap().get::<_, String>(0);
    // Can also use extract (days from interval) or extract(hour from interval)
    
    details.uptime = out[3].table_or_error()?
        .display_content_at(0, 0, 1)
        .ok_or(format!("Missing uptime"))?.to_string();
    
    Ok(details)
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

/*fn split_on_type<'a>(s : &'a str) -> impl Iterator<Item=&'a str> +'a {

    let mut splits = Vec::new();


    "double precision"
    "timestamp with time zone"
    "timestamp without time zone"
    "time with time zone"
    "time without time zone"

    let mut ix =

    split.drain(..)
}*/

/* Alternatively, to search for funcs:
// This version is slower, and does not make use of pg_get_function_arguments.
/*let fn_query = format!(r#"
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
order by pg_proc.oid;"#, schema);*/

// Or even:
    SELECT routines.routine_name, parameters.data_type, parameters.ordinal_position
FROM information_schema.routines
    LEFT JOIN information_schema.parameters ON routines.specific_name=parameters.specific_name
WHERE routines.specific_schema='public'
ORDER BY routines.routine_name, parameters.ordinal_position;
*/

// Function query, that should be parametrized by $SCHEMA before execution.
const FN_QUERY : &'static str = r#"
select cast (pg_proc.proname as text),
   pg_get_function_identity_arguments(pg_proc.oid) as args,
   cast(pg_type.typname as text) as ret_typename
from pg_proc
left join pg_namespace on pg_proc.pronamespace = pg_namespace.oid
left join pg_type on pg_type.oid = pg_proc.prorettype
where pg_namespace.nspname like '$SCHEMA' and
    pg_namespace.nspname not in ('pg_catalog', 'information_schema')
order by proname;
"#;

// Retrieve schemata without parametrizations.
const SCHEMATA_QUERY : &'static str = r"select schema_name from information_schema.schemata;";

// Retrieve tables, without parametrizations.
const TBL_QUERY : &'static str = r#"select schemaname::text, tablename::text
    from pg_catalog.pg_tables
    where schemaname != 'pg_catalog' and schemaname != 'information_schema';"#;

// View query, that should be parametrized by $SCHEMA before execution.
const VIEW_QUERY : &'static str = r#"
select cast(table_schema as text) as schema_name,
       cast(table_name as text) as view_name
from information_schema.views
where table_schema like '$SCHEMA' and table_schema not in ('information_schema', 'pg_catalog')
order by schema_name, view_name;"#;

// Primary key query, that should be parametrized by $SCHEMA and $TABLE before execution.
const PK_QUERY : &'static str = r#"select
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
where tc.constraint_type = 'PRIMARY KEY' and tc.table_name='$TABLE' and tc.table_schema='$SCHEMA';
"#;

// Relationship query, that should be parametrized by table and schema.
const REL_QUERY : &'static str = r#"
select
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
where tc.constraint_type = 'FOREIGN KEY' and tc.table_name='$TABLE' and tc.table_schema='$SCHEMA';
"#;

const COLUMN_QUERY : &'static str = r#"select column_name::text, data_type::text
    from information_schema.columns where table_name = '$TABLE' and table_schema='$SCHEMA';"#;

// const PG_SCHEMA_STR : &'static str = concat!(
//    SCHEMATA_QUERY,
//    TBL_QUERY
// );

fn retrieve_functions(fn_info : &Table, schema : &str) -> Option<Vec<DBObject>> {
    let mut fns = Vec::new();
    let names = Vec::<String>::try_from(fn_info.get_column(0).unwrap().clone()).ok()?;
    let full_args = Vec::<String>::try_from(fn_info.get_column(1).unwrap().clone()).ok()?;
    let rets = Vec::<String>::try_from(fn_info.get_column(2).unwrap().clone()).ok()?;
    //let fn_iter = names.iter().zip(arg_names.iter().zip(arg_types.iter().zip(ret.iter())));
    //for (name, (arg_ns, (arg_tys, ret))) in fn_iter {
    for (name, (arg, ret)) in names.iter().zip(full_args.iter().zip(rets.iter())) {

        /*let args = match arg_tys {
            Some(Field::Json(serde_json::Value::Array(arg_names))) => {
                arg_names.iter().map(|arg| match arg {
                    serde_json::Value::String(s) => DBType::from_str(&s[..]).unwrap_or(DBType::Unknown),
                    _ => DBType::Unknown
                }).collect()
            },
            _ => {
                Vec::new()
            }
        }
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
        };*/
        let mut func_arg_names = Vec::new();
        let mut args = Vec::new();
        let mut split_arg = Vec::new();
        if !arg.is_empty() {
            for arg_str in arg.split(",") {
                split_arg = arg_str.split(" ").filter(|s| !s.is_empty() ).collect::<Vec<_>>();

                // println!("Func: '{}', Full: '{}'; Split: {:?}", name, arg, split_arg);

                // Some SQL types such as double precision and timestamp with time zone have spaces,
                // which is why the name is the first field, the type the second..last.
                match split_arg.len() {
                    1 => {
                        args.push(DBType::from_str(&split_arg[0].trim()).unwrap_or(DBType::Unknown));
                    },
                    2 => {
                        if split_arg[0].trim() == "double" && split_arg[1].trim() == "precision" {
                            args.push(DBType::F64);
                        } else {
                            func_arg_names.push(split_arg[0].to_string());
                            args.push(DBType::from_str(&split_arg[1].trim()).unwrap_or(DBType::Unknown));
                        }
                    },
                    3 => {
                        if split_arg[1].trim() == "double" && split_arg[2].trim() == "precision" {
                            func_arg_names.push(split_arg[0].to_string());
                            args.push(DBType::F64);
                        } else {
                            args.push(DBType::from_str(&arg_str[..]).unwrap_or(DBType::Unknown));
                        }
                    },
                    4 => {
                        // timestamp with time zone | timestamp without time zone will have 4 splits but no arg name
                        args.push(DBType::from_str(&arg_str[..]).unwrap_or(DBType::Unknown));
                    },
                    5 => {
                        // timestamp with time zone | timestamp without time zone will have 4 splits but and a type name
                        if split_arg[1].trim() == "time" || split_arg[1].trim() == "timestamp" {
                            func_arg_names.push(split_arg[0].to_string());
                            args.push(DBType::Time);
                        }
                    },
                    n => {
                        args.push(DBType::from_str(&arg_str[..]).unwrap_or(DBType::Unknown));
                    }
                }
            }
        } else {
            split_arg.clear();
        }

        let ret = match &ret[..] {
            "VOID" | "void" => None,
            _ => Some(DBType::from_str(ret).unwrap_or(DBType::Unknown))
        };

        let opt_func_arg_names = if func_arg_names.len() > 0 && func_arg_names.len() == args.len() {
            Some(func_arg_names)
        } else {
            None
        };
        fns.push(DBObject::Function { schema : schema.to_string(), name : name.clone(), args, arg_names : opt_func_arg_names, ret });
    }

    fns.sort_by(|a, b| {
        a.obj_name().cmp(&b.obj_name())
    });
    Some(fns)
}

/*// pg_proc.prokind codes: f = function; p = procedure; a = aggregate; w = window
// To retrieve source: case when pg_language.lanname = 'internal' then pg_proc.prosrc else pg_get_functiondef(pg_proc.oid) end as source
// -- pg_language.lanname not like 'internal' and
// Alternative query:
// select cast(cast(pg_proc.oid as regprocedure) as text) from pg_proc left join pg_namespace on
// pg_proc.pronamespace = pg_namespace.oid where pg_namespace.nspname like 'public';
// Then parse the resulting text (but it won't give the return type).
// TODO the generate_series seems to be slowing the query down, making the connection startup
// unreasonably slow. But removing it makes the arguments be unnested at the incorrect order.
fn get_postgres_functions(conn : &mut PostgresConnection, schema : &str) -> Option<Vec<DBObject>> {

    // Use this to collect names and types from first method.
    // let arg_types = (0..names.len()).map(|ix| fn_info.get_column(1).unwrap().at(ix) ).collect::<Vec<_>>();
    // let arg_names = (0..names.len()).map(|ix| fn_info.get_column(2).unwrap().at(ix) ).collect::<Vec<_>>();

    // This version is much faster (since it does not require subqueries),
    // but assumes pg_get_function_arguments function is
    // registered at catalog. Also, we must parse the names from types from the textual result.
    // This might be fine if the strategy shows to be stable across postgres versions.
    let fn_query = FN_QUERY.replace("$SCHEMA", schema);
    let ans = conn.try_run(&fn_query, &HashMap::new(), SafetyLock::default()) /*.map_err(|e| println!("{}", e) )*/ .ok()?;
    match ans.get(0)? {
        StatementOutput::Valid(_, fn_info) => {
            retrieve_functions(&fn_info)
        },
        StatementOutput::Invalid(msg, _) => { /*println!("{}", msg);*/ None },
        _ => None
    }
}*/

fn retrieve_schemata(table : &Table) -> Option<HashMap<String, Vec<String>>> {
    let mut schem_hash = HashMap::new();
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
}

/// Return HashMap of Schema->Tables
fn get_postgres_schemata(conn : &mut PostgresConnection) -> Result<HashMap<String, Vec<String>>, String> {

    let out = conn.query_async(&[AnyStatement::from_sql(SCHEMATA_QUERY).unwrap(), AnyStatement::from_sql(TBL_QUERY).unwrap()]);
    if let Some(schem_out) = out.get(0) {
        match schem_out {
            StatementOutput::Valid(_, schem_tbl) => {
                let schem_names = Vec::<String>::try_from(schem_tbl.get_column(0).unwrap().clone()).ok().unwrap();
                
                /*let mut schem_hash = HashMap::new();
                for s in schem_name.iter() {
                    if s.starts_with("pg") || &s[..] == "information_schema" {
                        continue;
                    }
                    schem_hash.insert(s, Vec::new());
                }*/
                
                // Retrieve schemata that have at least one table.
                let mut schemata = HashMap::new();
                if let Some(tbl_out) = out.get(1) {
                    match tbl_out {
                        StatementOutput::Valid(_, tbl) => {
                            schemata = retrieve_schemata(&tbl).unwrap();
                        },
                        StatementOutput::Invalid(e, _) => {
                            return Err(format!("{}", e));
                        },
                        _ => unimplemented!()
                    }
                } else {
                    return Err(format!("Missing table output"));
                }
                
                // Insert schemata without tables.
                for n in schem_names {
                    if n.starts_with("pg") || &n[..] == "information_schema" {
                        continue;
                    }
                    if schemata.get(&n).is_none() {
                        schemata.insert(n.to_string(), Vec::new());
                    }
                }
                Ok(schemata)
            },
            StatementOutput::Invalid(e, _) => {
                Err(e.to_string())
            },
            _ => {
                unimplemented!()
            }
        }
    } else {
        Err(format!("Missing schema information"))
    }

    /*match conn.client.query(SCHEMATA_QUERY, &[]) {
        Ok(rows) => {
            for s in rows.iter().map(|r| r.get::<_, String>(0) ) {
                if s.starts_with("pg") || &s[..] == "information_schema" {
                    continue;
                }
                schem_hash.insert(s, Vec::new());
            }
        },
        Err(_) => { return None; }
    }

    let tbl_query = String::from(TBL_QUERY);
    let ans = conn.try_run(tbl_query, &HashMap::new(), SafetyLock::default())
        /*.map_err(|e| println!("{}", e) )*/ .ok()?;
    let q_res = ans.get(0)?;
    match q_res {
        StatementOutput::Valid(_, table) => {
            retrieve_schemata(&table)
        },
        StatementOutput::Invalid(msg, _) => { println!("{}", msg); None },
        _ => None
    }*/
}

fn retrieve_views(view_info : &Table) -> Option<Vec<DBObject>> {
    let mut views = Vec::new();
    let schema_info = Vec::<String>::try_from(view_info.get_column(0).unwrap().clone()).ok()?;
    let name_info = Vec::<String>::try_from(view_info.get_column(1).unwrap().clone()).ok()?;
    for (schema, name) in schema_info.iter().zip(name_info.iter()) {
        views.push(DBObject::View { schema : schema.to_string(), name : name.clone() });
    }
    views.sort_by(|a, b| {
        a.obj_name().cmp(&b.obj_name())
    });
    Some(views)
}

/*fn get_postgres_views(conn : &mut PostgresConnection, schema : &str) -> Option<Vec<DBObject>> {
    let view_query = VIEW_QUERY.replace("$SCHEMA", schema);
    let ans = conn.try_run(&view_query, &HashMap::new(), SafetyLock::default()).ok()?;
    match ans.get(0)? {
        StatementOutput::Valid(_, view_info) => {
            retrieve_views(&view_info)
        },
        StatementOutput::Invalid(msg, _) => { None },
        _ => None
    }
}*/

fn retrieve_pks(col_info : &Table) -> Option<Vec<String>> {
    let cols = col_info.get_column(3)
        .and_then(|c| { let s : Option<Vec<String>> = c.clone().try_into().ok(); s })?;
    Some(cols)
}

/*fn get_postgres_pks(
    conn : &mut PostgresConnection,
    schema_name : &str,
    tbl_name : &str
) -> Option<Vec<String>> {
    let pk_query = PK_QUERY.replace("$TABLE", tbl_name).replace("$SCHEMA", schema_name);
    let ans = conn.try_run(pk_query, &HashMap::new(), SafetyLock::default()) /*.map_err(|e| println!("{}", e) )*/ .ok()?;
    if let Some(q_res) = ans.get(0) {
        match q_res {
            StatementOutput::Valid(_, col_info) => {
                retrieve_pks(&col_info)
            },

            // Will throw an error when there are no relations.
            StatementOutput::Invalid(msg, _) => { /*println!("{}", msg);*/ None },
            _ => None
        }
    } else {
        // println!("Database info query did not return any results");
        None
    }
}*/

fn retrieve_relations(col_info : &Table) -> Option<Vec<Relation>> {
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
}

/*/// Get foreign key relations for a given table.
fn get_postgres_relations(conn : &mut PostgresConnection, schema_name : &str, tbl_name : &str) -> Option<Vec<Relation>> {
    let rel_query = REL_QUERY.replace("$TABLE", tbl_name).replace("$SCHEMA", schema_name);
    let ans = conn.try_run(rel_query, &HashMap::new(), SafetyLock::default()) /*().map_err(|e| println!("{}", e) ) */ .ok()?;
    if let Some(q_res) = ans.get(0) {
        match q_res {
            StatementOutput::Valid(_, col_info) => {
                retrieve_relations(&col_info)
            },
            StatementOutput::Invalid(msg, _) => { /*println!("{}", msg);*/ None },
            _ => None
        }
    } else {
        // println!("Database info query did not return any results");
        None
    }
}*/

/*fn retrieve_cols(col_info : &Table) -> Option<DBObject> {
    let names = col_info.get_column(0)
        .and_then(|c| { let s : Option<Vec<String>> = c.clone().try_into().ok(); s })?;
    let col_types = col_info.get_column(1)
        .and_then(|c| { let s : Option<Vec<String>> = c.clone().try_into().ok(); s })?;
    let pks = get_postgres_pks(conn, schema_name, tbl_name).unwrap_or(Vec::new());
    let cols = crate::sql::pack_column_types(names, col_types, pks).ok()?;
    let rels = get_postgres_relations(conn, schema_name, tbl_name).unwrap_or(Vec::new());

    let obj = DBObject::Table{ schema : schema_name.to_string(), name : tbl_name.to_string(), cols, rels };
    Some(obj)
}*/

/*fn get_postgres_columns(conn : &mut PostgresConnection, schema_name : &str, tbl_name : &str) -> Option<DBObject> {
    let col_query = COLUMN_QUERY.replace("$TABLE", tbl_name).replace("$SCHEMA", schema_name);
    let ans = conn.try_run(col_query, &HashMap::new(), SafetyLock::default())
        /*.map_err(|e| println!("{}", e) )*/ .ok()?;
    if let Some(q_res) = ans.get(0) {
        match q_res {
            StatementOutput::Valid(_, col_info) => {
                retrieve_cols(&col_info)
            },
            StatementOutput::Invalid(msg, _) => { None },
            _ => None
        }
    } else {
        None
    }
}*/

/*/// Copies from the PostgreSQL server into a client
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
}*/

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

/*fn run_local_statement(
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
}*/

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

