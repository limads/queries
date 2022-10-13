/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use crate::sql::{*, object::*};
use std::error::Error;
use crate::tables::table::{Table};
use crate::sql::object::{DBObject, DBType, DBInfo};
use crate::sql::parsing::AnyStatement;
use super::Connection;
use std::collections::HashMap;
use std::fs::{self};
use std::convert::{TryFrom, TryInto};
use std::str::FromStr;
use tokio_postgres::Client;
use crate::client::ConnectionInfo;
use crate::client::{ConnURI, ConnConfig};
use sqlparser::ast::Statement;
use futures::future;
use std::ops::Range;

pub struct PostgresConnection {

    info : ConnectionInfo,

    client : tokio_postgres::Client,
    
    rt : Option<tokio::runtime::Runtime>,

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
        }
    } else {
        if crate::client::is_local(&uri.info)  == Some(true) {
            // Only connect without SSL/TLS when the client is local.
            match tokio_postgres::connect(&uri.uri[..], tokio_postgres::NoTls{ }).await {
                Ok((cli, conn)) => {
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
        let client = rt.block_on(async {
            connect(&rt, &uri).await
        })?;
        Ok(Self {
            info : uri.info,
            rt : Some(rt),
            client,
        })
    }

}

fn build_table(rows : &[tokio_postgres::Row], query : &str) -> StatementOutput {
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
            
            StatementOutput::Valid(query.to_string(), tbl)
            
        },
        Err(e) => StatementOutput::Invalid(crate::sql::build_error_with_stmt(&e, &query[..]), false)
    }
}

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
            _other => {
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

    fn listen_at_channel(&mut self, _channel : String) {

    }

    fn query(&mut self, query : &str, _subs : &HashMap<String, String>) -> StatementOutput {
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
                        Ok(tr) => {
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
                                    _other_stmt => {
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
    
    fn exec(&mut self, stmt : &AnyStatement, _subs : &HashMap<String, String>) -> StatementOutput {

        self.rt.as_ref().unwrap().block_on(async {
            let ans = match stmt {
                AnyStatement::Parsed(_, s) | AnyStatement::ParsedTransaction(_, s) => {
                    self.client.execute(&s[..], &[]).await
                },
                AnyStatement::Raw(_, s, _) => {
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

    }

    fn import(
        &mut self,
        tbl : &mut Table,
        dst : &str,
    ) -> Result<usize, String> {
    
        let cols = tbl.names();
        
        let sql = tbl.sql_table_insertion(&dst, &cols)?;
        
        let stmt = AnyStatement::from_sql(&sql)
            .ok_or(String::from("Invalid insertion SQL"))?;
        let out = self.exec(&stmt, &HashMap::new());
        match out {
            StatementOutput::Statement(_) => {
                Ok(tbl.nrows())
            },
            StatementOutput::Invalid(err, _) => {
                Err(err)
            },
            _ => {
                Err(String::from("Invalid insertion output"))
            }
        }
        
        // TODO use copy protocol instead.
        /*self.rt.as_ref().unwrap().block_on(async {
            let client = &mut self.client;
            let copy_stmt = match cols.len() {
                0 => format!("COPY {} FROM stdin with csv header quote '\"';", dst),
                n => {
                    /*let mut cols_agg = String::new();
                    for i in 0..n {
                        cols_agg += &cols[n];
                        if i <= n-1 {
                            cols_agg += ",";
                        }
                    }
                    format!("COPY {} ({}) FROM stdin with csv header quote '\"';", dst, cols_agg)*/
                    return Err(String::from("Copying from just a few columns is unsupported"))
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

            /*let mut writer = client.copy_in(&copy_stmt[..]).await
                .map_err(|e| format!("{}", e) )?;
            let tbl_content = crate::tables::table::full_csv_display(tbl, cols.into());
            writer.write_all(tbl_content.as_bytes())
                .map_err(|e| format!("Copy from stdin error: {}", e) )?;
            writer.finish()
                .map_err(|e| format!("Copy from stdin error: {}", e) )?;
            Ok(tbl.shape().0)*/
        })*/
    }


}

const SERVER_VERSION_QUERY : &'static str = "show server_version";

const COLLATION_QUERY : &'static str = "show lc_collate";

const SIZE_QUERY : &'static str = r#"
    select pg_size_pretty(pg_database_size('$DBNAME'));
"#;

const UPTIME_QUERY : &'static str = r#"
with uptime as (select current_timestamp - pg_postmaster_start_time() as uptime)
select cast(extract(day from uptime) as integer) || 'd ' ||
    cast(extract(hour from uptime) as integer) || 'h ' ||
    cast(extract(minute from uptime) as integer) || 'm'
from uptime;
"#;

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
    
    details.uptime = out[3].table_or_error()?
        .display_content_at(0, 0, 1)
        .ok_or(format!("Missing uptime"))?.to_string();
    
    Ok(details)
}

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

fn retrieve_functions(fn_info : &Table, schema : &str) -> Option<Vec<DBObject>> {
    let mut fns = Vec::new();
    let names = Vec::<String>::try_from(fn_info.get_column(0).unwrap().clone()).ok()?;
    let full_args = Vec::<String>::try_from(fn_info.get_column(1).unwrap().clone()).ok()?;
    let rets = Vec::<String>::try_from(fn_info.get_column(2).unwrap().clone()).ok()?;
    for (name, (arg, ret)) in names.iter().zip(full_args.iter().zip(rets.iter())) {

        let mut func_arg_names = Vec::new();
        let mut args = Vec::new();
        let mut split_arg = Vec::new();
        if !arg.is_empty() {
            for arg_str in arg.split(",") {
                split_arg = arg_str.split(" ").filter(|s| !s.is_empty() ).collect::<Vec<_>>();

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
                    _n => {
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
            eprintln!("Could not load table names to String vector");
            None
        }
    } else {
        eprintln!("Could not load schema column to String vector");
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
                
                // Retrieve schemata that have at least one table.
                let mut schemata;
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

fn retrieve_pks(col_info : &Table) -> Option<Vec<String>> {
    let cols = col_info.get_column(3)
        .and_then(|c| { let s : Option<Vec<String>> = c.clone().try_into().ok(); s })?;
    Some(cols)
}

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
    Some(rels)
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

