use std::collections::HashMap;
use crate::sql::*;
use crate::sql::object::{DBObject, DBInfo};
use crate::sql::parsing::{AnyStatement, SQLError};
use sqlparser::ast::*;
use crate::tables::table::Table;
use crate::client::ConnectionInfo;
use crate::client::ConnConfig;
use crate::sql::SafetyLock;
use std::error::Error;

/*
The client must parse the SQL file because it needs to dispatch statements to either the
Client::exec (if statement isn't a query) or Client::query (if statement is a query). While
it is not a hard driver error to swap those, calling exec with queries wouldn't return any
results. 

But the issue is that sqlparser does not understand the full PostgreSQL dialect, so
we end up with the situation that a few PostgreSQL statements aren't supported by Queries.
Perhaps this will change in the future if the parser achieves feature parity with the
server parser.

Another reason for parsing SQL is that we must know what the columns are, even if
the server does not return any results. Since the postgres driver binds column information
to the rows (rather than the iterable returned by query or query_raw), when there are zero
rows we want to show an empty table to the user, but with the column names.

Another reason for parsing SQL is to inform the user of destructive actions, and possibly
block them.

Another reason for parsing SQL is to determine if a table/view/function was created/dropped,
so that the SchemaTree can be updated.

*/

mod pg;

pub use pg::*;

mod sqlite;

pub use sqlite::*;

// mod arrow;

// pub use arrow::*;

pub trait Connection
where
    Self : Send
{

    fn configure(&mut self, cfg : ConnConfig);

    fn query(&mut self, q : &str, subs : &HashMap<String, String>) -> StatementOutput;

    fn exec(&mut self, stmt : &AnyStatement, subs : &HashMap<String, String>) -> StatementOutput;
    
    fn query_async(&mut self, stmts : &[AnyStatement]) -> Vec<StatementOutput>;
    
    fn exec_transaction(&mut self, stmt : &AnyStatement) -> StatementOutput;

    fn listen_at_channel(&mut self, channel : String);

    fn conn_info(&self) -> ConnectionInfo;

    fn db_info(&mut self) -> Result<DBInfo, Box<dyn Error>>;

    fn import(
        &mut self,
        tbl : &mut Table,
        dst : &str,
        cols : &[String],
    ) -> Result<usize, String>;

    /// It is important that every time this method is called,
    /// at least one query result is pushed into the queue, or else
    /// the GUI will be insensitive waiting for a response.
    fn try_run(
        &mut self,
        query_seq : String,
        subs : &HashMap<String, String>,
        lock : SafetyLock
    ) -> Result<Vec<StatementOutput>, String> {

        // Substitute $() (variable) and ${} (command) macros before parsing the SQL.
        // let (query_seq, copies) = Self::substitute_copies(query_seq)?;
        // println!("Captured copies: {:?}", copies);
        /*match parse {
            // true => match crate::sql::parsing::partially_parse_sql(&query_seq, &subs) {
                Ok(stmts) => {
                    self.run_parsed_sql(stmts, &subs)
                },
                Err(SQLError::Lexing(err)) | Err(SQLError::Parsing(err)) | Err(SQLError::Unsupported(err)) => {
                    Err(err)
                },
                /*Err(SQLError::Lexing(err)) => {
                    Err(err)
                },
                Err(SQLError::Parsing(err)) => {
                    self.run_unparsed_sql(query_seq, &subs)
                }*/
            },
            false => {
                self.run_unparsed_sql(query_seq, &subs)
            }
        }*/
        
        match crate::sql::parsing::fully_parse_sql(&query_seq) {
            Ok(stmts) => {
                
                if stmts.len() == 0 {
                    return Err(String::from("Empty statement sequence"));
                }
                
                // If all statements are queries, perform asynchronous execution.
                let all_queries = stmts.iter().all(|stmt| {
                    match stmt { 
                        AnyStatement::Parsed(stmt, _) => crate::sql::is_like_query(&stmt),
                        _ => false 
                    }
                } );
                if all_queries {
                    return Ok(self.query_async(&stmts[..]));
                }
                
                // If sequence has at least one non-query statement, default to synchronous exection.
                let mut results = Vec::new();
                for any_stmt in stmts {
                    match any_stmt {
                        AnyStatement::Parsed(stmt, s) => match stmt {
                            Statement::Query(q) => {
                                results.push(self.query(&s, &subs));
                            },
                            stmt => {
                                lock.accepts(&stmt)?;
                                results.push(self.exec(&AnyStatement::Parsed(stmt.clone(), format!("{}", s)), &subs));
                            }
                        },
                        AnyStatement::ParsedTransaction(stmts, s) => {
                            for stmt in &stmts {
                                lock.accepts(&stmt)?;
                            }
                            results.push(self.exec_transaction(&AnyStatement::ParsedTransaction(stmts.clone(), format!("{}", s))));
                        },
                        AnyStatement::Local(local) => {
                            // Self::run_local_statement(&local, conn, exec, &mut results)?;
                            return Err(String::from("Unsupported statement"));
                        },
                        AnyStatement::Raw(stmt_tokens, stmt_string, is_select) => {
                            if is_select {
                                results.push(self.query(&format!("{}", stmt_string), &subs));
                            } else {
                                results.push(self.exec(&AnyStatement::Raw(stmt_tokens, format!("{}", stmt_string), is_select), &subs));
                            }
                        }
                    }
                }
                
                Ok(results)
            },
            Err(SQLError::Lexing(err)) | Err(SQLError::Parsing(err)) | Err(SQLError::Unsupported(err)) => {
                Err(err)
            }
        }
    }

    /*fn run_parsed_sql(
        &mut self,
        stmts : Vec<AnyStatement>,
        subs : &HashMap<String, String>
    ) -> Result<Vec<StatementOutput>, String> {
        // let stmts = crate::sql::parsing::filter_repeated_queries(stmts);
        let mut results = Vec::new();
        if stmts.len() == 0 {
            return Err(String::from("Empty query sequence"));
        }

        // Copies are parsed and executed at client-side. It is important to
        // give just the copy feedback when we have only copies, but we give
        // a statement feedback otherwise.
        let mut all_copies = stmts.iter().all(|stmt| match stmt {
            AnyStatement::Local(LocalStatement::Copy(_)) => true,
            _ => false
        });

        for any_stmt in stmts {
            match any_stmt {
                AnyStatement::Parsed(stmt, s) => match stmt {
                    Statement::Query(q) => {
                        results.push(self.query(&s, &subs));
                    },
                    stmt => {
                        results.push(self.exec(&AnyStatement::Parsed(stmt.clone(), format!("{}", s)), &subs));
                    }
                },
                AnyStatement::Local(local) => {
                    // Self::run_local_statement(&local, conn, exec, &mut results)?;
                    return Err(String::from("Unsupported statement: COPY"));
                },
                AnyStatement::Raw(_, r, _) => {
                    return Err(String::from("Tried to run raw statement, but required parsed"));
                }
            }
        }

        Ok(results)
    }

    /// Runs the informed query sequence without client-side parsing (Just tokenization).
    fn run_unparsed_sql(
        &mut self,
        query_seq : String,
        subs : &HashMap<String, String>
    ) -> Result<Vec<StatementOutput>, String> {
        let stmts = crate::sql::parsing::split_unparsed_statements(query_seq)
            // .map(|stmts| filter_repeated_queries(stmts) )
            .map_err(|e| format!("{}", e) )?;
        let mut results = Vec::new();
        for s in stmts {
            match s {
                AnyStatement::Raw(stmt_tokens, stmt_string, is_select) => {
                    if is_select {
                        results.push(self.query(&format!("{}", stmt_string), &subs));
                    } else {
                        results.push(self.exec(&AnyStatement::Raw(stmt_tokens, format!("{}", stmt_string), is_select), &subs));
                    }
                },
                AnyStatement::Local(local) => {
                    // Self::run_local_statement(&local, conn, exec, &mut results)?;
                    // unimplemented!()
                    return Err(String::from("Unsupported statement: COPY"));
                },
                AnyStatement::Parsed(_, _) => {
                    return Err(format!("Tried to execute parsed statement (expected unparsed)"));
                }
            }
        }
        Ok(results)
    }*/


}


