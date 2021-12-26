use std::collections::HashMap;
use crate::sql::*;
use crate::sql::object::{DBObject, DBInfo};
use crate::sql::parsing::AnyStatement;
use sqlparser::ast::*;

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

    fn query(&mut self, q : &str, subs : &HashMap<String, String>) -> StatementOutput;

    fn exec(&mut self, stmt : &AnyStatement, subs : &HashMap<String, String>) -> StatementOutput;

    fn listen_at_channel(&mut self, channel : String);

    fn info(&mut self) -> Option<DBInfo>;

    /// It is important that every time this method is called,
    /// at least one query result is pushed into the queue, or else
    /// the GUI will be insensitive waiting for a response.
    fn try_run(
        &mut self,
        query_seq : String,
        subs : &HashMap<String, String>,
        parse : bool
    ) -> Result<Vec<StatementOutput>, String> {

        // Substitute $() (variable) and ${} (command) macros before parsing the SQL.
        // let (query_seq, copies) = Self::substitute_copies(query_seq)?;
        // println!("Captured copies: {:?}", copies);
        match parse {
            true => match crate::sql::parsing::partially_parse_sql(&query_seq, &subs) {
                Ok(stmts) => {
                    self.run_parsed_sql(stmts, &subs)
                },
                Err(e) => {
                    println!("Parsing error: {}", e);
                    self.run_unparsed_sql(query_seq, &subs)
                }
            },
            false => self.run_unparsed_sql(query_seq, &subs)
        }
    }

    fn run_parsed_sql(
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
                    unimplemented!()
                },
                AnyStatement::Raw(_, r, _) => {
                    panic!("Tried to run raw statement, but required parsed");
                }
            }
        }

        Ok(results)
    }

    /// Runs the informed query sequence without client-side parsing.
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
                    unimplemented!()
                },
                AnyStatement::Parsed(_, _) => {
                    return Err(format!("Tried to execute parsed statement (expected unparsed)"));
                }
            }
        }
        Ok(results)
    }


}


