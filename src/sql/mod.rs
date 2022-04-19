use std::fmt::Display;
use std::fmt;
use std::error::Error;
use monday::tables::table::*;
use std::path::PathBuf;
use crate::sql::object::{DBObject, DBType};
use std::convert::TryInto;
use std::collections::HashMap;
use regex::Regex;
use std::string::ToString;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::cell::RefCell;
use std::rc::Rc;
use std::mem;
use std::cmp::{PartialEq, Eq};
use std::ffi::OsStr;
use monday::tables::column::Column;
use itertools::Itertools;
use std::str::FromStr;
use either::Either;
use std::iter::Peekable;
use crate::sql::object::Relation;
use std::convert::TryFrom;
use monday::tables::field::Field;
use postgres::fallible_iterator::FallibleIterator;
use std::time::Duration;
use sqlparser::dialect::{PostgreSqlDialect, GenericDialect};
use sqlparser::ast::{Statement, Function, Select, Value, Expr, SetExpr, SelectItem, Ident, TableFactor, Join, JoinOperator, ObjectType};
use sqlparser::parser::{Parser, ParserError};
use sqlparser::dialect::keywords::Keyword;
use sqlparser::dialect;
use sqlparser::tokenizer::{Tokenizer, Token, Word, Whitespace};
use std::sync::{Arc, Mutex};
use crate::command::Executor;

// TODO sqlparser is not accepting creating views with distinct clause.

/// Database objects (schema, tables, columns).
pub mod object;

/// General SQL parsing.
pub mod parsing;

/// Locally-parsed and executed copy statements.
pub mod copy;

/// PostgreSQL notifications
pub mod notify;

// Wraps thread that listen to SQL commands.
// pub mod listener;

use object::*;

use parsing::*;

use copy::*;

use self::notify::*;

// use listener::*;

#[cfg(feature="arrowext")]
use datafusion::execution::context::ExecutionContext;

#[cfg(feature="arrowext")]
use datafusion::datasource::csv::{CsvFile, CsvReadOptions};

/// This enum represents non-standard SQL statements that are parsed and
/// executed at client-side by queries.  The DB engine only sees the
/// results of those statements (which usually boils down to a copy to/from,
/// if applicable; or to a change in the current execution environment).
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum LocalStatement {
    Decl(Declare),
    Exec(Execute),
    Copy(Copy)
}

/// Tries to parse a local statement starting from the current token. If
/// the first token does not initialize a local statement, return it. For exec
/// statements, it may take multiple tokens to define if it is local or remote,
/// in this case the failure will return the multiple iterated tokens.
/// If the iterator is done, return None.
pub fn local_statement_or_tokens<'a, I>(
    token_iter : &mut Peekable<I>
) -> Result<Option<Either<LocalStatement, Either<Token, Vec<Token>>>>, String>
where
    I : Iterator<Item=&'a Token>
{
    match token_iter.next() {
        Some(token) => {
            let ans = match token {
                Token::Word(w) => {
                    match w.keyword {
                        Keyword::COPY => {
                            Ok(Some(Either::Left(LocalStatement::Copy(parse_remaining_copy_tokens(token_iter)?))))
                        },
                        Keyword::EXEC | Keyword::EXECUTE => {

                            // Since we must advance the iterator more than once,
                            // stores the tokens in case local statement parsing
                            // fails.
                            let mut exec_tokens = Vec::new();

                            while let Some(tk) = token_iter.next() {

                                // Peek here because parse_remaining_run_tokens expect SingleQuotedString
                                // to be the first token.
                                match token_iter.peek() {
                                    Some(Token::SingleQuotedString(_)) => {
                                        let local = LocalStatement::Exec(parse_remaining_run_tokens(token_iter)?);
                                        return Ok(Some(Either::Left(local)));
                                    },

                                    // While peeked token is whitespace, there is a chance it might be a local or remote statement.
                                    Some(Token::Whitespace(_)) => {
                                        exec_tokens.push(token.clone());
                                    },

                                    // Local statement parsing failed. Returns tokens up to this point.
                                    _ => {
                                        exec_tokens.push(token.clone());
                                        break;
                                    }
                                }
                            }

                            Ok(Some(Either::Right(Either::Right(exec_tokens))))

                        },
                        Keyword::DECLARE => {
                            Ok(Some(Either::Left(LocalStatement::Decl(parse_remaining_declare_tokens(token_iter)?))))
                        },
                        _ => Ok(Some(Either::Right(Either::Left(token.clone()))))
                    }
                },
                other => Ok(Some(Either::Right(Either::Left(other.clone()))))
            };
            match ans {
                Ok(Some(Either::Left(_))) => {
                    if !close_valid_statement(token_iter) {
                        return Err(format!("Could not close local statement"));
                    }
                },
                _ => { }
            }
            ans
        },
        None => Ok(None)
    }
}

#[derive(Debug)]
pub struct DecodingError {
    msg : &'static str
}

impl Display for DecodingError {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({})", self.msg)
    }
}

impl Error for DecodingError {

}

impl DecodingError {

    pub fn new(msg : &'static str) -> Box<Self> {
        Box::new(DecodingError{ msg })
    }

}

// Carries a result (arranged over columns)
#[derive(Debug, Clone)]
pub enum StatementOutput {

    // Returns a valid executed query with its table represented over columns.
    Valid(String, Table),

    // Returns the result of a successful insert/update/delete statement.
    Statement(String),

    // Returns the result of a successful create/drop/alter statement.
    Modification(String),

    // Returns a query/statement rejected by the database engine (if true),
    // or client (if false), carrying its error message.
    Invalid(String, bool),

    // Resulting from a local command invocation
    Empty

}

pub fn condense_errors(stmts : &[StatementOutput]) -> Option<String> {
    let mut errs : Vec<String> = stmts.iter().filter_map(|stmt| {
        match stmt {
            StatementOutput::Invalid(msg, _) => {
                Some(msg.clone())
            },
            _ => None
        }
    }).collect();
    match errs.len() {
        0 => None,
        1 => Some(errs.remove(0)),
        2 => Some(format!("{} (+1 previous error)", errs.last().unwrap())),
        n => Some(format!("{} (+{} previous errors)", errs.last().unwrap(), n-1))
    }
}

pub fn condense_statement_outputs(stmts : &[StatementOutput]) -> Option<String> {
    let mut msgs : Vec<String> = stmts.iter().filter_map(|stmt| {
        match stmt {
            StatementOutput::Statement(msg) | StatementOutput::Modification(msg) => {
                Some(msg.clone())
            },
            _ => None
        }
    }).collect();
    match msgs.len() {
        0 => None,
        1 => Some(msgs.remove(0)),
        2 => Some(format!("{} (+1 previous change)", msgs.last().unwrap())),
        n => Some(format!("{} (+{} previous changes)", msgs.last().unwrap(), n-1))
    }
}

pub fn sql2table(result : Result<Vec<Statement>, String>) -> String {
    format!("{:?}", result)
}

pub fn make_query(query : &str) -> String {
    sql2table(crate::sql::parse_sql(query, &HashMap::new()))
}

/*pub enum SqlEngine {
    Inactive,

    Local{conn : rusqlite::Connection },

    // Channel carries channel name, filter, and whether it is active.
    PostgreSql{conn_str : String, conn : postgres::Client, exec : Arc<Mutex<(Executor, String)>>, channel : Option<(String, String, bool)> },

    Sqlite3{path : Option<PathBuf>, conn : rusqlite::Connection},

    #[cfg(feature="arrowext")]
    Arrow{ ctx : ExecutionContext }
}*/

pub fn build_statement_result(any_stmt : &AnyStatement, n : usize) -> StatementOutput {
    match any_stmt {
        AnyStatement::Parsed(stmt, _) => match stmt {
            Statement::CreateView{..} => StatementOutput::Modification(format!("Create view")),
            Statement::CreateTable{..} | Statement::CreateVirtualTable{..} => {
                StatementOutput::Modification(format!("Create table"))
            },

            // Not implemented yet
            // Statement::CreateFunction{ .. } => StatementOutput::Modification(format!("Create function")),

            Statement::CreateIndex{..} => StatementOutput::Modification(format!("Create index")),
            Statement::CreateSchema{..} => StatementOutput::Modification(format!("Create schema")),
            Statement::AlterTable{..} => StatementOutput::Modification(format!("Alter table")),
            Statement::Drop{ object_type, ..} => {
                let drop_msg = match object_type {
                    ObjectType::Table => "Drop table",
                    ObjectType::View => "Drop view",
                    ObjectType::Index => "Drop index",
                    ObjectType::Schema => "Drop schema",
                };
                StatementOutput::Modification(format!("{}", drop_msg))
            },
            Statement::Truncate { .. } => {
                StatementOutput::Statement(format!("Truncate"))
            },
            Statement::Copy{..} => StatementOutput::Modification(format!("Copy")),
            Statement::Insert { .. } => {
                StatementOutput::Statement(format!("{} row(s) inserted", n))
            },
            Statement::Update { .. } => {
                StatementOutput::Statement(format!("{} row(s) updated", n))
            },
            Statement::Delete { .. } => {
                StatementOutput::Statement(format!("{} row(s) deleted", n))
            },
            _ => StatementOutput::Statement(format!("Statement executed"))
        },
        AnyStatement::Raw(_, s, _) => {

            // Process statements that make sense to PostgreSQL but for some reason were not parsed by sqlparser.

            let mut prefix : (Option<String>, Option<String>, Option<String>) = (None, None, None);
            let mut split = s.split_whitespace();
            prefix.0 = split.next().map(|s| s.trim().to_lowercase().to_string() );
            prefix.1 = split.next().map(|s| s.trim().to_lowercase().to_string() );
            prefix.2 = split.next().map(|s| s.trim().to_lowercase().to_string() );

            if let (Some(p1), Some(p2), Some(p3)) = prefix {
                match (&p1[..], &p2[..], &p3[..]) {
                    ("create", "table", _) | ("create", "virtual", "table") | ("create", "temporary", "table") => {
                        return StatementOutput::Modification(format!("Create table"));
                    },
                    ("drop", "table", _) => {
                        return StatementOutput::Modification(format!("Drop table"));
                    },
                    ("alter", "table", _) => {
                        return StatementOutput::Modification(format!("Alter table"));
                    },
                    ("create", "schema", _) => {
                        return StatementOutput::Modification(format!("Create schema"));
                    },
                    ("create", "view", _) => {
                        return StatementOutput::Modification(format!("Create view"));
                    },
                    ("create", "procedure", _) => {
                        return StatementOutput::Modification(format!("Create procedure"));
                    },
                    ("create",  "function", _) => {
                        return StatementOutput::Modification(format!("Create function"));
                    },
                    ("drop", "function", _) => {
                        return StatementOutput::Modification(format!("Drop function"));
                    },
                    ("drop", "procedure", _) => {
                        return StatementOutput::Modification(format!("Drop procedure"));
                    },
                    ("drop", "view", _) => {
                        return StatementOutput::Modification(format!("Drop view"));
                    },
                    ("insert", _, _) => {
                        return StatementOutput::Statement(format!("{} row(s) inserted", n));
                    },
                    ("update", _, _) => {
                        return StatementOutput::Statement(format!("{} row(s) updated", n));
                    },
                    ("delete", _, _) => {
                        return StatementOutput::Statement(format!("{} row(s) deleted", n));
                    },
                    _ => { }
                }
            }

            StatementOutput::Modification(format!("Statement executed"))
        },
        AnyStatement::Local(local) => {
            match local {
                LocalStatement::Copy(_) => {
                    StatementOutput::Modification(format!("Copy"))
                },
                LocalStatement::Decl(_) | LocalStatement::Exec(_) => {
                    StatementOutput::Empty
                }
            }
        }
    }
}

pub fn append_relation(t_expr : &TableFactor, out : &mut String) {
    match t_expr {
        TableFactor::Table{ name, .. } => {
            if !out.is_empty() {
                *out += " : ";
            }
            *out += &name.to_string();
        },
        TableFactor::Derived{ .. } | TableFactor::NestedJoin(_) | TableFactor::TableFunction{ .. } => {

        }
    }
}

pub fn table_name_from_sql(sql : &str) -> Option<(String, String)> {
    let dialect = PostgreSqlDialect{};
    let ast = Parser::parse_sql(&dialect, sql).ok()?;
    if let Some(Statement::Query(q)) = ast.get(0) {
        if let SetExpr::Select(s) = &q.body {
            let mut from_names = String::new();
            let mut relation = String::new();
            for t_expr in s.from.iter() {
                append_relation(&t_expr.relation, &mut from_names);
                for join in t_expr.joins.iter() {
                    append_relation(&join.relation, &mut from_names);
                    if relation.is_empty() {
                        match join.join_operator {
                            JoinOperator::Inner(_) => relation += "inner",
                            JoinOperator::LeftOuter(_) => relation += "left",
                            JoinOperator::RightOuter(_) => relation += "right",
                            JoinOperator::FullOuter(_) => relation += "full",
                            _ => { }
                        }
                    }
                }
            }
            // println!("Name: {:?}", from_names);
            // println!("Relation: {:?}", relation);
            Some((from_names, relation))
        } else {
            None
        }
    } else {
        None
    }
}

/// col_types might be an empty string here because sqlite3 does not require
/// that the types for all columns are declared. We treat the type as unknown in this case.
pub fn pack_column_types(
    col_names : Vec<String>,
    col_types : Vec<String>,
    pks : Vec<String>
) -> Option<Vec<(String, DBType, bool)>> {
    if col_names.len() != col_types.len() {
        println!("Column names different than column types length");
        return None;
    }
    let mut types = Vec::new();
    for ty in col_types {
        if let Ok(t) = ty.parse::<DBType>() {
            types.push(t);
        } else {
            println!("Unable to parse type: {:?}", ty);
            return None;
        }
    }
    let cols : Vec<(String, DBType, bool)> = col_names.iter()
        .zip(types.iter())
        .map(|(s1, s2)| (s1.clone(), s2.clone(), pks.iter().find(|pk| &pk[..] == &s1[..]).is_some() ))
        .collect();
    Some(cols)
}

pub fn wait_command_execution(call : &str, exec : &Arc<Mutex<(Executor, String)>>) -> Result<String, String> {
    let mut executor = exec.lock().map_err(|e| format!("{}", e))?;
    let input = mem::take(&mut executor.1);
    if input.len() == 0 {
        executor.0.queue_command(call.to_string(), None);
    } else {
        executor.0.queue_command(call.to_string(), Some(input));
    }
    let mut content = String::new();
    executor.0.on_command_result(|out| {
        if out.status {
            if out.txt.len() > 0 {
                content = out.txt;
                Ok(())
            } else {
                Err(format!("Program standard output is empty"))
            }
        } else {
            Err(format!("Command execution failed: {}", out.txt))
        }
    })?;
    // println!("Captured into stdout: {}", content);
    Ok(content)
}

/*pub fn try_run_all(&mut self) {
    if let Ok(mut maybe_conn) = self.rc_conn.clone().try_borrow_mut() {
        //let maybe_conn = *maybe_conn;
        if let Some(mut c) = maybe_conn.as_mut() {
            for q in self.queries.iter_mut() {
                q.run(&c);
                if let Some(msg) = &q.err_msg {
                    println!("{}", msg);
                }
            }
        }
    }
}

pub fn try_run_some(&mut self) {
    if let Ok(mut maybe_conn) = self.rc_conn.clone().try_borrow_mut() {
        if let Some(mut c) = maybe_conn.as_mut() {
            println!("valid queries : {:?}", self.valid_queries);
            for i in self.valid_queries.iter() {
                if let Some(mut q) = self.queries.get_mut(*i) {
                    q.run(&c);
                    if let Some(msg) = &q.err_msg {
                        println!("{}", msg);
                    }
                }
            }
        } else {
            println!("No connections available");
        }
    }
}*/

/*pub fn mark_all_valid(&mut self) {
    self.valid_queries = (0..self.queries.len()).collect();
}*/
/*pub fn get_valid_queries(&self) -> Vec<&PostgreQuery> {
    let mut queries : Vec<&PostgreQuery> = Vec::new();
    //for q in self.queries.iter() {
    //if q.err_msg.is_none() {
    //    valid_queries.push(&q);
    //}
    //}
    for i in self.valid_queries.iter() {
        if let Some(q) = self.queries.get(*i) {
            queries.push(q);
        }
    }
    queries
}*/

/*pub fn get_valid_queries_code(&self) -> Vec<String> {
    let queries = self.get_valid_queries();
    queries.iter().map(|q|{ q.query.clone() }).collect()
}

pub fn get_all_queries_code(&self) -> Vec<&str> {
    self.queries.iter().map(|q| { q.query.as_str() }).collect()
}

pub fn get_subset_valid_queries(
    &self,
    idx : Vec<usize>)
-> Vec<&PostgreQuery> {
    let queries = self.get_valid_queries().clone();
    let mut keep_queries = Vec::new();
    for i in idx {
        keep_queries.push(queries[i]);
    }
    keep_queries
}*/


/*fn run_expression(
    mut table : String,
    name : Option<String>,
    mut expr : String,
) -> Result<String, String> {

    /*if let Some(n) = name {
        let prefix = n + " = X; ";
        expr = prefix + &expr[..];
    }
    let mut arg_expr = String::from("-e '");
    arg_expr = arg_expr + &expr[..] + "'";
    let spawned_cmd = Command::new("r")
        .stdin(Stdio::piped());

    spawned_cmd.stdin.unwrap()
        .arg("-d")  // Evaluate stdin as CSV input
        .arg("-p")  // Output last evaluated expression
        .arg(&arg_expr[..])
        .spawn();
    println!("Command : {:?}", spawned_cmd);

    // output.status
    // output.stdout
    // output.stderr

    match spawned_cmd {
        Ok(cmd) => {
            let mut cmd_stdin = cmd.stdin.unwrap();
            println!("STDIN : {:?}", table);
            let mut writer = BufWriter::new(&mut cmd_stdin);
            if let Err(e) = writer.write_all(&mut table.as_bytes()) {
                println!("Error : {}", e);
                return Err(format!("{}", e));
            }
            match cmd.stdout {
                Some(mut out) => {
                    let mut content = Vec::new();
                    if let Ok(_) = out.read(&mut content) {
                        if let Ok(utf8) = String::from_utf8(content) {
                            Ok(utf8)
                        } else {
                            Err("Could not parse result as UTF-8".into())
                        }
                    } else {
                        Err("Could not read result into string".into())
                    }
                },
                None => Err("Could not recover stdout hande".into())
            }
        },
        Err(e) => { return Err(e.to_string()); }
    }*/
    // Err("Unimplemented".into())

    Ok(make_query(&expr[..]))
}*/

// TODO maybe return Cow here?
pub fn substitute_if_required(q : &str, subs : &HashMap<String, String>) -> String {
    let mut txt = q.to_string();
    if !subs.is_empty() {
        for (k, v) in subs.iter() {
            txt = txt.replace(k, v);
        }
    }
    txt
}

pub fn build_error_with_stmt(msg : &str, query : &str) -> String {
    let compact_query = query.replace("\t", " ").replace("\n", " ");
    let query = compact_query.trim();
    let q = if query.len() > 60 {
        &query[0..60]
    } else {
        &query[..]
    };
    let ellipsis = if query.len() > 60 { "..." } else { "" };
    format!("<b>Error</b> {}\n<b>Statement</b> {}{}", msg, q, ellipsis)
}

pub fn parse_sql(sql : &str, subs : &HashMap<String, String>) -> Result<Vec<Statement>, String> {
    let sql = substitute_if_required(sql, subs);
    //let dialect = PostgreSqlDialect {};
    let dialect = PostgreSqlDialect {};
    Parser::parse_sql(&dialect, &sql[..])
        .map_err(|e| {
            match e {
                ParserError::TokenizerError(s) => s,
                ParserError::ParserError(s) => s
            }
        })
}

