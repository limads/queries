/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use std::fmt::Display;
use std::fmt;
use std::error::Error;
use crate::tables::table::*;
use crate::sql::object::{DBType};
use std::collections::HashMap;
use std::string::ToString;
use std::cmp::{PartialEq, Eq};
use std::str::FromStr;
use either::Either;
use std::iter::Peekable;
use sqlparser::dialect::{PostgreSqlDialect};
use sqlparser::ast::{Statement, SetExpr, TableFactor, JoinOperator, ObjectType};
use sqlparser::parser::{Parser, ParserError};
use sqlparser::dialect::keywords::Keyword;
use sqlparser::tokenizer::{Token};

pub fn is_like_query(s : &Statement) -> bool {
    match s {
        Statement::Query(_) | Statement::ShowCreate{ .. } | Statement::ShowTables{ .. } | 
        Statement::ShowColumns{ .. } | Statement::ShowVariable{ .. } |
        Statement::ShowCollation{ .. } | Statement::ShowVariables{ .. } | Statement::Analyze { .. } | 
        Statement::Explain { .. } | Statement::ExplainTable{ .. } => {
            true
        },
        _ => {
            false
        }
    }
}

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



use parsing::*;

use copy::*;



// use listener::*;

#[derive(Debug, Clone, Copy, Default)]
pub struct SafetyLock {
    pub accept_ddl : bool,
    pub accept_dml : bool
}

fn safety_msg(stmt : &str) -> Result<(), String> {
    Err(format!("Cannot execute {} statement (disabled at settings)", stmt))
}

impl SafetyLock {

    pub fn accepts(&self, stmt : &Statement) -> Result<(), String> {
        match (stmt, self.accept_dml) {
            (Statement::Delete { .. }, false) => {
                safety_msg("delete")
            },
            (Statement::Update { .. }, false) => {
                safety_msg("update")
            },
            (other, _) => match (other, self.accept_ddl) { 
                (Statement::Truncate { .. }, false) => {
                    safety_msg("truncate")
                },
                (Statement::Drop { .. }, false) => {
                    safety_msg("drop")
                },
                (Statement::AlterTable { .. }, false) => {
                    safety_msg("alter")
                },
                _ => {
                    Ok(())
                }
            }
        }
    }
    
}

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

                            while let Some(_tk) = token_iter.next() {

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

impl StatementOutput {

    pub fn table(&self) -> Option<&Table> {
        match self {
            StatementOutput::Valid(_, tbl) => Some(&tbl),
            _ => None
        }
    }
    
    pub fn error(&self) -> Result<(), Box<dyn Error>> {
        match self {
            StatementOutput::Invalid(msg, _) => Err(msg.clone().into()),
            _ => Ok(())
        }
    }
    
    pub fn table_or_error(&self) -> Result<&Table, Box<dyn Error>> {
        match self.table() {
            Some(tbl) => Ok(tbl),
            _ => match self.error() {
                Err(e) => Err(e),
                _ => {
                    Err(String::from("Non-query statement").into())
                }
            }
        }
    }
    
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

pub fn build_statement_result(any_stmt : &AnyStatement, n : usize) -> StatementOutput {
    match any_stmt {
        AnyStatement::Parsed(stmt, _) => match stmt {
            Statement::CreateView{..} => StatementOutput::Modification(format!("Create view")),
            Statement::CreateTable{..} | Statement::CreateVirtualTable{..} => {
                StatementOutput::Modification(format!("Create table"))
            },

            Statement::CreateFunction{ .. } => {
                StatementOutput::Modification(format!("Create function"))
            },

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
        AnyStatement::ParsedTransaction(stmts, _) => {
            StatementOutput::Statement(format!("Transaction executed ({} statements, {} rows modified)", stmts.len(), n))
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
        TableFactor::Derived{ .. } | TableFactor::NestedJoin{ .. } | TableFactor::TableFunction{ .. } | TableFactor::UNNEST { .. } => {

        }
    }
}

pub fn table_name_from_sql(sql : &str) -> Option<(String, String)> {
    let dialect = PostgreSqlDialect{};
    let ast = Parser::parse_sql(&dialect, sql).ok()?;
    if let Some(Statement::Query(q)) = ast.get(0) {
        if let SetExpr::Select(s) = q.body.as_ref() {
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
) -> Result<Vec<(String, DBType, bool)>, Box<dyn Error>> {
    if col_names.len() != col_types.len() {
        return Err("Column names different than column types length".into());
    }
    let mut types = Vec::new();
    for ty in col_types {
        if let Ok(t) = ty.parse::<DBType>() {
            types.push(t);
        } else {
            return Err(format!("Unable to parse type: {:?}", ty).into());
        }
    }
    let cols : Vec<(String, DBType, bool)> = col_names.iter()
        .zip(types.iter())
        .map(|(s1, s2)| (s1.clone(), s2.clone(), pks.iter().find(|pk| &pk[..] == &s1[..]).is_some() ))
        .collect();
    Ok(cols)
}

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

