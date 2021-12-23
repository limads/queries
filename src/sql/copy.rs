use std::cmp::{Eq, PartialEq};
use sqlparser::tokenizer::{Tokenizer, Token, Word, Whitespace};
use sqlparser::ast::{Statement, Function, Select, Value, Expr, SetExpr, SelectItem, Ident, TableFactor, Join, JoinOperator};
use sqlparser::parser::{Parser, ParserError};
use sqlparser::dialect::keywords::Keyword;
use std::str::FromStr;
use regex::Regex;
use super::*;

// Supported syntax for now:
// copy patients to file '/home/diego/Downloads/patients.csv';
// copy patients to program 'echo -e hello';

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CopyTarget {
    // Copies from server to client
    To,

    // Copies from client to server
    From
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum CopyClient {

    // Results from a "copy to/from program 'prog'" command
    Program(String),

    // Results from a "copy to/from 'file'"
    File(String),

    // Results from a "copy to/from variable var"
    Variable(String),

    // Copy to/from stdin/stdout
    Stdio
}

// Ideally, we want to support a copy in/out version, something like:
// copy tbl to program 'myprogram' then copy out_tbl from stdout;
// So that users can write executable filters that process text or binary copy
// streams from PostgreSQL. Scheduled/Event-driven SQL files (jobs) could then process
// arbitrarily complex data manipulation pipelines with good performance (relying on
// the binary copy format). Every loaded SQL file could then have a time icon/label
// or exclamation mark icon/label indicating whether this SQL file is currently
// scheduled or marked to be executed on an event. Perhaps just a sequence of copy to program
// then copy from stdout at the next statement could allow us to to that without violating
// PostgreSQL's syntax too much. Non-selected scheduled/listener-driven scripts do not change
// the current table environment. select clauses are ignored, and only update/insert/copy clauses
// are executed. Copy clauses are fundamentally different than select in that select alters
// mainly the table environment; but copy to clauses (either text or binary) might change
// the filesystem or call an external program to execute arbitrary code. If queries is used
// as a service CLI, scheduling SQL files to run might be a nice way to setup client-side
// services that listen to database changes. By parsing the client SQL a single time, we
// can build a vector of actions that are called repeatedly against the database.
// TODO consider the syntax copy tbl from program 'hello' with input (select * from patients)
// args '-i in.csv';
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Copy {

    // Copy to or from?
    pub target : CopyTarget,

    // Table string
    pub table : String,

    // Table columns target (if any)
    pub cols : Vec<String>,

    // Everything that goes in the 'with' clause.
    pub options : String,

    pub client : CopyClient,
}

pub fn parse_remaining_copy_tokens<'a, I>(token_iter : &mut I) -> Result<Copy, String>
where
    I : Iterator<Item=&'a Token>
{
    let table = parsing::decide_table(token_iter)?;
    let mut cols = Vec::new();
    let target : CopyTarget = parsing::decide_target(token_iter, &mut cols)?;
    let client = parsing::decide_client(token_iter, &target)?;
    let options = parsing::parse_options(token_iter);
    if close_valid_statement(token_iter) {
        Ok(Copy{ table, cols, client, target, options })
    } else {
        Err(String::from("Invalid copy token at statement end"))
    }
}

impl FromStr for Copy {

    type Err = String;

    fn from_str(s : &str) -> Result<Copy, String> {
        let mut query = s.to_string();
        let copy_regx = Regex::new(COPY_REGEX).unwrap();
        let c_match = copy_regx.find(&query).ok_or(format!("Copy statement regex parsing error"))?;
        let whitespace_err = format!("Missing whitespace at copy statement");
        let is_whitespace = |tk : &Token| -> Result<(), String> {
            match tk {
                Token::Whitespace(_) => Ok(()),
                _ => Err(whitespace_err.clone())
            }
        };
        let tokens = parsing::extract_postgres_tokens(&query[c_match.start()..c_match.end()])?;
        let mut token_iter = tokens.iter();
        if let Some(Token::Word(w)) = take_while_not_whitespace(&mut token_iter) {
            if w.keyword != Keyword::COPY {
                return Err(format!("Invalid first word for copy statement"));
            }
            parse_remaining_copy_tokens(&mut token_iter)
        } else {
            return Err(format!("Invalid first token for copy statement"));
        }
    }
}

impl ToString for Copy {
    fn to_string(&self) -> String {
        let mut cp_s = format!("COPY {} ", self.table);
        if self.cols.len() > 0 {
            cp_s += "(";
            for (i, c) in self.cols.iter().enumerate() {
                cp_s += c;
                if i < self.cols.len() - 1 {
                    cp_s += ",";
                }
            }
            cp_s += ") ";
        }
        match self.target {
            CopyTarget::From => cp_s += "FROM STDIN",
            CopyTarget::To => cp_s += "TO STDOUT"
        }
        if self.options.len() > 0 {
            cp_s += " WITH ";
            cp_s += &self.options[..];
        }
        cp_s += ";";
        // println!("Built copy statement: {}", cp_s);
        cp_s
    }
}

const COPY_REGEX : &'static str =
    r"(copy|COPY)\s+.*\s+(from|FROM|to|TO)\s+((program|PROGRAM|variable|VARIABLE)\s)?('.*'|\$\$.*\$\$|stdin|STDIN|stdout|STDOUT)(\s+with.*)?;";

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Execute {

    pub call : String,

    pub using : Option<String>,

    pub into : Option<String>

}

impl FromStr for Execute {

    type Err = String;

    fn from_str(s : &str) -> Result<Execute, String> {
        let tokens = parsing::extract_postgres_tokens(s)?;
        let mut token_iter = tokens.iter();
        if let Some(Token::Word(w)) = parsing::take_while_not_whitespace(&mut token_iter) {
            if w.keyword != Keyword::EXEC && w.keyword != Keyword::EXECUTE {
                return Err(format!("Invalid first word for run statement"));
            }
            parsing::parse_remaining_run_tokens(&mut token_iter)
        } else {
            Err(format!("Invalid first word for run statement"))
        }
    }

}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum VariableType {
    Text,
    Bytea
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Declare {
    pub names : Vec<String>,
    pub types : Vec<VariableType>
}

