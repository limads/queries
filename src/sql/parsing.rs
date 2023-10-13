/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use super::*;
use sqlparser::dialect::{PostgreSqlDialect};
use sqlparser::ast::{Statement, SetExpr, SelectItem};
use sqlparser::parser::{Parser, ParserError};
use sqlparser::dialect::keywords::Keyword;
use sqlparser::dialect;
use sqlparser::tokenizer::{Tokenizer, Token, Word, Whitespace};
use either::Either;

/// The Parsed variant represents a server-side syntatically-valid SQL statement (although not
/// guaranteed to be semantically valid); Raw represents an unparsed statement with no
/// correctness guarantees which will not be locally-executed
/// (we only know if it is has a select token or not and excludes the "into" token, suggesting
/// it might be a query attempt); Local represents a non-standard SQL statement which is executed locally.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum AnyStatement {

    // Parsed statement; full statement.
    Parsed(Statement, String),
    
    // Parsed statement block.
    ParsedTransaction { begin : Statement, middle : Vec<Statement>, end : Statement, raw : String },

    // Raw SQL tokens; full statement; whether the statement is a query or not.
    Raw(Vec<Token>, String, bool),

    Local(LocalStatement)
}

impl AnyStatement {

    pub fn from_sql(sql : &str) -> Option<Self> {
        let dialect = PostgreSqlDialect {};
        let mut ans = Parser::parse_sql(&dialect, sql).ok()?;
        if ans.len() != 1 {
            None
        } else {
            Some(AnyStatement::Parsed(ans.remove(0), sql.to_string()))
        }
    }
    
    pub fn sql(&self) -> &str {
        match &self {
            Self::Parsed(_, sql) => &sql[..],
            Self::ParsedTransaction{ raw, .. } => &raw[..],
            Self::Raw(_, sql, _) => &sql[..],
            Self::Local(_) => unimplemented!()
        }
    }
    
}

pub fn take_word<'a, I>(token_iter : &mut I) -> Option<String>
where
    I : Iterator<Item=&'a Token>
{
    match take_while_not_whitespace(token_iter) {
        Some(Token::Word(w)) => {
            if w.keyword == Keyword::NoKeyword {
                Some(w.value.to_string())
            } else {
                None
            }
        },
        Some(_) | None => None
    }
}

pub fn take_first_keyword<'a, I>(token_iter : &mut I) -> Option<Keyword>
where
    I : Iterator<Item=&'a Token>
{
    match take_while_not_whitespace(token_iter) {
        Some(Token::Word(w)) => {
            Some(w.keyword)
        },
        Some(_) | None => None
    }
}

pub fn parse_remaining_run_tokens<'a, I>(token_iter : &mut I) -> Result<Execute, String>
where
    I : Iterator<Item=&'a Token>
{
    let mut program = if let Some(Token::SingleQuotedString(s)) = take_while_not_whitespace(token_iter) {
        Execute{ call : s.clone(), using : None, into : None }
    } else {
        return Err("Invalid run call".to_string());
    };

    match take_while_not_whitespace(token_iter) {
        Some(Token::Word(w)) => match w.keyword {
            Keyword::USING => {
                match take_word(token_iter) {
                    Some(using) => program.using = Some(using),
                    _ => { return Err(String::from("Invalid run call")); }
                }
            },
            Keyword::INTO => {
                match take_word(token_iter) {
                    Some(into) => program.into = Some(into),
                    _ => { return Err(String::from("Invalid run call")); }
                }
            },
            _ => return Err(String::from("Invalid run call"))
        },
        Some(Token::SemiColon) | None => {
            return Ok(program);
        },
        _ => { return Err(String::from("Invalid run call")); }
    }

    match take_while_not_whitespace(token_iter) {
        Some(Token::Word(w)) => if w.keyword == Keyword::INTO {
            match take_word(token_iter) {
                Some(word) => program.into = Some(word),
                _ => { return Err(String::from("Invalid run call")); }
            }
        },
        Some(Token::SemiColon) | None => {
            return Ok(program);
        },
        _ => { return Err(String::from("Invalid run call")); }
    }

    if close_valid_statement(token_iter) {
        Ok(program)
    } else {
        Err(String::from("Invalid run token at statement end"))
    }
}

pub fn parse_sql(sql : &str, _subs : &HashMap<String, String>) -> Result<Vec<Statement>, String> {
    
    let dialect = PostgreSqlDialect {};
    Parser::parse_sql(&dialect, &sql[..])
        .map_err(|e| {
            match e {
                ParserError::TokenizerError(s) => s,
                ParserError::ParserError(s) => s,
                ParserError::RecursionLimitExceeded => String::from("Recursion limit exceeded")
            }
        })
}

pub fn is_token_whitespace(token : &Token) -> bool {
    match token {
        Token::Whitespace(_) => true,
        _ => false
    }
}

fn add_token(stmt_tokens : &mut Option<Vec<Token>>, tk : Token) {
    match stmt_tokens {
        Some(ref mut stmt_tokens) => stmt_tokens.push(tk),
        None => {
            *stmt_tokens = Some(vec![tk])
        },
    }
}

/// Splits Vec<Token>  into Vec<Vec<Token>>, where each inner vector is a separate SQL
/// statement.
pub fn split_statement_tokens(mut tokens : Vec<Token>) -> Result<Vec<Vec<Token>>, String> {

    // Each inner vector stores the tokens for a separated statement
    let mut split_tokens : Vec<Vec<Token>> = Vec::new();

    // Stores the tokens of the current statement.
    let mut stmt_tokens : Option<Vec<Token>> = None;

    let mut last_tk_is_dollar = false;
    let mut inside_dollar_quote = false;

    // Reject dollar+number patterns such as $1, $2, which postgres::Client expect to be
    // query parameters. This will be lexed into Char('$') Number(1) using sqlparser.
    // Unless this is done at the tokenization stage, the database thread
    // will panic when we execute the non-parametrized query. We must make an effort here
    // to parse away the substitutions before the client is made aware of the SQL, to avoid
    // any panics. This is clearly a sub-optimal way to do it, but sqlparser does not recognize
    // dollar-quoted strings as its own kind of token.
    for tk in tokens.drain(0..) {

        match &tk {
            // Clear current token group and push as a new inner statement tokens vector
            Token::SemiColon => {

                if inside_dollar_quote {
                    add_token(&mut stmt_tokens, tk.clone());
                } else if let Some(mut group) = stmt_tokens.take() {
                    group.push(Token::SemiColon);
                    split_tokens.push(group);
                }

                last_tk_is_dollar = false;
            },

            Token::Char('$') => {

                if inside_dollar_quote {
                    if last_tk_is_dollar {
                        inside_dollar_quote = false;
                    }
                } else if last_tk_is_dollar {
                    inside_dollar_quote = true;
                }

                add_token(&mut stmt_tokens, tk.clone());
                last_tk_is_dollar = true;

            },

            Token::Whitespace(Whitespace::SingleLineComment{ .. }) => {
                // A single line comment starting a statement block
                // is preventing Parser::new(.) of returning a valid select statement,
                // so they are parsed away here.
                last_tk_is_dollar = false;
            },
            Token::Number(n, _) => {

                if last_tk_is_dollar && !inside_dollar_quote {
                    return Err(format!("Invalid SQL token: ${}", n));
                }

                add_token(&mut stmt_tokens, tk.clone());
                last_tk_is_dollar = false;
            },
            _other => {
                add_token(&mut stmt_tokens, tk.clone());
                last_tk_is_dollar = false;
            }
        }
    }

    if inside_dollar_quote {
        return Err(format!("Unclosed dollar quote"));
    }

    if let Some(last_tokens) = stmt_tokens {
        split_tokens.push(last_tokens);
    }

    // Remove token groups which have only whitespaces. Sequences of empty spaces separated
    // by semicolons are legal SQL statements, but we do not need to send them to the server
    // since they will not change the output.
    let non_ws_token_groups : Vec<_> = split_tokens.drain(0..)
        .filter(|group| !group.iter().all(|tk| is_token_whitespace(tk)) )
        .collect();

    Ok(non_ws_token_groups)
}

pub enum SQLError {

    /* An error was encountered at the first lexing stage (before splitting statements) */
    Lexing(String),

    /* An error was encountered at the second parsing stage (after statements are split,
    each statement is parsed separately, and an error was encountered */
    Parsing(String),
    
    /* Parsing was successful, but running this statement is unsupported by queries,
    such as a 'copy to' statement. */
    Unsupported(String)

}

fn join_tokens(token_group : &[Token]) -> String {
    // Reconstruct the statement to send it to the parser. This will capitalize the SQL sent by the user.
    let mut orig = String::new();
    for tk in token_group.iter() {
        orig += &tk.to_string()[..];
    }
    orig.trim().to_string()
}

/* Tries to determine the nature of an statement that couldn't be parsed by SQLParse from its
first keyword. */
fn _define_raw_statement(tokens : Vec<Token>) -> Result<AnyStatement, SQLError> {
    let kw = take_first_keyword(&mut tokens.iter())
        .ok_or(SQLError::Lexing(format!("No first token")))?;
    let is_query = match kw {
        Keyword::CREATE | Keyword::ALTER | Keyword::DROP | Keyword::INSERT | Keyword::UPDATE | Keyword::DELETE | Keyword::GRANT | Keyword::REVOKE => {
            false
        },
        Keyword::EXPLAIN | Keyword::ANALYZE | Keyword::SELECT => {
            true
        },
        _other => {
            return Err(SQLError::Parsing(format!("Could not determine statement")));
        }
    };
    Ok(AnyStatement::Raw(tokens.clone(), join_tokens(&tokens[..]), is_query))
}

pub fn parse_query_cols(query : &str) -> Result<Vec<String>, String> {
    let dialect = dialect::PostgreSqlDialect{};
    let mut stmts = Parser::parse_sql(&dialect, query)
        .map_err(|e| format!("{}", e) )?;
    if stmts.len() == 1 {
        match stmts.remove(0) {
            Statement::Query(query) => {
                match *query.body {
                    SetExpr::Select(sel) => {
                    
                        let mut items = Vec::new();
                        for it in &sel.projection {
                            match it {
                                SelectItem::UnnamedExpr(expr) => {
                                    items.push(format!("{}", expr));
                                },
                                SelectItem::ExprWithAlias { alias,.. } => {
                                    items.push(format!("{}", alias));
                                },
                                SelectItem::QualifiedWildcard(obj, _) => {
                                    items.push(format!("{}.* (empty)", obj));
                                },
                                SelectItem::Wildcard(_) => {
                                    items.push(format!("* (empty)"));
                                }
                            }
                        }
                        
                        Ok(items)
                    },
                    
                    // If there is a parenthesized nested query, repeat the call with the inner item.
                    SetExpr::Query(q) => {
                        parse_query_cols(&format!("{}", q))
                    },
                    
                    // Since set ops (union, intersect) always must have all operands with
                    // the same cols, it is reasonable to repeat the call with the first operand.
                    SetExpr::SetOperation { left, .. } => {
                        parse_query_cols(&format!("{}", left))
                    },
                    
                    _ => {
                        Err(format!("Expected select"))
                    }
                }
            },
            _ => {
                Err(format!("Statement is not query"))
            }
        }
    } else {
        Err(format!("Invalid number of statements"))
    }
}

fn agg_statements(stmts : &[Statement]) -> String {
    let mut s = String::new();
    for stmt in stmts {
        s += &format!("{};\n", stmt);
    }
    s
}

fn close_transaction(mut stmts : Vec<Statement>) -> Result<AnyStatement, SQLError> {
    let raw = agg_statements(&stmts);
    if stmts.len() < 2 {
        return Err(SQLError::Parsing(format!("Invalid transaction command")));
    }
    let begin = stmts.remove(0);
    let end = stmts.remove(stmts.len()-1);
    match begin {
        Statement::StartTransaction { .. } => { },
        _ => {
            return Err(SQLError::Parsing(format!("Invalid transaction start command (expected BEGIN)")));
        }
    }
    match end {
        Statement::Commit { .. } | Statement::Rollback { .. } => {  },
        _ => {
            return Err(SQLError::Parsing(format!("Invalid transaction end command (expected COMMIT or ROLLBACK)")));
        }
    }
    Ok(AnyStatement::ParsedTransaction { begin, end, middle : stmts, raw })
}

pub fn fully_parse_sql(
    sql : &str
) -> Result<Vec<AnyStatement>, SQLError> {

    let tokens = extract_postgres_tokens(&sql)
        .map_err(|e| SQLError::Lexing(e) )?;

    // It is important to reject queries with placeholder tokens, because
    // the postgres driver panics on any placeholder/argument mismatch, and
    // the call never passes any statement arguments.
    for (ix, tk) in tokens.iter().enumerate() {
        match tk {
            Token::Placeholder(pl) => {
                
                if let Some(next_tk) = tokens.get(ix + 1) {
                    match next_tk {
                        Token::Placeholder(_) => {
                            return Err(SQLError::Unsupported(format!("Unsupported SQL token: '$$'")));
                        },
                        _ => { }
                    }
                }
                
                return Err(SQLError::Unsupported(format!("Unsupported SQL token: '{}'", pl)));
            },
            _ => { }
        }
    }
    
    let dialect = dialect::PostgreSqlDialect{};
    let mut any_stmts = Vec::new();
   
    match Parser::parse_sql(&dialect, sql) {
        
        Ok(mut stmts) => {
            
            let mut curr_transaction = None;
            while stmts.len() > 0 {
                
                match stmts.remove(0) {
                    Statement::Copy{ .. } => {
                        return Err(SQLError::Unsupported(format!("Unsupported statement (copy)")));
                    },
                    Statement::Savepoint { .. } => {
                        return Err(SQLError::Unsupported(format!("Unsupported feature (Transaction SAVEPOINT)")));
                    },
                    Statement::StartTransaction { modes, begin }  => {
                        if curr_transaction.is_some() {
                            return Err(SQLError::Unsupported(format!("Nested transactions are currently unsupported.")));
                        }
                        curr_transaction = Some(vec![Statement::StartTransaction { modes, begin }]);
                    },
                    Statement::Rollback { chain } => {
                        if let Some(mut ct) = curr_transaction.take() {
                            ct.push(Statement::Rollback { chain });
                            let tr = close_transaction(ct)?;
                            any_stmts.push(tr);
                        } else {
                            return Err(SQLError::Parsing(format!("ROLLBACK without any open transactions (missing BEGIN)")));
                        }
                    },
                    Statement::Commit { chain }  => {
                        if let Some(mut ct) = curr_transaction.take() {
                            ct.push(Statement::Commit { chain });
                            let tr = close_transaction(ct)?;
                            any_stmts.push(tr);
                        } else {
                            return Err(SQLError::Parsing(format!("COMMIT statement without any open transactions (missing BEGIN)")));
                        }
                    },
                    other_stmt => { 
                        if let Some(ref mut curr_t) = curr_transaction {
                            curr_t.push(other_stmt);
                        } else {
                            let orig = format!("{}", other_stmt);
                            any_stmts.push(AnyStatement::Parsed(other_stmt.clone(), orig));
                        }
                    }
                }
            }
            
            if curr_transaction.is_some() {
                return Err(SQLError::Parsing(format!("Unfinished transaction block\n(expected COMMIT or ROLLBACK)")));
            }
        },
        
        Err(e) => {
            return Err(SQLError::Parsing(format!("{}", e)));
        }
    }
    
    Ok(any_stmts)
}

/*/* Parse SQL, and continue to execute even if some statements could not be parsed. */
/// Parse this query sequence, first splitting the token vector
/// at the semi-colons (delimiting statements) and then parsing
/// each statement individually. On error, the un-parsed statement is returned.
/// Might fail globally if the tokenizer did not yield a valid token vector.
pub fn partially_parse_sql(
    sql : &str,
    _subs : &HashMap<String, String>
) -> Result<Vec<AnyStatement>, SQLError> {

    let tokens = extract_postgres_tokens(&sql)
        .map_err(|e| SQLError::Lexing(e) )?;

    let split_tokens = split_statement_tokens(tokens).map_err(|e| SQLError::Lexing(e) )?;

    let dialect = dialect::PostgreSqlDialect{};

    let mut any_stmts = Vec::new();
    for token_group in split_tokens {

        match Parser::new(token_group.clone(), &dialect).parse_statement() {
            Ok(stmt) => {

                // Parsing each group of tokens should yield exactly one statement.
                // Whitespace-only token groups should already have been filtered at
                // split_tokens.
                // if stmts.len() != 1 {
                //    return Err(SQLError::Parsing(format!("Found {} statements (expected 1)", stmts.len())));
                // }

                match stmt {
                    Statement::Copy{ table_name, columns, .. } => {

                        // sqlparser::parse_copy (0.9.0) is only accepting the copy (..) from stdin sequence.
                        // In this case, hard-code the copy statement here. We use a custom type for copy
                        // because queries parse this statement at client-side to support local command execution.
                        let cols = columns.iter().map(|c| c.to_string()).collect();
                        any_stmts.push(AnyStatement::Local(LocalStatement::Copy(Copy {
                            target : copy::CopyTarget::From,
                            cols,
                            table : table_name.to_string(),
                            options : String::new(),
                            client : copy::CopyClient::Stdio
                         })));

                    },
                    other_stmt => {
                        let orig = join_tokens(&token_group[..]);
                        any_stmts.push(AnyStatement::Parsed(other_stmt.clone(), orig));
                    },
                    // None => {
                    //    return Err(SQLError::Parsing(format!("Found {} statements (expected 1)", stmts.len())));
                    // }
                }
            },
            Err(e) => {

                // Sqlparser (0.9.0) will fail when the full PostgreSQL 'copy' command is
                // invoked in full form. Do custom parsing in this case, adding a copy command
                // and parsing the remaining statements.
                match local_statement_or_tokens(&mut token_group.iter().peekable()).map_err(|e| SQLError::Parsing(e) )? {
                    Some(Either::Left(local)) => {
                        any_stmts.push(AnyStatement::Local(local));
                    },
                    Some(Either::Right(_)) => {
                        return Err(SQLError::Parsing(format!("Error parsing SQL statement: {}", e)));
                    },
                    None => { }
                }
            }
        }
    }
    Ok(any_stmts)
}*/

// Remove repeated statements if they are queries. Only the first repeated query
// is executed. Repeated non-query statements are executed normally.
pub fn filter_repeated_queries(any_stmts : Vec<AnyStatement>) -> Vec<AnyStatement> {
    let mut filt_stmts : Vec<AnyStatement> = Vec::new();

    for any_stmt in any_stmts {
        match &any_stmt {
            AnyStatement::Parsed(stmt, _) => match stmt {
                Statement::Query(_q) => {
                    if filt_stmts.iter().find(|filt_stmt| **filt_stmt == any_stmt ).is_none() {
                        filt_stmts.push(any_stmt.clone());
                    }
                },
                _ => { filt_stmts.push(any_stmt); }
            },
            AnyStatement::Raw(_, _, is_select) => {
                if *is_select {
                    if filt_stmts.iter().find(|filt_stmt| **filt_stmt == any_stmt ).is_none() {
                        filt_stmts.push(any_stmt.clone());
                    }
                } else {
                    filt_stmts.push(any_stmt.clone());
                }
            },
            _ => {
                filt_stmts.push(any_stmt.clone());
            }
        }
    }
    filt_stmts
}

/// Modifies the iterator until the first non-whitespace token is found, returning it.
pub fn take_while_not_whitespace<'a, I>(token_iter : &mut I) -> Option<&'a Token>
where
    I : Iterator<Item=&'a Token>
{
    token_iter.next().and_then(|tk| match tk {
        Token::Whitespace(_) => take_while_not_whitespace(token_iter),
        other => Some(other)
    })
}

/// Walks token_inter until either a semicolon is found or the iterator ends (returning true).
/// If anything other is found, return false.
pub fn close_valid_statement<'a, I>(token_iter : &mut I) -> bool
where
    I : Iterator<Item=&'a Token>
{
    match take_while_not_whitespace(token_iter) {
        Some(Token::SemiColon) => true,
        Some(_) => false,
        None => true
    }
}

pub fn decide_table<'a, I>(token_iter : &mut I) -> Result<String, String>
where
    I : Iterator<Item=&'a Token>
{
    if let Some(tk) = take_while_not_whitespace(token_iter) {
        match tk {
            Token::Word(w) => {
                Ok(w.value.to_string())
            },
            Token::LParen => {
                let mut tbl = String::from("(");
                while let Some(tk) = token_iter.next()  {
                    match tk {
                        Token::RParen => {
                            tbl += ")";
                            break;
                        },
                        tk => {
                            tbl += &tk.to_string();
                        }
                    }
                }
                Ok(tbl)
            },
            _ => Err(format!("Invalid table name"))
        }
    } else {
        Err(format!("Missing table name"))
    }
}

pub fn decide_target_keyword(w : &Word) -> Result<copy::CopyTarget, String> {
    match w.keyword {
        Keyword::FROM => Ok(copy::CopyTarget::From),
        Keyword::TO => Ok(copy::CopyTarget::To),
        _ => return Err(format!("Unknown copy destination: {}", w))
    }
}

pub fn decide_target<'a, I>(
    token_iter : &mut I,
    cols : &mut Vec<String>
) -> Result<copy::CopyTarget, String>
where
    I : Iterator<Item=&'a Token>
{
    match take_while_not_whitespace(token_iter) {
        Some(&Token::LParen) => {
            while let Some(tk) = token_iter.next()  {
                match tk {
                    Token::Word(w) => {
                        cols.push(w.value.to_string());
                    },
                    Token::RParen => {
                        break;
                    },
                    _ => { }
                }
            }
            match take_while_not_whitespace(token_iter) {
                Some(Token::Word(w)) => {
                    decide_target_keyword(w)
                },
                Some(other) => {
                    Err(format!("Invalid target copy token: {}", other))
                },
                None => {
                    Err(format!("Missing copy target"))
                }
            }
        },
        Some(Token::Word(w)) => {
            decide_target_keyword(&w)
        },
        Some(other) => {
            Err(format!("Invalid copy token: {}", other))
        },
        None => {
            Err(format!("Empty copy destination"))
        }
    }
}

pub fn decide_client<'a, I>(
    token_iter : &mut I,
    _target : &copy::CopyTarget
) -> Result<copy::CopyClient, String>
where
    I : Iterator<Item=&'a Token>
{
    match take_while_not_whitespace(token_iter) {
        Some(Token::Word(w)) => {
            if &w.value[..] == "PROGRAM" || &w.value[..] == "program" {
                if let Some(tk) = take_while_not_whitespace(token_iter) {
                    match tk {
                        Token::SingleQuotedString(prog) => Ok(copy::CopyClient::Program(prog.to_string())),
                        _ => Err(format!("Invalid program string"))
                    }
                } else {
                    Err(format!("Missing program string"))
                }
            } else if &w.value[..] == "FILE" || &w.value[..] == "file" {
                if let Some(tk) = take_while_not_whitespace(token_iter) {
                    match tk {
                        Token::SingleQuotedString(file) => Ok(copy::CopyClient::File(file.to_string())),
                        _ => Err(format!("Invalid program string"))
                    }
                } else {
                    Err(format!("Invalid copy client specification"))
                }
            } else {
                Err(format!("Invalid copy client specification"))
            }
        },
        Some(Token::SingleQuotedString(s)) => {
            Err(format!("Invalid client copy specification: {}", s))
        },
        Some(other) => {
            Err(format!("Invalid client copy specification: {}", other))
        },
        None => {
            Err(format!("Missing copy destination"))
        }
    }
}

pub fn parse_options<'a, I>(token_iter : &mut I) -> String
where
    I : Iterator<Item=&'a Token>
{
    let mut options = String::new();
    if let Some(Token::Word(w)) = take_while_not_whitespace(token_iter) {
        if w.keyword == Keyword::WITH {
            while let Some(tk) = token_iter.next() {
                match tk {
                    Token::Word(w) => {
                        options += &w.to_string()[..];
                        options += " ";
                    },
                    _ => { }
                }
            }
        }
    }

    // Use with csv header by default, since this is what most users will
    // be working with when declaring, writing into and reading from variables.
    // If the user informed other options, options will not be empty and this
    // default won't be applied.
    if options.is_empty() {
        options = String::from("csv header")
    }

    options
}

pub fn extract_postgres_tokens(stmt : &str) -> Result<Vec<Token>, String> {
    let dialect = PostgreSqlDialect{};
    let mut tokenizer = Tokenizer::new(&dialect, stmt);
    tokenizer.tokenize().map_err(|e| format!("{}", e) )
}

pub fn define_if_select(tk : &Token, might_be_select : &mut bool, is_select : &mut bool) {
    match tk {
        Token::Word(w) => {
            match w.keyword {
                Keyword::SELECT => {
                    if *might_be_select {
                        *is_select = true;
                    }
                },
                Keyword::INSERT => {
                    *is_select = false;
                    *might_be_select = false;
                },
                Keyword::UPDATE => {
                    *is_select = false;
                    *might_be_select = false;
                },
                Keyword::DELETE => {
                    *is_select = false;
                    *might_be_select = false;
                },
                Keyword::CREATE => {
                    *is_select = false;
                    *might_be_select = false;
                },
                Keyword::GRANT => {
                    *is_select = false;
                    *might_be_select = false;
                },
                Keyword::ALTER => {
                    *is_select = false;
                    *might_be_select = false;
                },
                Keyword::COPY => {
                    *is_select = false;
                    *might_be_select = false;
                },

                // Excludes select a into b, which is actually an insert statement for b.
                Keyword::INTO => {
                    *is_select = false;
                    *might_be_select = false;
                },
                _ => { }
            }
        },
        _ => { }
    }
}

/// Returns true for each statement if the resulting statement
/// is a select. This is a crude fallible approach, used as fallbach when
/// sqlparser is unable to parse a query due to engine-specific SQL extensions.
/// TODO filter out locally-parsed statements here. Queries work by, in the best
/// case, being able to parse the statement and show any errors to the user at client-side;
/// in the worst case, the non-parsed statements are sent anyway to be rejected by
/// the SQL engine. On both cases, locally-executed statements need to be parsed away
/// for execution.
pub fn split_unparsed_statements(sql_text : String) -> Result<Vec<AnyStatement>, String> {
    let mut unparsed_stmts = Vec::new();
    let tokens = extract_postgres_tokens(&sql_text)?;
    let split_tokens = split_statement_tokens(tokens)?;
    for stmt_tokens in split_tokens {
        let mut token_iter = stmt_tokens.iter().peekable();
        let mut stmt_string = String::new();
        let mut is_select = false;
        let mut might_be_select = true;
        let mut is_local = false;
        while let Some(either_stmt) = local_statement_or_tokens(&mut token_iter)? {
            match either_stmt {
                Either::Left(local) => {
                    unparsed_stmts.push(AnyStatement::Local(local));
                    is_local = true;
                    break;
                },
                Either::Right(tks) => {
                    match tks {
                        Either::Left(tk) => {
                            stmt_string += &tk.to_string()[..];
                            define_if_select(&tk, &mut might_be_select, &mut is_select);
                        },
                        Either::Right(tks) => {
                            for tk in tks {
                                stmt_string += &tk.to_string()[..];
                                define_if_select(&tk, &mut might_be_select, &mut is_select);
                            }
                        }
                    }

                }
            }
        }

        // SQLite special case
        if stmt_string.starts_with("pragma") || stmt_string.starts_with("PRAGMA") {
            is_select = true;
        }

        if !is_local {
            unparsed_stmts.push(AnyStatement::Raw(stmt_tokens.clone(), stmt_string.trim().to_string(), is_select));
        }
    }

    Ok(unparsed_stmts)
}

pub fn parse_declare_items<'a, I>(token_iter : &mut I, names : &mut Vec<String>, types : &mut Vec<VariableType>) -> Result<(), String>
where
    I : Iterator<Item=&'a Token>
{
    if let Some(Token::Word(w)) = take_while_not_whitespace(token_iter) {
        if w.keyword == Keyword::NoKeyword {
            let name = w.value.to_string();
            if let Some(Token::Word(w)) = take_while_not_whitespace(token_iter) {
                let ty = match w.keyword {
                    Keyword::TEXT => {
                        VariableType::Text
                    },
                    Keyword::BYTEA => {
                        VariableType::Bytea
                    },
                    _ => return Err(String::from("Invalid declare statement (expected declare 'var' 'text|bytea';"))
                };
                match take_while_not_whitespace(token_iter) {
                    Some(Token::Comma) => {
                        names.push(name);
                        types.push(ty);
                        parse_declare_items(token_iter, names, types)
                    },
                    Some(Token::SemiColon) => {
                        names.push(name);
                        types.push(ty);
                        Ok(())
                    },
                    _ => Err(String::from("Invalid declare statement (expected ',' or ';')"))
                }
            } else {
                Err(String::from("Invalid declare token at statement end"))
            }
        } else {
            Err(String::from("Invalid declare token at statement end"))
        }
    } else {
        Err(String::from("Invalid declare token at statement end"))
    }
}

pub fn parse_remaining_declare_tokens<'a, I>(token_iter : &mut I) -> Result<Declare, String>
where
    I : Iterator<Item=&'a Token>
{
    let (mut names, mut types) = (Vec::new(), Vec::new());
    parse_declare_items(token_iter, &mut names, &mut types)?;
    Ok(Declare{ names, types })
}

impl FromStr for Declare {

    type Err = String;

    fn from_str(s : &str) -> Result<Declare, String> {
        let tokens = extract_postgres_tokens(s)?;
        let mut token_iter = tokens.iter();
        if let Some(Token::Word(w)) = take_while_not_whitespace(&mut token_iter) {
            if w.keyword != Keyword::DECLARE {
                return Err(format!("Invalid declare statement"));
            }
            parse_remaining_declare_tokens(&mut token_iter)
        } else {
            return Err(String::from("Invalid declare token at statement end"));
        }

    }
}

