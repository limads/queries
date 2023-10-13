use sqlparser::parser::*;
use sqlparser::dialect::*;
use quickcheck::{TestResult, quickcheck, QuickCheck};
use queries::ui::builder::*;

pub fn test_produced_sql(sql : &str) ->Result<(), Box<dyn std::error::Error>> {
    for dialect in [&PostgreSqlDialect{} as &dyn Dialect, &SQLiteDialect{} as &dyn Dialect] {
        Parser::parse_sql(dialect, sql)?;
    }
    Ok(())
}

fn test_query(tbl : Table) -> TestResult {
    TestResult::from_bool(test_produced_sql(&tbl.sql()).is_ok())
}

// cargo test gen -- --nocapture
#[test]
fn gen() {
    QuickCheck::new().tests(1000).quickcheck(test_query as fn(Table)->TestResult);
}


