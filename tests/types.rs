use filecase::*;
use queries::client::*;
use gtk4::glib;
mod common;
use queries::sql::StatementOutput;
use std::rc::Rc;
use std::cell::RefCell;

// Missing types: hstore, ltree, lquery, ltxtquery, ipaddr, bit/varbit, macaddr, cidr, inet, money, line
const TABLE_CREATION : &'static str = r#"
create table all_types(
    col_bool bool,
    col_char char,
    col_smallint smallint,
    col_int integer,
    col_oid oid,
    col_bigint bigint,
    col_real real,
    col_double double precision,
    col_text text,
    col_bytea bytea,
    col_time timestamp,
    col_date date,
    col_time_tz timestamp with time zone,
    col_pt point,
    col_box box,
    col_path path,
    col_json json,
    col_uuid uuid
);

insert into all_types values (
    'true',
    'a',
    1,
    1,
    1,
    1,
    1.0,
    2.0,
    'a',
    '\xDEADBEEF',
    '2023-03-05 21:33:02.675009-03',
    '2023-03-05',
    '2023-03-05 21:33:02.675009-03',
    '(1,1)',
    '((1,1),(2,2))',
    '((1,1),(2,2))',
    '{"a":1}',
    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'
);
"#;

const FULL_SELECTION : &'static str = r#"
    select * from all_types;
"#;

// cargo test -- all_types --nocapture
#[test]
fn all_types() {
    gtk4::init();
    common::run_with_temp_db(|temp| {
        let user_state = SharedUserState::default();
        let conn = ActiveConnection::new(&user_state);
        {
            let mut us = user_state.borrow_mut();
            us.execution.accept_dml = true;
            us.execution.accept_ddl = true;
        }

        let all_stmts = [
            TABLE_CREATION.to_string(),
            FULL_SELECTION.to_string()
        ];

        let stmt_ix = Rc::new(RefCell::new(0));
        conn.connect_db_connected({
            let sender = conn.sender().clone();
            move|(conn,info)| {
                println!("Conneted to {:?}", conn);
                println!("Database info: {:?}", info);
                sender.send(ActiveConnectionAction::ExecutionRequest(TABLE_CREATION.to_string())).unwrap();
            }
        });
        conn.connect_db_error(|e| {
            panic!("{}", e);
        });
        conn.connect_exec_result({
            let sender = conn.sender().clone();
            let stmt_ix = stmt_ix.clone();
            move |res| {
                for r in &res {
                    common::print_output(&r);
                }
                // common::exec_next_statement(&stmt_ix, &all_stmts[..], &sender);
            }
        });
        conn.connect_schema_update({
            let stmt_ix = stmt_ix.clone();
            let sender = conn.sender().clone();
            let all_stmts = all_stmts.clone();
            move |_update| {
                common::exec_next_statement(&stmt_ix, &all_stmts[..], &sender);
            }
        });

        conn.send(ActiveConnectionAction::ConnectRequest(temp.uri()));
        common::run_loop_for_ms(10_000);

    });
}


