use filecase::*;
use queries::client::*;
use gtk4::glib;
mod common;
use queries::sql::StatementOutput;
use std::rc::Rc;
use std::cell::RefCell;

const TABLE_CREATION : &'static str = r#"
create table all_types(
    integer_col integer,
    text_col text,
    real_col real,
    blob_col blob
);
insert into all_types values (1, 'a', 1.0, x'0100');
"#;

const TABLE_SELECTION : &'static str = "select * from all_types";

// cargo test -- sqlite --nocapture
#[test]
fn sqlite() {

    gtk4::init();

    let user_state = SharedUserState::default();
    let conn = ActiveConnection::new(&user_state);
    {
        let mut us = user_state.borrow_mut();
        us.execution.accept_dml = true;
        us.execution.accept_ddl = true;
    }

    let all_stmts = [
        TABLE_CREATION.to_string(),
        TABLE_SELECTION.to_string()
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
    conn.connect_db_conn_failure(move |e| {
        panic!("{:?}", e);
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
    let dt = common::run("date +%y_%m_%d_%H_%M_%S").unwrap().trim().to_string();
    let mut info = ConnectionInfo::new_sqlite(&format!("file:///tmp/queries_test_{}.db", dt));
    let uri = ConnURI::new(info, "").unwrap();
    conn.send(ActiveConnectionAction::ConnectRequest(uri));
    common::run_loop_for_ms(1_000);
}


