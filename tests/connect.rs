mod common;
use queries::client::ActiveConnection;
use queries::client::SharedUserState;
use queries::client::ActiveConnectionAction;
use queries::server::*;
use std::collections::HashMap;
use std::convert::TryInto;

// Verifies a remote connection is using SSL.
#[test]
pub fn remote_connection() {
    let f = |mut edb : common::ExistingDB| {
        let ssl_query = "select pg_stat_ssl.pid, application_name, ssl, version
        from pg_stat_ssl inner join pg_stat_activity on pg_stat_ssl.pid = pg_stat_activity.pid
        where application_name='Queries';";
        let ans = edb.conn.query(ssl_query);
        let tbl = ans.table().unwrap();
        println!("{}", tbl);
        let ssl : Vec<bool> = tbl["ssl"].clone().try_into().unwrap();
        assert!(ssl[0] == true);
        println!("Done");
    };
    match common::run_with_existing_db(f) {
        Ok(_) => { },
        Err(e) => {
            eprintln!("{}", e);
        }
    }
}

#[test]
pub fn local_connection() {
    common::run_with_temp_db(|temp| {
        gtk4::init();
        let conn = ActiveConnection::new(&SharedUserState::default());
        conn.connect_db_connected(move|(conn,info)| {
            println!("Conneted to {:?}", conn);
            println!("Database info: {:?}", info);
        });
        conn.connect_db_disconnected(move|_| {
            println!("Database disconnected");
        });
        conn.connect_db_error(|e| {
            panic!("{}", e);
        });
        conn.connect_db_conn_failure(move |e| {
            panic!("{}", e.1);
        });
        conn.send(ActiveConnectionAction::ConnectRequest(temp.uri()));
        common::run_loop_for_ms(500);
    });
}

