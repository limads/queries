mod common;
use queries::client::ActiveConnection;
use queries::client::SharedUserState;
use queries::client::ActiveConnectionAction;

#[test]
pub fn simple_connection() {
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

