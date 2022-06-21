use std::mem;
use archiver::MultiArchiverAction;
use archiver::MultiArchiverImpl;

pub struct QueriesClient {
    pub conn_set : ConnectionSet,
    pub active_conn : ActiveConnection,
    pub env : Environment,
    pub scripts : OpenedScripts,
}

impl QueriesClient {

    pub fn new(user_state : &SharedUserState) -> Self {
        let client = Self {
            conn_set : ConnectionSet::new(),
            active_conn : ActiveConnection::new(user_state),
            env : Environment::new(),
            scripts : OpenedScripts::new(),
        };
        client.update(&user_state);
        client
    }

    pub fn update(&self, state : &SharedUserState) {
        let mut state = state.borrow_mut();

        // The connection and scripts vectors are moved out of state
        // because they will be re-set by the connect_added events. Adding them
        // via the events guarantees the GUI is automatically updated.
        for conn in mem::take(&mut state.conns) {
            self.conn_set.send.send(ConnectionAction::Add(Some(conn.clone())));
        }

        for script in mem::take(&mut state.scripts) {
            self.scripts.sender().send(MultiArchiverAction::Add(script.clone()));
        }
    }

}

mod conn;

pub use conn::*;

mod listener;

pub use listener::*;

mod environment;

pub use environment::*;

mod scripts;

pub use scripts::*;

mod user;

pub use user::*;

mod exec;

pub use exec::*;

