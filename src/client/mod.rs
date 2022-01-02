pub struct QueriesClient {
    pub conn_set : ConnectionSet,
    pub active_conn : ActiveConnection,
    pub env : Environment,
    pub scripts : OpenedScripts,
}

impl QueriesClient {

    pub fn new() -> Self {
        Self {
            conn_set : ConnectionSet::new(),
            active_conn : ActiveConnection::new(),
            env : Environment::new(),
            scripts : OpenedScripts::new(),
        }
    }

    pub fn update(&self, state : &SharedUserState) {

        for conn in state.borrow().conns.iter() {
            self.conn_set.send.send(ConnectionAction::Add(Some(conn.clone())));
        }

        for script in state.borrow().scripts.iter() {
            self.scripts.send.send(ScriptAction::Add(script.clone()));
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


