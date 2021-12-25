pub struct QueriesClient {
    pub conn_set : ConnectionSet,
    pub active_conn : ActiveConnection,
    pub env : Environment,
    pub scripts : OpenedScripts
}

impl QueriesClient {

    pub fn new() -> Self {
        Self {
            conn_set : ConnectionSet::new(),
            active_conn : ActiveConnection::new(),
            env : Environment::new(),
            scripts : OpenedScripts::new()
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


