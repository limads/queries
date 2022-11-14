/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use std::mem;
use filecase::MultiArchiverAction;
use filecase::MultiArchiverImpl;

pub struct QueriesClient {
    pub conn_set : ConnectionSet,
    pub active_conn : ActiveConnection,
    pub env : Environment,
    pub scripts : OpenedScripts,
}

impl QueriesClient {

    pub fn new(user_state : &SharedUserState) -> Self {
        let client = Self {
            conn_set : ConnectionSet::new(user_state),
            active_conn : ActiveConnection::new(user_state),
            env : Environment::new(user_state),
            scripts : OpenedScripts::new(),
        };
        
        let mut state = user_state.borrow_mut();

        // The connection and scripts vectors are moved out of state
        // because they will be re-set when the implementations for those
        // signals is called. Note nothing is done here because the MainLoop hasn't been
        // started yet, but those actions are queued for when it does and the
        // state is re-updated accordingly.
        // for conn in mem::take(&mut state.conns) {
        //    client.conn_set.send.send(ConnectionAction::Add(Some(conn.clone()))).unwrap();
        // }

        for script in mem::take(&mut state.scripts) {
            client.scripts.sender().send(MultiArchiverAction::Add(script.clone())).unwrap();
        }
        
        client
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

/* TODO enable custom logging

fn _glib_logger() {

    /*static glib_logger: glib::GlibLogger = glib::GlibLogger::new(
        glib::GlibLoggerFormat::Plain,
        glib::GlibLoggerDomain::CrateTarget,
    );

    log::set_logger(&glib_logger);
    log::set_max_level(log::LevelFilter::Debug);
    log::info!("This line will get logged by glib");*/

    // glib::log_set_handler(None, glib::LogLevels::all(), false, true, |_, level, msg| {
    // });

}

fn _systemd_logger() {
    // Alternatively, use simple_logger
    // systemd_journal_logger::init();
    // log::set_max_level(log::LevelFilter::Info);
}
*/


