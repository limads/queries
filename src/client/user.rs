use super::{ConnectionSet, ConnectionInfo, ActiveConnection, OpenedScripts, OpenedFile};
use chrono::prelude::*;
use serde::{Serialize, Deserialize};
use std::fs::File;
use std::io::{Read, Write};
use crate::ui::QueriesWindow;
use std::cell::RefCell;
use crate::React;
use std::rc::Rc;
use std::ops::Deref;
use std::thread;
use gtk4::*;
use gtk4::prelude::*;
use crate::client::QueriesClient;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UserState {
    pub main_handle_pos : i32,
    pub side_handle_pos : i32,
    pub window_width : i32,
    pub window_height : i32,
    pub scripts : Vec<OpenedFile>,
    pub conns : Vec<ConnectionInfo>,
    pub templates : Vec<String>,
    pub selected_template : usize
}

#[derive(Clone)]
pub struct SharedUserState(Rc<RefCell<UserState>>);

impl Deref for SharedUserState {

    type Target = RefCell<UserState>;

    fn deref(&self) -> &RefCell<UserState> {
        &self.0
    }

}

impl Default for SharedUserState {

    fn default() -> Self {
        SharedUserState(Rc::new(RefCell::new(UserState {
            main_handle_pos : 100,
            side_handle_pos : 400,
            window_width : 1024,
            window_height : 768,
            scripts : Vec::new(),
            conns : Vec::new(),
            templates : Vec::new(),
            selected_template : 0
        })))
    }

}

impl SharedUserState {

    /// Attempts to open UserState by deserializing it from a JSON path.
    /// This is a blocking operation.
    pub fn open(path : &str) -> Option<SharedUserState> {
        let state : UserState = serde_json::from_reader(File::open(path).ok()?).ok()?;
        Some(SharedUserState(Rc::new(RefCell::new(state))))
    }

}

/// Saves the state to the given path by spawning a thread. This is
/// a nonblocking operation.
pub fn persist_user_preferences(user_state : &SharedUserState, path : &str) -> thread::JoinHandle<bool> {
    let mut state : UserState = user_state.borrow().clone();
    state.scripts.iter_mut().for_each(|s| { s.content.as_mut().map(|c| c.clear() ); } );
    let path = path.to_string();

    // TODO filter repeated scripts and connections

    thread::spawn(move|| {
        if let Ok(f) = File::create(&path) {
            serde_json::to_writer(f, &state).is_ok()
        } else {
            false
        }
    })
}

/*impl React<super::ActiveConnection> for SharedUserState {

    fn react(&self, conn : &ActiveConnection) {
        conn.connect_db_connected(move |opt_db_info| {
            // Connection already present? If not, add it and save.
            if let Some(info) = opt_db_info {

            }
        });
    }

}*/

/*impl React<super::ConnectionSet> for SharedUserState {

    fn react(&self, set : &ConnectionSet) {
        set.connect_removed({
            let state = self.clone();
            move |ix| {
                let mut state = state.borrow_mut();
                if ix >= 0 {
                    state.conns.remove(ix as usize);
                }
            }
        });
        set.connect_updated({
            let state = self.clone();
            move |(ix, info)| {
                let mut state = state.borrow_mut();
                state.conns[ix as usize] = info;
            }
        });
        set.connect_added({
            let state = self.clone();
            move |conn| {
                let mut state = state.borrow_mut();

                // A connection might be added to the set when the user either activates the
                // connection switch or connection is added from the disk at startup. We ignore
                // the second case here, since the connection will already be loaded at the state.
                // if state.conns.iter().find(|c| c.is_like(&conn) ).is_none() {
                state.conns.push(conn);
                // }

            }
        });
    }

}

impl React<super::OpenedScripts> for SharedUserState {

    fn react(&self, scripts : &OpenedScripts) {
        scripts.connect_file_persisted({
            let state = self.clone();
            move |file| {
                add_file(&state, file);
            }
        });
        scripts.connect_opened({
            let state = self.clone();
            move |file| {
                add_file(&state, file);
            }
        });
        scripts.connect_added({
            let state = self.clone();
            move |file| {
                add_file(&state, file);
            }
        });
    }

}*/

/*fn add_file(state : &SharedUserState, file : OpenedFile) {
    let mut state = state.borrow_mut();
    if let Some(path) = &file.path {
        if state.scripts.iter().find(|f| &f.path.as_ref().unwrap()[..] == &path[..] ).is_none() {
            state.scripts.push(file);
        }
    }
}*/

impl React<crate::ui::QueriesWindow> for SharedUserState {

    fn react(&self, win : &QueriesWindow) {
        let state = self.clone();
        let main_paned = win.paned.clone();
        let sidebar_paned = win.sidebar.paned.clone();
        win.window.connect_close_request(move |win| {
            // Query all paned positions
            let main_paned_pos = main_paned.position();
            let side_paned_pos = sidebar_paned.position();
            {
                let mut state = state.borrow_mut();
                state.main_handle_pos = main_paned_pos;
                state.side_handle_pos = side_paned_pos;
                state.window_width = win.allocation().width;
                state.window_height = win.allocation().height;
            }
            gtk4::Inhibit(false)
        });

        win.settings.report_bx.entry.connect_changed({
            let state = self.clone();
            move|entry| {
                let path = entry.text().as_str().to_string();
                if !path.is_empty() {
                    let mut state = state.borrow_mut();
                    state.templates.clear();
                    state.templates.push(path);
                }
            }
        });
    }

}

pub fn set_window_state(user_state : &SharedUserState, queries_win : &QueriesWindow) {
    let state = user_state.borrow();
    queries_win.paned.set_position(state.main_handle_pos);
    queries_win.sidebar.paned.set_position(state.side_handle_pos);
    queries_win.window.set_default_size(state.window_width, state.window_height);
}

pub fn set_client_state(user_state : &SharedUserState, client : &QueriesClient) {
    let state = user_state.borrow();
    client.conn_set.add_connections(&state.conns);
    client.scripts.add_files(&state.scripts);
    crate::log_debug_if_required("Client updated with user state");
}

// React to all common data structures, to persist state to filesystem.
// impl React<ActiveConnection> for UserState { }

