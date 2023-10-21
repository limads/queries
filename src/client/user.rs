/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use super::{ConnectionInfo, OpenedScripts};
use filecase::OpenedFile;
use serde::{Serialize, Deserialize};
use crate::ui::QueriesWindow;
use std::cell::RefCell;
use stateful::React;
use std::rc::Rc;
use std::ops::Deref;
use gtk4::*;
use gtk4::prelude::*;
use crate::client::QueriesClient;
use filecase::MultiArchiverImpl;
use stateful::PersistentState;
use std::thread::JoinHandle;
use crate::ui::SecurityChange;
use std::fmt;
use itertools::Itertools;
use std::error::Error;
use crate::sql::SafetyLock;

pub fn display_as_json<T>(t : &T, f : &mut fmt::Formatter) -> fmt::Result
where
    T : Serialize
{
    write!(f, "{}", serde_json::to_string_pretty(&t).unwrap())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnSettings {
    pub timeout : i32,
    pub save_conns : bool,
    pub app_name : String
}

impl fmt::Display for ConnSettings {

    fn fmt(&self, f : &mut fmt::Formatter) -> fmt::Result {
        display_as_json(self, f)
    }
}

impl Default for ConnSettings {

    fn default() -> Self {
        Self {
            timeout : 10,
            save_conns : true,
            app_name : String::from("Queries")
        }
    }
    
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EditorSettings {
    pub scheme : String,
    pub font_family : String,
    pub font_size : i32,
    pub show_line_numbers : bool,
    pub highlight_current_line : bool,
    pub split_view : bool
}

impl Default for EditorSettings {

    fn default() -> Self {
        let is_dark = libadwaita::StyleManager::default().is_dark();
        let scheme = if is_dark {
            String::from("Adwaita-dark")
        } else {
            String::from("Adwaita")
        };
        Self {
            scheme,
            font_family : String::from("Source Code Pro"),
            font_size : 16,
            show_line_numbers : true,
            highlight_current_line : false,
            split_view : false
        }
    }

}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionSettings {
    pub row_limit : i32,
    
    // pub column_limit : i32,

    // Interval between scheduled executions, in seconds
    pub execution_interval : i32,

    // Statement execution timeout, in seconds
    pub statement_timeout : i32,
    
    // Whether to execute destructive ddl statements
    pub accept_ddl  : bool,
    
    // Whether to execute destructive dml statements
    pub accept_dml : bool,
    
    pub enable_async : bool,

    pub unroll_json : bool

}

impl Default for ExecutionSettings {

    fn default() -> Self {
        Self {
            row_limit : 500,
            execution_interval : 5,
            statement_timeout : 5,
            accept_ddl : false,
            accept_dml : false,
            enable_async : false,
            unroll_json : true
        }
    }

}

impl fmt::Display for ExecutionSettings {

    fn fmt(&self, f : &mut fmt::Formatter) -> fmt::Result {
        display_as_json(self, f)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct UserState {

    pub paned : filecase::PanedState,

    pub window : filecase::WindowState,

    pub conn : ConnSettings,
    
    pub scripts : Vec<OpenedFile>,
    
    pub conns : Vec<ConnectionInfo>,

    pub editor : EditorSettings,

    pub execution : ExecutionSettings,

}

impl fmt::Display for UserState {

    fn fmt(&self, f : &mut fmt::Formatter) -> fmt::Result {
        display_as_json(self, f)
    }
}

impl UserState {

    pub fn safety(&self) -> SafetyLock {
        SafetyLock {
            accept_dml : self.execution.accept_dml,
            accept_ddl : self.execution.accept_ddl,
            enable_async : self.execution.enable_async
        }
    }
    
}

const ERR_PREFIX : &'static str =
r#"Queries internal error: Integrity check violated (this is a bug and should
be reported at https://github.com/limads/queries/issues)
"#;

pub fn assert_user_state_integrity(state : &UserState) {
    match user_state_integrity_check(state) {
        Ok(_) => { },
        Err(e) => {
            panic!("{}{}", ERR_PREFIX, e)
        }
    }
}

pub fn user_state_integrity_check(state : &UserState) -> Result<(), std::boxed::Box<dyn Error>> {

    for a in state.conns.iter() {
        if a.security.tls_version.is_none() {
            if a.security.cert_path.is_some() {
                return Err(format!("Connections to {} are not encrypted but setting carries a certificate", a.host).into());
            }
            if a.security.verify_hostname.is_some() {
                return Err(format!("Connections to {} are not encrypted but setting carries a verify hostname setting", a.host).into());
            }
        }
    }

    for (a, b) in state.conns.iter().cartesian_product(state.conns.iter()) {
        if &a.host[..] == &b.host[..] {
            if a.security != b.security {
                let mismatch_msg = "Mismatch in security settings for host";
                return Err(format!("{} {}: {} vs. {}", mismatch_msg, a.host, a.security, b.security).into())
            }
        }
    }

    if !state.conn.app_name.chars().all(|c| c.is_alphanumeric() ) {
        return Err("Application name contains non-alphanumeric character(s)".into());
    }

    Ok(())
}

#[derive(Clone, Debug)]
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
            paned : filecase::PanedState { primary : 280, secondary : 320 },
            window : filecase::WindowState { width : 1440, height : 1080 },
            ..Default::default()
        })))
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

}

fn add_file(state : &SharedUserState, file : OpenedFile) {
    let mut state = state.borrow_mut();
    if let Some(path) = &file.path {
        if state.scripts.iter().find(|f| &f.path.as_ref().unwrap()[..] == &path[..] ).is_none() {
            state.scripts.push(file);
        }
    }
}

impl React<crate::ui::QueriesWindow> for SharedUserState {

    fn react(&self, win : &QueriesWindow) {
        let state = self.clone();
        let main_paned = win.paned.clone();
        let sidebar_paned = win.sidebar.paned.clone();

        // Window and paned
        win.window.connect_close_request(move |win| {
            let mut state = state.borrow_mut();
            filecase::set_win_dims_on_close(&win, &mut state.window);
            filecase::set_paned_on_close(&main_paned, &sidebar_paned, &mut state.paned);
            glib::signal::Propagation::Proceed
        });
        
        // Connection
        win.settings.conn_bx.app_name_entry.connect_changed({
            let state = self.clone();
            move|entry| {
                let name = entry.text().as_str().to_string();
                let mut state = state.borrow_mut();
                state.conn.app_name = name;
            }
        });
        win.settings.conn_bx.timeout_scale.adjustment().connect_value_changed({
            let state = self.clone();
            move |adj| {
                state.borrow_mut().conn.timeout = adj.value() as i32;
            }
        });
        win.settings.conn_bx.save_switch.connect_state_set({
            let state = self.clone();
            move|switch, _| {
                state.borrow_mut().conn.save_conns = switch.is_active();
                glib::signal::Propagation::Proceed
            }
        });

        // Execution
        win.settings.exec_bx.row_limit_spin.connect_value_changed({
            let state = self.clone();
            move |spin| {
                state.borrow_mut().execution.row_limit = spin.value() as i32;
            }
        });
        win.settings.exec_bx.schedule_scale.adjustment().connect_value_changed({
            let state = self.clone();
            move |adj| {
                state.borrow_mut().execution.execution_interval = adj.value() as i32;
            }
        });
        win.settings.exec_bx.timeout_scale.adjustment().connect_value_changed({
            let state = self.clone();
            move |adj| {
                state.borrow_mut().execution.statement_timeout = adj.value() as i32;
            }
        });
        win.settings.exec_bx.ddl_switch.connect_state_set({
            let state = self.clone();
            move|switch, _| {
                state.borrow_mut().execution.accept_ddl = switch.is_active();
                glib::signal::Propagation::Proceed
            }
        });
        win.settings.exec_bx.dml_switch.connect_state_set({
            let state = self.clone();
            move|switch, _| {
                state.borrow_mut().execution.accept_dml = switch.is_active();
                glib::signal::Propagation::Proceed
            }
        });
        win.settings.exec_bx.async_switch.connect_state_set({
            let state = self.clone();
            move|switch, _| {
                state.borrow_mut().execution.enable_async = switch.is_active();
                glib::signal::Propagation::Proceed
            }
        });
        win.settings.exec_bx.json_switch.connect_state_set({
            let state = self.clone();
            move|switch, _| {
                state.borrow_mut().execution.unroll_json = switch.is_active();
                glib::signal::Propagation::Proceed
            }
        });

        // Editor
        win.settings.editor_bx.scheme_combo.connect_changed({
            let state = self.clone();
            move |combo| {
                if let Some(txt) = combo.active_text() {
                    state.borrow_mut().editor.scheme = txt.to_string();
                }
            }
        });
        win.settings.editor_bx.split_switch.connect_state_set({
            let state = self.clone();
            move |switch, _| {
                state.borrow_mut().editor.split_view = switch.is_active();
                glib::signal::Propagation::Proceed
            }
        });
        win.settings.editor_bx.font_btn.connect_font_set({
            let state = self.clone();
            move |btn| {
                if let Some(title) = btn.font() {
                    if let Some((family, sz)) = crate::ui::parse_font(&title.to_string()) {
                        let mut s = state.borrow_mut();
                        s.editor.font_family = family;
                        s.editor.font_size = sz;
                    } else {
                        eprintln!("Failed parsing font definition");
                    }
                } else {
                    eprintln!("No font set");
                }
            }
        });
        win.settings.editor_bx.line_highlight_switch.connect_state_set({
            let state = self.clone();
            move|switch, _| {
                state.borrow_mut().editor.highlight_current_line = switch.is_active();
                glib::signal::Propagation::Proceed
            }
        });
        win.settings.editor_bx.line_num_switch.connect_state_set({
            let state = self.clone();
            move|switch, _| {
                state.borrow_mut().editor.show_line_numbers = switch.is_active();
                glib::signal::Propagation::Proceed
            }
        });

        // Security
        win.settings.security_bx.update_action.connect_activate({
            let state = self.clone();
            move |_, param| {
                if let Some(param) = param {
                    let change : SecurityChange = serde_json::from_str(&param.get::<String>().unwrap()).unwrap();
                    state.borrow_mut().conns.iter_mut().for_each(|c| {
                        crate::ui::try_modify_security_for_conn(c, &change);
                    });
                }
            }
        });
    }

}

impl PersistentState<QueriesWindow> for SharedUserState {

    fn recover(path : &str) -> Option<SharedUserState> {
        let state = filecase::load_shared_serializable(path)?;
        {
            let mut inner_state = state.borrow_mut();
            match user_state_integrity_check(&inner_state) {
                Ok(_) => {
                    inner_state.scripts.retain(|s| {
                        if let Some(p) = &s.path {
                            std::path::Path::new(&p[..]).exists()
                        } else {
                            false
                        }
                    });
                },
                Err(e) => {
                    eprintln!("User state integrity check failed (loading default instead): {}", e);
                    return None;
                }
            }
        }
        Some(SharedUserState(state))
    }

    fn persist(&self, path : &str) -> JoinHandle<bool> {
        match self.try_borrow_mut() {
            Ok(mut s) => {

                if !s.conn.app_name.chars().all(|c| c.is_alphanumeric() ) {
                    s.conn.app_name = "Queries".into();
                }

                assert_user_state_integrity(&s);

                if s.conn.save_conns {
                    s.conns.sort_by(|a, b| {
                        a.host.cmp(&b.host).then(a.database.cmp(&b.database)).then(a.user.cmp(&b.user))
                    });
                    s.conns.dedup_by(|a, b| {
                        &a.host[..] == &b.host[..] && &a.database[..] == &b.database[..] && &a.user[..] == &b.user[..]
                    });

                    s.conns.retain(|c| !ignore_save_connection(c) );

                    s.conns.iter_mut().for_each(|c| {
                        if c.port == crate::client::DEFAULT_PORT {
                            c.port.clear();
                        }
                        if c.user == crate::client::DEFAULT_USER {
                            c.user.clear();
                        }
                        if c.database == crate::client::DEFAULT_DB {
                            c.database.clear();
                        }
                    });
                } else {
                    s.conns.clear();
                }

                s.scripts.iter_mut().for_each(|script| { script.content.as_mut().map(|c| c.clear() ); } );
            },
            Err(e) => {
                eprintln!("Unable to save application state: {}", e);
            }
        }
        filecase::save_shared_serializable(&self.0, path)
    }

    fn update(&self, queries_win : &QueriesWindow) {
        
        // The cert_added action is still inert here because we haven't called react<win> for update.
        let state = self.borrow();
        queries_win.settings.security_bx.update(&state.conns);

        queries_win.paned.set_position(state.paned.primary);
        queries_win.sidebar.paned.set_position(state.paned.secondary);
        queries_win.window.set_default_size(state.window.width, state.window.height);
        if state.paned.primary == 0 {
            queries_win.titlebar.sidebar_toggle.set_active(false);
        } else {
            queries_win.titlebar.sidebar_toggle.set_active(true);
        }
        
        queries_win.settings.editor_bx.split_switch.set_state(state.editor.split_view);
        if state.editor.split_view {
            queries_win.content.switch_to_split();
        }

        queries_win.settings.conn_bx.timeout_scale.adjustment().set_value(state.conn.timeout as f64);
        queries_win.settings.conn_bx.app_name_entry.set_text(&state.conn.app_name);
        queries_win.settings.conn_bx.save_switch.set_active(state.conn.save_conns);
        
        queries_win.settings.exec_bx.row_limit_spin.adjustment().set_value(state.execution.row_limit as f64);
        queries_win.settings.exec_bx.schedule_scale.adjustment().set_value(state.execution.execution_interval as f64);
        queries_win.settings.exec_bx.timeout_scale.adjustment().set_value(state.execution.statement_timeout as f64);
        queries_win.settings.exec_bx.dml_switch.set_active(state.execution.accept_dml);
        queries_win.settings.exec_bx.ddl_switch.set_active(state.execution.accept_ddl);
        queries_win.settings.exec_bx.async_switch.set_active(state.execution.enable_async);
        queries_win.settings.exec_bx.json_switch.set_active(state.execution.unroll_json);

        let font = format!("{} {}", state.editor.font_family, state.editor.font_size);
        queries_win.settings.editor_bx.scheme_combo.set_active_id(Some(&state.editor.scheme));
        queries_win.settings.editor_bx.font_btn.set_font(&font);
        queries_win.settings.editor_bx.line_num_switch.set_active(state.editor.show_line_numbers);
        queries_win.settings.editor_bx.line_highlight_switch.set_active(state.editor.highlight_current_line);
    }

}

fn ignore_save_connection(c : &ConnectionInfo) -> bool {
    c.is_default() ||
        c.host.is_empty() ||
        c.host == crate::client::DEFAULT_HOST ||
        c.host == "file://"
}

// It would be best to move this to PersistentState::update, but the client for now is
// updated AFTER the GUI react signals have been set, so we might guarantee the GUI
// and client state are the same.
pub fn set_client_state(user_state : &SharedUserState, client : &QueriesClient) {
    let state = user_state.borrow();
    client.scripts.add_files(&state.scripts);
}

