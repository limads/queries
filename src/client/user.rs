/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use super::{ConnectionSet, ConnectionInfo, OpenedScripts};
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
use crate::ui::Certificate;
use filecase::MultiArchiverImpl;
use stateful::PersistentState;
use std::thread::JoinHandle;

use crate::sql::SafetyLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EditorSettings {
    pub scheme : String,
    pub font_family : String,
    pub font_size : i32,
    pub show_line_numbers : bool,
    pub highlight_current_line : bool
}

impl Default for EditorSettings {

    fn default() -> Self {
        Self {
            scheme : String::from("Adwaita"),
            font_family : String::from("Source Code Pro"),
            font_size : 16,
            show_line_numbers : true,
            highlight_current_line : false
        }
    }

}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecuritySettings {
    pub save_conns : bool
}

impl Default for SecuritySettings {

    fn default() -> Self {
        SecuritySettings { save_conns : true }
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
    
    pub enable_async : bool
}

impl Default for ExecutionSettings {

    fn default() -> Self {
        Self {
            row_limit : 500,
            execution_interval : 5,
            statement_timeout : 5,
            accept_ddl : false,
            accept_dml : false,
            enable_async : false
        }
    }

}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct UserState {

    pub paned : filecase::PanedState,

    pub window : filecase::WindowState,

    pub scripts : Vec<OpenedFile>,
    
    // #[serde(serialize_with = "ser_conns")]
    // #[serde(deserialize_with = "deser_conns")]
    pub conns : Vec<ConnectionInfo>,

    // #[serde(skip)]
    pub certs : Vec<Certificate>,

    pub editor : EditorSettings,

    pub execution : ExecutionSettings,

    pub security : SecuritySettings

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

impl React<super::ConnectionSet> for SharedUserState {

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
                state.conns.push(conn);

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
            gtk4::Inhibit(false)
        });

        // Report
        /*win.settings.report_bx.entry.connect_changed({
            let state = self.clone();
            move|entry| {
                let path = entry.text().as_str().to_string();
                if !path.is_empty() {
                    let mut state = state.borrow_mut();
                    state.templates.clear();
                    state.templates.push(path);
                }
            }
        });*/

        // Execution
        win.settings.exec_bx.row_limit_spin.adjustment().connect_value_changed({
            let state = self.clone();
            move |adj| {
                state.borrow_mut().execution.row_limit = adj.value() as i32;
            }
        }); 
        /*win.settings.exec_bx.col_limit_spin.adjustment().connect_value_changed({
            let state = self.clone();
            move |adj| {
                state.borrow_mut().execution.column_limit = adj.value() as i32;
            }
        });*/
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
                Inhibit(false)
            }
        });
        win.settings.exec_bx.dml_switch.connect_state_set({
            let state = self.clone();
            move|switch, _| {
                state.borrow_mut().execution.accept_dml = switch.is_active();
                Inhibit(false)
            }
        });
        win.settings.exec_bx.async_switch.connect_state_set({
            let state = self.clone();
            move|switch, _| {
                state.borrow_mut().execution.enable_async = switch.is_active();
                Inhibit(false)
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
                Inhibit(false)
            }
        });
        win.settings.editor_bx.line_num_switch.connect_state_set({
            let state = self.clone();
            move|switch, _| {
                state.borrow_mut().editor.show_line_numbers = switch.is_active();
                Inhibit(false)
            }
        });

        // Security
        win.settings.security_bx.save_switch.connect_state_set({
            let state = self.clone();
            move|switch, _| {
                state.borrow_mut().security.save_conns = switch.is_active();
                Inhibit(false)
            }
        });
        win.settings.security_bx.cert_added.connect_activate({
            let state = self.clone();
            move |_, param| {
                if let Some(s) = param {
                    let cert_str = s.get::<String>().unwrap();
                    let new_cert : Certificate = serde_json::from_str(&cert_str).unwrap();
                    let mut state = state.borrow_mut();

                    // Set this certificate to existing conns.
                    let mut conn_iter = state.conns
                        .iter_mut()
                        .filter(|conn| &conn.host[..] == &new_cert.host[..] );
                    while let Some(conn) = conn_iter.next() {
                        conn.cert = Some(new_cert.cert.clone());
                        conn.is_tls = Some(new_cert.is_tls);
                        conn.mode = Some(new_cert.mode);
                    }
                    
                    // Set this certificate to the certificate vector.
                    let no_duplicates = state.certs.iter()
                        .find(|c| &c.cert[..] == &new_cert.cert[..] && &c.host[..] == &new_cert.host[..] )
                        .is_none();
                    if no_duplicates {
                        state.certs.push(new_cert);
                    }
                    
                }
            }
        });
        win.settings.security_bx.cert_removed.connect_activate({
            let state = self.clone();
            move |_, param| {
                if let Some(s) = param {
                    let cert : Certificate = serde_json::from_str(&s.get::<String>().unwrap()).unwrap();
                    let mut state = state.borrow_mut();
                    for conn in state.conns.iter_mut().filter(|c| c.host == cert.host ) {
                        conn.cert = None;
                        conn.is_tls = None;
                        conn.mode = None;
                    }

                    for i in (0..state.certs.len()).rev() {
                        if &state.certs[i].cert[..] == &cert.cert[..] && &state.certs[i].host[..] == &cert.host[..] {
                            state.certs.remove(i);
                        }
                    }
                    
                }
            }
        });
    }

}

impl PersistentState<QueriesWindow> for SharedUserState {

    fn recover(path : &str) -> Option<SharedUserState> {
        Some(SharedUserState(filecase::load_shared_serializable(path)?))
    }

    fn persist(&self, path : &str) -> JoinHandle<bool> {
        let _ = self.try_borrow_mut().and_then(|mut s| {

            if s.security.save_conns {
                s.conns.sort_by(|a, b| {
                    a.host.cmp(&b.host).then(a.database.cmp(&b.database)).then(a.user.cmp(&b.user))
                });
                s.conns.dedup_by(|a, b| {
                    &a.host[..] == &b.host[..] && &a.database[..] == &b.database[..] && &a.user[..] == &b.user[..]
                });

                // Only preserve connections that have been accepted at least once.
                s.conns.retain(|c| !c.is_default() && !c.host.is_empty() && !c.database.is_empty() && !c.user.is_empty() && c.dt.is_some() );
            } else {
                s.conns.clear();
                s.certs.clear();
            }
            
            s.scripts.iter_mut().for_each(|script| { script.content.as_mut().map(|c| c.clear() ); } );
            Ok(())
        });
        filecase::save_shared_serializable(&self.0, path)
    }

    fn update(&self, queries_win : &QueriesWindow) {
        
        // The cert_added action is still inert here because we haven't called react<win> for update.
        let state = self.borrow();
        for cert in &state.certs {
            crate::ui::append_certificate_row(
                queries_win.settings.security_bx.exp_row.clone(),
                &cert.host,
                &cert.cert,
                &cert.mode,
                cert.is_tls,
                &queries_win.settings.security_bx.rows,
                &queries_win.settings.security_bx.cert_added,
                &queries_win.settings.security_bx.cert_removed
            );
        }

        queries_win.paned.set_position(state.paned.primary);
        queries_win.sidebar.paned.set_position(state.paned.secondary);
        queries_win.window.set_default_size(state.window.width, state.window.height);
        if state.paned.primary == 0 {
            queries_win.titlebar.sidebar_toggle.set_active(false);
        } else {
            queries_win.titlebar.sidebar_toggle.set_active(true);
        }
        
        queries_win.settings.exec_bx.row_limit_spin.adjustment().set_value(state.execution.row_limit as f64);
        queries_win.settings.exec_bx.schedule_scale.adjustment().set_value(state.execution.execution_interval as f64);
        queries_win.settings.exec_bx.timeout_scale.adjustment().set_value(state.execution.statement_timeout as f64);
        queries_win.settings.exec_bx.dml_switch.set_active(state.execution.accept_dml);
        queries_win.settings.exec_bx.ddl_switch.set_active(state.execution.accept_ddl);
        queries_win.settings.exec_bx.async_switch.set_active(state.execution.enable_async);

        let font = format!("{} {}", state.editor.font_family, state.editor.font_size);
        queries_win.settings.editor_bx.scheme_combo.set_active_id(Some(&state.editor.scheme));
        queries_win.settings.editor_bx.font_btn.set_font(&font);
        queries_win.settings.editor_bx.line_num_switch.set_active(state.editor.show_line_numbers);
        queries_win.settings.editor_bx.line_highlight_switch.set_active(state.editor.highlight_current_line);
        
        queries_win.settings.security_bx.save_switch.set_active(state.security.save_conns);
    }

}

// It would be best to move this to PersistentState::update, but the client for now is
// updated AFTER the GUI react signals have been set, so we might guarantee the GUI
// and client state are the same.
pub fn set_client_state(user_state : &SharedUserState, client : &QueriesClient) {
    let state = user_state.borrow();
    client.conn_set.add_connections(&state.conns);
    client.conn_set.add_certificates(&state.certs);
    client.scripts.add_files(&state.scripts);
}

// React to all common data structures, to persist state to filesystem.
// impl React<ActiveConnection> for UserState { }

