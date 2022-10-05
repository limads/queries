/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use gtk4::*;
use gtk4::prelude::*;
use stateful::{React, Callbacks};
use crate::ui::ConnectionList;
use crate::ui::ConnectionBox;
use std::boxed;
use glib::MainContext;
use std::collections::HashMap;
use super::listener::SqlListener;
use crate::server::*;
use std::thread;
use crate::sql::object::{DBInfo, DBDetails};
use crate::sql::StatementOutput;
use crate::ui::ExecButton;
use chrono::prelude::*;
use serde::{Serialize, Deserialize};
use std::rc::Rc;
use std::cell::RefCell;
use crate::ui::QueriesWindow;
use crate::sql::object::DBObject;
use crate::ui::{SchemaTree};
use crate::sql::object::DBType;
use crate::sql::copy::*;
use std::time::Duration;
use std::hash::Hash;
use crate::client::SharedUserState;
use super::listener::ExecMode;
use crate::tables::table::Table;
use crate::ui::Certificate;

// The actual connection info that is persisted on disk (excludes password for obvious
// security reasons).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct ConnectionInfo {

    // Holds either a host-only string (assuming default 5432 port)
    // or host:port string.
    pub host : String,

    // PostgreSQL user.
    pub user : String,

    // Database name.
    pub database : String,

    // Database details, queried automatically by the application every time
    // there is a new connection to the database. If this query fails, holds None.
    // This information is also persisted in disk.
    // pub details : Option<DBDetails>,

    // Optional path to certificate
    pub cert : Option<String>,
    
    pub is_tls : Option<bool>,

    // When this connection was last established (datetime-formatted).
    pub dt : Option<String>

}

#[derive(Debug, Clone)]
pub struct ConnConfig {

    // Statement timeout, in milliseconds.
    pub timeout : usize

}

const DEFAULT_HOST : &'static str = "Host:Port";

const DEFAULT_USER : &'static str = "User";

const DEFAULT_DB : &'static str = "Database";

impl ConnectionInfo {

    pub fn is_default(&self) -> bool {
        &self.host[..] == DEFAULT_HOST && &self.user[..] == DEFAULT_USER && &self.database[..] == DEFAULT_DB
    }

    pub fn is_like(&self, other : &Self) -> bool {
        &self.host[..] == &other.host[..] && &self.user[..] == &other.user[..] && &self.database[..] == &other.database[..]
    }

}

impl Default for ConnectionInfo {

    fn default() -> Self {
        Self {
            host : String::from(DEFAULT_HOST),
            user : String::from(DEFAULT_USER),
            database : String::from(DEFAULT_DB),
            dt : None,
            // details : None,
            cert : None,
            is_tls : None
        }
    }

}

pub enum ConnectionChange {
    Add(ConnectionInfo),
    Remove(usize)
}

#[derive(Debug, Clone)]
pub enum ConnectionAction {
    Switch(Option<i32>),
    Add(Option<ConnectionInfo>),
    Update(ConnectionInfo),
    UpdateHost(String),
    UpdateUser(String),
    UpdateDB(String),
    EraseCertificate(String),
    // QueryCertificate(String),
    AddCertificate(Certificate),
    Remove(i32),
    CloseWindow
    // ViewState(Vec<ConnectionInfo>)
}

// TODO rename to ConnectionHistory.
pub struct ConnectionSet {

    final_state : Rc<RefCell<Vec<ConnectionInfo>>>,

    added : Callbacks<ConnectionInfo>,

    removed : Callbacks<i32>,

    updated : Callbacks<(i32, ConnectionInfo)>,

    selected : Callbacks<Option<(i32, ConnectionInfo)>>,

    // on_view : Callbacks<Vec<ConnectionInfo>>,

    pub(super) send : glib::Sender<ConnectionAction>

}

pub type ConnSetTypes = (
    Callbacks<Option<(i32, ConnectionInfo)>>,
    Callbacks<ConnectionInfo>,
    Callbacks<(i32, ConnectionInfo)>,
    Callbacks<i32>
);

impl ConnectionSet {

    pub fn final_state(&self) -> Rc<RefCell<Vec<ConnectionInfo>>> {
        self.final_state.clone()
    }

    pub fn add_certificates(&self, certs : &[Certificate]) {
        for cert in certs {
            self.send.send(ConnectionAction::AddCertificate(cert.clone()));
        }
    }
    
    pub fn add_connections(&self, conns : &[ConnectionInfo]) {
        for conn in conns.iter() {
            if !conn.host.is_empty() && !conn.database.is_empty() && !conn.user.is_empty() {
                self.send.send(ConnectionAction::Add(Some(conn.clone())));
            }
        }
    }

    pub fn new() -> Self {
        let (send, recv) = MainContext::channel::<ConnectionAction>(glib::source::PRIORITY_DEFAULT);
        let (selected, added, updated, removed) : ConnSetTypes = Default::default();
        // let on_view : Callbacks<Vec<ConnectionInfo>> = Default::default();
        let final_state = Rc::new(RefCell::new(Vec::new()));
        recv.attach(None, {

            // Holds the set of connections added by the user. This is synced to the
            // Connections list seen by the user on startup. The connections are
            // set to the final state just before the window closes.
            let mut conns : (Vec<ConnectionInfo>, Option<i32>) = (Vec::new(), None);

            let (selected, added, updated, removed) = (selected.clone(), added.clone(), updated.clone(), removed.clone());
            let final_state = final_state.clone();
            // let on_view = on_view.clone();
            let mut certs = Vec::new();
            move |action| {
                match action {
                    ConnectionAction::Switch(opt_ix) => {
                        conns.1 = opt_ix;
                        selected.call(opt_ix.map(|ix| (ix, conns.0[ix as usize].clone() )));
                    },
                    
                    /* This is called at startup via UserState::update->append_certificate_row->cert_added.activate,
                    which is also called every time the user presses the add certificate button. */
                    ConnectionAction::AddCertificate(cert) => {
                        for conn in &mut conns.0[..] {
                            if &conn.host[..] == &cert.host[..] {
                                conn.is_tls = Some(cert.is_tls);
                                conn.cert = Some(cert.cert.clone());
                            }
                        }
                        certs.push(cert);
                    },
                    
                    ConnectionAction::EraseCertificate(host) => {
                        for conn in &mut conns.0[..] {
                            if &conn.host[..] == &host[..] {
                                conn.cert = None;
                                conn.is_tls = None;
                            }
                        }
                        for i in (0..(certs.len())).rev() {
                            if &certs[i].host[..] == &host[..] {
                                certs.remove(i);
                            }
                        }
                    },
                    ConnectionAction::Add(opt_conn) => {
                        /*if let Some(conn) = &opt_conn {
                            if !conn.is_like(&ConnectionInfo::default()) && conns.0.iter().find(|c| c.is_like(&conn) ).is_some() {
                                return Continue(true);
                            }
                        }*/

                        // If the user clicked the 'plus' button, this will be None. If the connection
                        // was added from the settings file, there will be a valid value here.
                        let mut conn = opt_conn.unwrap_or_default();
                        update_certificate(&mut conn, &certs);
                        
                        conns.0.push(conn.clone());

                        // The selection will be re-set when the list triggers the callback at connect_added.
                        conns.1 = None;

                        added.call(conn.clone());
                    },
                    
                    ConnectionAction::UpdateHost(host) => {
                        if let Some(ix) = conns.1 {
                            conns.0[ix as usize].host = host;
                        }
                    },
                    
                    ConnectionAction::UpdateUser(user) => {
                        if let Some(ix) = conns.1 {
                            conns.0[ix as usize].user = user;
                        }
                    },
                    
                    ConnectionAction::UpdateDB(db) => {
                        if let Some(ix) = conns.1 {
                            conns.0[ix as usize].database = db;
                        }
                    },
                    
                    // Called when the user connects to the database
                    // and the date field is set at ActiveConnection::Accepted.
                    ConnectionAction::Update(mut info) => {

                        // On update, it might be the case the info is the same as some
                        // other connection. Must decide how to resolve duplicates (perhaps
                        // remove old one by sending ConnectionAction::remove(other_equal_ix)?).
                        if let Some(ix) = conns.1 {
                            
                            info.dt = Some(Local::now().to_string());
                            
                            conns.0[ix as usize] = info;
                            update_certificate(&mut conns.0[ix as usize], &certs);
                            
                            updated.call((ix, conns.0[ix as usize].clone()));
                        } else {
                            eprintln!("No connection selected");
                        }
                    },
                    
                    ConnectionAction::Remove(ix) => {
                        let _rem_conn = conns.0.remove(ix as usize);
                        removed.call(ix);
                        selected.call(None);
                    },
                    ConnectionAction::CloseWindow => {

                        final_state.replace(conns.0.clone());
                    }
                }
                Continue(true)
            }
        });
        Self {
            send,
            selected,
            added,
            updated,
            removed,
            final_state
        }
    }

    pub fn connect_added(&self, f : impl Fn(ConnectionInfo) + 'static) {
        self.added.bind(f);
    }

    pub fn connect_updated(&self, f : impl Fn((i32, ConnectionInfo)) + 'static) {
        self.updated.bind(f);
    }

    pub fn connect_removed(&self, f : impl Fn(i32) + 'static) {
        self.removed.bind(f);
    }

    pub fn connect_selected(&self, f : impl Fn(Option<(i32, ConnectionInfo)>) + 'static) {
        self.selected.bind(f);
    }

}

fn update_certificate(conn : &mut ConnectionInfo, certs : &[Certificate]) {
    if !conn.is_default() {
        if let Some(cert) = certs.iter().find(|c| &c.host[..] == &conn.host[..] ) {
            conn.cert = Some(cert.cert.clone());
            conn.is_tls = Some(cert.is_tls);
        }
    }
}

impl React<ConnectionBox> for ConnectionSet {

    fn react(&self, conn_bx : &ConnectionBox) {
        conn_bx.host.entry.connect_changed({
            let send = self.send.clone();
            move |entry| {
                let txt = entry.text().to_string();
                if &txt[..] != "" {
                    send.send(ConnectionAction::UpdateHost(txt));
                } else {
                    send.send(ConnectionAction::UpdateHost("Host:Port".to_string()));
                }
                
            }
        });
        conn_bx.user.entry.connect_changed({
            let send = self.send.clone();
            move |entry| {
                let txt = entry.text().to_string();
                if &txt[..] != "" {
                    send.send(ConnectionAction::UpdateUser(txt));
                } else {
                    send.send(ConnectionAction::UpdateUser("User".to_string()));
                }
                
            }
        });
        conn_bx.db.entry.connect_changed({
            let send = self.send.clone();
            move |entry| {
                let txt = entry.text().to_string();
                if &txt[..] != "" {
                    send.send(ConnectionAction::UpdateDB(txt));
                } else {
                    send.send(ConnectionAction::UpdateDB("Database".to_string()));
                }
               
            }
        });
    }
    
}

impl React<ConnectionList> for ConnectionSet {

    fn react(&self, conn_list : &ConnectionList) {
        conn_list.list.connect_row_selected({
            let send = self.send.clone();
            move |_, opt_row| {
                send.send(ConnectionAction::Switch(opt_row.map(|row| row.index() ))).unwrap();
            }
        });
        conn_list.add_btn.connect_clicked({
            let send = self.send.clone();
            move |_btn| {
                send.send(ConnectionAction::Add(None)).unwrap();
            }
        });
        conn_list.remove_btn.connect_clicked({
            let send = self.send.clone();
            let list = conn_list.list.clone();
            move |_btn| {
                if let Some(ix) = list.selected_row().map(|row| row.index() ) {
                    send.send(ConnectionAction::Remove(ix)).unwrap();
                }
            }
        });
    }

}

impl React<ActiveConnection> for ConnectionSet {

    fn react(&self, conn : &ActiveConnection) {
        let send = self.send.clone();
        conn.connect_db_connected(move |(conn_info, _)| {
            send.send(ConnectionAction::Update(conn_info));
        });
        let send = self.send.clone();
        conn.connect_db_conn_failure(move |(conn_info, _)| {
            send.send(ConnectionAction::Update(conn_info));
        });
    }

}

impl React<QueriesWindow> for ConnectionSet {

    fn react(&self, win : &QueriesWindow) {
        let send = self.send.clone();
        win.window.connect_close_request(move |_win| {
            send.send(ConnectionAction::CloseWindow);
            Inhibit(false)
        });
        win.settings.security_bx.cert_removed.connect_activate({
            let send = self.send.clone();
            move |_, param| {
                if let Some(s) = param {
                    let cert : Certificate = serde_json::from_str(&s.get::<String>().unwrap()).unwrap();
                    send.send(ConnectionAction::EraseCertificate(cert.host.to_string()));
                }
           }
       });
       win.settings.security_bx.cert_added.connect_activate({
            let send = self.send.clone();
            move |_, param| {
                if let Some(s) = param {
                    let cert : Certificate = serde_json::from_str(&s.get::<String>().unwrap()).unwrap();
                    send.send(ConnectionAction::AddCertificate(cert));
                }
           }
       });
    }

}

// View connection URI spec at
// https://www.postgresql.org/docs/current/libpq-connect.html
fn validate_conn_info() {
    // Connection URI postgresql://[userspec@][hostspec][/dbname][?paramspec]
}

// #[test]
fn conn_str_test() -> Result<(), std::boxed::Box<dyn std::error::Error>> {

    // Reference: https://www.postgresql.org/docs/current/libpq-connect.html
    // Eventually the settings GUI might include those URI parameters as well.
    // "postgresql://other@localhost/otherdb?connect_timeout=10&application_name=myapp",
    // "postgresql://host1:123,host2:456/somedb?target_session_attrs=any&application_name=myapp"
    
    // let info_noport = ConnectionInfo { host : format!("localhost"), user : format!("user"), database : format!("mydb"), ..Default::default() };
    // let uri = ConnURI::new(info_noport, "secret").unwrap();
    // assert!(&uri.uri[..] == "postgresql://user:secret@localhost:5432/mydb");
    
    let info_port = ConnectionInfo { host : format!("localhost:1234"), user : format!("user2"), database : format!("mydb2"), ..Default::default() };
    let uri_port = ConnURI::new(info_port, "secret2").unwrap();
    assert!(&uri_port.uri[..] == "postgresql://user2:secret2@localhost:1234/mydb2");

    Ok(())

}

pub fn is_local(info : &ConnectionInfo) -> Option<bool> {
    if let Some(fst_part) = info.host.split(":").next() {
        Some(fst_part.trim() == "127.0.0.1" || fst_part.trim() == "localhost")
    } else {
        None
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Security {
    TLS,
    SSL,
    None
}

/// Short-lived data structure used to collect information from the connection
/// form. The URI contains all the credentials (including password) to connect
/// to the database. Only the info field is persisted in the client component
/// memory after the connection is established (or establishing it fails). This
/// structure should never implement serialize, and/or be persisted to disk.
#[derive(Debug, Clone)]
pub struct ConnURI {
    pub info : ConnectionInfo,
    pub uri : String
}

impl ConnURI {

    /* Builds a connection URI from the GTK widgets */
    pub fn new(
        info : ConnectionInfo,
        password : &str
    ) -> Result<ConnURI, String> {
        
        if info.user.chars().any(|c| c == ':' ) {
            return Err(String::from("User field cannot contain ':' character"));
        }
        
        // if info.host.chars().any(|c| c == ':' ) {
        //    return Err(String::from("Host field cannot contain ':' character"));
        // }

        let mut uri = "postgresql://".to_owned();
        uri += &info.user;
        uri += ":";

        if password.is_empty() {
            return Err(format!("Missing password"));
        }
        uri += password;

        uri += "@";
        let split_port : Vec<&str> = info.host.split(":").collect();
        let (host_prefix, port) = match split_port.len() {
            2 => {
                (split_port[0], split_port[1])
            },
            _n => {
                return Err(format!("Invalid host value (expected host:port format)\n(ex. 127.0.0.1:5432"));
            }
        };
        uri += host_prefix;
        uri += ":";
        // if let Some(p) = port {
        //    uri += p;
        // } else {
        uri += port;
        // }
        uri += "/";
        uri += &info.database;
        
        /*// sslmode=verify-ca/verify-full
        let local = is_local(&info)
            .ok_or(String::from("Could not determine if host is local"))?;
        if !local {
            uri += "?sslmode=require";
        }*/
        
        Ok(ConnURI { info, uri })
    }

}

/* Extract connection info from GTK widgets (except password, which is held separately 
at the uri field of ConnURI. */
fn extract_conn_info(host_entry : &Entry, db_entry : &Entry, user_entry : &Entry) -> Result<ConnectionInfo, String> {
    let mut host_s = host_entry.text().as_str().to_owned();
    if host_s.is_empty() {
        return Err(format!("Missing host"));
    }
    let db_s = db_entry.text().as_str().to_owned();
    if db_s.is_empty() {
        return Err(format!("Missing database"));
    }
    let user_s = user_entry.text().as_str().to_owned();
    if user_s.is_empty() {
        return Err(format!("Missing user"));
    }
    let mut info : ConnectionInfo = Default::default();
    info.host = host_s.to_string();
    info.database = db_s.to_string();
    info.user = user_s.to_string();
    Ok(info)
}

fn generate_conn_uri_from_entries(
    host_entry : &Entry,
    db_entry : &Entry,
    user_entry : &Entry,
    password_entry : &PasswordEntry
) -> Result<ConnURI, String> {
    let info = extract_conn_info(host_entry, db_entry, user_entry)?;
    let pwd = password_entry.text().as_str().to_owned();
    ConnURI::new(info, &pwd)
}

pub enum ErrorKind {

    Client,

    Server,

    EstablishConnection

}

pub enum ActiveConnectionAction {

    ConnectRequest(ConnURI),

    ConnectAccepted(boxed::Box<dyn Connection>, Option<DBInfo>),

    ConnectFailure(ConnectionInfo, String),

    Disconnect,

    // Requires an arbitrary sequence of SQL commands.
    ExecutionRequest(String),

    // Requires a sigle table or view name to do a single SQL query.
    SingleQueryRequest,

    StartSchedule(String),

    EndSchedule,

    ExecutionCompleted(Vec<StatementOutput>),

    SingleQueryCompleted(StatementOutput),

    SchemaUpdate(Option<Vec<DBObject>>),

    ObjectSelected(Option<Vec<usize>>),

    /// Carries path to CSV file.
    TableImport(String),

    Error(String)

}

pub type ActiveConnCallbacks = (Callbacks<(ConnectionInfo, Option<DBInfo>)>, Callbacks<()>, Callbacks<String>);

pub struct ActiveConnection {

    on_connected : Callbacks<(ConnectionInfo, Option<DBInfo>)>,

    on_conn_failure : Callbacks<(ConnectionInfo, String)>,
    
    on_disconnected : Callbacks<()>,

    on_error : Callbacks<String>,

    on_exec_result : Callbacks<Vec<StatementOutput>>,

    on_single_query_result : Callbacks<Table>,

    send : glib::Sender<ActiveConnectionAction>,

    on_schema_invalidated : Callbacks<()>,
    
    on_schema_update : Callbacks<Option<Vec<DBObject>>>,

    on_object_selected : Callbacks<Option<DBObject>>

}

// pub struct ActiveConnState {
// }

impl ActiveConnection {

    //pub fn final_state(&self) -> ActiveConnState {
    //    ActiveConnState { }
    //}

    pub fn sender(&self) -> &glib::Sender<ActiveConnectionAction> {
        &self.send
    }

    pub fn send(&self, msg : ActiveConnectionAction) {
        self.send.send(msg);
    }

    pub fn new(user_state : &SharedUserState) -> Self {
        let (on_connected, on_disconnected, on_error) : ActiveConnCallbacks = Default::default();
        let on_exec_result : Callbacks<Vec<StatementOutput>> = Default::default();
        let on_single_query_result : Callbacks<Table> = Default::default();
        let on_conn_failure : Callbacks<(ConnectionInfo, String)> = Default::default();
        let (send, recv) = glib::MainContext::channel::<ActiveConnectionAction>(glib::source::PRIORITY_DEFAULT);
        let on_schema_update : Callbacks<Option<Vec<DBObject>>> = Default::default();
        let on_object_selected : Callbacks<Option<DBObject>> = Default::default();
        let on_schema_invalidated : Callbacks<()> = Default::default();
        
        let mut schema_valid = true;
        
        /* Active schedule, unlike the other state variables, needs to be wrapped in a RefCell
        because it is shared with any new callbacks that start when the user schedule a set of statements. */
        let active_schedule = Rc::new(RefCell::new(false));
        
        // Thread that waits for SQL statements via the standard library mpsc channels (with a
        // single producer).
        let mut listener = SqlListener::launch({
            let send = send.clone();
            move |mut results, mode| {
                match mode {
                    ExecMode::Single => {
                        send.send(ActiveConnectionAction::SingleQueryCompleted(results.remove(0))).unwrap();
                    },
                    ExecMode::Multiple => {
                        send.send(ActiveConnectionAction::ExecutionCompleted(results)).unwrap();
                    }
                }
            }
        });
        
        /* Keeps the current database schema. Must be Some(schema) when connected,
        or None when not connected OR database information could not be received after
        connection (those two conditions aren't discriminated). Potentially updated when
        queries executes a DDL statement (create table, create view...). */
        let mut schema : Option<Vec<DBObject>> = None;
        
        /* Keeps the currently-selected object at the schema tree (might be a table, view,
        column or schema. Must necessarily be a node of the schema variable above. */
        let mut selected_obj : Option<DBObject> = None;
        
        recv.attach(None, {
            let send = send.clone();
            let (on_connected, on_disconnected, on_error, on_exec_result, on_single_query_result) = (
                on_connected.clone(),
                on_disconnected.clone(),
                on_error.clone(),
                on_exec_result.clone(),
                on_single_query_result.clone()
            );
            let on_conn_failure = on_conn_failure.clone();
            let on_object_selected = on_object_selected.clone();
            let on_schema_update = on_schema_update.clone();
            let on_schema_invalidated = on_schema_invalidated.clone();
            let user_state = (*user_state).clone();
            move |action| {
                match action {

                    // At this stage, the connection URI was successfully parsed, 
                    // but the connection hasn't been established yet. This URI is captured from
                    // the entries, so no certificate is associated with it yet.
                    ActiveConnectionAction::ConnectRequest(mut uri) => {

                        // Spawn a thread that captures the database connection URI. The URI
                        // and the sensitive information (password) is forgotten when this thread dies.
                        thread::spawn({
                            let send = send.clone();
                            let us = user_state.borrow();
                            if let Some(cert) = us.certs.iter().find(|cert| &cert.host[..] == &uri.info.host[..] ) {
                                uri.info.cert = Some(cert.cert.clone());
                                uri.info.is_tls = Some(cert.is_tls);
                            }
                            
                            let timeout_secs = us.execution.statement_timeout;
                            move || {
                                match PostgresConnection::try_new(uri.clone()) {
                                    Ok(mut conn) => {
                                    
                                        let db_info = match conn.db_info() {
                                            Ok(info) => Some(info),
                                            Err(e) => {
                                                None
                                            }
                                        };
                                        
                                        if timeout_secs > 0 {
                                            conn.configure(ConnConfig {
                                                timeout : timeout_secs as usize * 1000
                                            });
                                        }

                                        // From now on, the URI is forgotten (no password is kept in memory anymore), and only the
                                        // database info and details are sent back to the main thread.
                                        send.send(ActiveConnectionAction::ConnectAccepted(boxed::Box::new(conn), db_info)).unwrap();
                                    },
                                    Err(e) => {
                                        send.send(ActiveConnectionAction::ConnectFailure(uri.info.clone(), e)).unwrap();
                                    }
                                }
                            }
                        });
                    },

                    // At this stage, the connection is active, and the URI is already
                    // forgotten.
                    ActiveConnectionAction::ConnectAccepted(conn, db_info) => {
                        
                        schema = db_info.as_ref().map(|info| info.schema.clone() );
                        selected_obj = None;
                        let info = conn.conn_info();
                        if let Err(e) = listener.update_engine(conn) {
                            eprintln!("{}", e);
                        }
                        // if let Some(info) = &mut db_info {
                        //    info.info.dt = Some(Local::now().to_string());
                        // }
                        on_connected.call((info, db_info));
                    },
                    
                    ActiveConnectionAction::Disconnect => {
                        schema = None;
                        selected_obj = None;
                        active_schedule.replace(false);
                        on_disconnected.call(());
                    },
                    
                    // When the user clicks the exec button or activates the execute action.
                    ActiveConnectionAction::ExecutionRequest(stmts) => {
                    
                        if !schema_valid {
                            on_error.call(format!("Cannot execute command right now (schema update pending)"));
                            return glib::Continue(true);
                        }

                        if *(active_schedule.borrow()) {
                            on_error.call(format!("Attempted to execute statement during active schedule"));
                            return glib::Continue(true);
                        }
                        
                        if listener.is_running() {
                            // This shouldn't happen. The user is prevented from sending statements
                            // when the engine is working.
                            on_error.call(format!("Previous statement not completed yet."));
                            return glib::Continue(true);
                        }

                        let us = user_state.borrow();
                        match listener.send_commands(stmts, HashMap::new(), us.safety(), false, us.execution.statement_timeout as usize) {
                            Ok(_) => { },
                            Err(e) => {
                                on_error.call(e.clone());
                            }
                        }
                    },
                    
                    // SingleQueryRequest is used when the schema tree is useed to generate a report.
                    ActiveConnectionAction::SingleQueryRequest => {
                    
                        if !schema_valid {
                            on_error.call(format!("Cannot execute command right now (schema update pending)"));
                            return glib::Continue(true);
                        }
                        
                        if *(active_schedule.borrow()) {
                            on_error.call(format!("Attempted to execute statement during active schedule"));
                            return glib::Continue(true);
                        }
                        
                        if listener.is_running() {
                            // This shouldn't happen. The user is prevented from sending statements
                            // when the engine is working.
                            on_error.call(format!("Previous statement not completed yet."));
                            return glib::Continue(true);
                        }
                        
                        match &selected_obj {
                            Some(DBObject::View { schema, name, .. }) | Some(DBObject::Table { schema, name, .. }) => {
                                let cmd = format!("select * from {schema}.{name};");
                                let timeout = user_state.borrow().execution.statement_timeout as usize;
                                let us = user_state.borrow();
                                match listener.send_single_command(cmd, timeout, us.safety()) {
                                    Ok(_) => { },
                                    Err(e) => {
                                        on_error.call(e.clone());
                                    }
                                }
                            },
                            _ => { }
                        }
                    },
                    
                    // Execute action was clicked while execution mode is set to scheduled.
                    ActiveConnectionAction::StartSchedule(stmts) => {
                    
                        if *(active_schedule.borrow()) {
                            on_error.call(format!("Tried to start schedule twice"));
                            return glib::Continue(true);
                        }
                        
                        active_schedule.replace(true);
                        glib::timeout_add_local(Duration::from_secs(user_state.borrow().execution.execution_interval as u64), {
                            let active_schedule = active_schedule.clone();
                            let on_error = on_error.clone();
                            let listener = listener.clone();
                            let user_state = user_state.clone();
                            move || {

                                // Just ignore this schedule step if the previous statement is not
                                // executed yet. Queries will try to execute it again at the next timeout interval.
                                if listener.is_running() {
                                    return Continue(true);
                                }

                                let us = user_state.borrow();
                                let send_ans = listener.send_commands(
                                    stmts.clone(),
                                    HashMap::new(),
                                    us.safety(),
                                    true,
                                    us.execution.statement_timeout as usize
                                );
                                match send_ans {
                                    Ok(_) => { },
                                    Err(e) => {
                                        on_error.call(e.clone());
                                    }
                                }
                                let should_continue = *active_schedule.borrow();
                                Continue(should_continue)
                            }
                        });
                    },
                    
                    // Execution was un-toggled in scheduled mode.
                    ActiveConnectionAction::EndSchedule => {
                    
                        if !*(active_schedule.borrow()) {
                            on_error.call(format!("Tried to end schedule, but there is no active schedule."));
                            return glib::Continue(true);
                        }
                        
                        active_schedule.replace(false);
                    },
                    
                    // Table import at the schema tree.
                    ActiveConnectionAction::TableImport(csv_path) => {
                        if let Some(obj) = &selected_obj {
                            match obj {
                                DBObject::Table { name, .. } => {
                                    let copy = Copy {
                                        table : name.clone(),
                                        target : CopyTarget::From,
                                        cols : Vec::new(),
                                        options : String::new(),
                                        client : CopyClient::Stdio
                                    };
                                    let send = send.clone();
                                    listener.on_import_request_done(csv_path, copy, move |ans| {
                                        match ans {
                                            Ok(n) => {
                                                let msg = format!("{} row(s) imported", n);
                                                send.send(ActiveConnectionAction::ExecutionCompleted(vec![StatementOutput::Statement(msg)]));
                                            },
                                            Err(e) => {
                                                send.send(ActiveConnectionAction::Error(e));
                                            }
                                        }
                                    });
                                },
                                _ => { }
                            }
                        }
                    },
                    
                    // A new set of results arrived to the client.
                    ActiveConnectionAction::ExecutionCompleted(results) => {
                    
                        // assert!(!listener.is_running());
                        
                        let fst_error = results.iter()
                            .filter_map(|res| {
                                match res {
                                    StatementOutput::Invalid(e, _) => Some(e.clone()),
                                    _ => None
                                }
                            }).next();
                        if let Some(error) = fst_error {
                            on_error.call(error.clone());
                        } else {
                            on_exec_result.call(results.clone());
                        }
                        
                        // This will block any new user statements until the schema information is updated.
                        // If a new statement is issued at the on_exec_result callback, the info will only
                        // be updated when all recursive calls are done (used during testing). Ideally, should
                        // block execution of any new statements until schematree is updated with the catalog
                        // changes.
                        let any_schema_updates = results.iter()
                            .find(|res| {
                                match res {
                                    StatementOutput::Modification(_) => true,
                                    _ => false
                                }
                            }).is_some();
                        if any_schema_updates {
                            schema_valid = false;
                            on_schema_invalidated.call(());
                            let send = send.clone();
                            listener.spawn_db_info(move |info| {
                                send.send(ActiveConnectionAction::SchemaUpdate(info));
                            });
                        }
                        
                    },
                    
                    // Results arrived from a report request.
                    ActiveConnectionAction::SingleQueryCompleted(out) => {
                        match out {
                            StatementOutput::Valid(_, tbl) => {
                                on_single_query_result.call(tbl.clone());
                            },
                            StatementOutput::Invalid(msg, _) => {
                                on_error.call(msg.clone());
                            },
                            _ => { }
                        }
                    },
                    
                    // Schema update after a DDL statement was executed by queries.
                    ActiveConnectionAction::SchemaUpdate(opt_schema) => {
                        schema_valid = true;
                        schema = opt_schema.clone();
                        selected_obj = None;
                        on_schema_update.call(opt_schema.clone());
                    },
                    
                    ActiveConnectionAction::ObjectSelected(obj_ixs) => {
                        match (&schema, obj_ixs) {
                            (Some(schema), Some(ixs)) => {
                                selected_obj = crate::sql::object::index_db_object(&schema[..], ixs);
                            },
                            _ => {
                                selected_obj = None;
                            }
                        }
                        on_object_selected.call(selected_obj.clone());
                    },
                    ActiveConnectionAction::ConnectFailure(info, e) => {
                        on_conn_failure.call((info, e.clone()));
                    },
                    ActiveConnectionAction::Error(e) => {
                        on_error.call(e.clone());
                    }
                }
                glib::Continue(true)
            }
        });

        Self {
            on_connected,
            on_disconnected,
            on_error,
            send,
            on_exec_result,
            on_conn_failure,
            on_schema_update,
            on_object_selected,
            on_single_query_result,
            on_schema_invalidated
        }
    }

    pub fn emit_error(&self, msg : String) {
        self.send.send(ActiveConnectionAction::Error(msg));
    }

    pub fn connect_db_connected<F>(&self, f : F)
    where
        F : Fn((ConnectionInfo, Option<DBInfo>)) + 'static
    {
        self.on_connected.bind(f);
    }

    pub fn connect_db_disconnected<F>(&self, f : F)
    where
        F : Fn(()) + 'static
    {
        self.on_disconnected.bind(f);
    }

    pub fn connect_db_error<F>(&self, f : F)
    where
        F : Fn(String) + 'static
    {
        self.on_error.bind(f);
    }

    pub fn connect_db_conn_failure<F>(&self, f : F)
    where
        F : Fn((ConnectionInfo, String)) + 'static
    {
        self.on_conn_failure.bind(f);
    }

    pub fn connect_exec_result<F>(&self, f : F)
    where
        F : Fn(Vec<StatementOutput>) + 'static
    {
        self.on_exec_result.bind(f);
    }

    pub fn connect_single_query_result<F>(&self, f : F)
    where
        F : Fn(Table) + 'static
    {
        self.on_single_query_result.bind(f);
    }

    pub fn connect_schema_invalidated<F>(&self, f : F)
    where
        F : Fn(()) + 'static
    {
        self.on_schema_invalidated.bind(f);
    }
    
    pub fn connect_schema_update<F>(&self, f : F)
    where
        F : Fn(Option<Vec<DBObject>>) + 'static
    {
        self.on_schema_update.bind(f);
    }

    pub fn connect_object_selected<F>(&self, f : F)
    where
        F : Fn(Option<DBObject>) + 'static
    {
        self.on_object_selected.bind(f);
    }

    /*pub fn connect_exec_message<F>(&self, f : F)
    where
        F : Fn(String) + 'static
    {
        self.on_exec_messaeg.borrow_mut().push(boxed::Box::new(f));
    }*/
}

/*fn call_when_info_arrived(info : Callbacks<Option<DBInfo>>) {
    glib::timeout_add_local(Duration::from_millis(16), move || {

    });
}*/

impl React<ConnectionBox> for ActiveConnection {

    fn react(&self, conn_bx : &ConnectionBox) {
        // let conn_bx = r.0;
        let (host_entry, db_entry, user_entry, password_entry) = (
            conn_bx.host.entry.clone(),
            conn_bx.db.entry.clone(),
            conn_bx.user.entry.clone(),
            conn_bx.password.entry.clone()
        );
        let send = self.send.clone();
        // let state = r.1.clone();
        conn_bx.switch.connect_state_set(move |switch, _state| {

            if switch.is_active() {
                
                // if host_entry.text().starts_with("file") {
                //    unimplemented!()
                // }

                match generate_conn_uri_from_entries(&host_entry, &db_entry, &user_entry, &password_entry) {
                    Ok(mut uri) => {

                        // let mut state = state.borrow_mut();
                        /*// If there is already a bound certificate
                        for conn in state.conns.iter() {
                            if conn.host == uri.info.host {
                                if let Some(cert) = conn.cert.as_ref() {
                                    uri.info.cert = Some(cert.to_string());
                                    uri.info.is_tls = conn.is_tls;
                                }
                            }
                        }
                        // Match certificate to this new host, if there is a pending certificate.
                        if uri.info.cert.is_none() {
                            for c in state.certs.clone().iter() {
                                if c.host == uri.info.host {
                                    uri.info.cert = Some(c.cert.to_string());
                                    while let Some(ix) = state.conns.iter().cloned().position(|conn| conn.host == c.host ) {
                                        state.conns[ix].cert = Some(c.cert.to_string());
                                        state.conns[ix].is_tls = Some(c.is_tls);
                                    }
                                }
                            }
                        }*/

                        send.send(ActiveConnectionAction::ConnectRequest(uri)).unwrap();
                    },
                    Err(e) => {
                        let info = extract_conn_info(&host_entry, &db_entry, &user_entry).unwrap_or_default();
                        send.send(ActiveConnectionAction::ConnectFailure(info, e)).unwrap();
                        // crate::ui::disconnect_with_delay(switch.clone());
                    }
                }
            } else {
                send.send(ActiveConnectionAction::Disconnect).unwrap();
            }

            Inhibit(false)
        });

           /*if let Ok(mut t_env) = table_env.try_borrow_mut() {
                if state {
                    let conn_res  : Result<(), String> = if let Ok(db_path) = conn_popover.db_path.try_borrow() {
                        match (db_path.len(), conn_popover.check_entries_clear()) {
                            (0, true) => Self::try_local_connection(&conn_popover, None, &mut t_env),
                            (0, false) => Self::try_remote_connection(&conn_popover, &mut t_env),
                            (1, true) => {
                                println!("{:?}", db_path);
                                if let Some(ext) = db_path[0].extension().map(|ext| ext.to_str()) {
                                    match ext {
                                        Some("csv") | Some("txt") => {
                                            let ans = Self::try_local_connection(&conn_popover, None, &mut t_env);
                                            if ans.is_ok() {
                                                // Self::upload_csv(db_path[0].clone(), &mut t_env, status.clone(), switch.clone());
                                                Self::create_csv_vtab(db_path[0].clone(), &mut t_env, status.clone(), switch.clone());
                                            }
                                            ans
                                        },
                                        _ => Self::try_local_connection(&conn_popover, Some(db_path[0].clone()), &mut t_env)
                                    }
                                } else {
                                    Self::try_local_connection(&conn_popover, None, &mut t_env)
                                }
                            },
                            (_, true) => {
                                let ans = Self::try_local_connection(&conn_popover, None, &mut t_env);
                                if ans.is_ok() {
                                    for p in db_path.iter() {
                                        // Self::upload_csv(p.clone(), &mut t_env, status.clone(), switch.clone());
                                        Self::create_csv_vtab(p.clone(), &mut t_env, status.clone(), switch.clone());
                                    }
                                }
                                ans
                            },
                            _ => {
                                println!("Invalid connection mode");
                                Err(format!("Invalid connection mode"))
                            }
                        }
                    } else {
                        println!("Could not acquire lock over DB path");
                        Err(format!("Could not acquire lock over DB path"))
                    };

                    match conn_res {
                        Ok(_) => {
                            connected.set(true);
                            status.update(Status::Connected);
                            if let Some(f) = on_connected.borrow().as_ref() {
                                f();
                            }
                        },
                        Err(e) => {
                            status.update(Status::ConnectionErr(e));
                            connected.set(false);
                            Self::disconnect_with_delay(switch.clone());
                            if let Some(f) = on_disconnected.borrow().as_ref() {
                                f();
                            }
                        }
                    }

                } else {
                    // Disable remote connection
                    if t_env.is_engine_active() {
                        t_env.disable_engine();
                    }
                    conn_popover.set_non_db_mode();
                    conn_popover.clear_entries();
                    status.update(Status::Disconnected);
                    connected.set(false);
                    Self::clear_session(
                        sql_editor.clone(),
                        workspace.clone(),
                        table_notebook.clone(),
                        &mut t_env
                    );
                }
            } else {
                println!("Could not acquire lock over table environment");
            }
            if let Some(status) = status.get_status() {
                match status {
                    Status::Connected => {
                        sql_editor.set_active(true);
                        workspace.set_active(true);
                        fn_reg.set_sensitive(false);
                        schema_tree.repopulate(table_env.clone());
                    },
                    _ => {
                        fn_reg.set_sensitive(true);
                        schema_tree.clear();
                        if let Ok(mut t_env) = table_env.try_borrow_mut() {
                            Self::clear_session(
                                sql_editor.clone(),
                                workspace.clone(),
                                table_notebook.clone(),
                                &mut t_env
                            );
                        } else {
                            println!("Failed to acquire lock over table environment");
                        }
                    }
                }
            }*/
        //Inhibit(false)
        // });
    }
}

/*fn try_remote_connection(
    conn_popover : &ConnPopover,
    t_env : &mut TableEnvironment
) -> Result<(), String> {
    match crate::client::generate_conn_str(conn_popover.entries()) {
        Ok(conn_str) => {
            let res = t_env.update_source(
                EnvironmentSource::PostgreSQL((conn_str, "".into())),
                true
            );
            match res {
                Ok(_) => {
                    conn_popover.set_db_loaded_mode();
                    Ok(())
                },
                Err(e) => {
                    Err(format!("{}", e))
                }
            }
        },
        Err(err_str) => {
            Err(err_str)
        }
    }
}*/

/*fn try_local_connection(
    conn_popover : &ConnPopover,
    opt_path : Option<PathBuf>,
    t_env : &mut TableEnvironment
) -> Result<(), String> {
    if t_env.is_engine_active() {
        return Err(format!("Invalid connection state"));
    }

    #[cfg(feature="arrowext")]
    {
        let source = EnvironmentSource::Arrow(String::new());
        if let Err(e) = t_env.update_source(source, true) {
            println!("{}", e);
            return Err(e);
        }
        conn_popover.entries[3].set_text("(In-memory database)");
        conn_popover.set_db_loaded_mode();
        return Ok(());
    }
    let source = EnvironmentSource::SQLite3((opt_path.clone(), String::new()));
    if let Err(e) = t_env.update_source(source, true) {
        println!("{}", e);
        return Err(e);
    }
    let conn_name = match &opt_path {
        Some(path) => {
            if let Some(str_path) = path.to_str() {
                str_path
            } else {
                "(Invalid UTF-8 path)"
            }
        }
        None => "(In-memory database)"
    };
    conn_popover.entries[3].set_text(conn_name);
    conn_popover.set_db_loaded_mode();
    Ok(())
}*/

impl React<ExecButton> for ActiveConnection {

    fn react(&self, btn : &ExecButton) {
        let send = self.send.clone();
        let schedule_action = btn.schedule_action.clone();
        let mut is_scheduled = Rc::new(RefCell::new(false));
        let exec_btn = btn.btn.clone();
        btn.exec_action.connect_activate(move |_action, param| {

            // Perhaps replace by a ValuedCallback that just fetches the contents of editor.
            // Then impl React<ExecBtn> for Editor, then React<Editor> for ActiveConnection,
            // where editor exposes on_script_read(.).

            let mut is_scheduled = is_scheduled.borrow_mut();

            if *is_scheduled {
                exec_btn.set_icon_name("download-db-symbolic");
                *is_scheduled = false;
                send.send(ActiveConnectionAction::EndSchedule);
            } else {

                let stmts = param.unwrap().get::<String>().unwrap();
                let must_schedule = schedule_action.state().unwrap().get::<bool>().unwrap();
                if must_schedule {
                    exec_btn.set_icon_name("clock-app-symbolic");
                    *is_scheduled = true;
                    send.send(ActiveConnectionAction::StartSchedule(stmts)).unwrap();
                } else {
                    send.send(ActiveConnectionAction::ExecutionRequest(stmts)).unwrap();
                }
            }

            // println!("Should execute: {}", );
        });

        /*btn.schedule_action.connect_state_notify({
            let send = self.send.clone();
            move |action| {
                if !action.state().unwrap().get::<bool>().unwrap() {
                    println!("Unscheduled");
                    send.send(ActiveConnectionAction::EndSchedule);
                }
            }
        });*/
    }

}

impl React<SchemaTree> for ActiveConnection {

    fn react(&self, tree : &SchemaTree) {
        tree.tree_view.selection().connect_changed({
            let send = self.send.clone();
            move |sel| {
                let mut n_selected = 0;
                sel.selected_foreach(|_, path, _| {
                    n_selected += 1;
                    let res_ixs : Result<Vec<usize>, ()> = path.indices()
                        .iter()
                        .map(|ix| if *ix >= 0 { Ok(*ix as usize) } else { Err(()) })
                        .collect();
                    if let Ok(ixs) = res_ixs {
                        send.send(ActiveConnectionAction::ObjectSelected(Some(ixs)));
                    }
                });

                if n_selected == 0 {
                    send.send(ActiveConnectionAction::ObjectSelected(None));
                }
            }
        });

        tree.query_action.connect_activate({
            let send = self.send.clone();
            move |action, _| {
                if let Some(state) = action.state() {
                    let s = state.get::<String>().unwrap();
                    if !s.is_empty() {
                        let obj : DBObject = serde_json::from_str(&s).unwrap();
                        match obj {
                            DBObject::Table { schema, name, .. } | DBObject::View { schema, name, .. } => {
                                send.send(ActiveConnectionAction::ExecutionRequest(format!("select * from {}.{} limit 500;", schema, name)));
                            },
                            _ => { }
                        }
                    }
                }
            }
        });
        tree.report_dialog.btn_gen.connect_clicked({
            let send = self.send.clone();
            let dialog = tree.report_dialog.dialog.clone();
            move |_| {
                dialog.hide();
                send.send(ActiveConnectionAction::SingleQueryRequest);
            }
        });

        tree.form.btn_ok.connect_clicked({

            let insert_action = tree.insert_action.clone();
            let call_action = tree.call_action.clone();
            let entries = tree.form.entries.clone();
            let send = self.send.clone();
            move |_| {
                let state_str : String = if let Some(state) = insert_action.state() {
                    let s = state.get::<String>().unwrap();
                    if !s.is_empty() {
                         s
                    } else {
                        if let Some(state) = call_action.state() {
                            let s = state.get::<String>().unwrap();
                            if !s.is_empty() {
                                s
                            } else {
                                return;
                            }
                        } else {
                            return;
                        }
                    }
                } else {
                    return;
                };
                let values = entries.iter().map(|e| e.text().to_string() ).collect::<Vec<_>>();
                let obj : DBObject = serde_json::from_str(&state_str[..]).unwrap();
                //match form_action {
                //    FormAction::Table(obj) => {

                match obj {
                    DBObject::Table { schema, name, cols, .. } => {

                        let tys : Vec<DBType> = cols.iter().map(|col| col.1 ).collect();
                        match sql_literal_tuple(&entries, &tys) {
                            Ok(tuple) => {
                                let insert_stmt = format!("insert into {}.{} values {};", schema, name, tuple);

                                send.send(ActiveConnectionAction::ExecutionRequest(insert_stmt));
                            },
                            Err(e) => {
                                send.send(ActiveConnectionAction::Error(e));
                            }
                        }
                    },
                    DBObject::Function { schema, name, args, ret, .. } => {
                        if args.len() == 0 {
                            let call_stmt = if ret.is_some() {
                                format!("select {}.{}();", schema, name)
                            } else {
                                format!("call {}.{}();", schema, name)
                            };
                            send.send(ActiveConnectionAction::ExecutionRequest(call_stmt));
                        } else {
                            match sql_literal_tuple(&entries, &args) {
                                Ok(tuple) => {
                                    let call_stmt = if ret.is_some() {
                                        format!("select {}.{}{};", schema, name, tuple)
                                    } else {
                                        format!("call {}.{}{};", schema, name, tuple)
                                    };
                                    send.send(ActiveConnectionAction::ExecutionRequest(call_stmt));
                                },
                                Err(e) => {
                                    send.send(ActiveConnectionAction::Error(e));
                                }
                            }
                        }
                    },
                    _ => {

                    }
                }

                entries.iter().for_each(|e| e.set_text("") );

               //     },
               //     FormAction::FnCall(obj) => {
                        // send.send(ActiveConnectionAction::ExecutionRequest(format!("select * from {} limit 500;", name)));
               //     },
               // }
            }
        });

        let send = self.send.clone();
        tree.import_dialog.dialog.connect_response({
            move |dialog, resp| {
                match resp {
                    ResponseType::Accept => {
                        if let Some(path) = dialog.file().and_then(|f| f.path() ) {
                            send.send(ActiveConnectionAction::TableImport(path.to_str().unwrap().to_string())).unwrap();
                        }
                    },
                    _ => { }
                }
            }
        });
    }
}

fn sql_literal_tuple(entries : &[Entry], tys : &[DBType]) -> Result<String, String> {
    let mut ix = 0;
    let mut tuple = String::from("(");
    for (entry, col) in entries.iter().zip(tys) {

        // if EntryExt::is_visible(entry) {
        //    return Err(String::from("Entry should be visible"));
        // }

        match text_to_sql_literal(&entry, &col) {
            Ok(txt) => {
                tuple += &txt;
            },
            Err(e) => {
                return Err(format!("Error at {} field ({})", ordinal::Ordinal(ix), e));
            }
        }
        if ix == tys.len() - 1 {
            tuple += ")"
        } else {
            tuple += ", "
        }
        ix += 1;
    }
    Ok(tuple)
}

fn text_to_sql_literal(entry : &Entry, ty : &DBType) -> Result<String, String> {
    use sqlparser::tokenizer::{Tokenizer, Token};
    let entry_s = entry.text();
    let entry_s = entry_s.as_str();
    if entry_s.is_empty() {
        return Ok("null".to_string())
    } else {
        let desired_lit = match ty {
            DBType::Text | DBType::Date | DBType::Time | DBType::Bytes |
            DBType::Json | DBType::Xml | DBType::Array | DBType::Bool => {

                if entry_s.contains("'") {
                    return Err(String::from("Invalid character (') at entry"));
                }

                format!("'{}'", entry_s)
            },
            _ => format!("{}", entry_s)
        };
        let dialect = sqlparser::dialect::PostgreSqlDialect{};
        let mut tkn = sqlparser::tokenizer::Tokenizer::new(&dialect, desired_lit.trim());
        match tkn.tokenize() {
            Ok(tokens) => {
                if tokens.len() == 1 {
                    match &tokens[0] {
                        Token::Number(_, _) | Token::SingleQuotedString(_) => {
                            Ok(desired_lit.trim().to_string())
                        },
                        _ => {
                            Err(format!("Invalid literal"))
                        }
                    }
                } else {
                    Err(format!("Invalid literal"))
                }
            },
            Err(e) => {
                Err(format!("Invalid literal"))
            }
        }
    }
}


