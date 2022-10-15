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
use crate::sql::object::{DBInfo};
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
use crate::ui::TlsVersion;

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
    
    pub min_tls_version : Option<TlsVersion>,
    
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
            cert : None,
            min_tls_version : None
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
    AddCertificate(Certificate),
    Remove(i32),
    CloseWindow
}

// TODO rename to ConnectionHistory.
pub struct ConnectionSet {

    final_state : Rc<RefCell<Vec<ConnectionInfo>>>,

    added : Callbacks<ConnectionInfo>,

    removed : Callbacks<i32>,

    updated : Callbacks<(i32, ConnectionInfo)>,

    selected : Callbacks<Option<(i32, ConnectionInfo)>>,

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
            self.send.send(ConnectionAction::AddCertificate(cert.clone())).unwrap();
        }
    }
    
    pub fn add_connections(&self, conns : &[ConnectionInfo]) {
        for conn in conns.iter() {
            if !conn.host.is_empty() && !conn.database.is_empty() && !conn.user.is_empty() {
                self.send.send(ConnectionAction::Add(Some(conn.clone()))).unwrap();
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
                                conn.min_tls_version = Some(cert.min_version);
                                conn.cert = Some(cert.cert.clone());
                            }
                        }
                        certs.push(cert);
                    },
                    
                    ConnectionAction::EraseCertificate(host) => {
                        for conn in &mut conns.0[..] {
                            if &conn.host[..] == &host[..] {
                                conn.cert = None;
                                conn.min_tls_version = None;
                            }
                        }
                        for i in (0..(certs.len())).rev() {
                            if &certs[i].host[..] == &host[..] {
                                certs.remove(i);
                            }
                        }
                    },
                    ConnectionAction::Add(opt_conn) => {

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
            conn.min_tls_version = Some(cert.min_version);
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
                    send.send(ConnectionAction::UpdateHost(txt)).unwrap();
                } else {
                    send.send(ConnectionAction::UpdateHost("Host:Port".to_string())).unwrap();
                }
                
            }
        });
        conn_bx.user.entry.connect_changed({
            let send = self.send.clone();
            move |entry| {
                let txt = entry.text().to_string();
                if &txt[..] != "" {
                    send.send(ConnectionAction::UpdateUser(txt)).unwrap();
                } else {
                    send.send(ConnectionAction::UpdateUser("User".to_string())).unwrap();
                }
                
            }
        });
        conn_bx.db.entry.connect_changed({
            let send = self.send.clone();
            move |entry| {
                let txt = entry.text().to_string();
                if &txt[..] != "" {
                    send.send(ConnectionAction::UpdateDB(txt)).unwrap();
                } else {
                    send.send(ConnectionAction::UpdateDB("Database".to_string())).unwrap();
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
            send.send(ConnectionAction::Update(conn_info)).unwrap();
        });
        let send = self.send.clone();
        conn.connect_db_conn_failure(move |(conn_info, _)| {
            send.send(ConnectionAction::Update(conn_info)).unwrap();
        });
    }

}

impl React<QueriesWindow> for ConnectionSet {

    fn react(&self, win : &QueriesWindow) {
        let send = self.send.clone();
        win.window.connect_close_request(move |_win| {
            send.send(ConnectionAction::CloseWindow).unwrap();
            Inhibit(false)
        });
        win.settings.security_bx.cert_removed.connect_activate({
            let send = self.send.clone();
            move |_, param| {
                if let Some(s) = param {
                    let cert : Certificate = serde_json::from_str(&s.get::<String>().unwrap()).unwrap();
                    send.send(ConnectionAction::EraseCertificate(cert.host.to_string())).unwrap();
                }
           }
       });
       win.settings.security_bx.cert_added.connect_activate({
            let send = self.send.clone();
            move |_, param| {
                if let Some(s) = param {
                    let cert : Certificate = serde_json::from_str(&s.get::<String>().unwrap()).unwrap();
                    send.send(ConnectionAction::AddCertificate(cert)).unwrap();
                }
           }
       });
    }

}

/*mod tests {

    use super::{ConnectionInfo, ConnURI};
    
    #[test]
    fn conn_str_test() {

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

    }
}*/

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

const INVALID_HOST_PORT : &'static str = "Invalid host value (expected host:port format)\n(ex. 127.0.0.1:5432)";

pub fn split_host_port(host_with_port : &str) -> Result<(&str, &str), String> {
    let split_port : Vec<&str> = host_with_port.split(":").collect();
    let (host_prefix, port) = match split_port.len() {
        2 => {
            (split_port[0], split_port[1])
        },
        _n => {
            return Err(format!("{}", INVALID_HOST_PORT));
        }
    };
    if port.parse::<usize>().is_err() {
        return Err(format!("{}", INVALID_HOST_PORT));
    }
    Ok((host_prefix, port))
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
        
        let mut uri = "postgresql://".to_owned();
        uri += &info.user;
        uri += ":";

        if password.is_empty() {
            return Err(format!("Missing password"));
        }
        uri += password;

        uri += "@";
        let (host_prefix, port) = split_host_port(&info.host)?;
        
        uri += host_prefix;
        uri += ":";        
        uri += port;
        uri += "/";
        uri += &info.database;
        
        Ok(ConnURI { info, uri })
    }

}

/* Extract connection info from GTK widgets (except password, which is held separately 
at the uri field of ConnURI. */
fn extract_conn_info(host_entry : &Entry, db_entry : &Entry, user_entry : &Entry) -> Result<ConnectionInfo, String> {
    let host_s = host_entry.text().as_str().to_owned();
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

    TableImport(String),

    Error(String)

}

pub type ActiveConnCallbacks = (Callbacks<(ConnectionInfo, Option<DBInfo>)>, Callbacks<()>, Callbacks<String>);

pub struct ActiveConnection {

    user_state : SharedUserState,
    
    on_connected : Callbacks<(ConnectionInfo, Option<DBInfo>)>,

    on_conn_failure : Callbacks<(ConnectionInfo, String)>,
    
    on_disconnected : Callbacks<()>,
    
    on_schedule_start : Callbacks<()>,
    
    on_schedule_end : Callbacks<()>,

    on_error : Callbacks<String>,

    on_exec_result : Callbacks<Vec<StatementOutput>>,

    on_single_query_result : Callbacks<Table>,

    send : glib::Sender<ActiveConnectionAction>,

    on_schema_invalidated : Callbacks<()>,
    
    on_schema_update : Callbacks<Option<Vec<DBObject>>>,

    on_object_selected : Callbacks<Option<DBObject>>

}

impl ActiveConnection {

    pub fn sender(&self) -> &glib::Sender<ActiveConnectionAction> {
        &self.send
    }

    pub fn send(&self, msg : ActiveConnectionAction) {
        self.send.send(msg).unwrap();
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
        let on_schedule_start : Callbacks<()> = Default::default();
        let on_schedule_end : Callbacks<()> = Default::default();
        
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
            let (on_schedule_start, on_schedule_end) = (on_schedule_start.clone(), on_schedule_end.clone());
            let on_conn_failure = on_conn_failure.clone();
            let on_object_selected = on_object_selected.clone();
            let on_schema_update = on_schema_update.clone();
            let on_schema_invalidated = on_schema_invalidated.clone();
            let user_state = (*user_state).clone();
            
            let mut trying_connection = false;
            
            move |action| {
                match action {

                    // At this stage, the connection URI was successfully parsed, 
                    // but the connection hasn't been established yet. This URI is captured from
                    // the entries, so no certificate is associated with it yet.
                    ActiveConnectionAction::ConnectRequest(uri) => {

                        if trying_connection {
                            on_error.call(format!("Previous connect attempt not finished yet"));
                            return glib::source::Continue(true);
                        }
                        
                        trying_connection = true;
                        
                        // Spawn a thread that captures the database connection URI. The URI
                        // and the sensitive information (password) is forgotten when this thread dies.
                        thread::spawn({
                            let send = send.clone();
                            let us = user_state.borrow();
                            let timeout_secs = us.execution.statement_timeout;
                            move || {
                                match PostgresConnection::try_new(uri.clone()) {
                                    Ok(mut conn) => {
                                    
                                        let db_info = match conn.db_info() {
                                            Ok(info) => Some(info),
                                            Err(_e) => {
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
                        
                        trying_connection = false;
                        schema = db_info.as_ref().map(|info| info.schema.clone() );
                        selected_obj = None;
                        let info = conn.conn_info();
                        if let Err(e) = listener.update_engine(conn) {
                            eprintln!("{}", e);
                        }
                        on_connected.call((info, db_info));
                    },
                    
                    ActiveConnectionAction::Disconnect => {
                        trying_connection = false;
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
                        match listener.send_commands(stmts, HashMap::new(), us.safety(), false) {
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
                                let us = user_state.borrow();
                                match listener.send_single_command(cmd, us.safety()) {
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
                        let dur = Duration::from_secs(user_state.borrow().execution.execution_interval as u64);
                        glib::timeout_add_local(dur, {
                            let active_schedule = active_schedule.clone();
                            let listener = listener.clone();
                            let user_state = user_state.clone();
                            let send = send.clone();
                            move || {

                                // Just ignore this schedule step if the previous statement is not
                                // executed yet. Queries will try to execute it again at the next timeout interval.
                                if listener.is_running() {
                                    return Continue(true);
                                }

                                let us = user_state.borrow();
                                
                                let should_continue = *active_schedule.borrow();
                                if !should_continue {
                                    return Continue(false);
                                }
                                let send_ans = listener.send_commands(
                                    stmts.clone(),
                                    HashMap::new(),
                                    us.safety(),
                                    true
                                );
                                match send_ans {
                                    Ok(_) => { 
                                        Continue(should_continue)    
                                    },
                                    Err(e) => {
                                        send.send(ActiveConnectionAction::Error(e)).unwrap();
                                        Continue(false)
                                    }
                                }
                            }
                        });
                        on_schedule_start.call(());
                    },
                    
                    // Execution was un-toggled in scheduled mode.
                    ActiveConnectionAction::EndSchedule => {
                    
                        if !*(active_schedule.borrow()) {
                            on_error.call(format!("Tried to end schedule, but there is no active schedule."));
                            return glib::Continue(true);
                        }
                        
                        active_schedule.replace(false);
                        on_schedule_end.call(());
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
                                                send.send(ActiveConnectionAction::ExecutionCompleted(vec![StatementOutput::Statement(msg)])).unwrap();
                                            },
                                            Err(e) => {
                                                send.send(ActiveConnectionAction::Error(e)).unwrap();
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
                            
                            if *(active_schedule.borrow()) == true {
                                send.send(ActiveConnectionAction::EndSchedule).unwrap();
                            }
                        
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
                                send.send(ActiveConnectionAction::SchemaUpdate(info)).unwrap();
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
                        trying_connection = false;
                        on_conn_failure.call((info, e.clone()));
                    },
                    
                    ActiveConnectionAction::Error(e) => {
                        on_error.call(e.clone());
                        if *(active_schedule.borrow()) == true {
                            send.send(ActiveConnectionAction::EndSchedule).unwrap();
                        }
                    }
                }
                glib::Continue(true)
            }
        });

        Self {
            user_state : user_state.clone(),
            on_connected,
            on_disconnected,
            on_error,
            send,
            on_exec_result,
            on_conn_failure,
            on_schema_update,
            on_object_selected,
            on_single_query_result,
            on_schema_invalidated,
            on_schedule_start,
            on_schedule_end
        }
    }

    pub fn emit_error(&self, msg : String) {
        self.send.send(ActiveConnectionAction::Error(msg)).unwrap();
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
    
    pub fn connect_schedule_start<F>(&self, f : F)
    where
        F : Fn(()) + 'static
    {
        self.on_schedule_start.bind(f);
    }

    pub fn connect_schedule_end<F>(&self, f : F)
    where
        F : Fn(()) + 'static
    {
        self.on_schedule_end.bind(f);
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

}

const NO_CERT : &'static str = "No SSL certificate associated with this host.\nConfigure one at the security settings";

const MANY_CERTS : &'static str = "Multiple SSL certificates associated with this host.\nRemove the duplicates at the security settings";

const CONN_NAME_ERR : &'static str = "Application name at settings contain non-alphanumeric characters";

fn augment_uri(uri : &mut String, extra_args : &[String]) {
    let n = extra_args.len();
    if n >= 1 {
        *uri += "?";
        for arg in extra_args.iter().take(n-1) {
            *uri += &arg[..];
            *uri += ",";
        }
        *uri += &extra_args[n-1][..];
    }
}

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
        let user_state = self.user_state.clone();
        conn_bx.switch.connect_state_set(move |switch, _state| {
            if switch.is_active() {
                match generate_conn_uri_from_entries(&host_entry, &db_entry, &user_entry, &password_entry) {
                    Ok(mut uri) => {
                    
                        let mut extra_args = Vec::new();
                        let us = user_state.borrow();
                        
                        let app_name = &us.conn.app_name;
                        if !app_name.is_empty() {
                            if app_name.chars().any(|c| !c.is_alphanumeric() ) {
                                send.send(ActiveConnectionAction::ConnectFailure(uri.info.clone(), format!("{}", CONN_NAME_ERR))).unwrap();
                                return Inhibit(false);
                            } else {
                                extra_args.push(format!("application_name={}", app_name));
                            }
                        }
                        
                        extra_args.push(format!("connect_timeout={}", us.conn.timeout));
                        
                        let local = is_local(&uri.info).unwrap_or(false);
                        if local {
                            augment_uri(&mut uri.uri, &extra_args[..]);
                            send.send(ActiveConnectionAction::ConnectRequest(uri)).unwrap();
                        } else {
                            let matching_certs : Vec<_> = us.certs.iter()
                                .filter(|cert| &cert.host[..] == &uri.info.host[..] )
                                .collect();
                            match matching_certs.len() {
                                0 => {
                                    send.send(ActiveConnectionAction::ConnectFailure(uri.info.clone(), NO_CERT.to_string())).unwrap();
                                },
                                1 => {
                                    let cert = &matching_certs[0];
                                    uri.info.cert = Some(cert.cert.clone());
                                    uri.info.min_tls_version = Some(cert.min_version);
                                    extra_args.push(format!("sslmode=require"));
                                    augment_uri(&mut uri.uri, &extra_args[..]);
                                    send.send(ActiveConnectionAction::ConnectRequest(uri)).unwrap();
                                },
                                _ => {
                                    send.send(ActiveConnectionAction::ConnectFailure(uri.info.clone(), MANY_CERTS.to_string())).unwrap();
                                }
                            }
                        }
                    },
                    Err(e) => {
                        let info = extract_conn_info(&host_entry, &db_entry, &user_entry).unwrap_or_default();
                        send.send(ActiveConnectionAction::ConnectFailure(info, e)).unwrap();
                    }
                }
            } else {
                send.send(ActiveConnectionAction::Disconnect).unwrap();
            }

            Inhibit(false)
        });
    }
}

impl React<ExecButton> for ActiveConnection {

    fn react(&self, btn : &ExecButton) {
        let send = self.send.clone();
        let schedule_action = btn.schedule_action.clone();
        let is_scheduled = Rc::new(RefCell::new(false));
        btn.exec_action.connect_activate({
            let is_scheduled = is_scheduled.clone();
            let exec_btn = btn.btn.clone();
            move |_action, param| {

                // Perhaps replace by a ValuedCallback that just fetches the contents of editor.
                // Then impl React<ExecBtn> for Editor, then React<Editor> for ActiveConnection,
                // where editor exposes on_script_read(.).

                let mut is_scheduled = is_scheduled.borrow_mut();

                if *is_scheduled {
                    exec_btn.set_icon_name("download-db-symbolic");
                    *is_scheduled = false;
                    send.send(ActiveConnectionAction::EndSchedule).unwrap();
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
            }
        });
        
        self.connect_schedule_end({
            let is_scheduled = is_scheduled.clone();
            let exec_btn = btn.btn.clone();
            move|_| { 
                let mut is_scheduled = is_scheduled.borrow_mut();
                if *is_scheduled {
                    *is_scheduled = false;
                    exec_btn.set_icon_name("download-db-symbolic");
                }
             }
        });

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
                        send.send(ActiveConnectionAction::ObjectSelected(Some(ixs))).unwrap();
                    }
                });

                if n_selected == 0 {
                    send.send(ActiveConnectionAction::ObjectSelected(None)).unwrap();
                }
            }
        });

        tree.query_action.connect_activate({
            let send = self.send.clone();
            let user_state = self.user_state.clone();
            move |action, _| {
                if let Some(state) = action.state() {
                    let s = state.get::<String>().unwrap();
                    let row_limit = user_state.try_borrow().map(|us| us.execution.row_limit ).unwrap_or(500);
                    if !s.is_empty() {
                        let obj : DBObject = serde_json::from_str(&s).unwrap();
                        match obj {
                            DBObject::Table { schema, name, .. } | DBObject::View { schema, name, .. } => {
                                send.send(ActiveConnectionAction::ExecutionRequest(format!("select * from {}.{} limit {};", schema, name, row_limit))).unwrap();
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
                send.send(ActiveConnectionAction::SingleQueryRequest).unwrap();
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
                let _values = entries.iter().map(|e| e.text().to_string() ).collect::<Vec<_>>();
                let obj : DBObject = serde_json::from_str(&state_str[..]).unwrap();

                match obj {
                    DBObject::Table { schema, name, cols, .. } => {
                        let names : Vec<String> = cols.iter().map(|col| col.0.clone() ).collect();
                        let tys : Vec<DBType> = cols.iter().map(|col| col.1 ).collect();
                        match sql_literal_tuple(&entries, Some(&names), &tys) {
                            Ok(tuple) => {
                                let tpl_names = crate::tables::table::insertion_tuple(&names);
                                let insert_stmt = format!("insert into {}.{} {} values {};", schema, name, tpl_names, tuple);
                                match crate::sql::require_insert_n_from_sql(&insert_stmt, tys.len(), 1) {
                                    Ok(_) => {
                                        send.send(ActiveConnectionAction::ExecutionRequest(insert_stmt)).unwrap();
                                    },
                                    Err(e) => {
                                        send.send(ActiveConnectionAction::Error(e)).unwrap();
                                    }
                                }
                            },
                            Err(e) => {
                                send.send(ActiveConnectionAction::Error(e)).unwrap();
                            }
                        }
                    },
                    DBObject::Function { schema, name, args, ret, .. } => {
                        if ret.is_some() {
                            let tuple = if args.len() == 0 {
                                String::from("()")
                            } else {
                                match sql_literal_tuple(&entries, None, &args) {
                                    Ok(tpl) => tpl,
                                    Err(e) => {
                                        send.send(ActiveConnectionAction::Error(e)).unwrap();
                                        return;
                                    }
                                }
                            };
                            let stmt = format!("select {}.{}{};", schema, name, tuple);
                            match crate::sql::require_single_fn_select_from_sql(&stmt) {
                                Ok(_) => {
                                    send.send(ActiveConnectionAction::ExecutionRequest(stmt)).unwrap();
                                },
                                Err(e) => {
                                    send.send(ActiveConnectionAction::Error(e)).unwrap();
                                }
                            }
                        } else {
                            send.send(ActiveConnectionAction::Error(format!("Cannot call procedure via menu."))).unwrap();
                        }
                    },
                    _ => {

                    }
                }
                entries.iter().for_each(|e| e.set_text("") );
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

fn sql_literal_tuple(entries : &[Entry], names : Option<&[String]>, tys : &[DBType]) -> Result<String, String> {
    let mut ix = 0;
    let mut tuple = String::from("(");
    for (entry, col) in entries.iter().zip(tys) {
        match text_to_sql_literal(&entry, &col) {
            Ok(txt) => {
                tuple += &txt;
            },
            Err(e) => {
                let e = if let Some(names) = names {
                    format!("Error at {} value ({}):\n{}", ordinal::Ordinal(ix), names[ix], e)
                } else {
                    format!("Error at {} argument:\n{}", ordinal::Ordinal(ix), e)
                };
                return Err(e);
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
    use sqlparser::tokenizer::{Token};
    let entry_s = entry.text();
    let entry_s = entry_s.as_str();
    if entry_s.contains("'") {
        return Err(String::from("Invalid character (')"));
    }
    
    if entry_s.is_empty() {
        return Ok("NULL".to_string())
    } else {
    
        // Quote literals from types in the first branch, do not
        // quote literals from types in the second branch.
        let desired_lit = match ty {
            DBType::Text | DBType::Date | DBType::Time | DBType::Bytes |
            DBType::Json | DBType::Xml | DBType::Array | DBType::Bool => {
                format!("'{}'", entry_s.trim())
            },
            _ => format!("{}", entry_s.trim())
        };
        
        let dialect = sqlparser::dialect::PostgreSqlDialect{};
        let mut tkn = sqlparser::tokenizer::Tokenizer::new(&dialect, &desired_lit[..]);
        match tkn.tokenize() {
            Ok(tokens) => {
                if tokens.len() == 1 {
                    match &tokens[0] {
                        Token::Number(_, _) | Token::SingleQuotedString(_) => {
                            Ok(desired_lit)
                        },
                        _ => {
                            Err(format!("Invalid literal (expected integer, decimal or quoted literal)"))
                        }
                    }
                } else {
                    Err(format!("Invalid literal (multiple tokens parsed)"))
                }
            },
            Err(_e) => {
                Err(format!("Invalid literal (not valid SQL token)"))
            }
        }
    }
}


