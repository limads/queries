/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use gtk4::*;
use gtk4::prelude::*;
use stateful::{React, Callbacks};
use crate::ui::ConnectionList;
use crate::ui::ConnectionBox;
use crate::ui::QueryBuilderWindow;
use crate::ui::GraphWindow;
use std::boxed;
use glib::MainContext;
use super::listener::SqlListener;
use crate::server::*;
use std::thread;
use crate::sql::object::{DBInfo};
use crate::sql::StatementOutput;
use crate::ui::ExecButton;
use serde::{Serialize, Deserialize};
use std::rc::Rc;
use std::cell::RefCell;
use crate::sql::object::DBObject;
use crate::ui::{SchemaTree};
use crate::sql::object::DBType;
use crate::sql::copy::*;
use std::time::Duration;
use std::hash::Hash;
use crate::client::SharedUserState;
use super::listener::QueryTarget;
use crate::tables::table::Table;
use std::str::FromStr;
use std::net::{Ipv4Addr, Ipv6Addr};
use crate::client::UserState;
use url::Url;
use std::fmt;
use std::error::Error;
use std::cell::Cell;
use crate::Share;
use sqlparser::keywords::Keyword;
use sqlparser::tokenizer::Word;
use crate::ui::apply::*;
use serde_json::Value;
use std::convert::TryFrom;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Engine {
    Postgres,
    MySQL,
    SQLite
}

impl fmt::Display for Engine {

    fn fmt(&self, f : &mut fmt::Formatter) -> fmt::Result {
        match self {
            Engine::Postgres => write!(f, "Postgres"),
            Engine::MySQL => write!(f, "MySQL"),
            Engine::SQLite => write!(f, "SQLite")
        }
    }

}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TlsVersion {
    pub major : usize,
    pub minor : usize
}

impl std::string::ToString for TlsVersion {

    fn to_string(&self) -> String {
        format!("{}.{}", self.major, self.minor)
    }

}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Security {

    // What minimum version of TLS to use for encryption. If this is None,
    // then the connection is not encrypted. Users can only use non-encrypted connections
    // to localhost (127.0.0.1) or local network (192.168.x.x).
    pub tls_version : Option<TlsVersion>,

    // Whether to require hostname verification. If tls_version is Some(.), then
    // this field should be Some(.) as well.
    pub verify_hostname : Option<bool>,

    // Path to certificate. If tls_version is Some(.), then
    // this field should be Some(.) as well.
    pub cert_path : Option<String>

}

impl Security {

    // The default security created by new_secure won't connect
    // because it still needs a cert_path. The user should
    // inform a certificate when a secure connection is created.
    // The secure state serializes as a JSON object
    // with a single field tls_version.
    pub fn new_secure() -> Self {
        Self {
            tls_version : Some(TlsVersion { major : 1, minor : 2 }),
            verify_hostname : Some(true),
            cert_path : None
        }
    }

    // Creates a new insecure connection. Used only for localhost or
    // private network. The "insecure" state
    // serializes as a json object with all null fields.
    pub fn new_insecure() -> Self {
        Self {
            tls_version : None,
            verify_hostname : None,
            cert_path : None
        }
    }

}

impl fmt::Display for Security {

    fn fmt(&self, f : &mut fmt::Formatter) -> fmt::Result {
        crate::client::display_as_json(self, f)
    }
}

// The actual connection info that is persisted on disk (excludes password for obvious
// security reasons). This is the internal state of what the user sees in the right
// form when establishing new connections. This eventually resolves into a ConnURI at the moment
// a password is inserted and the connection switch is set to active. After the connection
// is established or failed, the ConnURI ceasse to exist and this carries again carries all
// the information of the recently-established connection to the rest of the GUI.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectionInfo {

    pub engine : Engine,

    pub host : String,

    pub port : String,

    // PostgreSQL user.
    pub user : String,

    // Database name.
    pub database : String,

    // Optional path to certificate. If connection does not have an
    // associated certificate, it is non-encrypted.
    pub security : Security,

    pub readonly : Option<bool>

}

impl fmt::Display for ConnectionInfo {

    fn fmt(&self, f : &mut fmt::Formatter) -> fmt::Result {
        crate::client::user::display_as_json(self, f)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostKind {
    Uri,
    Ipv4,
    Ipv6
}

impl ConnectionInfo {

    pub fn default_local() -> Self {
        Self {
            engine : Engine::SQLite,
            host : String::new(),
            port : String::new(),
            user : String::new(),
            database : String::new(),
            security : Security::new_insecure(),
            readonly : None
        }
    }

    pub fn new_sqlite(path : &str) -> Self {
        let mut info = ConnectionInfo::default();
        info.host = path.to_string();
        info.security = Security::new_insecure();
        info.engine = Engine::SQLite;
        info
    }

    pub fn kind(&self) -> Option<HostKind> {
        if Ipv4Addr::from_str(&self.host[..]).is_ok() {
            Some(HostKind::Ipv4)
        } else if Ipv6Addr::from_str(&self.host[..]).is_ok() {
            Some(HostKind::Ipv6)
        } else if Url::parse(&self.host[..]).is_ok() {
            Some(HostKind::Uri)
        } else {
            None
        }
    }

    // Check if state matches with the default start of
    // ConnectionInfo::Default. This means the form should
    // display the placeholder instead of the actual value.
    pub fn is_default(&self) -> bool {
        &self.host[..] == DEFAULT_HOST &&
            &self.port[..] == DEFAULT_PORT &&
            &self.user[..] == DEFAULT_USER &&
            &self.database[..] == DEFAULT_DB
    }

    // Checks if the credentials match
    pub fn is_like(&self, other : &Self) -> bool {
        &self.host[..] == &other.host[..] &&
            &self.port[..] == &other.port[..] &&
            &self.user[..] == &other.user[..] &&
            &self.database[..] == &other.database[..]
    }

    pub fn is_certificate_valid(&self) -> bool {
        if let Some(path) = &self.security.cert_path {
            std::path::Path::new(&path).exists() &&
                (path.ends_with(".crt") || path.ends_with(".pem"))
        } else {
            false
        }
    }

    /* The connection info description that is shown at the security settings UI */
    pub fn description(&self) -> String {
        let mut s = String::from(self.host_description());
        s += "\t\t";
        if self.is_encrypted() {
            s += "✓ Encrypted";
            s += "\t\t";
            if self.is_certificate_valid(){
                s += "✓ Certificate path valid";
            } else {
                s += "⨯ Certificate path invalid";
            }
            s += "\t\t";
            if self.is_verified() {
                s += "✓ Host name verified";
            } else {
                s += "⨯ Host name not verified";
            }
        } else {
            s += "⨯ Not encrypted";
        }
        s
    }

    pub fn host_description(&self) -> &'static str {
        if self.is_localhost() || self.is_loopback() {
            "Local"
        } else if self.is_private_network() {
            "Private network"
        } else if self.is_file() {
            "File"
        } else if self.kind().is_some() {
            "Remote"
        } else {
            "Unknown"
        }
    }

    pub fn is_encrypted(&self) -> bool {
        self.security.tls_version.is_some()
    }

    pub fn is_verified(&self) -> bool {
        self.security.verify_hostname == Some(true)
    }

    pub fn is_file(&self) -> bool {
        self.kind() == Some(HostKind::Uri) && self.host.starts_with("file://")
    }

    pub fn is_loopback(&self) -> bool {
        if let Ok(ip) = Ipv4Addr::from_str(&self.host[..]) {
            ip.is_loopback()
        } else if let Ok(ip) = Ipv6Addr::from_str(&self.host[..]) {
            ip.is_loopback()
        } else {
            false
        }
    }

    pub fn is_localhost(&self) -> bool {
        if let Ok(ip) = Ipv4Addr::from_str(&self.host[..]) {

            /* Quoting from the stdlib docs:
            "An IPv4 address with the address pointing to localhost: 127.0.0.1" */
            ip == Ipv4Addr::LOCALHOST
        } else if let Ok(ip) = Ipv6Addr::from_str(&self.host[..]) {

            /* From the stdlib docs: "An IPv6 address representing localhost: ::1." */
            ip == Ipv6Addr::LOCALHOST

        } else {
            &self.host[..] == "localhost"
        }
    }

    pub fn is_private_network(&self) -> bool {
        if let Ok(ip) = Ipv4Addr::from_str(&self.host[..]) {

            /* Defines if an IP is private. Quoting from the stdlib docs:
            "The private address ranges are defined in IETF RFC 1918 and include:
            10.0.0.0/8
            172.16.0.0/12
            192.168.0.0/16" "*/
            ip.is_private()
        } else {
            false
        }
    }

    /* Defines if an user can disble TLS altogether for this connection. This
    is done by selecting TLS version 'None' at the TLS version combo. While the
    user can configure anything here, the application will refuse to proceed with
    the connection unless the connection is to a file, private, localhost or loopback. */
    pub fn requires_tls(&self) -> bool {
        let accept_insecure = self.is_file() ||
            self.is_private_network() ||
            self.is_localhost() ||
            self.is_loopback();
        !accept_insecure
    }

}

#[derive(Debug, Clone)]
pub struct ConnConfig {

    // Statement timeout, in milliseconds.
    pub timeout : usize

}

pub const DEFAULT_HOST : &str = "Host";

pub const DEFAULT_PORT : &str = "Port";

pub const DEFAULT_USER : &str = "User";

pub const DEFAULT_DB : &str = "Database";

impl Default for ConnectionInfo {

    fn default() -> Self {
        Self {
            engine : Engine::Postgres,
            host : String::from(DEFAULT_HOST),
            port : String::from(DEFAULT_PORT),
            user : String::from(DEFAULT_USER),
            database : String::from(DEFAULT_DB),
            security : Security::new_secure(),
            readonly : None
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
    Add { remote : bool },
    UpdateHost(String),
    UpdatePort(String),
    UpdateUser(String),
    UpdateReadonly(bool),
    UpdateDB(String),
    Remove(i32),
}

// The ConnectionSet keeps all history of established connections. It is loaded
// from disk when Queries start if the save credentials settings is on.
// The user views the connection set on the ListBox to the
// left in the main overview GUI. The user manipulates the Connection set by adding
// or removing from the buttons next to this GUI. The user edits the ConnectionSet
// by manipulating the form to the right in the overview or changing something in
// the security settings. More than one connection might share the same host, so
// changing the security settings for a given host makes the change reflect in all
// connections matching this host. If a host field is edited, the keys in the security
// settings must be edited accordingly. Changing the host name re-sets all its
// security settings to the default.
pub struct ConnectionSet {

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

    pub fn new(user_state : &SharedUserState) -> Self {
        let (send, recv) = MainContext::channel::<ConnectionAction>(glib::source::Priority::DEFAULT);
        let (selected, added, updated, removed) : ConnSetTypes = Default::default();
        recv.attach(None, {

            // Holds the set of connections added by the user. This is synced to the
            // Connections list seen by the user on startup. The connections are
            // set to the final state just before the window closes.
            let mut curr_conn : Option<i32> = None;

            let (selected, added, updated, removed) = (selected.clone(), added.clone(), updated.clone(), removed.clone());
            let user_state = user_state.clone();

            move |action| {
                match action {

                    ConnectionAction::Switch(opt_ix) => {
                        curr_conn = opt_ix;
                        selected.call(opt_ix.map(|ix| (ix, user_state.borrow().conns[ix as usize].clone()) ));
                    },
                    
                    ConnectionAction::Add { remote } => {

                        // If the user clicked the 'plus' button, this will be None. If the connection
                        // was added from the settings file, there will be a valid value here.
                        let conn = if remote {
                            ConnectionInfo::default()
                        } else {
                            ConnectionInfo::default_local()
                        };
                        
                        user_state.borrow_mut().conns.push(conn.clone());

                        // The selection will be re-set when the list triggers the callback at connect_added.
                        curr_conn = None;

                        added.call(conn.clone());
                    },
                    
                    ConnectionAction::UpdateHost(updated_host) => {
                        if let Some(ix) = curr_conn {
                            let mut us = user_state.borrow_mut();
                            let matching_conn = us.conns.iter()
                                .find(|c| &c.host[..] == &updated_host[..] )
                                .cloned();
                            let mut this_conn = &mut us.conns[ix as usize];
                            if this_conn.host != updated_host {
                                // If this is a totally new host, it should have its security
                                // settings re-set to a default value.
                                this_conn.host = updated_host;

                                // The user edited the host to have a a name that
                                // already existed in the connection history. Inherit
                                // its security settings in this case.
                                if let Some(matching) = matching_conn {
                                    this_conn.security = matching.security;
                                } else {
                                    if this_conn.is_localhost() || this_conn.is_file() || this_conn.is_loopback() {
                                        this_conn.security = Security::new_insecure();
                                    } else {
                                        this_conn.security = Security::new_secure();
                                    }
                                }

                                // Update engine if required
                                if this_conn.is_file() {
                                    this_conn.engine = Engine::SQLite;
                                } else {
                                    this_conn.engine = Engine::Postgres;
                                }

                                updated.call((ix as i32, this_conn.clone()));
                            }
                        }
                    },
                    
                    ConnectionAction::UpdatePort(updated_port) => {
                        if let Some(ix) = curr_conn {
                            let mut us = user_state.borrow_mut();
                            let mut this_conn = &mut us.conns[ix as usize];

                            if this_conn.port != updated_port {
                                this_conn.port = updated_port;
                                updated.call((ix as i32, this_conn.clone()));
                            }
                        }
                    },

                    ConnectionAction::UpdateUser(updated_user) => {
                        if let Some(ix) = curr_conn {
                            let mut us = user_state.borrow_mut();
                            let mut this_conn = &mut us.conns[ix as usize];

                            if this_conn.user != updated_user {
                                this_conn.user = updated_user;
                                updated.call((ix as i32, this_conn.clone()));
                            }
                        }
                    },
                    
                    ConnectionAction::UpdateDB(updated_db) => {
                        if let Some(ix) = curr_conn {
                            let mut us = user_state.borrow_mut();
                            let mut this_conn = &mut us.conns[ix as usize];
                            
                            if this_conn.database != updated_db {
                                this_conn.database = updated_db;
                                updated.call((ix as i32, this_conn.clone()));
                            }
                        }
                    },
                    
                    ConnectionAction::UpdateReadonly(readonly) => {
                        if let Some(ix) = curr_conn {
                            let mut us = user_state.borrow_mut();
                            if us.conns[ix as usize].engine == Engine::SQLite {
                                let mut this_conn = &mut us.conns[ix as usize];
                                this_conn.readonly = Some(readonly);
                                updated.call((ix as i32, this_conn.clone()));
                            }
                        }
                    },

                    ConnectionAction::Remove(ix) => {
                        user_state.borrow_mut().conns.remove(ix as usize);
                        removed.call(ix);
                        selected.call(None);
                    },

                }
                glib::ControlFlow::Continue
            }
        });
        Self {
            send,
            selected,
            added,
            updated,
            removed
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

impl React<ConnectionBox> for ConnectionSet {

    fn react(&self, conn_bx : &ConnectionBox) {
        let host_changed = conn_bx.host.entry.connect_changed({
            let send = self.send.clone();
            move |entry| {
                let txt = entry.text().to_string();
                if &txt[..] != "" {
                    send.send(ConnectionAction::UpdateHost(txt)).unwrap();
                } else {
                    send.send(ConnectionAction::UpdateHost("Host".to_string())).unwrap();
                }
            }
        });
        let port_changed = conn_bx.port.entry.connect_changed({
            let send = self.send.clone();
            move |entry| {
                let txt = entry.text().to_string();
                if &txt[..] != "" {
                    send.send(ConnectionAction::UpdatePort(txt)).unwrap();
                } else {
                    send.send(ConnectionAction::UpdatePort("Port".to_string())).unwrap();
                }

            }
        });
        let user_changed = conn_bx.user.entry.connect_changed({
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
        let db_changed = conn_bx.db.entry.connect_changed({
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
        let local_changed = conn_bx.local_entry.connect_changed({
            let send = self.send.clone();
            move |entry| {
                let txt = entry.text().to_string();
                if &txt[..] != "" {
                    send.send(ConnectionAction::UpdateHost(txt)).unwrap();
                } else {
                    send.send(ConnectionAction::UpdateHost("file://".to_string())).unwrap();
                }
            }
        });
        conn_bx.check_readonly.connect_toggled({
            let send = self.send.clone();
            move |check| {
                send.send(ConnectionAction::UpdateReadonly(check.is_active()));
            }
        });
        conn_bx.host_changed.replace(Some(host_changed));
        conn_bx.user_changed.replace(Some(user_changed));
        conn_bx.db_changed.replace(Some(db_changed));
        conn_bx.port_changed.replace(Some(port_changed));
        conn_bx.local_changed.replace(Some(local_changed));
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
        conn_list.add_remote_btn.connect_clicked({
            let send = self.send.clone();
            move |_btn| {
                send.send(ConnectionAction::Add { remote : true }).unwrap();
            }
        });
        conn_list.add_local_btn.connect_clicked({
            let send = self.send.clone();
            move |_btn| {
                send.send(ConnectionAction::Add { remote : false }).unwrap();
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

/// Short-lived data structure used to collect information from the connection
/// form. The URI contains all the credentials (including password) to connect
/// to the database. Only the info field is persisted in the client component
/// memory after the connection is established (or establishing it fails). This
/// structure should never implement serialize, and/or be persisted to disk.
#[derive(Debug, Clone)]
pub struct ConnURI {
    pub info : ConnectionInfo,
    pub uri : Url
}

impl ConnURI {

    pub fn require_tls(&self) -> bool {
        self.uri.query_pairs().any(|pair| pair.0.as_ref() == "sslmode" && pair.1.as_ref() == "require" )
    }

    pub fn file_path(&self) -> Result<std::path::PathBuf, ()> {
        self.uri.to_file_path()
    }

    pub fn is_file(&self) -> bool {
        self.uri.scheme() == "file"
    }

    pub fn is_postgres(&self) -> bool {
        self.uri.scheme() == "postgresql"
    }

    pub fn verify_integrity(&self) -> Result<(), boxed::Box<dyn Error>> {
        match self.info.engine {
            Engine::Postgres => {
                if !self.is_postgres() {
                    return Err("Invalid URI for Postgres connection".into());
                }
            },
            Engine::SQLite => {
                if !self.is_file() {
                    return Err("Invalid URI for SQLite connection".into());
                }
            },
            _ => { }
        }

        if !self.is_file() {
            if self.uri.host_str() != Some(&self.info.host[..]) {
                return Err("Mismatch between connection host and URI domain".into());
            }
            if self.uri.username() != &self.info.user[..] {
                return Err("Mismatch between connection username and URI username".into());
            }
            if let Ok(port) = u16::from_str(&self.info.port[..]) {
                if self.uri.port() != Some(port) {
                    return Err("Mismatch between connection port and URI port".into());
                }
            } else {
                return Err("Invalid port value".into());
            }
        }

        Ok(())
    }

}

impl ConnURI {

    pub fn new(
        mut info : ConnectionInfo,
        password : &str
    ) -> Result<ConnURI, String> {

        if info.host.is_empty() || &info.host[..] == "file://" {
            return Err(String::from("Missing host"));
        }

        if info.engine == Engine::SQLite {

            if !info.host.starts_with("file://") {
                info.host = format!("file://{}", info.host);
            }

            match Url::parse(&info.host) {
                Ok(uri) => {
                    Ok(ConnURI { info, uri })
                },
                Err(e) => {
                    Err(format!("{}",e))
                }
            }
        } else {
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

            uri += &info.host;
            uri += ":";
            uri += &info.port;
            uri += "/";
            uri += &info.database;

            match Url::parse(&uri) {
                Ok(uri) => {
                    Ok(ConnURI { info, uri })
                },
                Err(e) => {
                    Err(format!("{}",e))
                }
            }
        }
    }

}

/* Extract connection info from GTK widgets (except password, which is held separately 
at the uri field of ConnURI. */
fn extract_conn_info(
    host_entry : &Entry,
    port_entry : &Entry,
    db_entry : &Entry,
    user_entry : &Entry
) -> Result<ConnectionInfo, String> {
    let host_s = host_entry.text().as_str().to_owned();
    if host_s.is_empty() {
        return Err(format!("Missing host"));
    }
    let port_s = port_entry.text().as_str().to_owned();
    let is_file = host_s.starts_with("file://");
    if port_s.is_empty() && !is_file {
        return Err(format!("Missing port"));
    }
    let db_s = db_entry.text().as_str().to_owned();
    if db_s.is_empty() && !is_file {
        return Err(format!("Missing database"));
    }
    let user_s = user_entry.text().as_str().to_owned();
    if user_s.is_empty() && !is_file {
        return Err(format!("Missing user"));
    }
    let mut info : ConnectionInfo = Default::default();
    info.host = host_s.to_string();
    if is_file {
        if !port_s.is_empty() {
            Err("Unexpected 'Port'' argument for file database".to_string())?;
        }
        if !user_s.is_empty() {
            Err("Unexpected 'User' argument for file database".to_string())?;
        }
        if !db_s.is_empty() {
            Err("Unexpected 'Database' argument for file database".to_string())?;
        }
        info.engine = Engine::SQLite;
    } else {
        info.port = port_s.to_string();
        info.database = db_s.to_string();
        info.user = user_s.to_string();
        info.engine = Engine::Postgres;
    }
    Ok(info)
}

fn generate_conn_uri_from_entries(
    host_entry : &Entry,
    port_entry : &Entry,
    db_entry : &Entry,
    user_entry : &Entry,
    password_entry : &PasswordEntry
) -> Result<ConnURI, String> {
    let info = extract_conn_info(host_entry, port_entry, db_entry, user_entry)?;
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
    // Also carries a flag informing if the commands should
    // be executed in plan mode.
    ExecutionRequest(String, bool),

    FunctionApplyRequest(String),

    // Requires a sigle table or view name to do a single SQL query.
    SingleQueryRequest,

    // StartSchedule(String),

    // EndSchedule,

    ExecutionCompleted(Vec<StatementOutput>),

    ReportQueryCompleted(StatementOutput),

    SchemaUpdate(Option<Vec<DBObject>>),

    ProcessRequest(Table),

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
    
    // on_schedule_start : Callbacks<()>,
    
    // on_schedule_end : Callbacks<()>,

    on_error : Callbacks<String>,

    on_exec_result : Callbacks<Vec<StatementOutput>>,

    // "single queries" are queries sent by interactions with the GUI
    // (Query and Report on the popover in the left schema tree). The
    // callbacks are different because the GUI should react differently
    // to results from those kinds of queries.
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

    pub fn new(
        user_state : &SharedUserState,
        mods : crate::ui::apply::Modules,
        call_params : Rc<RefCell<crate::ui::apply::CallParams>>
    ) -> Self {
        let (on_connected, on_disconnected, on_error) : ActiveConnCallbacks = Default::default();
        let on_exec_result : Callbacks<Vec<StatementOutput>> = Default::default();
        let on_single_query_result : Callbacks<Table> = Default::default();
        let on_conn_failure : Callbacks<(ConnectionInfo, String)> = Default::default();
        let (send, recv) = glib::MainContext::channel::<ActiveConnectionAction>(glib::source::Priority::DEFAULT);
        let on_schema_update : Callbacks<Option<Vec<DBObject>>> = Default::default();
        let on_object_selected : Callbacks<Option<DBObject>> = Default::default();
        let on_schema_invalidated : Callbacks<()> = Default::default();
        // let on_schedule_start : Callbacks<()> = Default::default();
        // let on_schedule_end : Callbacks<()> = Default::default();
        
        let mut schema_valid = true;
        
        /* Active schedule, unlike the other state variables, needs to be wrapped in a RefCell
        because it is shared with any new callbacks that start when the user schedule a set of statements. */
        // let active_schedule = Rc::new(RefCell::new(false));
        
        // Thread that waits for SQL statements via the standard library mpsc channels (with a
        // single producer).
        let mut listener = SqlListener::launch({
            let send = send.clone();
            move |mut results, tgt| {
                match tgt {
                    QueryTarget::Report => {
                        send.send(ActiveConnectionAction::ReportQueryCompleted(results.remove(0))).unwrap();
                    },
                    QueryTarget::ClientFunction => {
                        let out = results.remove(0);
                        match out {
                            StatementOutput::Valid(_, tbl) => {
                                send.send(ActiveConnectionAction::ProcessRequest(tbl));
                            },
                            other => {
                                send.send(ActiveConnectionAction::ExecutionCompleted(vec![other])).unwrap();
                            }
                        }
                    },
                    QueryTarget::Environment => {
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
            // let (on_schedule_start, on_schedule_end) = (on_schedule_start.clone(), on_schedule_end.clone());
            let on_conn_failure = on_conn_failure.clone();
            let on_object_selected = on_object_selected.clone();
            let on_schema_update = on_schema_update.clone();
            let on_schema_invalidated = on_schema_invalidated.clone();
            let user_state = (*user_state).clone();
            
            // If the user disconnects the switch when a connection is still being attempted,
            // then when eventually the connection is established or timed out, it should be
            // left to die without any error messages (irrespective of whether it was successful)
            // since the user turning off the switch should mean the user gave up on the connection.
            let mut attempting_conn = false;
            
            move |action| {
                match action {

                    // At this stage, the connection URI was successfully parsed, 
                    // but the connection hasn't been established yet. This URI is captured from
                    // the entries, so no certificate is associated with it yet.
                    ActiveConnectionAction::ConnectRequest(uri) => {

                        if attempting_conn {
                            send.send(ActiveConnectionAction::ConnectFailure(
                                uri.info.clone(),
                                format!("Previous connection attempt not finished yet")
                            )).unwrap();
                            return glib::ControlFlow::Continue;
                        }
                        attempting_conn = true;
                        
                        // Spawn a thread that captures the database connection URI. The URI
                        // carrying the password is forgotten when this thread dies.
                        thread::spawn({
                            let send = send.clone();
                            let us : UserState = user_state.borrow().clone();
                            move || {
                                match uri.info.engine {
                                    Engine::Postgres => {
                                        connect(PostgresConnection::try_new, uri.clone(), send.clone(), &us);
                                    },
                                    Engine::SQLite => {
                                        connect(SqliteConnection::try_new, uri.clone(), send.clone(), &us);
                                    },
                                    other_engine => {
                                        send.send(ActiveConnectionAction::ConnectFailure(
                                            uri.info.clone(),
                                            format!("Unsupported engine: {}", other_engine)
                                        )).unwrap();
                                    }
                                }
                            }
                        });
                    },

                    ActiveConnectionAction::ProcessRequest(tbl) => {
                        match call_client_function(&mods, &call_params, &tbl) {
                            Ok(tbl) => {
                                let ans = vec![StatementOutput::Valid(String::new(), tbl)];
                                send.send(ActiveConnectionAction::ExecutionCompleted(ans)).unwrap();
                            },
                            Err(e) => {
                                on_error.call(format!("{e}"));
                            }
                        }
                    },

                    // At this stage, the connection is active, and the URI is already
                    // forgotten.
                    ActiveConnectionAction::ConnectAccepted(conn, db_info) => {
                        attempting_conn = false;
                        schema = db_info.as_ref().map(|info| info.schema.clone() );
                        selected_obj = None;
                        let info = conn.conn_info();
                        conn.bind_functions(&mods);
                        if let Err(e) = listener.update_engine(conn) {
                            eprintln!("{}", e);
                        }

                        on_connected.call((info, db_info));
                    },
                    
                    ActiveConnectionAction::Disconnect => {
                        // This means the switch has been turned off while the application
                        // was still trying to make a connection. Send a message to ignore
                        // this connection, so that the connecting threads does not send a
                        // connect_accepted message back.
                        schema = None;
                        selected_obj = None;
                        // active_schedule.replace(false);
                        on_disconnected.call(());
                    },
                    
                    // When the user clicks the exec button or activates the execute action.
                    ActiveConnectionAction::ExecutionRequest(stmts, is_plan) => {
                    
                        if !schema_valid {
                            on_error.call(format!("Cannot execute command right now (schema update pending)"));
                            return glib::ControlFlow::Continue;
                        }

                        // if *(active_schedule.borrow()) {
                        //     on_error.call(format!("Attempted to execute statement during active schedule"));
                        //     return glib::ControlFlow::Continue;
                        // }
                        
                        if listener.is_running() {
                            // This shouldn't happen. The user is prevented from sending statements
                            // when the engine is working.
                            on_error.call(format!("Previous statement not completed yet."));
                            return glib::ControlFlow::Continue;
                        }

                        let us = user_state.borrow();
                        match listener.send_commands(stmts, us.safety(), is_plan) {
                            Ok(_) => { },
                            Err(e) => {
                                on_error.call(e.clone());
                            }
                        }
                    },
                    
                    ActiveConnectionAction::FunctionApplyRequest(cmd) => {
                        let us = user_state.borrow();
                        match listener.send_single_command(cmd, us.safety(), QueryTarget::ClientFunction) {
                            Ok(_) => { },
                            Err(e) => {
                                on_error.call(e.clone());
                            }
                        }
                    }

                    // SingleQueryRequest is used when the schema tree is used to generate a report.
                    ActiveConnectionAction::SingleQueryRequest => {
                    
                        if !schema_valid {
                            on_error.call(format!("Cannot execute command right now (schema update pending)"));
                            return glib::ControlFlow::Continue;
                        }
                        
                        // if *(active_schedule.borrow()) {
                        //    on_error.call(format!("Attempted to execute statement during active schedule"));
                        //    return glib::ControlFlow::Continue;
                        //}
                        
                        if listener.is_running() {
                            // This shouldn't happen. The user is prevented from sending statements
                            // when the engine is working.
                            on_error.call(format!("Previous statement not completed yet."));
                            return glib::ControlFlow::Continue;
                        }
                        
                        match &selected_obj {
                            Some(DBObject::View { schema, name, .. }) | Some(DBObject::Table { schema, name, .. }) => {
                                let cmd = format!("SELECT * from {schema}.{name};");
                                let us = user_state.borrow();
                                match listener.send_single_command(cmd, us.safety(), QueryTarget::Report) {
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
                    /*ActiveConnectionAction::StartSchedule(stmts) => {
                    
                        if *(active_schedule.borrow()) {
                            on_error.call(format!("Tried to start schedule twice"));
                            return glib::ControlFlow::Continue;
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
                                    return glib::ControlFlow::Continue;
                                }

                                let us = user_state.borrow();
                                
                                let should_continue = *active_schedule.borrow();
                                if !should_continue {
                                    return glib::ControlFlow::Break;
                                }
                                let send_ans = listener.send_commands(
                                    stmts.clone(),
                                    //HashMap::new(),
                                    us.safety(),
                                    true
                                );
                                match send_ans {
                                    Ok(_) => { 
                                        if should_continue {
                                            glib::ControlFlow::Continue
                                        } else {
                                            glib::ControlFlow::Break
                                        }
                                    },
                                    Err(e) => {
                                        send.send(ActiveConnectionAction::Error(e)).unwrap();
                                        glib::ControlFlow::Break
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
                            return glib::ControlFlow::Continue;
                        }
                        
                        active_schedule.replace(false);
                        on_schedule_end.call(());
                    },*/
                    
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
                                    listener.spawn_import_and_then(csv_path, copy, move |ans| {
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
                        
                        let fst_error = results.iter()
                            .filter_map(|res| {
                                match res {
                                    StatementOutput::Invalid(e, _) => Some(e.clone()),
                                    _ => None
                                }
                            }).next();
                        if let Some(error) = fst_error {
                            on_error.call(error.clone());
                            
                            // if *(active_schedule.borrow()) == true {
                            //     send.send(ActiveConnectionAction::EndSchedule).unwrap();
                            // }
                        
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
                    ActiveConnectionAction::ReportQueryCompleted(out) => {
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
                        attempting_conn = false;
                        on_conn_failure.call((info, e.clone()));
                    },
                    
                    ActiveConnectionAction::Error(e) => {
                        on_error.call(e.clone());
                        // if *(active_schedule.borrow()) == true {
                        //     send.send(ActiveConnectionAction::EndSchedule).unwrap();
                        // }
                    }
                }
                glib::ControlFlow::Continue
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
            // on_schedule_start,
            // on_schedule_end
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
    
    /*pub fn connect_schedule_start<F>(&self, f : F)
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
    }*/
    
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

    pub fn connect_report_query_result<F>(&self, f : F)
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

const CONN_NAME_ERR : &str = "Application name at settings contain non-alphanumeric characters";

fn augment_uri_with_params(
    uri : &mut String,
    extra_args : &[String]
) -> Result<(), std::boxed::Box<dyn Error>> {
    if Url::parse(&uri)?.query().is_some() {
        return Err(format!("Attempted to configure connection URL query string multiple times").into());
    }
    let n = extra_args.len();
    if n >= 1 {
        *uri += "?";
        for arg in extra_args.iter().take(n-1) {
            *uri += &arg[..];
            *uri += "&";
        }
        *uri += &extra_args[n-1][..];
    }
    if Url::parse(&uri)?.query_pairs().count() != extra_args.len() {
        return Err(format!("Malformed query string at connection URL").into());
    }
    Ok(())
}

pub fn connect<F, C>(
    f : F,
    uri : ConnURI,
    send : glib::Sender<ActiveConnectionAction>,
    us : &UserState,
)
where
    F : Fn(ConnURI)->Result<C, String>,
    C : Connection + 'static
{
    let timeout_secs = us.execution.statement_timeout;
    match f(uri.clone()) {
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

// Returns extra arguments to the connection string based on connection state and security settings
pub fn get_user_state_conn_params(us : &UserState, sec : &Security) -> Result<Vec<String>, String> {
    let mut extra_args = Vec::new();
    let app_name = &us.conn.app_name;
    if !app_name.is_empty() {
        if app_name.chars().any(|c| !c.is_alphanumeric() ) {
            return Err(format!("{}", CONN_NAME_ERR));
        } else {
            extra_args.push(format!("application_name={}", app_name));
        }
    }
    extra_args.push(format!("connect_timeout={}", us.conn.timeout));
    if sec.tls_version.is_some() {
        extra_args.push(format!("sslmode=require"));
    } else {
        extra_args.push(format!("sslmode=prefer"));
    }
    Ok(extra_args)
}

impl React<ConnectionBox> for ActiveConnection {

    fn react(&self, conn_bx : &ConnectionBox) {
        let (host_entry, port_entry, db_entry, user_entry, password_entry) = (
            conn_bx.host.entry.clone(),
            conn_bx.port.entry.clone(),
            conn_bx.db.entry.clone(),
            conn_bx.user.entry.clone(),
            conn_bx.password.entry.clone()
        );
        let send = self.send.clone();
        let user_state = self.user_state.clone();
        conn_bx.remote_switch.connect_state_set(move |switch, _state| {
            if switch.is_active() {

                // The form URI is built from the entry values - It does not
                // have a security or user state options set yet.
                let res_form_uri = generate_conn_uri_from_entries(
                    &host_entry,
                    &port_entry,
                    &db_entry,
                    &user_entry,
                    &password_entry
                );
                match res_form_uri {
                    Ok(uri) => {
                    
                        // Retrieve most recent security settings from host, for the formed URI.
                        // If this host hasn't been configured yet, create a new secure default for
                        // remove/private network or insecure for localhost.
                        // This will happen if the user edited the host entry but did not configure
                        // a new security state.
                        // All hosts with the same name will have the same security settings, so
                        // take the first one.

                        // let req_sent = if uri.is_file() {
                        //    send_sqlite_conn_request(uri.clone(), &send)
                        // } else if uri.is_postgres() {
                        let req_sent = if uri.is_postgres() {
                            send_postgres_conn_request(uri, &user_state, &send)
                        } else {
                            let info = extract_conn_info(&host_entry, &port_entry, &db_entry, &user_entry).unwrap_or_default();
                            send.send(ActiveConnectionAction::ConnectFailure(info, "Unrecognized engine".into())).unwrap();
                            false
                        };

                        if req_sent {
                            switch.set_sensitive(false);
                        }
                    },
                    Err(e) => {
                        let info = extract_conn_info(&host_entry, &port_entry, &db_entry, &user_entry).unwrap_or_default();
                        send.send(ActiveConnectionAction::ConnectFailure(info, e)).unwrap();
                    }
                }
            } else {
                send.send(ActiveConnectionAction::Disconnect).unwrap();
            }
            glib::signal::Propagation::Proceed
        });

        let local_entry = conn_bx.local_entry.clone();
        let send = self.send.clone();
        let check_readonly = conn_bx.check_readonly.clone();
        conn_bx.local_switch.connect_state_set(move |switch, _state| {
            if switch.is_active() {
                let path = local_entry.text().to_string();
                let mut info = ConnectionInfo::new_sqlite(&path);
                if check_readonly.is_active() {
                    info.readonly = Some(true);
                }
                match ConnURI::new(info.clone(), "") {
                    Ok(uri) => {
                        send_sqlite_conn_request(uri.clone(), &send);
                    },
                    Err(e) => {
                        send.send(ActiveConnectionAction::ConnectFailure(info.clone(), e)).unwrap();
                    }
                }
            } else {
                send.send(ActiveConnectionAction::Disconnect).unwrap();
            }
            glib::signal::Propagation::Proceed
        });
    }
}

fn send_sqlite_conn_request(
    uri : ConnURI,
    send : &glib::Sender<ActiveConnectionAction>
) -> bool {
    send.send(ActiveConnectionAction::ConnectRequest(uri)).unwrap();
    true
}

fn send_postgres_conn_request(
    mut uri : ConnURI,
    user_state : &SharedUserState,
    send : &glib::Sender<ActiveConnectionAction>
) -> bool {
    let us = user_state.borrow();
    crate::client::assert_user_state_integrity(&us);
    let security = if let Some(c) = us.conns
        .iter()
        .find(|c| &c.host[..] == &uri.info.host[..] )
    {
        c.security.clone()
    } else {
        send.send(ActiveConnectionAction::ConnectFailure(
            uri.info.clone(),
            format!("Security not configured for this host"))
        ).unwrap();
        return false;
    };

    // Now link the security information to the established connection.
    uri.info.security = security;

    match get_user_state_conn_params(&us, &uri.info.security) {
        Ok(params) => {
            let mut uri_str = uri.uri.as_str().to_string();
            match augment_uri_with_params(&mut uri_str, &params[..]) {
                Ok(_) => { },
                Err(e) => {
                    send.send(ActiveConnectionAction::ConnectFailure(
                        uri.info.clone(),
                        format!("{}", e)
                    )).unwrap();
                    return false;
                }
            }

            // Guarantee the URI is valid after being augmented with the
            // user state parameters.
            match Url::parse(&uri_str) {
                Ok(new_uri) => {

                    // Effectively update the URI with user state parameters.
                    uri.uri = new_uri;

                    // Checks if the URI fields matches with what is at the
                    // info field.
                    match uri.verify_integrity() {
                        Ok(_) => {
                            send.send(ActiveConnectionAction::ConnectRequest(uri)).unwrap();
                            true
                        },
                        Err(e) => {
                            send.send(ActiveConnectionAction::ConnectFailure(
                                uri.info.clone(),
                                format!("{}", e)
                            )).unwrap();
                            false
                        }
                    }
                },
                Err(e) => {
                    send.send(ActiveConnectionAction::ConnectFailure(
                        uri.info.clone(),
                        format!("Connection string URL parsing error\n{}", e))
                    ).unwrap();
                    false
                }
            }
        },
        Err(e) => {
            send.send(ActiveConnectionAction::ConnectFailure(uri.info.clone(), e)).unwrap();
            false
        }
    }
}

impl React<ExecButton> for ActiveConnection {

    fn react(&self, btn : &ExecButton) {
        let send = self.send.clone();
        let is_plan_action = btn.set_plan_action.clone();
        let is_plan = Rc::new(RefCell::new(false));
        btn.exec_action.connect_activate({
            let is_plan = is_plan.clone();
            let exec_btn = btn.btn.clone();
            move |_action, param| {

                // let mut is_plan = is_plan.borrow_mut();

                /*if *is_scheduled {
                    exec_btn.set_icon_name("download-db-symbolic");
                    *is_scheduled = false;
                    send.send(ActiveConnectionAction::EndSchedule).unwrap();
                } else {*/
                let stmts = param.unwrap().get::<String>().unwrap();
                let is_plan = is_plan_action.state().unwrap().get::<bool>().unwrap();
                // if must_schedule {
                //    exec_btn.set_icon_name("clock-app-symbolic");
                //    *is_scheduled = true;
                //    send.send(ActiveConnectionAction::StartSchedule(stmts)).unwrap();
                // } else {
                send.send(ActiveConnectionAction::ExecutionRequest(stmts, is_plan)).unwrap();
                // }
                // }
            }
        });
        
        /*self.connect_schedule_end({
            let is_scheduled = is_scheduled.clone();
            let exec_btn = btn.btn.clone();
            move|_| { 
                let mut is_scheduled = is_scheduled.borrow_mut();
                if *is_scheduled {
                    *is_scheduled = false;
                    exec_btn.set_icon_name("download-db-symbolic");
                }
             }
        });*/

    }

}

impl React<GraphWindow> for ActiveConnection {

    fn react(&self, win : &GraphWindow) {
        let send = self.send.clone();
        let win_c = win.clone();

        let [is_remote_1, is_remote_2] = Rc::new_shared(Cell::new(true));
        self.connect_db_connected({
            move |(info, _)| {
                is_remote_1.set(info.engine != Engine::SQLite);
            }
        });
        win.btn_plot.connect_clicked(move |_| {
            match win_c.plot_sql(is_remote_2.get()) {
                Ok(sql) => {
                    send.send(ActiveConnectionAction::ExecutionRequest(sql, false));
                },
                Err(e) => {
                    send.send(ActiveConnectionAction::Error(e));
                }
            }
        });
    }

}

impl React<QueryBuilderWindow> for ActiveConnection {

    fn react(&self, win : &QueryBuilderWindow) {
        let send = self.send.clone();
        let win_c = win.clone();
        win.btn_run.connect_clicked(move |_| {
            let sql = win_c.current_sql();
            // println!("SQL = {}", sql);
            send.send(ActiveConnectionAction::ExecutionRequest(sql, false));
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
                                let src = if schema.is_empty() {
                                    name.clone()
                                } else {
                                    format!("{schema}.{name}")
                                };
                                let query = format!("SELECT * FROM {src} LIMIT {row_limit};");
                                send.send(ActiveConnectionAction::ExecutionRequest(query, false)).unwrap();
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
            let dialog = tree.form.dialog.clone();
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
                        let names : Vec<String> = cols.iter().map(|col| col.name.clone() ).collect();
                        let tys : Vec<DBType> = cols.iter().map(|col| col.ty ).collect();
                        match sql_literal_tuple(&entries, Some(&names), &tys) {
                            Ok(tuple) => {
                                let tpl_names = crate::tables::table::insertion_tuple(&names);
                                let src = if schema.is_empty() {
                                    format!("{schema}.{name}")
                                } else {
                                    name.clone()
                                };
                                let insert_stmt = format!("INSERT INTO {src} {tpl_names} VALUES {tuple};");
                                match crate::sql::require_insert_n_from_sql(&insert_stmt, tys.len(), 1) {
                                    Ok(_) => {
                                        send.send(ActiveConnectionAction::ExecutionRequest(insert_stmt, false)).unwrap();
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
                            let arg_tuple = if args.len() == 0 {
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

                            /* This will work for postgres only. */
                            let stmt = format!("SELECT {schema}.{name}{arg_tuple};");
                            match crate::sql::require_single_fn_select_from_sql(&stmt) {
                                Ok(_) => {
                                    send.send(ActiveConnectionAction::ExecutionRequest(stmt, false)).unwrap();
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
                dialog.close();
            }
        });

        tree.create_dialog.btn_create.connect_clicked({
            let send = self.send.clone();
            let dialog = tree.form.dialog.clone();
            let curr_tbl = tree.create_dialog.curr_tbl.clone();
            let create_action = tree.create_action.clone();
            let tbl_entry = tree.create_dialog.tbl_entry.clone();
            let overlay = tree.create_dialog.overlay.clone();
            move |_| {
                let tbl = &*curr_tbl.borrow();
                if let Some(name) = table_create_name(&create_action, &tbl_entry) {
                    match tbl.sql(&name) {
                        Ok(stmt) => {
                            send.send(ActiveConnectionAction::ExecutionRequest(stmt, false)).unwrap();
                            dialog.close();
                        },
                        Err(e) => {
                            let toast = libadwaita::Toast::builder().title(&e.to_string()).build();
                            overlay.add_toast(toast.clone());
                        }
                    }
                } else {
                    let toast = libadwaita::Toast::builder().title("Invalid table name").build();
                    overlay.add_toast(toast.clone());
                }
            }
        });
        tree.create_dialog.btn_sql.connect_clicked({
            let curr_tbl = tree.create_dialog.curr_tbl.clone();
            let overlay = tree.create_dialog.overlay.clone();
            let create_action = tree.create_action.clone();
            let send = self.send.clone();
            let tbl_entry = tree.create_dialog.tbl_entry.clone();
            move |_| {
                if let Some(displ) = gdk::Display::default() {
                    let tbl = &*curr_tbl.borrow();
                    if let Some(name) = table_create_name(&create_action, &tbl_entry) {
                        match tbl.sql(&name) {
                            Ok(stmt) => {
                                displ.clipboard().set_text(&stmt);
                                let toast = libadwaita::Toast::builder().title("Statement copied to clipboard").build();
                                overlay.add_toast(toast.clone());
                            },
                            Err(e) => {
                                let toast = libadwaita::Toast::builder().title(&e.to_string()).build();
                                overlay.add_toast(toast.clone());
                            }
                        }
                    } else {
                        let toast = libadwaita::Toast::builder().title("Invalid table name").build();
                        overlay.add_toast(toast.clone());
                    }
                }
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

fn table_create_name(action : &gio::SimpleAction, entry : &Entry) -> Option<String> {
    if let Some(state) = action.state() {
        let s = state.get::<String>().unwrap();
        let tbl_name = entry.text().to_string();
        if tbl_name.is_empty() {
            return None;
        }
        if s.is_empty() {
            Some(tbl_name)
        } else {
            Some(format!("{}.{}", s, tbl_name))
        }
    } else {
        None
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
                        Token::Number(_, _) | Token::SingleQuotedString(_) |
                        Token::Word(Word { keyword : Keyword::TRUE, ..} | Word { keyword : Keyword::FALSE, ..} ) => {
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

impl React<ApplyWindow> for ActiveConnection {

    fn react(&self, win : &ApplyWindow) {
        win.call_btn.connect_clicked({
            let call_params = win.call_params.clone();
            let send = self.send.clone();
            let dialog = win.dialog.clone();
            move |_| {
                match call_params.borrow().sql() {
                    Ok(sql) => {
                        send.send(ActiveConnectionAction::FunctionApplyRequest(sql));
                        call_params.borrow_mut().pending = true;
                        dialog.hide();
                    },
                    Err(e) => {

                    }
                }
            }
        });
    }

}

fn call_client_function(
    modules : &Modules,
    call_params : &Rc<RefCell<CallParams>>,
    tbl : &Table
) -> Result<Table,std::boxed::Box<std::error::Error>> {
    let mut call_params = call_params.borrow_mut();
    if !call_params.pending {
        return Err("Function call not pending".into());
    }
    call_params.pending = false;
    let mut modules = modules.borrow_mut();
    if let (Some(module), Some(func)) = (&call_params.module, &call_params.func) {
        if let Some(m) = modules.get_mut(&module[..]) {
            let args_map = collect_call_args(&call_params, tbl)?;
            let val = m.call(&func.symbol, &tbl, &args_map)?;
            let tbl = Table::try_from(val)?;
            Ok(tbl)
        } else {
            Err(format!("No module named {}", module).into())
        }
    } else {
        Err(format!("No function at current call context").into())
    }
}

fn collect_call_args(
    call_params : &CallParams,
    tbl : &Table
) -> Result<serde_json::Map<String, serde_json::Value>, std::boxed::Box<std::error::Error>>  {
    let mut args_map = serde_json::Map::new();
    for i in 0..call_params.args.len() {
        match &call_params.args[i].val {
            ArgVal::Column(_, _, _) | ArgVal::Table(_, _) => {
                if let Some(col) = tbl.get_column(i) {
                    if let Some(vals) = col.as_json_values() {
                        args_map.insert(call_params.args[i].arg.name.to_string(), vals);
                    } else {
                        Err("Error converting column to JSON")?;
                    }
                } else {
                    Err(format!("Missing column at index {i}"))?;
                }
            },

            ArgVal::Float(real) => {
                args_map.insert(call_params.args[i].arg.name.to_string(), Value::from(*real));
            },
            ArgVal::Bool(b) => {
                args_map.insert(call_params.args[i].arg.name.to_string(), Value::from(*b));
            },
            ArgVal::Integer(int) => {
                args_map.insert(call_params.args[i].arg.name.to_string(), Value::from(*int));
            },
            _ => {

            }
        }
    }
    Ok(args_map)
}


