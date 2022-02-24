use gtk4::*;
use gtk4::prelude::*;
use crate::{React, Callbacks};
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct ConnectionInfo {

    pub host : String,

    pub user : String,

    pub database : String,

    pub details : Option<DBDetails>,

    // Optional path to certificate
    pub cert : Option<String>,

    pub dt : String

}

impl ConnectionInfo {

    pub fn is_default(&self) -> bool {
        &self.host[..] == "Host" && &self.user[..] == "User" && &self.database[..] == "Database" //&&
        // &self.encoding[..] == "Unknown" && &self.size[..] == "Unknown" && &self.locale[..] == "Unknown"
    }

    pub fn is_like(&self, other : &Self) -> bool {
        &self.host[..] == &other.host[..] && &self.user[..] == &other.user[..] && &self.database[..] == &other.database[..]
    }

}

impl Default for ConnectionInfo {

    fn default() -> Self {
        Self {
            host : String::from("Host"),
            user : String::from("User"),
            database : String::from("Database"),
            dt : Local::now().to_string(),
            details : None,
            cert : None
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
    Remove(i32),
    CloseWindow
    // ViewState(Vec<ConnectionInfo>)
}

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
            move |action| {
                println!("{:?} {:?}", action, conns.0);
                match action {
                    ConnectionAction::Switch(opt_ix) => {
                        conns.1 = opt_ix;
                        selected.borrow().iter().for_each(|f| f(opt_ix.map(|ix| (ix, conns.0[ix as usize].clone() ))) );
                    },
                    ConnectionAction::Add(opt_conn) => {
                        /*if let Some(conn) = &opt_conn {
                            if !conn.is_like(&ConnectionInfo::default()) && conns.0.iter().find(|c| c.is_like(&conn) ).is_some() {
                                return Continue(true);
                            }
                        }*/

                        // If the user clicked the 'plus' button, this will be None. If the connection
                        // was added from the settings file, there will be a valid value here.
                        let conn = opt_conn.unwrap_or_default();
                        conns.0.push(conn.clone());

                        // The selection will be re-set when the list triggers the callback at connect_added.
                        conns.1 = None;

                        added.borrow().iter().for_each(|f| f(conn.clone()) );
                    },
                    ConnectionAction::Update(mut info) => {

                        // On update, it might be the case the info is the same as some
                        // other connection. Must decide how to resolve duplicates (perhaps
                        // remove old one by sending ConnectionAction::remove(other_equal_ix)?).
                        if let Some(ix) = conns.1 {
                            info.dt = Local::now().to_string();
                            conns.0[ix as usize] = info;
                            updated.borrow().iter().for_each(|f| f((ix, conns.0[ix as usize].clone())) );
                        } else {
                            panic!()
                        }
                        // TODO update settings
                    },
                    ConnectionAction::Remove(ix) => {
                        let _rem_conn = conns.0.remove(ix as usize);
                        removed.borrow().iter().for_each(|f| f(ix) );
                        selected.borrow().iter().for_each(|f| f(None) );
                        // TODO remove from settings
                    },
                    ConnectionAction::CloseWindow => {
                        // println!("Replacing with {:?}", conns.0);
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
        self.added.borrow_mut().push(boxed::Box::new(f))
    }

    pub fn connect_updated(&self, f : impl Fn((i32, ConnectionInfo)) + 'static) {
        self.updated.borrow_mut().push(boxed::Box::new(f))
    }

    pub fn connect_removed(&self, f : impl Fn(i32) + 'static) {
        self.removed.borrow_mut().push(boxed::Box::new(f))
    }

    pub fn connect_selected(&self, f : impl Fn(Option<(i32, ConnectionInfo)>) + 'static) {
        self.selected.borrow_mut().push(boxed::Box::new(f))
    }

}

impl React<ConnectionBox> for ConnectionSet {

    fn react(&self, _conn_bx : &ConnectionBox) {
        // conn_bx.switch.connect_activate(move |switch| {
        // });
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
        conn.connect_db_connected(move |(info, _)| {
            send.send(ConnectionAction::Update(info));
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
    }

}

fn generate_conn_str(
    host_entry : &Entry,
    db_entry : &Entry,
    user_entry : &Entry,
    password_entry : &PasswordEntry
) -> Result<(ConnectionInfo, String), String> {
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
    let password_s = password_entry.text().as_str().to_owned();
    if password_s.is_empty() {
        return Err(format!("Missing password"));
    }
    let split_port : Vec<String> = host_s.split(":").map(|s| s.to_string() ).collect();
    let mut port_s = String::from("5432");
    match split_port.len() {
        0..=1 => { },
        2 => {
            host_s = split_port[0].clone();
            port_s = split_port[1].clone();
        },
        _n => {
            return Err(format!("Host string can contain only a single colon"));
        }
    }
    let mut conn_str = "postgresql://".to_owned();
    conn_str += &user_s;
    conn_str = conn_str + ":" + &password_s;
    if host_s == "localhost" || host_s == "127.0.0.1" {
        conn_str = conn_str + "@" + &host_s;
    } else {
        return Err(format!("Remote connections not allowed yet."));
    }
    conn_str = conn_str + ":" + &port_s;
    conn_str = conn_str + "/" + &db_s;

    let mut info : ConnectionInfo = Default::default();
    info.host = host_s.to_string();
    info.database = db_s.to_string();
    info.user = user_s.to_string();
    Ok((info, conn_str))
}

pub enum ErrorKind {

    Client,

    Server,

    EstablishConnection

}

pub enum ActiveConnectionAction {

    ConnectRequest(ConnectionInfo, String),

    ConnectAccepted(boxed::Box<dyn Connection>, ConnectionInfo, Option<DBInfo>),

    ConnectFailure(String),

    Disconnect,

    ExecutionRequest(String),

    StartSchedule(String),

    EndSchedule,

    ExecutionCompleted(Vec<StatementOutput>),

    SchemaUpdate(Option<Vec<DBObject>>),

    ObjectSelected(Option<Vec<usize>>),

    /// Carries path to CSV file.
    TableImport(String),

    Error(String)

}

pub type ActiveConnCallbacks = (Callbacks<(ConnectionInfo, Option<DBInfo>)>, Callbacks<()>, Callbacks<String>);

pub struct ActiveConnection {

    on_connected : Callbacks<(ConnectionInfo, Option<DBInfo>)>,

    on_conn_failure : Callbacks<String>,

    on_disconnected : Callbacks<()>,

    on_error : Callbacks<String>,

    on_exec_result : Callbacks<Vec<StatementOutput>>,

    send : glib::Sender<ActiveConnectionAction>,

    on_schema_update : Callbacks<Option<Vec<DBObject>>>,

    on_object_selected : Callbacks<Option<DBObject>>

}

impl ActiveConnection {

    pub fn new(user_state : &SharedUserState) -> Self {
        let (on_connected, on_disconnected, on_error) : ActiveConnCallbacks = Default::default();
        let on_exec_result : Callbacks<Vec<StatementOutput>> = Default::default();
        let on_conn_failure : Callbacks<String> = Default::default();
        let (send, recv) = glib::MainContext::channel::<ActiveConnectionAction>(glib::source::PRIORITY_DEFAULT);
        let on_schema_update : Callbacks<Option<Vec<DBObject>>> = Default::default();
        let on_object_selected : Callbacks<Option<DBObject>> = Default::default();
        let mut active_schedule = Rc::new(RefCell::new(false));
        let mut listener = SqlListener::launch({
            let send = send.clone();
            move |results| {
                send.send(ActiveConnectionAction::ExecutionCompleted(results)).unwrap();
            }
        });
        let mut schema : Option<Vec<DBObject>> = None;
        let mut selected_obj : Option<DBObject> = None;
        recv.attach(None, {
            let send = send.clone();
            let (on_connected, on_disconnected, on_error, on_exec_result) = (
                on_connected.clone(),
                on_disconnected.clone(),
                on_error.clone(),
                on_exec_result.clone(),
            );
            let on_conn_failure = on_conn_failure.clone();
            let on_object_selected = on_object_selected.clone();
            let on_schema_update = on_schema_update.clone();
            let user_state = (*user_state).clone();
            move |action| {
                match action {
                    ActiveConnectionAction::ConnectRequest(mut conn_info, conn_str) => {
                        thread::spawn({
                            let send = send.clone();
                            move || {
                                match PostgresConnection::try_new(conn_str, conn_info.clone()) {
                                    Ok(mut conn) => {
                                        let db_info = conn.info();
                                        conn_info.details = if let Some(info) = &db_info {
                                            info.details.clone()
                                        } else {
                                            None
                                        };
                                        send.send(ActiveConnectionAction::ConnectAccepted(boxed::Box::new(conn), conn_info, db_info)).unwrap();
                                    },
                                    Err(e) => {
                                        send.send(ActiveConnectionAction::ConnectFailure(e)).unwrap();
                                    }
                                }
                            }
                        });
                    },
                    ActiveConnectionAction::ConnectAccepted(conn, conn_info, db_info) => {
                        schema = db_info.clone().map(|info| info.schema.clone() );
                        selected_obj = None;
                        if let Err(e) = listener.update_engine(conn) {
                            println!("{}", e);
                        }
                        on_connected.borrow().iter().for_each(|f| f((conn_info.clone(), db_info.clone())) );
                    },
                    ActiveConnectionAction::Disconnect => {
                        schema = None;
                        selected_obj = None;
                        on_disconnected.borrow().iter().for_each(|f| f(()) );
                    },
                    ActiveConnectionAction::ExecutionRequest(stmts) => {

                        if listener.is_working() {
                            // This shouldn't happen. The user is prevented from sending statements
                            // when the engine is working.
                            on_error.borrow().iter().for_each(|f| f(format!("Previous statement not completed yet.")) );
                            return glib::Continue(true);
                        }

                        match listener.send_command(stmts, HashMap::new(), true, user_state.borrow().execution.statement_timeout as usize) {
                            Ok(_) => { },
                            Err(e) => {
                                on_error.borrow().iter().for_each(|f| f(e.clone()) );
                            }
                        }
                    },
                    ActiveConnectionAction::StartSchedule(stmts) => {
                        active_schedule.replace(true);
                        glib::timeout_add_local(Duration::from_secs(user_state.borrow().execution.execution_interval as u64), {
                            let active_schedule = active_schedule.clone();
                            let on_error = on_error.clone();
                            let listener = listener.clone();
                            let user_state = user_state.clone();
                            move || {

                                // Just ignore this schedule step if the previous statement is not
                                // executed yet. Queries will try to execute it again at the next timeout interval.
                                if listener.is_working() {
                                    return Continue(true);
                                }

                                let send_ans = listener.send_command(
                                    stmts.clone(),
                                    HashMap::new(),
                                    true,
                                    user_state.borrow().execution.statement_timeout as usize
                                );
                                match send_ans {
                                    Ok(_) => { },
                                    Err(e) => {
                                        on_error.borrow().iter().for_each(|f| f(e.clone()) );
                                    }
                                }
                                Continue(*active_schedule.borrow())
                            }
                        });
                    },
                    ActiveConnectionAction::EndSchedule => {
                        active_schedule.replace(false);
                    },
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
                    ActiveConnectionAction::ExecutionCompleted(results) => {
                        let any_schema_updates = results.iter()
                            .find(|res| {
                                match res {
                                    StatementOutput::Modification(_) => true,
                                    _ => false
                                }
                            }).is_some();
                        if any_schema_updates {
                            let send = send.clone();
                            listener.on_db_info_arrived(move |info| {
                                send.send(ActiveConnectionAction::SchemaUpdate(info));
                            });
                        }
                        let fst_error = results.iter()
                            .filter_map(|res| {
                                match res {
                                    StatementOutput::Invalid(e, _) => Some(e.clone()),
                                    _ => None
                                }
                            }).next();
                        if let Some(error) = fst_error {
                            on_error.borrow().iter().for_each(|f| f(error.clone()) );
                        } else {
                            on_exec_result.borrow().iter().for_each(|f| f(results.clone()) );
                        }
                    },
                    ActiveConnectionAction::SchemaUpdate(opt_schema) => {
                        schema = opt_schema.clone();
                        selected_obj = None;
                        on_schema_update.borrow().iter().for_each(|f| f(opt_schema.clone()) );
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
                        on_object_selected.borrow().iter().for_each(|f| f(selected_obj.clone()) );
                    },
                    ActiveConnectionAction::ConnectFailure(e) => {
                        on_conn_failure.borrow().iter().for_each(|f| f(e.clone()) );
                    },
                    ActiveConnectionAction::Error(e) => {
                        on_error.borrow().iter().for_each(|f| f(e.clone()) );
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
            on_object_selected
        }
    }

    pub fn connect_db_connected<F>(&self, f : F)
    where
        F : Fn((ConnectionInfo, Option<DBInfo>)) + 'static
    {
        self.on_connected.borrow_mut().push(boxed::Box::new(f));
    }

    pub fn connect_db_disconnected<F>(&self, f : F)
    where
        F : Fn(()) + 'static
    {
        self.on_disconnected.borrow_mut().push(boxed::Box::new(f));
    }

    pub fn connect_db_error<F>(&self, f : F)
    where
        F : Fn(String) + 'static
    {
        self.on_error.borrow_mut().push(boxed::Box::new(f));
    }

    pub fn connect_db_conn_failure<F>(&self, f : F)
    where
        F : Fn(String) + 'static
    {
        self.on_conn_failure.borrow_mut().push(boxed::Box::new(f));
    }

    pub fn connect_exec_result<F>(&self, f : F)
    where
        F : Fn(Vec<StatementOutput>) + 'static
    {
        self.on_exec_result.borrow_mut().push(boxed::Box::new(f));
    }

    pub fn connect_schema_update<F>(&self, f : F)
    where
        F : Fn(Option<Vec<DBObject>>) + 'static
    {
        self.on_schema_update.borrow_mut().push(boxed::Box::new(f));
    }

    pub fn connect_object_selected<F>(&self, f : F)
    where
        F : Fn(Option<DBObject>) + 'static
    {
        self.on_object_selected.borrow_mut().push(boxed::Box::new(f));
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

impl<'a> React<(&'a ConnectionBox, &'a SharedUserState)> for ActiveConnection {

    fn react(&self, r : &(&'a ConnectionBox, &'a SharedUserState)) {
        let conn_bx = r.0;
        let (host_entry, db_entry, user_entry, password_entry) = (
            conn_bx.host.entry.clone(),
            conn_bx.db.entry.clone(),
            conn_bx.user.entry.clone(),
            conn_bx.password.entry.clone()
        );
        let send = self.send.clone();
        let state = r.1.clone();
        conn_bx.switch.connect_state_set(move |switch, _state| {

            if switch.is_active() {
                if host_entry.text().starts_with("file") {
                    unimplemented!()
                }

                match generate_conn_str(&host_entry, &db_entry, &user_entry, &password_entry) {
                    Ok((mut info, conn_str)) => {

                        let mut state = state.borrow_mut();
                        for c in state.conns.iter() {
                            if c.host == info.host {
                                if let Some(cert) = c.cert.as_ref() {
                                    info.cert = Some(cert.to_string());
                                }
                            }
                        }

                        if info.cert.is_none() {
                            for c in state.unmatched_certs.clone().iter() {
                                if c.host == info.host {
                                    info.cert = Some(c.cert.to_string());
                                    while let Some(ix) = state.conns.iter().cloned().position(|conn| conn.host == c.host ) {
                                        state.conns[ix].cert = Some(c.cert.to_string());
                                    }
                                }
                            }
                        }

                        send.send(ActiveConnectionAction::ConnectRequest(info, conn_str)).unwrap();
                    },
                    Err(e) => {
                        send.send(ActiveConnectionAction::Error(e)).unwrap();
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
                            DBObject::Table { name, .. } | DBObject::View { name, .. } => {
                                send.send(ActiveConnectionAction::ExecutionRequest(format!("select * from {} limit 500;", name)));
                            },
                            _ => { }
                        }
                    }
                }
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
                    DBObject::Table { name, cols, .. } => {
                        let tys : Vec<DBType> = cols.iter().map(|col| col.1 ).collect();
                        match sql_literal_tuple(&entries, &tys) {
                            Ok(tuple) => {
                                let insert_stmt = format!("insert into {} values {};", name, tuple);

                                // println!("{}", insert_stmt);

                                send.send(ActiveConnectionAction::ExecutionRequest(insert_stmt));
                            },
                            Err(e) => {
                                send.send(ActiveConnectionAction::Error(e));
                            }
                        }
                    },
                    DBObject::Function { name, args, ret, .. } => {
                        if args.len() == 0 {

                            // With no arguments, we might have a function (with return value) or
                            // a procedure. The syntax is slightly different for procedures.
                            let call_stmt = if ret.is_some() {
                                format!("select {}();", name)
                            } else {
                                format!("call {}();", name)
                            };

                            send.send(ActiveConnectionAction::ExecutionRequest(call_stmt));
                        } else {
                            match sql_literal_tuple(&entries, &args) {
                                Ok(tuple) => {
                                    let call_stmt = format!("select {}{};", name, tuple);
                                    send.send(ActiveConnectionAction::ExecutionRequest(call_stmt));
                                },
                                Err(e) => {
                                    send.send(ActiveConnectionAction::Error(e));
                                }
                            }
                        }
                    },
                    _ => { }
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
                            println!("Asked to save to path {:?}", path);
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


