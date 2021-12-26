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
use std::time::Duration;
use std::thread;
use crate::sql::object::DBInfo;
use crate::sql::StatementOutput;
use crate::ui::ExecButton;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionInfo {
    pub host : String,
    pub user : String,
    pub database : String,
    pub encoding : String,
    pub size : String,
    pub locale : String
}

impl ConnectionInfo {

    pub fn is_default(&self) -> bool {
        &self.host[..] == "Host" && &self.user[..] == "User" && &self.database[..] == "Database" &&
        &self.encoding[..] == "Unknown" && &self.size[..] == "Unknown" && &self.locale[..] == "Unknown"
    }

}

impl Default for ConnectionInfo {

    fn default() -> Self {
        Self {
            host : String::from("Host"),
            user : String::from("User"),
            database : String::from("Database"),
            encoding : String::from("Unknown"),
            size : String::from("Unknown"),
            locale : String::from("Unknown"),
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
    Add,
    Remove(i32)
}

pub struct ConnectionSet {

    added : Callbacks<ConnectionInfo>,

    removed : Callbacks<i32>,

    selected : Callbacks<Option<(i32, ConnectionInfo)>>,

    send : glib::Sender<ConnectionAction>

}

impl ConnectionSet {

    pub fn new() -> Self {
        let (send, recv) = MainContext::channel::<ConnectionAction>(glib::source::PRIORITY_DEFAULT);
        let (selected, added, removed) : (Callbacks<Option<(i32, ConnectionInfo)>>, Callbacks<ConnectionInfo>, Callbacks<i32>) = Default::default();
        recv.attach(None, {
            let mut conns : (Vec<ConnectionInfo>, Option<i32>) = (Vec::new(), None);
            let (selected, added, removed) = (selected.clone(), added.clone(), removed.clone());
            move |action| {
                match action {
                    ConnectionAction::Switch(opt_ix) => {
                        conns.1 = opt_ix;
                        selected.borrow().iter().for_each(|f| f(opt_ix.map(|ix| (ix, conns.0[ix as usize].clone() ))) );
                    },
                    ConnectionAction::Add => {
                        conns.0.push(Default::default());
                        conns.1 = None;
                        added.borrow().iter().for_each(|f| f(Default::default()) );
                    },
                    ConnectionAction::Remove(ix) => {
                        let rem_conn = conns.0.remove(ix as usize);
                        removed.borrow().iter().for_each(|f| f(ix) );
                        selected.borrow().iter().for_each(|f| f(None) );
                    },
                }
                Continue(true)
            }
        });
        Self {
            send,
            selected,
            added,
            removed
        }
    }

    pub fn connect_added(&self, f : impl Fn(ConnectionInfo) + 'static) {
        self.added.borrow_mut().push(boxed::Box::new(f))
    }

    pub fn connect_removed(&self, f : impl Fn(i32) + 'static) {
        self.removed.borrow_mut().push(boxed::Box::new(f))
    }

    pub fn connect_selected(&self, f : impl Fn(Option<(i32, ConnectionInfo)>) + 'static) {
        self.selected.borrow_mut().push(boxed::Box::new(f))
    }

}

impl React<ConnectionBox> for ConnectionSet {

    fn react(&self, conn_bx : &ConnectionBox) {
        // conn_bx.switch.connect_activate(move |switch| {
        // });
    }

}

impl React<ConnectionList> for ConnectionSet {

    fn react(&self, conn_list : &ConnectionList) {
        conn_list.list.connect_row_selected({
            let send = self.send.clone();
            move |_, opt_row| {
                send.send(ConnectionAction::Switch(opt_row.map(|row| row.index() )));
            }
        });
        conn_list.add_btn.connect_clicked({
            let send = self.send.clone();
            move |_btn| {
                send.send(ConnectionAction::Add);
            }
        });
        conn_list.remove_btn.connect_clicked({
            let send = self.send.clone();
            let list = conn_list.list.clone();
            move |_btn| {
                if let Some(ix) = list.selected_row().map(|row| row.index() ) {
                    send.send(ConnectionAction::Remove(ix));
                }
            }
        });
    }

}

fn generate_conn_str(
    host_entry : &Entry,
    db_entry : &Entry,
    user_entry : &Entry,
    password_entry : &PasswordEntry
) -> Result<String, String> {
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
        n => {
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
    Ok(conn_str)
}

pub enum ErrorKind {

    Client,

    Server,

    EstablishConnection

}

pub enum ActiveConnectionAction {

    ConnectRequest(String),

    ConnectAccepted(boxed::Box<dyn Connection>, Option<DBInfo>),

    Disconnect,

    ExecutionRequest(String),

    ExecutionCompleted(Vec<StatementOutput>),

    Error(String)

}

pub type ActiveConnCallbacks = (Callbacks<Option<DBInfo>>, Callbacks<()>, Callbacks<String>);

pub struct ActiveConnection {

    on_connected : Callbacks<Option<DBInfo>>,

    on_disconnected : Callbacks<()>,

    on_error : Callbacks<String>,

    on_exec_result : Callbacks<Vec<StatementOutput>>,

    send : glib::Sender<ActiveConnectionAction>

}

impl ActiveConnection {

    pub fn new() -> Self {
        let (on_connected, on_disconnected, on_error) : ActiveConnCallbacks = Default::default();
        let on_exec_result : Callbacks<Vec<StatementOutput>> = Default::default();
        let (send, recv) = glib::MainContext::channel::<ActiveConnectionAction>(glib::source::PRIORITY_DEFAULT);
        let mut listener = SqlListener::launch({
            let send = send.clone();
            move |results| {
                send.send(ActiveConnectionAction::ExecutionCompleted(results));
            }
        });
        recv.attach(None, {
            let send = send.clone();
            let (on_connected, on_disconnected, on_error, on_exec_result) = (
                on_connected.clone(),
                on_disconnected.clone(),
                on_error.clone(),
                on_exec_result.clone(),
            );
            move |action| {
                match action {
                    ActiveConnectionAction::ConnectRequest(conn_str) => {
                        thread::spawn({
                            let send = send.clone();
                            move || {
                                match PostgresConnection::try_new(conn_str) {
                                    Ok(mut conn) => {
                                        let info = conn.info();
                                        send.send(ActiveConnectionAction::ConnectAccepted(boxed::Box::new(conn), info));
                                    },
                                    Err(e) => {
                                        send.send(ActiveConnectionAction::Error(e));
                                    }
                                }
                            }
                        });
                    },
                    ActiveConnectionAction::ConnectAccepted(conn, info) => {
                        listener.update_engine(conn);
                        on_connected.borrow().iter().for_each(|f| f(info.clone()) );
                    },
                    ActiveConnectionAction::Disconnect => {
                        on_disconnected.borrow().iter().for_each(|f| f(()) );
                    },
                    ActiveConnectionAction::ExecutionRequest(stmts) => {
                        match listener.send_command(stmts, HashMap::new(), true) {
                            Ok(_) => { },
                            Err(e) => {
                                on_error.borrow().iter().for_each(|f| f(e.clone()) );
                            }
                        }
                    },
                    ActiveConnectionAction::ExecutionCompleted(results) => {
                        let fst_error = results.iter()
                            .filter_map(|res| {
                                match res {
                                    StatementOutput::Invalid(e, _) => Some(e.clone()),
                                    _ => None
                                }
                            })
                            .next();
                        if let Some(error) = fst_error {
                            on_error.borrow().iter().for_each(|f| f(error.clone()) );
                        } else {
                            on_exec_result.borrow().iter().for_each(|f| f(results.clone()) );
                        }
                    },
                    ActiveConnectionAction::Error(e) => {
                        on_error.borrow().iter().for_each(|f| f(e.clone()) );
                    }
                }
                glib::Continue(true)
            }
        });

        // TODO create glib timeout to listen to commands. The send channel will be cloned into this timeout.

        Self {
            on_connected,
            on_disconnected,
            on_error,
            send,
            on_exec_result,
        }
    }

    pub fn connect_db_connected<F>(&self, f : F)
    where
        F : Fn(Option<DBInfo>) + 'static
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

    pub fn connect_exec_result<F>(&self, f : F)
    where
        F : Fn(Vec<StatementOutput>) + 'static
    {
        self.on_exec_result.borrow_mut().push(boxed::Box::new(f));
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
        let (host_entry, db_entry, user_entry, password_entry) = (
            conn_bx.host.entry.clone(),
            conn_bx.db.entry.clone(),
            conn_bx.user.entry.clone(),
            conn_bx.password.entry.clone()
        );
        let send = self.send.clone();
        conn_bx.switch.connect_state_set(move |switch, state| {

            if switch.is_active() {
                if host_entry.text().starts_with("file") {
                    unimplemented!()
                }

                match generate_conn_str(&host_entry, &db_entry, &user_entry, &password_entry) {
                    Ok(conn_str) => {
                        send.send(ActiveConnectionAction::ConnectRequest(conn_str));
                    },
                    Err(e) => {
                        send.send(ActiveConnectionAction::Error(e));
                    }
                }
            } else {
                send.send(ActiveConnectionAction::Disconnect);
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
        btn.exec_action.connect_activate(move |action, param| {
            let stmts = param.unwrap().get::<String>().unwrap();
            send.send(ActiveConnectionAction::ExecutionRequest(stmts));
            // println!("Should execute: {}", );
        });
    }

}
