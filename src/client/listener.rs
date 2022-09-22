use std::thread::{self, JoinHandle};
use crate::sql::{StatementOutput, LocalStatement};
use std::sync::{Arc, Mutex, mpsc::{self, channel, Sender, Receiver}};
use super::*;
use std::collections::HashMap;
use std::time::Duration;
use crate::server::Connection;
use sqlparser::ast::Statement;
use crate::sql::object::{DBObject, DBInfo};
use crate::sql::{parsing, SafetyLock};
use crate::sql::parsing::AnyStatement;
use std::ops::Deref;
use stateful::Callbacks;
use std::fs::File;
use std::io::Read;
use crate::sql::copy::*;
use crate::tables::table::*;

#[derive(Debug, Clone)]
pub struct ExecutionRequest {
    sql : String,
    subs : HashMap<String, String>,
    safety : SafetyLock,
    timeout : usize,
    mode : ExecMode
}

#[derive(Clone)]
pub struct SqlListener {

    /// Carries a query sequence; sequence substitutions; and whether this query should be parsed at the client; and a timeout in seconds.
    cmd_sender : Sender<ExecutionRequest>,

    pub engine : Arc<Mutex<Option<Box<dyn Connection>>>>,

    pub last_cmd : Arc<Mutex<Vec<String>>>,

    listen_channels : Arc<Mutex<Vec<String>>>,

    handle : Arc<JoinHandle<()>>

}

impl SqlListener {

    pub fn update_engine(&mut self, engine : Box<dyn Connection>) -> Result<(), String> {
        self.listen_channels.lock().unwrap().clear();
        if let Ok(mut old_engine) = self.engine.lock() {
            *old_engine = Some(engine);
            Ok(())
        } else {
            Err("Error acquiring lock over engine when updating it".into())
        }
    }

    pub fn listen_to_notification(&mut self, channel : &str, filter : &str) {
        /*if !self.listen_channels.lock().unwrap().iter().find(|ch| &ch[..] == channel ).is_some() {
            // self.listener.listen_to_notification(channel);
            self.listen_channels.push(channel.to_string());
        }*/
    }

    pub fn clear_notifications(&self) {
        /*match *self.engine.lock().unwrap() {
            SqlEngine::PostgreSql { ref mut channel, .. } => {
                *channel = None;
            },
            _ => { }
        }*/
    }

    pub fn has_notification_queued(&self, at_channel : &str, filter : &str) -> bool {
        /*if at_channel.is_empty() {
            return false;
        }
        match *self.engine.lock().unwrap() {
            SqlEngine::PostgreSql { ref mut channel, .. } => {

                // If there isn't a notification queued yet, queue it and wait for the next iteration.
                if let Some(ref mut ch) = channel {

                    // Use is requesting a channel different from the configured one. Re-configure
                    // the channel and returns false, waiting for new notification.
                    if &ch.0[..] != &at_channel[..] || &ch.1[..] != filter {
                        ch.0 = format!("{}", at_channel);
                        ch.1 = format!("{}", filter);
                        return false;
                    }

                    // Sets "notification queued" status to false and return true: There
                    // is a notification queued for now.
                    if ch.2 {
                        ch.2 = false;
                        return true;
                    }

                } else {
                    *channel = Some((format!("{}", at_channel), format!("{}", filter), false));
                }
            },
            _ => {
                // Listen only supported on PostgreSQL engine
            }
        }
        false*/
        unimplemented!()
    }

    pub fn launch<F>(result_cb : F) -> Self
    where
        F : Fn(Vec<StatementOutput>, ExecMode) + 'static + Send
    {
        let (cmd_tx, cmd_rx) = mpsc::channel::<ExecutionRequest>();
        let engine : Arc<Mutex<Option<Box<dyn Connection>>>> = Arc::new(Mutex::new(None));

        /*// Channel listening thread
        thread::spawn({
            let engine = engine.clone();
            move|| {
            /*loop {
                match *engine.lock().unwrap() {
                    Some(ref mut engine) => {
                        // if let Some(ch) = self.listen_channels.get(0).clone() {
                        //    engine.listen_at_channel(ch);
                        // }
                    },
                    _ => {
                        println!("Listen only supported on PostgreSQL engine");
                    }
                }
                thread::sleep(Duration::from_millis(16));
            }*/
            }
        });*/
        // let on_results_arrived : Callbacks<&'a [StatementOutput]> = Default::default();

        // Statement listening thread.
        let handle = spawn_listener_thread(engine.clone(), result_cb, cmd_rx);

        /*let (info_sender, info_r) = mpsc::channel::<()>();
        let (info_s, info_recv) = mpsc::channel::<Option<DBInfo>>();
        // Database info thread
        thread::spawn({
            let engine = self.engine.clone();
            move|| {
                loop {
                    if let Ok(_) = info_r.recv() {
                        let info = if let Some(ref mut engine) = *engine.lock().unwrap() {
                            let opt_info = engine.info();
                            if let Some(info) = &opt_info {
                                // println!("{}", crate::sql::object::build_er_diagram(String::new(), &info[..]));
                            }
                            opt_info
                        } else {
                            println!("Unable to acquire lock over SQL engine");
                            None
                        };
                        info_s.push(info);
                    }
                }
            }
        });*/
        Self {
            // info_sender,
            // info_receiver,
            // ans_receiver : Some(ans_rx),
            cmd_sender : cmd_tx,
            engine,
            last_cmd : Arc::new(Mutex::new(Vec::new())),
            listen_channels : Arc::new(Mutex::new(Vec::new())),
            handle : Arc::new(handle)
            
        }
    }

    pub fn send_single_command(&self, sql : String, timeout : usize, safety : SafetyLock) -> Result<(), String> {
        match self.cmd_sender.send(ExecutionRequest { sql : sql.clone(), subs : HashMap::new(), safety, timeout, mode : ExecMode::Single }) {
            Ok(_) => {

            },
            Err(e) => {
                // Most likely, a panic when running the client caused this.
                return Err(format!("Database connection thread is down.\nPlease restart the application."));
            }
        }
        Ok(())
    }

    /// Tries to parse SQL at client side. If series of statements at string
    /// are correctly parsed, send the SQL to the server. If sequence is not
    /// correctly parsed, do not send anything to the server, and return the
    /// error to the user.
    pub fn send_commands(&self, sql : String, subs : HashMap<String, String>, safety : SafetyLock, timeout : usize) -> Result<(), String> {

        // Before sending a command, it might be interesting to check if self.handle.is_running()
        // when this stabilizes at the stdlib. If it is not running (i.e. there is a panic at the
        // database connection thread), we re-launch it. To do that, we must establish a new command
        // (receiver, sender) pair, since the original sender will be de-allocated if the database
        // thread panics.

        if sql.chars().all(|c| c.is_whitespace() ) {
            return Err(String::from("Empty statement sequence"));
        }

        /*if let Ok(mut last_cmd) = self.last_cmd.lock() {
            last_cmd.clear();
            match parse {
                true => {
                    match crate::sql::parsing::parse_sql(&sql[..], &subs) {
                        Ok(stmts) => {
                            for stmt in stmts.iter() {
                                let stmt_txt = match stmt {
                                    Statement::Query(_) => String::from("select"),
                                    _ => String::from("other")
                                };
                                last_cmd.push(stmt_txt);
                            }
                        },
                        Err(e) => {
                            for stmt in parsing::split_unparsed_statements(sql.clone())? {
                                match stmt {
                                    AnyStatement::Raw(_, _, is_select) => {
                                        let stmt_txt = match is_select {
                                            true => String::from("select"),
                                            false => String::from("other")
                                        };
                                        last_cmd.push(stmt_txt);
                                    },
                                    AnyStatement::Local(local) => {
                                        match local {
                                            LocalStatement::Exec(ref exec) => if exec.into.is_none() {
                                                last_cmd.push(String::from("select"))
                                            } else {
                                                last_cmd.push(String::from("other"))
                                            },
                                            _ => last_cmd.push(String::from("other"))
                                        }
                                    },
                                    _ => { /*Parsed variant not expected here*/ }
                                }
                            }
                        }
                    }
                },
                false => {
                    last_cmd.push(String::from("other"));
                }
            }
        } else {
            return Err(format!("Unable to acquire lock over last commands"));
        }*/

        let request = ExecutionRequest { 
            sql : sql.clone(), 
            subs, 
            safety, 
            timeout, 
            mode : ExecMode::Multiple 
        };
        match self.cmd_sender.send(request) {
            Ok(_) => {

            },
            Err(e) => {
                // Most likely, a panic when running the client caused this.
                return Err(format!("Database connection thread is down.\nPlease restart the application."));
            }
        }

        Ok(())
    }

    /*/// Gets all results which might have been queued at the receiver.
    pub fn maybe_get_result(&self) -> Option<Vec<StatementOutput>> {
        let mut full_ans = Vec::new();
        while let Ok(ans) = self.ans_receiver.as_ref().unwrap().try_recv() {
            full_ans.extend(ans);
        }
        if full_ans.len() > 0 {
            Some(full_ans)
        } else {
            None
        }
    }

    pub fn wait_for_results(&self) -> Vec<StatementOutput> {
        let mut full_ans = Vec::new();
        while let Ok(ans) = self.ans_receiver.as_ref().unwrap().recv() {
            full_ans.extend(ans);
        }
        full_ans
    }

    pub fn take_receiver(&mut self) -> Receiver<Vec<StatementOutput>> {
        self.ans_receiver.take().unwrap()
    }

    pub fn give_back_receiver(&mut self, recv : Receiver<Vec<StatementOutput>>) {
        assert!(self.ans_receiver.is_none());
        self.ans_receiver = Some(recv);
    }*/

    /*pub fn clear_results(&self) {
        while let Ok(mut res) = self.ans_receiver.as_ref().unwrap().try_recv() {
            let _ = res;
        }
    }*/

    /*pub fn last_commands(&self) -> Vec<String> {
        if let Ok(cmds) = self.last_cmd.lock() {
            cmds.clone()
        } else {
            println!("Unable to acquire lock over last commands");
            Vec::new()
        }
    }*/

    // pub fn request_db_info(&self) {
    //    self.info_sender.send(());
    // }

    pub fn is_running(&self) -> bool {
        self.engine.try_lock().is_err()
    }

    pub fn db_info(&self) -> Option<DBInfo> {
        /*match self.info_recv.try_recv() {
            Ok(info) => Ok(info),
            _ => Err(format!("Info unavailable"))
        }*/
        if let Ok(ref mut opt_engine) = self.engine.lock() {
            if let Some(ref mut engine) = opt_engine.as_mut() {
                let opt_info = engine.db_info().ok();
                // if let Some(info) = &opt_info {
                // println!("{}", crate::sql::object::build_er_diagram(String::new(), &info[..]));
                // }
                opt_info
            } else {
                println!("No active engine");
                None
            }
        } else {
            println!("Unable to acquire lock over SQL engine");
            None
        }
    }

    /// Queries the database info, executing the given closure when the
    /// info arrives.
    pub fn spawn_db_info(&self, f : impl Fn(Option<Vec<DBObject>>) + Send + 'static) {
        let engine = self.engine.clone();
        thread::spawn(move|| {
            if let Ok(mut opt_engine) = engine.lock() {
                if let Some(mut engine) = opt_engine.as_mut() {
                    f(engine.db_info().ok().map(|info| info.schema ));
                } else {
                    f(None);
                }
            } else {
                println!("Unable to acquire lock over engine");
            }
        });
    }

    pub fn on_import_request_done(
        &self,
        path : String,
        action : crate::sql::copy::Copy,
        f : impl Fn(Result<usize, String>)->() + Send + 'static
    ) {
        let engine = self.engine.clone();
        thread::spawn(move|| {
            if let Ok(mut opt_engine) = engine.lock() {
                if let Some(mut engine) = opt_engine.as_mut() {
                    let ans = copy_table_from_csv(path, engine.as_mut(), action);
                    f(ans);
                } else {
                    f(Err(String::from("No active connection")));
                }
            } else {
                println!("Unable to acquire lock over engine");
            }
        });
    }

}

/// The queries table environment only listens to "multiple" mode. Use
/// "single" mode to query information that wont't be displayed as tables.
#[derive(Debug, Clone, Copy)]
pub enum ExecMode {
    Single,
    Multiple
}

fn spawn_listener_thread<F>(
    engine : Arc<Mutex<Option<Box<dyn Connection>>>>,
    result_cb : F,
    cmd_rx : Receiver<ExecutionRequest>
) -> JoinHandle<()>
where
    F : Fn(Vec<StatementOutput>, ExecMode) + 'static + Send
{
    thread::spawn(move ||  {
        loop {
            match cmd_rx.recv() {
            
                Ok(ExecutionRequest { sql, subs, safety, timeout, mode }) => {
                
                    let mut result = Vec::new();
                    
                    match engine.lock() {
                        Ok(mut opt_eng) => match &mut *opt_eng {
                            Some(ref mut eng) => {
                                result = match eng.try_run(sql, &subs, safety) {
                                    Ok(stmt_results) => {
                                        stmt_results
                                    },
                                    Err(e) => {
                                        vec![StatementOutput::Invalid(e.to_string(), false )]
                                    }
                                };
                            },
                            None => {
                                result = vec![StatementOutput::Invalid(format!("Database connection is down. Please restart the connection"), false)];
                            }
                        },
                        Err(_) => {
                            // This is only reachable if the mutex is poisoned, in which case there is nothing
                            // to do but restart the application. This should never be reached in ordinary use.
                            result = vec![StatementOutput::Invalid(format!("Unable to acquire lock over database engine. Please restart the application."), false)];
                        }
                    }
                    
                    assert!(engine.try_lock().is_ok());
                    
                    /* It is important to call the result callback only after the engine mutex
                    is unlocked, so that new statements can be promptly sent after results arrive
                    (used during testing, but a good practice for ordinary use nevertheless). */
                    result_cb(result, mode);
                    
                },
                Err(e) => {

                    // If this is reached, it means the main thread is down. 
                    // There is nothing to do except break the loop.
                    break;
                }
            }
        }
    })
}

fn copy_table_from_csv(
    path : String,
    conn : &mut dyn Connection,
    action : crate::sql::copy::Copy
) -> Result<usize, String> {
    assert!(action.target == CopyTarget::From);
    if let Ok(mut f) = File::open(&path) {
        let mut txt = String::new();
        if let Err(e) = f.read_to_string(&mut txt) {
            return Err(format!("{}", e));
        }
        match Table::new_from_text(txt) {
            Ok(mut tbl) => {
                conn.import(
                    &mut tbl,
                    &action.table[..],
                    &action.cols[..],
                    // false,
                    // true
                )
            },
            Err(e) => {
                Err(format!("Error parsing table: {}", e))
            }
        }
    } else {
        Err(format!("Error opening file"))
    }
}



