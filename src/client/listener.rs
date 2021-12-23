use std::thread::{self, JoinHandle};
use crate::sql::{QueryResult, LocalStatement};
// use super::SqlEngine;
use std::sync::{Arc, Mutex, mpsc::{self, channel, Sender, Receiver}};
use super::*;
use std::collections::HashMap;
use std::time::Duration;
use crate::server::Connection;
use sqlparser::ast::Statement;
use crate::sql::object::{DBObject, DBInfo};
use crate::sql::parsing;
use crate::sql::parsing::AnyStatement;
use std::ops::Deref;

pub struct SqlListener {

    // _handle : JoinHandle<()>,

    ans_receiver : Receiver<Vec<QueryResult>>,

    // info_receiver : Receiver<Option<DBInfo>>,

    // info_sender : Sender<()>,

    /// Carries a query sequence; sequence substitutions; and whether this query should be parsed at the client

    cmd_sender : Sender<(String, HashMap<String, String>, bool)>,

    pub engine : Arc<Mutex<Option<Box<dyn Connection>>>>,

    pub last_cmd : Arc<Mutex<Vec<String>>>,

    listen_channels : Vec<String>,

    //loader : Arc<Mutex<FunctionLoader>>
}

impl SqlListener {

    pub fn update_engine(&mut self, engine : Box<dyn Connection>) -> Result<(), String> {
        self.listen_channels.clear();
        if let Ok(mut old_engine) = self.engine.lock() {
            *old_engine = Some(engine);
            Ok(())
        } else {
            Err("Error acquiring lock over engine when updating it".into())
        }
    }

    pub fn listen_to_notification(&mut self, channel : &str, filter : &str) {
        if !self.listen_channels.iter().find(|ch| &ch[..] == channel ).is_some() {
            // self.listener.listen_to_notification(channel);
            self.listen_channels.push(channel.to_string());
        }
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

    pub fn launch() -> Self {
        let (cmd_tx, cmd_rx) = mpsc::channel::<(String, HashMap<String, String>, bool)>();
        let (ans_tx, ans_rx) = mpsc::channel::<Vec<QueryResult>>();
        let engine : Arc<Mutex<Option<Box<dyn Connection>>>> = Arc::new(Mutex::new(None));
        let listen_channels = Vec::new();

        // Channel listening thread
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
        });

        // Statement listening thread.
        thread::spawn({
            let engine = engine.clone();
            move ||  {
                //let loader = loader.clone();
                loop {
                    // TODO perhaps move SQL parsing to here so loader is passed to
                    // try_run iff there are local functions matching the query.
                    match cmd_rx.recv() {
                        Ok((cmd, subs, parse)) => {
                            match **engine.lock().as_mut().unwrap() {
                                Some(ref mut eng) => {
                                    let result = eng.try_run(cmd, &subs, parse,  /*Some(&loader)*/ );
                                    match result {
                                        Ok(ans) => {
                                            if let Err(e) = ans_tx.send(ans) {
                                                println!("{}", e);
                                            }
                                        },
                                        Err(e) => {
                                            let inv_res = vec![QueryResult::Invalid( e.to_string(), false )];
                                            if let Err(e) = ans_tx.send(inv_res) {
                                                println!("{}", e);
                                            }
                                        }
                                    }
                                },
                                None => {

                                },
                                // Err(e) => {
                                //    panic!("Failed to acquire lock over engine: {}", e);
                                // }
                            }
                        },
                        Err(e) => {
                            // println!("Receiver on SQL engine thread found a closed channel");
                            return;
                        }
                    }
                }
            }
        });

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
            ans_receiver : ans_rx,
            cmd_sender : cmd_tx,
            engine,
            last_cmd : Arc::new(Mutex::new(Vec::new())),
            listen_channels
        }
    }

    /// Tries to parse SQL at client side. If series of statements at string
    /// are correctly parsed, send the SQL to the server. If sequence is not
    /// correctly parsed, do not send anything to the server, and return the
    /// error to the user.
    pub fn send_command(&self, sql : String, subs : HashMap<String, String>, parse : bool) -> Result<(), String> {

        if sql.chars().all(|c| c.is_whitespace() ) {
            return Err(String::from("Empty statement sequence"));
        }

        if let Ok(mut last_cmd) = self.last_cmd.lock() {
            last_cmd.clear();
            self.clear_results();
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
        }
        self.cmd_sender.send((sql.clone(), subs, parse))
            .expect("Error sending SQL command over channel");
        Ok(())
    }

    /// Gets all results which might have been queued at the receiver.
    pub fn maybe_get_result(&self) -> Option<Vec<QueryResult>> {
        let mut full_ans = Vec::new();
        while let Ok(ans) = self.ans_receiver.try_recv() {
            full_ans.extend(ans);
        }
        if full_ans.len() > 0 {
            Some(full_ans)
        } else {
            None
        }
    }

    pub fn clear_results(&self) {
        while let Ok(mut res) = self.ans_receiver.try_recv() {
            let _ = res;
        }
    }

    pub fn last_commands(&self) -> Vec<String> {
        if let Ok(cmds) = self.last_cmd.lock() {
            cmds.clone()
        } else {
            println!("Unable to acquire lock over last commands");
            Vec::new()
        }
    }

    // pub fn request_db_info(&self) {
    //    self.info_sender.send(());
    // }

    pub fn db_info(&self) -> Option<DBInfo> {
        /*match self.info_recv.try_recv() {
            Ok(info) => Ok(info),
            _ => Err(format!("Info unavailable"))
        }*/
        if let Some(ref mut engine) = *self.engine.lock().unwrap() {
            let opt_info = engine.info();
            // if let Some(info) = &opt_info {
            // println!("{}", crate::sql::object::build_er_diagram(String::new(), &info[..]));
            // }
            opt_info
        } else {
            println!("Unable to acquire lock over SQL engine");
            None
        }
    }

}

