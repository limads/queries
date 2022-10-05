/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use std::boxed;
use std::cell::RefCell;
use std::rc::Rc;
use once_cell::sync::OnceCell;
use anyhow;
use gtk4::glib::{LogLevel, g_log};
use std::fmt::Display;

pub mod tables;

pub mod ui;

pub mod client;

pub mod server;

pub mod sql;

pub mod command;

pub const SETTINGS_FILE : &'static str = "user.json";

// pub const SETTINGS_PATH : &'static str = "/home/diego/.local/share/flatpak/app/com.github.limads.queries/user.json";

pub const APP_ID : &'static str = "com.github.limads.queries";

/*pub static DEBUG_LOG : OnceCell<bool> = OnceCell::new();

pub fn log_debug_if_required(msg : &str) {
    if let Some(debug) = DEBUG_LOG.get() {
        if *debug {
            g_log!(LogLevel::Debug, "{}", msg);
        }
    } else {
        log_critical_then_abort(anyhow::Error::msg("QUERIES_DEBUG not initialized"));
    }
}

pub fn log_critical_then_abort<E>(e : E)
where
    E : Display
{
    g_log!(LogLevel::Critical, "{}", e);
    panic!("Queries aborted ({})", e);
}

pub fn log_error<E>(e : E)
where
    E : Display
{
    g_log!(LogLevel::Error, "{}", e);
}*/


