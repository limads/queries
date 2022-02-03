#![allow(warnings)]

use std::boxed;
use std::cell::RefCell;
use std::rc::Rc;
use once_cell::sync::OnceCell;
use anyhow;
use gtk4::glib::{LogLevel, g_log};
use std::fmt::Display;

pub mod ui;

pub mod client;

pub mod server;

pub mod sql;

pub mod command;

pub const SETTINGS_PATH : &'static str = "/home/diego/.local/share/flatpak/app/com.github.limads.queries/user.json";

pub const APP_ID : &'static str = "com.github.limads.queries";

pub static DEBUG_LOG : OnceCell<bool> = OnceCell::new();

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
}

pub type Callbacks<T> = Rc<RefCell<Vec<boxed::Box<dyn Fn(T) + 'static>>>>;

pub type ValuedCallbacks<A, R> = Rc<RefCell<Vec<boxed::Box<dyn Fn(A)->R + 'static>>>>;

/// Generic trait to represent interactions between Views (widgets or sets of grouped widgets affected by data change),
/// Models (data structures that encapsulate data-modifying algorithms) and controls (widgets
/// that modify the data contained in models). Widgets that affect a model (Controls) are represented by having the model imlement React<Widget>.
/// Widgets that are affected by a model (Views) are represented by having the widget implement React<Model>.
/// The implementation will usually bind one or more closures to the argument. Usually, an widget is either a control OR a view
/// with respect to a given model, but might a assume both roles. A widget might also be a view for one model but the control for another model. Models
/// usually encapsulate a call to glib::Receiver::attach(.), waiting for messages that change their state. The models are implemented
/// by closures activated on "signals", implemented using Rust enums. The actual data is not held in the model structure, but is owned
/// by a single closure executing on the main thread whenever some signal enum is received. If required, the model might spawn new
/// threads or wait for response from worker threads, but they should never block.
pub trait React<S> {

    fn react(&self, source : &S);

}

