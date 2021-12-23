use std::boxed;
use std::cell::RefCell;
use std::rc::Rc;

pub mod ui;

pub mod client;

pub mod server;

pub mod sql;

pub mod tables;

pub mod command;

pub type Callbacks<T> = Rc<RefCell<Vec<boxed::Box<dyn Fn(T) + 'static>>>>;

/// Generic trait to signify interactions between Views (widgets or sets of grouped widgets affected by data change),
/// Models (data structures that encapsulate data-modifying algorithms) and controls (widgets
/// that modify the data contained in models). Widgets that affect a model (Controls) are represented by having the model imlement React<Widget>.
/// Widgets that are affected by a model (Views) are represented by having the widget implement React<Model>.
/// The implementation will usually bind one or more closures to the argument. Usually, an widget is either a control OR a view
/// with respect to a given model. But a widget might be a view for one model but the control for another model. Models
/// usually encapsulate a call to glib::Receiver::attach(.), waiting for messages that change their state.
pub trait React<S> {

    fn react(&self, source : &S);

}

