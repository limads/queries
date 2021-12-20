use gtk4::*;
use gtk4::prelude::*;
use crate::{React, Callbacks};
use crate::ui::ConnectionList;
use crate::ui::ConnectionBox;
use std::boxed;
use glib::MainContext;

#[derive(Default, Clone, Debug)]
pub struct ConnectionInfo {
    pub host : String,
    pub user : String,
    pub database : String,
    pub encoding : String,
    pub size : String,
    pub locale : String
}

pub enum ConnectionChange {
    Add(ConnectionInfo),
    Remove(usize)
}


#[derive(Debug, Clone)]
pub enum ConnectionAction {
    Switch(Option<i32>),
    Add(ConnectionInfo)
}

#[derive(Default)]
pub struct Connections {

    added : Callbacks<ConnectionInfo>,

    removed : Callbacks<i32>,

    selected : Callbacks<Option<i32>>,

}

impl Connections {

    pub fn new() -> Self {
        Default::default()
    }

    pub fn connect_added(&self, f : impl Fn(ConnectionInfo) + 'static) {
        self.added.borrow_mut().push(boxed::Box::new(f))
    }

    pub fn connect_removed(&self, f : impl Fn(i32) + 'static) {
        self.removed.borrow_mut().push(boxed::Box::new(f))
    }

    pub fn connect_selected(&self, f : impl Fn(Option<i32>) + 'static) {
        self.selected.borrow_mut().push(boxed::Box::new(f))
    }

}

impl React<ConnectionBox> for Connections {

    fn react(&self, conn_bx : &ConnectionBox) {
        // conn_bx.switch.connect_activate(move |switch| {
        // });
    }

}

impl React<ConnectionList> for Connections {

    fn react(&self, conn_list : &ConnectionList) {
        let (send, recv) = MainContext::channel::<ConnectionAction>(glib::source::PRIORITY_DEFAULT);
        conn_list.list.connect_row_selected({
            let send = send.clone();
            move |_, opt_row| {
                send.send(ConnectionAction::Switch(opt_row.map(|row| row.index() )));
            }
        });
        conn_list.list.connect_row_activated({
            let send = send.clone();
            move|list, row| {
                let n = list.observe_children().n_items();
                if row.index() == (n-1) as i32 {
                    send.send(ConnectionAction::Add(Default::default()));
                }
            }
        });
        recv.attach(None, {
            // let conn_list = conn_list.clone();
            let mut conns : (Vec<ConnectionInfo>, Option<i32>) = (Vec::new(), None);
            let (selected, added, removed) = (self.selected.take(), self.added.take(), self.removed.take());
            move |action| {
                match action {
                    ConnectionAction::Switch(opt_ix) => {
                        conns.1 = opt_ix;
                        selected.iter().for_each(|f| f(opt_ix) );
                    },
                    ConnectionAction::Add(info) => {
                        conns.0.push(info.clone());
                        conns.1 = None;
                        added.iter().for_each(|f| f(info.clone()) );
                    }
                }
                Continue(true)
            }
        });
    }

}

