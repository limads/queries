use gtk4::prelude::*;
use gtk4::*;
use crate::tables::environment::TableEnvironment;

pub enum WorkspaceAction {

    Add,

    Clear,

    Select

}

pub struct Workspace {

    send : glib::Sender<WorkspaceAction>

}

impl Workspace {

    pub fn new() -> Self {
        let (send, recv) = glib::MainContext::channel::<WorkspaceAction>(glib::PRIORITY_DEFAULT);
        recv.attach(None, {

            move |action| {

                Continue(true)
            }
        });
        Self { send, }
    }
}
