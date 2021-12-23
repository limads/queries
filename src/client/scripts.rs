use crate::React;
use crate::ui::{QueriesEditor, ScriptList, SaveDialog};
use crate::ui::MainMenu;

pub struct Scripts {

}

impl Scripts {

    pub fn new() -> Self {

        Self { }
    }

}

pub struct OpenedFile {
    path : Option<String>,
    saved : bool
}

pub struct OpenedScripts {

}

impl OpenedScripts {

    pub fn new() -> Self {

        Self { }
    }

}

impl React<SaveDialog> for OpenedScripts {

    fn react(&self, dialog : &SaveDialog) {

    }

}

impl React<MainMenu> for OpenedScripts {

    fn react(&self, menu : &MainMenu) {
        menu.action_new.connect_activate(move |_,_| {
            println!("New");
        });
        menu.action_open.connect_activate(move |_,_| {
            println!("Open");
        });
    }

}


