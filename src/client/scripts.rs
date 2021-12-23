use gtk4::prelude::*;
use gtk4::*;
use crate::React;
use crate::ui::{QueriesEditor, ScriptList, SaveDialog, OpenDialog};
use crate::ui::MainMenu;
use crate::Callbacks;
use crate::ui::FileList;
use std::boxed;
use std::thread;

pub enum ScriptAction {

    OpenRequest(String),

    OpenSuccess(OpenedFile),

    OpenFailure(String),

    CloseRequest(usize),

    SaveRequest(usize),

    Opened(String),

    Closed(String),

    NewRequest,

    Select(Option<usize>)

}

pub struct OpenedScripts {

    send : glib::Sender<ScriptAction>,

    on_open : Callbacks<OpenedFile>,

    on_save : Callbacks<OpenedFile>,

    on_new : Callbacks<OpenedFile>,

    on_closed : Callbacks<(usize, usize)>,

    on_selected : Callbacks<Option<usize>>

}

impl OpenedScripts {

    pub fn new() -> Self {
        let (send, recv) = glib::MainContext::channel::<ScriptAction>(glib::PRIORITY_DEFAULT);
        let on_open : Callbacks<OpenedFile> = Default::default();
        let on_new : Callbacks<OpenedFile> = Default::default();
        let on_save : Callbacks<OpenedFile> = Default::default();
        let on_selected : Callbacks<Option<usize>> = Default::default();
        let on_closed : Callbacks<(usize, usize)> = Default::default();
        let mut files : Vec<OpenedFile> = Vec::new();
        let mut selected : Option<usize> = None;
        recv.attach(None, {
            let send = send.clone();
            let (on_open, on_new, on_save, on_selected, on_closed) = (
                on_open.clone(),
                on_new.clone(),
                on_save.clone(),
                on_selected.clone(),
                on_closed.clone()
            );
            move |action| {
                match action {
                    ScriptAction::NewRequest => {
                        if files.len() == 16 {
                            return Continue(true);
                        }
                        let n = files.len();
                        let new_file = OpenedFile { path : None, name : format!("Untitled {}.sql", files.len() + 1), saved : true, content : None };
                        files.push(new_file.clone());
                        println!("{:?}", files);
                        on_new.borrow().iter().for_each(|f| f(new_file.clone()) );
                    },
                    ScriptAction::OpenRequest(path) => {
                        if files.len() == 16 {
                            return Continue(true);
                        }
                        thread::spawn({
                            let send = send.clone();
                            move || {
                                // Open file and write content to OpenedFile.
                                // Send OpenSuccess event
                            }
                        });
                    },
                    ScriptAction::CloseRequest(ix) => {
                        if files[ix].saved {
                            files.remove(ix);
                            let n = files.len();
                            on_closed.borrow().iter().for_each(|f| f((ix, n)) );
                        } else {
                            println!("Cannot close (unsaved changes)");
                        }
                    },
                    ScriptAction::OpenSuccess(file) => {
                        on_open.borrow().iter().for_each(|f| f(file.clone()) );
                    },
                    ScriptAction::Select(opt_ix) => {
                        selected = opt_ix;
                        on_selected.borrow().iter().for_each(|f| f(opt_ix) );
                    },
                    _ => { }
                }
                Continue(true)
            }
        });
        Self { on_open, on_save, on_new, send, on_selected, on_closed }
    }

    pub fn connect_new<F>(&self, f : F)
    where
        F : Fn(OpenedFile) + 'static
    {
        self.on_new.borrow_mut().push(boxed::Box::new(f));
    }

    pub fn connect_selected<F>(&self, f : F)
    where
        F : Fn(Option<usize>) + 'static
    {
        self.on_selected.borrow_mut().push(boxed::Box::new(f));
    }

    pub fn connect_opened<F>(&self, f : F)
    where
        F : Fn(OpenedFile) + 'static
    {
        self.on_open.borrow_mut().push(boxed::Box::new(f));
    }

    pub fn connect_closed<F>(&self, f : F)
    where
        F : Fn((usize, usize)) + 'static
    {
        self.on_closed.borrow_mut().push(boxed::Box::new(f));
    }


}

#[derive(Debug, Clone)]
pub struct OpenedFile {
    pub name : String,
    pub path : Option<String>,
    pub content : Option<String>,
    pub saved : bool
}

pub struct ScriptHistory {

}

impl ScriptHistory {

    pub fn new() -> Self {

        Self { }
    }

}

impl React<SaveDialog> for OpenedScripts {

    fn react(&self, dialog : &SaveDialog) {
        // call on_saved
    }

}

impl React<MainMenu> for OpenedScripts {

    fn react(&self, menu : &MainMenu) {
        let send = self.send.clone();
        menu.action_new.connect_activate(move |_,_| {
            send.send(ScriptAction::NewRequest);
        });
    }

}

impl React<ScriptList> for OpenedScripts {

    fn react(&self, list : &ScriptList) {
        let send = self.send.clone();
        list.new_btn.connect_clicked(move |_| {
            send.send(ScriptAction::NewRequest);
        });
    }
}

impl React<OpenDialog> for OpenedScripts {

    fn react(&self, dialog : &OpenDialog) {
        let send = self.send.clone();
        dialog.dialog.connect_response(move |dialog, resp| {
            match resp {
                ResponseType::Accept => {
                    if let Some(path) = dialog.file().and_then(|f| f.path() ) {
                        send.send(ScriptAction::OpenRequest(path.to_str().unwrap().to_string()));
                    }
                },
                _ => { }
            }
        });
    }

}

impl React<FileList> for OpenedScripts {

    fn react(&self, list : &FileList) {
        list.list.connect_row_selected({
            let send = self.send.clone();
            move |_, opt_row| {
                if let Some(row) = opt_row {
                    send.send(ScriptAction::Select(Some(row.index() as usize)));
                } else {
                    send.send(ScriptAction::Select(None));
                }
            }
        });
        list.close_action.connect_activate({
            let send = self.send.clone();
            move |action, param| {
                let ix = param.unwrap().get::<i32>().unwrap();
                send.send(ScriptAction::CloseRequest(ix as usize));
            }
        });
    }

}


