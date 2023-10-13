/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use stateful::{React, Inherit};

use gtk4::prelude::*;
use gtk4::*;
use crate::ui::{QueriesEditor, ScriptList, SaveDialog, OpenDialog};
use crate::ui::QueriesWindow;
use crate::ui::PackedImageLabel;
use crate::ui::MainMenu;
use crate::ui::FileList;

use filecase::{MultiArchiver, MultiArchiverImpl, MultiArchiverAction};

// TODO At startup, remove script from scriptlist if its path does not exist anymore.

pub struct OpenedScripts(MultiArchiver);

impl OpenedScripts  {

    pub fn new() -> Self {
        OpenedScripts(MultiArchiver::new(String::from("sql")))
    }

    pub fn send(&self, action : MultiArchiverAction) {
        if let Err(e) = self.0.sender().send(action) {
            eprintln!("{}", e);
        }
    }

}

impl MultiArchiverImpl for OpenedScripts { }

impl AsRef<MultiArchiver> for OpenedScripts {

    fn as_ref(&self) -> &MultiArchiver {
        &self.0
    }

}

impl Inherit for OpenedScripts {

    type Parent = MultiArchiver;
    
    fn parent<'a>(&'a self) -> &'a Self::Parent {
        &self.0
    }
    
    fn parent_mut<'a>(&'a mut self) -> &'a mut Self::Parent {
        &mut self.0
    }
    
}

impl React<SaveDialog> for OpenedScripts {

    fn react(&self, dialog : &SaveDialog) {
        let send = self.sender().clone();
        dialog.0.dialog.connect_response(move |dialog, resp| {
            match resp {
                ResponseType::Accept => {
                    if let Some(path) = dialog.file().and_then(|f| f.path() ) {
                        send.send(MultiArchiverAction::SaveRequest(Some(path.to_str().unwrap().to_string()))).unwrap();
                    }
                },
                _ => { }
            }
        });
    }

}

impl React<MainMenu> for OpenedScripts {

    fn react(&self, menu : &MainMenu) {

        menu.action_new.connect_activate({
            let send = self.sender().clone();
            move |_,_| {
                send.send(MultiArchiverAction::NewRequest).unwrap();
            }
        });
        menu.action_save.connect_activate({
            let send = self.sender().clone();
            move |_,_| {
                send.send(MultiArchiverAction::SaveRequest(None)).unwrap();
            }
        });
    }

}

impl React<OpenDialog> for OpenedScripts {

    fn react(&self, dialog : &OpenDialog) {
        let send = self.sender().clone();
        dialog.0.dialog.connect_response(move |dialog, resp| {
            match resp {
                ResponseType::Accept => {
                    if let Some(path) = dialog.file().and_then(|f| f.path() ) {
                        send.send(MultiArchiverAction::OpenRequest(path.to_str().unwrap().to_string())).unwrap();
                    }
                },
                _ => { }
            }
        });
    }

}

impl React<ScriptList> for OpenedScripts {

    fn react(&self, list : &ScriptList) {
        let send = self.sender().clone();
        list.new_btn.connect_clicked(move |_| {
            send.send(MultiArchiverAction::NewRequest).unwrap();
        });
    }
}

impl React<FileList> for OpenedScripts {

    fn react(&self, list : &FileList) {
        list.list.connect_row_selected({
            let send = self.sender().clone();
            move |_, opt_row| {
                if let Some(row) = opt_row {
                    send.send(MultiArchiverAction::Select(Some(row.index() as usize))).unwrap();
                } else {
                    send.send(MultiArchiverAction::Select(None)).unwrap();
                }
            }
        });
        list.close_action.connect_activate({
            let send = self.sender().clone();
            move |_action, param| {
                let ix = param.unwrap().get::<i32>().unwrap();
                send.send(MultiArchiverAction::CloseRequest(ix as usize, false)).unwrap();
            }
        });
    }

}

impl React<QueriesEditor> for OpenedScripts {

    fn react(&self, editor : &QueriesEditor) {
        editor.views.iter().for_each(|view| {
            let send = self.sender().clone();
            let view = view.clone();
            let stack = editor.stack.clone();
            view.buffer().connect_end_user_action(move |_| {
                if let Some(sel_ix) = crate::ui::selected_editor_stack_index(&stack) {
                    send.send(MultiArchiverAction::SetSaved(sel_ix, false)).unwrap();
                }
            });
        });
        editor.ignore_file_save_action.connect_activate({
            let send = self.sender().clone();
            move |_action, param| {
                if let Some(variant) = param {
                    let ix = variant.get::<i32>().unwrap();
                    if ix >= 0 {
                        send.send(MultiArchiverAction::CloseRequest(ix as usize, true)).unwrap();
                    } else {
                        eprintln!("Ix is nonzero");
                    }
                } else {
                    eprintln!("Action does not have parameter");
                }
            }
        });
        editor.script_list.list.connect_row_activated({
            let send = self.sender().clone();
            move |_list, row| {
                let child = row.child().unwrap().downcast::<Box>().unwrap();
                let lbl = PackedImageLabel::extract(&child).unwrap();
                let path = lbl.lbl.text().as_str().to_string();
                send.send(MultiArchiverAction::OpenRequest(path)).unwrap();
            }
        });
    }

}

impl React<QueriesWindow> for OpenedScripts {

    fn react(&self, win : &QueriesWindow) {

        self.react(&win.content.editor.save_dialog);
        self.react(&win.content.editor.open_dialog);
        self.react(&win.titlebar.main_menu);
        self.react(&win.content.editor.script_list);
        self.react(&win.sidebar.file_list);
        self.react(&win.content.editor);

        let send = self.sender().clone();

        // When the usesr attempts to close the window, we inhibit the action so
        // that MultiArchiver can verify if there are any unsaved files. The window
        // will actually be closed on impl React<MultiArchiver> for QueriesWindow.
        win.window.connect_close_request(move |_win| {
            send.send(MultiArchiverAction::WindowCloseRequest).unwrap();
            glib::signal::Propagation::Stop
        });
    }
}



