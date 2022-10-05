/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use gtk4::prelude::*;
use gtk4::*;
use super::menu::MainMenu;
use libadwaita::SplitButton;
use super::FileList;
use stateful::React;
use crate::client::OpenedScripts;
use super::QueriesContent;
use crate::client::ActiveConnection;
use archiver::MultiArchiverImpl;

#[derive(Debug, Clone)]
pub struct QueriesTitlebar {
    pub header : HeaderBar,
    // pub editor_toggle : ToggleButton,
    // pub tbl_toggle : ToggleButton,
    pub menu_button : MenuButton,
    pub exec_btn : ExecButton,
    pub sidebar_toggle : ToggleButton,
    pub main_menu : MainMenu,
    pub sidebar_hide_action : gio::SimpleAction
}

impl QueriesTitlebar {

    pub fn build() -> Self {
        let header = HeaderBar::new();
        // let header_bx = Box::new(Orientation::Horizontal, 0);
        // header_bx.style_context().add_class("linked");
        // let tbl_toggle = ToggleButton::new();
        // tbl_toggle.set_width_request(100);
        // tbl_toggle.set_icon_name("queries-symbolic");
        // let editor_toggle = ToggleButton::new();
        // editor_toggle.set_width_request(100);
        // editor_toggle.set_icon_name("accessories-text-editor-symbolic");
        // header_bx.append(&tbl_toggle);
        // header_bx.append(&editor_toggle);
        // header.set_title_widget(Some(&header_bx));

        let left_bx = Box::new(Orientation::Horizontal, 0);

        // use dock-left-symbolic when stable
        let sidebar_toggle = ToggleButton::builder().icon_name("sidebar-symbolic").active(false).build();

        sidebar_toggle.set_active(true);
        let exec_btn = ExecButton::build();
        left_bx.append(&sidebar_toggle);
        left_bx.append(&exec_btn.btn);
        header.pack_start(&left_bx);

        // let menu_toggle = ToggleButton::builder().icon_name("open-menu-symbolic").active(true).build();
        let menu_button = MenuButton::builder().icon_name("open-menu-symbolic").build();
        header.pack_end(&menu_button);

        // let main_menu = Menu::build_with(["New", "Open", "Save", "Settings"]);
        // super::show_popover_on_toggle(&main_menu.popover, &menu_toggle, Vec::new());

        let main_menu = MainMenu::build();
        menu_button.set_popover(Some(&main_menu.popover));
        let sidebar_hide_action = gio::SimpleAction::new_stateful("sidebar_hide", None, &(0).to_variant());

        /*clear_action.connect_activate({
            let
            move || {

            }
        });*/

        Self { header, /*tbl_toggle, editor_toggle,*/ menu_button, exec_btn, sidebar_toggle, main_menu, sidebar_hide_action }
    }

}

#[derive(Debug, Clone)]
pub struct ExecButton {
    pub btn : SplitButton,

    // ExecAction carries the index of the opened SQL file as its integer parameter.
    // It carries the content of the SQL file as its state.
    pub exec_action : gio::SimpleAction,

    // This closes all queried tables. The table tabs can be restored with the restore action.
    pub clear_action : gio::SimpleAction,

    // Queries caches all results of the last query sequence, irrespective of whether
    // the user closed the windows. The "restore" action will reset the workspace to
    // the last query sequence, using the tables cached at the environment.
    pub restore_action : gio::SimpleAction,

    // Sets the query button to "schedule" mode, for which the sequence of SQL
    // statement is executed repeatedly every n seconds.
    pub schedule_action : gio::SimpleAction,

    pub single_action : gio::SimpleAction,

}

impl ExecButton {

    fn set_active(&self, active : bool) {
        self.btn.set_sensitive(active);
        self.exec_action.set_enabled(active);
        self.clear_action.set_enabled(active);
        self.restore_action.set_enabled(active);
        self.schedule_action.set_enabled(active);
        self.single_action.set_enabled(active);
    }
    
    fn build() -> Self {
        let exec_menu = gio::Menu::new();

        let exec_section = gio::Menu::new();
        exec_section.append(Some("Immediate"), Some("win.single"));
        exec_section.append(Some("Scheduled"), Some("win.schedule"));
        exec_menu.append_section(Some("Execution mode"), &exec_section);

        let workspace_section = gio::Menu::new();
        workspace_section.append(Some("Restore"), Some("win.restore"));
        workspace_section.append(Some("Clear"), Some("win.clear"));
        exec_menu.append_section(Some("Workspace"), &workspace_section);

        let btn = SplitButton::builder().icon_name("download-db-symbolic").menu_model(&exec_menu).sensitive(false).build();
        let exec_action = gio::SimpleAction::new_stateful("execute", Some(&String::static_variant_type()), &(-1i32).to_variant());
        let clear_action = gio::SimpleAction::new("clear", None);
        let restore_action = gio::SimpleAction::new("restore", None);
        exec_action.set_enabled(false);
        clear_action.set_enabled(false);
        restore_action.set_enabled(false);

        btn.set_sensitive(false);
        let schedule_action = gio::SimpleAction::new_stateful("schedule", None, &(false).to_variant());
        let single_action = gio::SimpleAction::new_stateful("single", None, &(true).to_variant());

        single_action.connect_activate({
            let schedule_action = schedule_action.clone();
            move |action, _| {
                action.set_state(&true.to_variant());
                schedule_action.set_state(&false.to_variant());
            }
        });

        schedule_action.connect_activate({
            let single_action = single_action.clone();
            move |action, _| {
                action.set_state(&true.to_variant());
                single_action.set_state(&false.to_variant());
            }
        });

        // single_action.set_enabled(true);
        // schedule_action.
        // btn.activate_action(&exec_action, None);
        Self { btn, exec_action, clear_action, restore_action, schedule_action, single_action }
    }

}

impl React<FileList> for ExecButton {

    fn react(&self, file_list : &FileList) {
        let btn = self.btn.clone();
        let exec_action = self.exec_action.clone();

        // What happens if the user selects a file (depending on exec_action state)
        file_list.list.connect_row_selected(move |_, opt_row| {
            if opt_row.is_some() && exec_action.is_enabled() {
                btn.set_sensitive(true);
            } else {
                btn.set_sensitive(false);
            }
        });

        // What happens if the user disconnects, then connects, but a SQL file remains selected.
        self.exec_action.connect_enabled_notify({
            let btn = self.btn.clone();
            let list = file_list.list.clone();
            move|action| {
                if action.is_enabled() {
                    if list.selected_row().is_some() {
                        btn.set_sensitive(true);
                    } else {
                        btn.set_sensitive(false);
                    }
                } else {
                    btn.set_sensitive(false);
                }
            }
        });
    }

}

impl React<OpenedScripts> for ExecButton {

    fn react(&self, scripts : &OpenedScripts) {
        let action = self.exec_action.clone();

        // Sets the index of the SQL file to be executed.
        scripts.connect_selected(move |opt_file| {
            if let Some(ix) = opt_file.map(|f| f.index ) {
                action.set_state(&(ix as i32).to_variant());
            } else {
                action.set_state(&(-1i32).to_variant());
            }
        });
    }

}

impl React<ActiveConnection> for ExecButton {

    fn react(&self, conn : &ActiveConnection) {
        conn.connect_db_connected({
            let exec_action = self.exec_action.clone();
            let clear_action = self.clear_action.clone();
            let restore_action = self.restore_action.clone();
            move |_| {
                exec_action.set_enabled(true);
                clear_action.set_enabled(true);
                restore_action.set_enabled(true);
            }
        });
        conn.connect_db_disconnected({
            let exec_action = self.exec_action.clone();
            let exec_btn = self.btn.clone();
            let clear_action = self.clear_action.clone();
            let restore_action = self.restore_action.clone();
            move |_| {
                exec_action.set_enabled(false);
                exec_btn.set_sensitive(false);
                clear_action.set_enabled(false);
                restore_action.set_enabled(false);
            }
        });
       
       // TODO only let actions be valid when the schema is updated.
       /* let mut is_valid : Rc<RefCell<bool>> = Default::default();
        conn.connect_schema_invalidated({
            let exec_btn = self.clone();
            move |_| {
                exec_btn.set_active(false);
                is_valid.replace(false);
            }
        });
        conn.connect_schema_update({
            let exec_btn = self.clone();
            move |_| {
                
                exec_btn.set_active(true);
            }
        });*/
        
        /*conn.connect_db_error({
            let clear_action = self.clear_action.clone();
            let restore_action = self.restore_action.clone();
            move |_| {
                clear_action.
            }
        });*/
    }

}

impl React<QueriesContent> for ExecButton {

    fn react(&self, content : &QueriesContent) {
        let exec_btn = self.btn.clone();
        let exec_action = self.exec_action.clone();
        content.stack.connect_visible_child_notify(move |stack| {
            if let Some(name) = stack.visible_child_name() {
                match name.as_str() {
                    "editor" => {
                        // i.e. there is a file selected
                        let any_file_selected = exec_action.state().unwrap().get::<i32>().unwrap() >= 0;
                        if exec_action.is_enabled() && any_file_selected {
                            exec_btn.set_sensitive(true);
                        } else {
                            exec_btn.set_sensitive(false);
                        }
                    },
                    "results" => {
                        // We can't set it to insensitive here, or else the user
                        // won't be able to cancel scheduled queries.
                        // exec_btn.set_sensitive(false);
                    },
                    _ => { }
                }
            }
        });
    }

}

/*impl React<QueriesContent> for ExecButton {
    fn react(&self, content : &QueriesContent) {
        let actions = [self.exec_action.clone(), self.schedule_action.clone()];
        content.stack.connect_visible_child_notify(move |stack| {
            if let Some(name) = stack.visible_child_name() {
                match name.as_str() {
                    "editor" => {
                        actions.iter().for_each(|action| action.set_enabled(true) );
                    },
                    "results" => {
                        actions.iter().for_each(|action| action.set_enabled(false) );
                    },
                    _ => { }
                }
            }
        });
    }
}*/

/*impl React<QueriesEditor> for ExecButton {

    fn react(&self, editor : &QueriesEditor) {

    }

}*/



