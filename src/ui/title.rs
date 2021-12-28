use gtk4::prelude::*;
use gtk4::*;
use super::menu::MainMenu;
use libadwaita::SplitButton;
use super::FileList;
use crate::React;
use crate::client::OpenedScripts;
use super::QueriesContent;

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

        Self { header, /*tbl_toggle, editor_toggle,*/ menu_button, exec_btn, sidebar_toggle, main_menu, sidebar_hide_action }
    }

}

#[derive(Debug, Clone)]
pub struct ExecButton {
    pub btn : SplitButton,

    // ExecAction carries the index of the opened SQL file as its integer parameter.
    // It carries the content of the SQL file as its state.
    pub exec_action : gio::SimpleAction,
    pub clear_action : gio::SimpleAction,
    pub schedule_action : gio::SimpleAction
}

impl ExecButton {

    fn build() -> Self {
        let exec_menu = gio::Menu::new();
        exec_menu.append(Some("Clear"), Some("win.clear"));
        exec_menu.append(Some("Schedule"), Some("win.schedule"));
        let btn = SplitButton::builder().icon_name("download-db-symbolic").menu_model(&exec_menu).sensitive(false).build();
        let exec_action = gio::SimpleAction::new_stateful("execute", Some(&String::static_variant_type()), &(-1i32).to_variant());
        let clear_action = gio::SimpleAction::new("clear", None);
        let schedule_action = gio::SimpleAction::new("schedule", None);
        // btn.activate_action(&exec_action, None);
        Self { btn, exec_action, clear_action, schedule_action }
    }
}

impl React<FileList> for ExecButton {

    fn react(&self, file_list : &FileList) {
        let btn = self.btn.clone();
        file_list.list.connect_row_selected(move |_, opt_row| {
            if opt_row.is_some() {
                btn.set_sensitive(true);
            } else {
                btn.set_sensitive(false);
            }
        });
    }

}

impl React<OpenedScripts> for ExecButton {

    fn react(&self, scripts : &OpenedScripts) {
        let action = self.exec_action.clone();
        scripts.connect_selected(move |opt_file| {
            if let Some(ix) = opt_file.map(|f| f.index ) {
                action.set_state(&(ix as i32).to_variant());
            } else {
                action.set_state(&(-1i32).to_variant());
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



