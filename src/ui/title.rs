use gtk4::prelude::*;
use gtk4::*;
use super::menu::MainMenu;
use libadwaita::SplitButton;

#[derive(Debug, Clone)]
pub struct QueriesTitlebar {
    pub header : HeaderBar,
    pub editor_toggle : ToggleButton,
    pub tbl_toggle : ToggleButton,
    pub menu_button : MenuButton,
    pub exec_btn : SplitButton,
    pub sidebar_toggle : ToggleButton,
    pub main_menu : MainMenu
}

impl QueriesTitlebar {

    pub fn build() -> Self {
        let header = HeaderBar::new();
        let header_bx = Box::new(Orientation::Horizontal, 0);
        header_bx.style_context().add_class("linked");
        let tbl_toggle = ToggleButton::new();
        tbl_toggle.set_width_request(100);
        tbl_toggle.set_icon_name("queries-symbolic");
        let editor_toggle = ToggleButton::new();
        editor_toggle.set_width_request(100);
        editor_toggle.set_icon_name("accessories-text-editor-symbolic");
        header_bx.append(&tbl_toggle);
        header_bx.append(&editor_toggle);
        // header.set_title_widget(Some(&header_bx));

        let left_bx = Box::new(Orientation::Horizontal, 0);
        let sidebar_toggle = ToggleButton::builder().icon_name("sidebar-symbolic").active(false).build();

        let exec_menu = gio::Menu::new();
        exec_menu.append(Some("Clear"), Some("win.new_file"));
        exec_menu.append(Some("Schedule"), Some("win.open_file"));

        let exec_btn = SplitButton::builder().icon_name("download-db-symbolic").menu_model(&exec_menu) /*.sensitive(false).*/ .build();
        left_bx.append(&sidebar_toggle);
        left_bx.append(&exec_btn);
        header.pack_start(&left_bx);

        //let menu_toggle = ToggleButton::builder().icon_name("open-menu-symbolic").active(true).build();
        let menu_button = MenuButton::builder().icon_name("open-menu-symbolic").build();
        header.pack_end(&menu_button);

        // let main_menu = Menu::build_with(["New", "Open", "Save", "Settings"]);
        // super::show_popover_on_toggle(&main_menu.popover, &menu_toggle, Vec::new());

        let main_menu = MainMenu::build();
        menu_button.set_popover(Some(&main_menu.popover));

        Self { header, tbl_toggle, editor_toggle, menu_button, exec_btn, sidebar_toggle, main_menu }
    }

}

