use gtk4::prelude::*;
use gtk4::*;

#[derive(Debug, Clone)]
pub struct QueriesTitlebar {
    pub header : HeaderBar,
    pub editor_toggle : ToggleButton,
    pub tbl_toggle : ToggleButton,
    pub menu_toggle : ToggleButton,
    pub exec_toggle : ToggleButton,
    pub sidebar_toggle : ToggleButton
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
        header.set_title_widget(Some(&header_bx));

        let left_bx = Box::new(Orientation::Horizontal, 0);
        let sidebar_toggle = ToggleButton::builder().icon_name("sidebar-symbolic").active(false).build();

        let exec_toggle = ToggleButton::builder().icon_name("download-db-symbolic").active(false).sensitive(false).build();
        left_bx.append(&sidebar_toggle);
        left_bx.append(&exec_toggle);
        header.pack_start(&left_bx);

        let menu_toggle = ToggleButton::builder().icon_name("open-menu-symbolic").active(true).build();
        header.pack_end(&menu_toggle);

        Self { header, tbl_toggle, editor_toggle, menu_toggle, exec_toggle, sidebar_toggle }
    }

}

