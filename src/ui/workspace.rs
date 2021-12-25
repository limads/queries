use gtk4::prelude::*;
use gtk4::*;
use crate::client::Environment;
use crate::React;
use libadwaita;
use super::table::*;

#[derive(Debug, Clone)]
pub struct QueriesWorkspace {
    pub tab_view : libadwaita::TabView,
    pub tab_bar : libadwaita::TabBar,
    pub bx : Box
}

impl QueriesWorkspace {

    pub fn build() -> Self {
        let tab_view = libadwaita::TabView::new();
        tab_view.set_halign(Align::Fill);
        tab_view.set_valign(Align::Fill);
        tab_view.set_vexpand(true);
        tab_view.set_hexpand(true);
        let tab_bar = libadwaita::TabBar::new();
        tab_bar.set_valign(Align::End);
        tab_bar.set_vexpand(false);
        tab_bar.set_halign(Align::Fill);
        tab_bar.set_hexpand(true);
        tab_bar.set_view(Some(&tab_view));
        tab_bar.set_autohide(false);
        tab_bar.set_expand_tabs(true);
        tab_bar.set_inverted(false);
        let bx = Box::new(Orientation::Vertical, 0);
        tab_bar.set_margin_bottom(0);
        bx.set_margin_bottom(0);

        bx.append(&tab_view);
        bx.append(&tab_bar);

        Self { tab_view, tab_bar, bx }
    }

}

impl React<Environment> for QueriesWorkspace {

    fn react(&self, env : &Environment) {
        let tab_view = self.tab_view.clone();
        env.connect_table_update(move |tables| {

            while let Some(page) = tab_view.nth_page(0) {
                tab_view.close_page(&page);
            }

            for tbl in tables.iter() {
                let tbl_wid = TableWidget::new_from_table(&tbl);
                let tab_page = tab_view.append(&tbl_wid.scroll_window).unwrap();
                tab_page.set_title("My Table");
                tab_page.set_icon(Some(&gio::ThemedIcon::new("queries-symbolic.svg")));
            }
        });
    }

}


