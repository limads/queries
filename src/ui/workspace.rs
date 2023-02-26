/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use gtk4::prelude::*;
use gtk4::*;
use crate::client::Environment;
use stateful::React;
use libadwaita;
use super::table::*;
use crate::tables::table::Table;
use crate::ui::PlotView;
use papyri::render::Panel;
use crate::client::UserState;

#[derive(Debug, Clone)]
pub struct QueriesWorkspace {
    pub tab_view : libadwaita::TabView,
    pub tab_bar : libadwaita::TabBar,
    pub bx : Box
}

fn configure_tab(tab_view : &libadwaita::TabView, tab_bar : &libadwaita::TabBar) {
    tab_view.set_halign(Align::Fill);
    tab_view.set_valign(Align::Fill);
    tab_view.set_vexpand(true);
    tab_view.set_hexpand(true);
    tab_bar.set_valign(Align::End);
    tab_bar.set_vexpand(false);
    tab_bar.set_halign(Align::Fill);
    tab_bar.set_hexpand(true);
    tab_bar.set_view(Some(&tab_view));
    tab_bar.set_autohide(false);
    tab_bar.set_expand_tabs(true);
    tab_bar.set_inverted(false);
}

impl QueriesWorkspace {

    pub fn build() -> Self {
        let tab_view = libadwaita::TabView::new();
        let tab_bar = libadwaita::TabBar::new();
        configure_tab(&tab_view, &tab_bar);
        let bx = Box::new(Orientation::Vertical, 0);
        tab_bar.set_margin_bottom(0);
        bx.set_margin_bottom(0);
        bx.append(&tab_view);
        bx.append(&tab_bar);
        Self { tab_view, tab_bar, bx }
    }

}

pub fn close_all_pages(tab_view : &libadwaita::TabView) {
    while tab_view.n_pages() > 0 {
        let page = tab_view.nth_page(0);
        tab_view.close_page(&page);
    }
}

const COLUMN_LIMIT : usize = 50;

pub fn populate_with_tables(
    tab_view : &libadwaita::TabView,
    tables : &[Table],
    state : &UserState,
) -> Vec<libadwaita::TabPage> {
    close_all_pages(&tab_view);
    let mut new_pages = Vec::new();
    for tbl in tables.iter() {
        if let Some(val) = tbl.single_json_field() {
            match Panel::new_from_json(&val.to_string()) {
                Ok(panel) => {
                    let view = PlotView::new_from_panel(panel.clone());
                    let tab_page = tab_view.append(&view.parent);
                    configure_plot_page(&tab_page, &panel);
                    new_pages.push(tab_page);
                    continue;
                },
                _ => { }
            }
        }
        let tbl_wid = TableWidget::new_from_table(&tbl, state.execution.row_limit as usize, COLUMN_LIMIT);
        let tab_page = tab_view.append(&tbl_wid.bx);
        new_pages.push(tab_page.clone());
        configure_table_page(&tab_page, &tbl, state.execution.row_limit as usize);
    }
    new_pages
}

impl<'a> React<Environment> for QueriesWorkspace {

    fn react(&self, env : &Environment) {
        let tab_view = self.tab_view.clone();
        let user_state = env.user_state.clone();
        env.connect_table_update(move |tables| {
            let user_state = user_state.borrow();
            let past_sel_page = tab_view.selected_page().map(|page| tab_view.page_position(&page) as usize );
            let past_n_pages = tab_view.n_pages() as usize;
            let new_pages = populate_with_tables(&tab_view, &tables[..], &*user_state);
            if let Some(page_ix) = past_sel_page {
                if new_pages.len() == past_n_pages {
                    tab_view.set_selected_page(&new_pages[page_ix]);
                }
            }
        });
    }

}

fn configure_plot_page(tab_page : &libadwaita::TabPage, _panel : &Panel) {
    tab_page.set_icon(Some(&gio::ThemedIcon::new("roll-symbolic")));
    tab_page.set_title("Plot");
}

fn configure_table_page(tab_page : &libadwaita::TabPage, table : &Table, row_limit : usize) {
    let source = table.source();
    let (icon, mut title) = match (source.name, source.relation) {
        (Some(name), Some(rel)) => (format!("{}", rel), name.to_string()),
        (Some(name), None) => (format!("table-symbolic"), name.to_string()),
        _ => (format!("table-symbolic"), format!("Unknown"))
    };
    let (nrows, ncols) = table.shape();
    if nrows <= row_limit {
        title += &format!(" ({} x {})", nrows, ncols);
    } else {
        title += &format!(" ({}/{} x {})", row_limit, nrows, ncols);
    }
    tab_page.set_title(&title);
    tab_page.set_icon(Some(&gio::ThemedIcon::new(&icon)));
}

