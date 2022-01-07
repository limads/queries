use gtk4::prelude::*;
use gtk4::*;
use crate::client::Environment;
use crate::React;
use libadwaita;
use super::table::*;
use crate::tables::table::Table;
use crate::ui::PlotView;
use plots::Panel;

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

            while tab_view.n_pages() > 0 {
                if let Some(page) = tab_view.nth_page(0) {
                    tab_view.close_page(&page);
                }
            }

            for tbl in tables.iter() {

                if let Some(val) = tbl.single_json_field() {
                    match Panel::new_from_json(&val.to_string()) {
                        Ok(panel) => {
                            let view = PlotView::new_from_panel(panel.clone());
                            let tab_page = tab_view.append(&view.parent).unwrap();
                            configure_plot_page(&tab_page, &panel);
                            continue;
                        },
                        _ => { }
                    }
                }

                let tbl_wid = TableWidget::new_from_table(&tbl);
                let tab_page = tab_view.append(&tbl_wid.scroll_window).unwrap();
                configure_table_page(&tab_page, &tbl);
            }
        });
    }

}

fn configure_plot_page(tab_page : &libadwaita::TabPage, panel : &Panel) {
    tab_page.set_icon(Some(&gio::ThemedIcon::new("folder-templates-symbolic")));
    tab_page.set_title("Plot");
}

fn configure_table_page(tab_page : &libadwaita::TabPage, table : &Table) {
    let source = table.source();
    let (icon, mut title) = match (source.name, source.relation) {
        (Some(name), Some(rel)) => (format!("{}", rel), name.to_string()),
        (Some(name), None) => (format!("queries"), name.to_string()),
        _ => (format!("queries"), format!("Unknown"))
    };
    let (nrows, ncols) = table.shape();
    title += &format!(" ({} x {})", nrows, ncols);
    tab_page.set_title(&title);
    tab_page.set_icon(Some(&gio::ThemedIcon::new(&icon)));
}

/*
for table in all_tbls.iter() {
    let info = table.table_info();
    if let Some(val) = table.single_json_field() {
        let plot_created = tables_nb.create_json_plot_rep(
            val,
            table_bar.clone(),
            workspace.layout_window.clone()
        );
        if !plot_created {
            tables_nb.create_data_table(
                TableSource::Database(info.0, info.1),
                table.text_rows(),
                workspace.clone(),
                table_bar.clone()
            );
        }
    } else {
        tables_nb.create_data_table(
            TableSource::Database(info.0, info.1),
            table.text_rows(),
            workspace.clone(),
            table_bar.clone()
        );
    }
}*/

/*pub fn create_json_plot_rep(&self, val : Value, bar : TableBar, layout_window : LayoutWindow) -> bool {
    match PlotView::new_from_json(&val.to_string()) {
        Ok(view) => {
            let vp = Viewport::new(None::<&Adjustment>, None::<&Adjustment>);
            vp.override_background_color(StateFlags::NORMAL, Some(&RGBA::from_str("#fafafa").unwrap()));
            vp.set_shadow_type(ShadowType::None);
            vp.add(&view.parent);
            // let bx = Box::new(Orientation::Horizontal, 0);
            // bx.pack_start(&view.parent, true, true, 0);
            // bx.show_all();

            // self.nb.add(&bx);
            self.nb.add(&vp);
            self.nb.next_page();
            view.redraw();
            println!("Plot added");
            // } else {
            //    println!("Unable to borrow view");
            //    return;
            // }

            self.nb.show_all();
            self.sources.borrow_mut().push(TableSource::Plot);
            crate::plots::plot_view::connect_draw_to_set(&view, Rc::downgrade(&self.plots));
            //if let Some(mut plots) = self.plots.upgrade() {
            self.plots.borrow_mut().push(view.clone());
            // } else {
            //    println!("Unable to get mutable reference to table vector");
            // }
            let img = Image::from_icon_name(Some("folder-templates-symbolic"), IconSize::SmallToolbar);
            let ev_bx = create_sheet_tab(&img, &Label::new(Some("Plot")), &TableSource::Plot, &bar, None, Some(layout_window.clone()));
            self.nb.set_tab_label(&vp, Some(&ev_bx));
            true
        },
        Err(e) => {
            println!("{}", e );
            false
        }
    }
}*/

