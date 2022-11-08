/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use gtk4::*;
use gtk4::prelude::*;
use crate::tables::table::Table;
use std::iter::ExactSizeIterator;
use gtk4::gdk::Cursor;
use std::rc::Rc;

#[derive(Debug, Clone)]
pub struct TablePopover {
    pub popover : Popover,
    pub fst_scale : Scale,
    pub num_scale : Scale,
    pub filter_entry : Entry,
    pub btn_ascending : ToggleButton,
    pub btn_descending : ToggleButton
}

impl TablePopover {

    pub fn new() -> Self {
        let fst_scale = Scale::new(Orientation::Horizontal, Some(&Adjustment::new(1., 1., 50., 1.0, 1.0, 10.0)));
        fst_scale.set_value_pos(PositionType::Right);
        fst_scale.set_digits(0);
        fst_scale.set_draw_value(true);
        fst_scale.set_hexpand(true);
        fst_scale.set_halign(Align::Fill);

        let top_bx = Box::new(Orientation::Horizontal, 0);
        let middle_bx = Box::new(Orientation::Horizontal, 0);
        let bottom_bx = Box::new(Orientation::Horizontal, 0);
        top_bx.style_context().add_class("linked");
        // let btn_filter = ToggleButton::builder().icon_name("funnel-symbolic").build();
        let btn_ascending = ToggleButton::builder().icon_name("view-sort-ascending-symbolic").build();
        let btn_descending = ToggleButton::builder().icon_name("view-sort-descending-symbolic").build();
        btn_ascending.style_context().add_class("flat");
        btn_descending.style_context().add_class("flat");

        let filter_entry = Entry::new();
        filter_entry.set_primary_icon_name(Some("funnel-symbolic"));

        top_bx.append(&btn_descending);
        top_bx.append(&btn_ascending);
        top_bx.append(&filter_entry);
        btn_ascending.set_group(Some(&btn_descending));

        let line_img = Image::from_icon_name("view-continuous-symbolic");
        middle_bx.append(&line_img);
        middle_bx.append(&fst_scale);

        let num_img = Image::from_icon_name("type-integer-symbolic");
        let num_scale = Scale::new(Orientation::Horizontal, Some(&Adjustment::new(0., 0., 50., 1.0, 1.0, 10.0)));
        num_scale.set_value_pos(PositionType::Right);
        num_scale.set_hexpand(true);
        num_scale.set_draw_value(true);
        num_scale.set_halign(Align::Fill);
        num_scale.set_digits(0);

        bottom_bx.append(&num_img);
        bottom_bx.append(&num_scale);

        let popover = Popover::new();
        let bx = Box::new(Orientation::Vertical, 0);
        bx.append(&top_bx);
        bx.append(&middle_bx);
        bx.append(&bottom_bx);
        popover.set_child(Some(&bx));
        popover.set_position(PositionType::Right);
        Self { popover, btn_ascending, btn_descending, filter_entry, fst_scale, num_scale }
    }

}

#[derive(Clone, Debug)]
pub struct TableWidget {

    pub grid : Grid,

    pub scroll_window : ScrolledWindow,

    _parent_ctx : StyleContext,

    provider : CssProvider,

    popover : TablePopover,

    tbl : Rc<Table>,

    max_nrows : usize

}

const TABLE_WHITE_CSS : &'static str = r#"
.scrolledwindow {
  background-color : #FFFFFF;
}

.table-cell {
  padding-left: 10px;
  padding-right: 10px;
  padding-top: 10px;
  padding-bottom: 10px;
  border : 1px solid #F0F0F0;
  /*border : 1px solid #000000;*/
  background-color : #FFFFFF;
}

.selected {
  background-color : #F5F6F7;
  border : 1px solid #E9E9E9;
}

.first-row {
  /*background-color : #E9E9E9;*/
  font-weight : bold;
  border-bottom : 1px solid #F0F0F0;
}
"#;

const TABLE_DARK_CSS : &'static str = r#"
.scrolledwindow {
  background-color : #1E1E1E;
}

.table-cell {
  padding-left: 10px;
  padding-right: 10px;
  padding-top: 10px;
  padding-bottom: 10px;
  border : 1px solid #454545;
  /*border : 1px solid #000000;*/
  background-color : #1E1E1E;
}

.selected {
  background-color : #F5F6F7;
  border : 1px solid #454545;
}

.first-row {
  /*background-color : #E9E9E9;*/
  font-weight : bold;
  border-bottom : 1px solid #454545;
}
"#;

impl TableWidget {

    pub fn new_from_table(tbl : &Table, max_nrows : usize, max_ncols : usize) -> Self {
        let mut tbl_wid = Self::new();
        tbl_wid.max_nrows = max_nrows;
        tbl_wid.tbl = Rc::new(tbl.clone());
        let data = tbl.text_rows(Some(max_nrows), Some(max_ncols), true, 0);
        tbl_wid.update_data(data, true);
        tbl_wid
    }

    pub fn new() -> TableWidget {
        let grid = Grid::new();
        // let _message = Label::new(None);
        let provider = CssProvider::new();
        
        if libadwaita::StyleManager::default().is_dark() {
            provider.load_from_data(TABLE_DARK_CSS.as_bytes());
        } else {
            provider.load_from_data(TABLE_WHITE_CSS.as_bytes());
        }
        
        let parent_ctx = grid.style_context();
        parent_ctx.add_provider(&provider,800);

        // let msg = Label::new(None);
        // let box_container = Box::new(Orientation::Vertical, 0);
        // box_container.append(&grid, true, true, 0);
        // box_container.pack_start(&msg, true, true, 0);
        let scroll_window = ScrolledWindow::new();
        scroll_window.set_vexpand(true);
        scroll_window.set_valign(Align::Fill);

        // Some(&Adjustment::new(0.0, 0.0, 100.0, 10.0, 10.0, 100.0)),
        //    Some(&Adjustment::new(0.0, 0.0, 100.0, 10.0, 10.0, 100.0))
        // );
        /*let scroll_window = ScrolledWindow::new(
            Some(&Adjustment::new(0.0, 0.0, 100.0, 0.0, 0.0, 100.0)),
            Some(&Adjustment::new(0.0, 0.0, 100.0, 0.0, 0.0, 100.0))
        );*/
        // scroll_window.set_shadow_type(ShadowType::None);

        scroll_window.set_child(Some(&grid));

        let popover = TablePopover::new();
        popover.popover.set_parent(&scroll_window);

        // scroll_window.show_all();
        // let selected = Rc::new(RefCell::new(Vec::new()));
        // let dims = Rc::new(RefCell::new((0, 0)));
        // let tbl = Table::new_empty(None);
        TableWidget {
            grid,
            max_nrows : 500,
            scroll_window,
            _parent_ctx : parent_ctx,
            provider,
            popover,
            tbl : Rc::new(Table::empty(Vec::new()))
        }
    }

    pub fn parent(&self) -> ScrolledWindow {
        self.scroll_window.clone()
    }

    /*fn create_header_cell(
        &self,
        data : &str,
        row : usize,
        col : usize,
        nrows : usize,
        ncols : usize
    ) -> gtk::EventBox {
        let label = self.create_data_cell(data, row, col, nrows, ncols);
        let ev_box = gtk::EventBox::new();
        //ev_box.set_above_child(true);
        //ev_box.set_visible_window(true);
        ev_box.add(&label);
        if let Ok(mut sel) = self.selected.try_borrow_mut() {
            sel.push((data.to_string(), col, false));
        }
        ev_box
    }*/

    fn create_data_cell(
        &self,
        data : &str,
        row : usize,
        col : usize,
        nrows : usize,
        ncols : usize,
        include_header : bool
    ) -> Label {

        // No allocation happens here
        let mut trimmed_data = String::new();

        let label_data : &str = if data.len() <= 140 {
            &data[..]
        } else {
            // Only if content is too big, allocate it.
            trimmed_data += &data[0..140];
            trimmed_data += "...";
            &trimmed_data[..]
        };
        let label = Label::new(None);
        label.set_use_markup(true);
        label.set_markup(label_data);
        label.set_hexpand(true);
        let ctx = label.style_context();

        /*label.connect_activate_link(move |_, uri| {
            if uri.starts_with("queries://") {
                let split = uri.trim_start_matches("queries://").split("+");
                if let Some(fn_name) = split.next() {
                    let mut params = Vec::new();
                    while let Some(param) = split.next() {
                        params.push(param.to_string());
                    }
                }
                Inhibit(true)
            } else {
                Inhibit(false)
            }
        });*/

        ctx.add_provider(&(self.provider),800); // PROVIDER_CONTEXT_USER
        ctx.add_class("table-cell");

        // Add this only when all columns have a title, maybe on a set_header method.
        if row == 0 {
            ctx.add_class("first-row");
        }
        if row == nrows - 1 {
            ctx.add_class("last-row");
        }
        if col % 2 != 0 {
            ctx.add_class("odd-col");
        } else {
            ctx.add_class("even-col");
        }
        if col == ncols-1 {
            ctx.add_class("last-col");
        }
        if (row + 1) % 2 != 0 {
            ctx.add_class("odd-row");
        }

        if row == 0 && include_header {
            let cursor = Cursor::builder().name("pointer").build();
            label.set_cursor(Some(&cursor));
            let click = GestureClick::new();
            click.set_button(gdk::BUTTON_PRIMARY);
            label.add_controller(&click);
            click.connect_pressed({
                let grid = self.grid.clone();
                let label = label.clone();
                let popover = self.popover.clone();
                let tbl = self.tbl.clone();
                let tbl_wid = self.clone();
                let max_nrows = self.max_nrows.clone();
                move |_gesture, _n_press, _x, _y| {
                    let ctx = label.style_context();
                    let is_selected = ctx.has_class("selected");
                    if !is_selected {
                        // popover.popover.set_parent(&label);
                        popover.popover.set_pointing_to(Some(&label.allocation()));
                        popover.popover.popup();
                        if let Some(sorted_tbl) = tbl.sorted_by(col, true) {
                            // println!("{:?}", sorted_tbl);
                            let new_data = sorted_tbl.text_rows(Some(nrows), Some(ncols), false, 0);
                            tbl_wid.update_data(new_data, false);
                        }
                    }
                    set_selected_style(grid.clone(), col, !is_selected);
                    if !is_selected {
                        for c in 0..ncols {
                            if c != col {
                                set_selected_style(grid.clone(), c, is_selected);
                            }
                        }
                    }
                }
            });

            if col == 0 {
                let max_nrows = self.max_nrows.clone();
                println!("adj max = {}", nrows);
                let fst_adj = Adjustment::new(1., 1., nrows as f64, 1.0, 1.0, 10.0);
                self.popover.fst_scale.set_adjustment(&fst_adj);

                let eff_rows = nrows.min(max_nrows) as f64;
                let num_adj = Adjustment::new(eff_rows, 1., eff_rows, 1.0, 1.0, 10.0);
                self.popover.num_scale.set_adjustment(&num_adj);

                fst_adj.connect_value_changed({
                    let tbl = self.tbl.clone();
                    let tbl_wid = self.clone();
                    let grid = self.grid.clone();
                    let num_scale = self.popover.num_scale.clone();
                    move|adj| {
                        let sel_col = selected_col(&grid, ncols);
                        let fst_row = adj.value() as usize;
                        let num_rows = num_scale.adjustment().value() as usize;
                        if let Some(sorted_tbl) = tbl.sorted_by(sel_col, true) {
                            // let new_data = sorted_tbl.text_rows(Some(nrows), Some(ncols), false, fst_row - 1);
                            let new_data = sorted_tbl.text_rows(Some(num_rows), Some(ncols), false, fst_row - 1);
                            tbl_wid.update_data(new_data, false);
                            set_selected_style(grid.clone(), sel_col, true);
                            // num_scale.adjustment().set_upper(effective - offset);
                        }
                    }
                });

                num_adj.connect_value_changed({
                    let tbl = self.tbl.clone();
                    let tbl_wid = self.clone();
                    let grid = self.grid.clone();
                    let fst_scale = self.popover.fst_scale.clone();
                    move |adj| {
                        let sel_col = selected_col(&grid, ncols);
                        let fst_row = fst_scale.adjustment().value() as usize;
                        let num_rows = adj.value() as usize;
                        if let Some(sorted_tbl) = tbl.sorted_by(sel_col, true) {
                            let new_data = sorted_tbl.text_rows(Some(num_rows), Some(ncols), false, fst_row - 1);
                            tbl_wid.update_data(new_data, false);
                            set_selected_style(grid.clone(), sel_col, true);
                            // fst_scale.adjustment().set_upper(num_rows as f64);
                        }
                    }
                });

                self.popover.filter_entry.connect_changed({
                    let grid = self.grid.clone();
                    let tbl_wid = self.clone();
                    let fst_scale = self.popover.fst_scale.clone();
                    let num_scale = self.popover.num_scale.clone();
                    let tbl = self.tbl.clone();
                    move |entry| {
                        let sel_col = selected_col(&grid, ncols);
                        let txt = entry.buffer().text();
                        let fst_row = fst_scale.adjustment().value() as usize;
                        let num_rows = num_scale.adjustment().value() as usize;
                        if txt.is_empty() {
                            let new_data = tbl.text_rows(Some(num_rows), Some(ncols), false, 0);
                            tbl_wid.update_data(new_data, false);
                            set_selected_style(grid.clone(), sel_col, true);
                            fst_scale.adjustment().set_value(1.0);
                            num_scale.adjustment().set_upper(tbl.nrows().min(max_nrows) as f64);
                            num_scale.adjustment().set_value(f64::MAX);
                        } else {
                            if let Some(filtered_tbl) = tbl.filtered_by(sel_col, &txt) {
                                let new_data = filtered_tbl.text_rows(Some(num_rows), Some(ncols), false, 0);
                                tbl_wid.update_data(new_data, false);
                                set_selected_style(grid.clone(), sel_col, true);
                                fst_scale.adjustment().set_value(1.0);
                                num_scale.adjustment().set_upper(filtered_tbl.nrows().min(max_nrows) as f64);
                                num_scale.adjustment().set_value(f64::MAX);
                            }
                        }
                    }
                });
            }

            self.popover.popover.connect_closed({
                let label = label.clone();
                let grid = self.grid.clone();
                move |_| {
                    let ctx = label.style_context();
                    let is_selected = ctx.has_class("selected");
                    if is_selected {
                        set_selected_style(grid.clone(), col, false);
                    }
                }
            });
        }

        label
    }

    /*/// Returns selected columns, as a continuous index from the first
    /// column of the current table
    pub fn selected_cols(&self) -> Vec<usize> {
        if let Ok(sel) = self.selected.try_borrow() {
            sel.iter().filter(|s| s.2 == true ).map(|s| s.1 ).collect()
        } else {
            println!("Selected is borrowed");
            Vec::new()
        }
    }

    pub fn unselected_cols(&self) -> Vec<usize> {
        let n = if let Ok(sel) = self.selected.try_borrow() {
            sel.len()
        } else {
            println!("Selected is borrowed");
            return Vec::new();
        };
        let selected = self.selected_cols();
        let mut unselected = Vec::new();
        for i in 0..n {
            if !selected.iter().any(|s| i == *s) {
                unselected.push(i);
            }
        }
        unselected
    }

    pub fn unselect_all(&self) {
        let selected = self.selected_cols();
        if let Ok(mut sel) = self.selected.try_borrow_mut() {
            for s in selected {
                Self::switch_selected(self.grid.clone(), &mut sel, s);
            }
        } else {
            println!("Could not retrieve mutable reference to selected");
        }
    }

    fn switch_all(grid : Grid, cols : &mut [(String, usize, bool)]) {
        let switch_to = !cols.iter().any(|c| c.2);
        let n = cols.len();
        for i in 0..n {
            Self::switch_to(grid.clone(), &mut cols[i], n, switch_to);
        }
    }

    pub fn expose_event_box(&self, ix : usize) -> Option<EventBox> {
        // let children = self.grid.get_children();

        // TODO correct "attempted to subtract with overflow: changing from one
        // mapping to the other leads to search for inexistent table.
        let (nrows, ncols) = self.dimensions();
        let children = self.grid.get_children();
        let header_iter = children.iter()
            .skip(ncols * nrows - ncols);
        let n = header_iter.clone().count();
        let wid = header_iter.skip(n-ix-1).next()?;
        if let Ok(ev_box) = wid.clone().downcast::<EventBox>() {
            Some(ev_box.clone())
        } else {
            None
        }
    }

    /// Function supplied by user should take all selected columns at
    /// the third argument and the index of the last selected column at
    /// the last argument.
    pub fn set_selected_action<F>(&self, f : F, btn : u32)
    where
        F : Clone,
        for<'r,'s> F : Fn(&'r EventBox, &'s gdk::EventButton, Vec<usize>, usize)->Inhibit+'static
    {
        let children = self.grid.get_children();
        let (nrows, ncols) = self.dimensions();
        let header_iter = children.iter()
            .skip(ncols * nrows - ncols);
        let n = header_iter.clone().count();
        for (i, wid) in header_iter.enumerate() {
            if let Ok(ev_box) = wid.clone().downcast::<EventBox>() {
                let selected = self.selected.clone();
                let f = f.clone();
                ev_box.connect_button_press_event(move |ev_box, ev| {
                    if ev.get_button() == btn {
                        if let Ok(sel) = selected.try_borrow() {
                            let sel_ix : Vec<_> = sel.iter()
                                .filter(|c| c.2)
                                .map(|c| c.1)
                                .collect();
                            f(ev_box, ev, sel_ix, n - i - 1);
                        } else {
                            println!("Unable to retrieve reference to selected vector");
                        }
                    }
                    glib::signal::Inhibit(false)
                });
            } else {
                println!("Could not convert widget to event box");
            }
        }
    }*/

    fn update_data<I, S>(&self, mut data : Vec<I>, include_header : bool)
    where
        I : ExactSizeIterator<Item=S>,
        S : AsRef<str>
    {
        if include_header {
            self.clear_table();
        } else {
            self.clear_table_data();
        }

        if data.len() == 0 {
            return;
        }
        let nrows = data.len();
        let mut ncols = 0;
        for (i, row) in data.iter_mut().enumerate().take(nrows) {
            if i == 0 {
                ncols = row.len();
                if ncols == 0 {
                    return;
                }

                // Dimensions are defined here, when table is created. This assumes
                // any new tables generated by sorting/filtering will have number of
                // rows smaller than or equal to the first table.
                if include_header {
                    self.update_table_dimensions(nrows as i32, ncols as i32);
                }
            }
            if include_header {
                for (j, col) in row.enumerate() {
                    let cell = self.create_data_cell(col.as_ref(), i, j, nrows, ncols, include_header);
                    self.grid.attach(&cell, j as i32, i as i32, 1, 1);
                }
            } else {
                for (j, col) in row.enumerate() {
                    let cell = self.create_data_cell(col.as_ref(), i+1, j, nrows, ncols, include_header);
                    self.grid.attach(&cell, j as i32, (i+1) as i32, 1, 1);
                }
            }
        }
    }

    /*fn clear_tail(&self, remaining_rows : usize) {
        while self.grid.child_at(0, (remaining_rows-1) as i32).is_some() {
            self.grid.remove_row((remaining_rows-1) as i32);
        }
    }

    fn grow_tail(&self, _new_sz : usize) {

    }*/

    // Removes all rows of table, including header
    fn clear_table(&self) {
        while self.grid.child_at(0, 0).is_some() {
            self.grid.remove_row(0);
        }
    }

    // Remove all but the first (header) row of the table
    fn clear_table_data(&self) {
        while self.grid.child_at(0, 1).is_some() {
            self.grid.remove_row(1);
        }
    }

    fn update_table_dimensions(&self, nrows : i32, ncols : i32) {

        for c in 0..ncols {
            self.grid.insert_column(c);
        }

        for r in 0..nrows {
            self.grid.insert_row(r);
        }

    }

    /*pub fn set_selected(&self, new_sel : &[usize]) {
        self.unselect_all();
        if let Ok(mut sel) = self.selected.try_borrow_mut() {
            for i in new_sel.iter() {
                Self::switch_selected(self.grid.clone(), &mut sel[..], *i);
            }
        } else {
            println!("Failed to retrieve mutable reference to selected columns");
        }
    }*/

    /*pub fn dimensions(&self) -> (usize, usize) {
        self.dims.borrow().clone()
    }*/

}

/*fn switch_to(
    grid : Grid,
    col : &mut (String, usize, bool),
    ncols : usize,
    selected : bool
) {
    set_selected_style(grid.clone(), ncols, col.1, selected);
    *col = (col.0.clone(), col.1, selected);
}

fn _switch_selected(grid : Grid, cols : &mut [(String, usize, bool)], pos : usize) {
    let ncols = cols.len();
    if let Some(col) = cols.get_mut(pos) {
        if col.2 == true {
            switch_to(grid.clone(), col, ncols, false);
        } else {
            switch_to(grid.clone(), col, ncols, true);
        }
    }
}*/

fn selected_col(grid : &Grid, ncols : usize) -> usize {
    let mut sel_col = 0;
    for c in 0..ncols {
        if grid.child_at(c as i32, 0 as i32).unwrap().style_context().has_class("selected") {
            sel_col = c;
        }
    }
    sel_col
}

fn set_selected_style(grid : Grid, col : usize, selected : bool) {
    let mut row = 0;
    // for wid in grid.children().iter().skip(ncols - col - 1).step_by(ncols) {
    while let Some(wid) = grid.child_at(col as i32, row) {
        let wid = wid.clone().downcast::<Label>().unwrap();
        let ctx = wid.style_context();
        if selected {
            if !ctx.has_class("selected") {
                ctx.add_class("selected");
            }
        } else {
            if ctx.has_class("selected") {
                ctx.remove_class("selected");
            }
        }
        row += 1;
    }
}


