/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use gtk4::*;
use gtk4::prelude::*;
use crate::tables::table::Table;
use std::iter::ExactSizeIterator;
use gtk4::gdk::Cursor;
use std::rc::Rc;
use std::cell::RefCell;

#[derive(Debug, Clone)]
pub struct TablePopover {
    pub popover : Popover,
    pub fst_scale : Scale,
    pub num_scale : Scale,
    pub filter_entry : Entry,
    pub btn_ascending : ToggleButton,
    pub btn_descending : ToggleButton
}

fn configure_scale(scale : &Scale) {
    scale.set_digits(0);
    scale.set_draw_value(true);
    scale.set_hexpand(true);
    scale.set_halign(Align::Fill);
    scale.set_value_pos(PositionType::Right);
    scale.set_has_origin(false);
}

fn scale_adjustment(nrows : usize) -> Adjustment {
    Adjustment::builder().lower(1.).upper(nrows as f64).value(1.).build()
}

impl TablePopover {

    pub fn reset_navigation(&self) {
        self.fst_scale.adjustment().set_value(1.0);
        self.num_scale.adjustment().set_value(std::f64::MAX);
        // self.fst_scale.set_fill_level(1.0);
        // self.num_scale.set_fill_level(std::f64::MAX);
        self.filter_entry.set_text("");
    }

    pub fn new(nrows : usize, max_nrows : usize) -> Self {
        let fst_scale = Scale::new(Orientation::Horizontal,Some(&scale_adjustment(max_nrows)));
        configure_scale(&fst_scale);

        let top_bx = Box::new(Orientation::Horizontal, 0);
        let middle_bx = Box::new(Orientation::Horizontal, 0);
        let bottom_bx = Box::new(Orientation::Horizontal, 0);
        top_bx.style_context().add_class("linked");
        let btn_ascending = ToggleButton::builder().icon_name("view-sort-ascending-symbolic").build();
        let btn_descending = ToggleButton::builder().icon_name("view-sort-descending-symbolic").build();
        btn_ascending.style_context().add_class("flat");
        btn_descending.style_context().add_class("flat");

        let filter_entry = Entry::new();
        filter_entry.set_max_width_chars(32);
        filter_entry.set_primary_icon_name(Some("funnel-symbolic"));

        top_bx.append(&btn_ascending);
        top_bx.append(&btn_descending);
        top_bx.append(&filter_entry);
        btn_ascending.set_group(Some(&btn_descending));
        btn_ascending.set_active(true);

        let line_img = Image::from_icon_name("view-continuous-symbolic");
        middle_bx.append(&line_img);
        middle_bx.append(&fst_scale);

        let num_img = Image::from_icon_name("type-integer-symbolic");
        let num_scale = Scale::new(Orientation::Horizontal,Some(&scale_adjustment(max_nrows)));

        configure_scale(&num_scale);

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

    //_parent_ctx : StyleContext,

    provider : CssProvider,

    popover : TablePopover,

    tbl : Rc<Table>,

    max_nrows : usize

}

impl Drop for TableWidget {

    fn drop(&mut self) {
        // This avoids the following warning when the ScrolledWindow is dropped:
        // Finalizing GtkScrolledWindow 0x560206700e60, but it still has children left:
        if self.popover.popover.parent().is_some() {
            self.popover.popover.unparent();
        }
    }

}

fn update_cols(tbl : &Table, grid : &Grid, fst_row : usize, max_nrows : usize) {
    for col in 0..tbl.size().1 {
        grid.child_at(col as i32, 1).unwrap().downcast::<Label>().unwrap()
            .set_text(&tbl.display_lines(col, Some(fst_row), Some(max_nrows)));
    }
}

const HEADER_DARK_CSS : &'static str =r#"
label {
  font-weight : bold;
  border-bottom : 1px solid #454545;
  border-right : 1px solid #454545;
  padding-left: 10px;
  padding-right: 10px;
  padding-top: 10px;
  padding-bottom: 10px;
  background-color : #1E1E1E;
}

.selected {
  background-color : #404040;
  border : 1px solid #454545;
}
"#;

const DATA_DARK_CSS : &'static str =r#"
label {
  padding-left: 10px;
  padding-right: 10px;
  line-height : 39px;
  background-color: #1E1E1E;
  background-size: 1px 39px;
  background-image: linear-gradient(0deg, #454545, #454545 1px, #1E1E1E 1px, #1E1E1E);
  border-left : 1px solid #454545;
  border-right : 1px solid #454545;
}

.selected {
  background-color : #F5F6F7;
  background-size: 1px 39px;
  background-image: linear-gradient(0deg, #454545, #454545 1px, #404040 1px, #404040);
  border-left : 1px solid #454545;
  border-right : 1px solid #454545;
}
"#;

const HEADER_WHITE_CSS : &'static str =r#"
label {
  font-weight : bold;
  border-bottom : 1px solid #dcdcdc;
  border-right : 1px solid #dcdcdc;
  padding-left: 10px;
  padding-right: 10px;
  padding-top: 10px;
  padding-bottom: 10px;
  background-color : #FFFFFF;
}

.selected {
  background-color : #F5F6F7;
  border : 1px solid #E9E9E9;
}
"#;

const DATA_WHITE_CSS : &'static str = r#"
label {
  padding-left: 10px;
  padding-right: 10px;
  line-height : 39px;
  background-color: #ffffff;
  background-size: 1px 39px;
  background-image: linear-gradient(0deg, #dcdcdc, #dcdcdc 1px, #ffffff 1px, #ffffff);
  border-left : 1px solid #F0F0F0;
  border-right : 1px solid #F0F0F0;
}

.selected {
  background-color : #F5F6F7;
  background-size: 1px 39px;
  background-image: linear-gradient(0deg, #E9E9E9, #E9E9E9 1px, #F5F6F7 1px, #F5F6F7);
  border-left : 1px solid #E9E9E9;
  border-right : 1px solid #E9E9E9;
}
"#;

#[derive(Debug, Clone)]
pub struct DisplayedTable {

    sorted_by : usize,

    ascending : bool,

    filtered_by : Option<String>,

    tbl : Table

}

pub fn column_label(tbl : &Table, col : usize, fst_row : Option<usize>, nrows : Option<usize>) -> Label {
    let lbl = Label::new(Some(&tbl.display_lines(col, fst_row, nrows)));
    lbl.set_justify(Justification::Center);
    //lbl.set_height_request(nrows.unwrap_or(tbl.nrows()) as i32*39);
    lbl.set_vexpand(false);
    lbl.set_hexpand(true);
    lbl.set_halign(Align::Fill);
    lbl.set_valign(Align::Start);
    // let provider = CssProvider::new();
    // provider.load_from_data(css.as_bytes());
    // let parent_ctx = lbl.style_context();
    // parent_ctx.add_provider(&provider,800);
    lbl
}

fn add_header_css(lbl : &Label) {
    let provider = CssProvider::new();
    if libadwaita::StyleManager::default().is_dark() {
        provider.load_from_data(HEADER_DARK_CSS.as_bytes());
    } else {
        provider.load_from_data(HEADER_WHITE_CSS.as_bytes());
    }
    let ctx = lbl.style_context();
    ctx.add_provider(&provider,800);
}

fn add_data_css(lbl : &Label) {
    let provider = CssProvider::new();
    if libadwaita::StyleManager::default().is_dark() {
        provider.load_from_data(DATA_DARK_CSS.as_bytes());
    } else {
        provider.load_from_data(DATA_WHITE_CSS.as_bytes());
    }
    let ctx = lbl.style_context();
    ctx.add_provider(&provider,800);
}

fn set_table_selection_style(grid : &Grid, col : usize, ncols : usize, was_selected : bool) {
    // For the current column, negate the previous state
    set_selected_style(grid.clone(), col, !was_selected);

    // For all other columns, set the old state of this column.
    if !was_selected {
        for c in 0..ncols {
            if c != col {
                set_selected_style(grid.clone(), c, was_selected);
            }
        }
    }
}

impl TableWidget {

    pub fn new_from_table(tbl : &Table, max_nrows : usize, max_ncols : usize) -> Self {
        let mut tbl_wid = Self::new(tbl.nrows(), max_nrows);
        tbl_wid.tbl = Rc::new(tbl.clone());
        tbl_wid.update_data(&tbl, Some(1), Some(max_nrows), true);
        tbl_wid
    }

    pub fn new(nrows : usize, max_nrows : usize) -> TableWidget {
        let grid = Grid::new();
        let provider = CssProvider::new();
        
        /*if libadwaita::StyleManager::default().is_dark() {
            provider.load_from_data(TABLE_DARK_CSS.as_bytes());
        } else {
            provider.load_from_data(TABLE_WHITE_CSS.as_bytes());
        }*/
        
        // let parent_ctx = grid.style_context();
        // parent_ctx.add_provider(&provider,800);

        // let msg = Label::new(None);
        // let box_container = Box::new(Orientation::Vertical, 0);
        // box_container.append(&grid, true, true, 0);
        // box_container.pack_start(&msg, true, true, 0);
        let scroll_window = ScrolledWindow::new();
        scroll_window.set_vexpand(true);
        scroll_window.set_valign(Align::Fill);
        scroll_window.set_child(Some(&grid));
        let popover = TablePopover::new(nrows, max_nrows);
        popover.popover.set_parent(&scroll_window);
        TableWidget {
            grid,
            max_nrows,
            scroll_window,
            // _parent_ctx : parent_ctx,
            provider,
            popover,
            tbl : Rc::new(Table::empty(Vec::new()))
        }
    }

    pub fn parent(&self) -> ScrolledWindow {
        self.scroll_window.clone()
    }

    fn create_header_cell(
        &self,
        data : &str,
        col : usize,
        nrows : usize,
        ncols : usize,
        include_header : bool,
        displayed_tbl : &Rc<RefCell<Option<DisplayedTable>>>
    ) -> Label {
        let label = Label::new(None);
        label.set_use_markup(true);
        label.set_markup(&data);
        label.set_hexpand(true);
        let ctx = label.style_context();
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
            let max_nrows = self.max_nrows.clone();
            let displayed_tbl = displayed_tbl.clone();
            move |_gesture, _n_press, _x, _y| {
                let ctx = label.style_context();
                let was_selected = ctx.has_class("selected");
                if !was_selected {
                    popover.popover.set_pointing_to(Some(&label.allocation()));
                    popover.popover.popup();
                    if popover.btn_ascending.is_active() {
                        update_display_table(
                            &tbl,
                            &displayed_tbl,
                            "",
                            col,
                            true,
                            &grid,
                            max_nrows
                        );
                        set_table_selection_style(&grid, col, ncols, was_selected);
                        popover.reset_navigation();
                    } else {
                        // Setting the style before setting the button active is important
                        // so the right selected column is retrieved.
                        set_table_selection_style(&grid, col, ncols, was_selected);

                        // Call update_display_table on the on_toggle signal of the button
                        popover.btn_ascending.set_active(true);
                    }
                } else {
                    set_table_selection_style(&grid, col, ncols, was_selected);
                    popover.popover.hide();
                }
            }
        });

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

        label
    }

    pub fn add_popover_signals(&self, displayed_tbl : &Rc<RefCell<Option<DisplayedTable>>>) {
        let eff_rows = self.tbl.nrows().min(self.max_nrows) as f64;
        let fst_adj = Adjustment::builder().value(1.).lower(1.).upper(eff_rows).build();
        let num_adj = Adjustment::builder().value(eff_rows).lower(1.).upper(eff_rows).build();
        self.popover.fst_scale.set_adjustment(&fst_adj);
        self.popover.num_scale.set_adjustment(&num_adj);
        self.popover.btn_ascending.connect_toggled({
            let displayed_tbl = displayed_tbl.clone();
            let popover = self.popover.clone();
            let max_nrows = self.max_nrows.clone();
            let tbl = self.tbl.clone();
            let grid = self.grid.clone();
            move |btn| {
                if btn.is_active() {
                    if let Some(sel_col) = selected_col(&grid, tbl.ncols()) {
                        update_display_table(
                            &tbl,
                            &displayed_tbl,
                            "",
                            sel_col,
                            true,
                            &grid,
                            max_nrows
                        );
                        popover.reset_navigation();
                    } else {
                        eprintln!("No column selected");
                    }
                }
            }
        });
        self.popover.btn_descending.connect_toggled({
            let displayed_tbl = displayed_tbl.clone();
            let popover = self.popover.clone();
            let max_nrows = self.max_nrows.clone();
            let tbl = self.tbl.clone();
            let grid = self.grid.clone();
            move |btn| {
                if btn.is_active() {
                    if let Some(sel_col) = selected_col(&grid, tbl.ncols()) {
                        update_display_table(
                            &tbl,
                            &displayed_tbl,
                            "",
                            sel_col,
                            false,
                            &grid,
                            max_nrows
                        );
                        popover.reset_navigation();
                    } else {
                        eprintln!("No column selected");
                    }
                }
            }
        });
        fst_adj.connect_value_changed({
            let grid = self.grid.clone();
            let num_scale = self.popover.num_scale.clone();
            let displayed_tbl = displayed_tbl.clone();
            let max_rows = self.max_nrows.clone();
            move|adj| {
                let fst_row = adj.value() as usize;
                let num_rows = num_scale.adjustment().value() as usize;
                if let Ok(displ_tbl) = displayed_tbl.try_borrow() {
                    if let Some(tbl) = &*displ_tbl {
                        let row_limit = tbl.tbl.nrows().min(max_rows);
                        let rem_rows = row_limit.saturating_sub(fst_row.saturating_sub(1)).max(1);

                        update_cols(&tbl.tbl, &grid, fst_row, row_limit);
                        num_scale.set_range(1.0, rem_rows as f64);

                        // Letting the adjustment changed signal to be emitted here
                        // is safe, since displayed_table is not borrowed mutably for the scale
                        // callbacks.
                        num_scale.set_value(rem_rows as f64);

                    } else {
                        eprintln!("No display table configured");
                    }
                } else {
                    eprintln!("Unable to acquire borrow over display table");
                }
            }
        });

        num_adj.connect_value_changed({
            let grid = self.grid.clone();
            let fst_scale = self.popover.fst_scale.clone();
            let displayed_tbl = displayed_tbl.clone();
            move |adj| {
                let num_rows = adj.value() as usize;
                if let Ok(displ_tbl) = displayed_tbl.try_borrow() {
                    if let Some(tbl) = &*displ_tbl {
                        let fst_row = fst_scale.adjustment().value() as usize;
                        update_cols(&tbl.tbl, &grid, fst_row, num_rows);
                    } else {
                        eprintln!("No display table configured");
                    }
                } else {
                    eprintln!("Unable to acquire borrow over display table");
                }
            }
        });

        self.popover.filter_entry.connect_changed({
            let grid = self.grid.clone();
            let fst_scale = self.popover.fst_scale.clone();
            let num_scale = self.popover.num_scale.clone();
            let displayed_tbl = displayed_tbl.clone();
            let max_nrows = self.max_nrows.clone();
            let orig_tbl = self.tbl.clone();
            move |entry| {
                let txt = entry.buffer().text();
                let mut num_scale_new_val = None;
                let mut num_scale_new_max = None;
                let mut fst_scale_new_val = None;
                let mut fst_scale_new_max = None;
                if let Ok(mut displ_tbl) = displayed_tbl.try_borrow_mut() {
                    if let Some(mut displ_tbl) = displ_tbl.as_mut() {

                        if let Some(sel_col) = selected_col(&grid, orig_tbl.ncols()) {
                            if txt.is_empty() {
                                update_cols(&orig_tbl, &grid, 1, max_nrows);
                                let rem_rows = orig_tbl.nrows().min(max_nrows).max(1) as f64;
                                fst_scale_new_val = Some(1.0);
                                num_scale_new_val = Some(rem_rows);
                                num_scale_new_max = Some(rem_rows);
                                fst_scale_new_max = Some(rem_rows);
                                displ_tbl.tbl = orig_tbl.as_ref().clone();
                                displ_tbl.filtered_by = None;
                            } else {

                                // filtering preserves the order.
                                if let Some(filtered_tbl) = orig_tbl.filtered_by(sel_col, &txt) {
                                    let rem_rows = filtered_tbl.nrows().min(max_nrows).max(1) as f64;
                                    update_cols(&filtered_tbl, &grid, 1, max_nrows);
                                    fst_scale_new_val = Some(1.0);
                                    num_scale_new_val = Some(rem_rows);
                                    num_scale_new_max = Some(rem_rows);
                                    fst_scale_new_max = Some(rem_rows);
                                    displ_tbl.tbl = filtered_tbl;
                                    displ_tbl.filtered_by = Some(txt.to_string());
                                } else {
                                    eprintln!("Could not filter table");
                                }
                            }
                        } else {
                            eprintln!("No column selected");
                        }
                    } else {
                        eprintln!("No display table available");
                    }
                } else {
                    eprintln!("Unable to acquire mutable borrow over display table");
                    return;
                }

                // It is important to set the new maxima before setting
                // the new value.
                if let Some(new_max) = num_scale_new_max {
                    num_scale.set_range(1.0, new_max);
                }
                if let Some(new_max) = fst_scale_new_max {
                    fst_scale.set_range(1.0, new_max);
                }
                if let Some(new_val) = num_scale_new_val {
                    num_scale.set_value(new_val);
                }
                if let Some(new_val) = fst_scale_new_val {
                    fst_scale.set_value(new_val);
                }
            }
        });
    }

    /*/// Returns selected columns, as a continuous index from the first
    /// column of the current table
    pub fn selected_cols(&self) -> Vec<usize> {
        if let Ok(sel) = self.selected.try_borrow() {
            sel.iter().filter(|s| s.2 == true ).map(|s| s.1 ).collect()
        } else {
            Vec::new()
        }
    }

    pub fn unselected_cols(&self) -> Vec<usize> {
        let n = if let Ok(sel) = self.selected.try_borrow() {
            sel.len()
        } else {
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
                        }
                    }
                    glib::signal::Inhibit(false)
                });
            } else {
            }
        }
    }*/

    fn update_data(&self, tbl : &Table, fst_row : Option<usize>, num_rows : Option<usize>, include_header : bool) {
        if include_header {
            self.clear_table();
        } else {
            self.clear_table_data();
        }

        // if data.len() == 0 {
        //    return;
        // }
        let (nrows, ncols) = tbl.size();
        if nrows == 0 || ncols == 0 {
            return;
        }
        if include_header {
            self.update_table_dimensions2(ncols as i32);
        }

        // Keeps state of user navigation (sort, filter, offset and length)
        let displayed_tbl : Rc<RefCell<Option<DisplayedTable>>> = Rc::new(RefCell::new(None));

        // Add header
        for (j, col) in tbl.names().drain(..).enumerate() {
            let cell = self.create_header_cell(col.as_ref(), j, nrows, ncols, include_header, &displayed_tbl);
            add_header_css(&cell);
            self.grid.attach(&cell, j as i32, 0 as i32, 1, 1);
        }
        self.add_popover_signals(&displayed_tbl);

        // Add data column
        for j in 0..ncols {
            let cell = column_label(&tbl, j, fst_row, num_rows);
            add_data_css(&cell);
            self.grid.attach(&cell, j as i32, 1, 1, 1);
        }
    }

    /*fn update_data<I, S>(&self, mut data : Vec<I>, include_header : bool)
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
    }*/

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

    pub fn update_table_dimensions2(&self, ncols : i32) {
        for c in 0..ncols {
            self.grid.insert_column(c);
        }

        for r in 0..2 {
            self.grid.insert_row(r);
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

// This shows the full table from row 1 up to max_rows.
fn update_display_table(
    tbl : &Rc<Table>,
    displayed_tbl : &Rc<RefCell<Option<DisplayedTable>>>,
    filter_key : &str,
    col : usize,
    ascending : bool,
    grid : &Grid,
    max_nrows : usize
) {
    if let Ok(mut displayed_tbl) = displayed_tbl.try_borrow_mut() {
        let requires_update = if let Some(displayed) = displayed_tbl.as_ref() {
            if displayed.sorted_by == col && displayed.ascending == ascending {

                // No filter is required if text was unchanged or there is no filter set.
                let no_filter_change = (filter_key.is_empty() && displayed.filtered_by.is_none()) ||
                    displayed.filtered_by.as_ref().map(|s| &s[..] == &filter_key[..] ).unwrap_or(false);
                if no_filter_change {
                    update_cols(&displayed.tbl, &grid, 1, max_nrows);
                    false
                } else {
                    true
                }
            } else {
                true
            }
        } else {
            true
        };
        if requires_update {
            let opt_new_tbl = if filter_key.is_empty() {
                tbl.sorted_by(col, ascending)
            } else {
                tbl.filtered_by(col, filter_key).and_then(|tbl| tbl.sorted_by(col, ascending) )
            };
            if let Some(new_tbl) = opt_new_tbl {
                let filtered_by = if filter_key.is_empty() { None } else { Some(filter_key.to_string()) };
                update_cols(&new_tbl, &grid, 1, max_nrows);
                *displayed_tbl = Some(DisplayedTable {
                    tbl : new_tbl,
                    ascending,
                    sorted_by : col,
                    filtered_by
                });
            } else {
                eprintln!("Failed to get new table");
            }
        }
    } else {
        eprintln!("Could not acquire mutable borrow over display table");
    }
}

fn selected_col(grid : &Grid, ncols : usize) -> Option<usize> {
    let mut sel_col = None;
    for c in 0..ncols {
        let style_ctx = grid.child_at(c as i32, 0 as i32)
            .unwrap()
            .style_context();
        if style_ctx.has_class("selected") {
            if sel_col.is_none() {
                sel_col = Some(c);
            } else {
                eprintln!("Multiple columns with selected style");
            }
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


