use gtk4::*;
use gtk4::prelude::*;
use std::rc::Rc;
use std::cell::RefCell;
use monday::tables::table::Table;
// use gdk::prelude::*;
// use gdk::{Cursor, CursorType};
use std::iter::ExactSizeIterator;

#[derive(Clone, Debug)]
pub struct TableWidget {

    pub grid : Grid,

    pub scroll_window : ScrolledWindow,

    parent_ctx : StyleContext,

    provider : CssProvider,

}

impl TableWidget {

    pub fn new_from_table(tbl : &Table, max_nrows : usize, max_ncols : usize) -> Self {
        let mut tbl_wid = Self::new();
        println!("Table created");
        let data = tbl.text_rows(Some(max_nrows), Some(max_ncols));
        tbl_wid.update_data(data);
        println!("Table populated");
        tbl_wid
    }

    pub fn new() -> TableWidget {
        let grid = Grid::new();
        // let _message = Label::new(None);
        let provider = CssProvider::new();
        provider.load_from_path("assets/styles/tables.css");
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
        // scroll_window.show_all();
        // let selected = Rc::new(RefCell::new(Vec::new()));
        // let dims = Rc::new(RefCell::new((0, 0)));
        // let tbl = Table::new_empty(None);
        TableWidget { grid, scroll_window, /*box_container,*/ parent_ctx, provider, /*selected, dims,*/ /*tbl*/ }
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
        ncols : usize
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

    fn update_data<I, S>(&mut self, mut data : Vec<I>)
    where
        I : ExactSizeIterator<Item=S>,
        S : AsRef<str>
    {
        self.clear_table();
        if data.len() == 0 {
            return;
        }
        let nrows = data.len(); /*.min(200);*/
        let mut ncols = 0;
        for (i, mut row) in data.iter_mut().enumerate().take(nrows) {
            if i == 0 {
                ncols = row.len();
                if ncols == 0 {
                    return;
                }
                self.update_table_dimensions(nrows as i32, ncols as i32);
            }
            for (j, col) in row.enumerate() {
                if i == 0 {
                    let header_cell = self.create_data_cell(col.as_ref(), i, j, nrows, ncols);
                    self.grid.attach(&header_cell, j as i32, i as i32, 1, 1);
                } else {
                    let cell = self.create_data_cell(col.as_ref(), i, j, nrows, ncols);
                    self.grid.attach(&cell, j as i32, i as i32, 1, 1);
                }
            }
        }
    }

    fn clear_tail(&self, remaining_rows : usize) {
        while self.grid.child_at(0, (remaining_rows-1) as i32).is_some() {
            self.grid.remove_row((remaining_rows-1) as i32);
        }
    }

    fn grow_tail(&self, new_sz : usize) {

    }

    fn clear_table(&self) {
        while self.grid.child_at(0, 0).is_some() {
            self.grid.remove_row(0);
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

fn switch_to(
    grid : Grid,
    col : &mut (String, usize, bool),
    ncols : usize,
    selected : bool
) {
    set_selected_style(grid.clone(), ncols, col.1, selected);
    *col = (col.0.clone(), col.1, selected);
}

fn switch_selected(grid : Grid, cols : &mut [(String, usize, bool)], pos : usize) {
    let ncols = cols.len();
    if let Some(col) = cols.get_mut(pos) {
        if col.2 == true {
            switch_to(grid.clone(), col, ncols, false);
        } else {
            switch_to(grid.clone(), col, ncols, true);
        }
    } else {
        println!("Invalid column index")
    }
}

fn set_selected_style(grid : Grid, ncols : usize, col : usize, selected : bool) {
    /*for wid in grid.get_children().iter().skip(ncols - col - 1).step_by(ncols) {
        let wid = wid.clone().downcast::<Label>(); /*if let Ok(ev) = wid.clone().downcast::<Label>() {
            ev.get_child().unwrap()
        } else {
            wid.clone()
        };*/
        let ctx = wid.get_style_context();
        if selected {
            if !ctx.has_class("selected") {
                ctx.add_class("selected");
            }
        } else {
            if ctx.has_class("selected") {
                ctx.remove_class("selected");
            }
        }
    }*/
    unimplemented!()
}


