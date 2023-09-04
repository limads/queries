/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.
For a copy, see http://www.gnu.org/licenses.*/

use gtk4::*;
use gtk4::prelude::*;
use stateful::React;
use crate::ui::MainMenu;
use libadwaita::ExpanderRow;
use papyri::render::GroupSplit;
use libadwaita::prelude::*;
use papyri::model::MappingType;
use std::rc::Rc;
use std::cell::{Cell,RefCell};
use gdk::RGBA;
use crate::client::ActiveConnection;
use crate::sql::object::DBObject;
use gdk_pixbuf::Pixbuf;
use crate::ui::PackedImageLabel;
use crate::ui::settings::NamedBox;
use std::collections::BTreeMap;
use std::cmp::{Ord, Ordering, PartialEq};
use stateful::{Deferred, Reactive, Exclusive /*Owned*/};
use tuples::TupleCloned;
use either::Either;

/* Query builder restrictions:
(1) Each table can have a single source column for a join operation to the right-hand table
(2) */

#[derive(Clone, Debug)]
pub struct QueryBuilderWindow {
    win : Window,
    btn_clear : Button,
    btn_sql : Button,
    pub btn_run : Button,
    toggles : Rc<RefCell<BTreeMap<String, ToggleButton>>>,
    boxes : Rc<RefCell<Vec<Box>>>,
    objs : Rc<RefCell<Vec<DBObject>>>,
    cols_model : Rc<RefCell<Option<ListStore>>>,
    entry_col : Entry,
    entry_filter : Entry,
    entry_join : Entry,
    join_combo : ComboBoxText,
    filter_combo : ComboBoxText,
    add_btn : Button,
    delete_btn : Button,
    middle_bx : Box,
    combo_group : ComboBoxText,
    combo_sort : ComboBoxText,
    query : Deferred<Query>
}

pub struct JoinBox {
    outer : Box,
    inner : Box,
}

impl JoinBox {

    pub fn new(join : &Join) -> Self {
        let join_bx = Box::new(Orientation::Horizontal, 0);

        let join_bx_inner = Box::new(Orientation::Horizontal, 0);
        let join_icon = match join.op {
            JoinOp::Inner => "inner-symbolic",
            JoinOp::LeftOuter => "left-symbolic",
            JoinOp::RightOuter => "right-symbolic",
            JoinOp::FullOuter => "full-symbolic"
        };
        let join_icon = Image::from_icon_name(join_icon);
        // join_bx_inner.append(&);
        let lbl_bx = Box::new(Orientation::Vertical, 6);

        lbl_bx.append(&Label::new(Some(&format!("{}.{}", join.src_table, join.src_col))));
        lbl_bx.append(&join_icon);
        lbl_bx.append(&Label::new(Some(&format!("{}.{}", join.dst_table, join.dst_col))));
        lbl_bx.set_vexpand(false);
        lbl_bx.set_valign(Align::Center);
        join_bx_inner.append(&lbl_bx);
        join_bx_inner.set_hexpand(true);
        join_bx_inner.set_halign(Align::Center);
        join_bx.append(&join_bx_inner);

        let outer = Box::new(Orientation::Horizontal, 0);
        let inner = Box::new(Orientation::Vertical, 0);
        inner.set_hexpand(true);

        join_bx.set_hexpand(false);
        outer.append(&join_bx);
        outer.append(&inner);

        set_css(&join_bx);

        Self { outer, inner }
    }
}

#[derive(Debug, Clone)]
pub struct TableBox {
    bx : Box,
    title : Label,
    toggle_bx : gtk4::Box
}

const TABLE_WHITE_CSS : &str = r#"
box {
  padding: 12px;
  background-color: #ffffff;
  border : 1px solid #F0F0F0;
}"#;

const TABLE_DARK_CSS : &str = r#"
box {
  padding: 12px;
  background-color: #454545;
  border : 1px solid #1E1E1E;
}"#;

impl TableBox {

    pub fn build(tbl : &Table, toggles : &Rc<RefCell<BTreeMap<String, ToggleButton>>>) -> Self {
        let bx = Box::new(Orientation::Vertical, 0);
        let title_bx = Box::new(Orientation::Horizontal, 0);
        title_bx.set_margin_bottom(6);

        let title_inner_bx = Box::new(Orientation::Horizontal, 0);
        title_inner_bx.set_halign(Align::Start);
        title_inner_bx.set_hexpand(true);
        let img = Image::from_icon_name("table-symbolic");
        img.set_margin_end(6);

        title_inner_bx.append(&img);
        let title = Label::new(Some(&tbl.name));
        title_inner_bx.append(&title);
        title_bx.append(&title_inner_bx);

        let toggle_bx = Box::new(Orientation::Horizontal, 0);
        toggle_bx.style_context().add_class("linked");
        bx.append(&title_bx);
        bx.append(&toggle_bx);
        set_css(&bx);
        let tbl_bx = Self { title, toggle_bx, bx };
        for col in &tbl.cols {
            add_toggle(&toggles, &tbl.name, &col.name, &tbl_bx);
        }
        tbl_bx
    }

}

fn set_css(bx : &gtk4::Box) {
    let provider = CssProvider::new();
    let css = if libadwaita::StyleManager::default().is_dark() {
        TABLE_DARK_CSS
    } else {
        TABLE_WHITE_CSS
    };
    provider.load_from_data(css.as_bytes());
    bx.style_context().add_provider(&provider, 800);
}

const FILTER_OPS : [&'static str; 7] = [
    "No filter",
    "= (Equal)",
    "> (Greater)",
    "≥ (Greater/equal)",
    "< (Less)",
    "≤ (Less/equal)",
    "like (Pattern match)"
];

const JOIN_OPS : [&'static str; 5] = [
    "No joins",
    "Inner join",
    "Left outer join",
    "Right outer join",
    "Full outer join"
];

const GROUP_OPS : [&'static str; 9] = [
    "No aggregate",
    "Group by",
    "Count",
    "Sum",
    "Average",
    "Minimum",
    "Maximum",
    "Every",
    "Any"
];

const SORT_OPS : [&'static str; 3] = [
    "Unsorted",
    "Ascending",
    "Descending"
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupOp {
    GroupBy,
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Every,
    Any
}

impl GroupOp {

    pub fn combo_str(&self) -> &str {
        match self {
             GroupOp::GroupBy => "Group by",
             GroupOp::Count => "Count",
             GroupOp::Sum => "Sum",
             GroupOp::Avg => "Average",
             GroupOp::Min => "Minimum",
             GroupOp::Max => "Maximum",
             GroupOp::Every => "Every",
             GroupOp::Any => "Any",
        }
    }

    pub fn from_str(s : &str) -> Option<Self> {
        match s {
            "Group by" => Some(GroupOp::GroupBy),
            "Count" => Some(GroupOp::Count),
            "Sum" => Some(GroupOp::Sum),
            "Average" => Some(GroupOp::Avg),
            "Minimum" => Some(GroupOp::Min),
            "Maximum" => Some(GroupOp::Max),
            "Every" => Some(GroupOp::Every),
            "Any" => Some(GroupOp::Any),
            "No aggregate" | _ => None
        }
    }

}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOp {
    Ascending,
    Descending
}

impl SortOp {

    pub fn combo_str(&self) -> &str {
        match self {
            SortOp::Ascending => "Ascending",
            SortOp::Descending => "Descending"
        }
    }

    pub fn from_str(s : &str) -> Option<Self> {
        match s {
            "Ascending" => Some(SortOp::Ascending),
            "Descending" => Some(SortOp::Descending),
            "Unsorted" | _ => None
        }
    }

}

const HELP : &'static str = r#"
• Add columns by specifying their table name as a prefix.

• The second to last columns must belong to the same table as any previous
column (or be the argument to a new join clause).

• Aggregates (if any) must be applied to all columns.
"#;

/* TODO if the filter field is set and the
variable is set as a grouping variable, use
the having clause. Else, use regular where
clause. */

impl QueryBuilderWindow {

    pub fn current_sql(&self) -> String {
        self.query.view().sql().unwrap_or(String::new())
    }

    pub fn build() -> Self {
        let win = Window::new();
        super::configure_dialog(&win, false);
        win.set_title(Some("Query builder"));
        win.set_width_request(1200);
        win.set_height_request(800);
        let bx = Box::new(Orientation::Vertical, 6);
        bx.set_margin_start(64);
        bx.set_margin_end(64);

        let entry_col = Entry::new();
        entry_col.set_placeholder_text(Some("Column"));

        let toggles = Rc::new(RefCell::new(BTreeMap::new()));
        let boxes = Rc::new(RefCell::new(Vec::new()));

        let middle_bx = Box::new(Orientation::Horizontal, 0);
        middle_bx.set_valign(Align::Center);
        middle_bx.set_vexpand(true);

        let bottom_bx = Box::new(Orientation::Horizontal, 16);
        bottom_bx.set_margin_top(18);
        bottom_bx.set_margin_bottom(18);
        bottom_bx.set_halign(Align::Center);
        let btn_clear = Button::builder().label("Clear").build();
        let btn_sql = Button::builder().label("Copy SQL").build();

        let middle_stack = Stack::new();

        let help_bx = Box::new(Orientation::Horizontal, 0);
        set_css(&help_bx);
        let help_lbl = Label::new(None);
        help_lbl.set_use_markup(true);
        help_lbl.set_text(HELP);
        help_bx.append(&help_lbl);
        help_bx.set_vexpand(false);
        help_bx.set_valign(Align::Center);

        middle_stack.add_named(&middle_bx, Some("tables"));
        middle_stack.add_named(&help_bx, Some("help"));
        middle_stack.set_visible_child_name("help");

        let query = Deferred::new(Query::default());
        query.on_changed({
            let (middle_bx, toggles, boxes, entry_col, middle_stack) = (&middle_bx, &toggles, &boxes, &entry_col, &middle_stack).cloned();
            move |query| {
                update_ui_with_query(&middle_bx, &toggles, &boxes, query, &entry_col);
                if let Some(_) = query.0.get(0) {
                    middle_stack.set_visible_child_name("tables");
                } else {
                    middle_stack.set_visible_child_name("help");
                }
            }
        });
        btn_clear.connect_clicked({
            let query = query.share();
            move|_| {
                query.update(QueryMsg::Clear);
            }
        });
        btn_sql.connect_clicked({
            let query = query.share();
            move|_| {
                if let Some(displ) = gdk::Display::default() {
                    if let Some(sql) = query.view().sql() {
                        displ.clipboard().set_text(&sql);
                    }
                } else {
                    eprintln!("No default display to use");
                }
            }
        });
        let btn_run = Button::builder().label("Run").build();
        for btn in [&btn_clear, &btn_sql, &btn_run] {
            bottom_bx.append(btn);
            btn.style_context().add_class("pill");
        }
        btn_run.style_context().add_class("suggested-action");

        let top_bx = Box::new(Orientation::Vertical, 6);
        let info_bar = InfoBar::new();
        info_bar.set_revealed(false);
        let info_lbl = Label::new(None);
        info_lbl.set_text("Table must appear in a join clause before any of its columns are added");
        info_bar.set_show_close_button(true);
        info_bar.add_child(&info_lbl);
        info_bar.connect_response(move|info_bar, res| {
            match res {
                ResponseType::Close => info_bar.hide(),
                _ => { }
            }
        });

        top_bx.set_margin_top(18);
        let top_bx_upper = Box::new(Orientation::Horizontal, 0);
        let top_bx_lower = Box::new(Orientation::Horizontal, 0);

        let sort_bx = Box::new(Orientation::Horizontal, 0);
        let sort_icon = Image::from_icon_name("view-sort-descending-symbolic");
        sort_bx.append(&sort_icon);
        let combo_sort = ComboBoxText::new();
        for it in SORT_OPS {
            combo_sort.append(Some(it), it);
        }
        combo_sort.set_active_id(Some("Unsorted"));
        sort_bx.append(&combo_sort);

        let group_bx = Box::new(Orientation::Horizontal, 0);
        let group_icon = Image::from_icon_name("view-grid-symbolic");
        group_bx.append(&group_icon);
        let combo_group = ComboBoxText::new();
        combo_group.set_hexpand(true);
        group_bx.append(&combo_group);

        // bool_and/bool_or
        for it in GROUP_OPS {
            combo_group.append(Some(it), it);
        }
        combo_group.set_active_id(Some("No aggregate"));

        top_bx.append(&top_bx_upper);
        top_bx.append(&top_bx_lower);

        bx.append(&top_bx);
        bx.append(&middle_stack);
        bx.append(&bottom_bx);

        bottom_bx.set_margin_top(18);
        bottom_bx.set_margin_bottom(18);

        let scroll = ScrolledWindow::new();
        scroll.set_child(Some(&bx));
        win.set_child(Some(&scroll));

        entry_col.set_hexpand(true);
        let add_btn = Button::from_icon_name("list-add-symbolic");
        let delete_btn = Button::from_icon_name("user-trash-symbolic");
        add_btn.style_context().add_class("flat");
        delete_btn.style_context().add_class("flat");
        add_btn.set_halign(Align::End);
        delete_btn.set_halign(Align::End);
        delete_btn.set_sensitive(false);
        add_btn.set_sensitive(false);

        let tbl_img = Image::from_icon_name("table-symbolic");
        tbl_img.set_margin_end(6);
        top_bx_upper.append(&tbl_img);
        top_bx_upper.append(&entry_col);

        let entry_filter = Entry::new();
        entry_filter.set_placeholder_text(Some("Filter argument"));

        let filter_combo = ComboBoxText::new();
        for op in FILTER_OPS.iter() {
            filter_combo.append(Some(op), op);
        }
        filter_combo.set_active_id(Some("No filter"));
        let entry_filter_c = entry_filter.clone();
        filter_combo.connect_changed(move|combo| {
            if combo.active_id() == Some(glib::GString::from("No filter")) {
                entry_filter_c.set_text("");
            }
        });

        let filter_bx = Box::new(Orientation::Horizontal, 0);
        filter_bx.style_context().add_class("linked");
        let filt_img = Image::from_icon_name("funnel-symbolic");
        filt_img.set_margin_end(6);
        filter_bx.append(&filt_img);
        filter_bx.append(&filter_combo);
        filter_bx.append(&entry_filter);
        entry_filter.set_hexpand(true);
        filter_bx.set_hexpand(true);
        top_bx_lower.append(&filter_bx);
        top_bx_lower.append(&group_bx);
        top_bx_lower.append(&sort_bx);

        let join_combo = ComboBoxText::new();
        for op in JOIN_OPS.iter() {
            join_combo.append(Some(op), op);
        }
        join_combo.set_active_id(Some("No joins"));

        let join_bx = Box::new(Orientation::Horizontal, 0);
        join_bx.set_hexpand(true);
        let entry_join = Entry::new();
        entry_join.set_placeholder_text(Some("Join column"));
        let join_img = Image::from_icon_name("inner-symbolic");
        join_img.set_margin_start(18);
        join_img.set_margin_end(6);

        let entry_join_c = entry_join.clone();
        join_combo.connect_changed(move|combo| {
            if combo.active_id() == Some(glib::GString::from("No joins")) {
                entry_join_c.set_text("");
            }
        });

        join_bx.append(&join_img);
        join_bx.style_context().add_class("linked");
        join_bx.append(&join_combo);
        join_bx.append(&entry_join);
        entry_join.set_hexpand(true);
        top_bx_upper.append(&join_bx);

        top_bx_upper.append(&add_btn);
        top_bx_upper.append(&delete_btn);

        // let tbl_bx = TableBox::build();
        // let tbl_bx2 = TableBox::build();
        // let join_bx = JoinBox::new("Column1 : Column 2", tbl_bx.clone(), tbl_bx2.clone());
        // middle_bx.append(&join_bx.outer);

        // add_toggle(&toggles, "Column1", &tbl_bx);
        // add_toggle(&toggles, "Column2", &tbl_bx);
        // add_toggle(&toggles, "Column3", &tbl_bx2);

        let qw = Self {
            win,
            btn_clear,
            btn_sql,
            btn_run,
            toggles,
            boxes,
            objs : Rc::new(RefCell::new(Vec::new())),
            cols_model : Rc::new(RefCell::new(None)),
            entry_col,
            entry_filter,
            entry_join,
            join_combo,
            filter_combo,
            add_btn,
            delete_btn,
            middle_bx,
            combo_group,
            combo_sort,
            query : query.share()
        };

        qw.add_btn.connect_clicked({
            let (entry_col, entry_filter, entry_join, join_combo, filter_combo, middle_bx, combo_group, combo_sort) = (
                &qw.entry_col,
                &qw.entry_filter,
                &qw.entry_join,
                &qw.join_combo,
                &qw.filter_combo,
                &qw.middle_bx,
                &qw.combo_group,
                &qw.combo_sort
            ).cloned();
            // let (toggles, boxes) = (&qw.toggles, &qw.boxes).cloned();
            let query = query.share();
            // let query_tx = query_tx.clone();
            move|_| {
                let Some((tbl, colname)) = split_tbl_col(entry_col.text().as_ref()) else { return };
                let filter_arg = entry_filter.text().to_string();
                let join_arg = entry_join.text().to_string();
                let join = join_combo.active_text()
                    .and_then(|txt| JoinOp::from_str(txt.as_ref()) )
                    .and_then(|op| split_tbl_col(join_arg.as_ref())
                        .map(|arg| Join {
                            op,
                            src_table : tbl.clone(),
                            dst_table : arg.0,
                            src_col : colname.to_string(),
                            dst_col : arg.1
                        })
                    );
                let filter = filter_combo.active_text()
                    .and_then(|txt| FilterOp::from_str(txt.as_ref()) )
                    .and_then(|op| if filter_arg.is_empty() { None } else { Some(Filter { op, arg : filter_arg }) } );
                let group = combo_group.active_text()
                    .and_then(|txt| GroupOp::from_str(txt.as_ref()) );
                let sort = combo_sort.active_text()
                    .and_then(|txt| SortOp::from_str(txt.as_ref()) );
                // query_tx.send(QueryMsg::Add(tbl.clone(), Column { name : colname, join, filter, group, sort }));
                // let tbl = tbl.to_string();

                // let mut query = query.try_borrow_mut().unwrap();
                // query.add(&tbl, Column { name : colname, join, filter, group, sort });
                // update_ui_with_query(&middle_bx, &toggles, &boxes, &query, &entry_col);
                query.update(QueryMsg::Add(tbl.to_string(), Column { name : colname, join, filter, group, sort }));

                clear_fields(&entry_col, &entry_join, &entry_filter, &join_combo, &filter_combo, &combo_group, &combo_sort);
            }
        });

        qw.join_combo.connect_changed({
            let entry_col = qw.entry_col.clone();
            let entry_join = qw.entry_join.clone();
            let add_btn = qw.add_btn.clone();
            let query = query.share();
            move |join_combo| {
                let can_add = verify_can_add(&query.view(), &entry_col, &entry_join, &join_combo);
                add_btn.set_sensitive(can_add);
            }
        });
        qw.entry_join.connect_changed({
            let entry_col = qw.entry_col.clone();
            let add_btn = qw.add_btn.clone();
            let join_combo = qw.join_combo.clone();
            let query = query.share();
            move |entry_join| {
                let can_add = verify_can_add(&query.view(), &entry_col, &entry_join, &join_combo);
                add_btn.set_sensitive(can_add);
            }
        });

        qw.entry_col.connect_changed({
            let (add_btn, delete_btn, toggles, entry_filter, entry_join, filter_combo, join_combo, combo_group, combo_sort) = (
                &qw.add_btn,
                &qw.delete_btn,
                &qw.toggles,
                &qw.entry_filter,
                &qw.entry_join,
                &qw.filter_combo,
                &qw.join_combo,
                &qw.combo_group,
                &qw.combo_sort
            ).cloned();
            let query = query.share();
            move |entry_col| {
                let query = query.view();
                let txt = entry_col.text().to_string();
                if let Some((tblname, colname)) = split_tbl_col(&txt) {
                    let opt_col = query.column(&tblname, &colname);
                    if let Some(col) = &opt_col {
                        if let Some(filt) = &col.filter {
                            entry_filter.set_text(&filt.arg);
                            filter_combo.set_active_id(Some(filt.op.combo_str()));
                        } else {
                            entry_filter.set_text("");
                            filter_combo.set_active_id(Some("No filter"));
                        }

                        if let Some(group) = &col.group {
                            combo_group.set_active_id(Some(group.combo_str()));
                        } else {
                            combo_group.set_active_id(Some("No aggregate"));
                        }

                        if let Some(sort) = &col.sort {
                            combo_sort.set_active_id(Some(sort.combo_str()));
                        } else {
                            combo_sort.set_active_id(Some("Unsorted"));
                        }

                        entry_filter.set_sensitive(false);
                        filter_combo.set_sensitive(false);
                        combo_group.set_sensitive(false);
                        combo_sort.set_sensitive(false);
                        if let Some(join) = &col.join {
                            entry_join.set_text(&format!("{}.{}", join.dst_table, join.dst_col));
                            join_combo.set_active_id(Some(join.op.combo_str()));
                        } else {
                            entry_join.set_text("");
                            join_combo.set_active_id(Some("No joins"));
                        }
                        entry_join.set_sensitive(false);
                        join_combo.set_sensitive(false);
                    } else {
                        entry_filter.set_sensitive(true);
                        filter_combo.set_sensitive(true);
                        entry_join.set_sensitive(true);
                        join_combo.set_sensitive(true);
                        combo_group.set_sensitive(true);
                        combo_sort.set_sensitive(true);
                    }

                    let can_add = verify_can_add(&query, &entry_col, &entry_join, &join_combo);

                    // Can only add column when
                    // (1) There aren't any columns (inaugurating first table)
                    // (2) The column belongs to an already-existing last table (this prevents
                    // adding tables that aren't part of a join). To add new tables, the user
                    // must always use them as argument to a join clause with the previous table.

                    // let joins_last = query.last().map(|last| &last.name == &tblname ).unwrap_or(false);
                    add_btn.set_sensitive(can_add);
                    delete_btn.set_sensitive(opt_col.is_some());

                } else {
                    add_btn.set_sensitive(false);
                    delete_btn.set_sensitive(false);
                }

                let toggles = toggles.borrow();
                if let Some(toggle) = toggles.get(&txt) {
                    toggle.set_active(true);
                } else {
                    toggles.values().for_each(|t| t.set_active(false) );
                }
            }
        });

        qw.delete_btn.connect_clicked({
            // let toggles = qw.toggles.clone();
            // let boxes = qw.boxes.clone();
            // let middle_bx = qw.middle_bx.clone();
            let entry_col = qw.entry_col.clone();
            let entry_join = qw.entry_join.clone();
            let entry_filter = qw.entry_filter.clone();
            let join_combo = qw.join_combo.clone();
            let filter_combo = qw.filter_combo.clone();
            let combo_group = qw.combo_group.clone();
            let combo_sort = qw.combo_sort.clone();
            move |_| {
                let col_txt = entry_col.text().to_string();
                query.update(QueryMsg::Delete(col_txt.to_string()));
                clear_fields(&entry_col, &entry_join, &entry_filter, &join_combo, &filter_combo, &combo_group, &combo_sort);
            }
        });
        qw
    }

}

fn clear_fields(
    entry_col : &Entry,
    entry_join : &Entry,
    entry_filter : &Entry,
    join_combo : &ComboBoxText,
    filter_combo : &ComboBoxText,
    group_combo : &ComboBoxText,
    sort_combo : &ComboBoxText
) {
    entry_col.set_text("");
    entry_join.set_text("");
    entry_filter.set_text("");
    join_combo.set_active_id(Some("No joins"));
    filter_combo.set_active_id(Some("No filter"));
    group_combo.set_active_id(Some("No aggregates"));
    sort_combo.set_active_id(Some("Unsorted"));
}

/*pub enum AddError {
    NoColumn,
    MissingJoin
}*/

fn verify_can_add(query : &Query, entry_col : &Entry, entry_join : &Entry, join_combo : &ComboBoxText) -> bool {
    if let Some((tblname, colname)) = split_tbl_col(entry_col.text().as_ref()) {
        let has_col = query.column(&tblname, &colname).is_some();
        let has_tbl = query.table(&tblname).is_some();
        let join_ok = validate_join(&entry_col, &entry_join, join_combo);
        // println!("has col = {:?} has tbl = {:?}", has_col, has_tbl);
        !has_col && join_ok && (query.0.is_empty() || has_tbl )
    } else {
        false
    }
}

fn validate_join(entry_col : &Entry, entry_join : &Entry, join_combo : &ComboBoxText) -> bool {
    let has_join = join_combo.active_text().and_then(|txt| JoinOp::from_str(txt.as_ref()) ).is_some();
    if !has_join && entry_join.text().is_empty() {
        true
    } else {
        if let Some((lhs_name, _)) = split_tbl_col(entry_col.text().as_ref()) {
            if let Some((rhs_name, _)) = split_tbl_col(entry_join.text().as_ref()) {
                has_join && lhs_name != rhs_name
            } else {
                false
            }
        } else {
            false
        }
    }
}

fn clear(bx : &Box, toggles : &Rc<RefCell<BTreeMap<String, ToggleButton>>>, boxes : &Rc<RefCell<Vec<Box>>>) {
    let mut boxes = boxes.borrow_mut();
    let mut toggles = toggles.borrow_mut();
    toggles.clear();
    for child_bx in &*boxes {
        bx.remove(child_bx);
    }
    boxes.clear();
}

fn update_ui_with_query(
    bx : &Box,
    toggles : &Rc<RefCell<BTreeMap<String, ToggleButton>>>,
    boxes : &Rc<RefCell<Vec<Box>>>,
    query : &Query,
    entry_col : &Entry
) {
    clear(bx, toggles, boxes);
    for tbl in &query.0 {
        let (tbl_bx, _) = add_join_or_table(&mut Vec::new(), &mut Vec::new(), tbl, toggles);
        bx.append(&tbl_bx);
        boxes.borrow_mut().push(tbl_bx);
    }

    let toggles = toggles.borrow();
    for (colname, toggle) in toggles.iter() {
        let entry_col = entry_col.clone();
        let colname = colname.clone();
        toggle.connect_toggled(move |btn| {
            if btn.is_active() {
                if &entry_col.text().to_string()[..] != &colname[..] {
                    entry_col.set_text(&colname);
                }
            }
        });
    }
}

// Keep ancestral box at first entry, inner boxes at second.
fn add_join_or_table(
    mut prev_join : &mut Vec<Join>,
    mut prev_tbl : &mut Vec<Table>,
    tbl : &Table,
    toggles : &Rc<RefCell<BTreeMap<String, ToggleButton>>>
) -> (Box, Box) {
    if let Some(join_rhs) = &tbl.join_rhs {
        prev_join.push(tbl.join().unwrap());
        prev_tbl.push(tbl.clone());
        let (ancestor, j_bx) = add_join_or_table(prev_join, prev_tbl, &join_rhs, toggles);
        match (prev_tbl.pop(), prev_join.pop()) {
            (Some(prev_tbl), Some(prev_join)) => {
                let j_bx_new = JoinBox::new(&prev_join);
                j_bx_new.inner.append(&TableBox::build(&prev_tbl, toggles).bx);
                // j_bx_new.inner.append(&TableBox::build(&tbl, toggles).bx);
                j_bx.prepend(&j_bx_new.outer);
                (ancestor, j_bx_new.inner)
            },
            (Some(prev_tbl), _) => {
                j_bx.prepend(&TableBox::build(&prev_tbl, toggles).bx);
                (ancestor, j_bx)
            },
            _ => {
                panic!()
            }
        }
    } else {
        if let Some(prev_join) = prev_join.pop() {
            let j_bx = JoinBox::new(&prev_join);
            let right_tbl = TableBox::build(&tbl, toggles);
            j_bx.inner.append(&right_tbl.bx);
            (j_bx.outer.clone(), j_bx.inner.clone())
        } else {
            let tbl_bx = TableBox::build(&tbl, toggles);
            (tbl_bx.bx.clone(), tbl_bx.bx.clone())
        }
    }
}

fn split_tbl_col(s : &str) -> Option<(String, String)> {
    let mut split : Vec<_> = s.split(".").collect();
    if split.iter().any(|s| s.is_empty() ) || split.len() < 2 {
        return None;
    }
    let col = split.pop()?.to_string();
    let tbl : String = split.join(".");
    Some((tbl, col))
}

#[derive(Clone, Default, Debug)]
pub struct Query(Vec<Table>);

impl Exclusive for QueryMsg { }

#[derive(Clone)]
pub enum QueryMsg {
    Add(String, Column),
    Delete(String),
    Clear
}

fn column_recursive(tbl : &Table, tblname : &str, colname : &str) -> Option<Column> {
    if tbl.name == tblname {
        if let Some(col) = tbl.cols.iter().find_map(|c| if &c.name == colname { Some(c.clone()) } else { None } ) {
            return Some(col);
        }
    }

    if let Some(rhs) = &tbl.join_rhs {
        column_recursive(rhs, tblname, colname)
    } else {
        None
    }
}

fn table_recursive(tbl : &Table, tblname : &str) -> Option<Table> {
    if &tbl.name == tblname {
        Some(tbl.clone())
    } else if let Some(rhs) = &tbl.join_rhs {
        table_recursive(rhs, tblname)
    } else {
        None
    }
}

fn table_recursive_mut<'a>(tbl : &'a mut Table, tblname : &str) -> Option<&'a mut Table> {
    if &tbl.name == tblname {
        Some(tbl)
    } else if let Some(rhs) = &mut tbl.join_rhs {
        table_recursive_mut(rhs, tblname)
    } else {
        None
    }
}

/*fn column_recursive_mut<'a>(tbl : &'a mut Table, tblname : &str, colname : &str) -> Option<&'a mut Column> {
    if let Some(rhs) = &mut tbl.join_rhs {
        column_recursive_mut(rhs, tblname, colname)
    } else {
        if tbl.name == tblname {
            tbl.cols.iter_mut().find_map(|c| if &c.name == colname { Some(c) } else { None } )
        } else {
            None
        }
    }
}*/

/*fn next_table(tbl : &Table, dst : &mut Vec<(Table, Join)>) -> Table {
    if let Some(rhs) = &tbl.join_rhs {
        dst.push((*tbl.clone(), tbl.join().unwrap()));
        next_table(rhs, dst)
    } else {
        tbl.clone()
    }
}*/

fn clear_no_cols_recursive(tbl : &mut Table) {
    if let Some(rhs) = &mut tbl.join_rhs {
        if rhs.cols.len() == 0 {
            tbl.join_rhs = None;
        } else {
            clear_no_cols_recursive(rhs)
        }
    }
}

impl Reactive for Query {

    type Message = QueryMsg;

    fn react(&mut self, msg : QueryMsg) {
        match msg {
            QueryMsg::Add(tblname, col) => self.add(&tblname, col),
            QueryMsg::Delete(colname) => { self.delete(&colname); },
            QueryMsg::Clear => {
                self.0.clear();
            }
        }
    }

}

impl Query {

    pub fn sql(&self) -> Option<String> {
        Some(self.0.get(0)?.sql())
    }

    pub fn table_mut(&mut self, tblname : &str) -> Option<&mut Table> {
        self.0.iter_mut().find_map(|tbl| table_recursive_mut(tbl, tblname) )
    }

    pub fn table(&self, tblname : &str) -> Option<Table> {
        self.0.iter().find_map(|tbl| table_recursive(tbl, tblname) )
    }

    pub fn column(&self, tblname : &str, colname : &str) -> Option<Column> {
        self.0.iter().find_map(|tbl| column_recursive(tbl, tblname, colname) )
    }

    // pub fn column_mut(&mut self, tblname : &str, colname : &str) -> Option<&mut Column> {
    //    self.0.iter_mut().find_map(|tbl| column_recursive_mut(tbl, tblname, colname) )
    // }

    pub fn clear_no_cols(&mut self) {
        for i in (0..self.0.len()).rev() {
            if self.0[i].cols.len() == 0 {
                self.0.remove(i);
            } else {
                clear_no_cols_recursive(&mut self.0[i]);
            }
        }
    }

    pub fn delete(&mut self, tbl_and_col : &str) -> bool {
        if let Some((tblname, colname)) = split_tbl_col(tbl_and_col) {
            if let Some(tbl) = self.table_mut(&tblname) {
                if let Some(pos) = tbl.cols.iter().position(|col| &col.name[..] == &colname[..] ) {
                    if tbl.cols[pos].join.is_some() {
                        tbl.join_rhs = None;
                    }
                    tbl.cols.remove(pos);
                    self.clear_no_cols();
                    true
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        }
    }

    pub fn add(&mut self, tbl_name : &str, col : Column) {
        for tbl in &mut self.0 {
            if search_recursively_and_update(tbl, tbl_name, col.clone()) {
                return;
            }
        }

        // Only when table not found and early return not executed.
        let mut new_tbl = Table { name : tbl_name.to_string(), cols : vec![col.clone()], join_rhs : None };
        add_join_if_any(&mut new_tbl, &col);
        self.0.push(new_tbl);

        /*if let Some(join) = &col.join {
            let joined_prev = if let Some(prev_col) = self.column_mut(&join.dst_table, &join.dst_col) {
                prev_col.join = join.clone();
                true
            } else {
                false
            };
            if joined_prev {
                let mut tbl = self.table_mut(&join.src_tbl).unwrap();
                tbl.rhs = new_tbl;
            } else {
                add_join_if_any(&mut new_tbl, &col);
                self.0.push(new_tbl);
            }
        } else {
            self.0.push(new_tbl);
        }*/
    }

}

fn search_recursively_and_update(tbl : &mut Table, tbl_name : &str, col : Column) -> bool {
    if &tbl.name[..] == tbl_name {
        update_table(tbl, col);
        true
    } else {
        if let Some(join_rhs) = &mut tbl.join_rhs {
            search_recursively_and_update(join_rhs, tbl_name, col)
        } else {
            false
        }
    }
}

fn add_join_if_any(tbl : &mut Table, col : &Column) {
    if let Some(join) = &col.join {
        tbl.join_rhs = Some(std::boxed::Box::new(Table {
            name : join.dst_table.clone(),
            // cols : vec![Column { name : join.dst_col.clone(), filter : None, join : None }],
            cols : Vec::new(),
            join_rhs : None
        }));
    }
}

fn update_table(tbl : &mut Table, col : Column) {
    add_join_if_any(tbl, &col);
    tbl.cols.push(col);
}

fn add_toggle(toggles : &Rc<RefCell<BTreeMap<String, ToggleButton>>>, table : &str, label : &str, tbl_bx : &TableBox) {
    let toggle = ToggleButton::new();
    let mut toggles = toggles.borrow_mut();
    if let Some(prev_toggle) = toggles.values().next() {
        toggle.set_group(Some(prev_toggle));
    }
    let colname = format!("{}.{}", table, label);
    toggles.insert(colname, toggle.clone());
    toggle.style_context().add_class("flat");
    toggle.set_label(label);
    toggle.set_hexpand(true);
    tbl_bx.toggle_bx.append(&toggle);
}

impl React<MainMenu> for QueryBuilderWindow {

    fn react(&self, menu : &MainMenu) {
        let win = self.win.clone();
        menu.action_builder.connect_activate(move|_,_| {
            win.show();
        });
    }

}

impl React<ActiveConnection> for QueryBuilderWindow {

    fn react(&self, conn : &ActiveConnection) {
        conn.connect_db_connected({
            let objs = self.objs.clone();
            let cols_model = self.cols_model.clone();
            let entry_col = self.entry_col.clone();
            let entry_join = self.entry_join.clone();
            move |(_, info)| {
                if let Some(info) = info {
                    super::update_completion_with_schema(objs.clone(), cols_model.clone(), Some(info.schema));
                    if let Some(model) = &*cols_model.borrow() {
                        super::add_completion(&entry_col, model);
                        super::add_completion(&entry_join, model);
                    } else {
                        entry_col.set_completion(None);
                        entry_join.set_completion(None);
                    }
                }
            }
        });
        conn.connect_schema_update({
            let objs = self.objs.clone();
            let cols_model = self.cols_model.clone();
            let entry_col = self.entry_col.clone();
            let entry_join = self.entry_join.clone();
            move |schema| {
                super::update_completion_with_schema(objs.clone(), cols_model.clone(), schema);
                if let Some(model) = &*cols_model.borrow() {
                    super::add_completion(&entry_col, model);
                    super::add_completion(&entry_join, model);
                } else {
                    entry_col.set_completion(None);
                    entry_join.set_completion(None);
                }
            }
        });
    }

}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum FilterOp {
    Eq,
    Greater,
    GreaterEq,
    Less,
    LessEq,
    Like
}

impl FilterOp {

    pub fn combo_str(&self) -> &str {
        match self {
            FilterOp::Eq => "= (Equal)",
            FilterOp::Greater => "> (Greater)",
            FilterOp::GreaterEq =>"≥ (Greater/equal)",
            FilterOp::Less =>"< (Less)",
            FilterOp::LessEq => "≤ (Less/equal)",
            FilterOp::Like => "like (Pattern match)"
        }
    }

    pub fn sql(&self) -> &str {
        match self {
            FilterOp::Eq => "=",
            FilterOp::Greater => ">",
            FilterOp::GreaterEq =>">=",
            FilterOp::Less =>"<",
            FilterOp::LessEq => "<=",
            FilterOp::Like => "like"
        }
    }

    pub fn from_str(s : &str) -> Option<Self> {
        match s {
            "= (Equal)" => Some(FilterOp::Eq),
            "> (Greater)" => Some(FilterOp::Greater),
            "≥ (Greater/equal)" => Some(FilterOp::GreaterEq),
            "< (Less)" => Some(FilterOp::Less),
            "≤ (Less/equal)" => Some(FilterOp::LessEq),
            "like (Pattern match)" => Some(FilterOp::Like),
            "No filter" | _ => None,
        }
    }

}

#[derive(Clone, Debug)]
pub struct Filter {
    op : FilterOp,
    arg : String
}

impl Filter {

    pub fn sql(&self) -> String {
        format!("{} {}", self.op.sql(), self.arg)
    }

}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum JoinOp {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter
}

impl JoinOp {

    pub fn sql_keyword(&self) -> &str {
        match self {
            JoinOp::Inner => "INNER JOIN",
            JoinOp::LeftOuter => "LEFT JOIN",
            JoinOp::RightOuter => "RIGHT JOIN",
            JoinOp::FullOuter => "FULL JOIN"
        }
    }

    pub fn description(&self) -> &'static str {
        match self {
            JoinOp::Inner => "Inner",
            JoinOp::LeftOuter => "Left",
            JoinOp::RightOuter => "Right",
            JoinOp::FullOuter => "Full"
        }
    }

    pub fn combo_str(&self) -> &'static str {
        match self {
            JoinOp::Inner => "Inner join",
            JoinOp::LeftOuter => "Left outer join",
            JoinOp::RightOuter => "Right outer join",
            JoinOp::FullOuter => "Full outer join"
        }
    }

    pub fn from_str(s : &str) -> Option<Self> {
        match s {
            "Inner join" => Some(Self::Inner),
            "Left outer join" => Some(Self::LeftOuter),
            "Right outer join" => Some(Self::RightOuter),
            "Full outer join" => Some(Self::FullOuter),
            "No joins" | _ => None
        }
    }

}

#[derive(Clone, Debug)]
pub struct Join {
    op : JoinOp,
    src_table : String,
    src_col : String,
    dst_col : String,
    dst_table : String
}

#[derive(Clone, Debug)]
pub struct Column {
    name : String,
    join : Option<Join>,
    filter : Option<Filter>,
    group : Option<GroupOp>,
    sort : Option<SortOp>
}

impl Column {

    pub fn sql(&self, tblname : &str) -> String {
        match self.group {
            Some(GroupOp::Count) => format!("count({}.{})", tblname, self.name),
            Some(GroupOp::Sum) => format!("sum({}.{})", tblname, self.name),
            Some(GroupOp::Avg) => format!("avg({}.{})", tblname, self.name),
            Some(GroupOp::Min) => format!("min({}.{})", tblname, self.name),
            Some(GroupOp::Max) => format!("max({}.{})", tblname, self.name),
            Some(GroupOp::Every) => format!("bool_and({}.{})", tblname, self.name),
            Some(GroupOp::Any) => format!("bool_or({}.{})", tblname, self.name),
            Some(GroupOp::GroupBy) | None => format!("{}.{}", tblname, self.name)
        }
    }

}

#[derive(Clone, Default, Debug)]
pub struct Table {
    name : String,
    join_rhs : Option<std::boxed::Box<Table>>,
    cols : Vec<Column>
}

fn filter_clause_recursive(s : &mut Vec<String>, tbl : &Table) {
    for c in &tbl.cols {
        if let Some(filt) = &c.filter {
            s.push(format!("{} {}", c.name, filt.sql()));
        }
    }
    if let Some(rhs_tbl) = &tbl.join_rhs {
        filter_clause_recursive(s, rhs_tbl);
    }
}

fn sort_clause_recursive(s : &mut Vec<String>, tbl : &Table) {
    for c in &tbl.cols {
        if let Some(sort) = &c.sort {
            if *sort == SortOp::Ascending {
                s.push(c.name.clone());
            } else {
                s.push(format!("{} DESC", c.name.clone()));
            }
        }
    }
    if let Some(rhs_tbl) = &tbl.join_rhs {
        sort_clause_recursive(s, rhs_tbl);
    }
}

fn group_clause_recursive(s : &mut Vec<String>, tbl : &Table) {
    for c in &tbl.cols {
        if let Some(group) = &c.group {
            if *group == GroupOp::GroupBy {
                s.push(c.name.clone());
            }
        }
    }
    if let Some(rhs_tbl) = &tbl.join_rhs {
        group_clause_recursive(s, rhs_tbl);
    }
}

fn from_clause_recursive(s : &mut Vec<String>, tbl : &Table) {
    if let (Some(rhs_tbl), Some(join)) = (&tbl.join_rhs, tbl.join()) {
        s.push(format!(" {} {} ON {} = {} ", join.op.sql_keyword(), rhs_tbl.name, join.src_col, join.dst_col));
    }
}

fn columns_recursive(s : &mut Vec<String>, tbl : &Table) {
    for col in &tbl.cols {
        s.push(col.sql(&tbl.name));
    }
    if let Some(rhs) = &tbl.join_rhs {
        columns_recursive(s, rhs);
    }
}

impl Table {

    pub fn sql(&self) -> String {
        let mut s = format!("SELECT {}{}", self.body(), self.from_clause());
        if let Some(filter) = self.filter_clause() {
            s += "\n";
            s += &filter;
        }
        if let Some(group) = self.group_clause() {
            s += "\n";
            s += &group;
        }
        if let Some(sort) = self.sort_clause() {
            s += "\n";
            s += &sort;
        }
        s += ";";
        s
    }

    pub fn body(&self) -> String {
        let mut cols = Vec::new();
        columns_recursive(&mut cols, self);
        let mut s : String = cols.join(",\n    ");
        s += "\n";
        s
    }

    pub fn filter_clause(&self) -> Option<String> {
        let mut filter = Vec::new();
        filter_clause_recursive(&mut filter, self);
        if filter.is_empty() {
            None
        } else {
            Some(format!("WHERE {}", filter.join(" AND\n")))
        }
    }

    pub fn group_clause(&self) -> Option<String> {
        let mut group = Vec::new();
        group_clause_recursive(&mut group, self);
        if group.is_empty() {
            None
        } else {
            Some(format!("GROUP BY {}", group.join(", ")))
        }
    }

    pub fn sort_clause(&self) -> Option<String> {
        let mut sort = Vec::new();
        sort_clause_recursive(&mut sort, self);
        if sort.is_empty() {
            None
        } else {
            Some(format!("ORDER BY {}", sort.join(", ")))
        }
    }

    pub fn from_clause(&self) -> String {
        let mut s = Vec::new();;
        from_clause_recursive(&mut s, self);
        if s.len() >= 1 {
            format!("FROM {} {}", self.name, s.join("\n"))
        } else {
            format!("FROM {}", self.name)
        }
    }

    // TODO return multiple joins.
    fn join(&self) -> Option<Join> {
        self.cols.iter().find_map(|c| c.join.clone() )
    }

}


