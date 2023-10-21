/*Copyright (c) 2023 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.
For a copy, see http://www.gnu.org/licenses.*/

use gtk4::prelude::*;
use gtk4::*;
use libadwaita;
use crate::client::ActiveConnection;
use stateful::React;
use crate::client::Environment;
use crate::sql::StatementOutput;
use crate::client::OpenedScripts;
use crate::sql::object::{DBType, DBColumn};
use core::cell::RefCell;
use std::rc::Rc;
use filecase::MultiArchiverImpl;
use crate::client::SharedUserState;
use crate::client::Engine;
use filecase::OpenDialog;
use super::table::TableWidget;
use crate::tables::table::Table;
use crate::tables::column::Column;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub enum ColDefault {
    NoDefault,
    AutoIncr,
    Value(String)
}

#[derive(Debug, Clone)]
pub enum Uniqueness {
    Null,
    NotNull,
    Unique,
    NotNullUnique,
    PrimaryKey
}

impl FromStr for Uniqueness {

    type Err = ();

    fn from_str(s : &str) -> Result<Uniqueness,Self::Err> {
        match s {
            "Null" => Ok(Uniqueness::Null),
            "Not null" => Ok(Uniqueness::NotNull),
            "Unique" => Ok(Uniqueness::Unique),
            "Unique and not null" => Ok(Uniqueness::NotNullUnique),
            "Primary key" => Ok(Uniqueness::PrimaryKey),
            _ => Err(())
        }
    }

}

#[derive(Debug, Default, Clone)]
pub struct CreateCommand {

    cols : Vec<DBColumn>,

    defaults : Vec<ColDefault>,

    uniqueness : Vec<Uniqueness>,

    references : Vec<String>,

    tbl : Option<Table>,

}

impl CreateCommand {

    pub fn clear(&mut self) {
        *self = Self::default();
    }

    pub fn append(&mut self) {
        self.cols.push(DBColumn { name : String::new(), ty : DBType::Text, is_pk : false });
        self.defaults.push(ColDefault::NoDefault);
        self.uniqueness.push(Uniqueness::Null);
        self.references.push(String::new());
    }

    pub fn remove(&mut self, ix : usize) {
        self.cols.remove(ix);
        self.defaults.remove(ix);
        self.uniqueness.remove(ix);
        self.references.remove(ix);
    }

    pub fn sql(&self, name : &str) -> Result<String, std::boxed::Box<std::error::Error>> {
        if let Some(tbl) = &self.tbl {
            let creation = self.sql_creation(name)?;
            if let Some(insertion) = tbl.sql_table_insertion(name, &tbl.view_names()[..], false)? {
                Ok(format!("BEGIN;\n{creation}\n{insertion};\nCOMMIT;"))
            } else {
                Ok(creation)
            }
        } else {
            self.sql_creation(name)
        }
    }

    fn sql_creation(&self, tbl_name : &str) -> Result<String, std::boxed::Box<std::error::Error>> {
        let mut sql = format!("CREATE TABLE {}(", tbl_name);
        for (i, DBColumn { name, ty, .. }) in self.cols.iter().enumerate() {
            let name = match name.chars().find(|c| *c == ' ') {
                Some(_) => String::from("\"") + &name[..] + "\"",
                None => name.clone()
            };
            let ty_name = match (&self.defaults[i], ty) {
                (ColDefault::AutoIncr, DBType::I16) => "smallserial",
                (ColDefault::AutoIncr, DBType::I32) => "serial",
                (ColDefault::AutoIncr, DBType::I64) => "bigserial",
                _ => ty.name()
            };
            sql += &format!("\t{} {}", name, ty_name);
            match self.uniqueness[i] {
                Uniqueness::Null => { },
                Uniqueness::NotNull => { sql += " NOT NULL" },
                Uniqueness::Unique => { sql += " UNIQUE"},
                Uniqueness::NotNullUnique => { sql += " UNIQUE NOT NULL" },
                Uniqueness::PrimaryKey => { sql += " PRIMARY KEY" }
            }
            match &self.defaults[i] {
                ColDefault::NoDefault | ColDefault::AutoIncr => { },
                ColDefault::Value(val) => {
                    let lit = if ty.requires_quotes() {
                        format!("'{val}'")
                    } else {
                        val.clone()
                    };
                    sql += &format!(" DEFAULT {lit}");
                }
            }
            if !self.references[i].is_empty() {
                sql += &format!(" REFERENCES {}", self.references[i]);
            }
            if i < self.cols.len() - 1 {
                sql += ",\n"
            } else {
                sql += "\n);\n"
            }
        }
        Ok(sql)
    }

}

#[derive(Clone, Debug)]
pub struct CreateDialog {
    pub dialog : Dialog,
    open_dialog : OpenDialog,
    pub tbl_entry : Entry,
    pub curr_tbl : Rc<RefCell<CreateCommand>>,
    pub btn_sql : Button,
    pub btn_create : Button,
    pub overlay : libadwaita::ToastOverlay
}

impl CreateDialog {

    pub fn new() -> Self {
        let dialog = Dialog::new();
        dialog.set_title(Some("Create table"));
        dialog.set_width_request(1200);
        dialog.set_height_request(800);
        let open_dialog = OpenDialog::build(&["*.csv"]);
        open_dialog.dialog.set_transient_for(Some(&dialog));
        super::configure_dialog(&dialog, true);
        let paned = Paned::new(Orientation::Vertical);

        let bx_outer = Box::new(Orientation::Vertical, 0);
        bx_outer.append(&paned);

        let overlay = libadwaita::ToastOverlay::new();
        overlay.set_child(Some(&bx_outer));
        dialog.set_child(Some(&overlay));

        let left_scroll = ScrolledWindow::new();
        let left_list = ListBox::new();
        left_list.set_vexpand(true);
        let right_bx = Box::new(Orientation::Vertical, 0);
        left_scroll.set_child(Some(&left_list));
        paned.set_start_child(Some(&left_scroll));
        paned.set_end_child(Some(&right_bx));
        paned.set_vexpand(true);
        paned.set_hexpand(true);
        paned.set_position(400);

        let header_left_bx = Box::new(Orientation::Horizontal, 0);
        let tbl_entry = Entry::new();
        tbl_entry.set_primary_icon_name(Some("table-symbolic"));
        tbl_entry.set_placeholder_text(Some("Table name"));
        tbl_entry.set_hexpand(true);
        let add_btn = Button::new();
        add_btn.set_icon_name("list-add-symbolic");
        header_left_bx.style_context().add_class("linked");

        header_left_bx.append(&tbl_entry);
        header_left_bx.append(&add_btn);

        let open_btn = Button::new();
        open_btn.set_icon_name("document-open-symbolic");
        let erase_btn = Button::new();
        erase_btn.style_context().add_class("pill");
        erase_btn.set_label("Clear");
        erase_btn.set_sensitive(false);
        header_left_bx.append(&open_btn);

        let btn_sql = Button::new();
        btn_sql.style_context().add_class("pill");
        btn_sql.set_label("Copy SQL");
        btn_sql.set_sensitive(false);

        let tw = TableWidget::new(1,1);
        tw.grid.set_visible(false);
        right_bx.append(&tw.bx);
        tw.bx.set_vexpand(true);

        let btn_bx = Box::new(Orientation::Horizontal, 18);
        let btn_create = Button::new();
        btn_create.set_label("Create");
        btn_create.style_context().add_class("pill");
        btn_create.style_context().add_class("suggested-action");
        btn_bx.append(&erase_btn);
        btn_bx.append(&btn_sql);
        btn_bx.append(&btn_create);
        btn_bx.set_hexpand(true);
        btn_bx.set_halign(Align::Center);
        bx_outer.append(&btn_bx);
        btn_bx.set_margin_top(24);
        btn_bx.set_margin_bottom(24);

        open_btn.connect_clicked({
            let open_dialog = open_dialog.clone();
            move |_| {
                open_dialog.dialog.show();
            }
        });

        let cols_bx = Box::new(Orientation::Vertical, 0);
        cols_bx.style_context().add_class("linked");
        cols_bx.set_vexpand(true);

        let curr_tbl = Rc::new(RefCell::new(CreateCommand::default()));
        erase_btn.connect_clicked({
            let curr_tbl = curr_tbl.clone();
            let cols_bx = cols_bx.clone();
            let tw = tw.clone();
            let add_btn = add_btn.clone();
            let tbl_entry = tbl_entry.clone();
            move |_| {
                curr_tbl.borrow_mut().clear();
                clear_box(&cols_bx);
                tw.grid.set_visible(false);
                add_btn.set_sensitive(true);
                tbl_entry.set_text("");
            }
        });

        add_btn.connect_clicked({
            let curr_tbl = curr_tbl.clone();
            let tbl_entry = tbl_entry.clone();
            let cols_bx = cols_bx.clone();
            let btn_sql = btn_sql.clone();
            let btn_create = btn_create.clone();
            move |_| {
                curr_tbl.borrow_mut().append();
                add_form_row(&cols_bx, None, None, &curr_tbl, &btn_sql, &btn_create);
                btn_sql.set_sensitive(true);
                btn_create.set_sensitive(true);
            }
        });

        tbl_entry.connect_changed({
            let curr_tbl = curr_tbl.clone();
            let btn_sql = btn_sql.clone();
            let btn_create = btn_create.clone();
            move |entry| {
                // curr_tbl.borrow_mut().name = entry.text().to_string();
                if entry.text().is_empty() {
                    btn_sql.set_sensitive(false);
                    btn_create.set_sensitive(false);
                }
            }
        });

        for w in [&header_left_bx, &cols_bx] {
            let row = ListBoxRow::new();
            row.set_child(Some(w));
            row.set_selectable(false);
            row.set_activatable(false);
            left_list.append(&row);
        }

        open_dialog.dialog.connect_response({
            let add_btn = add_btn.clone();
            let tw = tw.clone();
            let cols_bx = cols_bx.clone();
            let curr_tbl = curr_tbl.clone();
            let btn_sql = btn_sql.clone();
            let btn_create = btn_create.clone();
            let overlay = overlay.clone();
            let tbl_entry = tbl_entry.clone();
            move |dialog, resp| {
                match resp {
                    ResponseType::Accept => {
                        if let Some(path) = dialog.file().and_then(|f| f.path() ) {
                            if let Some(path) = path.to_str() {
                                if let Ok(b) = std::fs::read(path) {
                                    if let Ok(s) = String::from_utf8(b) {
                                        match Table::new_from_text(s) {
                                            Ok(tbl) => {
                                                tw.update_data(&tbl, None, Some(100), true, false);
                                                tw.grid.set_visible(true);
                                                add_btn.set_sensitive(false);
                                                erase_btn.set_sensitive(true);
                                                clear_box(&cols_bx);
                                                for (name, ty) in tbl.description() {
                                                    add_form_row(&cols_bx, Some(name), Some(ty), &curr_tbl, &btn_sql, &btn_create);
                                                }
                                                let mut tbl_inner = curr_tbl.borrow_mut();
                                                tbl_inner.clear();
                                                // tbl_inner.name = tbl_entry.text().to_string();
                                                for (name, ty) in tbl.description() {
                                                    tbl_inner.append();
                                                    *tbl_inner.cols.last_mut().unwrap() = DBColumn {
                                                        name : name.to_string(),
                                                        ty,
                                                        is_pk : false
                                                    };
                                                }
                                                tbl_inner.tbl = Some(tbl);
                                                btn_sql.set_sensitive(true);
                                                btn_create.set_sensitive(true);
                                            },
                                            Err(e) => {
                                                let toast = libadwaita::Toast::builder().title(e).build();
                                                overlay.add_toast(toast.clone());
                                            }
                                        }
                                    } else {
                                        let toast = libadwaita::Toast::builder().title("Invalid UTF-8").build();
                                        overlay.add_toast(toast.clone());
                                    }
                                } else {
                                    let toast = libadwaita::Toast::builder().title("Could not open file").build();
                                    overlay.add_toast(toast.clone());
                                }
                            }
                        }
                    },
                    _ => { }
                }
            }
        });
        Self { dialog, tbl_entry, open_dialog, curr_tbl, btn_create, btn_sql, overlay }
    }

}

fn clear_box(bx : &Box) {
    while let Some(child) = bx.first_child() {
        bx.remove(&child);
    }
}

const DB_TYPES : [&str;12] = [
    "text",
    "bool",
    "integer",
    "smallint",
    "bigint",
    "real",
    "dp",
    "numeric",
    "date",
    "time",
    "bytea",
    "json"
];

const UNIQUE : [&str;5] = [
    "Null",
    "Not null",
    "Unique",
    "Unique and not null",
    "Primary key",
];

const DEFAULT : [&str;3] = [
    "No default",
    "Auto-increment",
    "Custom value"
];

fn add_form_row(
    parent : &Box,
    name : Option<&str>,
    db_ty : Option<DBType>,
    curr_tbl : &Rc<RefCell<CreateCommand>>,
    btn_sql : &Button,
    btn_create : &Button
) {
    let entry = Entry::new();
    if let Some(name) = name {
        entry.set_text(name);
    }
    entry.set_hexpand(true);
    let ty_combo = ComboBoxText::new();
    for ty in DB_TYPES {
        ty_combo.append(Some(ty), ty);
    }
    let ty = db_ty.unwrap_or(DBType::Text);
    let icon_name = super::get_type_icon_name(&ty, libadwaita::StyleManager::default().is_dark());
    ty_combo.set_active_id(Some(ty.name()));
    entry.set_primary_icon_name(Some(icon_name));
    entry.connect_changed({
        let curr_tbl = curr_tbl.clone();
        let n = parent.observe_children().n_items();
        move |entry| {
            curr_tbl.borrow_mut().cols[n as usize].name = entry.text().to_string();
        }
    });
    ty_combo.connect_changed({
        let curr_tbl = curr_tbl.clone();
        let n = parent.observe_children().n_items();
        let entry = entry.clone();
        move |ty_combo| {
            if let Some(new_id) = ty_combo.active_id() {
                if let Ok(ty) = DBType::from_str(&new_id) {
                    curr_tbl.borrow_mut().cols[n as usize].ty = ty;
                    let icon_name = super::get_type_icon_name(&ty, libadwaita::StyleManager::default().is_dark());
                    entry.set_primary_icon_name(Some(icon_name));
                }
            }
        }
    });

    let bx = Box::new(Orientation::Horizontal, 0);
    bx.style_context().add_class("linked");
    bx.append(&entry);
    bx.append(&ty_combo);

    let unique_combo = ComboBoxText::new();
    for u in UNIQUE.iter() {
        unique_combo.append(Some(&u), &u);
    }
    bx.append(&unique_combo);
    unique_combo.set_active_id(Some("Null"));
    unique_combo.connect_changed({
        let curr_tbl = curr_tbl.clone();
        let n = parent.observe_children().n_items();
        move |unique_combo| {
            if let Some(id_str) = unique_combo.active_id() {
                curr_tbl.borrow_mut().uniqueness[n as usize] = Uniqueness::from_str(&id_str).unwrap();
            }
        }
    });

    let references_entry = Entry::new();
    references_entry.set_primary_icon_name(Some("key-symbolic"));
    references_entry.set_placeholder_text(Some("References"));
    bx.append(&references_entry);
    references_entry.connect_changed({
        let curr_tbl = curr_tbl.clone();
        let n = parent.observe_children().n_items();
        move |entry| {
            curr_tbl.borrow_mut().references[n as usize] = entry.text().to_string();
        }
    });

    let default_entry = Entry::new();
    default_entry.set_sensitive(false);
    default_entry.set_placeholder_text(Some("Default value"));

    let default_combo = ComboBoxText::new();
    for d in DEFAULT.iter() {
        default_combo.append(Some(&d), &d);
    }
    bx.append(&default_combo);
    bx.append(&default_entry);

    default_combo.set_active_id(Some("No default"));
    default_combo.connect_changed({
        let curr_tbl = curr_tbl.clone();
        let n = parent.observe_children().n_items();
        let default_entry = default_entry.clone();
        move |default_combo| {
            if let Some(id_str) = default_combo.active_id() {
                let this_default = match &id_str[..] {
                    "Auto-increment" => {
                        default_entry.set_text("");
                        default_entry.set_sensitive(false);
                        ColDefault::AutoIncr
                    },
                    "Custom value" => {
                        default_entry.set_text("");
                        default_entry.set_sensitive(true);
                        ColDefault::Value(String::new())
                    },
                    "No default" | _ => {
                        default_entry.set_text("");
                        default_entry.set_sensitive(false);
                        ColDefault::NoDefault
                    }
                };
                curr_tbl.borrow_mut().defaults[n as usize] = this_default;
            }
        }
    });
    default_entry.connect_changed({
        let curr_tbl = curr_tbl.clone();
        let n = parent.observe_children().n_items();
        move |entry| {
            if let Some(ColDefault::Value(ref mut v)) = curr_tbl.borrow_mut().defaults.get_mut(n as usize) {
                *v = entry.text().to_string();
            }
        }
    });

    let btn_erase = Button::new();
    btn_erase.set_icon_name("user-trash-symbolic");
    let btn_sql = btn_sql.clone();
    let btn_create = btn_create.clone();
    btn_erase.connect_clicked({
        let parent = parent.clone();
        let bx = bx.clone();
        let curr_tbl = curr_tbl.clone();
        let n = parent.observe_children().n_items();
        move |_| {
            parent.remove(&bx);
            curr_tbl.borrow_mut().remove(n as usize);
            if parent.observe_children().n_items() == 0 {
                btn_sql.set_sensitive(false);
                btn_create.set_sensitive(false);
            }
        }
    });
    if name.is_some() {
        entry.set_sensitive(false);
        btn_erase.set_sensitive(false);
    }
    bx.append(&btn_erase);
    parent.append(&bx);
}

