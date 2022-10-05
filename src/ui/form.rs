/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

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
use crate::sql::object::{DBObject, DBType};

pub const MAX_ENTRIES : usize = 32;

#[derive(Debug, Clone)]
pub struct Form {
    pub bx : Box,
    pub entries : [Entry; MAX_ENTRIES],
    pub btn_cancel : Button,
    pub btn_ok : Button,
    pub dialog : Dialog
}

impl Form {

    pub fn new() -> Self {
        let bx = Box::new(Orientation::Vertical, 0);

        let entries_bx = Box::new(Orientation::Vertical, 0);
        let entries : [Entry; MAX_ENTRIES] = Default::default();
        for ix in 0..MAX_ENTRIES {
            entries_bx.append(&entries[ix]);
            entries[ix].set_width_request(320);
            entries[ix].set_visible(false);
        }
        entries_bx.style_context().add_class("linked");
        bx.append(&entries_bx);
        super::set_margins(&entries_bx, 64, 16);

        let btn_bx = Box::new(Orientation::Horizontal, 32);
        btn_bx.set_hexpand(true);
        btn_bx.set_halign(Align::Center);
        let btn_cancel = Button::builder().label("Cancel").build();
        let btn_ok = Button::builder().label("Insert").build();
        btn_cancel.style_context().add_class("pill");
        btn_ok.style_context().add_class("pill");
        btn_ok.style_context().add_class("suggested-action");
        btn_bx.append(&btn_cancel);
        btn_bx.append(&btn_ok);
        // btn_bx.style_context().add_class("linked");
        bx.append(&btn_bx);
        super::set_margins(&btn_bx, 64,  16);
        super::set_margins(&bx, 32,  32);
        let dialog = Dialog::new();
        super::configure_dialog(&dialog);
        dialog.set_child(Some(&bx));
        dialog.connect_close({
            let entries = entries.clone();
            move |_dialog| {
                entries.iter().for_each(|e| e.set_text("") );
            }
        });
        Self { bx, entries, btn_cancel, btn_ok, dialog }
    }

    pub fn update_from_table(&self, tbl : &DBObject) {
        self.entries.iter().for_each(|e| e.set_visible(false) );
        match tbl {
            DBObject::Table { name, cols, .. } => {
                self.dialog.set_title(Some(&format!("Insert ({})", name)));
                for (ix, col) in cols.iter().enumerate() {
                    self.entries[ix].set_visible(true);
                    self.entries[ix].set_primary_icon_name(Some(super::get_type_icon_name(&col.1)));
                    self.entries[ix].set_placeholder_text(Some(&col.0));
                }
                self.bx.grab_focus();
                self.btn_ok.set_label("Insert");
            },
            _ => { }
        }
    }

    pub fn update_from_function(&self, func  : &DBObject) {
        self.entries.iter().for_each(|e| e.set_visible(false) );
        match func {
            DBObject::Function { name, args, arg_names, .. } => {
                self.dialog.set_title(Some(&format!("Call ({})", name)));
                for (ix, arg) in args.iter().enumerate() {
                    self.entries[ix].set_visible(true);
                    self.entries[ix].set_primary_icon_name(Some(super::get_type_icon_name(&arg)));
                    if let Some(names) = &arg_names {
                        self.entries[ix].set_placeholder_text(Some(&names[ix]));
                    }
                }
                self.bx.grab_focus();
                self.btn_ok.set_label("Call");
            },
            _ => { }
        }
    }


}


