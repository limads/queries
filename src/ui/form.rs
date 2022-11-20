/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use gtk4::prelude::*;
use gtk4::*;
use crate::sql::object::{DBObject};

pub const MAX_ENTRIES : usize = 32;

#[derive(Debug, Clone)]
pub struct Form {
    pub bx : Box,
    pub entries : [Entry; MAX_ENTRIES],
    pub btn_cancel : Button,
    pub btn_ok : Button,
    pub dialog : Dialog,
    pub err_lbl : Label
}

impl Form {

    pub fn new() -> Self {
        let bx = Box::new(Orientation::Vertical, 0);
        let err_lbl = Label::new(Some("Target should have at most 32 elements"));
        err_lbl.set_visible(false);
        let entries_bx = Box::new(Orientation::Vertical, 0);
        entries_bx.append(&err_lbl);
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
        Self { bx, entries, btn_cancel, btn_ok, dialog, err_lbl }
    }

    pub fn update_from_table(&self, tbl : &DBObject) -> bool {
        self.entries.iter().for_each(|e| e.set_visible(false) );
        match tbl {
            DBObject::Table { name, cols, .. } => {
                if cols.len() > MAX_ENTRIES {
                    self.err_lbl.set_visible(true);
                    self.btn_ok.set_sensitive(false);
                    false
                } else {
                    self.err_lbl.set_visible(false);
                    self.btn_ok.set_sensitive(true);
                    self.dialog.set_title(Some(&format!("Insert ({})", name)));
                    for (ix, col) in cols.iter().enumerate() {
                        self.entries[ix].set_visible(true);

                        /* The theme variant does not matter here, since the symbolic icon
                        should be rendered according to the theme. */
                        self.entries[ix].set_primary_icon_name(Some(super::get_type_icon_name(&col.ty, false)));

                        self.entries[ix].set_placeholder_text(Some(&col.name));
                    }
                    self.bx.grab_focus();
                    self.btn_ok.set_label("Insert");
                    true
                }
            },
            _ => { 
                false
            }
        }
    }

    pub fn update_from_function(&self, func  : &DBObject) -> bool {
        self.entries.iter().for_each(|e| e.set_visible(false) );
        match func {
            DBObject::Function { name, args, arg_names, .. } => {
                self.dialog.set_title(Some(&format!("Call ({})", name)));
                if args.len() > MAX_ENTRIES {
                    self.err_lbl.set_visible(true);
                    self.btn_ok.set_sensitive(false);
                    false
                } else {
                    self.err_lbl.set_visible(false);
                    self.btn_ok.set_sensitive(true);
                    for (ix, arg) in args.iter().enumerate() {
                        self.entries[ix].set_visible(true);

                        /* The theme variant does not matter here, since the symbolic icon
                        should be rendered according to the theme. */
                        self.entries[ix].set_primary_icon_name(Some(super::get_type_icon_name(&arg, false)));

                        if let Some(names) = &arg_names {
                            self.entries[ix].set_placeholder_text(Some(&names[ix]));
                        }
                    }
                    self.bx.grab_focus();
                    self.btn_ok.set_label("Call");
                    true
                }
            },
            _ => false
        }
    }


}


