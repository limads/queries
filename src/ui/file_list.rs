/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use gtk4::*;
use gtk4::prelude::*;
use crate::ui::PackedImageLabel;
use crate::client::OpenedScripts;
use stateful::React;
use archiver::MultiArchiverImpl;

#[derive(Debug, Clone)]
pub struct FileList {
    pub list : ListBox,
    pub close_action : gio::SimpleAction,
    pub bx : Box
}

impl FileList {

    pub fn build() -> Self {
        let list = ListBox::builder().valign(Align::Fill).vexpand(true).build();
        list.set_can_focus(false);
        let title = PackedImageLabel::build("accessories-text-editor-symbolic", "Scripts");
        title.bx.set_vexpand(false);
        title.bx.set_valign(Align::Start);
        super::set_border_to_title(&title.bx);
        let bx = Box::new(Orientation::Vertical, 0);
        bx.append(&title.bx);

        // bx.set_vexpand(true);
        // bx.set_valign(Align::Fill);
        let close_action = gio::SimpleAction::new("close_file", Some(&i32::static_variant_type()));

        let scroll = ScrolledWindow::new();
        scroll.set_child(Some(&list));
        bx.append(&scroll);

        // let list = ListBox::builder().valign(Align::Fill).vexpand(true).vexpand_set(true).build();
        // list.set_valign(Align::Fill);

        Self { list, bx, close_action }
    }

}

pub fn add_file(list : &ListBox, path : &str) {
    let lbl = PackedImageLabel::build("text-x-generic-symbolic", path);
    /*let img_close = Image::from_icon_name(
        Some("application-exit-symbolic"),
        IconSize::SmallToolbar
    );
    let ev_box = EventBox::new();*/

    let ev_btn = Button::builder().icon_name("application-exit-symbolic").build();
    let btn_ctx = ev_btn.style_context();
    btn_ctx.add_class("flat");
    // btn_ctx.add_class("circular");

    // ev_box.add(&img_close);
    // let close_action = close_action.clone();
    let row = ListBoxRow::new();
    row.set_hexpand(true);
    row.set_halign(Align::Fill);
    row.set_hexpand_set(true);

    ev_btn.connect_clicked({
        let row = row.clone();
        move |btn| {
            btn.activate_action("win.close_file", Some(&row.index().to_variant()));
        }
    });
    ev_btn.set_halign(Align::End);
    ev_btn.set_hexpand(false);
    lbl.bx.set_hexpand(true);
    lbl.bx.set_hexpand_set(true);
    lbl.bx.set_halign(Align::Fill);
    lbl.lbl.set_halign(Align::Start);
    lbl.img.set_halign(Align::Start);

    let sep_bx = Box::new(Orientation::Horizontal, 0);
    sep_bx.set_halign(Align::Fill);
    sep_bx.set_hexpand(true);
    sep_bx.set_hexpand_set(true);
    lbl.bx.append(&sep_bx);

    lbl.bx.append(&ev_btn);

    lbl.lbl.set_use_markup(true);
    row.set_selectable(true);
    row.set_activatable(false);
    row.set_child(Some(&lbl.bx));
    list.append(&row);

    // This will trigger OpenedScripts::on_selected, which in turn changes the editor.
    list.select_row(Some(&row));
}

impl React<OpenedScripts> for FileList {

    fn react(&self, opened : &OpenedScripts) {
        opened.connect_new({
            let list = self.list.clone();
            move |info| {
                add_file(&list, &info.name);
            }
        });
        opened.connect_opened({
            let list = self.list.clone();
            move |info| {
                add_file(&list, &info.name);
            }
        });
        opened.connect_reopen({
            let list = self.list.clone();
            move |info| {
                if let Some(row) = list.row_at_index(info.index as i32) {
                    list.select_row(Some(&row));
                } else {
                    eprintln!("Missing file {:?}, at index {}", info.path, info.index);
                }
            }
        });
        opened.connect_closed({
            let list = self.list.clone();
            move |(old_file, _n_remaining)| {
                let row = list.row_at_index(old_file.index as i32).unwrap();
                list.remove(&row);
            }
        });
        opened.connect_file_changed({
            let list = self.list.clone();
            move |file| {
                let lbl = get_label_child(&list, file.index);
                let txt = lbl.label();
                if !txt.starts_with(ITALIC_SPAN_END) && !txt.ends_with(ITALIC_SPAN_END) {
                    lbl.set_label(&format!("{}{}{}", ITALIC_SPAN_START, txt, ITALIC_SPAN_END));
                }
            }
        });
        opened.connect_file_persisted({
            let list = self.list.clone();
            move |file| {
                let lbl = get_label_child(&list, file.index);
                let txt = lbl.label();
                if txt.starts_with(ITALIC_SPAN_START) && txt.ends_with(ITALIC_SPAN_END) {
                    let n_chars = txt.as_str().chars().count();
                    let chars = txt.as_str().chars();
                    lbl.set_label(&format!("{}", chars.skip(26).take(n_chars-26-7).collect::<String>()));
                }
            }
        });
        opened.connect_name_changed({
            let list = self.list.clone();
            move |(ix, name)| {
                let lbl = get_label_child(&list, ix);
                lbl.set_label(&name);
            }
        });
    }

}

const ITALIC_SPAN_START : &'static str = "<span font_style=\"italic\">";

const ITALIC_SPAN_END : &'static str = "</span>";

pub fn get_label_child(list : &ListBox, ix : usize) -> Label {
    let row = list.row_at_index(ix as i32).unwrap();
    let bx = row.child().clone().unwrap().downcast::<Box>().unwrap();
    super::get_child_by_index::<Label>(&bx, 1)
}
