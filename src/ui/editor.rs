use gtk4::prelude::*;
use gtk4::*;
use sourceview5;
use crate::ui::PackedImageLabel;

#[derive(Debug, Clone)]
pub struct QueriesEditor {
    pub views : [sourceview5::View; 16],
    pub script_list : ScriptList,
    pub stack : Stack,
    pub save_dialog : SaveDialog
}

impl QueriesEditor {

    pub fn build() -> Self {
        let stack = Stack::new();
        let script_list = ScriptList::build();
        let save_dialog = SaveDialog::build();
        Self { views : Default::default(), stack, script_list, save_dialog }
    }

}

#[derive(Debug, Clone)]
pub struct ScriptList {
    open_btn : Button,
    new_btn : Button,
    list : ListBox
}

impl ScriptList {

    pub fn build() -> Self {
        let open_btn = Button::builder().icon_name("document-open-symbolic").build();
        let new_btn = Button::builder().icon_name("document-new-symbolic").build();
        let btn_bx = Box::new(Orientation::Horizontal, 0);
        btn_bx.append(&new_btn);
        btn_bx.append(&open_btn);
        btn_bx.set_valign(Align::End);
        btn_bx.set_halign(Align::Fill);
        btn_bx.style_context().add_class("linked");

        let list = ListBox::new();
        list.set_activate_on_single_click(true);
        let bx = Box::new(Orientation::Vertical, 0);
        bx.set_halign(Align::Center);
        bx.set_width_request(420);
        bx.append(&list);
        bx.append(&btn_bx);

        // bind_list_to_buttons(&list, &new_btn, &open_btn);
        Self { open_btn, new_btn, list }
    }

    pub fn add_row(&self, path : &str) {
        let row = ListBoxRow::new();
        let lbl = PackedImageLabel::build("emblem-documents-symbolic", path);
        row.set_child(Some(&lbl.bx));
        row.set_selectable(false);
        row.set_activatable(true);
        self.list.append(&row);
    }

}

/*fn bind_list_to_buttons(list : &ListBox, new_btn : &Button, open_btn : &Button) {
    list.connect_row_activated(move |row| {

    });
    new_btn.connect_clicked(move|| {

    });
    open_btn.connect_clicked(move|| {

    });
}*/

#[derive(Debug, Clone)]
pub struct SaveDialog {
    dialog : FileChooserDialog
}

impl SaveDialog {

    pub fn build() -> Self {
        let dialog = FileChooserDialog::new(
            Some("New script"),
            None::<&Window>,
            FileChooserAction::Save,
            &[("Cancel", ResponseType::None), ("New", ResponseType::Accept)]
        );
        let filter = FileFilter::new();
        filter.add_pattern("*.sql");
        dialog.set_filter(&filter);
        Self { dialog }
    }

}


