use gtk4::*;
use gtk4::prelude::*;
use crate::ui::PackedImageLabel;

#[derive(Debug, Clone)]
pub struct FileList {
    pub list : ListBox,
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
        bx.append(&list);

        let list = ListBox::builder().valign(Align::Fill).vexpand(true).vexpand_set(true).build();
        list.set_valign(Align::Fill);

        Self { list, bx }
    }

}
