use gtk4::prelude::*;
use gtk4::*;
use sourceview5::prelude::*;
use glib::MainContext;
use std::rc::Rc;
use std::cell::RefCell;
use std::boxed;
use crate::React;
use crate::client::ConnectionInfo;
use crate::ui::PackedImageEntry;
use crate::ui::PackedImageLabel;

#[derive(Debug, Clone)]
pub struct QueriesOverview {
    pub conn_list : ConnectionList,
    conn_bx : ConnectionBox,
    detail_bx : DetailBox,
    pub bx : Box
}

impl QueriesOverview {

    pub fn build() -> Self {
        let conn_list = ConnectionList::build();
        let conn_bx = ConnectionBox::build();
        let detail_bx = DetailBox::build();


        let info_bx = Box::new(Orientation::Vertical, 0);
        info_bx.append(&conn_bx.bx);
        info_bx.append(&detail_bx.bx);
        info_bx.set_width_request(OVERVIEW_RIGHT_WIDTH_REQUEST);
        info_bx.set_margin_start(18);
        info_bx.set_margin_end(18);

        let bx = Box::new(Orientation::Horizontal, 0);
        //bx.append(&conn_list.scroll);
        bx.append(&conn_list.bx);
        bx.append(&info_bx);

        Self { conn_list, conn_bx, detail_bx, bx }
    }

}

const OVERVIEW_RIGHT_WIDTH_REQUEST : i32 = 600;

#[derive(Debug, Clone)]
pub struct DetailBox {
    bx : Box
}

impl DetailBox {

    pub fn build() -> Self {
        let bx = Box::new(Orientation::Vertical, 0);
        let dbname = PackedImageLabel::build("db-symbolic", "Database");
        let size = PackedImageLabel::build("drive-harddisk-symbolic", "Size");
        let encoding = PackedImageLabel::build("format-text-underline-symbolic", "Encoding");
        let locale = PackedImageLabel::build("globe-symbolic", "Locale");
        bx.append(&dbname.bx);
        bx.append(&size.bx);
        bx.append(&encoding.bx);
        bx.append(&locale.bx);
        Self { bx }
    }

}

#[derive(Debug, Clone)]
pub struct ConnectionRow {
    row : ListBoxRow,
    host : PackedImageLabel,
    db : PackedImageLabel,
    user : PackedImageLabel
}

/*impl React<ConnectionList> for ConnectionBox {

    fn react(&self, conn_list : &ConnectionList) {
        conn_list.list.connect_row_selected({
            move |_, opt_row| {
                if let Some(ix)
            }
        });
    }

}*/

impl ConnectionRow {

    fn from(info : &ConnectionInfo) -> Self {
        let row = Self::build();
        row.host.change_label(&info.host);
        row.db.change_label(&info.database);
        row.user.change_label(&info.user);
        row
    }

    fn build() -> Self {
        // Change to "network-server-symbolic" when connected
        let host = PackedImageLabel::build("gnome-netstatus-tx", "Host");
        let db = PackedImageLabel::build("db-symbolic", "Database");
        let user = PackedImageLabel::build("avatar-default-symbolic", "User");
        let bx = Box::new(Orientation::Vertical, 0);
        [&host, &db, &user].iter().for_each(|w| bx.append(&w.bx));
        let row = ListBoxRow::new();
        row.set_activatable(false);
        row.set_selectable(true);
        let viewp = Viewport::new(None::<&Adjustment>, None::<&Adjustment>);

        // let provider = CssProvider::new();
        // provider.load_from_data("* { border-bottom : 1px solid #d9dada; } ".as_bytes());
        // provider.load_from_data("* { box-shadow: 0 1px 1px 0px #d9dada; } ".as_bytes());
        // viewp.style_context().add_provider(&provider, 800);
        viewp.set_child(Some(&bx));
        row.set_child(Some(&viewp));
        ConnectionRow { row, host, db, user }
    }

}

#[derive(Debug, Clone)]
pub struct ConnectionList {
    pub list : ListBox,
    pub scroll : ScrolledWindow,
    add_row : ListBoxRow,
    pub bx : Box,
    pub add_btn : Button,
    pub remove_btn : Button
}

impl ConnectionList {

    pub fn build() -> Self {
        let list = ListBox::builder().valign(Align::Fill).vexpand(true).vexpand_set(true).build();
        list.set_valign(Align::Fill);
        list.set_selection_mode(SelectionMode::Single);
        list.set_activate_on_single_click(true);
        let add_row = ListBoxRow::new();
        add_row.set_child(Some(&Image::from_icon_name(Some("list-add-symbolic"))));
        add_row.set_selectable(false);
        add_row.set_activatable(true);
        list.append(&add_row);
        let scroll = ScrolledWindow::new();
        scroll.set_width_request(420);
        scroll.set_valign(Align::Fill);

        let provider = CssProvider::new();
        provider.load_from_data("* { border-right : 1px solid #d9dada; } ".as_bytes());
        scroll.style_context().add_provider(&provider, 800);
        list.set_show_separators(true);
        let add_btn = Button::builder().icon_name("list-add-symbolic").halign(Align::Fill).hexpand(true).build();
        let remove_btn = Button::builder().icon_name("list-remove-symbolic").halign(Align::Fill).hexpand(true).build();
        let btn_bx = Box::new(Orientation::Horizontal, 0);
        btn_bx.append(&remove_btn);
        btn_bx.append(&add_btn);
        btn_bx.set_valign(Align::End);
        btn_bx.set_halign(Align::Fill);
        btn_bx.style_context().add_class("linked");
        super::set_margins(&btn_bx, 32, 6);
        scroll.set_child(Some(&list));

        let bx = Box::new(Orientation::Vertical, 0);
        bx.append(&scroll);
        bx.append(&btn_bx);
        let conn_list = Self { list, add_row, scroll, add_btn, remove_btn, bx };
        conn_list.update();
        conn_list
    }

    fn clear(&self) {
        while self.list.observe_children().n_items() > 1 {
            self.list.remove(&self.list.row_at_index(0).unwrap());
        }
    }

    fn set(&self, info_slice : &[ConnectionInfo]) {
        self.clear();
        for info in info_slice.iter() {
            let n = self.list.observe_children().n_items();
            let new_row = ConnectionRow::from(info);
            self.list.insert(&new_row.row, (n-1) as i32);
        }
    }

    fn update(&self) {
        self.list.connect_row_activated({
            move|list, row| {
                let n = list.observe_children().n_items();
                if row.index() == (n-1) as i32 {
                    let new_row = ConnectionRow::build();
                    list.insert(&new_row.row, (n-1) as i32);
                    // rows.push(new_row);
                }
            }
        });
    }

}

#[derive(Debug, Clone)]
pub struct ConnectionBox {
    host : PackedImageEntry,
    user : PackedImageEntry,
    db : PackedImageEntry,
    password : PackedImageEntry,
    switch : Switch,
    bx : Box
}

impl ConnectionBox {

    /*pub fn connect_db_connected(&self) -> glib::Receiver<ConnectionInfo> {
        let (send, recv) = MainContext::default().channel();
        self.swtich.connect_activate(move|| {
            send.send(Default::default())
        });
        recv
    }*/

    // pub fn on_connected(f : Fn)
    pub fn build() -> Self {
        let host = PackedImageEntry::build("network-server-symbolic", "Host");
        let db = PackedImageEntry::build("db-symbolic", "Database");
        let cred_bx = Box::new(Orientation::Horizontal, 0);
        let user = PackedImageEntry::build("avatar-default-symbolic", "User");
        let password = PackedImageEntry::build("dialog-password-symbolic", "Password");
        let switch = Switch::new();
        // super::set_margins(&switch, 6, 12);
        switch.set_valign(Align::Center);
        switch.set_vexpand(false);
        cred_bx.append(&user.bx);
        cred_bx.append(&password.bx);
        cred_bx.append(&switch);
        let bx = Box::new(Orientation::Vertical, 0);
        bx.append(&host.bx);
        bx.append(&db.bx);
        bx.append(&cred_bx);

        host.entry.set_hexpand(true);
        db.entry.set_hexpand(true);
        db.entry.set_hexpand(true);
        user.entry.set_hexpand(true);
        password.entry.set_hexpand(true);

        ConnectionBox {
            host,
            user,
            db,
            password,
            bx,
            switch
        }
    }

    pub fn update_info(&self, info : &ConnectionInfo) {
        self.user.entry.set_text(&info.user);
        self.host.entry.set_text(&info.host);
        self.db.entry.set_text(&info.database);
        self.password.entry.set_text("");
    }

}

