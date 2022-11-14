/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use gtk4::prelude::*;
use gtk4::*;
use stateful::React;
use crate::client::ConnectionInfo;
use crate::ui::PackedImageEntry;
use crate::ui::PackedImagePasswordEntry;
use crate::ui::PackedImageLabel;
use crate::client::ConnectionSet;
use std::time::Duration;
use crate::client::ActiveConnection;
use crate::ui::settings::SecurityChange;
use libadwaita::ExpanderRow;
use crate::ui::settings::QueriesSettings;
use std::rc::Rc;
use std::cell::RefCell;
use crate::ui::SharedSignal;

#[derive(Debug, Clone)]
pub struct QueriesOverview {
    pub conn_list : ConnectionList,
    pub conn_bx : ConnectionBox,
    pub sec_bx : SecBox,
    pub detail_bx : DetailBox,
    pub bx : Box,
}

impl QueriesOverview {

    pub fn build() -> Self {
        let conn_list = ConnectionList::build();
        let conn_bx = ConnectionBox::build();
        conn_list.react(&conn_bx);
        let sec_bx = SecBox::build();
        let detail_bx = DetailBox::build();

        let info_bx = Box::new(Orientation::Vertical, 0);
        info_bx.append(&conn_bx.bx);
        info_bx.append(&sec_bx.bx);
        info_bx.append(&detail_bx.bx);
        let bx = Box::new(Orientation::Horizontal, 0);
        bx.append(&conn_list.bx);
        bx.append(&info_bx);
        bx.set_halign(Align::Center);
        bx.set_valign(Align::Center);

        Self { conn_list, conn_bx, detail_bx, sec_bx, bx }
    }

}

#[derive(Debug, Clone)]
pub struct SecBox {
    pub bx : Box,
    pub encryption_lbl : Label,
    pub encryption_img : Image,
    pub certificate_lbl : Label,
    pub _certificate_img : Image,
    pub curr_info : Rc<RefCell<Option<ConnectionInfo>>>
}

impl SecBox {

    pub fn build() -> Self {
        let bx = Box::new(Orientation::Vertical, 0);
        bx.set_margin_bottom(18);
        bx.set_hexpand(true);
        bx.set_halign(Align::Fill);
        let title = super::title_label("Security");
        let encryption = PackedImageLabel::build("padlock2-open-symbolic", "Encryption");
        let certificate = PackedImageLabel::build("application-certificate-symbolic", "Certificate");

        for item in [&encryption, &certificate].iter() {
            item.bx.set_hexpand(true);
            item.bx.set_halign(Align::Fill);
            item.lbl.set_halign(Align::Start);
            item.img.set_halign(Align::Start);
        }

        let encryption_lbl = Label::new(None);
        let certificate_lbl = Label::new(None);
        certificate_lbl.set_use_markup(true);
        for lbl in [&encryption_lbl, &certificate_lbl].iter() {
            lbl.set_hexpand(true);
            lbl.set_halign(Align::End);
        }

        encryption.bx.append(&encryption_lbl);
        certificate.bx.append(&certificate_lbl);

        bx.append(&title);
        bx.append(&encryption.bx);
        bx.append(&certificate.bx);

        Self {
            bx,
            encryption_lbl,
            certificate_lbl,
            encryption_img : encryption.img.clone(),
            _certificate_img : certificate.img.clone(),
            curr_info : Rc::new(RefCell::new(None))
        }
    }

}

#[derive(Debug, Clone)]
pub struct DetailBox {
    bx : Box,
    server_lbl : Label,
    size_lbl : Label,
    uptime_lbl : Label,
    locale_lbl : Label
}

impl DetailBox {

    pub fn build() -> Self {
        let bx = Box::new(Orientation::Vertical, 0);
        bx.set_hexpand(true);
        bx.set_halign(Align::Fill);
        let title = super::title_label("Details");
        let server = PackedImageLabel::build("db-symbolic", "Server");
        let size = PackedImageLabel::build("drive-harddisk-symbolic", "Size");
        let uptime = PackedImageLabel::build("clock-app-symbolic", "Uptime");
        let locale = PackedImageLabel::build("globe-symbolic", "Locale");
        for item in [&server, &size, &uptime, &locale].iter() {
            item.bx.set_hexpand(true);
            item.bx.set_halign(Align::Fill);
            item.lbl.set_halign(Align::Start);
            item.img.set_halign(Align::Start);
        }

        let server_lbl = Label::new(None);
        let size_lbl = Label::new(None);
        let uptime_lbl = Label::new(None);
        let locale_lbl = Label::new(None);
        for lbl in [&server_lbl, &size_lbl, &uptime_lbl, &locale_lbl].iter() {
            lbl.set_hexpand(true);
            lbl.set_halign(Align::End);
        }

        server.bx.append(&server_lbl);
        size.bx.append(&size_lbl);
        uptime.bx.append(&uptime_lbl);
        locale.bx.append(&locale_lbl);

        bx.append(&title);
        bx.append(&server.bx);
        bx.append(&size.bx);
        bx.append(&uptime.bx);
        bx.append(&locale.bx);
        Self { bx, server_lbl, size_lbl, uptime_lbl, locale_lbl }
    }

}

impl React<ActiveConnection> for DetailBox {

    fn react(&self, conn : &ActiveConnection) {
        conn.connect_db_connected({
            let (server_lbl, size_lbl, uptime_lbl, locale_lbl) = (
                self.server_lbl.clone(),
                self.size_lbl.clone(),
                self.uptime_lbl.clone(),
                self.locale_lbl.clone()
            );
            move |(_conn_info, db_info)| {
                if let Some(details) = db_info.as_ref().and_then(|info| info.details.as_ref() ) {
                    server_lbl.set_text(&details.server);
                    size_lbl.set_text(&details.size);
                    uptime_lbl.set_text(&details.uptime);
                    locale_lbl.set_text(&details.locale);
                } else {
                    server_lbl.set_text("Unknown");
                    size_lbl.set_text("Unknown");
                    uptime_lbl.set_text("Unknown");
                    locale_lbl.set_text("Unknown");
                }
            }
        });

        conn.connect_db_disconnected({
            let (server_lbl, size_lbl, uptime_lbl, locale_lbl) = (
                self.server_lbl.clone(),
                self.size_lbl.clone(),
                self.uptime_lbl.clone(),
                self.locale_lbl.clone()
            );
            move |_| {
                server_lbl.set_text("");
                size_lbl.set_text("");
                uptime_lbl.set_text("");
                locale_lbl.set_text("");
            }
        });
    }
}

// impl React<QueriesSettings> for SecBox {
//    fn react(&self, )
// }

fn update_with_info(
    encryption_lbl : &Label,
    certificate_lbl : &Label,
    encryption_img : &Image,
    info : &ConnectionInfo
) {
    if info.is_default() || info.host.is_empty() || info.is_file() {
        encryption_lbl.set_text("");
        certificate_lbl.set_text("");
        encryption_img.set_from_icon_name(Some("padlock2-open-symbolic"));
    } else {
        if info.security.tls_version.is_some() {
            encryption_lbl.set_text("Enabled");
            encryption_img.set_from_icon_name(Some("padlock2-symbolic"));
            if let Some(path) = &info.security.cert_path {
                if info.is_certificate_valid() {
                    if let Some(stem) = path.split("/").last() {
                        let hostname_verified = if info.security.verify_hostname == Some(true) {
                            "(Host verified ✓)"
                        } else {
                            "(Host unverified ⨯)"
                        };
                        certificate_lbl.set_markup(&format!("<a href=\"\">{}</a> {}", stem, hostname_verified));
                    } else {
                        certificate_lbl.set_markup(&format!("<a href=\"\">Configure</a>"));
                    }
                } else {
                    certificate_lbl.set_markup(&format!("<a href=\"\">Configure</a>"));
                }
            } else {
                certificate_lbl.set_markup(&format!("<a href=\"\">Configure</a>"));
            }
        } else {
            encryption_lbl.set_text("Disabled");
            certificate_lbl.set_markup(&format!("<a href=\"\">Configure</a>"));
            encryption_img.set_from_icon_name(Some("padlock2-open-symbolic"));
        }
    }
}

impl React<ConnectionSet> for SecBox {

    fn react(&self, conn_set : &ConnectionSet) {
        conn_set.connect_updated({
            let (encryption_lbl, certificate_lbl, encryption_img) = (
                self.encryption_lbl.clone(),
                self.certificate_lbl.clone(),
                self.encryption_img.clone()
            );
            let curr_info = self.curr_info.clone();
            move |(_, conn_info)| {
                *(curr_info.borrow_mut()) = Some(conn_info.clone());
                update_with_info(&encryption_lbl, &certificate_lbl, &encryption_img, &conn_info);
            }
        });
        conn_set.connect_selected({
            let (encryption_lbl, certificate_lbl, encryption_img) = (
                self.encryption_lbl.clone(),
                self.certificate_lbl.clone(),
                self.encryption_img.clone()
            );
            let curr_info = self.curr_info.clone();
            move |opt_sel| {
                if let Some((_, info)) = opt_sel {
                    *(curr_info.borrow_mut()) = Some(info.clone());
                    update_with_info(&encryption_lbl, &certificate_lbl, &encryption_img, &info);
                } else {
                    *(curr_info.borrow_mut()) = None;
                    encryption_lbl.set_text("");
                    certificate_lbl.set_text("");
                    encryption_img.set_from_icon_name(Some("padlock2-open-symbolic"));
                }
            }
        });
    }
}

impl React<QueriesSettings> for SecBox {

    fn react(&self, settings : &QueriesSettings) {
        settings.security_bx.update_action.connect_activate({
            let curr_info = self.curr_info.clone();
            let (encryption_lbl, certificate_lbl, encryption_img) = (
                self.encryption_lbl.clone(),
                self.certificate_lbl.clone(),
                self.encryption_img.clone(),
            );
            move |_, param| {
                let mut curr_info = curr_info.borrow_mut();
                if let Some(mut info) = curr_info.as_mut() {
                    if let Some(param) = param {
                        let change : SecurityChange = serde_json::from_str(&param.get::<String>().unwrap()).unwrap();
                        if change.host() == &info.host[..] {
                            crate::ui::settings::try_modify_security_for_conn(&mut info, &change);
                            update_with_info(&encryption_lbl, &certificate_lbl, &encryption_img, &info);
                        }
                    }
                }
            }
        });
    }

}

#[derive(Debug, Clone)]
pub struct ConnectionRow {
    pub row : ListBoxRow,
    pub host : PackedImageLabel,
    pub db : PackedImageLabel,
    pub user : PackedImageLabel
}

impl ConnectionRow {

    pub fn from(info : &ConnectionInfo) -> Self {
        let row = Self::build();
        if info.host.is_empty() {
            row.db.change_label(crate::client::DEFAULT_HOST);
        } else {
            row.host.change_label(&info.host);
        }
        if info.database.is_empty() {
            row.db.change_label(crate::client::DEFAULT_DB);
        } else {
            row.db.change_label(&info.database);
        }
        if info.user.is_empty() {
            row.user.change_label(crate::client::DEFAULT_USER);
        } else {
            row.user.change_label(&info.user);
        }
        row
    }

    fn build() -> Self {
        // Change to "network-server-symbolic" when connected
        let host = PackedImageLabel::build("preferences-system-network-proxy-symbolic", "Host");
        let db = PackedImageLabel::build("db-symbolic", "Database");
        let user = PackedImageLabel::build("avatar-default-symbolic", "User");
        let bx = Box::new(Orientation::Vertical, 0);
        [&host, &db, &user].iter().for_each(|w| bx.append(&w.bx));
        let row = ListBoxRow::new();
        row.set_activatable(false);
        row.set_selectable(true);
        let viewp = Viewport::new(None::<&Adjustment>, None::<&Adjustment>);

        viewp.set_child(Some(&bx));
        row.set_child(Some(&viewp));
        ConnectionRow { row, host, db, user }
    }

}

#[derive(Debug, Clone)]
pub struct ConnectionList {
    pub list : ListBox,
    pub scroll : ScrolledWindow,
    pub bx : Box,
    pub add_btn : Button,
    pub remove_btn : Button
}

impl ConnectionList {

    pub fn build() -> Self {
        let list = ListBox::builder().valign(Align::Fill).vexpand(true).vexpand_set(true).build();
        list.style_context().add_class("boxed-list");
        list.set_valign(Align::Fill);
        list.set_vexpand(true);
        list.set_selection_mode(SelectionMode::Single);
        let scroll = ScrolledWindow::new();
        scroll.set_width_request(600);
        scroll.set_valign(Align::Fill);
        scroll.set_vexpand(true);

        scroll.set_has_frame(false);

        list.set_show_separators(true);
        super::set_margins(&list, 1, 1);

        let btn_bx = super::ButtonPairBox::build("list-remove-symbolic", "list-add-symbolic");
        let add_btn = btn_bx.right_btn.clone();
        let remove_btn = btn_bx.left_btn.clone();
        remove_btn.set_sensitive(false);

        btn_bx.bx.set_valign(Align::End);
        btn_bx.bx.set_halign(Align::Fill);

        let title = super::title_label("Connections");
        let bx = Box::new(Orientation::Vertical, 0);

        let title_bx = Box::new(Orientation::Horizontal, 0);
        title_bx.append(&title);
        title_bx.append(&btn_bx.bx);
        btn_bx.bx.set_halign(Align::End);
        bx.append(&title_bx);

        scroll.set_child(Some(&list));
        bx.append(&scroll);
        bx.set_margin_end(72);

        list.connect_row_selected({
            let remove_btn = remove_btn.clone();
            move |_, opt_row| {
                remove_btn.set_sensitive(opt_row.is_some());
            }
        });
        let conn_list = Self { list, scroll, add_btn, /*local_btn,*/ remove_btn, bx };
        conn_list.update();
        conn_list
    }

    /*fn _clear(&self) {
        while self.list.observe_children().n_items() > 1 {
            self.list.remove(&self.list.row_at_index(0).unwrap());
        }
    }

    fn _set(&self, info_slice : &[ConnectionInfo]) {
        self._clear();
        for info in info_slice.iter() {
            let n = self.list.observe_children().n_items();
            let new_row = ConnectionRow::from(info);
            self.list.insert(&new_row.row, (n-1) as i32);
        }
    }*/

    fn update(&self) {
        self.list.connect_row_activated({
            move|list, row| {
                let n = list.observe_children().n_items();
                if row.index() == (n-1) as i32 {
                    let new_row = ConnectionRow::build();
                    list.insert(&new_row.row, (n-1) as i32);
                }
            }
        });
    }

}

impl React<ConnectionSet> for ConnectionList {

    fn react(&self, conns : &ConnectionSet) {
        conns.connect_added({
            let list = self.list.clone();
            move |info| {
                let new_row = ConnectionRow::from(&info);
                list.append(&new_row.row);
                list.select_row(Some(&list.row_at_index((list.observe_children().n_items()-1) as i32).unwrap()));
            }
        });

        conns.connect_removed({
            let list = self.list.clone();
            move |ix| {
                list.remove(&list.row_at_index(ix).unwrap());
            }
        });
    }

}

impl React<ActiveConnection> for ConnectionList {

    fn react(&self, conn : &ActiveConnection) {
        let (add_btn, remove_btn) = (self.add_btn.clone(), self.remove_btn.clone());
        let list = self.list.clone();
        conn.connect_db_connected(move |_| {
            add_btn.set_sensitive(false);
            remove_btn.set_sensitive(false);
            if list.is_sensitive() {
                list.set_sensitive(false);
            }
        });

        let (add_btn, remove_btn) = (self.add_btn.clone(), self.remove_btn.clone());
        let list = self.list.clone();
        conn.connect_db_disconnected(move |_| {
            add_btn.set_sensitive(true);
            remove_btn.set_sensitive(true);
            if !list.is_sensitive() {
                list.set_sensitive(true);
            }
        });
    }

}

impl React<ConnectionBox> for ConnectionList {

    fn react(&self, bx : &ConnectionBox) {
        for (ix, entry) in [&bx.host.entry, &bx.db.entry, &bx.user.entry].iter().enumerate() {
            entry.connect_changed({
                let list = self.list.clone();
                move |entry| {
                    change_text_at_conn_row(&list, ix, &entry)
                }
            });
        }
    }

}

fn change_text_at_conn_row(list : &ListBox, label_ix : usize, entry : &Entry) {
    if let Some(row) = list.selected_row() {
        let vp = row.child().unwrap().downcast::<Viewport>().unwrap();
        let bx = vp.child().unwrap().downcast::<Box>().unwrap();
        let child_bx = super::get_child_by_index(&bx, label_ix);
        let lbl = PackedImageLabel::extract(&child_bx).unwrap();
        let entry_txt = entry.buffer().text();
        if lbl.lbl.text() != entry_txt {
            if entry_txt.is_empty() {
                let placeholder = match label_ix {
                    0 => "Host",
                    1 => "Database",
                    2 => "User",
                    _ => ""  
                };
                lbl.lbl.set_text(placeholder);
            } else {
                lbl.lbl.set_text(&entry_txt.as_str());
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionBox {
    pub host : PackedImageEntry,
    pub port : PackedImageEntry,
    pub user : PackedImageEntry,
    pub db : PackedImageEntry,
    pub password : PackedImagePasswordEntry,
    pub switch : Switch,
    pub bx : Box,
    pub host_changed : SharedSignal,
    pub port_changed : SharedSignal,
    pub user_changed : SharedSignal,
    pub db_changed : SharedSignal
}

impl ConnectionBox {

    pub fn build() -> Self {
        let host_bx = Box::new(Orientation::Horizontal, 0);
        let host = PackedImageEntry::build("preferences-system-network-proxy-symbolic", "Host");
        let port = PackedImageEntry::build("arrow-into-box-symbolic", "Port");
        port.entry.set_max_width_chars(8);
        port.entry.set_input_purpose(InputPurpose::Digits);
        host_bx.append(&host.bx);
        host_bx.append(&port.bx);
        let db = PackedImageEntry::build("db-symbolic", "Database");
        let cred_bx = Box::new(Orientation::Horizontal, 0);
        let user = PackedImageEntry::build("avatar-default-symbolic", "User");
        let password = PackedImagePasswordEntry::build("dialog-password-symbolic", "Password");
        let switch = Switch::new();
        switch.set_valign(Align::Center);
        switch.set_vexpand(false);
        cred_bx.append(&user.bx);
        cred_bx.append(&password.bx);
        cred_bx.append(&switch);
        let title = super::title_label("Authentication");
        let bx = Box::new(Orientation::Vertical, 0);
        bx.append(&title);
        // bx.append(&host.bx);
        bx.append(&host_bx);
        bx.append(&db.bx);
        bx.append(&cred_bx);
        bx.set_margin_bottom(18);

        host.entry.set_hexpand(true);
        db.entry.set_hexpand(true);
        user.entry.set_hexpand(true);
        password.entry.set_hexpand(true);

        let conn_bx = ConnectionBox {
            host,
            port,
            user,
            db,
            password,
            bx,
            switch,
            host_changed : Default::default(),
            port_changed : Default::default(),
            user_changed : Default::default(),
            db_changed : Default::default()
        };
        conn_bx.set_sensitive(false);
        conn_bx
    }

    pub fn entries<'a>(&'a self) -> [&'a Entry; 4] {
        [&self.host.entry, &self.port.entry, &self.db.entry, &self.user.entry]
    }

    pub fn password_entry<'a>(&'a self) -> &'a PasswordEntry {
        &self.password.entry
    }

    // fn _set_db_loaded_mode(&self) {
    //    self.entries().iter().for_each(|entry| entry.set_sensitive(false) );
    // }

    pub fn set_non_db_mode(&self) {
        self.entries().iter().for_each(|entry| entry.set_sensitive(true) );
    }

    pub fn update_info(&self, info : &ConnectionInfo) {
        self.user.entry.set_text(&info.user);
        self.host.entry.set_text(&info.host);
        self.host.entry.set_text(&info.port);
        self.db.entry.set_text(&info.database);
        self.password.entry.set_text("");
    }

    /*fn _check_entries_clear(&self) -> bool {
        for entry in self.entries().iter().take(3) {
            let txt = entry.text().to_string();
            if !txt.is_empty() {
                return false;
            }
        }
        true
    }*/

    fn set_sensitive(&self, sensitive : bool) {
        self.host.entry.set_sensitive(sensitive);
        self.port.entry.set_sensitive(sensitive);
        self.db.entry.set_sensitive(sensitive);
        self.user.entry.set_sensitive(sensitive);
        self.password.entry.set_sensitive(sensitive);
        self.switch.set_sensitive(sensitive);
    }

}

impl React<ConnectionSet> for ConnectionBox {

    fn react(&self, connections : &ConnectionSet) {
        connections.connect_selected({
            let conn_bx = self.clone();
            move |opt_sel| {
                let signals = (
                    &*conn_bx.host_changed.borrow(),
                    &*conn_bx.port_changed.borrow(),
                    &*conn_bx.db_changed.borrow(),
                    &*conn_bx.user_changed.borrow()
                );
                if let (Some(host_s), Some(port_s), Some(db_s), Some(user_s)) = signals {
                    conn_bx.host.entry.block_signal(host_s);
                    conn_bx.port.entry.block_signal(port_s);
                    conn_bx.user.entry.block_signal(user_s);
                    conn_bx.db.entry.block_signal(db_s);
                }
                if let Some((_sel_ix, sel_info)) = opt_sel {
                    conn_bx.set_sensitive(true);
                    println!("Selected: {:?}", sel_info);
                    if sel_info.host == crate::client::DEFAULT_HOST {
                        conn_bx.host.entry.set_text("");
                    } else {
                        conn_bx.host.entry.set_text(&sel_info.host);
                    }
                    if sel_info.port == crate::client::DEFAULT_PORT {
                        conn_bx.port.entry.set_text("");
                    } else {
                        conn_bx.port.entry.set_text(&sel_info.port);
                    }
                    if sel_info.database == crate::client::DEFAULT_DB {
                        conn_bx.db.entry.set_text("");
                    } else {
                        conn_bx.db.entry.set_text(&sel_info.database);
                    }
                    if sel_info.user == crate::client::DEFAULT_USER {
                        conn_bx.user.entry.set_text("");
                    } else {
                        conn_bx.user.entry.set_text(&sel_info.user);
                    }
                    conn_bx.password.entry.set_text("");
                    conn_bx.password.entry.grab_focus();
                } else {
                    println!("No selection");
                    conn_bx.host.entry.set_text("");
                    conn_bx.port.entry.set_text("");
                    conn_bx.db.entry.set_text("");
                    conn_bx.user.entry.set_text("");
                    conn_bx.password.entry.set_text("");
                    conn_bx.set_sensitive(false);
                }
                if let (Some(host_s), Some(port_s), Some(db_s), Some(user_s)) = signals {
                    conn_bx.host.entry.unblock_signal(host_s);
                    conn_bx.port.entry.unblock_signal(port_s);
                    conn_bx.user.entry.unblock_signal(user_s);
                    conn_bx.db.entry.unblock_signal(db_s);
                }
            }
        });
        connections.connect_added({
            let conn_bx = self.clone();
            move |_info| {
                conn_bx.set_sensitive(true);
            }
        });
    }

}

impl React<ActiveConnection> for ConnectionBox {

    fn react(&self, conn : &ActiveConnection) {
        let switch = self.switch.clone();
        conn.connect_db_conn_failure(move |_| {
            disconnect_with_delay(switch.clone());
        });
    }

}

pub fn disconnect_with_delay(switch : Switch) {
    glib::timeout_add_local(Duration::from_millis(160), move || {
        switch.set_state(false);
        glib::Continue(false)
    });
}
