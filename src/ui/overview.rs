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
use crate::ui::PackedImagePasswordEntry;
use crate::ui::PackedImageLabel;
use crate::client::ConnectionSet;
use std::time::Duration;
use crate::client::ActiveConnection;

#[derive(Debug, Clone)]
pub struct QueriesOverview {
    pub conn_list : ConnectionList,
    pub conn_bx : ConnectionBox,
    pub detail_bx : DetailBox,
    pub bx : Box,
}

impl QueriesOverview {

    pub fn build() -> Self {
        let conn_list = ConnectionList::build();
        let conn_bx = ConnectionBox::build();
        conn_list.react(&conn_bx);
        let detail_bx = DetailBox::build();

        let info_bx = Box::new(Orientation::Vertical, 0);
        info_bx.append(&conn_bx.bx);
        info_bx.append(&detail_bx.bx);
        // info_bx.set_width_request(OVERVIEW_RIGHT_WIDTH_REQUEST);
        // info_bx.set_margin_start(18);
        // info_bx.set_margin_end(18);

        let bx = Box::new(Orientation::Horizontal, 0);
        //bx.append(&conn_list.scroll);
        bx.append(&conn_list.bx);
        bx.append(&info_bx);

        // super::set_margins(&bx, 198, 0);
        bx.set_halign(Align::Center);
        bx.set_valign(Align::Center);

        /*conn_bx.switch.connect_activate({
            let add_btn = conn_list.add_btn.clone();
            let remove_btn = conn_list.remove_btn.clone();
            move |switch| {
                if switch.is_active() {
                    add_btn.set_sensitive(false);
                    remove_btn.set_sensitive(false);
                } else {
                    add_btn.set_sensitive(true);
                    remove_btn.set_sensitive(true);
                }
            }
        });*/

        Self { conn_list, conn_bx, detail_bx, bx }
    }

}

#[derive(Debug, Clone)]
pub struct DetailBox {
    bx : Box
}

impl DetailBox {

    pub fn build() -> Self {
        let bx = Box::new(Orientation::Vertical, 0);
        let title = super::title_label("Details");
        let dbname = PackedImageLabel::build("db-symbolic", "Database");
        let size = PackedImageLabel::build("drive-harddisk-symbolic", "Size");
        let encoding = PackedImageLabel::build("format-text-underline-symbolic", "Encoding");
        let locale = PackedImageLabel::build("globe-symbolic", "Locale");
        bx.append(&title);
        bx.append(&dbname.bx);
        bx.append(&size.bx);
        bx.append(&encoding.bx);
        bx.append(&locale.bx);
        Self { bx }
    }

}

impl React<ConnectionSet> for DetailBox {

    fn react(&self, conns : &ConnectionSet) {
        let detail_bx = self.clone();
        conns.connect_selected(move |opt_sel| {
            if let Some((sel_ix, sel_info)) = opt_sel {
                // Set connection details
            } else {

            }
        });
    }

}

#[derive(Debug, Clone)]
pub struct ConnectionRow {
    row : ListBoxRow,
    host : PackedImageLabel,
    db : PackedImageLabel,
    user : PackedImageLabel
}

impl ConnectionRow {

    fn from(info : &ConnectionInfo) -> Self {
        let row = Self::build();
        row.host.change_label(&info.host);
        row.db.change_label(&info.database);
        row.user.change_label(&info.user);
        row
    }

    fn extract(row : &ListBoxRow) -> Option<Self> {
        let bx = row.child()?.downcast::<Box>().ok()?;
        let bx_host = super::get_child_by_index::<Box>(&bx, 0);
        let bx_db = super::get_child_by_index::<Box>(&bx, 1);
        let bx_user = super::get_child_by_index::<Box>(&bx, 2);
        Some(Self {
            row : row.clone(),
            host : PackedImageLabel::extract(&bx_host)?,
            db : PackedImageLabel::extract(&bx_db)?,
            user : PackedImageLabel::extract(&bx_user)?,
        })
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
    // add_row : ListBoxRow,
    pub bx : Box,
    pub add_btn : Button,
    // pub local_btn : Button,
    pub remove_btn : Button
}

impl ConnectionList {

    pub fn build() -> Self {
        let list = ListBox::builder().valign(Align::Fill).vexpand(true).vexpand_set(true).build();
        // list.style_context().add_class("boxed-list");
        list.set_valign(Align::Fill);
        list.set_selection_mode(SelectionMode::Single);
        // list.set_activate_on_single_click(true);
        // let add_row = ListBoxRow::new();
        // add_row.set_child(Some(&Image::from_icon_name(Some("list-add-symbolic"))));
        // add_row.set_selectable(false);
        // add_row.set_activatable(true);
        // list.append(&add_row);
        let scroll = ScrolledWindow::new();
        scroll.set_width_request(520);
        scroll.set_valign(Align::Fill);

        let provider = CssProvider::new();
        provider.load_from_data("* { border : 1px solid #d9dada; } ".as_bytes());
        scroll.style_context().add_provider(&provider, 800);
        list.set_show_separators(true);
        scroll.set_margin_bottom(36);

        let btn_bx = super::ButtonPairBox::build("list-remove-symbolic", "list-add-symbolic");
        let add_btn = btn_bx.right_btn.clone();
        let remove_btn = btn_bx.left_btn.clone();

        btn_bx.bx.set_valign(Align::End);
        btn_bx.bx.set_halign(Align::Fill);
        // btn_bx.style_context().add_class("linked");

        // super::set_margins(&btn_bx, 0, 36);
        scroll.set_child(Some(&list));

        let title = super::title_label("Connections");
        let bx = Box::new(Orientation::Vertical, 0);

        // bx.append(&title);
        let title_bx = Box::new(Orientation::Horizontal, 0);
        title_bx.append(&title);
        title_bx.append(&btn_bx.bx);
        btn_bx.bx.set_halign(Align::End);
        bx.append(&title_bx);
        bx.append(&scroll);

        // bx.append(&btn_bx);
        bx.set_margin_end(72);

        let conn_list = Self { list, scroll, add_btn, /*local_btn,*/ remove_btn, bx };
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

        // Row index here is not set yet.
        /*conns.connect_updated({
            let list = self.list.clone();
            move |(ix, info)| {
                let conn_row = ConnectionRow::extract(&list.row_at_index(ix).unwrap()).unwrap();
                conn_row.user.lbl.set_text(&format!("{}\t\t\t\t{}", info.user, info.dt));
            }
        });*/
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
            list.set_sensitive(false);
            println!("Sensitive false");

            // TODO add connection to settings
        });

        let (add_btn, remove_btn) = (self.add_btn.clone(), self.remove_btn.clone());
        let list = self.list.clone();
        conn.connect_db_disconnected(move |_| {
            add_btn.set_sensitive(true);
            remove_btn.set_sensitive(true);
            list.set_sensitive(true);
            println!("Sensitive true");
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

        /*bx.host.entry.connect_changed({
            let list = self.list.clone();
            move |entry| {
                change_text_at_conn_row(&list, 0, &entry)
            }
        });

        bx.db.entry.connect_changed({
            let list = self.list.clone();
            move |entry| {
                change_text_at_conn_row(&list, 1, &entry)
            }
        });

        bx.user.entry.connect_changed({
            let list = self.list.clone();
            move |entry| {
                change_text_at_conn_row(&list, 2, &entry)
            }
        });*/
    }

}

fn change_text_at_conn_row(list : &ListBox, label_ix : usize, entry : &Entry) {
    if let Some(row) = list.selected_row() {
        let vp = row.child().unwrap().downcast::<Viewport>().unwrap();
        let bx = vp.child().unwrap().downcast::<Box>().unwrap();
        let child_bx = super::get_child_by_index(&bx, label_ix);
        let lbl = PackedImageLabel::extract(&child_bx).unwrap();
        let txt = entry.buffer().text();
        lbl.lbl.set_text(&txt.as_str());
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionBox {
    pub host : PackedImageEntry,
    pub user : PackedImageEntry,
    pub db : PackedImageEntry,
    pub password : PackedImagePasswordEntry,
    pub switch : Switch,
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

    /*pub fn disconnect_with_delay(
        _switch : Switch
    ) {
        //let switch = switch.clone();
        glib::timeout_add_local(160, move || {
            //&switch.set_state(false);
            glib::Continue(false)
        });
    }*/

    // pub fn on_connected(f : Fn)
    pub fn build() -> Self {
        let host = PackedImageEntry::build("preferences-system-network-proxy-symbolic", "Host");
        let db = PackedImageEntry::build("db-symbolic", "Database");
        let cred_bx = Box::new(Orientation::Horizontal, 0);
        let user = PackedImageEntry::build("avatar-default-symbolic", "User");
        let password = PackedImagePasswordEntry::build("dialog-password-symbolic", "Password");
        let switch = Switch::new();
        // super::set_margins(&switch, 6, 12);
        switch.set_valign(Align::Center);
        switch.set_vexpand(false);
        cred_bx.append(&user.bx);
        cred_bx.append(&password.bx);
        cred_bx.append(&switch);
        let title = super::title_label("Credentials");
        let bx = Box::new(Orientation::Vertical, 0);
        bx.append(&title);
        bx.append(&host.bx);
        bx.append(&db.bx);
        bx.append(&cred_bx);
        bx.set_margin_bottom(36);

        host.entry.set_hexpand(true);
        db.entry.set_hexpand(true);
        user.entry.set_hexpand(true);
        password.entry.set_hexpand(true);

        let conn_bx = ConnectionBox {
            host,
            user,
            db,
            password,
            bx,
            switch
        };
        conn_bx.set_sensitive(false);
        conn_bx
    }

    pub fn entries<'a>(&'a self) -> [&'a Entry; 3] {
        [&self.host.entry, &self.db.entry, &self.user.entry]
    }

    pub fn password_entry<'a>(&'a self) -> &'a PasswordEntry {
        &self.password.entry
    }

    fn set_db_loaded_mode(&self) {
        self.entries().iter().for_each(|entry| entry.set_sensitive(false) );
        // self.db_file_btn.set_sensitive(false);
    }

    pub fn set_non_db_mode(&self) {
        self.entries().iter().for_each(|entry| entry.set_sensitive(true) );
        // self.db_file_btn.set_sensitive(true);
        // self.connected.set(false);
        // if let Ok(mut db_p) = self.db_path.try_borrow_mut() {
        //    *db_p = Vec::new();
        //}  else {
        //    println!("Could not get mutable reference to db path");
        //}
    }

    pub fn update_info(&self, info : &ConnectionInfo) {
        self.user.entry.set_text(&info.user);
        self.host.entry.set_text(&info.host);
        self.db.entry.set_text(&info.database);
        self.password.entry.set_text("");
    }

    fn check_entries_clear(&self) -> bool {
        for entry in self.entries().iter().take(3) {
            let txt = entry.text().to_string();
            if !txt.is_empty() {
                return false;
            }
        }
        true
    }

    fn set_sensitive(&self, sensitive : bool) {
        self.host.entry.set_sensitive(sensitive);
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
                if let Some((sel_ix, sel_info)) = opt_sel {
                    conn_bx.set_sensitive(true);
                    if sel_info.is_default() {
                        conn_bx.host.entry.set_text("");
                        conn_bx.db.entry.set_text("");
                        conn_bx.user.entry.set_text("");
                        conn_bx.password.entry.set_text("");
                    } else {
                        conn_bx.host.entry.set_text(&sel_info.host);
                        conn_bx.db.entry.set_text(&sel_info.database);
                        conn_bx.user.entry.set_text(&sel_info.user);
                        conn_bx.password.entry.set_text("");
                        conn_bx.password.entry.grab_focus();
                    }
                } else {
                    conn_bx.host.entry.set_text("");
                    conn_bx.db.entry.set_text("");
                    conn_bx.user.entry.set_text("");
                    conn_bx.password.entry.set_text("");
                    conn_bx.set_sensitive(false);
                }
            }
        });
        connections.connect_added({
            let conn_bx = self.clone();
            move |info| {
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

/*
{
        let conn_popover = self.clone();
        self.db_file_dialog.connect_response(move |dialog, resp| {
            match resp {
                ResponseType::Other(1) => {
                    let fnames = dialog.get_filenames();
                    if let Ok(mut db_p) = conn_popover.db_path.try_borrow_mut() {
                        if fnames.len() >= 1 {
                            conn_popover.clear_entries();
                            db_p.clear();
                            db_p.extend(fnames.clone());
                            let path = &fnames[0];
                            let db_name = if let Some(ext) = path.extension().map(|ext| ext.to_str()) {
                                match ext {
                                    Some("csv") | Some("txt") => {
                                        "In-memory"
                                    },
                                    Some("db") | Some("sqlite3") | Some("sqlite") => {
                                        if let Some(path_str) = path.to_str() {
                                            path_str
                                        } else {
                                            "(Non UTF-8 path)"
                                        }
                                    },
                                    _ => {
                                        "(Unknown extension)"
                                    }
                                }
                            } else {
                                "(Unknown extension)"
                            };
                            conn_popover.entries[3].set_text(db_name);
                        }
                    } else {
                        println!("Failed to get lock over db path");
                    }
                },
                _ => { }
            }
        });
    }
    */
    /*
    fn create_csv_vtab(path : PathBuf, t_env : &mut TableEnvironment, status_stack : StatusStack, switch : Switch) {
    let opt_name = path.clone().file_name()
        .and_then(|n| n.to_str() )
        .map(|n| n.to_string() )
        .and_then(|name| name.split('.').next().map(|n| n.to_string()) );
    if let Some(name) = opt_name {
        if let Err(e) = t_env.create_csv_table(path, &name) {
            println!("{}", e);
            status_stack.update(Status::ClientErr(e));
            Self::disconnect_with_delay(switch.clone());
        }
    } else {
        println!("Error retrieving table name from: {:?}", path);
    }
}

fn _upload_csv(path : PathBuf, t_env : &mut TableEnvironment, status_stack : StatusStack, switch : Switch) {
    if let Some(name) = path.clone().file_name().map(|n| n.to_str()) {
        if let Some(name) = name.map(|n| n.split('.').next()) {
            if let Some(name) = name {
                let mut content = String::new();
                if let Ok(mut f) = File::open(path) {
                    if let Ok(_) = f.read_to_string(&mut content) {
                        let t = Table::new_from_text(content);
                        match t {
                            Ok(t) => {
                                match t.sql_string(name) {
                                    Ok(sql) => {
                                        // TODO there is a bug here when the user executes the first query, because
                                        // the first call to indle callback will retrieve the create/insert statements,
                                        // not the actual user query.
                                        if let Err(e) = t_env.prepare_and_send_query(sql, HashMap::new(), false) {
                                            status_stack.update(Status::ClientErr(e));
                                        }
                                    },
                                    Err(e) =>  {
                                        status_stack.update(Status::ClientErr(
                                            format!("Failed to generate SQL: {}", e)
                                        ));
                                        Self::disconnect_with_delay(switch.clone());
                                    }
                                }
                            },
                            Err(e) => {
                                status_stack.update(Status::ClientErr(
                                    format!("Could not generate SQL: {}", e))
                                );
                                Self::disconnect_with_delay(switch.clone());
                            }
                        }
                    } else {
                        status_stack.update(Status::ClientErr(
                            format!("Could not read CSV content to string"))
                        );
                        Self::disconnect_with_delay(switch.clone());
                    }
                } else {
                    status_stack.update(Status::ClientErr(
                        format!("Could not open file"))
                    );
                    Self::disconnect_with_delay(switch.clone());
                }
            } else {
                println!("Could not get mutable reference to tenv or recover file name");
            }
        } else {
            status_stack.update(Status::ClientErr(
                format!("File should have any of the extensions: .csv|.db|.sqlite"))
            );
            Self::disconnect_with_delay(switch.clone());
        }
    } else {
        println!("Could not recover file name as string");
    }
}
*/

fn disconnect_with_delay(switch : Switch) {
    glib::timeout_add_local(Duration::from_millis(160), move || {
        switch.set_state(false);
        glib::Continue(false)
    });
}
