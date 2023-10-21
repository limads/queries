/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use gtk4::prelude::*;
use gtk4::*;
use libadwaita;
use stateful::React;
use super::MainMenu;
use sourceview5;
use libadwaita::prelude::*;
use std::cell::RefCell;
use std::rc::Rc;
use serde::{Serialize, Deserialize};
use libadwaita::ExpanderRow;
use crate::client::*;
use itertools::Itertools;

#[derive(Debug, Clone)]
pub struct SettingsWindow {
    pub dialog : Dialog,
    pub list : ListBox,
    stack : Stack,
    _paned : Paned
}

impl SettingsWindow {

    pub fn dialog(&self) -> &Dialog {
        &self.dialog
    }

    pub fn build(names : &'static [&'static str]) -> Self {
        let dialog = Dialog::new();
        dialog.set_title(Some("Settings"));
        dialog.set_default_width(1024);
        dialog.set_default_height(768);
        dialog.set_modal(true);
        dialog.set_deletable(true);
        dialog.set_destroy_with_parent(true);
        dialog.set_hide_on_close(true);
        let list = ListBox::new();
        list.set_selection_mode(SelectionMode::Single);
        list.set_width_request(200);
        for name in names {
            list.append(&build_settings_row(name));
        }

        let stack = Stack::new();
        stack.set_halign(Align::Fill);
        stack.set_hexpand(true);
        list.connect_row_selected({
            let stack = stack.clone();
            move |_list, opt_row| {
                if let Some(row) = opt_row {
                    let row_ix = row.index();
                    if row_ix >= 0 {
                        if let Some(selected_name) = names.get(row_ix as usize) {
                            stack.set_visible_child_name(selected_name);
                        }
                    } else {
                        eprintln!("Negative row index");
                    }
                }
            }
        });
        let paned = Paned::new(Orientation::Horizontal);
        paned.set_halign(Align::Fill);
        paned.set_hexpand(true);
        paned.set_position(200);
        paned.set_start_child(Some(&list));
        paned.set_end_child(Some(&stack));
        dialog.set_child(Some(&paned));
        Self { dialog, list : list, stack, _paned : paned }
    }

}

#[derive(Debug, Clone)]
pub struct NamedBox<W : IsA<Widget>> {
    pub bx : Box,
    pub w : W
}

impl<W: IsA<Widget>> NamedBox<W> {

    pub fn new(name : &str, subtitle : Option<&str>, w : W) -> Self {
        let label_bx = Box::new(Orientation::Vertical, 0);
        let label = Label::new(Some(&format!("<span>{}</span>", name)));
        label.set_justify(Justification::Left);
        label.set_halign(Align::Start);
        label.set_use_markup(true);
        label_bx.append(&label);

        if let Some(subtitle) = subtitle {
            let label = Label::new(Some(&format!("<span font_size='small' foreground='grey'>{}</span>", subtitle)));
            label.set_use_markup(true);
            label.set_justify(Justification::Left);
            label.set_halign(Align::Start);
            label_bx.append(&label);
        }

        label_bx.set_halign(Align::Start);

        let bx = Box::new(Orientation::Horizontal, 0);
        bx.set_halign(Align::Fill);
        bx.set_hexpand(true);

        bx.append(&label_bx);
        bx.append(&w);
        super::set_margins(&bx, 6, 6);

        w.set_halign(Align::End);
        w.set_hexpand(true);
        w.set_vexpand(false);
        w.set_valign(Align::Center);

        Self { bx, w }
    }

}

#[derive(Debug, Clone)]
pub struct ConnBox {
    list : ListBox,
    pub app_name_entry : Entry,
    pub timeout_scale : Scale,
    pub save_switch : Switch
}

impl ConnBox {

    pub fn build() -> Self {
        let timeout_scale = Scale::with_range(Orientation::Horizontal, 10.0, 60.0, 1.0);
        timeout_scale.set_width_request(240);
        timeout_scale.set_draw_value(true);
        timeout_scale.set_value_pos(PositionType::Top);
        let app_name_entry = Entry::new();
        let list = ListBox::new();
        configure_list(&list);
        list.append(&NamedBox::new("Application name", Some("Name used to identify the client application\nto the server."), app_name_entry.clone()).bx);
        list.append(&NamedBox::new("Connection timeout", Some("Maximum number of seconds to wait for a server\nreply when establishing connections"), timeout_scale.clone()).bx);
        set_all_not_selectable(&list);
        let save_row = ListBoxRow::new();
        save_row.set_selectable(false);
        let save_switch = Switch::new();
        list.append(&NamedBox::new("Remember credentials", Some("Store credentials (except passwords)\nand load them at future sessions"), save_switch.clone()).bx);
        Self { app_name_entry, timeout_scale, list, save_switch  }
    }
    
}

#[derive(Debug, Clone)]
pub struct EditorBox {
    pub list : ListBox,
    pub scheme_combo : ComboBoxText,
    pub font_btn : FontButton,
    pub line_num_switch : Switch,
    pub line_highlight_switch : Switch,
    pub split_switch : Switch
}

pub fn configure_list(list : &ListBox) {
    list.set_halign(Align::Center);
    list.set_valign(Align::Center);
    list.set_hexpand(true);
    list.set_vexpand(true);
    list.style_context().add_class("boxed-list");
    list.set_width_request(600);
}

impl EditorBox {

    pub fn build() -> Self {
        let list = ListBox::new();
        configure_list(&list);

        let font_btn = FontButton::new();
        font_btn.set_use_font(true);
        font_btn.set_use_size(true);

        let manager = sourceview5::StyleSchemeManager::new();
        let scheme_combo = ComboBoxText::new();
        for id in manager.scheme_ids() {
            scheme_combo.append(Some(&id), &id);
        }

        let split_switch = Switch::new();
        list.append(&NamedBox::new("Spit view", Some("Show editor on the same screen as the workspace"), split_switch.clone()).bx);

        list.append(&NamedBox::new("Color scheme", None, scheme_combo.clone()).bx);
        list.append(&NamedBox::new("Font", None, font_btn.clone()).bx);

        let line_num_switch = Switch::new();
        let line_highlight_switch = Switch::new();

        list.append(&NamedBox::new("Show line numbers", None, line_num_switch.clone()).bx);
        list.append(&NamedBox::new("Highlight current line", None, line_highlight_switch.clone()).bx);

        set_all_not_selectable(&list);
        
        Self { list, scheme_combo, font_btn, line_num_switch, line_highlight_switch, split_switch }
    }

}

#[derive(Debug, Clone)]
pub struct ExecutionBox {
    pub list : ListBox,
    pub row_limit_spin : SpinButton,
    pub schedule_scale : Scale,
    pub timeout_scale : Scale,
    pub dml_switch : Switch,
    pub ddl_switch : Switch,
    pub async_switch : Switch,
    pub json_switch : Switch
}

impl ExecutionBox {

    pub fn build() -> Self {
        let list = ListBox::new();
        configure_list(&list);
        let row_limit_spin = SpinButton::with_range(0.0, 10_000.0, 1.0);
        row_limit_spin.set_digits(0);
        row_limit_spin.set_value(500.);

        // let col_limit_spin = SpinButton::with_range(0.0, 100.0, 1.0);
        // col_limit_spin.set_digits(0);
        // col_limit_spin.set_value(25.);

        let schedule_scale = Scale::with_range(Orientation::Horizontal, 1.0, 30.0, 1.0);
        schedule_scale.set_width_request(240);
        schedule_scale.set_draw_value(true);
        schedule_scale.set_value_pos(PositionType::Top);

        let timeout_scale = Scale::with_range(Orientation::Horizontal, 1.0, 30.0, 1.0);
        timeout_scale.set_width_request(240);
        timeout_scale.set_draw_value(true);
        timeout_scale.set_value_pos(PositionType::Top);

        // let overflow_combo = ComboBoxText::new();
        // overflow_combo.append_text("Head (first rows)");
        // overflow_combo.append_text("Tail (last rows)");
        // overflow_combo.append_text("Random sample (ordered)");

        list.append(&NamedBox::new("Row limit", None, row_limit_spin.clone()).bx);
        // list.append(&NamedBox::new("Column limit", None, col_limit_spin.clone()).bx);
        // list.append(&NamedBox::new("Row overflow", Some("Which rows to display when results\n extrapolate the row limit"), schedule_scale.clone()).bx);
        list.append(&NamedBox::new("Schedule interval", Some("Interval (in seconds)\nbetween scheduled executions"), schedule_scale.clone()).bx);
        list.append(&NamedBox::new("Statement timeout", Some("Maximum time (in seconds)\nto wait for database response"), timeout_scale.clone()).bx);
        
        let dml_switch = Switch::new();
        let ddl_switch = Switch::new();
        let async_switch = Switch::new();
        let json_switch = Switch::new();

        list.append(&NamedBox::new("Enable UPDATE and DELETE", Some("Allow execution of potentially destructive \ndata modification statements\n"), dml_switch.clone()).bx);
        list.append(&NamedBox::new("Enable ALTER, DROP and TRUNCATE", Some("Allow execution of potentially destructive \ndata definition statements\n"), ddl_switch.clone()).bx);
        list.append(&NamedBox::new("Enable asynchronous queries", Some("Execute SELECT statements asynchronously when possible"), async_switch.clone()).bx);

        list.append(&NamedBox::new("Unroll JSON", Some("Unroll queries resulting in\nsingle-column JSON values"), json_switch.clone()).bx);

        set_all_not_selectable(&list);
        
        Self { list, row_limit_spin, /*col_limit_spin*/ schedule_scale, timeout_scale, dml_switch, ddl_switch, async_switch, json_switch }
    }

}

pub fn set_all_not_selectable(list : &ListBox) {
    let mut ix = 0;
    while let Some(r) = list.row_at_index(ix) {
        r.set_selectable(false);
        ix += 1;
    }
}

const TLS_DISABLED : &str = "Disabled";

const TLS_V10 : &str = "TLS (Version ≥ 1.0)";

const TLS_V11 : &str = "TLS (Version ≥ 1.1)";

const TLS_V12 : &str = "TLS (Version ≥ 1.2)";

/* A security settings row, generated dynamically every time
the settings window is opened. */
#[derive(Debug, Clone)]
pub struct SecurityRow {
    pub exp_row : ExpanderRow,
    pub tls_combo : ComboBoxText,
    pub hostname_switch : Switch,
    pub cert_entry : Entry
}

const TLS_MSG : &str =
r#"Connecting with TLS disabled is only supported
for hosts accessible locally or through a private network."#;

const HOSTNAME_MSG : &str =
r#"Disabling verification is discouraged, unless
you are connecting through a trusted network."#;

const CERT_MSG : &str =
r#"Path to the root certificate
(.crt or .pem file)."#;

/* The security settings work a bit differently, since its menu is generated
dynamically every time the settings window is opened. Any changes in the new
dynamically generated security form calls the security_update action carrying
this serialized enum as its parameter. The SharedUserState then listen for
the activated signal in this enum. */
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SecurityChange {
    Hostname { host : String, verify : Option<bool> },
    TLSVersion { host : String, version : Option<TlsVersion> },
    Certificate { host : String, path : Option<String> }
}

impl SecurityChange {

    pub fn host(&self) -> &str {
        match self {
            SecurityChange::Hostname { ref host, .. } => &host[..],
            SecurityChange::TLSVersion { ref host, .. } => &host[..],
            SecurityChange::Certificate { ref host, .. } => &host[..]
        }
    }

}

/* If this connection has a hostname that matches the hostname
targeted by the change, modify it. Keep it unchanged otherwise.
Returns a bool specifying whether it was modified. */
pub fn try_modify_security_for_conn(
    conn : &mut ConnectionInfo,
    change : &SecurityChange
) -> bool {
    match change {

        SecurityChange::TLSVersion { ref host, version } => {
            if &host[..] == &conn.host[..] {
                conn.security.tls_version = version.clone();
                if version.is_none() {
                    conn.security.cert_path = None;
                    conn.security.verify_hostname = None;
                }
                true
            } else {
                false
            }
        },
        SecurityChange::Hostname { ref host, verify } => {
            if &host[..] == &conn.host[..] {
                if (conn.security.tls_version.is_some() && verify.is_some()) ||
                    verify.is_none()
                {
                    conn.security.verify_hostname = verify.clone();
                    true
                } else {
                    false
                }
            } else {
                false
            }
        },

        SecurityChange::Certificate { ref host, path } => {
            if &host[..] == &conn.host[..] {
                if (conn.security.tls_version.is_some() && path.is_some()) ||
                    path.is_none()
                {
                    conn.security.cert_path = path.clone();
                    true
                } else {
                    false
                }
            } else {
                false
            }
        }

    }
}

fn update_security_row_info(exp_row : &ExpanderRow, info : &ConnectionInfo) {
    exp_row.set_title(&info.host);
    exp_row.set_subtitle(&info.description());
    let icon = match info.security.tls_version.is_some() {
        true => "padlock2-symbolic",
        false => "padlock2-open-symbolic"
    };
    exp_row.set_icon_name(Some(icon));
}

impl SecurityRow {

    pub fn new(info : ConnectionInfo, action : &gio::SimpleAction) -> Self {
        let exp_row = libadwaita::ExpanderRow::new();
        exp_row.set_selectable(false);
        update_security_row_info(&exp_row, &info);
        let tls_combo = ComboBoxText::new();
        let hostname_switch = Switch::new();
        let cert_entry = Entry::new();
        cert_entry.set_sensitive(info.security.tls_version.is_some());
        hostname_switch.set_sensitive(info.security.tls_version.is_some());
        cert_entry.set_primary_icon_name(Some("application-certificate-symbolic"));
        cert_entry.set_placeholder_text(Some("~/certificate.pem"));
        cert_entry.set_max_width_chars(40);

        let enc_bx = NamedBox::new("Encryption", Some(TLS_MSG), tls_combo.clone());
        let cert_bx = NamedBox::new("Certificate", Some(CERT_MSG), cert_entry.clone());
        let hostname_bx = NamedBox::new("Verify hostname", Some(HOSTNAME_MSG), hostname_switch.clone());
        let rows = [ListBoxRow::new(), ListBoxRow::new(), ListBoxRow::new()];
        rows[0].set_child(Some(&enc_bx.bx));
        rows[1].set_child(Some(&cert_bx.bx));
        rows[2].set_child(Some(&hostname_bx.bx));
        for r in &rows {
            r.set_selectable(false);
            r.set_activatable(false);
            exp_row.add_row(r);
        }
        for (id, mode) in [("0", TLS_DISABLED), ("1", TLS_V10), ("2", TLS_V11), ("3", TLS_V12)] {
            tls_combo.append(Some(id), mode);
        }
        if let Some(path) = info.security.cert_path.as_ref() {
            cert_entry.set_text(&path[..]);
        } else {
            cert_entry.set_text("");
        }
        if let Some(verify) = info.security.verify_hostname {
            hostname_switch.set_state(verify);
        } else {
            hostname_switch.set_state(false);
        }
        match info.security.tls_version {
            None => {
                tls_combo.set_active_id(Some("0"));
            },
            Some(TlsVersion { major : 1, minor : 0 }) => {
                tls_combo.set_active_id(Some("1"));
            },
            Some(TlsVersion { major : 1, minor : 1 }) => {
                tls_combo.set_active_id(Some("2"));
            },
            Some(TlsVersion { major : 1, minor : 2 }) => {
                tls_combo.set_active_id(Some("3"));
            },
            _ => {
                tls_combo.set_active_id(Some("3"));
            }
        }
        tls_combo.connect_changed({
            let action = action.clone();
            let exp_row = exp_row.clone();
            let host = info.host.to_string();
            let cert_entry = cert_entry.clone();
            let hostname_switch = hostname_switch.clone();
            move |tls_combo| {
                let active_txt = tls_combo.active_text();
                if let Some(txt) = active_txt {
                    let (version, icon) = match &txt[..] {
                        TLS_DISABLED => {
                            (None, "padlock2-open-symbolic")
                        },
                        TLS_V10 => {
                            (Some(TlsVersion { major : 1, minor : 0 }), "padlock2-symbolic")
                        },
                        TLS_V11 => {
                            (Some(TlsVersion { major : 1, minor : 1 }), "padlock2-symbolic")
                        },
                        TLS_V12 => {
                            (Some(TlsVersion { major : 1, minor : 2 }), "padlock2-symbolic")
                        },
                        _ => {
                            (Some(TlsVersion { major : 1, minor : 2 }), "padlock2-symbolic")
                        }
                    };
                    exp_row.set_icon_name(Some(icon));

                    // Those changes should be done before the state is updated,
                    // since the update signal will be sent from the callback of the
                    // hostname and cert widgets, and will be reject unless the encryption
                    // state is active.
                    if version.is_none() {
                        if hostname_switch.is_active() {
                            hostname_switch.set_active(false);
                        }
                        if !cert_entry.text().is_empty() {
                            cert_entry.set_text("");
                        }
                        hostname_switch.set_sensitive(false);
                        cert_entry.set_sensitive(false);
                    }

                    // Do the actual encryption change.
                    let change = SecurityChange::TLSVersion { host : host.clone(), version };
                    action.activate(Some(&serde_json::to_string(&change).unwrap().to_variant()));

                    // This is a non-encrypted -> encrypted change. Set the
                    // hostname verification to true, according to the default
                    // security setting. If hostname switch is sensitive, however,
                    // keep the current switch state.
                    if version.is_some() && !hostname_switch.is_sensitive() {
                        hostname_switch.set_sensitive(true);
                        hostname_switch.set_active(true);
                    }
                    if version.is_some() && !cert_entry.is_sensitive() {
                        cert_entry.set_sensitive(true);
                        cert_entry.set_text("");
                    }
                }
            }
        });
        cert_entry.connect_changed({
            let action = action.clone();
            let host = info.host.to_string();
            move |entry| {
                let txt = entry.buffer().text().to_string();
                let opt_cert = if txt.is_empty() {
                    None
                } else {
                    Some(txt.to_string())
                };
                let change = SecurityChange::Certificate { host : host.clone(), path : opt_cert };
                action.activate(Some(&serde_json::to_string(&change).unwrap().to_variant()));
            }
        });
        hostname_switch.connect_state_set({
            let action = action.clone();
            let host = info.host.to_string();
            let _exp_row = exp_row.clone();
            move |switch, _| {
                let change = SecurityChange::Hostname { host : host.clone(), verify : Some(switch.is_active()) };
                action.activate(Some(&serde_json::to_string(&change).unwrap().to_variant()));
                glib::signal::Propagation::Proceed
            }
        });

        // Update exp_row info if change is applicable to this ConnectionInfo.
        let info = Rc::new(RefCell::new(info));
        action.connect_activate({
            let exp_row = exp_row.clone();
            let info = info.clone();
            move |_, param| {
                if let Some(param) = param {
                    let change : SecurityChange = serde_json::from_str(&param.get::<String>().unwrap()).unwrap();
                    let mut info = info.borrow_mut();
                    if try_modify_security_for_conn(&mut info, &change) {
                        update_security_row_info(&exp_row, &info);
                    }
                }

            }
        });

        /*// Verify if the user state is consistent. This assumes the call order
        // of this callback is always after the
        action.connect_activate({
            let info = info.clone();
            move |_, param| {
                if let Some(param) = param {
                    let change : SecurityChange = serde_json::from_str(&param.get::<String>().unwrap()).unwrap();
                    let info = info.borrow_mut();
            }
        });*/

        Self { exp_row, tls_combo, hostname_switch, cert_entry }
    }

}

#[derive(Debug, Clone)]
pub struct SecurityBox {
    pub list : ListBox,
    pub scrolled : ScrolledWindow,
    pub update_action : gio::SimpleAction
}

impl SecurityBox {

    pub fn update(&self, conns : &[ConnectionInfo]) {
        while let Some(row) = self.list.row_at_index(0) {
            self.list.remove(&row);
        }
        let mut n_added = 0;
        for conn in conns.iter().sorted_by(|a, b| a.host.cmp(&b.host) ).unique_by(|c| &c.host[..] ) {
            let must_add = !conn.host.is_empty() &&
                &conn.host[..] != crate::client::DEFAULT_HOST
                && !conn.is_file();
            if must_add {
                let sec_row = SecurityRow::new(conn.clone(), &self.update_action);
                self.list.append(&sec_row.exp_row);
                n_added += 1;
            }
        }
        if n_added == 0 {
            let lbl = Label::builder().use_markup(true).build();
            lbl.set_margin_bottom(18);
            lbl.set_margin_top(18);
            lbl.set_markup("No configurable hosts included yet");
            let row = ListBoxRow::new();
            row.set_child(Some(&lbl));
            row.set_selectable(false);
            row.set_activatable(false);
            self.list.append(&row);
        }
    }

    pub fn build() -> Self {
        let scrolled = ScrolledWindow::new();
        let list = ListBox::new();
        scrolled.set_child(Some(&list));
        configure_list(&list);
        let update_action = gio::SimpleAction::new("security_update", Some(&glib::VariantTy::STRING));
        Self { list, update_action, scrolled }
    }

}

#[derive(Debug, Clone)]
pub struct QueriesSettings {
    pub settings : SettingsWindow,
    pub conn_bx : ConnBox,
    pub exec_bx : ExecutionBox,
    pub editor_bx : EditorBox,
    pub security_bx : SecurityBox
}

const SETTINGS : [&'static str; 4] = ["Connection", "Editor", "Statements", "Security"];

impl QueriesSettings {

    pub fn build() -> Self {
        let settings = SettingsWindow::build(&SETTINGS[..]);
        let conn_bx = ConnBox::build();
        let editor_bx = EditorBox::build();
        let exec_bx = ExecutionBox::build();
        let security_bx = SecurityBox::build();
        settings.stack.add_named(&conn_bx.list, Some(SETTINGS[0]));
        settings.stack.add_named(&editor_bx.list, Some(SETTINGS[1]));
        settings.stack.add_named(&exec_bx.list, Some(SETTINGS[2]));
        settings.stack.add_named(&security_bx.scrolled, Some(SETTINGS[3]));
        Self { settings, conn_bx, editor_bx, exec_bx, security_bx }
    }

}

impl React<MainMenu> for QueriesSettings {

    fn react(&self, menu : &MainMenu) {
        let dialog = self.settings.dialog.clone();
        menu.action_settings.connect_activate(move |_,_| {
            dialog.show();
        });
    }

}

fn build_settings_row(name : &str) -> ListBoxRow {
    let lbl = Label::builder()
        .label(name)
        .halign(Align::Start)
        .margin_start(6)
        .justify(Justification::Left)
        .build();
    ListBoxRow::builder().child(&lbl).height_request(42).build()
}




