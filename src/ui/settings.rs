/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use gtk4::prelude::*;
use gtk4::*;
use libadwaita;
use stateful::React;
use super::MainMenu;
use sourceview5;
use std::thread;
use std::path::Path;
use std::sync::mpsc;
use libadwaita::prelude::*;
use std::cell::RefCell;
use std::rc::Rc;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone)]
pub struct SettingsWindow {
    dialog : Dialog,
    _list : ListBox,
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
        Self { dialog, _list : list, stack, _paned : paned }
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
    pub timeout_scale : Scale
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
        Self { app_name_entry, timeout_scale, list }
    }
    
}

#[derive(Debug, Clone)]
pub struct EditorBox {
    pub list : ListBox,
    pub scheme_combo : ComboBoxText,
    pub font_btn : FontButton,
    pub line_num_switch : Switch,
    pub line_highlight_switch : Switch,
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
        list.append(&NamedBox::new("Color scheme", None, scheme_combo.clone()).bx);
        list.append(&NamedBox::new("Font", None, font_btn.clone()).bx);

        let line_num_switch = Switch::new();
        let line_highlight_switch = Switch::new();

        list.append(&NamedBox::new("Show line numbers", None, line_num_switch.clone()).bx);
        list.append(&NamedBox::new("Highlight current line", None, line_highlight_switch.clone()).bx);

        set_all_not_selectable(&list);
        
        Self { list, scheme_combo, font_btn, line_num_switch, line_highlight_switch }
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
    pub async_switch : Switch
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

        list.append(&NamedBox::new("Enable UPDATE and DELETE", Some("Allow execution of potentially destructive \ndata modification statements\n"), dml_switch.clone()).bx);
        list.append(&NamedBox::new("Enable ALTER, DROP and TRUNCATE", Some("Allow execution of potentially destructive \ndata definition statements\n"), ddl_switch.clone()).bx);
        list.append(&NamedBox::new("Enable asynchronous queries", Some("Execute SELECT statements asynchronously when possible"), async_switch.clone()).bx);

        set_all_not_selectable(&list);
        
        Self { list, row_limit_spin, /*col_limit_spin*/ schedule_scale, timeout_scale, dml_switch, ddl_switch, async_switch }
    }

}

#[derive(Debug, Clone)]
pub struct EditableCombo {
    pub bx : Box,
    pub combo : ComboBoxText
}

impl EditableCombo {

    fn build() -> Self {
        let bx = super::ButtonPairBox::build("list-remove-symbolic", "list-add-symbolic");
        let combo = ComboBoxText::with_entry();
        let combo_entry = combo.child().unwrap().downcast::<Entry>().unwrap();

        let (path_send, path_recv) = mpsc::channel::<String>();
        let (exists_send, exists_recv) = glib::MainContext::channel::<bool>(glib::source::PRIORITY_DEFAULT);
        thread::spawn(move || {
            loop {
                if let Ok(path) = path_recv.recv() {
                    if Path::new(&path).exists() {
                        exists_send.send(true).unwrap();
                    } else {
                        exists_send.send(false).unwrap();
                    }
                }
            }
        });

        exists_recv.attach(None, {
            let combo = combo.clone();
            move |exists| {
                // TODO perhaps receive custom validator function in addition to exists.
                if combo.active_text().is_some() {
                    if exists {
                        combo.style_context().add_class("success");
                    } else {
                        combo.style_context().add_class("error");
                    }
                } else {
                    combo.style_context().add_class("regular");
                }
                Continue(true)
            }
        });
        let (remove_btn, add_btn) = (bx.left_btn.clone(), bx.right_btn.clone());
        remove_btn.connect_clicked({
            let combo = combo.clone();
            let combo_entry = combo_entry.clone();
            move |_| {
                if let Some(id) = combo.active_id() {
                    combo_entry.set_text("");
                    combo.remove(id.parse::<i32>().unwrap());
                    combo.set_active_id(None);
                }

            }
        });
        add_btn.connect_clicked({
            let combo = combo.clone();
            let combo_entry = combo_entry.clone();
            move |_| {
                if let Some(model) = combo.model() {
                    let mut n = 0;
                    model.foreach(|_, _, _| { n += 1; false } );
                    let id = n.to_string();
                    let text = combo_entry.text();
                    let txt = text.as_str();
                    if !txt.is_empty() {
                        combo.append(Some(&id), txt);
                        combo_entry.set_text("");
                        combo.set_active_id(Some(&id));
                    }
                }
            }
        });
        combo.connect_changed({
            move |combo| {
                if let Some(txt) = combo.active_text() {
                    path_send.send(txt.as_str().to_string()).unwrap();
                }
            }
        });
        combo_entry.connect_changed({
            let add_btn = add_btn.clone();
            let remove_btn = remove_btn.clone();
            move |entry| {
                let text = entry.text();
                let txt = text.as_str();
                if txt.is_empty() {
                    add_btn.set_sensitive(false);
                    remove_btn.set_sensitive(false);
                } else {
                    add_btn.set_sensitive(true);
                    remove_btn.set_sensitive(true);
                }
            }
        });

        bx.bx.prepend(&combo);
        Self { bx : bx.bx, combo }
    }

}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Copy, Hash)]
pub enum SSLMode {

    #[serde(rename="require")]
    Require,
    
    #[serde(rename="verify-ca")]
    VerifyCA,
    
    #[serde(rename="verify-full")]
    VerifyFull
}

impl std::string::ToString for SSLMode {

    fn to_string(&self) -> String {
        match self {
            Self::Require => format!("require"),
            Self::VerifyCA => format!("verify-ca"),
            Self::VerifyFull => format!("verify-full")
        }
    }
    
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TlsVersion {
    pub major : usize,
    pub minor : usize
}

impl std::string::ToString for TlsVersion {

    fn to_string(&self) -> String {
        format!("{}.{}", self.major, self.minor)
    }
    
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Certificate {
    pub host : String,
    pub cert : String,
    pub min_version : TlsVersion
}

#[derive(Debug, Clone)]
pub struct SecurityBox {
    pub list : ListBox,
    pub scrolled : ScrolledWindow,
    pub cert_added : gio::SimpleAction,
    pub cert_removed : gio::SimpleAction,
    pub exp_row : libadwaita::ExpanderRow,
    pub save_switch : Switch,
    pub version_combo : ComboBoxText,
   
    // pub ssl_switch : Switch,

    // TODO remove the rows list if/when libadwaita API allows recovering the ExpanderRow rows.
    // While this is not possible, we must keep a shared reference to the rows to remove
    // them.
    pub rows : Rc<RefCell<Vec<ListBoxRow>>>

}

fn validate((host, cert, _mode) : &(String, String, TlsVersion), rows : &[ListBoxRow]) -> bool {

    for row in rows.iter() {
        let bx = row.child().unwrap().clone().downcast::<Box>().unwrap();
        let bx_entries = super::get_child_by_index::<Box>(&bx, 0);
        let bx_top = super::get_child_by_index::<Box>(&bx_entries, 0);
        let lbl = super::get_child_by_index::<Label>(&bx_top, 1);
        if &lbl.text()[..] == &host[..] {
            return false;
        }
    }

    !host.is_empty() && cert.chars().count() > 4 /*&& cert.ends_with(".crt") || cert.ends_with(".pem") */
}

pub fn append_certificate_row(
    exp_row : libadwaita::ExpanderRow, 
    host : &str, 
    cert : &str, 
    min_version : TlsVersion, 
    rows : &Rc<RefCell<Vec<ListBoxRow>>>,
    cert_added : &gio::SimpleAction,
    cert_removed : &gio::SimpleAction
) {

    let mut cert_str = cert.to_string();
    cert_str += &format!(" (≥ TLS {})", min_version.to_string());
    
    let lbl_host = Label::new(Some(host));
    let lbl_cert = Label::new(Some(&cert_str));

    super::set_margins(&lbl_host, 0, 12);
    super::set_margins(&lbl_cert, 0, 12);
    let host_img = Image::from_icon_name("preferences-system-network-proxy-symbolic");
    let cert_img = Image::from_icon_name("application-certificate-symbolic");
    super::set_margins(&host_img, 12, 0);
    super::set_margins(&cert_img, 12, 0);

    let row = ListBoxRow::new();
    row.set_selectable(false);
    let bx = Box::new(Orientation::Horizontal, 0);
    
    let bx_top = Box::new(Orientation::Horizontal, 0);
    bx_top.append(&host_img);
    bx_top.append(&lbl_host);
    bx_top.set_hexpand(true);
    bx_top.set_halign(Align::Start);
    
    let bx_bottom = Box::new(Orientation::Horizontal, 0);
    bx_bottom.append(&cert_img);
    bx_bottom.append(&lbl_cert);
    bx_bottom.set_hexpand(true);
    bx_bottom.set_halign(Align::Start);
    
    row.set_child(Some(&bx));
    exp_row.add_row(&row);
    rows.borrow_mut().push(row.clone());

    let ev = EventControllerMotion::new();
    let exclude_btn = Button::builder().icon_name("user-trash-symbolic").build();
    exclude_btn.set_hexpand(false);
    exclude_btn.set_halign(Align::End);
    exclude_btn.style_context().add_class("flat");
    
    let bx_entries = Box::new(Orientation::Vertical, 0);
    bx_entries.append(&bx_top);
    bx_entries.append(&bx_bottom);
    bx_entries.set_hexpand(true);
    bx_entries.set_halign(Align::Fill);
    bx.append(&bx_entries);
    bx.append(&exclude_btn);

    // Account for exclude btn space
    lbl_cert.set_margin_end(34);
    exclude_btn.set_visible(false);
    
    ev.connect_enter({
        let exclude_btn = exclude_btn.clone();
        let lbl_cert = lbl_cert.clone();
        move |_, _, _| {
            exclude_btn.set_visible(true);
            lbl_cert.set_margin_end(0);
        }
    });
    ev.connect_leave({
        let exclude_btn = exclude_btn.clone();
        let lbl_cert = lbl_cert.clone();
        move |_| {
            let w = exclude_btn.allocation().width();
            exclude_btn.set_visible(false);
            lbl_cert.set_margin_end(w);
        }
    });
    row.add_controller(&ev);
    exclude_btn.connect_clicked({
        let exp_row = exp_row.clone();
        let rows = rows.clone();
        let host = host.to_string();
        let cert = cert.to_string();
        let cert_removed = cert_removed.clone();
        move |_| {
            exp_row.remove(&row);
            let mut rows = rows.borrow_mut();
            if let Some(ix) = rows.iter().position(|r| r == &row) {
                rows.remove(ix);
            }

            cert_removed.activate(
                Some(&serde_json::to_string(&Certificate {
                    host : host.clone(),
                    cert : cert.clone(),
                    min_version
                }).unwrap().to_variant())
            );
        }
    });
    
    cert_added.activate(Some(&serde_json::to_string(&Certificate { host : host.to_string(), min_version, cert : cert.to_string() }).unwrap().to_variant()));
}

pub fn set_all_not_selectable(list : &ListBox) {
    let mut ix = 0;
    while let Some(r) = list.row_at_index(ix) {
        r.set_selectable(false);
        ix += 1;
    }
}

const TLS_V10 : &'static str = "≥ TLS 1.0";

const TLS_V11 : &'static str = "≥ TLS 1.1";

const TLS_V12 : &'static str = "≥ TLS 1.2";

impl SecurityBox {

    pub fn build() -> Self {
        let scrolled = ScrolledWindow::new();
        let list = ListBox::new();
        scrolled.set_child(Some(&list));
        configure_list(&list);

        let _combo_bx = EditableCombo::build();

        // TODO just get lisboxrows from list, or else certificates added at startup won't count.
        let rows : Rc<RefCell<Vec<ListBoxRow>>> = Rc::new(RefCell::new(Vec::new()));
        let cert = Rc::new(RefCell::new((String::new(), String::new(), TlsVersion { major : 1, minor : 0 })));
        
        let save_switch = Switch::new();
        let save_row = ListBoxRow::new();
        save_row.set_selectable(false);
        let save_bx = NamedBox::new("Remember credentials", Some("Store credentials (except passwords)\nand load them at future sessions"), save_switch.clone());
        save_row.set_child(Some(&save_bx.bx));

        let exp_row = libadwaita::ExpanderRow::new();
        exp_row.set_selectable(false);
        
        exp_row.set_title("Certificates");
        exp_row.set_subtitle("Associate SSL/TLS certificates to\ndatabase cluster hosts");

        let add_row = ListBoxRow::new();
        add_row.set_selectable(false);
        let add_bx = Box::new(Orientation::Horizontal, 0);
        add_row.set_child(Some(&add_bx));
        let host_entry = Entry::new();
        host_entry.set_hexpand(true);
        host_entry.set_halign(Align::Fill);
        host_entry.set_primary_icon_name(Some("preferences-system-network-proxy-symbolic"));
        host_entry.set_placeholder_text(Some("Host:Port"));

        let cert_entry = Entry::new();
        cert_entry.set_primary_icon_name(Some("application-certificate-symbolic"));
        cert_entry.set_placeholder_text(Some("Root certificate path (.crt or .pem file)"));
        cert_entry.set_hexpand(true);
        cert_entry.set_halign(Align::Fill);

        let add_bx_top = Box::new(Orientation::Horizontal, 0);
        let add_bx_middle = Box::new(Orientation::Horizontal, 0);
        let add_bx_bottom = Box::new(Orientation::Horizontal, 0);
        
        let add_btn = Button::new();
        add_btn.set_sensitive(false);
        add_btn.style_context().add_class("flat");
        add_btn.set_icon_name("list-add-symbolic");
        add_btn.set_sensitive(false);
        add_btn.set_width_request(32);
        super::set_margins(&add_bx, 12, 12);
        add_btn.set_halign(Align::End);
        add_btn.set_hexpand(false);

        let version_combo = ComboBoxText::new();
        
        for (id, mode) in [("0", TLS_V10), ("1", TLS_V11), ("2", TLS_V12)] {
            version_combo.append(Some(id), mode);
        }
        version_combo.connect_changed({
            let add_btn = add_btn.clone();
            let cert = cert.clone();
            move |version_combo| {
                let active_txt = version_combo.active_text();
                add_btn.set_sensitive(active_txt.is_some());
                if let Some(txt) = active_txt {
                    match &txt[..] {
                        TLS_V10 => {
                            cert.borrow_mut().2 = TlsVersion { major : 1, minor : 0 };
                        },
                        TLS_V11 => {
                            cert.borrow_mut().2 = TlsVersion { major : 1, minor : 1 };
                        },
                        TLS_V12 => {
                            cert.borrow_mut().2 = TlsVersion { major : 1, minor : 2 };
                        },
                        _ => { 
                            cert.borrow_mut().2 = TlsVersion { major : 1, minor : 0 };
                        }
                    }
                } else {
                    cert.borrow_mut().2 = TlsVersion { major : 1, minor : 0 };
                }
            }
        });
        version_combo.set_active_id(Some("0"));
        version_combo.set_halign(Align::End);
        add_btn.set_halign(Align::End);
        add_bx_top.append(&host_entry);
        // add_bx_top.set_margin_bottom(12);
        add_bx_middle.append(&cert_entry);
        // version_combo.set_hexpand(true);
        // add_bx_bottom.set_margin_top(6);
        
        // add_bx_middle.append(&Label::new(Some("Minimum TLS version")));
        add_bx_middle.append(&version_combo);
        add_bx_middle.append(&add_btn);
        
        let add_bx_left = Box::new(Orientation::Vertical, 0);
        add_bx_left.append(&add_bx_top);
        add_bx_left.append(&add_bx_middle);
        add_bx_left.append(&add_bx_bottom);
        add_bx.append(&add_bx_left);
        add_bx_top.style_context().add_class("linked");
        add_bx_middle.style_context().add_class("linked");
        add_bx_bottom.style_context().add_class("linked");
        add_bx.style_context().add_class("linked");

        add_bx_left.set_hexpand(true);
        add_bx_left.set_halign(Align::Fill);
        
        host_entry.connect_changed({
            let cert = cert.clone();
            let add_btn = add_btn.clone();
            let _exp_row = exp_row.clone();
            let rows = rows.clone();
            move |entry| {
                let txt = entry.buffer().text().to_string();
                if txt.is_empty() || crate::client::split_host_port(&txt[..]).is_err() {
                    add_btn.set_sensitive(false);
                } else {
                    let mut cert = cert.borrow_mut();
                    cert.0 = txt;
                    if validate(&cert, rows.borrow().as_ref()) {
                        add_btn.set_sensitive(true);
                    } else {
                        add_btn.set_sensitive(false);
                    }
                }
            }
        });
        cert_entry.connect_changed({
            let cert = cert.clone();
            let add_btn = add_btn.clone();
            let _exp_row = exp_row.clone();
            let rows = rows.clone();
            move |entry| {
                let txt = entry.buffer().text().to_string();
                if txt.is_empty() {
                    add_btn.set_sensitive(false);
                } else {
                    let mut cert = cert.borrow_mut();
                    cert.1 = entry.buffer().text().to_string();
                    if validate(&cert, rows.borrow().as_ref()) {
                        add_btn.set_sensitive(true);
                    } else {
                        add_btn.set_sensitive(false);
                    }
                }
            }
        });

        let cert_added = gio::SimpleAction::new("cert_add", Some(&String::static_variant_type()));
        let cert_removed = gio::SimpleAction::new("cert_remove", Some(&String::static_variant_type()));

        add_btn.connect_clicked({
            let cert = cert.clone();
            let exp_row = exp_row.clone();
            let (host_entry, cert_entry) = (host_entry.clone(), cert_entry.clone());
            let cert_added = cert_added.clone();
            let rows = rows.clone();
            let cert_removed = cert_removed.clone();
            move |btn| {
                let mut cert = cert.borrow_mut();
                host_entry.set_text("");
                cert_entry.set_text("");
                append_certificate_row(exp_row.clone(), &cert.0, &cert.1, cert.2, &rows, &cert_added, &cert_removed);
                btn.set_sensitive(false);
                cert.0 = String::new();
                cert.1 = String::new();
                cert.2 = TlsVersion { major : 1, minor : 0 };
            }
        });

        exp_row.add_row(&add_row);
        exp_row.set_selectable(false);

        list.append(&save_row);
        list.append(&exp_row);

        set_all_not_selectable(&list);
        
        Self { list, cert_added, cert_removed, exp_row, rows, save_switch, scrolled, version_combo }
    }
}

// TODO add report settings.
/*#[derive(Debug, Clone)]
pub struct ReportingBox {
    pub list : ListBox,
    pub entry : Entry
}

impl ReportingBox {

    pub fn build() -> Self {
        let list = ListBox::new();
        configure_list(&list);
        let entry = Entry::new();
        list.append(&NamedBox::new("Template", Some("Path to html/fodt template from which\nreport will be rendered"), entry.clone()).bx);
        Self { list, entry }
    }
}*/

#[derive(Debug, Clone)]
pub struct QueriesSettings {
    pub settings : SettingsWindow,
    pub conn_bx : ConnBox,
    pub exec_bx : ExecutionBox,
    pub editor_bx : EditorBox,
    pub security_bx : SecurityBox
}

const SETTINGS : [&'static str; 4] = ["Connection", "Editor", "Execution", "Security"];

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


