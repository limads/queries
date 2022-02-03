use gtk4::prelude::*;
use gtk4::*;
use libadwaita;
use crate::client::ActiveConnection;
use crate::React;
use crate::client::Environment;
use crate::sql::StatementOutput;
use crate::client::OpenedScripts;
use super::MainMenu;
use sourceview5;
use std::thread;
use std::path::Path;
use std::sync::mpsc;

#[derive(Debug, Clone)]
pub struct QueriesSettings {
    pub dialog : Dialog,
    pub exec_bx : ExecutionBox,
    pub editor_bx : EditorBox,
    pub security_bx : SecurityBox,
    pub report_bx : ReportingBox
}

#[derive(Debug, Clone)]
pub struct NamedBox<W : IsA<Widget>> {
    bx : Box,
    w : W
}

impl<W: IsA<Widget>> NamedBox<W> {

    pub fn new(name : &str, subtitle : Option<&str>, w : W) -> Self {
        let label_bx = Box::new(Orientation::Vertical, 0);
        //let label = Label::new(Some(&format!("<span font_weight='bold'>{}</span>", name)));
        let label = Label::new(Some(&format!("<span>{}</span>", name)));
        super::set_margins(&label, 6, 6);
        label.set_justify(Justification::Left);
        label.set_halign(Align::Start);
        label.set_use_markup(true);
        label_bx.append(&label);

        if let Some(subtitle) = subtitle {
            let label = Label::new(Some(&format!("<span font_size='small'>{}</span>", subtitle)));
            label.set_use_markup(true);
            label.set_justify(Justification::Left);
            label.set_halign(Align::Start);
            label_bx.append(&label);
            super::set_margins(&label, 6, 6);
        }

        label_bx.set_halign(Align::Start);

        let bx = Box::new(Orientation::Horizontal, 0);
        bx.set_halign(Align::Fill);
        bx.set_hexpand(true);

        bx.append(&label_bx);
        bx.append(&w);
        super::set_margins(&bx, 18, 18);

        w.set_halign(Align::End);
        w.set_hexpand(true);
        w.set_vexpand(false);
        w.set_valign(Align::Center);

        Self { bx, w }
    }

}

#[derive(Debug, Clone)]
pub struct EditorBox {
    list : ListBox,
    scheme_combo : ComboBoxText,
    font_btn : FontButton,
    line_num_switch : Switch,
    line_highlight_switch : Switch,
}

fn configure_list(list : &ListBox) {
    list.set_halign(Align::Center);
    list.set_valign(Align::Center);
    list.set_hexpand(true);
    list.set_vexpand(true);
    list.style_context().add_class("boxed-list");
    list.set_width_request(600);
    // list.set_selectable(false);
    // list.set_activatable(false);
}

impl EditorBox {

    pub fn build() -> Self {
        //let list = Box::new(Orientation::Vertical, 0);
        let list = ListBox::new();
        configure_list(&list);

        let font_btn = FontButton::new();
        font_btn.set_use_font(true);
        font_btn.set_use_size(true);

        font_btn.connect_font_set(move |font_btn| {
            font_btn.font_family();
            font_btn.font_size();
        });

        let manager = sourceview5::StyleSchemeManager::new();
        let scheme_combo = ComboBoxText::new();
        for id in manager.scheme_ids() {
            scheme_combo.append_text(&id);
        }
        list.append(&NamedBox::new("Color scheme", None, scheme_combo.clone()).bx);
        /*combo.connect_changed(move |combo| {
            if let Some(txt) = combo.active_text() {
                let s = txt.as_str();
            }
        });*/
        list.append(&NamedBox::new("Font", None, font_btn.clone()).bx);

        let line_num_switch = Switch::new();
        let line_highlight_switch = Switch::new();

        list.append(&NamedBox::new("Show line numbers", None, line_num_switch.clone()).bx);
        list.append(&NamedBox::new("Highlight current line", None, line_highlight_switch.clone()).bx);

        Self { list, scheme_combo, font_btn, line_num_switch, line_highlight_switch }
    }

}

#[derive(Debug, Clone)]
pub struct ExecutionBox {
    list : ListBox,
    row_limit_spin : SpinButton,
    col_limit_spin : SpinButton,
    schedule_scale : Scale
}

impl ExecutionBox {

    pub fn build() -> Self {
        let list = ListBox::new();
        configure_list(&list);
        let row_limit_spin = SpinButton::with_range(0.0, 10_000.0, 1.0);
        row_limit_spin.set_digits(0);
        row_limit_spin.set_value(500.);

        let col_limit_spin = SpinButton::with_range(0.0, 100.0, 1.0);
        col_limit_spin.set_digits(0);
        col_limit_spin.set_value(25.);

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
        list.append(&NamedBox::new("Column limit", None, col_limit_spin.clone()).bx);
        // list.append(&NamedBox::new("Row overflow", Some("Which rows to display when results\n extrapolate the row limit"), schedule_scale.clone()).bx);
        list.append(&NamedBox::new("Execution interval", Some("Interval (in seconds)\nbetween scheduled executions"), schedule_scale.clone()).bx);
        list.append(&NamedBox::new("Statement timeout", Some("Maximum time (in seconds)\nto wait for database response"), timeout_scale.clone()).bx);

        Self { list, row_limit_spin, col_limit_spin, schedule_scale }
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
                        exists_send.send(true);
                    } else {
                        exists_send.send(false);
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
                println!("Combo changed");
                if let Some(txt) = combo.active_text() {
                    path_send.send(txt.as_str().to_string());
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

#[derive(Debug, Clone)]
pub struct SecurityBox {
    list : ListBox
}

impl SecurityBox {

    pub fn build() -> Self {
        let list = ListBox::new();
        configure_list(&list);

        //btn_bx.left_btn.connect_clicked(move |_| {
        //});
        //btn_bx.right_btn.connect-clicked(move |_| {
        //});
        let combo_bx = EditableCombo::build();
        // bx.append(&NamedBox::new("Certificate", Some("Inform the TLS certificate path if the \nconnection require it"), combo_bx.bx).bx);

        let entry = Entry::new();
        let model = ListStore::new(&[glib::types::Type::STRING]);
        // let pos = model.append();
        model.set(&model.append(), &[(0, &String::from("mycompletion") as &dyn ToValue)]);
        model.set(&model.append(), &[(0, &String::from("myothercompletion") as &dyn ToValue)]);
        model.set(&model.append(), &[(0, &String::from("othercompletion") as &dyn ToValue)]);

        // let renderer = CellRendererText::builder().foreground("#000000").foreground_set(true).build();
        let completion = EntryCompletion::builder().model(&model).minimum_key_length(0) /*.cell_area(&area).*/ /*.popup_completion(true)*/ .text_column(0).build();
        // completion.pack_start(&renderer, true);
        entry.set_icon_from_icon_name(EntryIconPosition::Primary, Some("document-open-symbolic"));
        entry.set_completion(Some(&completion));
        //completion.add_attribute(&renderer, "text", 0);

        list.append(&NamedBox::new("Certificate", Some("Inform the TLS certificate path if the \nconnection require it"), entry).bx);

        // combo.connect_changed(move |combo| {
        //    if let Some(txt) = combo.active_text() {
        //        let s = txt.as_str();
        //    }
        //});

        Self { list }
    }
}

#[derive(Debug, Clone)]
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
}

impl QueriesSettings {

    pub fn build() -> Self {
        let dialog = Dialog::new();
        dialog.set_title(Some("Settings"));

        dialog.set_default_width(1024);
        dialog.set_default_height(768);

        dialog.set_modal(true);
        dialog.set_deletable(true);
        dialog.set_destroy_with_parent(true);
        dialog.set_hide_on_close(true);

        let paned = Paned::new(Orientation::Horizontal);
        paned.set_halign(Align::Fill);
        paned.set_hexpand(true);
        paned.set_position(200);

        let list = ListBox::new();
        list.set_selection_mode(SelectionMode::Single);
        list.append(&build_settings_row("Editor"));
        list.append(&build_settings_row("Execution"));
        list.append(&build_settings_row("Security"));
        list.append(&build_settings_row("Reporting"));
        list.set_width_request(200);
        paned.set_start_child(&list);

        let editor_bx = EditorBox::build();
        let exec_bx = ExecutionBox::build();
        let security_bx = SecurityBox::build();
        let report_bx = ReportingBox::build();
        let stack = Stack::new();
        stack.set_halign(Align::Fill);
        stack.set_hexpand(true);
        stack.add_named(&editor_bx.list, Some("editor"));
        stack.add_named(&exec_bx.list, Some("execution"));
        stack.add_named(&security_bx.list, Some("security"));
        stack.add_named(&report_bx.list, Some("reporting"));

        paned.set_end_child(&stack);
        dialog.set_child(Some(&paned));

        list.connect_row_selected({
            let stack = stack.clone();
            move |list, opt_row| {
                if let Some(row) = opt_row {
                    let name = match row.index() {
                        0 => "editor",
                        1 => "execution",
                        2 => "security",
                        3 => "reporting",
                        _ => unreachable!()
                    };
                    stack.set_visible_child_name(name);
                }
            }
        });

        Self { dialog, editor_bx, exec_bx, security_bx, report_bx }
    }

}

impl React<MainMenu> for QueriesSettings {

    fn react(&self, menu : &MainMenu) {
        let dialog = self.dialog.clone();
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


