use gtk4::prelude::*;
use gtk4::*;
use sourceview5;
use crate::ui::PackedImageLabel;
use crate::ui::MainMenu;
use crate::React;
use crate::client::OpenedScripts;
use sourceview5::View;
use sourceview5::prelude::*;
use crate::ui::ExecButton;

#[derive(Debug, Clone)]
pub struct QueriesEditor {
    pub views : [sourceview5::View; 16],
    pub script_list : ScriptList,
    pub stack : Stack,
    pub ignore_file_save_action : gio::SimpleAction,
    pub save_dialog : SaveDialog,
    pub open_dialog : OpenDialog
}

impl QueriesEditor {

    pub fn build() -> Self {
        let stack = Stack::new();
        let script_list = ScriptList::build();
        let save_dialog = SaveDialog::build();
        let open_dialog = OpenDialog::build();
        stack.add_named(&script_list.bx, Some("list"));

        let views : [sourceview5::View; 16]= Default::default();
        for ix in 0..16 {
            configure_view(&views[ix]);
            stack.add_named(&views[ix], Some(&format!("editor{}", ix)));
        }

        open_dialog.react(&script_list);

        let ignore_file_save_action = gio::SimpleAction::new("ignore_file_save", Some(&i32::static_variant_type()), /*&(-1i32).to_variant()*/ );
        Self { views, stack, script_list, save_dialog, open_dialog, ignore_file_save_action }
    }

}

impl React<OpenedScripts> for QueriesEditor {

    fn react(&self, opened : &OpenedScripts) {
        opened.connect_selected({
            let stack = self.stack.clone();
            let views = self.views.clone();
            move |opt_ix| {
                match opt_ix {
                    Some(ix) => {
                        stack.set_visible_child_name(&format!("editor{}", ix));
                        // opened.set_active_text(retrieve_statements_from_buffer(&views[ix]).unwrap());
                    },
                    None => {
                        stack.set_visible_child_name("list");
                        // opened.set_active_text(None);
                    }
                }
            }
        });

        opened.connect_closed({
            let stack = self.stack.clone();
            move |(_, n_left)| {
                if n_left == 0 {
                    stack.set_visible_child_name("list");
                }
            }
        });
    }

}

impl React<ExecButton> for QueriesEditor {

    fn react(&self, btn : &ExecButton) {
        let weak_views : [glib::WeakRef<sourceview5::View>; 16] = self.views.clone().map(|view| view.downgrade() );
        let exec_action = btn.exec_action.clone();
        btn.btn.connect_clicked(move |btn| {
            let selected_view = exec_action.state().unwrap().get::<i32>().unwrap();
            if selected_view >= 0 {
                if let Some(view) = weak_views[selected_view as usize].upgrade() {
                    if let Ok(Some(txt)) = retrieve_statements_from_buffer(&view) {
                        println!("Executing...");

                        // Implemented at React<ExecButton> for ActiveConnection
                        exec_action.activate(Some(&txt.to_variant()));
                    } else {
                        println!("No text to be retrieved");
                    }
                }
            } else {
                println!("No selected view");
            }
        });
    }

}

/* pub fn get_n_untitled(&self) -> usize {
    self.files.borrow().iter()
        .filter(|f| f.name.starts_with("Untitled") )
        .filter_map(|f| f.name.split(' ').nth(1) )
        .last()
        .and_then(|suff| suff.split('.').next() )
        .and_then(|n| n.parse::<usize>().ok() )
        .unwrap_or(0)
}

pub fn mark_current_saved(&self) {
    if let Some(row) = self.list_box.get_selected_row() {
        let lbl = Self::get_label_from_row(&row);
        let txt = lbl.get_text();
        if txt.as_str().ends_with("*") {
            lbl.set_text(&txt[0..(txt.len()-1)]);
        }
    } else {
        println!("No selected row");
    }
}

pub fn mark_current_unsaved(&self) {
    if let Some(row) = self.list_box.get_selected_row() {
        let lbl = Self::get_label_from_row(&row);
        let txt = lbl.get_text();
        if !txt.as_str().ends_with("*") {
            lbl.set_text(&format!("{}*", txt));
        } else {
            // println!("Text already marked as unsaved");
        }
    } else {
        println!("No selected row");
    }
}
*/

pub fn retrieve_statements_from_buffer(view : &sourceview5::View) -> Result<Option<String>, String> {
    let buffer = view.buffer();
    let opt_text : Option<String> = match buffer.selection_bounds() {
        Some((from, to,)) => {
            from.text(&to).map(|txt| txt.to_string())
        },
        None => {
            Some(buffer.text(
                &buffer.start_iter(),
                &buffer.end_iter(),
                true
            ).to_string())
        }
    };
    Ok(opt_text)
}

#[derive(Debug, Clone)]
pub struct ScriptList {
    pub open_btn : Button,
    pub new_btn : Button,
    pub list : ListBox,
    pub bx : Box
}

impl ScriptList {

    pub fn build() -> Self {
        let new_btn = Button::builder().icon_name("document-new-symbolic") /*.label("New").*/ .halign(Align::Fill).hexpand(true).build();
        let open_btn = Button::builder().icon_name("document-open-symbolic") /*.label("Open").*/ .halign(Align::Fill).hexpand(true).build();
        // open_btn.style_context().add_class("image-button");
        // new_btn.style_context().add_class("image-button");

        let btn_bx = Box::new(Orientation::Horizontal, 0);
        btn_bx.append(&new_btn);
        btn_bx.append(&open_btn);
        btn_bx.set_halign(Align::Fill);
        btn_bx.set_hexpand(true);
        btn_bx.set_hexpand_set(true);
        btn_bx.style_context().add_class("linked");

        let list = ListBox::new();
        let scroll = ScrolledWindow::new();
        let provider = CssProvider::new();
        provider.load_from_data("* { border : 1px solid #d9dada; } ".as_bytes());
        scroll.style_context().add_provider(&provider, 800);
        scroll.set_child(Some(&list));
        scroll.set_width_request(520);
        scroll.set_height_request(220);
        scroll.set_margin_bottom(36);
        list.set_activate_on_single_click(true);

        let bx = Box::new(Orientation::Vertical, 0);
        bx.set_halign(Align::Center);

        let title = super::title_label("Scripts");
        bx.append(&title);
        bx.append(&scroll);
        bx.append(&btn_bx);
        bx.set_halign(Align::Center);
        bx.set_valign(Align::Center);

        // bind_list_to_buttons(&list, &new_btn, &open_btn);
        Self { open_btn, new_btn, list, bx }
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
    pub dialog : FileChooserDialog
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

#[derive(Debug, Clone)]
pub struct OpenDialog {
    pub dialog : FileChooserDialog
}

impl OpenDialog {

    pub fn build() -> Self {
        let dialog = FileChooserDialog::new(
            Some("Open script"),
            None::<&Window>,
            FileChooserAction::Open,
            &[("Cancel", ResponseType::None), ("Open", ResponseType::Accept)]
        );
        let filter = FileFilter::new();
        filter.add_pattern("*.sql");
        dialog.set_filter(&filter);
        Self { dialog }
    }

}

impl React<ScriptList> for OpenDialog {

    fn react(&self, list : &ScriptList) {
        let dialog = self.dialog.clone();
        list.open_btn.connect_clicked(move|_| {
            dialog.show()
        });
    }
}

impl React<MainMenu> for OpenDialog {

    fn react(&self, menu : &MainMenu) {
        let dialog = self.dialog.clone();
        menu.action_open.connect_activate(move |_,_| {
            dialog.show();
        });
    }

}

fn configure_view(view : &View) {
    let buffer = view.buffer()
        .downcast::<sourceview5::Buffer>().unwrap();
    let manager = sourceview5::StyleSchemeManager::new();
    // println!("available schemes: {:?}", manager.scheme_ids());

    let scheme = manager.scheme("Adwaita").unwrap();
    buffer.set_style_scheme(Some(&scheme));
    buffer.set_highlight_syntax(true);

    let provider = CssProvider::new();
    provider.load_from_data(b"textview { font-family: \"Ubuntu Mono\"; font-size: 13pt; }");
    let ctx = view.style_context();
    ctx.add_provider(&provider, 800);

    let lang_manager = sourceview5::LanguageManager::default().unwrap();
    let lang = lang_manager.language("sql").unwrap();
    buffer.set_language(Some(&lang));
    connect_source_key_press(&view);
    let buffer = view.buffer();
    buffer.connect_changed(move |_buf| {
        // file_list.mark_current_unsaved();
        // mark_current_unsaved();
    });
    view.set_tab_width(4);
    view.set_indent_width(4);
    view.set_auto_indent(true);
    view.set_insert_spaces_instead_of_tabs(true);
    // view.set_right_margin(80);
    view.set_highlight_current_line(false);
    view.set_indent_on_tab(true);
    view.set_show_line_marks(true);
    // view.set_right_margin_position(80);
    // view.set_show_right_margin(false);
    view.set_show_line_numbers(false);
}

fn connect_source_key_press(view : &View) {

    // EventControllerKey::new().connect_key_pressed(|ev, key, code, modifier| {
    //    Inhibit(false)
    // });

    /*view.connect_key_press_event(move |_view, ev_key| {
        if ev_key.get_state() == gdk::ModifierType::CONTROL_MASK && ev_key.get_keyval() == keys::constants::Return {
            // if refresh_btn.is_sensitive() {
            // exec_action.emit();
            // refresh_btn.emit_clicked();
            // }
            glib::signal::Inhibit(true)
        } else {
            glib::signal::Inhibit(false)
        }
    });*/
}


