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
use std::boxed;
use crate::client::OpenedFile;

#[derive(Debug, Clone)]
pub struct QueriesEditor {
    pub views : [sourceview5::View; 16],
    pub script_list : ScriptList,
    pub stack : Stack,
    pub ignore_file_save_action : gio::SimpleAction,
    pub save_dialog : SaveDialog,
    pub open_dialog : OpenDialog,
    pub export_dialog : ExportDialog
}

impl QueriesEditor {

    pub fn build() -> Self {
        let stack = Stack::new();
        let script_list = ScriptList::build();
        let save_dialog = SaveDialog::build();
        let open_dialog = OpenDialog::build();
        let export_dialog = ExportDialog::build();
        stack.add_named(&script_list.bx, Some("list"));
        let views : [sourceview5::View; 16]= Default::default();
        for ix in 0..16 {
            configure_view(&views[ix]);
            let scroll = ScrolledWindow::new();
            scroll.set_child(Some(&views[ix]));
            stack.add_named(&scroll, Some(&format!("editor{}", ix)));
        }
        open_dialog.react(&script_list);
        let ignore_file_save_action = gio::SimpleAction::new("ignore_file_save", Some(&i32::static_variant_type()));
        Self { views, stack, script_list, save_dialog, open_dialog, ignore_file_save_action, export_dialog }
    }

}

impl React<OpenedScripts> for QueriesEditor {

    fn react(&self, opened : &OpenedScripts) {
        opened.connect_selected({
            let stack = self.stack.clone();
            let _views = self.views.clone();
            move |opt_file| {
                match opt_file {
                    Some(file) => {
                        stack.set_visible_child_name(&format!("editor{}", file.index));
                        // opened.set_active_text(retrieve_statements_from_buffer(&views[ix]).unwrap());
                    },
                    None => {
                        stack.set_visible_child_name("list");
                        // opened.set_active_text(None);
                    }
                }
            }
        });
        opened.connect_opened({
            let views = self.views.clone();
            let list = self.script_list.clone();
            move |file| {
                if let Some(content) = file.content.clone() {
                    views[file.index].buffer().set_text(&content);
                } else {
                    println!("File does not have content");
                }
                add_if_not_present(&list, &file);
            }
        });
        opened.connect_closed({
            let stack = self.stack.clone();
            let views = self.views.clone();
            move |(ix, n_left)| {
                let buffer = views[ix].buffer();
                buffer.set_text("");
                if n_left == 0 {
                    stack.set_visible_child_name("list");
                }
            }
        });
        opened.connect_file_persisted({
            let list = self.script_list.clone();
            move |file| {
                add_if_not_present(&list, &file);
            }
        });
        opened.connect_added({
            let list = self.script_list.clone();
            move |file| {
                add_if_not_present(&list, &file);
            }
        });
        opened.connect_buffer_read_request({
            let views = self.views.clone();
            move |ix : usize| -> String {
                let buffer = views[ix].buffer();
                buffer.text(
                    &buffer.start_iter(),
                    &buffer.end_iter(),
                    true
                ).to_string()
            }
        });
    }

}

fn add_if_not_present(list : &ScriptList, file : &OpenedFile) {
    if let Some(path) = &file.path {
        let prev_paths = file_paths(&list.list);
        if prev_paths.iter().find(|p| &p[..] == &path[..] ).is_none() {
            list.add_row(&path[..]);
        }
    }
}

fn file_paths(list : &ListBox) -> Vec<String> {
    let mut paths = Vec::new();
    let n = list.observe_children().n_items();
    for ix in 0..n {
        let row = list.row_at_index(ix as i32).unwrap();
        let child = row.child().unwrap().downcast::<Box>().unwrap();
        let lbl = PackedImageLabel::extract(&child).unwrap();
        let txt = lbl.lbl.text().as_str().to_string();
        paths.push(txt);
    }
    paths
}

impl React<ExecButton> for QueriesEditor {

    fn react(&self, btn : &ExecButton) {
        let weak_views : [glib::WeakRef<sourceview5::View>; 16] = self.views.clone().map(|view| view.downgrade() );
        let exec_action = btn.exec_action.clone();
        btn.btn.connect_clicked(move |_btn| {
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
}*/

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
        let btn_bx = super::ButtonPairBox::build("document-new-symbolic", "document-open-symbolic");
        /*let new_btn = Button::builder().icon_name("document-new-symbolic") /*.label("New").*/ .halign(Align::Fill).hexpand(true).build();
        new_btn.style_context().add_class("flat");
        new_btn.set_width_request(64);
        let open_btn = Button::builder().icon_name("document-open-symbolic") /*.label("Open").*/ .halign(Align::Fill).hexpand(true).build();
        open_btn.style_context().add_class("flat");
        open_btn.set_width_request(64);*/
        let new_btn = btn_bx.left_btn.clone();
        let open_btn = btn_bx.right_btn.clone();

        // open_btn.style_context().add_class("image-button");
        // new_btn.style_context().add_class("image-button");

        /*let btn_bx = Box::new(Orientation::Horizontal, 0);
        btn_bx.append(&new_btn);
        btn_bx.append(&open_btn);
        btn_bx.set_halign(Align::Fill);
        btn_bx.set_hexpand(true);
        btn_bx.set_hexpand_set(true);*/
        // btn_bx.style_context().add_class("linked");

        let list = ListBox::new();
        // list.style_context().add_class("boxed-list");
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
        let title_bx = Box::new(Orientation::Horizontal, 0);
        title_bx.append(&title);
        title_bx.append(&btn_bx.bx);
        btn_bx.bx.set_halign(Align::End);
        bx.append(&title_bx);

        bx.append(&title_bx);
        bx.append(&scroll);

        // bx.append(&btn_bx);
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
            Some("Save script"),
            None::<&Window>,
            FileChooserAction::Save,
            &[("Cancel", ResponseType::None), ("Save", ResponseType::Accept)]
        );
        dialog.connect_response(move |dialog, resp| {
            match resp {
                ResponseType::Close | ResponseType::Reject | ResponseType::Accept | ResponseType::Yes |
                ResponseType::No | ResponseType::None | ResponseType::DeleteEvent => {
                    dialog.close();
                },
                _ => { }
            }
        });
        let filter = FileFilter::new();
        filter.add_pattern("*.sql");
        dialog.set_filter(&filter);
        Self { dialog }
    }

}

impl React<MainMenu> for SaveDialog {

    fn react(&self, menu : &MainMenu) {
        let dialog = self.dialog.clone();
        menu.action_save_as.connect_activate(move |_,_| {
            dialog.show();
        });
    }

}

impl React<OpenedScripts> for SaveDialog {

    fn react(&self, scripts : &OpenedScripts) {
        let dialog = self.dialog.clone();
        scripts.connect_save_unknown_path(move |path| {
            let _ = dialog.set_file(&gio::File::for_path(path));
            dialog.show();
        });
        let dialog = self.dialog.clone();
        scripts.connect_selected(move |opt_file| {
            if let Some(path) = opt_file.and_then(|f| f.path.clone() ) {
                let _ = dialog.set_file(&gio::File::for_path(&path));
            }
        });
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
        dialog.connect_response(move |dialog, resp| {
            match resp {
                ResponseType::Reject | ResponseType::Accept | ResponseType::Yes | ResponseType::No |
                ResponseType::None | ResponseType::DeleteEvent => {
                    dialog.close();
                },
                _ => { }
            }
        });
        dialog.set_modal(true);
        dialog.set_deletable(true);
        dialog.set_destroy_with_parent(true);
        dialog.set_hide_on_close(true);
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

/* use sourceview5::*;
use std::pin::Pin;
use gio::ListModel;
use std::error::Error;
use std::future::Future;*/

/*use gtk4::subclass::prelude::*;
use sourceview5::prelude::*;
use sourceview5::*;

glib::wrapper! {

    pub struct QueriesCompletion(ObjectSubclass<completion::QueriesCompletion>)

    @extends CompletionProvider,

    @implements CompletionProviderExt;

}

mod completion {

    #[derive(Default)]
    pub struct QueriesCompletion { }

    use gtk4::glib;
    use gtk4::subclass::prelude::*;
    use sourceview5::*;
    use sourceview5::prelude::*;

    #[glib::object_subclass]
    impl ObjectSubclass for super::QueriesCompletion {

        const NAME: &'static str = "QueriesCompletion";

        type Type = super::QueriesCompletion;

        type ParentType = sourceview5::CompletionProvider;

    }

    impl ObjectImpl for QueriesCompletion { }

    /*impl CompletionProviderExt for QueriesCompletion {

        /*fn display<P: IsA<CompletionProposal>>(
            &self,
            context: &CompletionContext,
            proposal: &P,
            cell: &CompletionCell
        ) {

        }*/
    }*/

}*/

fn configure_view(view : &View) {
    let buffer = view.buffer()
        .downcast::<sourceview5::Buffer>().unwrap();
    let manager = sourceview5::StyleSchemeManager::new();
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
    view.set_tab_width(4);
    view.set_indent_width(4);
    view.set_auto_indent(true);
    view.set_insert_spaces_instead_of_tabs(true);
    view.set_highlight_current_line(false);
    view.set_indent_on_tab(true);
    view.set_show_line_marks(true);
    view.set_enable_snippets(true);

    // view.set_right_margin_position(80);
    // view.set_show_right_margin(false);

    // https://developer-old.gnome.org/gtksourceview/unstable/GtkSourceCompletion.html

    // completioncell accepts an arbitrary widget via the builder::widget.
    // let cell = sourceview5::CompletionCell::builder().text("select").paintable(&IconPaintable::builder().icon_name("queries-symbolic").build()).build();

    // let activation = sourceview5::CompletionActivation::Interactive; //UserRequested
    // let ctx = sourceview5::CompletionContext::builder().build();

    // Completion is an object associated with each View. Each completion has zero, one or
    // more completion providers. The providers encapsulate the actual logic of how completions
    // are offered. Two examples of providers are CompletionWords and CompletionSnippets.

    // completion.set_select_on_show(true);
    // completion.set_remember_info_visibility(true);
    // completion.set_show_icons(true);
    // completion.unblock_interactive();
    // completion.set_show_icons(true);

    // Seems to be working, but only when you click on the the word
    // and **then** press CTRL+Space (simply pressing CTRL+space does not work).
    let completion = view.completion().unwrap();
    let words = sourceview5::CompletionWords::new(Some("main"));
    words.register(&view.buffer());
    completion.add_provider(&words);

    /*words.populate_async(&ctx, None::<&gio::Cancellable>, |res_list| {
        if let Ok(list) = res_list {
            println!("{}", list.n_items());
        } else {
            // panic!()
        }
    });*/
    /*let snippets = sourceview5::CompletionSnippets::new();
    snippets.set_title(Some("snippets provider"));
    snippets.populate_async(&ctx, None::<&gio::Cancellable>, |res_list| {
        if let Ok(list) = res_list {
            println!("{}", list.n_items());
        } else {
            // panic!()
        }
    });
    completion.add_provider(&snippets);*/
    // proposal
    // words.display(&ctx, &proposal, &cell);
    //let new_buffer = TextBuffer::new(None);
    //new_buffer.set_text("select\ninsert\n");
    //words.register(&new_buffer);
    // ctx.set_activation(activation);
    // let list = gio::ListStore::new(glib::Type::STRING);
    // list.insert(0, &"select".to_value());
    // list.insert(0);
    // list.append(&gio::glib::GString::from("select"));
    // list.append(&cell);
    // list.insert(0, &"select".to_value());
    // list.append(&glib::GString::from("select"));
    // list.append(&gtk4::glib::GString::from("select"));
    // let proposal = Completion
    // list.append(&proposal.upcast());
    // let words_list = gio::ListStore::new(glib::Type::OBJECT);
    // ctx.set_proposals_for_provider(&words, Some(&words_list));
    // let snippets_list = gio::ListStore::new(glib::Type::OBJECT);
    // ctx.set_proposals_for_provider(&snippets, Some(&snippets_list));
    // ctx.bounds() -> Gets a pair of TextIters that represent the current completion region.
    // ctx.word() -> Gets the word that is being completed.
    // .build();    /*
    // let provider = CompletionProvider::new();
    /*let compl = sourceview5::Completion::builder()
        .show_icons(true)
        .build();
    compl.add_provider(&provider);
    view.set_completion(compl);*/
    /*view.connect_show_completion(move|view| {
        println!("Completion requested");
    });
    view.connect_completion_notify(move|view| {
        println!("Completion notified");
    });*/

    view.set_show_line_numbers(false);
}

fn connect_source_key_press(_view : &View) {

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

#[derive(Debug, Clone)]
pub struct ExportDialog {
    pub dialog : FileChooserDialog
}

impl ExportDialog {

    pub fn build() -> Self {
        let dialog = FileChooserDialog::new(
            Some("Export"),
            None::<&Window>,
            FileChooserAction::Save,
            &[("Cancel", ResponseType::None), ("Save", ResponseType::Accept)]
        );
        dialog.connect_response(move |dialog, resp| {
            match resp {
                ResponseType::Close | ResponseType::Reject | ResponseType::Accept |
                ResponseType::Yes | ResponseType::No | ResponseType::None => {
                    dialog.close();
                },
                _ => { }
            }
        });
        // let filter = FileFilter::new();
        // filter.add_pattern("*.sql");
        // dialog.set_filter(&filter);
        Self { dialog }
    }

}

impl React<MainMenu> for ExportDialog {

    fn react(&self, menu : &MainMenu) {
        let dialog = self.dialog.clone();
        menu.action_export.connect_activate(move |_,_| {
            dialog.show();
        });
    }

}
