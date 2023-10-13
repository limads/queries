/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use gtk4::prelude::*;
use gtk4::*;
use sourceview5;
use crate::ui::PackedImageLabel;
use crate::ui::MainMenu;
use stateful::React;
use crate::client::OpenedScripts;
use sourceview5::View;
use sourceview5::prelude::*;
use crate::ui::ExecButton;
use filecase::OpenedFile;
use std::cell::RefCell;
use std::rc::Rc;
use sourceview5::{SearchContext, SearchSettings};
use sourceview5::Buffer;
use crate::client::SharedUserState;
use crate::ui::QueriesSettings;
use crate::client::EditorSettings;
use filecase::MultiArchiverImpl;
use once_cell::unsync::OnceCell;

const MAX_VIEWS : usize = 16;

#[derive(Debug, Clone)]
pub struct QueriesEditor {
    pub views : [sourceview5::View; MAX_VIEWS],
    pub script_list : ScriptList,
    pub stack : Stack,

    // This exists as an action because it interacts with libadwaita::Toast,
    // which takes an action when clicked. This forces the MultiArchiver to
    // close a file when the Toast button is clicked. The action argument is
    // the index of the currently-selected file.
    pub ignore_file_save_action : gio::SimpleAction,

    pub save_dialog : SaveDialog,
    pub open_dialog : OpenDialog,
    pub export_dialog : ExportDialog,
    user_state : SharedUserState
}

impl QueriesEditor {

    pub fn build(user_state : &SharedUserState) -> Self {
        let stack = Stack::new();
        let script_list = ScriptList::build();
        let save_dialog = SaveDialog::build();
        let open_dialog = OpenDialog::build();
        let export_dialog = ExportDialog::build();
        stack.add_named(&script_list.bx, Some("list"));
        // let provider = SqlCompletionProvider::new();
        let views : [sourceview5::View; MAX_VIEWS]= Default::default();
        for ix in 0..MAX_VIEWS {
            configure_view(&views[ix], &EditorSettings::default());
            let scroll = ScrolledWindow::new();
            scroll.set_child(Some(&views[ix]));
            stack.add_named(&scroll, Some(&format!("editor{}", ix)));
            // let compl = views[ix].completion();
            // compl.add_provider(&provider);
        }
        open_dialog.react(&script_list);
        let ignore_file_save_action = gio::SimpleAction::new("ignore_file_save", Some(&i32::static_variant_type()));
        Self { views, stack, script_list, save_dialog, open_dialog, ignore_file_save_action, export_dialog, user_state : user_state.clone() }
    }

    pub fn configure(&self, settings : &EditorSettings) {
        for ix in 0..MAX_VIEWS {
            configure_view(&self.views[ix], &settings);
        }
    }

}

pub fn selected_editor_stack_index(stack : &Stack) -> Option<usize> {
    if let Some(sel_name) = stack.visible_child_name() {
        sel_name.trim_start_matches("editor").parse::<usize>().ok()
    } else {
        None
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
                    },
                    None => {
                        stack.set_visible_child_name("list");
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
                }
                add_if_not_present(&list, &file);
            }
        });
        opened.connect_closed({
            let stack = self.stack.clone();
            let views = self.views.clone();
            move |(old_file, n_left)| {
            
                let buf_removed = views[old_file.index].buffer();
                buf_removed.set_text("");
                for ix in old_file.index..(MAX_VIEWS-1) {
                    let buf_right = views[ix+1].buffer();
                    views[ix].set_buffer(Some(&buf_right));
                }
                views[MAX_VIEWS-1].set_buffer(Some(&buf_removed));
                
                if n_left == 0 {
                    stack.set_visible_child_name("list");
                } else {
                    if let Some(sel_ix) = selected_editor_stack_index(&stack) {
                        if sel_ix > old_file.index {
                            stack.set_visible_child_name(&format!("editor{}", sel_ix-1));
                        }
                    }
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
            let stack = self.stack.clone();
            move |ix : usize| -> String {
                
                if let Some(vis_name) = stack.visible_child_name() {
                    if &vis_name[..] != &format!("editor{}", ix)[..] {
                        eprintln!("Warning: Currently selected file is different than desired file index");
                        return String::new();
                    }
                 } else {
                    eprintln!("Warning: Currently selected file is different than desired file index");
                    return String::new();
                 }
                
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
        let weak_views : [glib::WeakRef<sourceview5::View>; MAX_VIEWS] = self.views.clone().map(|view| view.downgrade() );
        let exec_action = btn.exec_action.clone();
        btn.queue_exec_action.connect_activate(move |_, _| {
            let selected_view = exec_action.state().unwrap().get::<i32>().unwrap();
            if selected_view >= 0 {
                if let Some(view) = weak_views[selected_view as usize].upgrade() {
                    if let Ok(Some(txt)) = retrieve_statements_from_buffer(&view) {

                        // Implemented at React<ExecButton> for ActiveConnection

                        exec_action.activate(Some(&txt.to_variant()));
                    } else {
                        eprintln!("No text to be retrieved");
                    }
                }
            } else {
                eprintln!("No selected view");
            }
        });
    }

}

pub fn retrieve_statements_from_buffer(view : &sourceview5::View) -> Result<Option<String>, String> {
    let buffer = view.buffer();
    let opt_text : Option<String> = match buffer.selection_bounds() {
        Some((from, to,)) => {
            Some(from.text(&to).to_string())
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
        let new_btn = btn_bx.left_btn.clone();
        let open_btn = btn_bx.right_btn.clone();
        let list = ListBox::new();
        super::set_margins(&list, 1, 1);
        list.style_context().add_class("boxed-list");
        let scroll = ScrolledWindow::new();
        scroll.set_child(Some(&list));
        scroll.set_width_request(520);
        scroll.set_height_request(380);
        scroll.set_has_frame(false);
        list.set_activate_on_single_click(true);
        list.set_show_separators(true);

        let bx = Box::new(Orientation::Vertical, 0);
        bx.set_halign(Align::Center);

        let title = super::title_label("Scripts");
        let title_bx = Box::new(Orientation::Horizontal, 0);
        title_bx.append(&title);
        title_bx.append(&btn_bx.bx);
        btn_bx.bx.set_halign(Align::End);
        bx.append(&title_bx);

        bx.append(&scroll);

        bx.set_halign(Align::Center);
        bx.set_valign(Align::Center);

        Self { open_btn, new_btn, list, bx }
    }

    pub fn add_row(&self, path : &str) {
        let row = ListBoxRow::new();
        row.set_height_request(64);
        let lbl = PackedImageLabel::build("emblem-documents-symbolic", path);
        row.set_child(Some(&lbl.bx));
        row.set_selectable(false);
        row.set_activatable(true);
        self.list.append(&row);
    }

}

#[derive(Debug, Clone)]
pub struct SaveDialog(pub(crate) filecase::SaveDialog);

impl SaveDialog {

    pub fn build() -> Self {
        Self(filecase::SaveDialog::build(&["*.sql"]))
    }

}

impl React<MainMenu> for SaveDialog {

    fn react(&self, menu : &MainMenu) {
        let dialog = self.0.dialog.clone();
        menu.action_save_as.connect_activate(move |_,_| {
            dialog.show();
        });
    }

}

impl React<OpenedScripts> for SaveDialog {

    fn react(&self, scripts : &OpenedScripts) {
        let dialog = self.0.dialog.clone();
        scripts.connect_save_unknown_path(move |name| {
            dialog.set_current_name(&name);
            dialog.show();
        });
        let dialog = self.0.dialog.clone();
        scripts.connect_selected(move |opt_file| {
            if let Some(path) = opt_file.as_ref().and_then(|f| f.path.clone() ) {
                let _ = dialog.set_file(&gio::File::for_path(&path));
            } else {
                if let Some(name) = opt_file.as_ref().map(|f| f.name.clone() ) {
                    dialog.set_current_name(&name);
                } else {
                    dialog.set_current_name("");
                }
            }
        });
    }

}

#[derive(Debug, Clone)]
pub struct OpenDialog(pub(crate) filecase::OpenDialog);

impl OpenDialog {

    pub fn build() -> Self {
        Self(filecase::OpenDialog::build(&["*.sql"]))
    }

}

impl React<ScriptList> for OpenDialog {

    fn react(&self, list : &ScriptList) {
        let dialog = self.0.dialog.clone();
        list.open_btn.connect_clicked(move|_| {
            dialog.show()
        });
    }
}

impl React<MainMenu> for OpenDialog {

    fn react(&self, menu : &MainMenu) {
        let dialog = self.0.dialog.clone();
        menu.action_open.connect_activate(move |_,_| {
            dialog.show();
        });
    }

}

fn configure_view(view : &View, settings : &EditorSettings) {
    let buffer = view.buffer()
        .downcast::<sourceview5::Buffer>().unwrap();
    let manager = sourceview5::StyleSchemeManager::new();
    if let Some(scheme) = manager.scheme(&settings.scheme) {
        buffer.set_style_scheme(Some(&scheme));
    }

    buffer.set_highlight_syntax(true);
    buffer.set_max_undo_levels(40);
    let provider = CssProvider::new();
    let font = format!("textview {{ font-family: \"{}\"; font-size: {}pt; }}", settings.font_family, settings.font_size);
    provider.load_from_data(&font);

    let ctx = view.style_context();
    ctx.add_provider(&provider, 800);
    let lang_manager = sourceview5::LanguageManager::default();
    let lang = lang_manager.language("sql").unwrap();
    buffer.set_language(Some(&lang));
    view.set_tab_width(4);
    view.set_indent_width(4);
    view.set_auto_indent(true);
    view.set_insert_spaces_instead_of_tabs(true);
    view.set_highlight_current_line(settings.highlight_current_line);
    view.set_show_line_numbers(settings.show_line_numbers);
    view.set_indent_on_tab(true);
    view.set_show_line_marks(true);
    view.set_enable_snippets(true);

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
        super::configure_dialog(&dialog, true);
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

#[derive(Debug, Clone)]
pub struct FindDialog {
    pub dialog  : Dialog,
    pub find_entry : Entry,
    pub search_up_btn : Button,
    pub search_down_btn : Button,
    pub replace_entry : Entry,
    pub find_btn : Button,
    pub replace_btn : Button,
    pub replace_all_btn : Button,
    pub matches_lbl : Label,
    pub find_action : gio::SimpleAction,
    pub replace_action : gio::SimpleAction,
    pub replace_all_action : gio::SimpleAction
}

impl FindDialog {

    pub fn build() -> Self {
        let dialog = Dialog::new();
        dialog.set_title(Some("Find and replace"));
        let find_bx = Box::new(Orientation::Horizontal, 0);
        find_bx.style_context().add_class("linked");
        let find_entry = Entry::builder().primary_icon_name("edit-find-symbolic").build();
        find_entry.set_hexpand(true);
        find_entry.set_placeholder_text(Some("Find"));
        let search_up_btn = Button::builder().icon_name("go-up-symbolic").build();
        let search_down_btn = Button::builder().icon_name("go-down-symbolic").build();
        find_bx.append(&find_entry);
        find_bx.append(&search_up_btn);
        find_bx.append(&search_down_btn);
        find_bx.set_halign(Align::Fill);
        find_bx.set_hexpand(true);
        super::set_margins(&find_bx, 6, 0);

        let replace_bx = Box::new(Orientation::Horizontal, 0);
        let replace_entry = Entry::builder().primary_icon_name("edit-find-replace-symbolic").build();
        replace_entry.set_placeholder_text(Some("Replace"));
        replace_entry.set_hexpand(true);
        replace_bx.append(&replace_entry);
        replace_bx.set_halign(Align::Fill);
        replace_bx.set_hexpand(true);
        replace_bx.set_margin_start(6);
        replace_bx.set_margin_end(6);
        replace_bx.set_margin_bottom(12);

        let btn_bx = Box::new(Orientation::Horizontal, 0);
        btn_bx.style_context().add_class("linked");
        let replace_btn = Button::builder().label("Replace").build();
        let replace_all_btn = Button::builder().label("Replace all").build();
        let find_btn = Button::builder().label("Find").build();
        btn_bx.append(&find_btn);
        btn_bx.append(&replace_btn);
        btn_bx.append(&replace_all_btn);
        btn_bx.set_halign(Align::Center);
        btn_bx.set_hexpand(false);
        btn_bx.set_margin_bottom(12);

        let matches_lbl = Label::new(None);
        matches_lbl.set_use_markup(true);
        matches_lbl.set_markup(NO_MATCHES);

        super::set_margins(&matches_lbl, 6, 12);
        
        matches_lbl.set_halign(Align::Center);
        matches_lbl.set_hexpand(false);
        let bx = Box::new(Orientation::Vertical, 0);

        let upper_bx = Box::new(Orientation::Vertical, 0);
        upper_bx.style_context().add_class("linked");
        upper_bx.append(&find_bx);
        upper_bx.append(&replace_bx);

        bx.append(&upper_bx);
        bx.append(&matches_lbl);
        bx.append(&btn_bx);

        super::set_margins(&bx, 18, 18);
        dialog.set_child(Some(&bx));
        super::configure_dialog(&dialog, true);
        dialog.set_modal(false);

        let find_action = gio::SimpleAction::new_stateful("find", None, &(-1i32).to_variant());
        let replace_action = gio::SimpleAction::new_stateful("replace", None, &(-1i32).to_variant());
        let replace_all_action = gio::SimpleAction::new_stateful("replace_all", None, &(-1i32).to_variant());

        find_btn.connect_clicked({
            let find_action = find_action.clone();
            move |_btn| {
                find_action.activate(None);
            }
        });
        replace_btn.connect_clicked({
            let replace_action = replace_action.clone();
            move |_btn| {
                replace_action.activate(None);
            }
        });
        replace_all_btn.connect_clicked({
            let replace_all_action = replace_all_action.clone();
            move |_btn| {
                replace_all_action.activate(None);
            }
        });
        Self {
            dialog,
            find_entry,
            search_up_btn,
            search_down_btn,
            replace_entry,
            find_btn,
            replace_btn,
            replace_all_btn,
            matches_lbl,
            find_action,
            replace_action,
            replace_all_action
        }
    }

}

impl React<MainMenu> for FindDialog {

    fn react(&self, menu : &MainMenu) {
        menu.action_find_replace.connect_activate({
            let dialog = self.dialog.clone();
            move |_, _| {
                dialog.show();
            }
        });
    }

}

impl React<QueriesEditor> for FindDialog {

    fn react(&self, editor : &QueriesEditor) {
        let ctx : Rc<RefCell<Option<(SearchContext, Option<TextIter>, Option<TextIter>)>>> = Default::default();
        {
            let ctx = ctx.clone();
            let views = editor.views.clone();
            let matches_lbl = self.matches_lbl.clone();
            let find_action = self.find_action.clone();
            self.search_down_btn.connect_clicked(move |_| {
                if let Some(ix) = get_index(&find_action) {
                    move_match(&views[ix], &ctx, &matches_lbl, true);
                }
            });
        }

        {
            let ctx = ctx.clone();
            let views = editor.views.clone();
            let matches_lbl = self.matches_lbl.clone();
            let find_action = self.find_action.clone();
            self.search_up_btn.connect_clicked(move |_| {
                if let Some(ix) = get_index(&find_action) {
                    move_match(&views[ix], &ctx, &matches_lbl, false);
                }
            });
        }

        {
            let views = editor.views.clone();
            let find_entry = self.find_entry.clone();
            let ctx = ctx.clone();
            let matches_lbl = self.matches_lbl.clone();
            let (replace_btn, replace_all_btn) = (self.replace_btn.clone(), self.replace_all_btn.clone());
            self.find_action.connect_activate(move |action, _| {
                if let Some(ix) = get_index(&action) {
                    let txt = find_entry.text().to_string();
                    if let Ok(mut ctx) = ctx.try_borrow_mut() {
                        if txt.is_empty() {
                            if let Some(new_ctx) = clear_search(&views[ix]) {
                                *ctx = Some((new_ctx, None, None));
                            } else {
                                eprintln!("Unable to get text buffer to create search context");
                            }
                        } else {
                            if let Some(new_ctx) = start_search(&views[ix], &txt) {
                                *ctx = Some((new_ctx, None, None));
                            } else {
                                eprintln!("Unable to get text buffer to create search context");
                            }
                        }
                    } else {
                        eprintln!("Unable to borrow search context");
                    }

                    if txt.is_empty() {
                        matches_lbl.set_markup(NO_MATCHES);
                        replace_btn.set_sensitive(false);
                        replace_all_btn.set_sensitive(false);
                    } else {
                        let n_found = move_match(&views[ix], &ctx, &matches_lbl, true);
                        let sensitive = if let Some(n_found) = n_found {
                            n_found >= 1
                        } else {
                            false
                        };
                        replace_btn.set_sensitive(sensitive);
                        replace_all_btn.set_sensitive(sensitive);
                    }
                } else {
                    eprintln!("No index available");
                }
            });
        }

        {
            let ctx = ctx.clone();
            let replace_entry = self.replace_entry.clone();
            let matches_lbl = self.matches_lbl.clone();
            self.replace_action.connect_activate(move |_action, _| {
                let new_txt = replace_entry.text().to_string();
                let mut ctx = ctx.borrow_mut();
                if let Some((ref ctx, Some(ref mut start), Some(ref mut end))) = &mut *ctx {
                    if ctx.settings().search_text().is_some() {
                        if let Err(e) = ctx.replace(start, end, &new_txt[..]) {
                            eprintln!("{}", e);
                        }
                    }
                }
                *ctx = None;
                matches_lbl.set_markup(NO_MATCHES);
            });
        }

        {
            let ctx = ctx.clone();
            let replace_all_entry = self.replace_entry.clone();
            let matches_lbl = self.matches_lbl.clone();
            self.replace_all_action.connect_activate(move |_action, _| {
                let new_txt = replace_all_entry.text().to_string();
                let mut ctx = ctx.borrow_mut();
                if let Some((ref ctx, _, _)) = &*ctx {
                    if ctx.settings().search_text().is_some() {
                        match ctx.replace_all(&new_txt[..]) {
                            Ok(_n) => { },
                            Err(e) => { eprintln!("{}", e) }
                        }
                    }
                }
                *ctx = None;
                matches_lbl.set_text(NO_MATCHES);
            });
        }

        {
            let find_btn = self.find_btn.clone();
            let replace_btn = self.replace_btn.clone();
            let replace_all_btn = self.replace_all_btn.clone();
            let ctx = ctx.clone();
            let views = editor.views.clone();
            let find_action = self.find_action.clone();
            self.find_entry.connect_changed(move |entry| {
                let txt = entry.text().to_string();
                let sensitive = txt.len() >= 1;
                find_btn.set_sensitive(sensitive);
                replace_btn.set_sensitive(false);
                replace_all_btn.set_sensitive(false);
                if txt.is_empty() {
                    if let Some(ix) = get_index(&find_action) {
                        if let Ok(mut ctx) = ctx.try_borrow_mut() {
                            if let Some(new_ctx) = clear_search(&views[ix]) {
                                *ctx = Some((new_ctx, None, None));
                            } else {
                                eprintln!("Unable to get text buffer to create search context");
                            }
                        }
                    }
                }
            });
        }

        {
            let ctx = ctx.clone();
            let matches_lbl = self.matches_lbl.clone();
            let _find_entry = self.find_entry.clone();
            let _replace_entry = self.replace_entry.clone();
            let (find_btn, replace_btn, replace_all_btn) = (self.find_btn.clone(), self.replace_btn.clone(), self.replace_all_btn.clone());
            self.dialog.connect_close(move |_| {
                *(ctx.borrow_mut()) = None;
                matches_lbl.set_text(NO_MATCHES);
                find_btn.set_sensitive(false);
                replace_btn.set_sensitive(false);
                replace_all_btn.set_sensitive(false);
            });
        }
    }

}

impl React<OpenedScripts> for FindDialog {

    fn react(&self, scripts : &OpenedScripts) {
        let find_action = self.find_action.clone();
        let replace_action = self.replace_action.clone();
        let replace_all_action = self.replace_all_action.clone();
        scripts.connect_selected(move |opt_file| {
            if let Some(ix) = opt_file.map(|f| f.index ) {
                find_action.set_state(&(ix as i32).to_variant());
                replace_action.set_state(&(ix as i32).to_variant());
                replace_all_action.set_state(&(ix as i32).to_variant());
            } else {
                find_action.set_state(&(-1i32).to_variant());
                replace_action.set_state(&(-1i32).to_variant());
                replace_all_action.set_state(&(-1i32).to_variant());
            }
        });
        scripts.connect_closed({
            let find_action = self.find_action.clone();
            let replace_action = self.replace_action.clone();
            let replace_all_action = self.replace_all_action.clone();
            move |(old_file, remaining)| {
                let curr_state = find_action.state().unwrap().get::<i32>().unwrap();
                if remaining > 0 {
                    if curr_state == old_file.index as i32 {
                        find_action.set_state(&(-1i32).to_variant());
                        replace_action.set_state(&(-1i32).to_variant());
                        replace_all_action.set_state(&(-1i32).to_variant());
                    } else if curr_state > old_file.index as i32 {
                        find_action.set_state(&(curr_state - 1).to_variant());
                        replace_action.set_state(&(curr_state - 1).to_variant());
                        replace_all_action.set_state(&(curr_state - 1).to_variant());
                    }
                } else {
                    find_action.set_state(&(-1i32).to_variant());
                    replace_action.set_state(&(-1i32).to_variant());
                    replace_all_action.set_state(&(-1i32).to_variant());
                }
            }
        });
    }

}

const NO_MATCHES : &str = "<b>Matches : 0</b>";

fn clear_search(view : &View) -> Option<SearchContext> {
    let buffer = view.buffer();
    let settings = SearchSettings::new();
    settings.set_search_text(None);
    settings.set_wrap_around(true);
    let downcasted_buffer : Buffer = buffer.clone().downcast().unwrap();
    let new_ctx = SearchContext::new(&downcasted_buffer, Some(&settings));
    Some(new_ctx)
}

fn start_search(view : &View, txt : &str) -> Option<SearchContext> {
    let buffer = view.buffer();
    let settings = SearchSettings::new();
    settings.set_search_text(Some(&txt));
    settings.set_wrap_around(true);
    let downcasted_buffer : Buffer = buffer.clone().downcast().unwrap();
    let new_ctx = SearchContext::new(&downcasted_buffer, Some(&settings));
    Some(new_ctx)
}

fn move_match(
    view : &View,
    ctx : &Rc<RefCell<Option<(SearchContext, Option<TextIter>, Option<TextIter>)>>>,
    matches_label : &Label,
    is_forward : bool
) -> Option<usize> {
    let buffer = view.buffer();
    if let Ok(mut ctx) = ctx.try_borrow_mut() {
        if let Some(ref mut ctx) = *ctx {
            let (ctx, start, end) = (&ctx.0, &mut ctx.1, &mut ctx.2);
            let start_search = match (is_forward, start.clone(), end.clone()) {
                (_, None, None) => {
                    buffer.start_iter()
                },
                (true, Some(_start), Some(end)) => {
                    end.clone()
                },
                (false, Some(start), Some(_end)) => {
                    start.clone()
                },
                _ => { eprintln!("No start buffer"); return None; }
            };
            let next_match = if is_forward {
                ctx.forward(&start_search)
            } else {
                ctx.backward(&start_search)
            };
            if let Some((t1, t2, _)) = next_match {
                buffer.select_range(&t1, &t2);
                let mark = buffer.get_insert();
                buffer.move_mark(&mark, &t1);
                view.scroll_mark_onscreen(&mark);
                let pos = ctx.occurrence_position(&t1, &t2);
                let count = ctx.occurrences_count();
                matches_label.set_markup(&format!("<b>Match : {}/{}</b>", pos, count));
                *start = Some(t1);
                *end = Some(t2);
                if count >= 0 {
                    Some(count as usize)
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            eprintln!("Unable to borrow context");
            None
        }
    } else {
        eprintln!("Unabele to borrow search context");
        None
    }
}

fn get_index(action : &gio::SimpleAction) -> Option<usize> {
    action.state().unwrap().get::<i32>()
        .and_then(|ix| if ix == -1 { None } else { Some(ix as usize) })
}

impl React<QueriesSettings> for QueriesEditor {

    fn react(&self, settings : &QueriesSettings) {
        settings.editor_bx.scheme_combo.connect_changed({
            let editor = self.clone();
            move|combo| {
                if let Some(txt) = combo.active_text() {
                    let mut state = editor.user_state.borrow_mut();
                    state.editor.scheme = txt.to_string();
                    editor.configure(&state.editor);
                }
            }
        });
        settings.editor_bx.font_btn.connect_font_set({
            let editor = self.clone();
            move |btn| {
                if let Some(s) = btn.font() {
                    let mut state = editor.user_state.borrow_mut();
                    if let Some((font_family, font_size)) = crate::ui::parse_font(&s[..]) {
                        state.editor.font_family = font_family;
                        state.editor.font_size = font_size;
                        editor.configure(&state.editor);
                    } else {
                        eprintln!("Could not parse font");
                    }
                }
            }
        });
        settings.editor_bx.line_num_switch.connect_state_set({
            let editor = self.clone();
            move |switch, _| {
                let mut state = editor.user_state.borrow_mut();
                state.editor.show_line_numbers = switch.is_active();
                editor.configure(&state.editor);
                glib::signal::Propagation::Proceed
            }
        });
        settings.editor_bx.line_highlight_switch.connect_state_set({
            let editor = self.clone();
            move |switch, _| {
                let mut state = editor.user_state.borrow_mut();
                state.editor.highlight_current_line = switch.is_active();
                editor.configure(&state.editor);
                glib::signal::Propagation::Proceed
            }
        });
    }

}

use sourceview5::CompletionContext;
use sourceview5::CompletionProposal;
use sourceview5::CompletionActivation;
use sourceview5::{CompletionCell, CompletionProvider};
use sourceview5::subclass::completion_provider::*;
use sourceview5::subclass::prelude::*;
use glib::{prelude::*, subclass::prelude::*};
use std::path::Path;
use gio::ListModel;

/* Blocking issue (gtksourceview-5 0.7.1):
GtkSourceView-CRITICAL: assertion GTK_SOURCE_IS_COMPLETION_PROVIDER(self) failed */
glib::wrapper! {
    pub struct SqlCompletionProvider(ObjectSubclass<imp::SqlCompletionProvider>)
        @implements CompletionProvider;
}

impl SqlCompletionProvider {
    pub fn new() -> Self {
        glib::Object::new::<SqlCompletionProvider>()
    }
}

pub mod imp {
    use std::cell::RefCell;

    use super::*;
    pub struct SqlCompletionProvider(Rc<RefCell<Option<Vec<String>>>>);
    impl Default for SqlCompletionProvider {
        fn default() -> Self {
            SqlCompletionProvider(Rc::new(RefCell::new(Some(vec![String::from("mytable")]))))
        }
    }
    #[glib::object_subclass]
    impl ObjectSubclass for SqlCompletionProvider {
        const NAME: &'static str = "SqlCompletionProvider";
        type Type = super::SqlCompletionProvider;
        type ParentType = glib::Object;
        type Interfaces = (CompletionProvider,);
    }
    impl ObjectImpl for SqlCompletionProvider {}
    impl CompletionProviderImpl for SqlCompletionProvider {

        fn is_trigger(&self, iter: &TextIter, c : char) -> bool {
            false
        }

        /*fn display(
            &self,
            context: &CompletionContext,
            proposal: &impl IsA<CompletionProposal>,
            cell: &CompletionCell
        ) {
            // cell.set_text(proposal.typed_text());
            // cell.set_icon_name("table-symbolic");
        }*/

        fn title(&self) -> Option<glib::GString> {
            Some("sql".into())
        }

        /*fn populate_async<P: FnOnce(Result<ListModel, Error>) + 'static>(
            &self,
            context: &CompletionContext,
            cancellable: Option<&impl IsA<gio::Cancellable>>,
            callback: P
        ) {

        }*/

        /*fn populate(&self, context: &CompletionContext) -> Result<ListModel, gtk4::glib::Error> {
            let Some(inner) = self.0.borrow().clone() else { panic!() };
            let tables = &inner.0;
            let word = context.word().to_string();
            let mut candidates : Vec<glib::Object> = tables.iter()
                .map(|tbl| {
                    let compl = CompletionCell::builder().text(tbl).build();
                    compl.set_icon_name("table-symbolic");
                    compl.upcast()
                }).collect();
            let obj = self.obj();
            let provider = obj.dynamic_cast_ref::<CompletionProvider>().unwrap();
            // context.set_proposals_for_provider(provider, Some(&*candidates));

            // let store = ListStore::new(&[candidates[0].type_()]);
            // let provider = obj.dynamic_cast_ref::<CompletionProvider>().unwrap();
            // context.add_proposals(provider, &*candidates, true);

            /*for cand in candidates {
                let iter = store.append();
                store.set(&iter, &[(0,&cand)]);
            }*/

            /*let list = StringList::new(&["World"]);
            list.append("Hello");
            let flm = FilterListModel::new(Some(&list), None::<&AnyFilter>);
            Ok(flm.model().unwrap())*/

            Ok(MyList::new().upcast())
        }*/
    }
}

use gtk4::subclass::prelude::ListModelImpl;
glib::wrapper! {
    pub struct MyList(ObjectSubclass<MyListInner>)
        @implements ListModel;
}
impl MyList {
    pub fn new() -> Self {
        glib::Object::new::<MyList>()
    }
}
#[derive(Default)]
pub struct MyListInner { }
#[glib::object_subclass]
impl ObjectSubclass for MyListInner {
    const NAME: &'static str = "MyList";
    type Type = MyList;
    type ParentType = glib::Object;
    type Interfaces = (gio::ListModel,);
}
impl ObjectImpl for MyListInner {}
impl ListModelImpl for MyListInner {
    fn item_type(&self) -> glib::Type {
        // CompletionCell::builder().build().type_()
        MyProposal::new().type_()
    }
    fn n_items(&self) -> u32 {
        1
    }
    fn item(&self, position: u32) -> Option<glib::Object> {
        /*let compl = CompletionCell::builder().text("mytable").build();
        compl.set_icon_name("table-symbolic");
        Some(compl.upcast())*/
        //Some(COMPL.get().unwrap().upcast())
        Some(MyProposal::new().upcast())
    }
}

glib::wrapper! {
    pub struct MyProposal(ObjectSubclass<MyInnerProposal>)
        @implements CompletionProposal;
}
impl MyProposal {
    pub fn new() -> Self {
        glib::Object::new::<MyProposal>()
    }
}
#[derive(Default)]
pub struct MyInnerProposal { }
#[glib::object_subclass]
impl ObjectSubclass for MyInnerProposal {
    const NAME: &'static str = "MyProposal";
    type Type = MyProposal;
    type ParentType = glib::Object;
    type Interfaces = (CompletionProposal,);
}
impl ObjectImpl for MyInnerProposal {}
impl CompletionProposalImpl for MyInnerProposal { }
