/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use gtk4::prelude::*;
use gtk4::*;
use libadwaita;
use crate::client::ActiveConnection;
use stateful::React;
use crate::client::Environment;
use crate::sql::StatementOutput;
use crate::client::OpenedScripts;
use crate::sql::object::{DBType, DBColumn};
use core::cell::RefCell;
use std::rc::Rc;
use filecase::MultiArchiverImpl;
use crate::client::SharedUserState;
use crate::client::Engine;

// TODO set find/replace insensitive when workspace is selected.

pub mod model;

pub use model::*;

mod overview;

pub mod analyze;

pub use analyze::*;

pub mod plots;

pub use plots::*;

pub use overview::*;

mod title;

pub use title::*;

mod workspace;

pub use workspace::*;

mod editor;

pub use editor::*;

mod sidebar;

pub use sidebar::*;

mod menu;

pub use menu::*;

mod schema_tree;

pub use schema_tree::*;

mod file_list;

pub use file_list::*;

mod table;

pub use table::*;

mod create;

pub use create::*;

mod plotarea;

pub use plotarea::*;

mod settings;

pub use settings::*;

mod form;

pub use form::*;

pub mod builder;

pub use builder::*;

pub mod apply;

pub type SharedSignal = Rc<RefCell<Option<glib::SignalHandlerId>>>;

// QueriesContent means everything outside the titlebar and sidebar.
#[derive(Debug, Clone)]
pub struct QueriesContent {

    pub switcher : libadwaita::ViewSwitcher,
    pub overlay : libadwaita::ToastOverlay,
    pub results : QueriesResults,
    pub editor : QueriesEditor,
    pub results_page : Rc<RefCell<libadwaita::ViewStackPage>>,
    pub editor_page : Rc<RefCell<libadwaita::ViewStackPage>>,
    pub curr_toast : Rc<RefCell<Option<libadwaita::Toast>>>,

    /* Parent widget for stacked mode. */
    pub stack : libadwaita::ViewStack,

    /* Parent widget for split mode */
    pub inner_paned : Paned

}

#[derive(Debug, Clone)]
pub struct QueriesResults {
    pub stack : Stack,
    pub workspace : QueriesWorkspace,
    pub overview : QueriesOverview
}

impl QueriesResults {

    pub fn build() -> Self {
        let stack = Stack::new();
        let overview = QueriesOverview::build();
        let workspace = QueriesWorkspace::build();
        stack.add_named(&overview.bx, Some("overview"));
        stack.add_named(&workspace.bx, Some("tables"));
        stack.set_visible_child_name("overview");
        Self { stack, workspace, overview }
    }

}

impl React<Environment> for QueriesContent {

    fn react(&self, env : &Environment) {
        let content_stack = self.stack.clone();
        let results_stack = self.results.stack.clone();
        env.connect_table_update(move |_tables| {
            if content_stack.child_by_name("results").is_some() {
                content_stack.set_visible_child_name("results");
            }
            results_stack.set_visible_child_name("tables");
        });
        env.connect_export_error({
            let overlay = self.overlay.clone();
            let curr_toast = self.curr_toast.clone();
            move |msg| {
                let mut last_toast = curr_toast.borrow_mut();
                if let Some(t) = last_toast.take() {
                    t.dismiss();
                }
                let toast = libadwaita::Toast::builder().title(&msg[..]).build();
                overlay.add_toast(toast.clone());
                connect_toast_dismissed(&toast, &curr_toast);
                *last_toast = Some(toast);
            }
        });
        env.connect_table_error({
            let overlay = self.overlay.clone();
            let curr_toast = self.curr_toast.clone();
            move |msg| {
                let mut last_toast = curr_toast.borrow_mut();
                if let Some(t) = last_toast.take() {
                    t.dismiss();
                }
                let toast = libadwaita::Toast::builder().title(&msg[..]).build();
                overlay.add_toast(toast.clone());
                connect_toast_dismissed(&toast, &curr_toast);
                *last_toast = Some(toast);
            }
        });
    }

}

impl React<QueriesWorkspace> for QueriesContent {

    fn react(&self, ws : &QueriesWorkspace) {
        let stack = self.results.stack.clone();
        let results_page = self.results_page.clone();
        ws.tab_view.connect_close_page(move |tab_view, _page| {
            if tab_view.n_pages() == 1 {
                stack.set_visible_child_name("overview");
                results_page.borrow().set_icon_name(Some("db-symbolic"));
            }
            false
        });
        let results_page = self.results_page.clone();
        ws.tab_view.connect_page_attached(move |_tab_view, _page, _pos| {
            results_page.borrow().set_icon_name(Some("table-symbolic"));
        });
    }

}

impl React<OpenedScripts> for QueriesContent {

    fn react(&self, scripts : &OpenedScripts) {
        scripts.connect_close_confirm({
            let overlay = self.overlay.clone();
            let curr_toast = self.curr_toast.clone();
            move |file| {
                let mut last_toast = curr_toast.borrow_mut();
                if let Some(t) = last_toast.take() {
                    t.dismiss();
                }
                let toast = libadwaita::Toast::builder()
                    .title(&format!("{} has unsaved changes", file.name))
                    .button_label("Close anyway")
                    .action_name("win.ignore_file_save")
                    .action_target(&(file.index as i32).to_variant())
                    .priority(libadwaita::ToastPriority::High)
                    .timeout(0)
                    .build();
                overlay.add_toast(toast.clone());
                connect_toast_dismissed(&toast, &curr_toast);
                *last_toast = Some(toast);
            }
        });
        scripts.connect_error({
            let overlay = self.overlay.clone();
            let curr_toast = self.curr_toast.clone();
            move |msg| {
                let mut last_toast = curr_toast.borrow_mut();
                if let Some(t) = last_toast.take() {
                    t.dismiss();
                }
                let toast = libadwaita::Toast::builder().title(&msg[..]).build();
                overlay.add_toast(toast.clone());
                *last_toast = Some(toast);
            }
        });
    }

}

impl QueriesContent {

    pub fn switch_to_split(&self) {
        if self.inner_paned.start_child().is_some() {
            return;
        }
        self.stack.remove(&self.editor.stack);
        self.stack.remove(&self.results.stack);
        self.inner_paned.set_position(360);
        self.inner_paned.set_start_child(Some(&self.editor.stack));
        self.inner_paned.set_end_child(Some(&self.results.stack));
        self.overlay.set_child(Some(&self.inner_paned));
        self.switcher.set_visible(false);
    }

    pub fn switch_to_stacked(&self) {
        if self.inner_paned.start_child().is_none() {
            return;
        }
        self.inner_paned.set_start_child(None::<&Stack>);
        self.inner_paned.set_end_child(None::<&Stack>);
        let editor_page = self.stack.add_named(&self.editor.stack, Some("editor"));
        let results_page = self.stack.add_named(&self.results.stack, Some("results"));
        editor_page.set_icon_name(Some("accessories-text-editor-symbolic"));
        results_page.set_icon_name(Some("db-symbolic"));
        *self.editor_page.borrow_mut() = editor_page;
        *self.results_page.borrow_mut() = results_page;
        self.stack.set_visible_child_name("results");
        self.overlay.set_child(Some(&self.stack));
        self.switcher.set_visible(true);
    }

    fn build(state : &SharedUserState) -> Self {
        let editor = QueriesEditor::build(state);
        let results = QueriesResults::build();
        let stack = libadwaita::ViewStack::new();

        // Use those for stacked view
        let editor_page = stack.add_named(&editor.stack, Some("editor"));
        let results_page = stack.add_named(&results.stack, Some("results"));

        // Use those for split view
        // let editor_page = stack.add_named(&Label::new(None), None);
        // let results_page = stack.add_named(&Label::new(None), None);

        editor_page.set_icon_name(Some("accessories-text-editor-symbolic"));
        results_page.set_icon_name(Some("db-symbolic"));
        stack.set_visible_child_name("results");

        let switcher = libadwaita::ViewSwitcher::builder()
            .stack(&stack)
            .can_focus(false)
            .policy(libadwaita::ViewSwitcherPolicy::Wide)
            .build();
        let overlay = libadwaita::ToastOverlay::new();
        overlay.set_opacity(1.0);
        overlay.set_visible(true);
        overlay.set_child(Some(&stack));
        // Use those for split view

        // Stacked view
        //
        let inner_paned = Paned::new(Orientation::Vertical);
        inner_paned.set_shrink_start_child(false);

        let curr_toast = Rc::new(RefCell::new(None));
        Self {
            stack,
            results,
            editor,
            switcher,
            overlay,
            results_page : Rc::new(RefCell::new(results_page)),
            editor_page : Rc::new(RefCell::new(editor_page)),
            curr_toast,
            inner_paned
        }
    }

}

impl React<ActiveConnection> for QueriesContent {

    fn react(&self, conn : &ActiveConnection) {
        conn.connect_db_error({
            let overlay = self.overlay.clone();
            let results_page = self.results_page.clone();
            let stack = self.results.stack.clone();
            let curr_toast = self.curr_toast.clone();
            move |err : String| {
                let mut last_toast = curr_toast.borrow_mut();
                if let Some(t) = last_toast.take() {
                    t.dismiss();
                }
                let toast = libadwaita::Toast::builder().title(&err).build();
                overlay.add_toast(toast.clone());
                connect_toast_dismissed(&toast, &curr_toast);
                *last_toast = Some(toast);
                results_page.borrow().set_icon_name(Some("db-symbolic"));
                if let Some(curr_name) = stack.visible_child_name() {
                    if &curr_name[..] != "overview" {
                        stack.set_visible_child_name("overview");   
                    }
                }
            }
        });
        conn.connect_db_conn_failure({
            let overlay = self.overlay.clone();
            let results_page = self.results_page.clone();
            let stack = self.results.stack.clone();
            let curr_toast = self.curr_toast.clone();
            move |(_info, err)| {
                let mut last_toast = curr_toast.borrow_mut();
                if let Some(t) = last_toast.take() {
                    t.dismiss();
                }
                let toast = libadwaita::Toast::builder().title(&err).build();
                overlay.add_toast(toast.clone());
                connect_toast_dismissed(&toast, &curr_toast);
                *last_toast = Some(toast);
                results_page.borrow().set_icon_name(Some("db-symbolic"));
                if let Some(curr_name) = stack.visible_child_name() {
                    if &curr_name[..] != "overview" {
                        stack.set_visible_child_name("overview");   
                    }
                }
            }
        });
        conn.connect_exec_result({
            let overlay = self.overlay.clone();
            let results_page = self.results_page.clone();
            let curr_toast = self.curr_toast.clone();
            move |res : Vec<StatementOutput>| {
                let mut any_errors = false;
                let msg = if let Some(err) = crate::sql::condense_errors(&res) {
                    any_errors = true;
                    Some(err)
                } else if let Some(msg) = crate::sql::condense_statement_outputs(&res) {
                    Some(msg)
                } else {
                    None
                };
                if let Some(msg) = msg {
                    let mut last_toast = curr_toast.borrow_mut();
                    if let Some(t) = last_toast.take() {
                        t.dismiss();
                    }
                    let toast = libadwaita::Toast::builder().title(&msg).build();
                    overlay.add_toast(toast.clone());
                    connect_toast_dismissed(&toast, &curr_toast);
                    *last_toast = Some(toast);
                }
                if !any_errors {
                    let has_any_tbl = res.iter()
                        .filter(|res| {
                            match res {
                                StatementOutput::Valid(_, _) => true,
                                _ => false
                            }
                        }).next().is_some();
                    if has_any_tbl {
                        results_page.borrow().set_icon_name(Some("table-symbolic"));
                    }
                }
            }
        });
    }

}

impl React<FileList> for QueriesContent {

    fn react(&self, list : &FileList) {
        let switcher = self.switcher.clone();
        list.list.connect_row_selected(move |_, opt_row| {
            if opt_row.is_some() {
                if switcher.stack().unwrap().child_by_name("editor").is_some() {
                    switcher.stack().unwrap().set_visible_child_name("editor");
                }
            }
        });

    }

}

impl React<ExecButton> for QueriesContent {

    fn react(&self, exec_btn : &ExecButton) {
        let stack = self.results.stack.clone();
        let results_page = self.results_page.clone();
        exec_btn.clear_action.connect_activate(move |_action, _param| {
            stack.set_visible_child_name("overview");
            results_page.borrow().set_icon_name(Some("db-symbolic"));
        });
    }

}

#[derive(Debug, Clone)]
pub struct QueriesWindow {
    pub window : ApplicationWindow,
    pub paned : Paned,
    pub titlebar : QueriesTitlebar,
    pub sidebar : QueriesSidebar,
    pub content : QueriesContent,
    pub graph_win : plots::GraphWindow,
    pub builder_win : QueryBuilderWindow,
    pub settings : QueriesSettings,
    pub find_dialog : FindDialog,
    pub model : model::ModelWindow,
    pub apply : apply::ApplyWindow
}

impl QueriesWindow {

    pub fn build(
        app : &Application,
        state : &SharedUserState,
        modules : &apply::Modules,
        params : &Rc<RefCell<apply::CallParams>>
    ) -> Self {

        let window = ApplicationWindow::builder()
            .application(app)
            .title("Queries")
            .default_width(1440)
            .default_height(960)
            .build();

        let sidebar = QueriesSidebar::build();
        let titlebar = QueriesTitlebar::build();
        let content = QueriesContent::build(state);

        content.results.overview.db_new_dialog.dialog.set_transient_for(Some(&window));
        content.results.overview.db_open_dialog.dialog.set_transient_for(Some(&window));

        let find_dialog = FindDialog::build();

        content.editor.save_dialog.0.dialog.set_transient_for(Some(&window));
        content.editor.open_dialog.0.dialog.set_transient_for(Some(&window));
        content.editor.export_dialog.dialog.set_transient_for(Some(&window));
        sidebar.schema_tree.form.dialog.set_transient_for(Some(&window));
        sidebar.schema_tree.create_dialog.dialog.set_transient_for(Some(&window));
        sidebar.schema_tree.report_dialog.dialog.set_transient_for(Some(&window));
        sidebar.schema_tree.report_export_dialog.dialog.set_transient_for(Some(&window));
        sidebar.schema_tree.import_dialog.dialog.set_transient_for(Some(&window));
        sidebar.schema_tree.react(&content.results.overview.conn_bx);
        find_dialog.dialog.set_transient_for(Some(&window));

        // titlebar.header.set_title(Some("Queries"));
        titlebar.header.set_title_widget(Some(&content.switcher));

        let paned = Paned::new(Orientation::Horizontal);
        paned.set_position(200);
        paned.set_start_child(Some(&sidebar.paned));

        paned.set_end_child(Some(&content.overlay));

        window.set_child(Some(&paned));
        window.set_titlebar(Some(&titlebar.header));
        window.set_decorated(true);

        // Add actions to main menu
        window.add_action(&titlebar.main_menu.action_new);
        window.add_action(&titlebar.main_menu.action_open);
        window.add_action(&titlebar.main_menu.action_save);
        window.add_action(&titlebar.main_menu.action_find_replace);
        window.add_action(&titlebar.main_menu.action_save_as);
        window.add_action(&titlebar.main_menu.action_graph);
        window.add_action(&titlebar.main_menu.action_builder);
        window.add_action(&titlebar.main_menu.action_export);
        window.add_action(&titlebar.main_menu.action_settings);
        window.add_action(&titlebar.main_menu.action_about);
        window.add_action(&titlebar.main_menu.action_apply);
        window.add_action(&titlebar.main_menu.action_model);
        window.add_action(&content.editor.ignore_file_save_action);
        window.add_action(&titlebar.sidebar_hide_action);

        // Add actions to execution menu
        window.add_action(&titlebar.exec_btn.queue_exec_action);
        window.add_action(&titlebar.exec_btn.exec_action);
        window.add_action(&titlebar.exec_btn.set_plan_action);
        window.add_action(&titlebar.exec_btn.set_exec_action);
        window.add_action(&titlebar.exec_btn.clear_action);
        window.add_action(&titlebar.exec_btn.restore_action);

        window.add_action(&sidebar.file_list.close_action);

        // Add action to schema tree.
        window.add_action(&sidebar.schema_tree.query_action);
        window.add_action(&sidebar.schema_tree.insert_action);
        window.add_action(&sidebar.schema_tree.import_action);
        window.add_action(&sidebar.schema_tree.create_action);
        window.add_action(&sidebar.schema_tree.call_action);
        window.add_action(&sidebar.schema_tree.report_action);

        content.editor.open_dialog.react(&titlebar.main_menu);
        content.editor.export_dialog.react(&titlebar.main_menu);

        content.react(&sidebar.file_list);
        titlebar.exec_btn.react(&sidebar.file_list);
        content.editor.react(&titlebar.exec_btn);
        content.react(&titlebar.exec_btn);
        content.react(&content.results.workspace);
        titlebar.main_menu.react(&content);

        let settings = QueriesSettings::build();
        settings.settings.dialog().set_transient_for(Some(&window));

        settings.react(&titlebar.main_menu);
        window.add_action(&settings.security_bx.update_action);

        content.results.overview.sec_bx.certificate_lbl.connect_activate_link({
            let dialog = settings.settings.dialog.clone();
            let list = settings.settings.list.clone();
            move |_, _| {
                dialog.show();
                list.select_row(list.row_at_index(3).as_ref());
                glib::signal::Propagation::Stop
            }
        });
        settings.settings.dialog.connect_show({
            let state = state.clone();
            let security_bx = settings.security_bx.clone();
            move |_| {
                security_bx.update(&state.borrow().conns[..]);
            }
        });
        settings.editor_bx.split_switch.connect_state_set({
            let content = content.clone();
            move |switch,_| {
                if switch.is_active() {
                    content.switch_to_split();
                } else {
                    content.switch_to_stacked();
                }
                glib::signal::Propagation::Proceed
            }
        });

        for info in &state.borrow().conns {
            if info.engine == Engine::SQLite {
                let new_row = LocalConnectionRow::from(info.clone());
                content.results.overview.conn_list.list.append(&new_row.row);
            } else {
                let new_row = ConnectionRow::from(info);
                content.results.overview.conn_list.list.append(&new_row.row);
            }
        }

        let graph_win = plots::GraphWindow::build();
        graph_win.react(&titlebar.main_menu);

        let builder_win = QueryBuilderWindow::build();
        builder_win.react(&titlebar.main_menu);

        // TODO move to impl React<MainMenu> for ApplyWindow
        let apply = apply::ApplyWindow::build(modules.clone(), params.clone());
        titlebar.main_menu.action_apply.connect_activate({
            let dialog = apply.dialog.clone();
            move |_,_| {
                dialog.show();
            }
        });
        apply.dialog.set_transient_for(Some(&window));

        let model = model::ModelWindow::build();
        titlebar.main_menu.action_model.connect_activate({
            let dialog = model.dialog.clone();
            move |_,_| {
                dialog.show();
            }
        });
        model.dialog.set_transient_for(Some(&window));

        Self { paned, sidebar, titlebar, content, window, settings, find_dialog, graph_win, builder_win, model, apply }
    }

}

impl React<QueriesTitlebar> for QueriesWindow {

    fn react(&self, titlebar : &QueriesTitlebar) {
        let hide_action = titlebar.sidebar_hide_action.clone();
        let paned = self.paned.clone();
        titlebar.sidebar_toggle.connect_toggled(move |btn| {
            if btn.is_active() {
                let sz = hide_action.state().unwrap().get::<i32>().unwrap();
                if sz > 0 {
                    paned.set_position(sz);
                } else {
                    paned.set_position(100);
                }
            } else {
                hide_action.set_state(&paned.position().to_variant());
                paned.set_position(0);
            }
        });
    }

}

impl React<OpenedScripts> for QueriesWindow {

    fn react(&self, scripts : &OpenedScripts) {
        let win = self.window.clone();
        scripts.connect_window_close(move |_| {
            win.destroy();
        });
    }

}

#[derive(Debug, Clone)]
pub struct PackedImageLabel  {
    pub bx : Box,
    pub img : Image,
    pub lbl : Label
}

impl PackedImageLabel {

    pub fn build(icon_name : &str, label_name : &str) -> Self {
        let bx = Box::new(Orientation::Horizontal, 0);
        let img = Image::from_icon_name(icon_name);
        let lbl = Label::new(Some(label_name));
        set_margins(&img, 6, 6);
        set_margins(&lbl, 6, 6);
        bx.append(&img);
        bx.append(&lbl);
        Self { bx, img, lbl }
    }

    pub fn extract(bx : &Box) -> Option<Self> {
        let img = get_child_by_index::<Image>(&bx, 0);
        let lbl = get_child_by_index::<Label>(&bx, 1);
        Some(Self { bx : bx.clone(), lbl, img })
    }

    pub fn change_label(&self, label_name : &str) {
        self.lbl.set_text(label_name);
    }

    pub fn change_icon(&self, icon_name : &str) {
        self.img.set_icon_name(Some(icon_name));
    }

}

#[derive(Debug, Clone)]
pub struct PackedImageEntry  {
    pub bx : Box,
    pub img : Image,
    pub entry : Entry
}

impl PackedImageEntry {

    pub fn build(icon_name : &str, entry_placeholder : &str) -> Self {
        let bx = Box::new(Orientation::Horizontal, 0);
        let img = Image::from_icon_name(icon_name);
        let entry = Entry::new();
        entry.set_placeholder_text(Some(entry_placeholder));
        set_margins(&img, 6, 6);
        set_margins(&entry, 6, 6);
        bx.append(&img);
        bx.append(&entry);
        Self { bx, img : img, entry }
    }

}

#[derive(Debug, Clone)]
pub struct PackedImagePasswordEntry  {
    pub bx : Box,
    _img : Image,
    pub entry : PasswordEntry
}

impl PackedImagePasswordEntry {

    pub fn build(icon_name : &str, entry_placeholder : &str) -> Self {
        let bx = Box::new(Orientation::Horizontal, 0);
        let img = Image::from_icon_name(icon_name);
        let entry = PasswordEntry::new();
        entry.set_placeholder_text(Some(entry_placeholder));
        set_margins(&img, 6, 6);
        set_margins(&entry, 6, 6);
        bx.append(&img);
        bx.append(&entry);
        Self { bx, _img : img, entry }
    }

}

pub fn set_margins<W : WidgetExt>(w : &W, horizontal : i32, vertical : i32) {
    w.set_margin_start(horizontal);
    w.set_margin_end(horizontal);
    w.set_margin_top(vertical);
    w.set_margin_bottom(vertical);
}

pub fn show_popover_on_toggle(popover : &Popover, toggle : &ToggleButton, alt : Vec<ToggleButton>) {
    toggle.connect_toggled({
        let popover = popover.clone();
        move |btn| {
            if btn.is_active() {
                popover.show();
                for toggle in alt.iter() {
                    if toggle.is_active() {
                        toggle.set_active(false);
                    }
                }
            } else {
                popover.hide();
            }
        }
    });

    popover.connect_closed({
        let toggle = toggle.clone();
        move |_| {
            if toggle.is_active() {
                toggle.set_active(false);
            }
        }
    });
}

pub fn title_label(txt : &str) -> Label {
    let lbl = Label::builder()
        .label(&format!("<span font_weight=\"600\" font_size=\"large\" fgalpha=\"60%\">{}</span>", txt))
        .use_markup(true)
        .justify(Justification::Left)
        .halign(Align::Start)
        .build();
    set_margins(&lbl, 0, 12);
    lbl
}

pub fn get_sibling_by_index<U, W>(w : &U, pos : usize) -> W
where
    U : WidgetExt,
    W : IsA<glib::Object>
{
    let parent = w.parent().clone().unwrap().downcast::<Box>().unwrap();
    get_child_by_index::<W>(&parent, pos)
}

pub fn get_child_by_index<W>(w : &Box, pos : usize) -> W
where
    W : IsA<glib::Object>
{
    w.observe_children().item(pos as u32).unwrap().clone().downcast::<W>().unwrap()
}

fn set_border_to_title(bx : &Box) {
    let provider = CssProvider::new();
    let css = if libadwaita::StyleManager::default().is_dark() {
        "* { border-bottom : 1px solid #454545; } "
    } else {
        "* { border-bottom : 1px solid #d9dada; } "
    };
    provider.load_from_data(css);
    bx.style_context().add_provider(&provider, 800);
}

#[derive(Debug, Clone)]
pub struct ButtonPairBox {
    pub left_btn : Button,
    pub right_btn : Button,
    pub bx : Box
}

impl ButtonPairBox {

    pub fn build(left_icon : &str, right_icon : &str) -> Self {
        let left_btn = Button::builder()
            .icon_name(left_icon)
            .halign(Align::Fill)
            .hexpand(true)
            .build();
        left_btn.set_width_request(64);
        left_btn.style_context().add_class("flat");
        let right_btn = Button::builder()
            .icon_name(right_icon)
            .halign(Align::Fill)
            .hexpand(true)
            .build();
        let bx = Box::new(Orientation::Horizontal, 0);
        for btn in [&left_btn, &right_btn] {
            btn.set_width_request(64);
            btn.style_context().add_class("flat");
            btn.set_focusable(false);
            bx.append(btn);
        }
        Self { left_btn, right_btn, bx }
    }

}

pub fn configure_dialog(dialog : &impl GtkWindowExt, modal : bool) {
    // dialog.set_modal(true);
    dialog.set_modal(modal);
    dialog.set_deletable(true);
    dialog.set_destroy_with_parent(true);
    dialog.set_hide_on_close(true);
}

pub fn get_type_icon_name(ty : &DBType, is_dark : bool) -> &'static str {
    if is_dark {
        match ty {
            DBType::Bool => "type-boolean-white",
            DBType::I16 | DBType::I32 | DBType::I64 => "type-integer-white",
            DBType::F32 | DBType::F64 | DBType::Numeric => "type-real-white",
            DBType::Text => "type-text-white",
            DBType::Date => "type-date-white",
            DBType::Time => "type-time-white",
            DBType::Json => "type-json-white",
            DBType::Xml => "type-xml-white",
            DBType::Bytes => "type-binary-white",
            DBType::Array => "type-array-white",
            DBType::Unknown | DBType::Trigger => "type-unknown-white",
        }
    } else {
        match ty {
            DBType::Bool => "type-boolean-symbolic",
            DBType::I16 | DBType::I32 | DBType::I64 => "type-integer-symbolic",
            DBType::F32 | DBType::F64 | DBType::Numeric => "type-real-symbolic",
            DBType::Text => "type-text-symbolic",
            DBType::Date => "type-date-symbolic",
            DBType::Time => "type-time-symbolic",
            DBType::Json => "type-json-symbolic",
            DBType::Xml => "type-xml-symbolic",
            DBType::Bytes => "type-binary-symbolic",
            DBType::Array => "type-array-symbolic",
            DBType::Unknown | DBType::Trigger => "type-unknown-symbolic",
        }
    }
}

pub fn parse_font(s : &str) -> Option<(String, i32)> {

    use regex::Regex;

    let digits_pattern = Regex::new(r"\d{2}$|\d{2}$").unwrap();
    if let Some(sz_match) = digits_pattern.find(&s) {
        let sz_txt = sz_match.as_str();
        if let Ok(font_size) = sz_txt.parse::<i32>() {
            let font_family = s.trim_end_matches(sz_txt).to_string();
            Some((font_family, font_size))
        } else {
            None
        }
    } else {
        None
    }
}

pub fn connect_toast_dismissed(t : &libadwaita::Toast, last : &Rc<RefCell<Option<libadwaita::Toast>>>) {
    let last = last.clone();
    t.connect_dismissed(move|_| {
        if let Ok(mut last) = last.try_borrow_mut() {
            *last = None;
        }
    });
}
