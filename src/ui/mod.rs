use gtk4::prelude::*;
use gtk4::*;
use libadwaita;
use crate::client::ActiveConnection;
use crate::React;
use crate::client::Environment;
use crate::sql::StatementOutput;
use crate::client::OpenedScripts;

mod overview;

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

#[derive(Debug, Clone)]
pub struct QueriesContent {
    pub stack : libadwaita::ViewStack,
    pub switcher : libadwaita::ViewSwitcher,
    pub overlay : libadwaita::ToastOverlay,
    pub results : QueriesResults,
    pub editor : QueriesEditor,
    pub results_page : libadwaita::ViewStackPage,
    pub editor_page : libadwaita::ViewStackPage
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
        // overview.bx.set_margin_bottom(0);
        // workspace.bx.set_margin_bottom(0);
        // stack.bx.set_margin_bottom(0);
        stack.add_named(&workspace.bx, Some("tables"));
        stack.set_visible_child_name("overview");
        // stack.set_visible_child_name("tables");

        /*use crate::ui::table::TableWidget;
        use crate::tables::{table::Table, column::Column};
        let tbl = Table::new(None, vec![String::from("Column 1"), String::from("Column 2")], vec![Column::from(vec![10; 10]), Column::from(vec![10; 10])]).unwrap();
        let tbl_wid = TableWidget::new_from_table(&tbl);
        let tbl_wid2 = TableWidget::new_from_table(&tbl);
        println!("Table widget created");
        let tab_page = workspace.tab_view.append(&tbl_wid.scroll_window).unwrap();
        let tab_page = workspace.tab_view.append(&tbl_wid2.scroll_window).unwrap();*/

        Self { stack, workspace, overview }
    }

}

impl React<Environment> for QueriesResults {

    fn react(&self, env : &Environment) {
        let stack = self.stack.clone();
        env.connect_table_update(move |tables| {
            stack.set_visible_child_name("tables");
        });
    }

}

impl React<ExecButton> for QueriesResults {

    fn react(&self, exec_btn : &ExecButton) {
        let stack = self.stack.clone();

        exec_btn.clear_action.connect_activate(move |_action, param| {
            stack.set_visible_child_name("overview");
        });
    }

}

impl React<QueriesWorkspace> for QueriesContent {

    fn react(&self, ws : &QueriesWorkspace) {
        let stack = self.results.stack.clone();
        let results_page = self.results_page.clone();
        ws.tab_view.connect_close_page(move |tab_view, page| {
            if tab_view.n_pages() == 1 {
                stack.set_visible_child_name("overview");
                results_page.set_icon_name(Some("db-symbolic"));
            }
            false
        });
    }

}

impl React<OpenedScripts> for QueriesContent {

    fn react(&self, scripts : &OpenedScripts) {
        let overlay = self.overlay.clone();
        scripts.connect_close_confirm({
            move |file| {
                let toast = libadwaita::Toast::builder()
                    .title(&format!("{} has unsaved changes", file.name))
                    .button_label("Close anyway")
                    .action_name("win.ignore_file_save")
                    .action_target(&(file.index as i32).to_variant())
                    .priority(libadwaita::ToastPriority::High)
                    .timeout(0)
                    .build();
                overlay.add_toast(&toast);
            }
        });
    }

}

impl QueriesContent {

    fn build() -> Self {
        let stack = libadwaita::ViewStack::new();
        let editor = QueriesEditor::build();
        let results = QueriesResults::build();
        let results_page = stack.add_named(&results.stack, Some("results")).unwrap();
        results_page.set_icon_name(Some("db-symbolic"));
        //page.connect_visible_notify(move |_| {
        //    println!("Results visible");
        // } );
        // stack.add_named(&workspace.nb, Some("workspace"));
        let editor_page = stack.add_named(&editor.stack, Some("editor")).unwrap();
        editor_page.set_icon_name(Some("accessories-text-editor-symbolic"));
        let switcher = libadwaita::ViewSwitcher::builder().stack(&stack).can_focus(false).policy(libadwaita::ViewSwitcherPolicy::Wide).build();
        let overlay = libadwaita::ToastOverlay::builder() /*.margin_bottom(10).*/ .opacity(1.0).visible(true).build();
        overlay.set_child(Some(&stack));

        // stack.set_visible_child_name("overview");

        Self { stack, results, editor, switcher, overlay, results_page, editor_page }
    }

    /*fn react(&self, titlebar : &QueriesTitlebar) {
        titlebar.editor_toggle.connect_toggled({
            let stack = self.stack.clone();
            move |_| {
                stack.set_visible_child_name("editor");
            }
        });
        titlebar.tbl_toggle.connect_toggled({
            let stack = self.stack.clone();
            move |_| {
                stack.set_visible_child_name("overview");
            }
        });
    }*/

}

impl React<ActiveConnection> for QueriesContent {

    fn react(&self, conn : &ActiveConnection) {
        conn.connect_db_error({
            let overlay = self.overlay.clone();
            let results_page = self.results_page.clone();
            let stack = self.results.stack.clone();
            move |err : String| {
                overlay.add_toast(&libadwaita::Toast::builder().title(&err).build());
                results_page.set_icon_name(Some("db-symbolic"));
                stack.set_visible_child_name("overview");
            }
        });
        conn.connect_exec_result({
            let overlay = self.overlay.clone();
            let results_page = self.results_page.clone();
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
                    overlay.add_toast(&libadwaita::Toast::builder().title(&msg).build());
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
                        results_page.set_icon_name(Some("queries-symbolic"));
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
                switcher.stack().unwrap().set_visible_child_name("editor");
            }
        });

    }

}

impl React<ExecButton> for QueriesContent {

    fn react(&self, exec_btn : &ExecButton) {
        let stack = self.stack.clone();
        exec_btn.clear_action.connect_activate(move |_action, param| {
            stack.set_visible_child_name("overview");
        });
    }

}

impl React<Environment> for QueriesContent {

    fn react(&self, env : &Environment) {
        let stack = self.stack.clone();
        env.connect_table_update(move |tables| {
            stack.set_visible_child_name("results");
        });
    }

}

#[derive(Debug, Clone)]
pub struct QueriesWindow {
    pub window : ApplicationWindow,
    pub paned : Paned,
    pub titlebar : QueriesTitlebar,
    pub sidebar : QueriesSidebar,
    pub content : QueriesContent
}

impl QueriesWindow {

    pub fn from(window : ApplicationWindow) -> Self {
        let sidebar = QueriesSidebar::build();
        let titlebar = QueriesTitlebar::build();
        let content = QueriesContent::build();

        titlebar.header.set_title_widget(Some(&content.switcher));

        // content.react(&titlebar);

        let paned = Paned::new(Orientation::Horizontal);
        paned.set_position(200);
        paned.set_start_child(&sidebar.paned);

        // let toast = libadwaita::Toast::builder().title("This is a toast").build();
        // toast.set_timeout(2);
        // toast.set_title("This is the toast title");

        // let provider = CssProvider::new();
        // provider.load_from_data("* { background-color : #000000; } ".as_bytes());
        // overlay.style_context().add_provider(&provider, 800);
        // overlay.add_toast(&toast);

        // paned.set_end_child(&content.stack);
        paned.set_end_child(&content.overlay);

        window.set_child(Some(&paned));
        window.set_titlebar(Some(&titlebar.header));
        window.set_decorated(true);

        // Add actions to main menu
        window.add_action(&titlebar.main_menu.action_new);
        window.add_action(&titlebar.main_menu.action_open);
        window.add_action(&titlebar.main_menu.action_save);
        window.add_action(&titlebar.main_menu.action_save_as);
        window.add_action(&content.editor.ignore_file_save_action);

        // Add actions to execution menu
        window.add_action(&titlebar.exec_btn.exec_action);
        window.add_action(&titlebar.exec_btn.clear_action);
        window.add_action(&titlebar.exec_btn.schedule_action);

        window.add_action(&sidebar.file_list.close_action);

        content.editor.open_dialog.react(&titlebar.main_menu);

        content.react(&sidebar.file_list);
        titlebar.exec_btn.react(&sidebar.file_list);
        // titlebar.exec_btn.react(&content.editor);
        content.editor.react(&titlebar.exec_btn);
        content.results.react(&titlebar.exec_btn);
        content.react(&content.results.workspace);
        titlebar.exec_btn.react(&content);
        titlebar.main_menu.react(&content);

        Self { paned, sidebar, titlebar, content, window }
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
        let img = Image::from_icon_name(Some(icon_name));
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
    img : Image,
    pub entry : Entry
}

impl PackedImageEntry {

    pub fn build(icon_name : &str, entry_placeholder : &str) -> Self {
        let bx = Box::new(Orientation::Horizontal, 0);
        let img = Image::from_icon_name(Some(icon_name));
        let entry = Entry::new();
        entry.set_placeholder_text(Some(entry_placeholder));
        set_margins(&img, 6, 6);
        set_margins(&entry, 6, 6);
        bx.append(&img);
        bx.append(&entry);
        Self { bx, img, entry }
    }

}

#[derive(Debug, Clone)]
pub struct PackedImagePasswordEntry  {
    pub bx : Box,
    img : Image,
    pub entry : PasswordEntry
}

impl PackedImagePasswordEntry {

    pub fn build(icon_name : &str, entry_placeholder : &str) -> Self {
        let bx = Box::new(Orientation::Horizontal, 0);
        let img = Image::from_icon_name(Some(icon_name));
        let entry = PasswordEntry::new();
        entry.set_placeholder_text(Some(entry_placeholder));
        set_margins(&img, 6, 6);
        set_margins(&entry, 6, 6);
        bx.append(&img);
        bx.append(&entry);
        Self { bx, img, entry }
    }

}

pub fn set_margins<W : WidgetExt>(w : &W, horizontal : i32, vertical : i32) {
    w.set_margin_start(horizontal);
    w.set_margin_end(horizontal);
    w.set_margin_top(vertical);
    w.set_margin_bottom(vertical);
}

/*pub fn stack_switch_on_toggle(this : &ToggleButton, this_name : &'static str, other : &ToggleButton, stack : &Stack) {
    let stack = stack.clone();
    let other = other.clone();
    this.connect_toggled(move |btn| {
        if btn.is_active() {
            stack.set_visible_child_name(this_name);
            other.set_active(false);
        }
    });
}*/

pub fn show_popover_on_toggle(popover : &Popover, toggle : &ToggleButton, alt : Vec<ToggleButton>) {
    // popover.set_relative_to(&toggle);
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

pub fn get_child_by_index<W>(w : &Box, pos : usize) -> W
where
    W : IsA<glib::Object>
{
    w.observe_children().item(pos as u32).unwrap().clone().downcast::<W>().unwrap()
}

fn set_border_to_title(bx : &Box) {
    let provider = CssProvider::new();
    provider.load_from_data("* { border-bottom : 1px solid #d9dada; } ".as_bytes());
    bx.style_context().add_provider(&provider, 800);
}

