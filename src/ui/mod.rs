use gtk4::prelude::*;
use gtk4::*;
use libadwaita;
use crate::client::ActiveConnection;
use crate::React;

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

#[derive(Debug, Clone)]
pub struct QueriesContent {
    pub stack : libadwaita::ViewStack,
    pub switcher : libadwaita::ViewSwitcher,
    pub overlay : libadwaita::ToastOverlay,
    pub overview : QueriesOverview,
    pub workspace : QueriesWorkspace,
    pub editor : QueriesEditor
}

impl QueriesContent {

    fn build() -> Self {
        let stack = libadwaita::ViewStack::new();
        let overview = QueriesOverview::build();
        let workspace = QueriesWorkspace::build();
        let editor = QueriesEditor::build();

        stack.add_named(&overview.bx, Some("overview")).unwrap().set_icon_name(Some("queries-symbolic"));

        // stack.add_named(&workspace.nb, Some("workspace"));

        stack.add_named(&editor.stack, Some("editor")).unwrap().set_icon_name(Some("accessories-text-editor-symbolic"));
        let switcher = libadwaita::ViewSwitcher::builder().stack(&stack).can_focus(false).policy(libadwaita::ViewSwitcherPolicy::Wide).build();
        let overlay = libadwaita::ToastOverlay::builder().margin_bottom(10).opacity(1.0).visible(true).build();
        overlay.set_child(Some(&stack));

        // stack.set_visible_child_name("overview");

        Self { stack, overview, workspace, editor, switcher, overlay }
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
        let overlay = self.overlay.clone();
        conn.connect_db_error(move |err : String| {
            overlay.add_toast(&libadwaita::Toast::builder().title(&err).build());
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
        window.add_action(&titlebar.main_menu.action_new);
        window.add_action(&titlebar.main_menu.action_open);
        window.add_action(&sidebar.file_list.close_action);

        content.editor.open_dialog.react(&titlebar.main_menu);

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

