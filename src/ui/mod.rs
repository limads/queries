use gtk4::prelude::*;
use gtk4::*;
use libadwaita;

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

#[derive(Debug, Clone)]
pub struct QueriesContent {
    stack : Stack,
    pub overview : QueriesOverview,
    workspace : QueriesWorkspace,
    editor : QueriesEditor
}

impl QueriesContent {

    fn build() -> Self {
        let stack = Stack::new();
        let overview = QueriesOverview::build();
        let workspace = QueriesWorkspace::build();
        let editor = QueriesEditor::build();
        stack.add_named(&overview.bx, Some("overview"));
        stack.add_named(&workspace.nb, Some("workspace"));
        stack.add_named(&editor.views[0], Some("editor"));
        stack.set_visible_child_name("overview");
        Self { stack, overview, workspace, editor }
    }

    fn react(&self, titlebar : &QueriesTitlebar) {
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
    }

}

#[derive(Debug, Clone)]
pub struct QueriesWindow {
    pub window : ApplicationWindow,
    pub paned : Paned,
    pub titlebar : QueriesTitlebar,
    pub sidebar : QueriesSidebar,
    pub content : QueriesContent,
    pub overlay : libadwaita::ToastOverlay
}

impl QueriesWindow {

    pub fn from(window : ApplicationWindow) -> Self {
        let sidebar = QueriesSidebar::build();
        let titlebar = QueriesTitlebar::build();
        let content = QueriesContent::build();
        content.react(&titlebar);
        let paned = Paned::new(Orientation::Horizontal);
        paned.set_position(200);
        paned.set_start_child(&sidebar.paned);

        let toast = libadwaita::Toast::builder().title("This is a toast").build();
        toast.set_timeout(2);
        toast.set_title("This is the toast title");
        let overlay = libadwaita::ToastOverlay::builder().margin_bottom(10).opacity(1.0).visible(true).build();

        // let provider = CssProvider::new();
        // provider.load_from_data("* { background-color : #000000; } ".as_bytes());
        // overlay.style_context().add_provider(&provider, 800);

        overlay.set_child(Some(&content.stack));
        overlay.add_toast(&toast);

        // paned.set_end_child(&content.stack);
        paned.set_end_child(&overlay);

        window.set_child(Some(&paned));
        window.set_titlebar(Some(&titlebar.header));
        window.set_decorated(true);

        Self { paned, sidebar, titlebar, content, window, overlay }
    }
}

#[derive(Debug, Clone)]
pub struct PackedImageLabel  {
    pub bx : Box,
    img : Image,
    lbl : Label
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
    entry : Entry
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

pub fn set_margins<W : WidgetExt>(w : &W, horizontal : i32, vertical : i32) {
    w.set_margin_start(horizontal);
    w.set_margin_end(horizontal);
    w.set_margin_top(vertical);
    w.set_margin_bottom(vertical);
}


