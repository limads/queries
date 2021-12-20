use gtk4::*;

#[derive(Debug, Clone)]
pub struct QueriesSidebar {
    pub paned : Paned
}

impl QueriesSidebar {

    pub fn build() -> Self {
        let file_list = ListBox::new();
        let schema_tree = TreeView::new();
        let paned = Paned::new(Orientation::Vertical);
        paned.set_start_child(&file_list);
        paned.set_end_child(&schema_tree);
        Self { paned }
    }
}

