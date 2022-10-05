/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use gtk4::*;
use crate::ui::{SchemaTree, FileList};

#[derive(Debug, Clone)]
pub struct QueriesSidebar {
    pub paned : Paned,
    pub schema_tree : SchemaTree,
    pub file_list : FileList
}

impl QueriesSidebar {

    pub fn build() -> Self {
        let file_list = FileList::build();
        let schema_tree = SchemaTree::build();
        let paned = Paned::new(Orientation::Vertical);
        paned.set_start_child(Some(&file_list.bx));
        paned.set_end_child(Some(&schema_tree.bx));
        Self { paned, schema_tree, file_list }
    }
}

