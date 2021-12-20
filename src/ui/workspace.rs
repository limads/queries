use gtk4::prelude::*;
use gtk4::*;

#[derive(Debug, Clone)]
pub struct QueriesWorkspace {
    pub nb : Notebook
}

impl QueriesWorkspace {

    pub fn build() -> Self {
        Self { nb : Notebook::new() }
    }

}

