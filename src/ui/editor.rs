use gtk4::prelude::*;
use gtk4::*;
use sourceview5;

#[derive(Debug, Clone)]
pub struct QueriesEditor {
    pub views : [sourceview5::View; 16]
}

impl QueriesEditor {

    pub fn build() -> Self {
        Self { views : Default::default() }
    }

}

