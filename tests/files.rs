use archiver::*;
use queries4::client::*;
use std::sync::Arc;
use std::process::{Command, Stdio};
mod common;

// cargo test -- --nocapture
#[test]
pub fn files() {
    gtk4::init();
    let scripts = OpenedScripts::new();
    scripts.connect_new(|f : OpenedFile| {
        // println!("New {f:?}");
    });
    scripts.connect_added(|f : OpenedFile| {
        // println!("Added {f:?}");
    });
    scripts.connect_selected(|f : Option<OpenedFile> | {

    });
    scripts.connect_opened(|f : OpenedFile| {

    });
    scripts.connect_closed(|f : (OpenedFile, usize)|{

    });
    scripts.connect_close_confirm(|f : OpenedFile| {

    });
    scripts.connect_file_changed(|f : OpenedFile| {

    });
    scripts.connect_file_persisted(|f : OpenedFile| {

    });
    scripts.connect_error(|e : String| {

    });
    scripts.connect_on_active_text_changed(|f : Option<String>| {

    });
    scripts.connect_window_close(|f : ()| {

    });
    scripts.connect_save_unknown_path(|f : String| {

    });
    scripts.connect_buffer_read_request(|f : usize| {
        String::new()
    });
    scripts.connect_name_changed(|f : (usize, String)| {

    });
    scripts.send(MultiArchiverAction::NewRequest);
    scripts.send(MultiArchiverAction::WindowCloseRequest);
    common::run_loop_for_ms(16);
    println!("{:?}", scripts.final_state());
}
