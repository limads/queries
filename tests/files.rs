use archiver::*;
use queries::client::*;


mod common;

// cargo test -- --nocapture
#[test]
pub fn files() {
    gtk4::init();
    let scripts = OpenedScripts::new();
    scripts.connect_new(|_f : OpenedFile| {
        // println!("New {f:?}");
    });
    scripts.connect_added(|_f : OpenedFile| {
        // println!("Added {f:?}");
    });
    scripts.connect_selected(|_f : Option<OpenedFile> | {

    });
    scripts.connect_opened(|_f : OpenedFile| {

    });
    scripts.connect_closed(|_f : (OpenedFile, usize)|{

    });
    scripts.connect_close_confirm(|_f : OpenedFile| {

    });
    scripts.connect_file_changed(|_f : OpenedFile| {

    });
    scripts.connect_file_persisted(|_f : OpenedFile| {

    });
    scripts.connect_error(|_e : String| {

    });
    scripts.connect_on_active_text_changed(|_f : Option<String>| {

    });
    scripts.connect_window_close(|_f : ()| {

    });
    scripts.connect_save_unknown_path(|_f : String| {

    });
    scripts.connect_buffer_read_request(|_f : usize| {
        String::new()
    });
    scripts.connect_name_changed(|_f : (usize, String)| {

    });
    scripts.send(MultiArchiverAction::NewRequest);
    scripts.send(MultiArchiverAction::WindowCloseRequest);
    common::run_loop_for_ms(16);
    println!("{:?}", scripts.final_state());
}
