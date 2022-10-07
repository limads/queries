use filecase::*;
use queries::client::*;
use std::path::Path;

mod common;

// cargo test -- --nocapture
#[test]
pub fn files() {
    gtk4::init();
    let scripts = OpenedScripts::new();
    scripts.connect_new(|_f : OpenedFile| {
        println!("Created");
    });
    scripts.connect_added(|_f : OpenedFile| {
        println!("Added");
    });
    scripts.connect_selected(|_f : Option<OpenedFile> | {
        println!("Selected");
    });
    scripts.connect_opened(|f : OpenedFile| {
        println!("Opened");
        assert!(Path::new(&f.path.unwrap()).exists());
    });
    scripts.connect_closed(|_f : (OpenedFile, usize)|{
        println!("Closed");
    });
    scripts.connect_close_confirm(|_f : OpenedFile| {
        println!("Close confirm");
    });
    scripts.connect_file_changed(|_f : OpenedFile| {
        println!("File changed");
    });
    scripts.connect_file_persisted(|f : OpenedFile| {
        println!("File persisted");
        assert!(Path::new(&f.path.unwrap()).exists());
    });
    scripts.connect_error(|e : String| {
        panic!("{}", e);
    });
    scripts.connect_on_active_text_changed(|_f : Option<String>| {
        println!("Active text changed");
    });
    scripts.connect_window_close(|_f : ()| {
        println!("Window closed");
    });
    scripts.connect_save_unknown_path(|_f : String| {
        println!("Save to unknown path");
    });
    scripts.connect_buffer_read_request(|_f : usize| {
        String::new()
    });
    scripts.connect_name_changed(|_f : (usize, String)| {
        println!("Name changed");
    });
    let tempfile = "/tmp/file";
    
    scripts.send(MultiArchiverAction::NewRequest);
    scripts.send(MultiArchiverAction::Select(Some(0)));
    scripts.send(MultiArchiverAction::SaveRequest(Some(format!("{}", tempfile))));
    
    // Call those only after file is saved.
    // scripts.send(MultiArchiverAction::CloseRequest(0, true));
    // scripts.send(MultiArchiverAction::OpenRequest(format!("{}", tempfile)));
    
    common::run_loop_for_ms(200);
    println!("{:?}", scripts.final_state());
}


