use archiver::*;
use queries4::client::*;
use std::sync::Arc;
use std::process::{Command, Stdio};

/* All of the tests that require external PostgreSQL tools should be ignored by
default and should be called explicitly by the user. */

// Runs a command, returning its stdout output (if any). Panics if command could not be called.
// Used to interact with external PostgreSQL tools for testing.
fn run(cmd : &str) -> Option<String> {
    let mut split = cmd.split(" ");
    let cmd = split.next().unwrap();
    let args = split.collect::<Vec<_>>();
    let out = Command::new(cmd).args(&args[..]).stdout(Stdio::Piped).wait_with_output().unwrap();
    if out.stdout.len() > 0 {
        Some(String::from_utf8(out.stdout.clone()).unwrap())
    } else {
        None
    }
}

/* Starts a glib main loop and run it for a given number of milliseconds. */
fn run_loop_for_ms(ms : usize) {
    let main = Arc::new(gtk4::glib::MainLoop::new(None, false));
    std::thread::spawn({
        let main = main.clone();
        move|| {
            std::thread::sleep(std::time::Duration::from_millis(16));
            main.quit();
        }
    });
    main.run();
}

// Creates a temporary database at localhost with the username and password
// set to the current unix user. The database is removed after the function is
// runned. Assumes PostgreSQL server is installed at the current database, and
// the executables createdb and dropdb are available. The closure receives the
// name of the current unix user and created database (automatically created).
// Note a random seed to name the database is required, since the function might be called from different
// threads, to avoid having duplicated database names.
fn run_with_temp_db(f : impl Fn(&str, &str)) {

    let r : u32 = rand::random();

    // Use a unique database name
    let dt = run("date +%y_%m_%d_%H_%M_%S").unwrap();
    let user = run("whoami").unwrap();
    let dbname = format!("queries_test_{}_{}", dt, r);

    // Note dropdb is never called when the name already exists at this stage -
    // no risk of erasing a database other than the one created right here.
    run(&format!("createdb {}", dbname));

    f(&user, &dbname);

    run(&format!("dropdb {}", dbname));
}

#[test]
#[ignore]
pub fn connection() {
    run_with_temp_db(|dbname, user| {
        gtk4::init();
        let conn = ActiveConnection::new(SharedUserState::new());
        let mut info = ConnectionInfo::default();
        info.user = user.to_string();
        info.database = dbname.to_string();
        conn.send(ActiveConnectionAction::ConnectRequest(info));
        run_loop_for_ms(16);
        println!("{:?}", conn.final_state());
    });
}

// cargo test -- --nocapture
#[test]
pub fn scripts() {
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
    run_loop_for_ms(16);
    println!("{:?}", scripts.final_state());
}