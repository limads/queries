use queries::client::*;
use std::sync::Arc;
use std::process::{Command, Stdio};

/*
The queries test set calls the following command-line applications:

createdb
dropdb
whoami
date

Which are expected to be installed and at the $PATH. whoami and date are default
Linux utilities, while createdb and dropdb are distributed via the PostgreSQL
package for any Linux distribution.

*/

// Runs a command, returning its stdout output (if any). Panics if command could not be called.
// Used to interact with external PostgreSQL tools for testing.
pub fn run(cmd : &str) -> Option<String> {
    let mut split = cmd.split(" ");
    let cmd_name = split.next().unwrap();
    let args = split.collect::<Vec<_>>();
    let out = Command::new(cmd_name)
        .args(&args[..])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    if !out.status.success() {
        panic!("Could not execute {}: {}", cmd, String::from_utf8(out.stderr.clone()).unwrap());
    }
    if out.stdout.len() > 0 {
        Some(String::from_utf8(out.stdout.clone()).unwrap())
    } else {
        None
    }
}

/* Starts a glib main loop and run it for a given number of milliseconds.
An external thread holds a handle to the main loop to quit it after the
desired time has ellapsed. */
pub fn run_loop_for_ms(ms : usize) {
    let main = Arc::new(gtk4::glib::MainLoop::new(None, false));
    std::thread::spawn({
        let main = main.clone();
        move|| {
            std::thread::sleep(std::time::Duration::from_millis(ms as u64));
            main.quit();
        }
    });
    main.run();
}

/* Represents a temporary database created by the test set. */
#[derive(Debug, Clone)]
pub struct TempDB {
    pub user : String,
    pub db : String
}

impl TempDB {

    pub fn uri(&self) -> ConnURI {
        let mut info = ConnectionInfo::default();
        info.user = self.user.to_string();
        info.database = self.db.to_string();
        info.host = "localhost:5432".to_string();
        let pwd = info.user.to_string();
        let uri = ConnURI::new(info, &pwd).unwrap();
        println!("Using URI: {}", uri.uri);
        uri 
    }
    
}

// Creates a temporary database at localhost with the username and password
// set to the current unix user. The database should be removed after the function is
// runned. Assumes PostgreSQL server is installed at the current database, and
// the executables createdb and dropdb are available. The closure receives the
// name of the current unix user and created database (automatically created).
// Note a random seed to name the database is required, since the function might be called from different
// threads, to avoid having duplicated database names.
pub fn run_with_temp_db(f : impl FnOnce(TempDB) + std::panic::UnwindSafe) {

    // Use a unique database name from date and random seed.
    let r : u32 = rand::random();
    let dt = run("date +%y_%m_%d_%H_%M_%S").unwrap().trim().to_string();
    let user = run("whoami").unwrap().trim().to_string();
    let dbname = format!("queries_test_{}_{}", dt, r);

    // This panics when dbname already exists, avoiding the risk of manipulating 
    // an existing database
    run(&format!("createdb {}", dbname));
    
    let temp_db = TempDB { user : user.clone(),  db : dbname.clone() };
    
    f(temp_db);
    
    // dropdb could be called automatically, but perhaps it is best to let the user
    // erase them manually afterwards.
    // Defer any panics to after the temporary db is erased.
    // let res = std::panic::catch_unwind(move || {
    //    f(temp_db);
    // });
    // run(&format!("dropdb {}", dbname));
    // Now that the db is erased, propagate the panic.
    // if let Err(e) = res {
    //    std::panic::resume_unwind(e);
    // }
    
}


