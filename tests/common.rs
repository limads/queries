use queries::client::*;
use std::sync::Arc;
use std::process::{Command, Stdio};
use std::env;
use queries::server::*;
use std::error::Error;
use url::Url;
use gtk4::glib;
use queries::sql::StatementOutput;
use std::rc::Rc;
use std::cell::RefCell;

// #![allow(warnings)]

/*// Launch a test run of Queries. This differs from a regular launch
// in that no user state is read/written into disk. Takes a closure F
// that executes in a parallel thread while the GUI is open to change queries
// state, and a close G called immediately after Application::run in the
// main thread to verify the state is kept at a consistent state with the client.
pub fn launch_test_gui<F, G>(f : F, g :G)
where
    F : Fn(&queries::client::QueriesClient) + Send + Sync,
    G : Fn(&queries::client::QueriesClient, &SharedUserState)
{

    register_resources();
    if let Err(e) = gtk4::init() {
        eprintln!("{}", e);
        return;
    }
    let application = Application::builder()
        .application_id(APP_ID)
        .build();
    let user_state = Arc::new(client::SharedUserState::default());
    let client = Arc::new(client::QueriesClient::new(&user_state));
    application.connect_activate({
        let user_state = user_state.clone();
        let client = client.clone();
        move |app| {
            if let Some(display) = gdk::Display::default() {
                let theme = IconTheme::for_display(&display);
                theme.add_resource_path("/io/github/limads/queries/icons");
            } else {
                eprintln!("Unable to get default GDK display");
            }
            let queries_win = QueriesWindow::build(app, &user_state);
            queries::setup(&queries_win, &user_state, &client);
            queries_win.window.show();
        }
    });

    let sent_client = client.clone();
    thread::spawn(move || {
        f(&sent_client);
    });

    // The final states for scripts and conn_set are updated just when the window is
    // closed, which happens before application::run unblocks the main thread.
    application.run();
    g(&client, &state);
}*/

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
pub struct TempDB {
    pub user : String,
    pub db : String,
    pub uri : ConnURI,
    pub conn : PostgresConnection
}

impl TempDB {

    pub fn uri(&self) -> ConnURI {
        self.uri.clone()
    }

    pub fn new(user : &str, db : &str) -> Result<Self, Box<dyn Error>> {
        let mut info = ConnectionInfo::default();
        info.user = user.to_string();
        info.database = db.to_string();
        info.host = "localhost".to_string();
        info.port = String::from("5432");
        info.security = Security::new_insecure();
        let pwd = info.user.to_string();
        let uri = ConnURI::new(info.clone(), &pwd).unwrap();
        let conn = PostgresConnection::try_new(uri.clone())?;
        Ok(Self { user : user.to_string(), db : db.to_string(), conn, uri })
    }

}

pub struct ExistingDB {
    pub user : String,
    pub db : String,
    pub conn : PostgresConnection
}

impl ExistingDB {

    pub fn from_env() -> Result<Self, Box<dyn Error>> {
        let info = ConnectionInfo {
            engine : Engine::Postgres,
            host : env::var("HOSTNAME").or(Err("Missing hostname"))?,
            port : env::var("PORT").or(Err("Missing port"))?,
            user : env::var("USERNAME").or(Err("Missing user"))?,
            database : env::var("DBNAME").or(Err("Missing database"))?,
            security : Security {
                tls_version : Some(TlsVersion { major : 1, minor : 0 }),
                cert_path : Some(env::var("PGSSLROOTCERT").or(Err("Missing hostname"))?),
                verify_hostname : Some(true)
            }
        };
        let mut uri = ConnURI::new(info.clone(), &env::var("PGPASSWORD").or(Err("Missing password"))?)?;
        uri.uri = Url::parse(&format!("{}?application_name=Queries&sslmode=require", uri.uri)).unwrap();
        let conn = PostgresConnection::try_new(uri.clone())?;
        let edb = ExistingDB { user : info.user.clone(), db : info.database.clone(), conn };
        Ok(edb)
    }

}

// This runs the closure with an existing database, with credentials
// queries from psql's connection environment variables. Exits with
// error if the credential environment vars are not set. This establishes
// a secure connection and requires a path to a root certificate.
pub fn run_with_existing_db<F>(f : F) -> Result<(), Box<dyn Error>>
where
    F : Fn(ExistingDB)
{
    let edb = ExistingDB::from_env()?;
    f(edb);
    Ok(())
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
    
    let temp_db = TempDB::new(&user, &dbname).unwrap();
    
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

pub fn exec_next_statement(stmt_ix : &Rc<RefCell<usize>>, all_stmts : &[String], sender : &glib::Sender<ActiveConnectionAction>) {
    let mut stmt_ix = stmt_ix.borrow_mut();
    *stmt_ix += 1;
    if *stmt_ix < all_stmts.len() {
        sender.send(ActiveConnectionAction::ExecutionRequest(all_stmts[*stmt_ix].to_string())).unwrap();
    } else {
        println!("All statements executed");
    }
}

pub fn print_output(r : &StatementOutput) {
    match r {
        StatementOutput::Invalid(msg, by_engine) => {
            if *by_engine {
                println!("Statement rejected by server: {}", msg);
            } else {
                println!("Statement rejected by client: {}", msg);
            }
        },
        out => {
            println!("Statement executed: {:?}", out);
        }
    }
}


