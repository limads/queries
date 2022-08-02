use std::path::PathBuf;
use std::fs::File;
use std::io::Read;
use super::stdin::StdinListener;

/// Environment source acts like a proxy for either the filesystem or a remote
/// database connection, encapsulating the details of establishing connections to this
/// source so the TableEnvironment does not have to worry about it. If the source is a file,
/// the data structure holds the file's path and contents since the last update;
/// If it is stdin, it holds the plain content; if it is a Sqlite3 connection,
/// it holds its path and the query that generated the tables; if it is a PostgreSql
/// connection, it holds the connection String and the
/// enum holds the last state of the file, or the Sqlite3/PostgreSQL connection.
#[derive(Clone, Debug)]
pub enum EnvironmentSource {

    // Conn string + Query + Connection
    PostgreSQL((String,String)),

    // Path + SQL query
    SQLite3((Option<PathBuf>, String)),

    // Plain stdin input
    Stream(StdinListener),

    // Path + Content
    File(String, String),

    Undefined,

    #[cfg(feature="arrowext")]
    Arrow(String)
}

impl EnvironmentSource {

    // pub fn new_from_conn_string() Just copy from TableEnvironment

    pub fn new_undefined() -> Self {
        EnvironmentSource::Undefined
    }

    pub fn new_from_stdin() -> Self {
        EnvironmentSource::Stream(StdinListener::new())
    }

    pub fn new_from_file(
        path : PathBuf,
        query : Option<String>
    ) -> Result<Self, String> {
        let mut f = File::open(&path).map_err(|e| e.to_string())?;
        let ext = path.extension().ok_or("No extension".to_string())?
            .to_str().ok_or("Invalid extension encoding".to_string())?;
        match ext {
            "csv" => {
                let mut content = String::new();
                f.read_to_string(&mut content)
                    .map_err(|e| { e.to_string() })?;
                if content.len() > 0 {
                    //println!("At new from file: {}", content);
                    let p = path.to_str().ok_or("Invalid path".to_string())?;
                    let env = EnvironmentSource::File(
                        p.to_owned(),
                        content
                    );
                    //env.upda
                    Ok(env)
                } else {
                    Err("Empty CSV file".into())
                }
            },
            "db" => {
                //println!("New sqlite3");
                Ok(EnvironmentSource::SQLite3((
                    Some(path),
                    query.unwrap_or("".into()))
                ))
            },
            _ => { return Err("Invalid file extension".into()); }
        }
    }

    pub fn close_source(&mut self) {
        match self {
            EnvironmentSource::Stream(listener) => {
                listener.closed_sender.send(true).unwrap();
            },
            _ => { }
        }
    }

    // pub fn new_from_remote_db(conn : String) -> Result<Self, String> {
    // }
}

/*impl Drop for EnvironmentSource {

    fn drop(&mut self) {
        self.close_source();
    }

}*/
