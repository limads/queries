use super::{Connections, ConnectionInfo, ActiveConnection, OpenedScripts, Scripts};

pub struct UserState {
    pub main_handle_pos : i32,
    pub side_handle_pos : i32,
    pub window_width : i32,
    pub window_height : i32,
    pub scripts : Vec<String>,
    pub conns : Vec<ConnectionInfo>,
    pub path : String
}

impl UserState {

    pub fn new() -> Self {
        unimplemented!()
    }

}

// React to all common data structures, to persist state to filesystem.
// impl React<ActiveConnection> for UserState { }

