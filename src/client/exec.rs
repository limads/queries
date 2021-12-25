#[derive(Debug, Clone)]
pub enum QuerySchedule {
    Off,
    Interval { interval : usize, passed : usize },
    Notification { channel : String, filter : String, selection : Vec<i32> }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionState {
    Idle,
    Evaluating
}

