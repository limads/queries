/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

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

