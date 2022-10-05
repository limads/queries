/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use std::thread::{self, JoinHandle};
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{ Sender, channel};
use std::io::{self};
use std::time::Duration;
use std::rc::Rc;

/// StdinListener accumulates text input every second.
/// First field is synchronized String to which STDIN writes to on IO thread
/// and main thread reads from; second field is bool channel via which
/// main thread signals that IO thread should exit (after window is closed).
/// third field used to check status and keep main thread alive while
/// listener thread did not finished.
#[derive(Clone, Debug)]
pub struct StdinListener {
    content : Arc<Mutex<Vec<String>>>,
    pub closed_sender : Sender<bool>,
    t_handle : Rc<JoinHandle<()>>
}

impl StdinListener {

    // Returns the size (in characters) of the last block read. Stream is interrupted
    // if buffer is too big.
    fn read_text_block(block_buffer : &mut String) -> usize {
        let max_block_size = 10000;
        let mut n_read = 0;
        let mut line_buffer = String::new();
        while n_read < max_block_size {
            if let Ok(n_bytes) = io::stdin().read_line(&mut line_buffer) {
                if n_bytes == 0 {
                    return 0;
                }
                match &line_buffer[..] {
                    "\n" => {
                        return n_read;
                    },
                    _ => {
                        *block_buffer += &line_buffer[..];
                        n_read += line_buffer.len();
                    }
                }
                line_buffer.clear();
            } else {
                println!("Could not read stdin line");
            }
        }
        println!("Reached buffer limit");
        n_read
    }

    pub fn new() -> Self {
        let (closed_send, closed_recv) = channel();
        let stdin_data = Arc::new(Mutex::new(Vec::new()));
        let stdin_data_c = stdin_data.clone();
        let pipe_thread = thread::spawn(move || {
            let mut block_buffer = String::new();
            loop {
                if Self::read_text_block(&mut block_buffer) > 0 {
                    if let Ok(mut data) = stdin_data_c.lock() {
                        data.push(block_buffer.clone());
                        println!("{:?}", data);
                    } else {
                        println!("Error: Could not acquire lock over data vector");
                    }
                }
                if let Ok(closed) = closed_recv.try_recv() {
                    if closed {
                        return;
                    }
                }
                block_buffer.clear();
                thread::sleep(Duration::from_millis(1000));
            }
        });
        Self{
            content : stdin_data,
            closed_sender : closed_send,
            t_handle : Rc::new(pipe_thread)
        }
    }

    pub fn get_full_content(&self) -> Vec<String> {
        if let Ok(content) = self.content.lock() {
            content.clone()
        } else {
            Vec::new()
        }
    }

    pub fn get_last_content(&self) -> Option<String> {
        if let Ok(content) = self.content.lock() {
            content.last().map(|s| s.clone())
        } else {
            None
        }
    }

    pub fn flush_content(&self) {
        if let Ok(mut content) = self.content.lock() {
            content.clear();
        }
    }
}

