use std::boxed;
use std::cell::RefCell;

pub mod ui;

pub mod client;

pub mod server;

pub mod user;

pub type Callbacks<T> = RefCell<Vec<boxed::Box<dyn Fn(T) + 'static>>>;

pub trait React<S> {

    fn react(&self, source : &S);

}

