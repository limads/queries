/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use stateful::PersistentState;
use stateful::React;
use gtk4::prelude::*;
use gtk4::glib;
use gtk4::gio;
use std::collections::BTreeMap;
use std::path::Path;
use std::ffi::OsStr;
use std::rc::Rc;
use std::cell::RefCell;
use std::cell::Cell;
use std::sync::Mutex;

#[derive(Clone, Copy, Default)]
pub struct MyType { }

pub trait Singleton {
    fn instance() -> &'static Self;
}

static MYTYPE : std::sync::OnceLock<Mutex<MyType>> = std::sync::OnceLock::new();

impl Singleton for Mutex<MyType> {
    fn instance() -> &'static Self {
        MYTYPE.get_or_init(|| Mutex::new(MyType::default()) )
    }
}

pub trait LockedSingleton {
    type Inner;
    fn instance() -> std::sync::MutexGuard<'static, Self::Inner>;
    fn try_instance() -> Option<std::sync::MutexGuard<'static, Self::Inner>>;
}

impl<T> LockedSingleton for Mutex<T>
where
    Mutex<T> : Singleton
{
    type Inner = T;
    fn instance() -> std::sync::MutexGuard<'static, T> {
        <Mutex::<T> as Singleton>::instance().lock().unwrap()
    }
    fn try_instance() -> Option<std::sync::MutexGuard<'static, T>> {
        <Mutex::<T> as Singleton>::instance().try_lock().ok()
    }
}

/*use elsa::sync::FrozenVec;
use stable_deref_trait::StableDeref;

pub trait FrozenSingleton
where
    Self : Default + StableDeref
{

    fn history() -> &'static FrozenVec<Self>;

    fn get() -> &'static Self {
        let v = Self::history();
        if v.len() == 0 {
            v.push(Self::default());
        }
        v.get(v.len()-1).unwrap()
    }

    fn set(val : Self) {
        let v = Self::history();
        v.push(val);
    }

}

static MYTYPE_HISTORY : std::sync::OnceLock<FrozenVec<MyType>> = std::sync::OnceLock::new();

impl FrozenSingleton for MyType {
    fn history() -> &'static FrozenVec<Self> {
        MYTYPE_HISTORY.get_or_init(|| FrozenVec::new() )
    }
}*/

pub trait ShareMany {
    type Output;
    fn share_many(&self) -> Self::Output;
}

impl<T1,T2> ShareMany for (&T1, &T2)
where
    T1 : Clone,
    T2 : Clone
{
    type Output=(T1,T2);
    fn share_many(&self) -> Self::Output {
        (self.0.clone(), self.1.clone())
    }
}

impl<T1,T2,T3> ShareMany for (&T1, &T2, &T3)
where
    T1 : Clone,
    T2 : Clone,
    T3 : Clone
{
    type Output=(T1,T2,T3);
    fn share_many(&self) -> Self::Output {
        (self.0.clone(), self.1.clone(), self.2.clone())
    }
}

impl<T1,T2,T3,T4> ShareMany for (&T1, &T2, &T3, &T4)
where
    T1 : Clone,
    T2 : Clone,
    T3 : Clone,
    T4 : Clone
{
    type Output=(T1,T2,T3,T4);
    fn share_many(&self) -> Self::Output {
        (self.0.clone(), self.1.clone(), self.2.clone(), self.3.clone())
    }
}

pub trait Share
where
    Self : Sized
{
    type Inner : Sized;
    fn share(&self) -> Self;
    fn new_shared<const N : usize>(inner : Self::Inner) -> [Self; N];
}

impl<T> Share for Rc<T>
where
    T : Sized,
{
    type Inner = T;
    fn share(&self) -> Self {
        self.clone()
    }

    fn new_shared<const N : usize>(val : T) -> [Self; N] {
        let val = Rc::new(val);
        std::array::from_fn(|_| val.clone() )
    }

}

pub mod tables;

pub mod ui;

pub mod client;

pub mod server;

pub mod sql;

pub const SETTINGS_FILE : &str = "user.json";

pub const APP_ID : &str = "io.github.limads.Queries";

pub fn register_resources() {
    let bytes = glib::Bytes::from_static(include_bytes!(concat!(env!("OUT_DIR"), "/", "compiled.gresource")));
    let resource = gio::Resource::from_data(&bytes).unwrap();
    gio::resources_register(&resource);
}

fn hook_signals(
    queries_win : &ui::QueriesWindow,
    user_state : &client::SharedUserState,
    client : &client::QueriesClient
) {
    user_state.react(queries_win);

    client.conn_set.react(&queries_win.content.results.overview.conn_list);
    client.conn_set.react(&queries_win.content.results.overview.conn_bx);
    client.active_conn.react(&queries_win.content.results.overview.conn_bx);
    client.active_conn.react(&queries_win.titlebar.exec_btn);
    client.active_conn.react(&queries_win.sidebar.schema_tree);
    client.active_conn.react(&queries_win.graph_win);
    client.active_conn.react(&queries_win.builder_win);

    client.env.react(&client.active_conn);
    client.env.react(&queries_win.content.results.workspace);
    client.env.react(&queries_win.content.editor.export_dialog);
    client.env.react(&queries_win.titlebar.exec_btn);

    queries_win.content.react(&client.active_conn);
    queries_win.content.results.overview.conn_bx.react(&client.conn_set);
    queries_win.content.results.overview.conn_list.react(&client.conn_set);
    queries_win.content.results.overview.conn_list.react(&client.active_conn);
    queries_win.content.results.overview.conn_bx.react(&client.active_conn);
    queries_win.content.results.overview.sec_bx.react(&client.conn_set);
    queries_win.content.results.overview.sec_bx.react(&queries_win.settings);
    queries_win.content.results.workspace.react(&client.env);

    queries_win.sidebar.schema_tree.react(&client.active_conn);
    queries_win.sidebar.file_list.react(&client.scripts);

    client.scripts.react(queries_win);

    queries_win.content.editor.react(&client.scripts);
    queries_win.content.editor.save_dialog.react(&client.scripts);
    queries_win.content.editor.save_dialog.react(&queries_win.titlebar.main_menu);

    queries_win.content.react(&client.scripts);
    queries_win.titlebar.exec_btn.react(&client.scripts);
    queries_win.titlebar.exec_btn.react(&client.active_conn);
    queries_win.titlebar.exec_btn.react(&queries_win.content);
    queries_win.titlebar.main_menu.react(&client.scripts);
    queries_win.titlebar.main_menu.react(&client.active_conn);

    queries_win.content.react(&client.env);
    queries_win.sidebar.file_list.react(&client.active_conn);

    queries_win.content.results.overview.detail_bx.react(&client.active_conn);

    queries_win.react(&queries_win.titlebar);
    queries_win.react(&client.scripts);
    queries_win.find_dialog.react(&queries_win.titlebar.main_menu);
    queries_win.find_dialog.react(&queries_win.content.editor);
    queries_win.find_dialog.react(&client.scripts);
    queries_win.find_dialog.react(&client.scripts);

    queries_win.model.react(&client.active_conn);
    queries_win.apply.react(&client.env);

    queries_win.window.add_action(&queries_win.find_dialog.find_action);
    queries_win.window.add_action(&queries_win.find_dialog.replace_action);
    queries_win.window.add_action(&queries_win.find_dialog.replace_all_action);

    queries_win.content.editor.react(&queries_win.settings);

    queries_win.graph_win.react(&client.active_conn);
    queries_win.builder_win.react(&client.active_conn);

    user_state.react(&client.scripts);
}

pub fn setup(
    queries_win : &ui::QueriesWindow,
    user_state : &client::SharedUserState,
    client : &client::QueriesClient
) {
    // It is critical that updating the window here is done before setting the react signal
    // below while the widgets are inert and thus do not change the state recursively.
    user_state.update(queries_win);

    // Now the window has been updated by the state, it is safe to add the callback signals.
    hook_signals(queries_win, user_state, client);

    // It is important to make this call to add scripts and connections
    // only after all signals have been setup, to guarantee the GUI will update
    // when the client updates.
    client::set_client_state(user_state, client);

    queries_win.content.editor.configure(&user_state.borrow().editor);
}

pub fn load_modules() -> crate::ui::apply::Modules {
    crate::CONTEXT.set(Context::new());
    if let Some(mut path) = filecase::get_datadir(crate::APP_ID) {
        path.push(Path::new("modules"));
        if !path.exists() {
            if let Err(e) = std::fs::create_dir(&path) {
                println!("{}", e);
            }
        }
        match load_modules_at_path(&path) {
            Ok(modules) => modules,
            Err(e) => {
                println!("{:?}", e);
                Rc::new(RefCell::new(BTreeMap::new()))
            }
        }
    } else {
        println!("Could not find datadir (modules won't be loaded)");
        Rc::new(RefCell::new(BTreeMap::new()))
    }
}

use extism::{Plugin, Context};
use extism::{Val, ValType, CurrentPlugin, UserData, Error};
use ui::apply::*;
use once_cell::sync::OnceCell;

static CONTEXT : OnceCell<Context> = OnceCell::new();

fn load_module(
    modules : &mut BTreeMap<String, Module>,
    module_name : &str,
    wasm : &[u8]
) -> Result<(), std::boxed::Box<dyn std::error::Error>> {
    let mut plugin = Plugin::new(&CONTEXT.get().unwrap(), &wasm, [], false)?;
    let bytes = plugin.call(&module_name, &[])?;
    let mut module_def = serde_json::from_reader::<_, ModuleDef>(bytes)?;
    module_def.functions.sort_by(|a, b| a.name.cmp(&b.name) );
    modules.insert(module_def.module.to_string(), Module { plugin, funcs : module_def.functions });
    Ok(())
}

fn load_modules_at_path(path : &Path) -> Result<Rc<RefCell<BTreeMap<String, Module>>>, std::boxed::Box<dyn std::error::Error>> {
    let mut modules = BTreeMap::new();
    for entry in std::fs::read_dir(&path)? {
        let entry = entry?;
        if entry.path().extension() == Some(OsStr::new("wasm")) {
            if let Some(name) = entry.path().file_stem().and_then(|f| f.to_str() ) {
                match std::fs::read(entry.path()) {
                    Ok(wasm) => {
                        if let Err(e) = load_module(&mut modules, &name, &wasm[..]) {
                            println!("{:?}",e);
                        }
                    },
                    Err(e) => {
                        println!("{:?}",e);
                    }
                }
            } else {
                println!("Invalid file name");
            }
        }
    }
    Ok(Rc::new(RefCell::new(modules.into())))
}

pub fn safe_to_write(path : &std::path::Path) -> Result<(), std::boxed::Box<dyn std::error::Error>> {
    if path.exists() && path.is_dir() {
        Err("Path is a directory".into())
    } else {
        Ok(())
    }
}


