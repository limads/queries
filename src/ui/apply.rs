use gtk4::prelude::*;
use gtk4::*;
use libadwaita;
use crate::client::ActiveConnection;
use stateful::React;
use crate::client::Environment;
use crate::sql::StatementOutput;
use crate::client::OpenedScripts;
use crate::sql::object::{DBType, DBColumn};
use core::cell::RefCell;
use std::rc::Rc;
use filecase::MultiArchiverImpl;
use crate::client::SharedUserState;
use crate::client::Engine;
use serde::{Serialize, Deserialize};
use extism::{Plugin, Context};
use extism::{Val, ValType, CurrentPlugin, UserData, Error};
use serde_json::Value;
use crate::tables::table::Table;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::collections::BTreeMap;
use once_cell::sync::OnceCell;
use std::sync::Mutex;
use gdk_pixbuf::Pixbuf;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Kind {

    /* Scalar functions must process a JSON array per row. */
    #[serde(rename = "scalar")]
    Scalar,

    /* Aggregate functions must process a JSON array of arrays,
    one inner array per column. */
    #[serde(rename = "aggregate")]
    Aggregate,

    /* Tabular functions are a special kind of scalar function,
    for which the first argument is a flat JSON table (a JSON
    object with keys as column names; and columns as JSON arrays).
    Remaining arguments are ordinary scalar arguments. They are
    represented in the GUI as having a first argument that is
    a table name, rather than a column name. Queries will implicitly
    flatten this table and pass it as the first argument. In Sqlite,
    a JSON object with same-sized arrays as values in the
    first argument is expected, usually resulting from a json aggregate
    function call. */
    #[serde(rename = "tabular")]
    Tabular
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Ty {

    #[serde(rename = "real")]
    Real { min : Option<f32>, max : Option<f32> },

    #[serde(rename = "integer")]
    Integer { min : Option<f32>, max : Option<f32> },

    #[serde(rename = "text")]
    Text,

}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Arg {

    #[serde(rename = "type")]
    pub ty : Ty,

    pub name : String,

    pub doc : Option<String>
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Function {

    pub name : String,

    pub doc : Option<String>,

    pub args : Vec<Arg>,

    /* The difference when the function isn't an aggregate is that
    the JSON output of the plugin function will be validated to contain
    the same number of rows as the input */
    pub aggregate : bool

}

/*
Function:
kind : aggregate, ordinary
columns: real, text, integer
Works in SQL scripts loaded at sqlite
Works via GUI for tables loaded in the environment.
*/

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ModuleDef {
    pub module : String,
    pub functions : Vec<Function>
}

pub struct Module {
    pub plugin : Plugin<'static>,
    pub funcs : Vec<Function>
}

impl std::fmt::Debug for Module {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{:?}", self.funcs)
    }
}

impl Module {

    pub fn call(
        &mut self,
        name : &str,
        tbl : &Table,
        args : &serde_json::Map<String,Value>
    ) -> Result<Value, std::boxed::Box<dyn std::error::Error>> {
        let mut data = tbl.as_json_values().ok_or("Table cannot be converted to JSON")?;
        data.extend(args.clone());
        let input = serde_json::to_string(&data)?;
        let bytes = self.plugin.call(&name, &input[..])?;
        Ok(serde_json::from_reader::<_, Value>(bytes)?)
    }

}

pub type Modules = Rc<RefCell<BTreeMap<String, Module>>>;

#[derive(Clone, Debug)]
pub struct ApplyWindow {
    pub dialog : Dialog,
    modules : Modules,
    call_btn : Button,
}

#[derive(Default)]
pub struct CallParams {
    tbl : Option<Table>,
    args : serde_json::Map<String, Value>,
    func : Option<(String,String)>
}

impl ApplyWindow {
    pub fn build(modules : Modules) -> Self {
        let dialog = Dialog::new();
        dialog.set_title(Some("Client functions"));
        let bx_upper = Box::new(Orientation::Horizontal, 0);
        super::set_margins(&bx_upper, 32, 6);
        let fn_icon = Image::from_icon_name("fn-dark-symbolic");
        fn_icon.set_margin_end(6);
        bx_upper.append(&fn_icon);
        let fn_entry = Entry::new();
        fn_entry.set_placeholder_text(Some("Functions"));
        bx_upper.append(&fn_entry);
        fn_entry.set_hexpand(true);
        fn_entry.set_halign(Align::Fill);

        let bx = Box::new(Orientation::Vertical, 0);
        bx.append(&bx_upper);

        dialog.set_child(Some(&bx));
        dialog.set_width_request(1200);
        dialog.set_height_request(800);
        super::configure_dialog(&dialog, false);

        let arg_list = ListBox::new();
        arg_list.style_context().add_class("boxed-list");
        let args_scroll = ScrolledWindow::new();
        args_scroll.set_width_request(320);
        args_scroll.set_child(Some(&arg_list));
        let bx_middle = Box::new(Orientation::Horizontal, 0);
        let tree_view = TreeView::new();
        let store = super::schema_tree::configure_tree_view(&tree_view);
        let fns_scroll = ScrolledWindow::new();
        fns_scroll.set_vexpand(true);
        fns_scroll.set_hexpand(true);
        fns_scroll.set_valign(Align::Fill);
        fns_scroll.set_halign(Align::Fill);
        fns_scroll.set_child(Some(&tree_view));
        bx_middle.append(&fns_scroll);
        bx_middle.append(&args_scroll);
        bx.append(&bx_middle);
        tree_view.set_enable_search(true);
        tree_view.set_search_entry(Some(&fn_entry));
        tree_view.set_search_column(1);
        tree_view.set_search_equal_func({
            move |model, i, s, iter| {
                if let Ok(sel) = model.get_value(iter, 1).get::<String>() {
                    !sel.contains(s)
                } else {
                    true
                }
            }
        });

        let mut icons = filecase::load_icons_as_pixbufs_from_resource(
            "/io/github/limads/queries",
            &["crate-symbolic", "fn-dark-symbolic"]
        ).unwrap();
        let crate_icon = icons.remove("crate-symbolic");
        let fn_icon = icons.remove("fn-dark-symbolic");
        for (mod_name, module) in modules.borrow().iter() {
            let new_mod_iter = store.append(None);
            store.set(&new_mod_iter, &[(0, &crate_icon), (1, &mod_name)]);
            for f in &module.funcs {
                let fn_iter = store.append(Some(&new_mod_iter));
                store.set(&fn_iter, &[(0, &fn_icon), (1, &f.name)]);
            }
        }
        tree_view.expand_all();

        let call_params = Rc::new(RefCell::new(CallParams::default()));
        tree_view.selection().connect_changed({
            let call_params = call_params.clone();
            let store = store.clone();
            let modules = modules.clone();
            move |selection| {
                let sel = get_selected(&store, &selection);
                populate_args(&arg_list, &modules.borrow(), &sel, call_params.clone());
                call_params.borrow_mut().func = sel;
            }
        });

        let bx_lower = Box::new(Orientation::Horizontal, 0);
        let call_btn = Button::with_label("Call");
        let cancel_btn = Button::with_label("Cancel");
        cancel_btn.connect_clicked({
            let dialog = dialog.clone();
            move |_| {
                dialog.hide();
            }
        });
        bx_lower.append(&cancel_btn);
        bx_lower.append(&call_btn);
        bx_lower.set_hexpand(true);
        bx_lower.set_halign(Align::Center);

        cancel_btn.style_context().add_class("pill");
        call_btn.style_context().add_class("pill");
        call_btn.style_context().add_class("suggested-action");

        bx.append(&bx_lower);

        call_btn.connect_clicked({
            let modules = modules.clone();
            let call_params = call_params.clone();
            move |_| {
                let mut modules = modules.borrow_mut();
                let params = call_params.borrow();
                if let Some(func) = &params.func {
                    if let Some(m) = modules.get_mut(&func.0) {
                        if let Some(tbl) = &params.tbl {
                            match m.call(&func.1, &tbl, &params.args) {
                                Ok(val) => {
                                    println!("{:?}", val);
                                },
                                Err(e) => {
                                    println!("{:?}",e);
                                }
                            }
                        } else {
                            println!("No table selected");
                        }
                    } else {
                        println!("No module named {}", func.0);
                    }
                } else {
                    println!("No func at context");
                }
            }
        });
        Self { dialog, modules, call_btn }
    }
}

fn populate_args(
    arg_list : &ListBox,
    modules : &BTreeMap<String, Module>,
    sel : &Option<(String, String)>,
    params : Rc<RefCell<CallParams>>
) {
    clear_list(arg_list);
    if let Some(sel) = sel {
        if let Some(module) = modules.get(&sel.0) {
            let Some(func) = module.funcs.iter().find(|f| &f.name[..] == &sel.1[..] ) else { return };
            for arg in &func.args {
                let row = ListBoxRow::new();
                row.set_selectable(false);
                row.set_activatable(false);
                let entry = Entry::new();
                entry.connect_changed({
                    let name = arg.name.clone();
                    let params = params.clone();
                    move |entry| {
                        let mut params = params.borrow_mut();
                        let mut e = params.args.entry(&name)
                            .or_insert(serde_json::Value::String(String::new()));
                        *e = Value::String(entry.text().to_string());
                    }
                });
                entry.set_hexpand(true);
                let icon = match arg.ty {
                    Ty::Text => "type-text-symbolic",
                    Ty::Integer { ..} => "type-integer-symbolic",
                    Ty::Real { .. } => "type-real-symbolic",
                };
                entry.set_primary_icon_name(Some(icon));
                let doc_lbl = Label::new(arg.doc.as_ref().map(|s| &s[..] ));
                let bx = Box::new(Orientation::Vertical, 0);
                bx.append(&entry);
                entry.set_placeholder_text(Some(&arg.name));
                bx.append(&doc_lbl);
                row.set_child(Some(&bx));
                arg_list.append(&row);
            }
            let row = ListBoxRow::new();
            row.set_selectable(false);
            row.set_activatable(false);
            let doc_lbl = Label::new(func.doc.as_ref().map(|s| &s[..] ));
            row.set_child(Some(&doc_lbl));
            arg_list.append(&row);
        }
    }
}

fn clear_list(list : &ListBox) {
    while let Some(r) = list.row_at_index(0) {
        list.remove(&r);
    }
}

fn get_selected(model : &TreeStore, selection : &TreeSelection) -> Option<(String, String)> {
    if selection.count_selected_rows() > 0 {
        let (paths, _) = selection.selected_rows();
        if let Some(fst_path_iter) = paths.get(0) {
            if let Some(iter) = model.iter(&fst_path_iter) {
                let mut parents = Vec::new();
                let mut path = fst_path_iter.clone();
                loop {
                    if let Some(iter) = model.iter(&path) {
                        // Take only chars before space to avoid names that contain a dimension specifier like [nxm]
                        let v : glib::Value = model.get(&iter, 1);
                        let name = v.get::<&str>().unwrap();
                        parents.push(name.split(" ").next().unwrap().to_string());
                    } else {
                        break;
                    }
                    if path.depth() == 1 {
                        break;
                    } else {
                        path.up();
                    }
                }
                parents.reverse();
                if parents.len() == 2 {
                    Some(((parents[0].clone(), parents[1].clone())))
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    }
}

impl React<Environment> for ApplyWindow {
    fn react(&self, env : &Environment) {

    }
}


