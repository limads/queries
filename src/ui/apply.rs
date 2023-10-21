/*Copyright (c) 2023 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.
For a copy, see http://www.gnu.org/licenses.*/

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
use crate::sql::object::*;
use sqlparser::tokenizer::{Tokenizer, Token, Word};
use sqlparser::keywords::Keyword::NoKeyword;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Kind {

    /* Scalar functions must process a JSON array per row. */
    #[serde(rename = "scalar")]
    Scalar,

    /* Aggregate functions must process a JSON array of arrays,
    one inner array per column. */
    #[serde(rename = "aggregate")]
    Aggregate,

}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum Ty {

    #[serde(rename = "real")]
    Real,

    #[serde(rename = "integer")]
    Integer,

    #[serde(rename = "bool")]
    Bool,

    #[serde(rename = "text")]
    Text,

    /* Tabular is a pseudo-type for scalar functions,
    representing a flattened JSON table (a JSON
    object with keys as column names; and columns as JSON arrays).
    Remaining arguments are ordinary scalar arguments. Queries will implicitly
    flatten a table with the given name and pass it as the first argument.
    #[serde(rename = "tabular")]*/
    #[serde(rename = "table")]
    Table

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

    pub symbol : String,

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
        args : &serde_json::Map<String, Value>
    ) -> Result<Value, std::boxed::Box<dyn std::error::Error>> {
        let mut data = tbl.as_json_values().ok_or("Table cannot be converted to JSON")?;
        data.extend(args.clone());
        let input = serde_json::to_string(&data)?;
        let bytes = self.plugin.call(&name, &input[..])?;
        Ok(serde_json::from_reader::<_, Value>(bytes)?)
    }

}

pub type Modules = Rc<RefCell<BTreeMap<String, Module>>>;

fn search_function(func : &(String, String), mods : &BTreeMap<String, Module>) -> Option<Function> {
    let m = mods.get(&func.0)?;
    m.funcs.iter().find(|f| &f.name[..] == &func.1[..] ).cloned()
}

#[derive(Clone, Debug)]
pub struct ApplyWindow {
    pub dialog : Dialog,
    modules : Modules,
    pub call_btn : Button,
    cols_model : Rc<RefCell<Option<ListStore>>>,
    tbls_model : Rc<RefCell<Option<ListStore>>>,
    objs : Rc<RefCell<Vec<DBObject>>>,
    pub call_params : Rc<RefCell<CallParams>>
}

#[derive(Clone, Debug)]
pub struct FilledArg {
    pub arg : Arg,
    pub val : ArgVal
}

impl FilledArg {
    pub fn is_empty(&self) -> bool {
        match &self.val {
            ArgVal::Text(t) => t.is_empty(),
            _ => false
        }
    }
}

#[derive(Clone, Debug)]
pub enum ArgVal {

    Integer(i64),

    Float(f64),

    Bool(bool),

    Text(String),

    // Schema, table, col
    Column(String, String, String),

    // Schema, col
    Table(String, String)

}

#[derive(Default, Debug)]
pub struct CallParams {
    pub pending : bool,
    pub args : Vec<FilledArg>,
    pub module : Option<String>,
    pub func : Option<Function>
}

impl CallParams {

    pub fn sql(&self) -> Result<String, std::boxed::Box<std::error::Error>> {
        let mut data_source = None;
        let mut cols = Vec::new();
        for arg in &self.args {
            match &arg.val {
                ArgVal::Integer(int) => {
                    cols.push(format!("{}", int));
                },
                ArgVal::Float(float) => {
                    cols.push(format!("{}", float));
                },
                ArgVal::Bool(b) => {
                    if *b {
                        cols.push("true".to_string());
                    } else {
                        cols.push("false".to_string());
                    }
                },
                ArgVal::Text(txt) => {
                    cols.push(format!("'{}'", txt));
                },
                ArgVal::Column(schema, tbl, col) => {
                    let tbl_nm = if !schema.is_empty() {
                        format!("{}.{}", schema, tbl)
                    } else {
                        tbl.clone()
                    };
                    let col_nm = format!("{}.{}", tbl_nm, col);
                    if let Some(src) = &data_source {
                        if src != &tbl_nm[..] {
                            Err("Multiple data sources informed")?;
                        }
                    } else {
                        data_source = Some(tbl_nm);
                    }
                    cols.push(col_nm);
                },
                ArgVal::Table(schema, tbl) => {
                    if data_source.is_none() {
                        let tbl_nm = if !schema.is_empty() {
                            format!("{}.{}", schema, tbl)
                        } else {
                            tbl.clone()
                        };
                        cols.push(format!("row_to_json({tbl_nm})"));
                        data_source = Some(tbl_nm);
                    } else {
                        Err("Multiple data sources informed")?;
                    }
                }
            }
        }
        let body : String = cols.join(", ");
        if let Some(src) = data_source {
            Ok(format!("SELECT {body} FROM {src};"))
        } else {
            Ok(format!("SELECT {body};"))
        }
    }

}

impl ApplyWindow {
    pub fn build(modules : Modules, call_params : Rc<RefCell<CallParams>>) -> Self {
        let dialog = Dialog::new();
        dialog.set_title(Some("Client functions"));
        let fn_entry = Entry::new();
        super::set_margins(&fn_entry, 6,6);
        fn_entry.set_placeholder_text(Some("Search"));
        fn_entry.set_hexpand(true);
        fn_entry.set_halign(Align::Fill);
        if libadwaita::StyleManager::default().is_dark() {
            fn_entry.set_primary_icon_name(Some("fn-white-symbolic"));
        } else {
            fn_entry.set_primary_icon_name(Some("fn-dark-symbolic"));
        }
        let paned = Paned::new(Orientation::Horizontal);
        paned.set_position(280);

        dialog.set_child(Some(&paned));
        dialog.set_width_request(1200);
        dialog.set_height_request(800);
        super::configure_dialog(&dialog, false);

        let arg_list = ListBox::new();
        set_border(&arg_list);
        arg_list.style_context().add_class("boxed-list");
        super::set_margins(&arg_list, 64,64);
        let args_scroll = ScrolledWindow::new();
        args_scroll.set_width_request(320);
        args_scroll.set_child(Some(&arg_list));
        let bx_middle = Box::new(Orientation::Vertical, 0);
        let tree_view = TreeView::new();
        let store = super::schema_tree::configure_tree_view(&tree_view);

        let bx_left = Box::new(Orientation::Vertical, 0);
        set_border_side(&bx_left);
        set_background(&bx_left);

        let fns_scroll = ScrolledWindow::new();
        fns_scroll.set_vexpand(true);
        fns_scroll.set_hexpand(true);
        fns_scroll.set_valign(Align::Fill);
        fns_scroll.set_halign(Align::Fill);
        fns_scroll.set_child(Some(&tree_view));

        bx_left.append(&fn_entry);
        bx_left.append(&fns_scroll);

        bx_left.set_hexpand(false);
        args_scroll.set_hexpand(true);
        args_scroll.set_vexpand(true);
        bx_middle.set_vexpand(true);

        paned.set_start_child(Some(&bx_left));
        paned.set_end_child(Some(&bx_middle));

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
            &["crate-symbolic", "fn-dark-symbolic", "crate-white", "fn-white"]
        ).unwrap();
        let (crate_icon, fn_icon) = if libadwaita::StyleManager::default().is_dark() {
            (icons.remove("crate-white"), icons.remove("fn-white"))
        } else {
            (icons.remove("crate-symbolic"), icons.remove("fn-dark-symbolic"))
        };
        for (mod_name, module) in modules.borrow().iter() {
            let new_mod_iter = store.append(None);
            store.set(&new_mod_iter, &[(0, &crate_icon), (1, &mod_name)]);
            for f in &module.funcs {
                let fn_iter = store.append(Some(&new_mod_iter));
                store.set(&fn_iter, &[(0, &fn_icon), (1, &f.name)]);
            }
        }
        tree_view.expand_all();

        let cols_model = Rc::new(RefCell::new(None));
        let tbls_model = Rc::new(RefCell::new(None));
        let objs = Rc::new(RefCell::new(Vec::new()));

        let bx_lower = Box::new(Orientation::Horizontal, 18);
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
        bx_lower.set_margin_top(18);
        bx_lower.set_margin_bottom(18);
        call_btn.set_sensitive(false);

        tree_view.selection().connect_changed({
            let call_params = call_params.clone();
            let store = store.clone();
            let modules = modules.clone();
            let cols_model = cols_model.clone();
            let tbls_model = tbls_model.clone();
            let objs = objs.clone();
            let call_btn = call_btn.clone();
            move |selection| {
                let sel = get_selected(&store, &selection);
                let modules = &modules.borrow();
                populate_args(
                    &arg_list,
                    &modules,
                    &sel,
                    call_params.clone(),
                    cols_model.clone(),
                    tbls_model.clone(),
                    &objs,
                    &call_btn
                );
                let mut call_params = call_params.borrow_mut();
                if let Some(sel) = &sel {
                    if let Some(sr) = search_function(&sel, &modules) {
                        call_params.func = Some(sr);
                        call_params.module = Some(sel.0.clone());
                    } else {
                        call_params.func = None;
                        call_params.func = None;
                    }
                } else {
                    call_params.func = None;
                    call_params.func = None;
                }
            }
        });

        cancel_btn.style_context().add_class("pill");
        call_btn.style_context().add_class("pill");
        call_btn.style_context().add_class("suggested-action");

        bx_middle.append(&args_scroll);
        bx_middle.append(&bx_lower);

        Self { dialog, modules, call_btn, cols_model, tbls_model, objs, call_params }
    }
}

fn populate_args(
    arg_list : &ListBox,
    modules : &BTreeMap<String, Module>,
    sel : &Option<(String, String)>,
    params : Rc<RefCell<CallParams>>,
    cols_model : Rc<RefCell<Option<ListStore>>>,
    tbls_model : Rc<RefCell<Option<ListStore>>>,
    objs : &Rc<RefCell<Vec<DBObject>>>,
    call_btn : &Button
) {
    clear_list(arg_list);
    call_btn.set_sensitive(false);
    if let Some(sel) = sel {
        if let Some(module) = modules.get(&sel.0) {
            let Some(func) = module.funcs.iter().find(|f| &f.name[..] == &sel.1[..] ) else { return };
            for (ix, arg) in func.args.iter().enumerate() {
                let row = ListBoxRow::new();
                row.set_selectable(false);
                row.set_activatable(false);
                let entry = Entry::new();
                params.borrow_mut().args.push(FilledArg { arg : arg.clone(), val : ArgVal::Text(String::new()) });
                match arg.ty {
                    Ty::Table => {
                        if let Some(model) = &*tbls_model.borrow() {
                            super::add_completion(&entry, model);
                        }
                    },
                    _ => {
                        if let Some(model) = &*cols_model.borrow() {
                            super::add_completion(&entry, model);
                        }
                    }
                }
                super::set_margins(&entry, 6,6);
                entry.connect_changed({
                    let name = arg.name.clone();
                    let params = params.clone();
                    let objs = objs.clone();
                    let call_btn = call_btn.clone();
                    move |entry| {
                        let mut params = params.borrow_mut();
                        if let Some(val) = arg_value(entry.text().as_ref(), params.args[ix].arg.ty, &objs) {
                            params.args[ix].val = val;
                            if params.args.iter().all(|p| !p.is_empty() ) {
                                call_btn.set_sensitive(true);
                            } else {
                                call_btn.set_sensitive(false);
                            }
                        } else {
                            params.args[ix].val = ArgVal::Text(String::new());
                            call_btn.set_sensitive(false);
                        }
                    }
                });
                entry.set_hexpand(true);
                let icon = match arg.ty {
                    Ty::Text => "type-text-symbolic",
                    Ty::Integer => "type-integer-symbolic",
                    Ty::Real => "type-real-symbolic",
                    Ty::Table => "table-symbolic",
                    Ty::Bool => "type-boolean-symbolic",
                };
                entry.set_primary_icon_name(Some(icon));
                let doc_lbl = Label::new(arg.doc.as_ref().map(|s| &s[..] ));
                super::set_margins(&doc_lbl, 6,6);
                doc_lbl.set_halign(Align::Start);
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
            arg_list.append(&row);
            let doc_bx = Box::new(Orientation::Vertical, 0);

            row.set_child(Some(&doc_bx));

            let title_lbl = Label::new(Some(&format!("<b>{}.{}</b>", sel.0, sel.1)));
            title_lbl.set_halign(Align::Start);
            title_lbl.set_use_markup(true);
            let doc_lbl = Label::new(func.doc.as_ref().map(|s| &s[..] ));
            super::set_margins(&title_lbl, 6, 6);
            super::set_margins(&doc_lbl, 6, 6);
            doc_lbl.set_halign(Align::Start);
            doc_bx.append(&title_lbl);
            doc_bx.append(&doc_lbl);
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

fn set_background<W : WidgetExt>(lst : &W) {
    let provider = CssProvider::new();
    let css = if libadwaita::StyleManager::default().is_dark() {
        "* { background-color : #1E1E1E; } "
    } else {
        "* { background-color : #FFFFFF; } "
    };
    provider.load_from_data(css);
    lst.style_context().add_provider(&provider, 800);
}

fn set_border<W : WidgetExt>(lst : &W) {
    let provider = CssProvider::new();
    let css = if libadwaita::StyleManager::default().is_dark() {
        "* { border : 1px solid #454545; } "
    } else {
        "* { border : 1px solid #d9dada; } "
    };
    provider.load_from_data(css);
    lst.style_context().add_provider(&provider, 800);
}

fn set_border_side<W : WidgetExt>(lst : &W) {
    let provider = CssProvider::new();
    let css = if libadwaita::StyleManager::default().is_dark() {
        "* { border-right : 1px solid #454545; } "
    } else {
        "* { border-right : 1px solid #d9dada; } "
    };
    provider.load_from_data(css);
    lst.style_context().add_provider(&provider, 800);
}

impl React<ActiveConnection> for ApplyWindow {

    fn react(&self, conn : &ActiveConnection) {
        conn.connect_db_connected({
            let objs = self.objs.clone();
            let cols_model = self.cols_model.clone();
            let tbls_model = self.tbls_model.clone();
            move |(_, info)| {
                if let Some(info) = info {
                    super::update_completion_with_columns(objs.clone(), cols_model.clone(), Some(info.schema.clone()));
                    super::update_completion_with_tables(objs.clone(), tbls_model.clone(), Some(info.schema));
                }
            }
        });
        conn.connect_schema_update({
            let objs = self.objs.clone();
            let cols_model = self.cols_model.clone();
            let tbls_model = self.tbls_model.clone();
            move |schema| {
                super::update_completion_with_columns(objs.clone(), cols_model.clone(), schema.clone());
                super::update_completion_with_tables(objs.clone(), tbls_model.clone(), schema);
            }
        });
    }

}

#[derive(Debug, Clone)]
pub struct Identifier {
    pub schema : Option<String>,
    pub table : Option<String>,
    pub column : Option<String>
}

impl Identifier {

    pub fn try_parse(v : &str, is_col : bool) -> Option<Self> {
        let mut tkn = Tokenizer::new(&sqlparser::dialect::PostgreSqlDialect{}, v);
        let tkns = tkn.tokenize().ok()?;
        match (tkns.get(0), tkns.get(1), tkns.get(2), tkns.get(3), tkns.get(4)) {
            (
                Some(Token::Word(Word { value : schema, keyword : NoKeyword, .. })),
                Some(Token::Period),
                Some(Token::Word(Word { value : tbl, keyword : NoKeyword, .. })),
                Some(Token::Period),
                Some(Token::Word(Word { value : col, keyword : NoKeyword, .. }))
            ) => {
                Some(Self { schema : Some(schema.to_string()), table : Some(tbl.to_string()), column : Some(col.to_string()) })
            },
            (
                Some(Token::Word(Word { value : fst, keyword : NoKeyword, .. })),
                Some(Token::Period),
                Some(Token::Word(Word { value : snd, keyword : NoKeyword, .. })),
                None,
                None
            ) => {
                if is_col {
                    Some(Self { schema : None, table : Some(fst.to_string()), column : Some(snd.to_string()) })
                } else {
                    Some(Self { schema : Some(fst.to_string()), table : Some(snd.to_string()), column : None })
                }
            },
            (Some(Token::Word(Word { value : fst, keyword : NoKeyword, .. })), _, _, _, _) => {
                if is_col {
                    None
                } else {
                    Some(Self { schema : None, table : Some(fst.to_string()), column : None })
                }
            },
            _ => None
        }
    }

}

fn search_col_at_obj(tbl : &DBObject, id : &Identifier) -> Option<ArgVal> {
    match search_table_at_obj(tbl, id)? {
        DBObject::Table { schema, name, cols, .. } | DBObject::View { schema, name, cols, .. } => {
            let col_name = &id.column.as_ref()?;
            for c in &cols {
                if &c.name[..] == &col_name[..] {
                    return Some(ArgVal::Column(schema.clone(), name.clone(), col_name.to_string()));
                }
            }
            None
        },
        _ => {
            None
        }
    }
}

fn search_table_at_obj(obj : &DBObject, id : &Identifier) -> Option<DBObject> {
    let tbl_name = &id.table.as_ref()?;
    match obj {
        DBObject::Table { schema, name, .. } | DBObject::View { schema, name, .. } => {
            if &tbl_name[..] == &name[..] {
                if (schema.is_empty() || &schema[..] == "public") && id.schema.is_none() {
                    Some(obj.clone())
                } else if let Some(schema_name) = &id.schema {
                    if &schema_name[..] == &schema[..] {
                        Some(obj.clone())
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        },
        DBObject::Schema { children, .. } => {
            for child in children.iter() {
                if let Some(val) = search_table_at_obj(child, id) {
                    return Some(val);
                }
            }
            None
        },
        _ => None
    }
}

fn arg_value(v : &str, ty : Ty, objs : &Rc<RefCell<Vec<DBObject>>>) -> Option<ArgVal> {
    // Check if v is accessible on the database namespace. If not,
    // consider it a text argument or other literal argument.
    let objs = objs.borrow();
    match ty {
        Ty::Table => {
            if let Some(id) = Identifier::try_parse(v, false) {
                for obj in &*objs {
                    if let Some(obj) = search_table_at_obj(&obj, &id) {
                        match obj {
                            DBObject::Table { schema, name, .. } | DBObject::View { schema, name, .. } => {
                                return Some(ArgVal::Table(schema.clone(), name.clone()));
                            },
                            _ => { }
                        }
                    }
                }
                None
            } else {
                None
            }
        },
        other_ty => {
            if let Some(id) = Identifier::try_parse(v, true) {
                for obj in &*objs {
                    if let Some(val) = search_col_at_obj(&obj, &id) {
                        return Some(val);
                    }
                }
                None
            } else {
                match other_ty {
                    Ty::Real  => {
                        if let Ok(f) = v.trim().parse::<f64>() {
                            Some(ArgVal::Float(f))
                        } else {
                            None
                        }
                    },
                    Ty::Integer  => {
                        if let Ok(i) = v.trim().parse::<i64>() {
                            Some(ArgVal::Integer(i))
                        } else {
                            None
                        }
                    },
                    Ty::Bool => if v.trim() == "true" {
                        Some(ArgVal::Bool(true))
                    } else if v.trim() == "false" {
                        Some(ArgVal::Bool(false))
                    } else {
                        None
                    },
                    _ => None
                }
            }
        }
    }
}

