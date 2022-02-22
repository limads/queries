use gtk4::*;
use gtk4::prelude::*;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use crate::sql::*;
use std::path::Path;
use monday::tables::*;
use std::sync::{Arc, Mutex};
use std::str::FromStr;
use std::cmp::{Eq, PartialEq};
use std::hash::Hash;
use std::fmt;
use itertools::Itertools;
use std::collections::HashMap;
use crate::sql::object::*;
use crate::{Callbacks, ValuedCallbacks};
use monday::tables::table::Table;
use crate::React;
use crate::client::ActiveConnection;
use std::boxed;
use monday::tables::table::TableSettings;
use monday::tables::table::Columns;
use plots::Panel;
use crate::ui::QueriesWorkspace;
use std::io::Write;
use std::thread;
use crate::ui::ExportDialog;
use crate::ui::QueriesSettings;
use monday;
use crate::client::ExecutionSettings;
use crate::client::SharedUserState;

// #[cfg(feature="arrowext")]
// use datafusion::execution::context::ExecutionContext;
// #[cfg(feature="arrowext")]
// use datafusion::execution::physical_plan::csv::CsvReadOptions;

#[derive(Debug, Clone)]
pub enum ExportItem {
    Table(Table),
    Panel(Panel)
}

pub struct ExecutionError {
    pub msg : String,
    pub is_server : bool
}

pub enum EnvironmentAction {

    Update(Vec<StatementOutput>),

    Clear,

    Select(Option<usize>),

    /// Request to export the currently selected item to the path given as the argument.
    ExportRequest(String),

    ChangeTemplate(String),

    ChangeSetting(ExecutionSettings),

    ExportError(String)

}

pub struct Environment {

    send : glib::Sender<EnvironmentAction>,

    on_tbl_update : Callbacks<Vec<Table>>,

    on_tbl_error : Callbacks<String>,

    on_export_error : Callbacks<String>

}

impl Environment {

    pub fn new() -> Self {
        let (send, recv) = glib::MainContext::channel::<EnvironmentAction>(glib::PRIORITY_DEFAULT);
        let mut tables = Tables::new();
        let mut plots = Plots::new();
        let on_tbl_update : Callbacks<Vec<Table>> = Default::default();
        // Replace by on_export_success and on_export_error. Exporting thread is spanwed and
        // result message is sent back to user.
        let on_export_error : Callbacks<String> = Default::default();
        let on_tbl_error : Callbacks<String> = Default::default();
        let mut selected : Option<usize> = None;
        let mut template_path : Option<String> = None;
        recv.attach(None, {
            let on_tbl_update = on_tbl_update.clone();
            let on_export_error = on_export_error.clone();
            let on_tbl_error = on_tbl_error.clone();
            let send = send.clone();
            move |action| {
                match action {
                    EnvironmentAction::Update(results) => {
                        let has_error = results.iter().filter(|res| {
                            match res {
                                StatementOutput::Invalid(_, _) => true,
                                _ => false
                            }
                        }).next().is_some();
                        if !has_error {
                            tables.update_from_query_results(results);
                            match plots.update_from_tables(&tables.tables[..]) {
                                Ok(_) => {
                                    if tables.tables.len() >= 1 {
                                        on_tbl_update.borrow().iter().for_each(|f| f(tables.tables.clone()) );
                                    }
                                },
                                Err(e) => {
                                    on_tbl_error.borrow().iter().for_each(|f| f(e.clone()) );
                                }
                            }
                        }
                    },
                    EnvironmentAction::Select(opt_pos) => {
                        selected = opt_pos;
                    },
                    EnvironmentAction::ExportRequest(path) => {
                        let item = if let Some(ix) = selected {
                            if let Some(plot_ix) = plots.ixs.iter().position(|i| *i == ix ) {
                                Some(ExportItem::Panel(plots.panels[plot_ix].clone()))
                            } else {
                                Some(ExportItem::Table(tables.tables[ix].clone()))
                            }
                        } else {
                            None
                        };
                        if let Some(item) = item {
                            thread::spawn({
                                let send = send.clone();
                                let template_path = template_path.clone();
                                move || {
                                    if let Err(e) = export_to_path(item, Path::new(&path[..]), template_path) {
                                        send.send(EnvironmentAction::ExportError(e));
                                    }
                                }
                            });
                        }
                    },
                    EnvironmentAction::ExportError(msg) => {
                        on_export_error.borrow().iter().for_each(|f| f(msg.clone()) );
                    },
                    EnvironmentAction::ChangeSetting(setting) => {

                    },
                    EnvironmentAction::ChangeTemplate(path) => {
                        if !path.is_empty() {
                            template_path = Some(path);
                        } else {
                            template_path = None;
                        }
                    },
                    _ => { }
                }
                Continue(true)
            }
        });
        Self { send, on_tbl_update, on_export_error, on_tbl_error }
    }

    pub fn connect_table_update<F>(&self, f : F)
    where
        F : Fn(Vec<Table>) + 'static
    {
        self.on_tbl_update.borrow_mut().push(boxed::Box::new(f));
    }

    pub fn connect_export_error<F>(&self, f : F)
    where
        F : Fn(String) + 'static
    {
        self.on_export_error.borrow_mut().push(boxed::Box::new(f));
    }

    pub fn connect_table_error<F>(&self, f : F)
    where
        F : Fn(String) + 'static
    {
        self.on_tbl_error.borrow_mut().push(boxed::Box::new(f));
    }

}

impl React<ActiveConnection> for Environment {

    fn react(&self, conn : &ActiveConnection) {
        let send = self.send.clone();
        conn.connect_exec_result(move |res : Vec<StatementOutput>| {
            send.send(EnvironmentAction::Update(res));
        });
    }

}

impl React<QueriesWorkspace> for Environment {

    fn react(&self, ws : &QueriesWorkspace) {
        let send = self.send.clone();
        ws.tab_view.connect_selected_page_notify(move|view| {
            if view.selected_page().is_some() {
                if let Some(pages) = view.pages() {
                    send.send(EnvironmentAction::Select(Some(pages.selection().nth(0) as usize)));
                }
            } else {
                send.send(EnvironmentAction::Select(None));
            }
        });
    }

}

impl React<ExportDialog> for Environment {

    fn react(&self, dialog : &ExportDialog) {
        // If table, set default to csv. If plot, set default to svg.
        /*self.connect_selected(move |path| {
            let _ = dialog.set_file(&gio::File::for_path(path));
            dialog.show();
        });*/

        let send = self.send.clone();
        dialog.dialog.connect_response(move |dialog, resp| {
            match resp {
                ResponseType::Accept => {
                    if let Some(path) = dialog.file().and_then(|f| f.path() ) {
                        send.send(EnvironmentAction::ExportRequest(path.to_str().unwrap().to_string())).unwrap();
                    }
                },
                _ => { }
            }
        });
    }

}

impl React<QueriesSettings> for Environment {

    fn react(&self, settings : &QueriesSettings) {
        settings.report_bx.entry.connect_changed({
            let send = self.send.clone();
            move |entry| {
                let txt = entry.text().as_str().to_string();
                send.send(EnvironmentAction::ChangeTemplate(txt));
            }
        });
    }

}

fn export_to_path(item : ExportItem, path : &Path, template_path : Option<String>) -> Result<(), String> {
    let mut f = File::create(path).map_err(|e| format!("{}", e) )?;
    let ext = path.extension().map(|ext| ext.to_str().unwrap_or("") );

    // Verify if table is csv/fodt/html
    // Verify if plot is svg/png
    // Verify if format agrees with export item modality.

    match item {
        ExportItem::Table(mut tbl) => {
            // let opt_fmt : Option<TableSettings> = None;
            // tbl.update_format(fmt);

            match ext {
                Some("csv") => {
                    let mut s = tbl.to_string();
                    f.write_all(s.as_bytes()).map_err(|e| format!("{}", e) )
                },
                Some("fodt") => {
                    let s = monday::report::ooxml::substitute_ooxml(&tbl, &read_template(template_path)?)
                        .map_err(|e| format!("{}", e) )?;
                    f.write_all(s.as_bytes()).map_err(|e| format!("{}", e) )
                },
                Some("html") => {
                    let s = monday::report::html::substitute_html(&tbl, &read_template(template_path)?)
                        .map_err(|e| format!("{}", e) )?;
                    f.write_all(s.as_bytes()).map_err(|e| format!("{}", e) )
                },
                _ => Err(format!("Invalid extension"))
            }
        },
        ExportItem::Panel(mut panel) => {
            panel.draw_to_file(path.to_str().unwrap())
        }
    }
}

fn read_template(template_path : Option<String>) -> Result<String, String> {
    if let Some(template) = template_path {
        let mut s = String::new();
        let mut f = File::open(&template).map_err(|e| format!("{}", e))?;
        f.read_to_string(&mut s).map_err(|e| format!("{}", e))?;
        Ok(s)
    } else {
        Err(format!("Missing template"))
    }
}

#[derive(Clone, Debug)]
pub enum EnvironmentUpdate {

    Clear,

    /// Environment has completely new set of tables.
    /// One outer vector per table; Inner vectors hold column names and queries.
    NewTables(Vec<Vec<String>>, Vec<String>),

    /// Environment has some new tables added to the right;
    /// But preserve old tables.
    AppendTables(Vec<Vec<String>>, Vec<String>),

    /// Preserve last column sequence, just update the data.
    Refresh,

    /// External table added to environment (by loading CSV file or executing command)
    NewExternal

}

/// The plot JSON representation is still kept in the Tables::tables
/// vector, it is just now shown. But the original plot content is kept
/// here in case it is required to be exported.
pub struct Plots {

    panels : Vec<Panel>,

    ixs : Vec<usize>

}

impl Plots {

    pub fn clear(&mut self) {
        self.panels.clear();
        self.ixs.clear();
    }

    pub fn new() -> Self {
        Self {
            panels : Vec::new(),
            ixs : Vec::new()
        }
    }

    pub fn update_from_tables(&mut self, tables : &[Table]) -> Result<(), String> {
        self.clear();
        for (ix, tbl) in tables.iter().enumerate() {
            if let Some(val) = tbl.single_json_field() {
                match val {
                    serde_json::Value::Object(ref map) => {
                        let is_panel = map.contains_key("elements") && map.contains_key("layout") && map.contains_key("design");
                        let is_plot = map.contains_key("x") && map.contains_key("y") && map.contains_key("mappings");
                        if is_panel || is_plot {
                            match Panel::new_from_json(&val.to_string()) {
                                Ok(panel) => {
                                    self.ixs.push(ix);
                                    self.panels.push(panel);
                                },
                                Err(e) => {
                                    return Err(format!("{}", e));
                                }
                            }
                        }
                    },
                    _ => { }
                }
            }
        }
        Ok(())
    }

}

pub struct Tables {

    // source : EnvironmentSource,

    // SQL substitutions
    // subs : HashMap<String, String>,

    // listener : SqlListener,

    /// Stores tables that returned successfully. 1:1 correspondence
    /// with self.queries
    tables : Vec<Table>,

    /// Stores queries which returned successfully. 1:1 correspondence with self.tables
    queries : Vec<String>,

    /// Stores message results of non-select statements that returned successfully.
    exec_results : Vec<StatementOutput>,

    last_update : Option<String>,

    history : Vec<EnvironmentUpdate>,



    // loader : Arc<Mutex<FunctionLoader>>,
}

impl Tables {

    /*pub fn clear(&mut self) {
        self.tables.clear();
        self.queries.clear();
        self.exec_results.clear();
        self.history.clear();
        self.last_update = None;
        self.listen_channels.clear();
    }*/

    pub fn new() -> Self {
        Self{
            tables : Vec::new(),
            last_update : None,
            queries : Vec::new(),
            history : vec![EnvironmentUpdate::Clear],
            exec_results : Vec::new(),
        }
    }

    /// Try to update the tables, potentially returning the first error
    /// message encountered by the database. Returns None if there
    /// is no update; Returns the Ok(result) if there is update, potentially
    /// carrying the first error the database encountered. If the update is valid,
    /// return the update event that happened (Refresh or NewTables).
    pub fn update_from_query_results(
        &mut self,
        results : Vec<StatementOutput>
    ) -> Option<Result<EnvironmentUpdate, ExecutionError>> {
        self.tables.clear();
        self.queries.clear();
        self.exec_results.clear();
        if results.len() == 0 {
            self.history.push(EnvironmentUpdate::Clear);
            return Some(Ok(EnvironmentUpdate::Clear));
        }
        let mut new_cols : Vec<Vec<String>> = Vec::new();
        let mut opt_err = None;
        let mut any_valid = false;
        for r in results {
            match r {
                StatementOutput::Valid(query, tbl) => {
                    new_cols.push(tbl.names());
                    self.tables.push(tbl);
                    self.queries.push(query.trim().to_string());
                    any_valid = true;
                },
                StatementOutput::Invalid(msg, is_server) => {
                    self.tables.clear();
                    self.history.push(EnvironmentUpdate::Clear);
                    opt_err = Some(ExecutionError { msg : msg.clone(), is_server });
                },
                StatementOutput::Statement(_) | StatementOutput::Modification(_) | StatementOutput::Empty => {
                    self.tables.clear();
                    self.exec_results.push(r.clone());
                    self.history.push(EnvironmentUpdate::Clear);
                },
            }
        }

        if let Some(err) = opt_err {
            self.history.push(EnvironmentUpdate::Clear);
            Some(Err(err))
        } else {
            if any_valid {
                let last_state = (self.last_table_columns(), self.last_queries());
                let last_update = if let (Some(last_cols), Some(last_queries)) = last_state {
                    let n_col_names_equal = last_cols.iter().flatten()
                        .zip(new_cols.iter().flatten())
                        .take_while(|(last, new)| last == new )
                        .count();
                    let n_queries_equal = last_queries.iter()
                        .zip(self.queries.iter())
                        .take_while(|(last, new)| last == new )
                        .count();
                    if n_col_names_equal == 0 && n_queries_equal == 0 {
                        EnvironmentUpdate::NewTables(new_cols, self.queries.clone())
                    } else {
                        if self.queries.len() > n_queries_equal && new_cols.len() > n_col_names_equal {
                            EnvironmentUpdate::AppendTables(new_cols.clone(), self.queries.clone())
                        } else {
                            EnvironmentUpdate::NewTables(new_cols, self.queries.clone())
                        }
                    }
                } else {
                    EnvironmentUpdate::NewTables(new_cols, self.queries.clone())
                };
                self.history.push(last_update.clone());
                // println!("History: {:?}", self.history);
                Some(Ok(last_update))
            } else {
                Some(Ok(EnvironmentUpdate::Clear))
            }
        }
    }

    pub fn update_from_statement(&mut self, results : Vec<StatementOutput>) -> Option<Result<String, ExecutionError>> {
        self.exec_results.clear();
        for r in results.iter() {
            match r {
                StatementOutput::Statement(_) | StatementOutput::Modification(_) => {
                    self.exec_results.push(r.clone());
                },
                StatementOutput::Invalid(e, is_server) => {
                    return Some(Err(ExecutionError{ msg : e.clone(), is_server : *is_server}));
                },
                _ => { }
            }
        }
        if let Some(r) = results.last() {
            // println!("Last statement: {:?}", r);
            match r {
                StatementOutput::Statement(s) => Some(Ok(s.clone())),
                StatementOutput::Invalid(e, is_server) => Some(Err(ExecutionError { msg : e.clone(), is_server : *is_server })),
                StatementOutput::Modification(m) => Some(Ok(m.clone())),
                StatementOutput::Valid(_, _) => None,
                StatementOutput::Empty => Some(Ok(format!("No results to show")))
            }
        } else {
            None
        }
    }

    pub fn any_modification_result(&self) -> bool {
        for res in self.exec_results.iter() {
            match res {
                StatementOutput::Modification(_) => return true,
                _ => { }
            }
        }
        false
    }

    pub fn queries(&self) -> &[String] {
        &self.queries[..]
    }

    /*pub fn set_new_postgre_engine(
        &mut self,
        conn_str : String
    ) -> Result<(), String> {
        match SqlEngine::try_new_postgre(conn_str) {
            Ok(engine) => { self.update_engine(engine)?; Ok(()) },
            Err(msg) => Err(msg)
        }
    }

    pub fn set_new_sqlite3_engine(
        &mut self,
        path : Option<PathBuf>
    ) -> Result<(), String> {
        match SqlEngine::try_new_sqlite3(path, &self.loader) {
            Ok(engine) => {
                self.update_engine(engine)?;
                Ok(())
            },
            Err(msg) => Err(msg)
        }
    }

    pub fn disable_engine(&mut self) {
        if let Ok(mut engine) = self.listener.engine.lock() {
            *engine = SqlEngine::Inactive;
        } else {
            println!("Could not acquire lock over SQL engine");
        }
    }*/

    pub fn current_hist_index(&self) -> usize {
        self.history.len() - 1
    }

    pub fn full_history(&self) -> &[EnvironmentUpdate] {
        &self.history[..]
    }

    /// Check if there were any data changes since the last informed position.
    /// TODO mapping not really being removed when table environment is updated with
    /// query that yields similarly-named columns. Criteria for removal should be query
    /// content, not query output (e.g. order by will preserve column names but return different data).
    pub fn preserved_since(&self, pos : usize) -> bool {
        // println!("Current history: {:?}", self.history);
        if pos == self.history.len() - 1 {
            true
        } else {
            let changed = self.history.iter()
                .skip(pos)
                .any(|h|
                    match h {
                        EnvironmentUpdate::Refresh => false,
                        _ => true
                    }
                );
            !changed
        }
    }

    /*fn on_notify(client : &mut Client, notif : &str) -> Result<(), String> {
        client.execute(&format!("listen {};", notif)[..], &[]).map_err(|e| format!("{}", e) )?;
        loop {
            let notif_received = match client.notifications().blocking_iter().next() {
                Ok(Some(notif)) => {
                    println!("Notification received from channel: {:?}", notif.channel());
                    println!("Notification message: {:?}", notif.payload());
                    true
                },
                Ok(None) => {
                    println!("Empty notification received");
                    false
                },
                Err(e) => {
                    println!("Connection lost: {}", e);
                    return Ok(());
                }
            };
            if notif_received {

            }
            thread::sleep(time::Duration::from_millis(200));
        }
    }

    fn on_interval(client : &mut Client, interval : usize) -> Result<(), String> {

        Ok(())
    }*/

    /*pub fn get_engine_name(&self) -> String {
        if let Ok(engine) = self.listener.engine.lock() {
            match *engine {
                SqlEngine::Inactive => String::from("Inactive"),
                SqlEngine::PostgreSql{..} => String::from("PostgreSQL"),
                SqlEngine::Sqlite3{..} => String::from("SQLite3"),
                SqlEngine::Local{..} => String::from("Local"),

                #[cfg(feature="arrowext")]
                SqlEngine::Arrow{..} => String::from("Arrow"),
            }
        } else {
            String::from("Unavailable")
        }
    }*/

    /*/// Executes the statement without changing the current state.
    pub fn execute_plain(&mut self, sql : &str) -> Result<(), String> {
        if let Ok(mut engine) = self.listener.engine.lock() {
            match *engine {
                SqlEngine::Inactive => {
                    Err(format!("No engine to execute statement"))
                },
                SqlEngine::PostgreSql{ ref mut conn, ..} => {
                    conn.execute(sql, &[]).map_err(|e| format!("{}", e))?;
                    Ok(())
                },
                SqlEngine::Sqlite3{ ref mut conn, ..} => {
                    unimplemented!()
                },
                SqlEngine::Local{ .. } => {
                    unimplemented!()
                },

                #[cfg(feature="arrowext")]
                SqlEngine::Arrow{..} => unimplemented!()
            }
        } else {
            Err(String::from("SQL engine unavailable"))
        }
    }

    /// Get engine active state. Consider it active in the event
    /// the lock could not be acquired.
    pub fn is_engine_active(&self) -> bool {
        if let Ok(engine) = self.listener.engine.lock() {
            match *engine {
                SqlEngine::Inactive => false,
                _ => true
            }
        } else {
            println!("Warning : Could not acquire lock over engine");
            true
        }
    }*/

    pub fn get_last_update_date(&self) -> String {
        match &self.last_update {
            Some(date) => date.clone(),
            None => String::from("Unknown")
        }
    }

    /*/// Execute a single function, appending the table to the end and returning a reference to it in case of
    /// success. Returns an error message from the user function otherwise.
    pub fn execute_func<'a>(&'a mut self, reg : Rc<FuncRegistry>, call : FunctionCall) -> Result<&'a Table, String> {
        if reg.has_func_name(&call.name[..]) {
            if let Some(f) = reg.retrieve_func(&call.name[..]) {
                let columns = self.get_columns(&call.source[..]);
                let ref_args : Vec<&str> = call.args.iter().map(|a| &a[..] ).collect();
                let ans = unsafe { f(columns, &ref_args[..]) };
                match ans {
                    Ok(res_tbl) => {
                        println!("{:?}", res_tbl);
                        let names = res_tbl.names();
                        self.tables.push(res_tbl);
                        self.history.push(EnvironmentUpdate::Function(call, names));
                        Ok(self.tables.last().unwrap())
                        /*let name = format!("({} x {})", nrows - 1, ncols);
                        tables_nb.add_page(
                            "network-server-symbolic",
                            Some(&name[..]),
                            None,
                            Some(t_rows),
                            fn_search.clone(),
                            pl_sidebar.clone(),
                            fn_popover.clone()
                        );*/
                        /*utils::set_tables(
                            &t_env,
                            &mut tbl_nb.clone(),
                            fn_search.clone(),
                            pl_sidebar.clone(),
                            fn_popover.clone()
                        );*/
                        //self.tables.last().to
                    },
                    Err(e) => {
                        println!("{}", e);
                        Err(e.to_string())
                    }
                }
            } else {
                Err("Error retrieving function".to_string())
            }
        } else {
            Err(format!("Function {} not in registry", call.name))
        }
        //Ok(())
    }*/

    /*/// Re-execute all function calls since the last NewTables history
    /// update, appending the resulting tables to the current environment.
    /// Returns a slice with the new generated tables.
    pub fn execute_saved_funcs<'a>(&'a mut self, reg : Rc<FuncRegistry>) -> Result<&'a [Table], String> {
        let recent_hist = self.history.iter().rev();
        let recent_hist : Vec<&EnvironmentUpdate> = recent_hist.take_while(|u| {
            match u {
                EnvironmentUpdate::NewTables(_) => false,
                _ => true
            }
        }).collect();
        let fns : Vec<FunctionCall> = recent_hist.iter().rev().filter_map(|update| {
            match update {
                EnvironmentUpdate::Function(call, _) => Some(call.clone()),
                _ => None
            }
        }).collect();
        println!("Last functions: {:?}", fns);
        let n_funcs = fns.len();
        for _ in 0..n_funcs {
            self.tables.remove(self.tables.len() - 1);
        }
        println!("Updated internal tables lenght before new call: {:?}", self.tables.len());
        for f in fns {
            self.execute_func(reg.clone(), f.clone())?;
        }
        println!("Internal tables length after new call: {:?}", self.tables.len());
        Ok(&self.tables[(self.tables.len() - n_funcs)..self.tables.len()])
    }*/

    /*pub fn send_current_query(&mut self, parse : bool) -> Result<(), String> {
        // println!("Sending current query: {:?}", self.source);
        let query = match self.source {
            EnvironmentSource::PostgreSQL(ref db_pair) =>{
                Some(db_pair.1.clone())
            },
            EnvironmentSource::SQLite3(ref db_pair) => {
                Some(db_pair.1.clone())
            },

            #[cfg(feature="arrowext")]
            EnvironmentSource::Arrow(ref q) => {
                Some(q.clone())
            },

            _ => None
        };
        if let Some(q) = query {
            if q.chars().all(|c| c.is_whitespace() ) {
                return Err(String::from("Empty query sequence"));
            }
            self.listener.send_command(q, self.subs.clone(), parse)
        } else {
            Err(format!("No query available to send."))
        }
    }*/

    /*pub fn create_csv_table(&mut self, path : PathBuf, name : &str) -> Result<(), String> {

        // Case DataFusion
        match self.listener.engine.lock() {
            Ok(engine) => {
                match *engine {
                    #[cfg(feature="arrowext")]
                    SqlEngine::Arrow{ ref mut ctx } => {
                        ctx.register_csv(
                            name,
                            path.to_str().unwrap(),
                            CsvReadOptions::new(),
                        ).map_err(|e| format!("{}", e) )?;
                        return Ok(());
                    },
                    _ => { }
                }
            },
            Err(e) => { return Err(format!("{}", e)); },
        }

        // Case Sqlite3
        let err = String::from("Could not parse table types");
        let mut content = String::new();
        let schema = if let Ok(mut f) = File::open(&path) {
            if let Ok(_) = f.read_to_string(&mut content) {
                if let Ok(tbl) = Table::new_from_text(content) {
                    tbl.sql_table_creation(name, &[]).ok_or(err)?
                } else {
                    return Err(err);
                }
            } else {
                return Err(err);
            }
        } else {
            return Err(err);
        };
        // println!("Schema: {}", schema); //schema='{}'
        let sql = format!("create virtual table temp.{} using \
            csv(filename='{}', header='YES', schema='{}');", name, path.to_str().unwrap(), schema
        );
        self.prepare_and_send_query(sql, HashMap::new(), false)?;
        Ok(())
    }*/

    /*pub fn clear_queries(&mut self) {
        let no_query = String::new();
        self.prepare_query(no_query, HashMap::new());
    }*/

    /*pub fn prepare_query(&mut self, sql : String, subs : HashMap<String, String>) {
        match self.source {
            EnvironmentSource::PostgreSQL((_, ref mut q)) => {
                *q = sql;
            },
            EnvironmentSource::SQLite3((_, ref mut q)) => {
                *q = sql;
            },

            #[cfg(feature="arrowext")]
            EnvironmentSource::Arrow(ref mut q) => {
                *q = sql;
            },

            _ => { }
        }
        self.subs = subs;
    }

    /// Original SQL; SQL with parameter substitutions
    pub fn prepare_and_send_query(&mut self, sql : String, subs : HashMap<String, String>, parse : bool) -> Result<(), String> {
        //self.listener.send_command(sql.clone());
        self.prepare_query(sql, subs);
        self.send_current_query(parse)
    }*/

    fn last_queries(&self) -> Option<Vec<String>> {
        for update in self.history.iter().rev() {
            match update {
                EnvironmentUpdate::NewTables(_, ref queries) => {
                    return Some(queries.clone());
                },
                EnvironmentUpdate::AppendTables(_, ref queries) => {
                    return Some(queries.clone());
                },
                EnvironmentUpdate::Clear => {
                    return None;
                }
                _ => { }
            }
        }
        None
    }

    /// Searches update history retroactively, returning the last
    /// original table update column names, if any exist, and
    /// no clear events are present between this last table set
    /// and the end of the history.
    fn last_table_columns(&self) -> Option<Vec<Vec<String>>> {
        for update in self.history.iter().rev() {
            match update {
                EnvironmentUpdate::NewTables(ref tbls, _) => {
                    return Some(tbls.clone());
                },
                EnvironmentUpdate::AppendTables(ref tbls, _) => {
                    return Some(tbls.clone());
                },
                EnvironmentUpdate::Clear => {
                    return None;
                }
                _ => { }
            }
        }
        None
    }

    // pub fn clear_results(&mut self) {
    //    self.listener.clear_results();
    // }

    /*/// Try to update the table from a source such as a SQL connection string
    /// or a file path.
    pub fn update_source(
        &mut self,
        src : EnvironmentSource,
        clear : bool
    ) -> Result<(),String> {
        if clear {
            self.tables.clear();
            self.history.push(EnvironmentUpdate::Clear);
        }
        //println!("{:?}", src );
        match src.clone() {
            // Updates from a single CSV file. This implies only a single
            // table is available.
            EnvironmentSource::File(path, content) => {
                //println!("Received source at update_from_source: {}", content);
                self.tables.clear();
                let p = Path::new(&path);
                let _p = p.file_stem().ok_or("Could not extract table name from path".to_string())?;
                //let _tbl_name = Some(p.to_str().ok_or("Could not convert table path to str".to_string())?.to_string());
                match Table::new_from_text(content.to_string()) {
                    Ok(tbl) => { self.tables.push(tbl)  },
                    Err(e) => { return Err(format!("Error: {}", e)); }
                }
            },
            EnvironmentSource::PostgreSQL((conn, q)) => {
                self.set_new_postgre_engine(conn)
                    .map_err(|e| { format!("{}", e) })?;
                if !q.is_empty() {
                    if let Err(e) = self.prepare_and_send_query(q, HashMap::new(), true) {
                        println!("{}", e);
                    }
                }
            },
            EnvironmentSource::SQLite3((conn, q)) => {
                self.set_new_sqlite3_engine(conn)
                    .map_err(|e|{ format!("{}", e) })?;
                if !q.is_empty() {
                    if let Err(e) = self.prepare_and_send_query(q, HashMap::new(), true) {
                        println!("{}", e);
                    }
                }
            },

            #[cfg(feature="arrowext")]
            EnvironmentSource::Arrow(_) => {
                let ctx = ExecutionContext::new();
                self.update_engine(SqlEngine::Arrow{ ctx })?;
            }

            _ => { println!("Invalid_source"); }
        }
        self.source = src;
        Ok(())
    }

    pub fn convert_source_to_in_memory_sqlite(&mut self) {
        match &self.source {
            EnvironmentSource::SQLite3(_) => {
                println!("Source is already SQLite3");
            },
            EnvironmentSource::PostgreSQL(_) => {
                println!("Invalid source update: PostgreSQL->SQLite3");
            },
            _ => {
                if self.tables.len() == 0 {
                    println!("No tables on environment for conversion to in-memory SQLite3");
                    return;
                }
                let new_src = EnvironmentSource::SQLite3((None,"".into()));
                let tables = self.tables.clone();
                self.clear_tables();
                match self.update_source(new_src, true) {
                    Ok(_) => {
                        if let Ok(mut engine) = self.listener.engine.lock() {
                            for t in tables {
                                engine.insert_external_table(&t);
                            }
                        } else {
                            println!("Unable to acquire lock over SQL listener to insert tables");
                        }
                    },
                    Err(e) => println!("{}", e)
                }
            }
        }
    }*/

    pub fn last_inserted_table(&self) -> Option<Table> {
        self.tables.last().map(|t| t.clone())
    }

    fn get_table_by_index(&mut self, idx : usize) -> Result<&mut Table,&'static str> {
        match self.tables.get_mut(idx) {
            Some(t) => Ok(t),
            None => Err("No table at informed index")
        }
    }

    /*fn get_column_at_index<'a>(&'a self, tbl_ix : usize, col_ix : usize) -> Result<&'a Column, &'static str> {
        let tbl = self.get_table_by_index(tbl_ix)?;
        tbl.get_column(col_ix).ok_or("Invalid column index")
    }*/

    /// Gets the textual representation of the table at the given index,
    /// optionally updating the table formatting before doing so.
    pub fn get_text_at_index(&mut self, idx : usize, opt_fmt : Option<TableSettings>) -> Option<String> {
        if let Ok(tbl) = self.get_table_by_index(idx) {
            if let Some(fmt) = opt_fmt {
                tbl.update_format(fmt);
            }
            Some(tbl.to_string())
        } else {
            None
        }
    }

    pub fn global_to_tbl_ixs(&self, global_ixs : &[usize]) -> Option<(usize, Vec<usize>)> {
        if let Some((cols,tbl_ix,_)) = self.get_columns(global_ixs) {
            Some((tbl_ix, cols.indices().iter().cloned().collect()))
        } else {
            None
        }
    }

    /// Get informed columns, where indices are counted
    /// from the first column of the first table up to the
    /// last column of the last table. Columns must be part of the same
    /// table. Return the table index at the second positoin and the query for the given
    /// table at the last position.
    pub fn get_columns<'a>(&'a self, global_ixs : &[usize]) -> Option<(Columns<'a>, usize, String)> {
        let mut base_ix : usize = 0;
        for (i, tbl) in self.tables.iter().enumerate() {
            let ncols = tbl.shape().1;
            let local_ixs : Vec<usize> = global_ixs.iter()
                .filter(|ix| **ix >= base_ix && **ix < base_ix + ncols)
                .map(|ix| ix - base_ix).collect();
            if (local_ixs.len() > 0) && (local_ixs.len() == global_ixs.len()) {
                let mut cols = Columns::new();
                cols = cols.clone().take_and_extend(tbl.get_columns(&local_ixs));
                let query = self.get_queries().get(i).cloned()
                    .unwrap_or(String::new());
                return Some((cols, i, query));
            }
            base_ix += ncols;
        }
        None
    }

    pub fn get_global_indices_for_names(&self, names : &[String]) -> Option<Vec<usize>> {
        let all_names = self.all_names();
        // println!("All names: {:?}", all_names);
        let mut global_ixs = Vec::new();
        // This does not guarantee uniqueness (just that at least one element exists)
        for name in names.iter() {
            if let Some(pos) = all_names.iter().position(|n| &name[..] == &n[..] ) {
                global_ixs.push(pos);
            } else {
                println!("No global index found for {} in the current environment", name);
                return None;
            }
        }
        // println!("Global indices: {:?}", global_ixs);
        Some(global_ixs)
    }

    pub fn get_local_indices_for_global(&self, global_ixs : &[usize]) -> Option<(usize, Vec<usize>)> {
        let n_tables = self.tables.len();
        let mut baseline = 0;
        let mut local_ixs = Vec::new();
        for ix_tbl in 0..n_tables {
            let n_cols = self.tables[ix_tbl].shape().1;
            for c_ix in 0..n_cols {
                for g_ix in global_ixs {
                    if *g_ix == baseline + c_ix {
                        local_ixs.push(c_ix);
                    }
                }
            }
            if global_ixs.len() == local_ixs.len() {
                return Some((ix_tbl, local_ixs));
            }
            baseline += n_cols;
        }
        None
    }

    /// Return global column indices for given query
    pub fn get_global_for_source(&self, source : &str) -> Option<Vec<usize>> {
        assert!(self.queries.len() == self.tables.len());
        let mut start = 0;
        for (ix, query) in self.queries.iter().enumerate() {
            let n = self.tables[ix].shape().1;

            // TODO only keywords should be standardized over lower/uppercase or else
            // SQL literals might be wrongly identified.
            if &source.trim().to_lowercase()[..] == &query.trim().to_lowercase()[..] {
                return Some((start..(start+n)).collect());
            }
            start += n;
        }
        None
    }

    pub fn filter_global_by_names(&self, global_ixs : &[usize], test_names : &[String]) -> Option<Vec<usize>> {
        let (all_names, _, _) = self.get_column_names(global_ixs)?;
        let mut ixs = Vec::new();
        let mut n_found = 0;
        for (i, cand_name) in all_names.iter().enumerate() {
            if let Some(test_name) = test_names.get(n_found) {
                if cand_name == test_name {
                    ixs.push(global_ixs[i]);
                    n_found += 1;
                }
            } else {
                println!("No table column name at index {}", i);
            }
            if ixs.len() == test_names.len() {
                return Some(ixs);
            }
        }
        None
    }

    pub fn get_columns_for_unique_names<'a>(&'a self, names : &[String]) -> Option<(Columns<'a>, usize, String)> {
        let global_ixs = self.get_global_indices_for_names(names)?;
        // Check uniqueness
        if self.are_column_names_unique(&global_ixs[..]) {
            self.get_columns(&global_ixs[..])
        } else {
            println!("Names are not unique");
            return None;
        }
    }

    pub fn all_names(&self) -> Vec<String> {
        let mut all_names = Vec::new();
        for tbl in self.tables.iter() {
            for name in tbl.names() {
                all_names.push(name);
            }
        }
        all_names
    }

    pub fn are_column_names_unique(&self, global_ixs : &[usize]) -> bool {
        if let Some((selected_names, _, _)) = self.get_column_names(global_ixs) {
            let all_names = self.all_names();
            // println!("Testing {:?} against {:?}", selected_names, all_names);
            for (name, global_ix) in selected_names.iter().zip(global_ixs.iter()) {
                let has_before = if *global_ix > 0 {
                    all_names[0..*global_ix].iter().find(|n| &name[..] == &n[..] ).is_some()
                } else {
                    false
                };
                let has_after = if *global_ix < all_names.len() - 1 {
                    all_names[(global_ix+1)..].iter().find(|n| &name[..] == &n[..] ).is_some()
                } else {
                    false
                };
                if has_before || has_after {
                    println!("Name {} at global index {} is not unique", name, global_ix);
                    return false;
                }
            }
            true
        } else {
            false
        }
    }

    /// Get unique column names from the informed global indices; as long as names are unique throughout
    /// the whole table environment.
    pub fn get_unique_column_names(&self, global_ixs : &[usize]) -> Option<(Vec<String>, usize, String)> {
        if self.are_column_names_unique(global_ixs) {
            let selected_names = self.get_column_names(global_ixs)?;
            Some(selected_names)
        } else {
            None
        }
    }

    /// Return (Full column names vector, table index, query that resulted in the table).
    pub fn get_column_names(&self, global_ixs : &[usize]) -> Option<(Vec<String>, usize, String)> {
        if let Some((cols, tbl_ix, query)) = self.get_columns(global_ixs) {
            let names : Vec<String> = cols.names().iter()
                .map(|name| name.to_string() )
                .collect();
            Some((names, tbl_ix, query))
        } else {
            println!("Failed getting column names for indices {:?}", global_ixs);
            None
        }
    }

    /// One query per table in the full environment.
    pub fn get_queries(&self) -> &[String] {
        &self.queries[..]
    }

    pub fn set_table_at_index(
        &mut self,
        content : String,
        index : usize
    ) -> Result<(), &'static str> {
        if let Ok(new_t) = Table::new_from_text(content) {
            if let Some(t) = self.tables.get_mut(index) {
                *t = new_t;
                return Ok(())
            } else {
                Err("Invalid index")
            }
        } else {
            Err("Could not parse content")
        }
    }

    pub fn all_tables(&self) -> &[Table] {
        &self.tables[..]
    }

    /*pub fn all_tables_as_rows(&self) -> Vec<Vec<Vec<&str>>> {
        let mut tables = Vec::new();
        for t in self.tables.iter() {
            tables.push(t.clone().text_rows());
        }
        tables
        /*if let Some(t) = self.tables.iter().next() {
            t.as_rows()
        } else {
            Vec::new()
        }*/
    }*/

    //pub fn all_tables<'a>(&'a self) -> Vec<&'a Table> {
    //    self.tables.iter().map(|t| t).collect()
    //}

    pub fn all_tables_as_csv(&self) -> Vec<String> {
        let mut tbls_csv = Vec::new();
        for t in &self.tables {
            tbls_csv.push(t.to_string());
        }
        tbls_csv
    }

    pub fn append_table_from_text(
        &mut self,
        _name : Option<String>,
        content : String
    ) -> Result<(), &'static str> {
        let t = Table::new_from_text(content)?;
        self.append_external_table(t)?;
        Ok(())
    }

    // TODO receive function name and arguments
    // to save it to history.
    pub fn append_external_table(
        &mut self,
        tbl : Table
    ) -> Result<(), &'static str> {
        self.tables.push(tbl);
        self.history.push(EnvironmentUpdate::NewExternal);
        Ok(())
    }

    /*pub fn update_from_current_source(&mut self) {
        self.tables.clear();
        match self.source {
            EnvironmentSource::Stream(ref s) => {
                if let Some(c) = s.get_last_content() {
                    self.append_table_from_text(Some("A".into()), c.clone())
                        .unwrap_or_else(|e| println!("{:?}", e));
                }
            },
            EnvironmentSource::File(ref path, ref mut content) => {
                if let Ok(mut f) = File::open(path) {
                    let mut new_content = String::new();
                    let _ = f.read_to_string(&mut new_content)
                        .unwrap_or_else(|e|{ println!("{:?}", e); 0} );
                    *content = new_content;
                } else {
                    println!("Could not re-open file");
                }
            },
            EnvironmentSource::SQLite3(_) | EnvironmentSource::PostgreSQL(_) => {
                self.send_current_query(true).map_err(|e| println!("{}", e) ).ok();
            },

            #[cfg(feature="arrowext")]
            EnvironmentSource::Arrow(_) => {
                self.send_current_query(true).map_err(|e| println!("{}", e) ).ok();
            },
            _ => { }
        }
    }

    /// Copies a table in the current environment to the database. Useful
    /// to call copy from/to from the TablePopover GUI.
    pub fn copy_to_database(
        &mut self,
        mut tbl : Table,
        dst : &str,
        cols : &[String],
        should_create : bool,
        should_convert : bool
    ) -> Result<(), String> {
        let info = self.db_info()
            .ok_or(String::from("Unable to query database schema"))?;
        if let Ok(mut engine) = self.listener.engine.lock() {
            match *engine {
                SqlEngine::PostgreSql{ ref mut conn, .. } => {
                    //if let Some(tbl) = self.tables.get_mut(tbl_ix) {
                    postgresql::copy_table_to_postgres(conn, &mut tbl, dst, cols, &info[..])
                    // } else {
                    //    Err(format!("Invalid table index: {}", tbl_ix))
                    // }
                },
                SqlEngine::Sqlite3 { ref mut conn, .. } | SqlEngine::Local { ref mut conn }=> {
                    // if let Some(tbl) = self.tables.get_mut(tbl_ix) {
                    sqlite::copy_table_to_sqlite(conn, &mut tbl, dst, cols, &info[..])
                    // } else {
                    //    Err(format!("Invalid table index: {}", tbl_ix))
                    // }
                },
                _ => unimplemented!()
            }
        } else {
            Err(String::from("Engine unavailable for copy"))
        }
    }*/

    pub fn clear_tables(&mut self) {
        self.tables.clear();
        /*self.queries.clear();
        self.exec_results.clear();
        self.history.clear();
        self.last_update = None;
        self.listen_channels.clear();*/
    }

    // Pass this to environment source
    /*pub fn table_names_as_hash(&self) -> Option<HashMap<String, Vec<(String, String)>>> {
        let mut names = HashMap::new();
        match &self.source {
            EnvironmentSource::SQLite3(_) => {
                if let Ok(mut engine) = self.listener.engine.lock() {
                    if let Some(objs) = engine.get_table_names() {
                        for obj in objs {
                            names.insert(obj.name().into(), obj.fields().unwrap());
                        }
                    } else {
                        println!("Could not get table names from engine");
                        return None;
                    }
                } else {
                    println!("Unable to get mutable reference to engine");
                    return None;
                }
                Some(names)
            },
            _ => {
                println!("Table environment is not Sqlite3 and data could not be fetch");
                None
            }
        }
    }*/

    /*pub fn try_backup(&self, path : PathBuf) {
        if let Ok(engine) = self.listener.engine.lock() {
            engine.backup_if_sqlite(path);
        } else {
            println!("Unable to retrieve lock over SQL listener");
        }
    }*/

}

impl<'a> React<QueriesSettings> for (&'a Environment, &'a SharedUserState) {

    fn react(&self, settings : &QueriesSettings) {

    }

}


