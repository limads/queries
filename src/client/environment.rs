/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use gtk4::*;
use gtk4::prelude::*;
use std::fs::File;
use crate::sql::*;
use std::path::Path;
use stateful::{Callbacks};
use crate::tables::table::Table;
use stateful::React;
use crate::client::ActiveConnection;
use crate::tables::table::TableSettings;
use crate::tables::table::Columns;
use papyri::render::Panel;
use crate::ui::QueriesWorkspace;
use std::io::Write;
use std::thread;
use crate::ui::ExportDialog;
use crate::client::ExecutionSettings;
use crate::client::SharedUserState;
use crate::ui::ExecButton;

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

    Restore,

    Clear,

    Select(Option<usize>),

    /// Request to export the currently selected item to the path given as the argument.
    ExportRequest(String),

    ChangeSetting(ExecutionSettings),

    ExportError(String)

}

pub struct Environment {

    pub user_state : SharedUserState,
    
    send : glib::Sender<EnvironmentAction>,

    on_tbl_update : Callbacks<Vec<Table>>,

    on_tbl_error : Callbacks<String>,

    on_export_error : Callbacks<String>

}

impl Environment {

    pub fn new(user_state : &SharedUserState) -> Self {
        let (send, recv) = glib::MainContext::channel::<EnvironmentAction>(glib::PRIORITY_DEFAULT);
        let mut tables = Tables::new();
        let mut plots = Plots::new();
        let on_tbl_update : Callbacks<Vec<Table>> = Default::default();
        let on_export_error : Callbacks<String> = Default::default();
        let on_tbl_error : Callbacks<String> = Default::default();
        let mut selected : Option<usize> = None;
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
                                        on_tbl_update.call(tables.tables.clone());
                                    }
                                },
                                Err(e) => {
                                    on_tbl_error.call(e.clone());
                                }
                            }
                        }
                    },
                    EnvironmentAction::Restore => {
                        // Use the last state set at EnvironmentAction::Update.
                        if tables.tables.len() >= 1 {
                            on_tbl_update.call(tables.tables.clone());
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
                                move || {
                                    if let Err(e) = export_to_path(item, Path::new(&path)) {
                                        send.send(EnvironmentAction::ExportError(e)).unwrap();
                                    }
                                }
                            });
                        }
                    },
                    EnvironmentAction::ExportError(msg) => {
                        on_export_error.call(msg.clone());
                    },
                    EnvironmentAction::ChangeSetting(_setting) => {

                    },
                    _ => { }
                }
                Continue(true)
            }
        });
        Self { send, on_tbl_update, on_export_error, on_tbl_error, user_state : user_state.clone() }
    }

    pub fn connect_table_update<F>(&self, f : F)
    where
        F : Fn(Vec<Table>) + 'static
    {
        self.on_tbl_update.bind(f);
    }

    pub fn connect_export_error<F>(&self, f : F)
    where
        F : Fn(String) + 'static
    {
        self.on_export_error.bind(f);
    }

    pub fn connect_table_error<F>(&self, f : F)
    where
        F : Fn(String) + 'static
    {
        self.on_tbl_error.bind(f);
    }

}

impl React<ActiveConnection> for Environment {

    fn react(&self, conn : &ActiveConnection) {
        let send = self.send.clone();
        conn.connect_exec_result(move |res : Vec<StatementOutput>| {
            send.send(EnvironmentAction::Update(res)).unwrap();
        });
    }

}

impl React<ExecButton> for Environment {

    fn react(&self, btn : &ExecButton) {
        btn.restore_action.connect_activate({
            let send = self.send.clone();
            move |_action, _param| {
                send.send(EnvironmentAction::Restore).unwrap();
            }
        });
    }

}

impl React<QueriesWorkspace> for Environment {

    fn react(&self, ws : &QueriesWorkspace) {
        let send = self.send.clone();
        ws.tab_view.connect_selected_page_notify(move|view| {
            if view.selected_page().is_some() {
                let pages = view.pages();
                send.send(EnvironmentAction::Select(Some(pages.selection().nth(0) as usize))).unwrap();
            } else {
                send.send(EnvironmentAction::Select(None)).unwrap();
            }
        });
    }

}

impl React<ExportDialog> for Environment {

    fn react(&self, dialog : &ExportDialog) {
        let send = self.send.clone();
        dialog.dialog.connect_response(move |dialog, resp| {
            match resp {
                ResponseType::Accept => {
                    if let Some(path) = dialog.file().and_then(|f| f.path() ) {
                        if let Some(p) = path.to_str() {
                            send.send(EnvironmentAction::ExportRequest(p.to_string())).unwrap();
                        } else {
                            eprintln!("Path is not valid UTF-8")
                        }
                    }
                },
                _ => { }
            }
        });
    }

}

fn export_to_path(item : ExportItem, path : &Path) -> Result<(), String> {
    let ext = path.extension().map(|ext| ext.to_str().unwrap_or("") );
    match item {
        ExportItem::Table(mut tbl) => {
            let mut export_format = TableSettings::default();
            export_format.prec = None;
            tbl.update_format(export_format);
            match ext {
                Some("csv") => {
                    let mut f = File::create(path).map_err(|e| format!("Error creating export file: {}", e) )?;
                    let s = tbl.to_csv();
                    f.write_all(s.as_bytes()).map_err(|e| format!("Error writing to export file: {}", e) )
                },
                Some("md") => {
                    let mut f = File::create(path).map_err(|e| format!("Error creating export file: {}", e) )?;
                    let s = tbl.to_markdown();
                    f.write_all(s.as_bytes()).map_err(|e| format!("Error writing to export file: {}", e) )
                },
                Some("tex") => {
                    let mut f = File::create(path).map_err(|e| format!("Error creating export file: {}", e) )?;
                    let s = tbl.to_tex();
                    f.write_all(s.as_bytes()).map_err(|e| format!("Error writing to export file: {}", e) )
                },
                _ => Err(format!("Invalid file extension for table export (expected .csv, .md or .tex)"))
            }
        },
        ExportItem::Panel(mut panel) => {
            match ext {
                Some("png") | Some("eps") | Some("svg") => {
                    panel.draw_to_file(path.to_str().unwrap()).map_err(|e| format!("{e}") )
                },
                _ => {
                    Err(format!("Invalid file extension for plot export (expected .png, .eps or .svg)"))
                }
            }
        }
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
                        let is_panel = map.contains_key("plots");
                        let is_plot = map.contains_key("x") && map.contains_key("y") && map.contains_key("mappings");
                        if is_panel || is_plot {
                            match Panel::new_from_json(&val.to_string()) {
                                Ok(panel) => {
                                    self.ixs.push(ix);
                                    self.panels.push(panel);
                                },
                                Err(e) => {
                                    return Err(format!("Error in plot definition: {}", e));
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

    /// Stores tables that returned successfully. 1:1 correspondence
    /// with self.queries
    tables : Vec<Table>,

    /// Stores queries which returned successfully. 1:1 correspondence with self.tables
    queries : Vec<String>,

    /// Stores message results of non-select statements that returned successfully.
    exec_results : Vec<StatementOutput>,

    last_update : Option<String>,

    history : Vec<EnvironmentUpdate>,

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
                StatementOutput::Statement(_) | 
                    StatementOutput::Modification(_) | 
                    StatementOutput::Empty | 
                    StatementOutput::Committed(_, _) | 
                    StatementOutput::RolledBack(_) => 
                {
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
            match r {
                StatementOutput::Statement(s) | StatementOutput::Committed(s, _) | StatementOutput::RolledBack(s) => {
                    Some(Ok(s.clone()))
                },
                StatementOutput::Invalid(e, is_server) => {
                    Some(Err(ExecutionError { msg : e.clone(), is_server : *is_server }))
                },
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

    pub fn get_last_update_date(&self) -> String {
        match &self.last_update {
            Some(date) => date.clone(),
            None => String::from("Unknown")
        }
    }

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

    pub fn last_inserted_table(&self) -> Option<Table> {
        self.tables.last().map(|t| t.clone())
    }

    fn get_table_by_index(&mut self, idx : usize) -> Result<&mut Table,&'static str> {
        match self.tables.get_mut(idx) {
            Some(t) => Ok(t),
            None => Err("No table at informed index")
        }
    }

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
        let mut global_ixs = Vec::new();
        // This does not guarantee uniqueness (just that at least one element exists)
        for name in names.iter() {
            if let Some(pos) = all_names.iter().position(|n| &name[..] == &n[..] ) {
                global_ixs.push(pos);
            } else {
                return None;
            }
        }
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

    pub fn clear_tables(&mut self) {
        self.tables.clear();
    }


}


