use gtk4::prelude::*;
use gtk4::*;
use std::rc::Rc;
use stateful::React;
use crate::ui::QueriesContent;
use crate::client::OpenedScripts;
use archiver::MultiArchiverImpl;

#[derive(Debug, Clone)]
pub struct MainMenu {
    pub popover : PopoverMenu,
    pub action_new : gio::SimpleAction,
    pub action_open : gio::SimpleAction,
    pub action_save : gio::SimpleAction,
    pub action_save_as : gio::SimpleAction,
    pub action_export : gio::SimpleAction,
    pub action_settings : gio::SimpleAction,
    pub action_find_replace : gio::SimpleAction
}

impl MainMenu {

    pub fn build() -> Self {
        let menu = gio::Menu::new();
        menu.append(Some("New"), Some("win.new_file"));
        menu.append(Some("Open"), Some("win.open_file"));
        menu.append(Some("Save"), Some("win.save_file"));
        menu.append(Some("Save as"), Some("win.save_as_file"));
        menu.append(Some("Find and replace"), Some("win.find_replace"));
        menu.append(Some("Export"), Some("win.export"));
        menu.append(Some("Settings"), Some("win.settings"));
        let popover = PopoverMenu::from_model(Some(&menu));

        let action_new = gio::SimpleAction::new("new_file", None);
        let action_open = gio::SimpleAction::new("open_file", None);
        let action_save = gio::SimpleAction::new("save_file", None);
        let action_save_as = gio::SimpleAction::new("save_as_file", None);
        let action_export = gio::SimpleAction::new("export", None);
        let action_settings = gio::SimpleAction::new("settings", None);
        let action_find_replace = gio::SimpleAction::new("find_replace", None);
        action_save.set_enabled(false);
        action_save_as.set_enabled(false);
        action_export.set_enabled(false);
        action_find_replace.set_enabled(false);

        Self { popover, action_new, action_open, action_save, action_save_as, action_export, action_settings, action_find_replace }
    }

}

impl React<QueriesContent> for MainMenu {

    fn react(&self, content : &QueriesContent) {
        let save_actions = [self.action_save.clone(), self.action_save_as.clone()];
        let export_action = self.action_export.clone();
        let results_stack = content.results.stack.clone();
        content.stack.connect_visible_child_notify(move |stack| {
            if let Some(name) = stack.visible_child_name() {
                match name.as_str() {
                    "editor" => {
                        save_actions.iter().for_each(|action| action.set_enabled(true) );
                        export_action.set_enabled(false);
                    },
                    "results" => {
                        save_actions.iter().for_each(|action| action.set_enabled(false) );
                        if let Some(name) = results_stack.visible_child_name() {
                            if name.as_str() == "tables" {
                                export_action.set_enabled(true);
                            }
                        }
                    },
                    _ => { }
                }
            }
        });
        content.results.stack.connect_visible_child_notify({
            let export_action = self.action_export.clone();
            move |stack| {
                if let Some(name) = stack.visible_child_name() {
                    match name.as_str() {
                        "tables" => {
                            export_action.set_enabled(true);
                        },
                        "overview" => {
                            export_action.set_enabled(false);
                        },
                        _ => { }
                    }
                }
            }
        });
    }
}

impl React<OpenedScripts> for MainMenu {

    fn react(&self, scripts : &OpenedScripts) {
        let action_find_replace = self.action_find_replace.clone();
        scripts.connect_selected(move |opt_file| {
            if let Some(_) = opt_file.map(|f| f.index ) {
                action_find_replace.set_enabled(true);
            } else {
                action_find_replace.set_enabled(false);
            }
        });
    }

}

/*
save_img_dialog.connect_response(move |dialog, resp|{
    match resp {
        ResponseType::Other(1) => {
            if let Some(path) = dialog.get_filename() {
                if let Some(mut plots) = plots.upgrade() {
                    if let Ok(mut pls) = plots.try_borrow_mut() {
                        if let Some(p) = path.to_str() {
                            if let Some(mut pl) = pls.selected_mut() {
                                if let Err(e) = pl.plot_group.draw_to_file(p) {
                                    println!("{}", e);
                                }
                            } else {
                                println!("No plot currently selected");
                            }
                        } else {
                            println!("Could not retrieve path as str");
                        }
                    } else {
                        println!("Could not retrieve reference to pl_view when saving image");
                    }
                } else {
                    println!("Unable to upgrade plots vector");
                }
            } else {
                println!("Invalid path for image");
            }
        },
        _ => { }
    }
});
*/

/*
pub fn copy_table_from_csv(
    path : String,
    t_env : &mut TableEnvironment,
    action : sql::copy::Copy
) {
    assert!(action.target == CopyTarget::From);
    if let Ok(mut f) = File::open(&path) {
        let mut txt = String::new();
        if let Err(e) = f.read_to_string(&mut txt) {
            println!("{}", e);
            return;
        }
        match Table::new_from_text(txt) {
            Ok(tbl) => {
                let copy_ans = t_env.copy_to_database(
                    tbl,
                    &action.table[..],
                    &action.cols[..],
                    false,
                    true
                );
                println!("{:?}", copy_ans);
            },
            Err(e) => {
                println!("Error parsing table");
            }
        }
    } else {
        println!("Error opening file");
    }
}*/

/*pub fn export_selected_table_to_csv(
    save_tbl_dialog : FileChooserDialog,
    tables_nb : TableNotebook,
    tbl_env : Rc<RefCell<TableEnvironment>>
) {
    save_tbl_dialog.clone().connect_response(move |dialog, resp| {
        match resp {
            ResponseType::Other(1) => {
                if let Some(path) = dialog.get_filename() {
                    let ext = path.as_path()
                        .extension()
                        .map(|ext| ext.to_str().unwrap_or(""));
                    if let Some(ext) = ext {
                        if let Ok(mut t_env) = tbl_env.try_borrow_mut() {
                            match ext {
                                "db" | "sqlite" | "sqlite3" => {
                                    t_env.try_backup(path);
                                },
                                _ => {
                                    write_table_to_csv(&tables_nb, &mut t_env, &path);
                                }
                            }
                        } else {
                            println!("Unable to get reference to table environment");
                        }
                    } else {
                        println!("Unknown extension");
                    }
                } else {
                    println!("No filename available");
                }
            },
            _ => { }
        }
    });
}*/

/*pub fn import_table_from_csv(tbl_env : &mut TableEnvironment>>) {
    if let Ok(mut t_env) = tbl_env.try_borrow_mut() {
        let should_create = create_check.get_active();
        let should_convert = convert_check.get_active();
        let copy_ans = t_env.copy_to_database(
            idx,
            &action.dst[..],
            &action.cols[..],
            false,
            false
        );
        if let Err(e) = copy_ans {
            println!("{}", e);
        }
    }
}*/

