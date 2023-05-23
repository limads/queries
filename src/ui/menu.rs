/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use gtk4::*;
use stateful::React;
use crate::ui::QueriesContent;
use crate::client::OpenedScripts;
use filecase::MultiArchiverImpl;

#[derive(Debug, Clone)]
pub struct MainMenu {
    pub popover : PopoverMenu,
    pub action_new : gio::SimpleAction,
    pub action_open : gio::SimpleAction,
    pub action_save : gio::SimpleAction,
    pub action_save_as : gio::SimpleAction,
    pub action_export : gio::SimpleAction,
    pub action_settings : gio::SimpleAction,
    pub action_find_replace : gio::SimpleAction,
    pub action_about : gio::SimpleAction,
    pub action_graph : gio::SimpleAction,
    pub action_builder : gio::SimpleAction
}

impl MainMenu {

    pub fn build() -> Self {
        let menu = gio::Menu::new();
        menu.append(Some("New"), Some("win.new_file"));
        menu.append(Some("Open"), Some("win.open_file"));
        menu.append(Some("Save"), Some("win.save_file"));
        menu.append(Some("Save as"), Some("win.save_as_file"));
        menu.append(Some("Find and replace"), Some("win.find_replace"));
        menu.append(Some("Query builder"), Some("win.builder"));
        menu.append(Some("Graph editor"), Some("win.graph"));
        menu.append(Some("Export"), Some("win.export"));
        menu.append(Some("Settings"), Some("win.settings"));
        menu.append(Some("About"), Some("win.about"));
        let popover = PopoverMenu::from_model(Some(&menu));

        let action_new = gio::SimpleAction::new("new_file", None);
        let action_open = gio::SimpleAction::new("open_file", None);
        let action_save = gio::SimpleAction::new("save_file", None);
        let action_save_as = gio::SimpleAction::new("save_as_file", None);
        let action_graph = gio::SimpleAction::new("graph", None);
        let action_builder = gio::SimpleAction::new("builder", None);
        let action_export = gio::SimpleAction::new("export", None);
        let action_settings = gio::SimpleAction::new("settings", None);
        let action_find_replace = gio::SimpleAction::new("find_replace", None);
        let action_about = gio::SimpleAction::new("about", None);
        action_save.set_enabled(false);
        action_save_as.set_enabled(false);
        action_graph.set_enabled(false);
        action_builder.set_enabled(false);
        action_export.set_enabled(false);
        action_find_replace.set_enabled(false);

        Self { popover, action_new, action_open, action_save, action_save_as, action_export,
        action_settings, action_find_replace, action_about, action_graph, action_builder
        }
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

impl React<crate::client::ActiveConnection> for MainMenu {

    fn react(&self, conn : &crate::client::ActiveConnection) {
        conn.connect_db_connected({
            let action_graph = self.action_graph.clone();
            let action_builder = self.action_builder.clone();
            move |_| {
                action_graph.set_enabled(true);
                action_builder.set_enabled(true);
            }
        });
        conn.connect_db_disconnected({
            let action_graph = self.action_graph.clone();
            let action_builder = self.action_builder.clone();
            move |_| {
                action_graph.set_enabled(false);
                action_builder.set_enabled(false);
            }
        });
    }

}
