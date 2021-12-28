use gtk4::prelude::*;
use gtk4::*;
use std::rc::Rc;
use crate::React;
use crate::ui::QueriesContent;

#[derive(Debug, Clone)]
pub struct MainMenu {
    pub popover : PopoverMenu,
    pub action_new : gio::SimpleAction,
    pub action_open : gio::SimpleAction,
    pub action_save : gio::SimpleAction,
    pub action_save_as : gio::SimpleAction,
    pub action_export : gio::SimpleAction
}

impl MainMenu {

    pub fn build() -> Self {
        let menu = gio::Menu::new();
        menu.append(Some("New"), Some("win.new_file"));
        menu.append(Some("Open"), Some("win.open_file"));
        menu.append(Some("Save"), Some("win.save_file"));
        menu.append(Some("Save as"), Some("win.save_as_file"));
        menu.append(Some("Export"), Some("win.export"));
        let popover = PopoverMenu::from_model(Some(&menu));

        let action_new = gio::SimpleAction::new("new_file", None);
        let action_open = gio::SimpleAction::new("open_file", None);
        let action_save = gio::SimpleAction::new("save_file", None);
        let action_save_as = gio::SimpleAction::new("save_as_file", None);
        let action_export = gio::SimpleAction::new("export", None);
        action_save.set_enabled(false);
        action_save_as.set_enabled(false);
        action_export.set_enabled(false);

        Self { popover, action_new, action_open, action_save, action_save_as, action_export }
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
