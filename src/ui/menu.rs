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
    pub action_save_as : gio::SimpleAction
}

impl MainMenu {

    pub fn build() -> Self {
        let menu = gio::Menu::new();
        menu.append(Some("New"), Some("win.new_file"));
        menu.append(Some("Open"), Some("win.open_file"));
        menu.append(Some("Save"), Some("win.save_file"));
        menu.append(Some("Save as"), Some("win.save_as_file"));
        let popover = PopoverMenu::from_model(Some(&menu));

        let action_new = gio::SimpleAction::new("new_file", None);
        let action_open = gio::SimpleAction::new("open_file", None);
        let action_save = gio::SimpleAction::new("save_file", None);
        let action_save_as = gio::SimpleAction::new("save_as_file", None);

        Self { popover, action_new, action_open, action_save, action_save_as }
    }

}

impl React<QueriesContent> for MainMenu {

    fn react(&self, content : &QueriesContent) {
        let actions = [self.action_save.clone(), self.action_save_as.clone()];
        content.stack.connect_visible_child_notify(move |stack| {
            if let Some(name) = stack.visible_child_name() {
                match name.as_str() {
                    "editor" => {
                        actions.iter().for_each(|action| action.set_enabled(true) );
                    },
                    "results" => {
                        actions.iter().for_each(|action| action.set_enabled(false) );
                    },
                    _ => { }
                }
            }
        });
    }
}
