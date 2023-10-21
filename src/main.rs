/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use gtk4::prelude::*;
use gtk4::*;
use filecase::MultiArchiverImpl;
use stateful::PersistentState;
use queries::client::*;
use queries::ui::*;
use std::cell::RefCell;
use std::rc::Rc;

fn main() {

    queries::register_resources();
    
    // queries::ui::editor::COMPL.set(queries::ui::editor::SqlCompletionProvider::new());

    if let Err(e) = gtk4::init() {
        eprintln!("{}", e);
        return;
    }

    let application = Application::builder()
        .application_id(queries::APP_ID)
        .build();

    let style_manager = libadwaita::StyleManager::default();
    style_manager.set_color_scheme(libadwaita::ColorScheme::Default);

    style_manager.set_color_scheme(libadwaita::ColorScheme::ForceDark);

    let user_state = if let Some(mut path) = filecase::get_datadir(queries::APP_ID) {
        path.push(queries::SETTINGS_FILE);
        SharedUserState::recover(path.to_str().unwrap()).unwrap_or_default()
    } else {
        eprintln!("Unable to get datadir for state recovery");
        SharedUserState::default()
    };
    let modules = queries::load_modules();
    let call_params = Rc::new(RefCell::new(queries::ui::apply::CallParams::default()));
    let client = QueriesClient::new(&user_state, modules.clone(), call_params.clone());

    // Take shared ownership of the client state, because they will be needed to
    // persist the client state before the application closes (which is done outside
    // connect_activate). The state is updated for both of these structures just before
    // the window is closed. We have to do this because client is moved to the connect_activate
    // closure, but we need to keep a reference to its state after that happens.
    let script_final_state = client.scripts.final_state();

    application.set_accels_for_action("win.save_file", &["<Ctrl>S"]);
    application.set_accels_for_action("win.open_file", &["<Ctrl>O"]);
    application.set_accels_for_action("win.new_file", &["<Ctrl>N"]);
    application.set_accels_for_action("win.save_as_file", &["<Ctrl><Shift>S"]);
    application.set_accels_for_action("win.find_replace", &["<Ctrl>F"]);
    
    application.set_accels_for_action("win.queue_execution", &["F7"]);
    application.set_accels_for_action("win.clear", &["F8"]);
    application.set_accels_for_action("win.restore", &["F5"]);
    
    application.connect_activate({
        let user_state = user_state.clone();
        move |app| {
            if let Some(display) = gdk::Display::default() {
                let theme = IconTheme::for_display(&display);
                theme.add_resource_path("/io/github/limads/queries/icons");
            } else {
                eprintln!("Unable to get default GDK display");
            }
            let queries_win = QueriesWindow::build(app, &user_state, &modules, &call_params);
            queries::setup(&queries_win, &user_state, &client);
            queries_win.window.show();
        }
    });

    // The final states for scripts and conn_set are updated just when the window is
    // closed, which happens before application::run unblocks the main thread.
    application.run();

    user_state.replace_with(|user_state| {
        user_state.scripts = script_final_state.borrow().recent.clone();
        user_state.clone()
    });

    if let Some(mut path) = filecase::get_datadir(queries::APP_ID) {
        path.push(queries::SETTINGS_FILE);
        user_state.persist(path.to_str().unwrap());
    } else {
        eprintln!("Unable to get datadir for state persistence");
    }

}


