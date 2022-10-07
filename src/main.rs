/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use gtk4::prelude::*;
use gtk4::*;
use libadwaita;
use std::env;
use stateful::React;
use filecase::MultiArchiverImpl;
use stateful::PersistentState;
use queries::*;
use queries::client::*;
use queries::ui::*;

// TODO views with a homonimous table are not being shown at the schema tree.

fn register_resource() {
    let bytes = glib::Bytes::from_static(include_bytes!(concat!(env!("OUT_DIR"), "/", "compiled.gresource")));
    let resource = gio::Resource::from_data(&bytes).unwrap();
    gio::resources_register(&resource);
}

fn _glib_logger() {

    /*static glib_logger: glib::GlibLogger = glib::GlibLogger::new(
        glib::GlibLoggerFormat::Plain,
        glib::GlibLoggerDomain::CrateTarget,
    );

    log::set_logger(&glib_logger);
    log::set_max_level(log::LevelFilter::Debug);
    log::info!("This line will get logged by glib");*/

    // glib::log_set_handler(None, glib::LogLevels::all(), false, true, |_, level, msg| {
    // });

}

fn _systemd_logger() {
    // Alternatively, use simple_logger
    // systemd_journal_logger::init();
    // log::set_max_level(log::LevelFilter::Info);
}

fn main() {

    register_resource();
    
    if let Err(e) = gtk4::init() {
        eprintln!("{}", e);
        return;
    }

    let application = Application::builder()
        .application_id(queries::APP_ID)
        .build();

    let style_manager = libadwaita::StyleManager::default();
    style_manager.set_color_scheme(libadwaita::ColorScheme::Default);

    let user_state = if let Some(mut path) = filecase::get_datadir(queries::APP_ID) {
        path.push(queries::SETTINGS_FILE);
        SharedUserState::recover(&path.to_str().unwrap()).unwrap_or_default()
    } else {
        eprintln!("Unable to get datadir for state recovery");
        SharedUserState::default()
    };
    let client = QueriesClient::new(&user_state);

    // Take shared ownership of the client state, because they will be needed to
    // persist the client state before the application closes (which is done outside
    // connect_activate). The state is updated for both of these structures just before
    // the window is closed. We have to do this because client is moved to the connect_activate
    // closure, but we need to keep a reference to its state after that happens.
    let script_final_state = client.scripts.final_state();
    let conn_final_state = client.conn_set.final_state();

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
                // theme.add_search_path("/home/diego/Software/gnome/queries/assets/icons");
                theme.add_resource_path("/com/github/limads/queries/icons");
            } else {
                eprintln!("Unable to get default GDK display");
            }

            // GTK4 widgets seem to be able to load them from the icon root. But the libadwaita
            // widgets expect them to be under icons/hicolor/scalable/actions.

            let window = ApplicationWindow::builder()
                .application(app)
                .title("Queries")
                .default_width(1024)
                .default_height(768)
                .build();
            let queries_win = QueriesWindow::build(window, &user_state);

            // It is critical that updating the window here is done before setting the react signal
            // below while the widgets are inert and thus do not change the state recursively.
            user_state.update(&queries_win);

            // Now the window has been updated by the state, it is safe to add the callback signals.
            user_state.react(&queries_win);

            client.conn_set.react(&queries_win.content.results.overview.conn_list);
            client.conn_set.react(&client.active_conn);
            client.conn_set.react(&queries_win);
            client.conn_set.react(&queries_win.content.results.overview.conn_bx);
            client.active_conn.react(&queries_win.content.results.overview.conn_bx);
            client.active_conn.react(&queries_win.titlebar.exec_btn);
            client.active_conn.react(&queries_win.sidebar.schema_tree);

            client.env.react(&client.active_conn);
            client.env.react(&queries_win.content.results.workspace);
            client.env.react(&queries_win.content.editor.export_dialog);
            // client.env.react(&queries_win.settings);
            client.env.react(&queries_win.titlebar.exec_btn);

            queries_win.content.react(&client.active_conn);
            queries_win.content.results.overview.conn_bx.react(&client.conn_set);
            queries_win.content.results.overview.conn_list.react(&client.conn_set);
            queries_win.content.results.overview.conn_list.react(&client.active_conn);
            queries_win.content.results.overview.conn_bx.react(&client.active_conn);
            queries_win.content.results.workspace.react(&client.env);

            queries_win.sidebar.schema_tree.react(&client.active_conn);
            queries_win.sidebar.file_list.react(&client.scripts);

            client.scripts.react(&queries_win.content.editor.save_dialog);
            client.scripts.react(&queries_win.content.editor.open_dialog);
            client.scripts.react(&queries_win.titlebar.main_menu);
            client.scripts.react(&queries_win.content.editor.script_list);
            client.scripts.react(&queries_win.sidebar.file_list);
            client.scripts.react(&queries_win.content.editor);
            client.scripts.react(&queries_win);

            queries_win.content.editor.react(&client.scripts);
            queries_win.content.editor.save_dialog.react(&client.scripts);
            queries_win.content.editor.save_dialog.react(&queries_win.titlebar.main_menu);

            queries_win.content.react(&client.scripts);
            queries_win.titlebar.exec_btn.react(&client.scripts);
            queries_win.titlebar.exec_btn.react(&client.active_conn);
            queries_win.titlebar.exec_btn.react(&queries_win.content);
            queries_win.titlebar.main_menu.react(&client.scripts);
            queries_win.content.react(&client.env);
            queries_win.sidebar.file_list.react(&client.active_conn);

            queries_win.content.results.overview.detail_bx.react(&client.active_conn);

            queries_win.react(&queries_win.titlebar);
            queries_win.react(&client.scripts);
            queries_win.find_dialog.react(&queries_win.titlebar.main_menu);
            queries_win.find_dialog.react(&queries_win.content.editor);
            queries_win.find_dialog.react(&client.scripts);

            queries_win.window.add_action(&queries_win.find_dialog.find_action);
            queries_win.window.add_action(&queries_win.find_dialog.replace_action);
            queries_win.window.add_action(&queries_win.find_dialog.replace_all_action);

            queries_win.content.editor.react(&queries_win.settings);
            user_state.react(&client.conn_set);
            user_state.react(&client.scripts);

            // It is important to make this call to add scripts and connections
            // only after all signals have been setup, to guarantee the GUI will update
            // when the client updates.
            crate::client::set_client_state(&user_state, &client);

            queries_win.content.editor.configure(&user_state.borrow().editor);

            queries_win.window.show();
        }
    });

    // The final states for scripts and conn_set are updated just when the window is
    // closed, which happens before application::run unblocks the main thread.
    application.run();

    user_state.replace_with(|user_state| {
        user_state.conns = conn_final_state.borrow().clone();
        user_state.scripts = script_final_state.borrow().recent.clone();
        user_state.clone()
    });

    if let Some(mut path) = filecase::get_datadir(queries::APP_ID) {
        path.push(queries::SETTINGS_FILE);
        user_state.persist(&path.to_str().unwrap());
    } else {
        eprintln!("Unable to get datadir for state persistence");
    }

}


