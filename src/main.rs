#![allow(warnings)]

use gtk4::prelude::*;
use gtk4::*;
use sourceview5::prelude::*;
use glib::MainContext;
use std::rc::Rc;
use std::cell::RefCell;
use std::boxed;
use libadwaita;
use glib::{LogLevel, g_log};
use std::env;
use stateful::React;
use archiver::MultiArchiverImpl;
use stateful::PersistentState;

use queries4::*;

use queries4::client::*;

use queries4::server::*;

use queries4::ui::*;

/*impl React<ConnectionBox> for Connections {

    type Change = ConnectionChange;

    fn react(mut self, ch : glib::Receiver<Self::Change>) {
        change.attach(None, move |change| {
            match change {
                ConnectionChange::Add(info) => {
                    self.conns.add(info);
                },
                ConnectionChange::Remove(ix) => {
                    self.conns.remove(ix);
                }
            }
        });
    }

}*/

// gtk-encode-symbolic-svg -o . queries-symbolic.svg 16x16

// GTK_THEME=Adwaita:dark cargo run
// GTK_THEME=Adwaita:light cargo run
// On inkscape: Path -> Stroke to path to make strokes into fills.
// sudo cp queries-symbolic.svg /usr/share/icons/hicolor/scalable/actions
// sudo cp queries-symbolic.svg /usr/share/icons/Yaru/scalable/actions
fn main() {

    /*static glib_logger: glib::GlibLogger = glib::GlibLogger::new(
        glib::GlibLoggerFormat::Plain,
        glib::GlibLoggerDomain::CrateTarget,
    );

    log::set_logger(&glib_logger);
    log::set_max_level(log::LevelFilter::Debug);
    log::info!("This line will get logged by glib");*/

    // glib::log_set_handler(None, glib::LogLevels::all(), false, true, |_, level, msg| {
    // });

    // Alternatively, use simple_logger
    systemd_journal_logger::init();
    log::set_max_level(log::LevelFilter::Info);

    if let Err(e) = gtk4::init() {
        log::error!("{}", e);
        return;
    }

    /*let res_bytes = include_bytes!("../assets/icons.bin");
    let data = glib::Bytes::from(&res_bytes[..]);
    let resource = gio::Resource::from_data(&data).unwrap();
    gio::resources_register(&resource);*/
    // let res = gio::Resource::load("assets/resources.gresource").expect("Could not load resources");
    // gio::resources_register(&res);

    // let theme = IconTheme::for_display(&Some(&gdk::Display::default())).unwrap();
    // theme.add_search_path("/home/diego/.local/share/org.limads.queries/icons");
    // theme.add_resource_path("/assets");

    let application = Application::builder()
        .application_id(queries4::APP_ID)
        .build();

    match libadwaita::StyleManager::default() {
        Some(style_manager) => {
            style_manager.set_color_scheme(libadwaita::ColorScheme::Default);
        },
        None => {
            log::warn!("Could not get default libadwaita style manager");
        }
    }

    /*let user_state = if let Some(s) = SharedUserState::recover(queries4::SETTINGS_PATH) {
        queries4::log_debug_if_required("User state loaded");
        s
    } else {
        queries4::log_debug_if_required("No user state found. Starting from Default.");
        Default::default()
    };*/
    let user_state = if let Some(mut path) = archiver::get_datadir(queries4::APP_ID) {
        path.push(queries4::SETTINGS_FILE);
        SharedUserState::recover(&path.to_str().unwrap()).unwrap_or_default()
    } else {
        log::warn!("Unable to get datadir for state recovery");
        SharedUserState::default()
    };
    let client = QueriesClient::new(&user_state);

    // Take shared ownership of the client state, because they will be needed to
    // persist the client state before the application closes (which is done outside
    // connect_activate). The state is updated for both of these structures just before
    // the window is closed.
    let script_final_state = client.scripts.final_state();
    let conn_final_state = client.conn_set.final_state();

    application.connect_activate({
        let user_state = user_state.clone();
        move |app| {
            if let Some(display) = gdk::Display::default() {
                if let Some(theme) = IconTheme::for_display(&display) {
                    theme.add_search_path("/home/diego/Software/queries/assets/icons");
                } else {
                    log::warn!("Unable to get theme for current GDK display");
                }
            } else {
                log::warn!("Unable to get default GDK display");
            }

            // GTK4 widgets seem to be able to load them from the icon root. But the libadwaita
            // widgets expect them to be under icons/hicolor/scalable/actions.

            let window = ApplicationWindow::builder()
                .application(app)
                .title("Queries")
                .default_width(1024)
                .default_height(768)
                .build();
            let queries_win = QueriesWindow::from(window);

            user_state.update(&queries_win);

            user_state.react(&queries_win);

            client.conn_set.react(&queries_win.content.results.overview.conn_list);
            client.conn_set.react(&client.active_conn);
            client.conn_set.react(&queries_win);
            client.active_conn.react(&(&queries_win.content.results.overview.conn_bx, &user_state));
            client.active_conn.react(&queries_win.titlebar.exec_btn);
            client.active_conn.react(&queries_win.sidebar.schema_tree);

            client.env.react(&client.active_conn);
            client.env.react(&queries_win.content.results.workspace);
            client.env.react(&queries_win.content.editor.export_dialog);
            client.env.react(&queries_win.settings);

            queries_win.content.react(&client.active_conn);
            queries_win.content.results.overview.detail_bx.react(&client.conn_set);
            queries_win.content.results.overview.conn_bx.react(&client.conn_set);
            queries_win.content.results.overview.conn_list.react(&client.conn_set);
            queries_win.content.results.overview.conn_list.react(&client.active_conn);
            queries_win.content.results.overview.conn_bx.react(&client.active_conn);
            queries_win.content.results.workspace.react(&(&client.env, &user_state));

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

            queries_win.content.results.overview.detail_bx.react(&client.active_conn);

            queries_win.react(&queries_win.titlebar);
            queries_win.react(&client.scripts);
            queries_win.find_dialog.react(&queries_win.titlebar.main_menu);
            queries_win.find_dialog.react(&queries_win.content.editor);
            queries_win.find_dialog.react(&client.scripts);

            queries_win.window.add_action(&queries_win.find_dialog.find_action);
            queries_win.window.add_action(&queries_win.find_dialog.replace_action);
            queries_win.window.add_action(&queries_win.find_dialog.replace_all_action);

            (&queries_win.content.editor, &user_state).react(&queries_win.settings);
            (&client.env, &user_state).react(&queries_win.settings);

            // It is important to make this call to add scripts and connections
            // only after all signals have been setup, to guarantee the GUI will update
            // when the client updates.
            crate::client::set_client_state(&user_state, &client);

            queries_win.content.editor.configure(&user_state.borrow().editor);

            queries_win.window.show();
        }
    });

    application.run();

    user_state.replace_with(|user_state| {
        user_state.conns = conn_final_state.borrow().clone();
        user_state.scripts = script_final_state.borrow().clone();
        user_state.clone()
    });

    if let Some(mut path) = archiver::get_datadir(queries4::APP_ID) {
        path.push(queries4::SETTINGS_FILE);
        user_state.persist(&path.to_str().unwrap());
    } else {
        log::warn!("Unable to get datadir for state persistence");
    }

}

// println!("File search path = {:?}", theme.search_path());
// println!("Resource search path = {:?}", theme.resource_path());
// println!("Theme name = {:?}", theme.theme_name());

// theme.add_search_path("/home/diego/Software/queries/assets/icons/hicolor/scalable/actions");
// println!("{}", theme.has_icon("queries-symbolic.svg"));
// println!("{}", theme.has_icon("queries-symbolic"));
// let icon = theme.lookup_icon("queries-symbolic", &[], 16, 1, TextDirection::Ltr, IconLookupFlags::FORCE_SYMBOLIC).unwrap();
// println!("{:?}", icon);
// println!("Icon name = {:?}", icon.icon_name());
// println!("Is symbolic = {:?}", icon.is_symbolic());
// println!("File = {:?}", icon.file().unwrap().path());

// tbl_toggle.set_icon_name("queries-symbolic");
// img.set_icon_size(IconSize::Menu);
// img.set_icon_name(Some("queries-symbolic"));
// let img = Image::from_icon_name(Some("queries-symbolic"));
// let img = Image::from_paintable(Some(&IconPaintable::for_file(&gio::File::for_path("/home/diego/.local/share/org.limads.queries/icons/queries-symbolic.svg"), 16, 1)));
// let img = Image::from_file("/home/diego/.local/share/org.limads.queries/icons/queries-symbolic.png");
// let img = Image::from_paintable(Some(&icon));
// tbl_toggle.set_child(Some(&img));

