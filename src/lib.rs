/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use stateful::PersistentState;
use stateful::React;
use gtk4::prelude::*;
use gtk4::glib;
use gtk4::gio;

pub mod tables;

pub mod ui;

pub mod client;

pub mod server;

pub mod sql;

pub const SETTINGS_FILE : &str = "user.json";

pub const APP_ID : &str = "io.github.limads.Queries";

pub fn register_resources() {
    let bytes = glib::Bytes::from_static(include_bytes!(concat!(env!("OUT_DIR"), "/", "compiled.gresource")));
    let resource = gio::Resource::from_data(&bytes).unwrap();
    gio::resources_register(&resource);
}

fn hook_signals(
    queries_win : &ui::QueriesWindow,
    user_state : &client::SharedUserState,
    client : &client::QueriesClient
) {
    user_state.react(queries_win);

    client.conn_set.react(&queries_win.content.results.overview.conn_list);
    client.conn_set.react(&queries_win.content.results.overview.conn_bx);
    client.active_conn.react(&queries_win.content.results.overview.conn_bx);
    client.active_conn.react(&queries_win.titlebar.exec_btn);
    client.active_conn.react(&queries_win.sidebar.schema_tree);
    client.active_conn.react(&queries_win.graph_win);
    client.active_conn.react(&queries_win.builder_win);

    client.env.react(&client.active_conn);
    client.env.react(&queries_win.content.results.workspace);
    client.env.react(&queries_win.content.editor.export_dialog);
    client.env.react(&queries_win.titlebar.exec_btn);

    queries_win.content.react(&client.active_conn);
    queries_win.content.results.overview.conn_bx.react(&client.conn_set);
    queries_win.content.results.overview.conn_list.react(&client.conn_set);
    queries_win.content.results.overview.conn_list.react(&client.active_conn);
    queries_win.content.results.overview.conn_bx.react(&client.active_conn);
    queries_win.content.results.overview.sec_bx.react(&client.conn_set);
    queries_win.content.results.overview.sec_bx.react(&queries_win.settings);
    queries_win.content.results.workspace.react(&client.env);

    queries_win.sidebar.schema_tree.react(&client.active_conn);
    queries_win.sidebar.file_list.react(&client.scripts);

    client.scripts.react(queries_win);

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

    queries_win.graph_win.react(&client.active_conn);
    queries_win.builder_win.react(&client.active_conn);

    user_state.react(&client.scripts);
}

pub fn setup(
    queries_win : &ui::QueriesWindow,
    user_state : &client::SharedUserState,
    client : &client::QueriesClient
) {
    // It is critical that updating the window here is done before setting the react signal
    // below while the widgets are inert and thus do not change the state recursively.
    user_state.update(queries_win);

    // Now the window has been updated by the state, it is safe to add the callback signals.
    hook_signals(queries_win, user_state, client);

    // It is important to make this call to add scripts and connections
    // only after all signals have been setup, to guarantee the GUI will update
    // when the client updates.
    client::set_client_state(user_state, client);

    queries_win.content.editor.configure(&user_state.borrow().editor);
}
