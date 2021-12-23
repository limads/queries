use gtk4::prelude::*;
use gtk4::*;
use sourceview5::prelude::*;
use glib::MainContext;
use std::rc::Rc;
use std::cell::RefCell;
use std::boxed;
use libadwaita;

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

    gtk4::init().expect("GTK initialization failed");

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
        .application_id("org.limads.queries")
        .build();

    let style_manager = libadwaita::StyleManager::default().unwrap();
    style_manager.set_color_scheme(libadwaita::ColorScheme::Default);

    application.connect_activate(move |app| {
        let display = &gdk::Display::default()
            .expect("Could not get default Display");
        let theme = IconTheme::for_display(display)
            .expect("Could not get IconTheme");
        theme.add_search_path("/home/diego/Software/queries/assets/icons");
        let window = ApplicationWindow::builder()
            .application(app)
            .title("Queries")
            .default_width(1024)
            .default_height(768)
            .build();
        let queries_win = QueriesWindow::from(window);

        /*queries_win.content.overview.conn_list.connect_changed({
            move |action| {
                match action {
                    ConnectionAction::Switch(sel) => {
                        match sel {
                            Some(row_ix) => {
                                // rows.iter().for_each(|row| row.stack.set_visible_child_name("closed") );
                                // rows[row_ix as usize].stack.set_visible_child_name("open")
                            },
                            None => {
                                // rows.iter().for_each(|row| row.stack.set_visible_child_name("closed") )
                            }
                        }
                    },
                    ConnectionAction::Add => {
                        // let n = conn_list.observe_children().n_items();
                        // let new_row = connection_row();
                        // conn_list.insert(&new_row.row, (n-1) as i32);
                        // rows.push(new_row);
                    }
                }
            }
        });*/
        queries_win.window.show();

        let connections = Connections::new();
        connections.react(&queries_win.content.overview.conn_list);
        queries_win.content.overview.detail_bx.react(&connections);
        queries_win.content.overview.conn_bx.react(&connections);
        queries_win.content.overview.conn_list.react(&connections);

        // let queries_win_c = queries_win.clone();
        // queries_win.titlebar.sidebar_toggle.connect_toggled(move|_|{
        //    queries_win_c.overlay.add_toast(&libadwaita::Toast::builder().title("This is a toast").build());
        // });

        let active_conn = ActiveConnection::new();
        queries_win.content.react(&active_conn);
        queries_win.content.overview.conn_bx.react(&active_conn);
        active_conn.react(&queries_win.content.overview.conn_bx);
        queries_win.sidebar.schema_tree.react(&active_conn);

        let workspace = Workspace::new();

        let scripts = Scripts::new();
        // scripts.react(&queries_win.content.editor);

        let opened_scripts = OpenedScripts::new();
        opened_scripts.react(&queries_win.content.editor.save_dialog);
        opened_scripts.react(&queries_win.titlebar.main_menu);

        // connections.react(queries_win.content.overview.conn_list.receiver());

        // rows.push(connection_row());
        // rows.push(connection_row());
        // conn_list.append(&rows[0].row);
        // conn_list.append(&rows[1].row);

        // overview_bx.set_margin_start(100);
        // overview_bx.set_margin_end(100);

        /*let conn_lbl = Label::new(Some("<span font_weight=\"semibold\" fgcolor=\"#3d3d3d\">Connections</span>"));
        conn_lbl.set_justify(Justification::Left);
        conn_lbl.set_halign(Align::Start);
        conn_lbl.set_use_markup(true);
        set_margins(&conn_lbl, 0, 18);

        overview_bx.append(&conn_lbl);*/

        /*let script_lbl = Label::new(Some("<span font_weight=\"semibold\" fgcolor=\"#3d3d3d\">Scripts</span>"));
        script_lbl.set_justify(Justification::Left);
        script_lbl.set_use_markup(true);
        script_lbl.set_halign(Align::Start);
        let script_list = ListBox::new();
        overview_bx.append(&script_lbl);
        overview_bx.append(&script_list);
        set_margins(&script_lbl, 0, 18);*/

        /*let action_quit = SimpleAction::new("quit", None);
        action_quit.connect_activate(clone!(@weak window => move |_, _| {
            window.close();
        }));
        window.add_action(&action_quit);
        app.set_accels_for_action("win.quit", &["<primary>W"]);*/
        /*let button = Button::builder()
            .label("Press me!")
            .action_name("win.count")
            .action_target(&1.to_variant())
            .build();*/
    });

    application.run();
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

