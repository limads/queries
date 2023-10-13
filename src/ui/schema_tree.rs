/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use gtk4::*;
use gtk4::prelude::*;
use std::rc::Rc;
use std::cell::{RefCell};
use std::fs::File;
use std::io::Read;
use crate::sql::object::{DBObject, DBType};
use std::path::{Path};
use glib::{types::Type, value::{ToValue}};
use gdk_pixbuf::Pixbuf;
use std::collections::HashMap;
use gdk::{self};
use stateful::React;
use crate::client::ActiveConnection;
use crate::ui::PackedImageLabel;
use gtk4::glib;
use serde_json;
use crate::ui::NamedBox;
use std::io::Write;
use crate::client::ActiveConnectionAction;
use crate::ui::ConnectionBox;

// The TreeView to the bottom left region that shows the database schema and allows 
// interactions with tables, views and functions.
#[derive(Clone, Debug)]
pub struct SchemaTree {
    pub tree_view : TreeView,
    model : TreeStore,
    type_icons : Rc<HashMap<DBType, Pixbuf>>,
    tbl_icon : Pixbuf,
    _clock_icon : Pixbuf,
    schema_icon : Pixbuf,
    fn_icon : Pixbuf,
    view_icon : Pixbuf,
    key_icon : Pixbuf,
    pub schema_popover : PopoverMenu,
    _scroll : ScrolledWindow,
    pub bx : Box,
    pub query_action : gio::SimpleAction,
    pub insert_action : gio::SimpleAction,
    pub import_action : gio::SimpleAction,
    pub call_action : gio::SimpleAction,
    pub report_action : gio::SimpleAction,
    pub form : super::Form,
    pub import_dialog : ImportDialog,
    pub report_dialog : ReportDialog,
    pub report_export_dialog : filecase::SaveDialog
}

// TODO views with a homonimous table are not being shown at the schema tree.

const ALL_TYPES : [DBType; 15] = [
    DBType::Bool,
    DBType::I16,
    DBType::I32,
    DBType::I64,
    DBType::F32,
    DBType::F64,
    DBType::Numeric,
    DBType::Text,
    DBType::Date,
    DBType::Time,
    DBType::Bytes,
    DBType::Json,
    DBType::Xml,
    DBType::Array,
    DBType::Unknown
];

impl SchemaTree {

    pub fn build() -> Self {

        let menu = gio::Menu::new();
        menu.append(Some("Query"), Some("win.query"));
        menu.append(Some("Report"), Some("win.report"));
        menu.append(Some("Insert"), Some("win.insert"));
        menu.append(Some("Import"), Some("win.import"));
        menu.append(Some("Call"), Some("win.call"));

        let schema_popover = PopoverMenu::builder().menu_model(&menu).build();

        let is_dark = libadwaita::StyleManager::default().is_dark();
        let type_icons = load_type_icons(is_dark);

        let [tbl_icon, db_icon, fn_icon, clock_icon, view_icon, key_icon] = if is_dark {
            ["table-white", "db-white", "fn-white", "clock-app-white", "view-white", "key-white"]
        } else {
            ["table-symbolic", "db-symbolic", "fn-dark-symbolic", "clock-app-symbolic", "view-symbolic", "key-symbolic"]
        };
        let mut icons = filecase::load_icons_as_pixbufs_from_resource(
            "/io/github/limads/queries",
            &[tbl_icon, db_icon, fn_icon, clock_icon, view_icon, key_icon]
        ).unwrap();
        
        let schema_icon = icons.remove(db_icon).unwrap();
        let fn_icon = icons.remove(fn_icon).unwrap();
        let clock_icon = icons.remove(clock_icon).unwrap();
        let view_icon = icons.remove(view_icon).unwrap();
        let key_icon = icons.remove(key_icon).unwrap();
        let tbl_icon = icons.remove(tbl_icon).unwrap();
        
        let tree_view = TreeView::new();
        tree_view.set_valign(Align::Fill);
        tree_view.set_vexpand(true);
        schema_popover.set_position(PositionType::Right);
        schema_popover.set_autohide(true);
        let model = configure_tree_view(&tree_view);

        let title = PackedImageLabel::build("db-symbolic", "Database");
        title.bx.set_vexpand(false);
        title.bx.set_valign(Align::Start);
        super::set_border_to_title(&title.bx);
        let bx = Box::new(Orientation::Vertical, 0);

        let scroll = ScrolledWindow::new();
        scroll.set_vexpand(true);
        scroll.set_valign(Align::Fill);
        scroll.set_child(Some(&tree_view));
        bx.append(&title.bx);
        bx.append(&scroll);

        // Popovers must always have a parent. Currently (4.5) GTK will
        // segfault when manipulating a Popover without a parent.
        // Setting the Treeview as the parent makes the popover unresponsive.
        // schema_popover.set_parent(&tree_view);
        schema_popover.set_parent(&scroll);

        let gesture_click = GestureClick::builder().build();
        gesture_click.set_button(gdk::BUTTON_SECONDARY);
        tree_view.add_controller(gesture_click.clone());
        gesture_click.connect_pressed({
            let schema_popover = schema_popover.clone();
            let tree_view = tree_view.clone();
            let _scroll = scroll.clone();
            move |_gesture, _n_press, x, y| {
                if let Some((Some(opt_path), Some(opt_col), _, _)) = tree_view.path_at_pos(x as i32, y as i32) {
                    let area = tree_view.cell_area(Some(&opt_path), Some(&opt_col));
                    schema_popover.set_pointing_to(Some(&area));
                    schema_popover.popup();
                }
            }
        });

        let form = super::Form::new();
        let query_action = gio::SimpleAction::new_stateful("query", None, &String::from("").to_variant());
        let insert_action = gio::SimpleAction::new_stateful("insert", None, &String::from("").to_variant());
        let import_action = gio::SimpleAction::new_stateful("import", None, &String::from("").to_variant());
        let call_action = gio::SimpleAction::new_stateful("call", None, &String::from("").to_variant());
        let report_action = gio::SimpleAction::new_stateful("report", None, &String::from("").to_variant());
        query_action.set_enabled(false);
        insert_action.set_enabled(false);
        import_action.set_enabled(false);
        call_action.set_enabled(false);
        report_action.set_enabled(false);
        insert_action.connect_activate({
            let form = form.clone();
            move |action, _| {
                if let Some(state) = action.state() {
                    let s = state.get::<String>().unwrap();
                    if !s.is_empty() {
                        let obj : DBObject = serde_json::from_str(&s[..]).unwrap();                        
                        form.update_from_table(&obj);
                        form.dialog.show();
                    }
                }
            }
        });
        call_action.connect_activate({
            let form = form.clone();
            move |action, _| {
                if let Some(state) = action.state() {
                    let s = state.get::<String>().unwrap();
                    if !s.is_empty() {
                        let obj : DBObject = serde_json::from_str(&s[..]).unwrap();
                            form.update_from_function(&obj);
                            form.dialog.show();
                    }
                }
            }
        });
        form.btn_cancel.connect_clicked({
            let dialog = form.dialog.clone();
            let entries = form.entries.clone();
            move |_| {
                dialog.close();
                entries.iter().for_each(|e| e.set_text("") );
            }
        });
        form.dialog.connect_close({
            let insert_action = insert_action.clone();
            let call_action = call_action.clone();
            let entries = form.entries.clone();
            move |_| {
                insert_action.set_state(&String::new().to_variant());
                call_action.set_state(&String::new().to_variant());
                entries.iter().for_each(|e| e.set_visible(false) );
            }
        });
        let import_dialog = ImportDialog::build();
        import_action.connect_activate({
            let import_dialog = import_dialog.clone();
            move |_, _| {
                import_dialog.dialog.show();
            }
        });
        let report_dialog = ReportDialog::build();
        report_action.connect_activate({
            let report_dialog = report_dialog.clone();
            move |_, _| {
                report_dialog.dialog.show();
            }
        });
        let report_export_dialog = filecase::SaveDialog::build(&["*.html"]);
        report_export_dialog.dialog.connect_response({
            let rendered_content = report_dialog.rendered_content.clone();
            move |dialog, resp| {
                match resp {
                    ResponseType::Accept => {
                        if let Some(path) = dialog.file().and_then(|f| f.path() ) {
                            if let Ok(mut f) = File::create(path) {
                                let mut rendc = rendered_content.borrow_mut();
                                if let Some(cont) = rendc.take() {
                                    if cont.is_empty() {
                                        eprintln!("Warning: Content to be rendered is empty");
                                    }
                                    if let Err(e) = f.write_all(cont.as_bytes()) {
                                        eprintln!("{}", e);
                                    }
                                } else {
                                    eprintln!("No content to be rendered");
                                }
                            } else {
                                eprintln!("Unable to create/write to file");
                            }
                        }
                    },
                    _ => { }
                }
            }
        });

        Self {
            tree_view,
            model,
            type_icons,
            tbl_icon,
            schema_icon,
            fn_icon,
            _clock_icon : clock_icon,
            view_icon,
            key_icon,
            schema_popover,
            bx,
            _scroll : scroll,
            query_action,
            insert_action,
            import_action,
            report_action,
            call_action,
            form,
            import_dialog,
            report_dialog,
            report_export_dialog,
        }
    }

    fn grow_tree(&self, model : &TreeStore, parent : Option<&TreeIter>, obj : DBObject) {
        match obj {
            DBObject::Schema{ name, children } => {
                let schema_iter = model.append(parent);
                model.set(&schema_iter, &[(0, &self.schema_icon), (1, &name)]);
                for child in children {
                    self.grow_tree(&model, Some(&schema_iter), child);
                }
            },
            DBObject::Table{ name, cols, rels, .. } => {
                let tbl_iter = model.append(parent);
                model.set(&tbl_iter, &[(0, &self.tbl_icon), (1, &name.to_value())]);
                for c in cols {
                    let col_iter = model.append(Some(&tbl_iter));
                    let opt_rel = rels.iter().find(|rel| &rel.src_col[..] == &c.name[..] );
                    let is_fk = opt_rel.is_some();

                    /* The empty schema is used for all sqlite tables. */
                    let name : String = if let Some(rel) = opt_rel {
                        let tgt_schema = if &rel.tgt_schema[..] == crate::server::PG_PUB || rel.tgt_schema.is_empty() {
                            format!("")
                        } else {
                            format!("{}.", rel.tgt_schema)
                        };
                        format!("{} ({}{})", c.name, tgt_schema, rel.tgt_tbl )
                    } else {
                        format!("{}", c.name)
                    };
                    let icon = if is_fk || c.is_pk {
                        &self.key_icon
                    } else {
                        &self.type_icons[&c.ty]
                    };
                    model.set(&col_iter, &[(0, icon), (1, &name.to_value())]);
                }
            },
            DBObject::Function { name, .. } => {
                let fn_iter = model.append(parent);
                let sig = format!("{}", name);
                model.set(&fn_iter, &[(0, &self.fn_icon.to_value()), (1, &sig.to_value())]);
            },
            DBObject::View { name, cols, .. } => {
                let view_iter = model.append(parent);
                model.set(&view_iter, &[(0, &self.view_icon.to_value()), (1, &name.to_value())]);
                for c in cols.iter() {
                    let col_iter = model.append(Some(&view_iter));
                    let icon = &self.type_icons[&c.ty];
                    model.set(&col_iter, &[(0, icon), (1, &c.name.to_value())]);
                }
            }
        }
    }

    pub fn repopulate(&self, objs : Vec<DBObject>) {
        self.model.clear();
        let _is_pg = false;
        for obj in objs {
            self.grow_tree(&self.model, None, obj);
        }
        self.model.foreach(|_model, path, _iter| {
            if path.depth() == 1 {
                self.tree_view.expand_row(path, false);
            }
            false
        });
    }

    pub fn clear(&self) {
        self.model.clear();
    }

}

impl React<ConnectionBox> for SchemaTree {

    fn react(&self, conn_bx : &ConnectionBox) {
        let schema_tree = self.clone();
        conn_bx.remote_switch.connect_state_set(move |switch, _| {
            clear_on_switch(&switch, &schema_tree);
            glib::signal::Propagation::Proceed
        });
        let schema_tree = self.clone();
        conn_bx.local_switch.connect_state_set(move |switch, _| {
            clear_on_switch(&switch, &schema_tree);
            glib::signal::Propagation::Proceed
        });
    }
    
}

fn clear_on_switch(switch : &gtk4::Switch, schema_tree : &SchemaTree) {
    if switch.is_active() {
        schema_tree.repopulate(vec![DBObject::Schema {
            name : String::from("Connecting..."),
            children : Vec::new()
        }]);
    } else {
        schema_tree.clear();
    }
}

impl React<ActiveConnection> for SchemaTree {

    fn react(&self, conn : &ActiveConnection) {
        let schema_tree = self.clone();
        conn.connect_db_connected(move |(_conn_info, db_info)| {
            if let Some(db_info) = db_info {
                schema_tree.repopulate(db_info.schema);
            } else {
                schema_tree.repopulate(vec![DBObject::Schema { name : format!("Catalog unavailable"), children : Vec::new() }]);
            }
        });
        conn.connect_db_disconnected({
            let schema_tree = self.clone();
            move |_| {
                schema_tree.clear();
            }
        });
        conn.connect_schema_update({
            let schema_tree = self.clone();
            move |info| {
                if let Some(info) = info {
                    schema_tree.repopulate(info);
                }
            }
        });
        conn.connect_object_selected({
            let insert_action = self.insert_action.clone();
            let query_action = self.query_action.clone();
            let call_action = self.call_action.clone();
            let import_action = self.import_action.clone();
            let report_action = self.report_action.clone();
            move |opt_obj| {
                match &opt_obj {
                    Some(DBObject::Table { .. }) => {
                        let s = serde_json::to_string(&opt_obj.unwrap()).unwrap().to_variant();
                        for action in [&insert_action, &query_action, &import_action, &report_action] {
                            action.set_enabled(true);
                            action.set_state(&s);
                        }
                        call_action.set_enabled(false);
                        call_action.set_state(&String::new().to_variant());
                    },
                    Some(DBObject::View { .. }) => {
                        let s = serde_json::to_string(&opt_obj.unwrap()).unwrap().to_variant();
                        query_action.set_enabled(true);
                        report_action.set_enabled(true);
                        query_action.set_state(&s);
                        for action in [&insert_action, &import_action, &call_action] {
                            action.set_enabled(false);
                            action.set_state(&String::new().to_variant());
                        }
                    },
                    Some(DBObject::Schema { .. }) => {
                        for action in [&insert_action, &query_action, &import_action, &call_action, &report_action] {
                            action.set_enabled(false);
                            action.set_state(&String::new().to_variant());
                        }
                    },
                    Some(DBObject::Function { .. }) => {
                        let s = serde_json::to_string(&opt_obj.unwrap()).unwrap().to_variant();
                        for action in [&insert_action, &query_action, &import_action, &report_action] {
                            action.set_enabled(false);
                            action.set_state(&String::new().to_variant());
                        }
                        call_action.set_enabled(true);
                        call_action.set_state(&s);
                    },
                    _ => {
                        for action in [&insert_action, &query_action, &import_action, &call_action, &report_action] {
                            action.set_enabled(false);
                            action.set_state(&String::new().to_variant());
                        }
                    }
                }
            }
        });
        conn.connect_single_query_result({
            let files = self.report_dialog.files.clone();
            let rendered_content = self.report_dialog.rendered_content.clone();
            let export_dialog = self.report_export_dialog.dialog.clone();
            let send = conn.sender().clone();
            let transpose_switch = self.report_dialog.transpose_switch.clone();
            let png_switch = self.report_dialog.png_switch.clone();
            let null_entry = self.report_dialog.null_entry.clone();
            move |tbl| {
                let fls = files.borrow();
                let mut rendered_content = rendered_content.borrow_mut();
                if let Some(template_path) = fls.0.get(fls.1) {
                    if let Ok(mut f) = File::open(template_path) {
                        let mut template_content = String::new();
                        if let Err(_e) = f.read_to_string(&mut template_content) {
                            send.send(ActiveConnectionAction::Error("Could not read template content".to_owned())).unwrap();
                            return;
                        }
                        if template_content.trim().is_empty() {
                            send.send(ActiveConnectionAction::Error("Empty template file".to_owned())).unwrap();
                            return;
                        }
                        let null_sub = null_entry.text();
                        let null = if null_sub.trim().is_empty() {
                            None
                        } else {
                            Some(null_sub.as_str())
                        };
                        let transpose = transpose_switch.is_active();
                        let png = png_switch.is_active();
                        match crate::tables::report::html::substitute_html(&tbl, &template_content, null, transpose, png) {
                            Ok(complete_report) => {
                                *rendered_content = Some(complete_report);
                                export_dialog.show();
                            },
                            Err(e) => {
                                *rendered_content = None;
                                send.send(ActiveConnectionAction::Error(format!("{e}"))).unwrap();
                            }
                        }
                    } else {
                        *rendered_content = None;
                        send.send(ActiveConnectionAction::Error("Invalid template path".to_owned())).unwrap();
                    }
                } else {
                    eprintln!("No file selected");
                }
            }
        });
    }

}

pub fn load_type_icons(is_dark : bool) -> Rc<HashMap<DBType, Pixbuf>> {
    let mut names = Vec::new();
    for ty in ALL_TYPES.iter() {
        names.push(super::get_type_icon_name(ty, is_dark));
    }
    let pixbufs = filecase::load_icons_as_pixbufs_from_resource("/io/github/limads/queries", &names[..]).unwrap();
    let mut type_icons = HashMap::new();
    for ty in ALL_TYPES.iter() {
        type_icons.insert(ty.clone(), pixbufs[super::get_type_icon_name(ty, is_dark)].clone());
    }
    Rc::new(type_icons)
}

pub fn configure_tree_view(tree_view : &TreeView) -> TreeStore {
    let model = TreeStore::new(&[Pixbuf::static_type(), Type::STRING]);
    tree_view.set_model(Some(&model));
    let pix_renderer = CellRendererPixbuf::new();
    pix_renderer.set_padding(6, 6);
    let txt_renderer = CellRendererText::new();

    let pix_col = TreeViewColumn::new();
    pix_col.pack_start(&pix_renderer, false);
    pix_col.add_attribute(&pix_renderer, "pixbuf", 0);
    // pix_col.add_attribute(&pix_renderer, "gicon", 0);

    let txt_col = TreeViewColumn::new();
    txt_col.pack_start(&txt_renderer, true);
    txt_col.add_attribute(&txt_renderer, "text", 1);

    tree_view.append_column(&pix_col);
    tree_view.append_column(&txt_col);
    tree_view.set_show_expanders(true);
    tree_view.set_can_focus(false);
    tree_view.set_has_tooltip(false);
    tree_view.set_headers_visible(false);

    model
}

#[derive(Debug, Clone)]
pub struct ImportDialog {
    pub dialog : FileChooserDialog
}

impl ImportDialog {

    pub fn build() -> Self {
        let dialog = FileChooserDialog::new(
            Some("Import table"),
            None::<&Window>,
            FileChooserAction::Open,
            &[("Cancel", ResponseType::None), ("Open", ResponseType::Accept)]
        );
        dialog.connect_response(move |dialog, resp| {
            match resp {
                ResponseType::Reject | ResponseType::Accept | ResponseType::Yes | ResponseType::No |
                ResponseType::None | ResponseType::DeleteEvent => {
                    dialog.close();
                },
                _ => { }
            }
        });
        super::configure_dialog(&dialog, true);
        let filter = FileFilter::new();
        filter.add_pattern("*.csv");
        dialog.set_filter(&filter);
        Self { dialog }
    }

}

#[derive(Debug, Clone)]
pub struct ReportDialog {
    pub dialog : Dialog,
    _template_combo : ComboBoxText,
    _list : ListBox,
    pub btn_gen : Button,
    rendered_content : Rc<RefCell<Option<String>>>,
    pub files : Rc<RefCell<(Vec<String>, usize)>>,
    null_entry : Entry,
    transpose_switch : Switch,
    png_switch : Switch
}

impl ReportDialog {

    pub fn build() -> Self {
        let dialog = Dialog::new();
        dialog.set_title(Some("Report generation"));
        crate::ui::configure_dialog(&dialog, true);
        let template_combo = ComboBoxText::new();
        template_combo.set_margin_start(12);
        let list = ListBox::new();
        crate::ui::settings::configure_list(&list);
        list.append(&NamedBox::new("Template", Some("Save HTML templates under ~/Templates\nto load them here"), template_combo.clone()).bx);

        let null_entry = Entry::new();
        null_entry.set_placeholder_text(Some("Null"));
        list.append(&NamedBox::new("Null string", Some("String to use when replacing\nnull values"), null_entry.clone()).bx);

        let transpose_switch = Switch::new();
        list.append(&NamedBox::new("Transpose tables", Some("Arrange JSON key-value pairs horizontally instead of vertically"), transpose_switch.clone()).bx);

        crate::ui::set_all_not_selectable(&list);
        
        // TODO append when feature is ready.
        let png_switch = Switch::new();
        // list.append(&NamedBox::new("Rasterize graphics", Some("Embed graphics as PNG (base64-encoded)\ninstead of Svg (vectorized)"), png_switch.clone()).bx);
        
        // Figure format (Svg / Png (embedded)
        // ( ) Include table headers
        // ( ) Include row count.

        let btn_gen = Button::builder().label("Generate").build();
        btn_gen.set_sensitive(false);
        btn_gen.style_context().add_class("pill");
        btn_gen.style_context().add_class("suggested-action");
        btn_gen.set_hexpand(false);
        btn_gen.set_halign(Align::Center);
        let bx = Box::new(Orientation::Vertical, 0);
        bx.append(&list);
        bx.append(&btn_gen);
        dialog.set_child(Some(&bx));
        let files = Rc::new(RefCell::new((Vec::new(), 0)));

        super::set_margins(&btn_gen, 64,  16);
        super::set_margins(&bx, 32,  32);

        dialog.connect_show({
            let template_combo = template_combo.clone();
            let files = files.clone();
            move |_| {
                let mut files = files.borrow_mut();
                files.1 = 0;
                if let Ok(entries) = std::fs::read_dir("/home/diego/Templates") {
                    for f in entries.filter_map(|e| e.ok() ) {
                        if let Some(ext) = f.path().extension() {
                            let s = f.path().to_str().unwrap().to_string();
                            if ext == Path::new("html") {
                                template_combo.append(Some(&s), &s);
                                files.0.push(s);
                            }
                        }
                    }
                }
            }
        });
        template_combo.connect_changed({
            let files = files.clone();
            let btn_gen = btn_gen.clone();
            move|combo| {
                let mut files = files.borrow_mut();
                if let Some(id) = combo.active_id() {
                    if let Some(ix) = files.0.iter().position(|f| &f[..] == id ) {
                        files.1 = ix;
                        btn_gen.set_sensitive(true);
                    } else {
                        eprintln!("No file with {:?}", id);
                    }
                } else {
                    btn_gen.set_sensitive(false);
                }
            }
        });
        Self {
            dialog,
            _list : list,
            _template_combo : template_combo,
            btn_gen,
            files,
            rendered_content : Rc::new(RefCell::new(None)),
            null_entry,
            transpose_switch,
            png_switch
        }
    }

}

/*// According to the GTK 3-4 migration guide, popovers can't be attached to random
// widgets (removal of Popover::set_relative_to), and we must create a custom widget
// to do that. This is an initial implementation of a TreeView with an associated popover.
// For now, it is possible to append popover to a TreeView just by calling popover.set_parent(tree_view)
// and updating the popover position by querying the cell position. If this solution stops working
// with future GTK versions, this implementation can be developed further.
mod imp {

    use gtk4::*;
    use gtk4::prelude::*;
    use gtk4::glib;
    use gtk4::subclass::prelude::*;
    use gtk4::gdk;
    use std::cell::RefCell;

    #[derive(Default)]
    pub struct PopoverTreeView(pub RefCell<Option<Popover>>);

    // The central trait for subclassing a GObject
    #[glib::object_subclass]
    impl ObjectSubclass for PopoverTreeView {

        const NAME: &'static str = "PopoverTreeView";

        type Type = super::PopoverTreeView;

        type ParentType = gtk4::TreeView;

    }

    impl ObjectImpl for PopoverTreeView {

        fn constructed(&self, obj: &Self::Type) {
            self.parent_constructed(obj);

            // let popover = Popover::new();
            // popover.set_parent(&obj.clone());
            // popover.present();
            // popover.popup();
            // self.0.replace(popover);
        }

    }

    impl WidgetImpl for PopoverTreeView {

        fn realize(&self, widget : &Self::Type) {
            self.parent_realize(widget);
            // popover.realize();

            // let alloc = widget.allocation();

            // self.0.
            // self.0.set_parent(&self);
            // self.0.realize();
        }

        fn size_allocate(
            &self,
            widget: &Self::Type,
            width: i32,
            height: i32,
            baseline: i32
        ) {
            self.parent_size_allocate(widget, width, height, baseline);

            if let Ok(mut opt_popover) = self.0.try_borrow_mut() {
                if opt_popover.is_none() {
                    let popover = Popover::new();
                    popover.set_position(PositionType::Right);
                    let bx_pop = Box::new(Orientation::Vertical, 0);
                    let btn1 = Button::with_label("Query");
                    btn1.style_context().add_class("flat");

                    use std::rc::Rc;
                    use std::cell::RefCell;
                    let n = Rc::new(RefCell::new(0usize));
                    let motion = EventControllerMotion::new();

                    motion.connect_contains_pointer_notify({
                        let btn1 = btn1.clone();
                        let popover = popover.clone();
                        move|motion| {
                            btn1.style_context().add_class("raised");
                        }
                    });
                    motion.connect_leave({
                        let btn1 = btn1.clone();
                        move|_| {
                            btn1.style_context().remove_class("raised");
                        }
                    });
                    //motion.connect_motion(move |motion, x, y| {
                    //});
                    btn1.add_controller(&motion);

                    let btn2 = Button::with_label("Insert");
                    btn2.style_context().add_class("flat");
                    bx_pop.append(&btn1);
                    bx_pop.append(&btn2);
                    popover.set_autohide(true);
                    popover.set_child(Some(&bx_pop));
                    let gesture_click = GestureClick::builder().build();
                    gesture_click.set_button(gdk::BUTTON_SECONDARY);
                    widget.add_controller(&gesture_click);
                    gesture_click.connect_pressed({
                        let popover = popover.clone();
                        let tree_view = widget.clone();
                        move |gesture, n_press, x, y| {
                            if let Some((Some(opt_path), Some(opt_col), _, _)) = tree_view.path_at_pos(x as i32, y as i32) {
                                let area = tree_view.cell_area(Some(&opt_path), Some(&opt_col));
                                popover.set_pointing_to(&area);
                                popover.popup();
                            }
                        }
                    });
                    popover.set_parent(&widget.clone());
                    *opt_popover = Some(popover);
                }
            }
            // let popover = self.0.borrow().clone().unwrap();
            // popover.set_pointing_to(&gdk::Rectangle { x : 0, y : 0, width : width / 2, height : height / 2 });
            // popover.present();
            // popover.popup();
        }

    }

    impl TreeViewImpl for PopoverTreeView {

        fn test_collapse_row(&self, tree_view: &Self::Type, iter: &TreeIter, path: &TreePath) -> bool {
            // self.parent_test_collapse_row(tree_view, iter, path)
            false
        }

        fn test_expand_row(&self, tree_view: &Self::Type, iter: &TreeIter, path: &TreePath) -> bool {
            // self.parent_test_expand_row(tree_view, iter, path)
            false
        }

    }

}

glib::wrapper! {
    pub struct PopoverTreeView(ObjectSubclass<imp::PopoverTreeView>)
        @extends gtk4::TreeView, gtk4::Widget,
        @implements gtk4::Accessible, gtk4::Buildable, gtk4::ConstraintTarget, gtk4::Scrollable;
}

impl PopoverTreeView {
    pub fn new() -> Self {
        glib::Object::new(&[])
            .expect("Failed to create `CustomButton`.")
    }
}*/

