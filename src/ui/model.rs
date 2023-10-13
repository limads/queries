use std::ffi::{CStr, CString};
use gdk_pixbuf::{Pixbuf,PixbufLoader};
use std::io::BufReader;
use std::collections::VecDeque;
use std::error::Error;
use gtk4::*;
use gtk4::prelude::*;
use std::ffi::{c_int, c_char};
use std::ptr;
use stateful::React;
use crate::sql::object::DBInfo;
use std::cell::RefCell;
use std::rc::Rc;
use crate::client::ActiveConnection;
use super::plots::{LabeledColorBtn, LabeledFontBtn};
use gdk::RGBA;
use crate::ui::PackedImageLabel;
use std::collections::HashSet;
use crate::sql::object::DBObject;
use filecase::SaveDialog;

#[derive(Clone, Debug)]
pub struct ModelWindow {
    pub dialog : Dialog,
    pub img : Picture,
    pub list : ListBox,
    model : Rc<RefCell<Option<DBInfo>>>,
    btn_update : Button,
    save_dialog : SaveDialog,
    btn_export : Button,
    must_draw : Rc<RefCell<HashSet<(String, String)>>>
}

#[derive(Clone, Debug)]
pub struct ModelDesign {
    pub background : String,
    pub node_fill : String,
    pub font_name : String,
    pub font_size : String
}

pub fn color_literal_rgb_lower_opaque(color_btn : &ColorButton) -> String {
    let rgba = color_btn.rgba();
    let red = (rgba.red() * 255.0) as u8;
    let green = (rgba.green() * 255.0) as u8;
    let blue = (rgba.blue() * 255.0) as u8;
    format!("'#{:02x}{:02x}{:02x}'", red, green, blue)
}

pub fn color_literal_rgb_lower(color_btn : &ColorButton) -> String {
    let rgba = color_btn.rgba();
    let red = (rgba.red() * 255.0) as u8;
    let green = (rgba.green() * 255.0) as u8;
    let blue = (rgba.blue() * 255.0) as u8;
    let alpha = (rgba.alpha() * 255.0) as u8;
    format!("#{:02x}{:02x}{:02x}{:02x}", red, green, blue, alpha)
}

fn read_design(color_btn : &ColorButton, font_btn : &FontButton, transparent : bool) -> ModelDesign {
    let background = if transparent {
        "#00000000".to_string()
    } else {
        "#fafafa".to_string()
    };
    let node_fill = color_literal_rgb_lower(&color_btn);
    let font = papyri::render::text::FontData::new_from_string(&super::plots::font_literal(&font_btn));
    ModelDesign {
        background,
        node_fill,
        font_name : font.font_family.clone(),
        font_size : format!("{}", font.font_size as f32)
    }
}

const NO_DIAGRAM : &str = r##"
strict graph { bgcolor="#fafafa"; }
"##;

impl ModelWindow {

    pub fn build() -> Self {
        let dialog = Dialog::new();
        dialog.set_width_request(1200);
        dialog.set_height_request(800);
        super::configure_dialog(&dialog, false);
        dialog.set_title(Some("Database model"));
        let img = Picture::new();
        img.set_can_shrink(false);
        //img.set_halign(Align::Fill);
        //img.set_valign(Align::Fill);
        img.set_halign(Align::Center);
        img.set_valign(Align::Center);
        img.set_vexpand(true);
        img.set_hexpand(true);

        // TODO this might slow down startup..
        render_to_image(&img, NO_DIAGRAM);

        let bx_outer = Box::new(Orientation::Horizontal, 0);
        let scroll_left = ScrolledWindow::new();

        let provider = CssProvider::new();
        let css = if libadwaita::StyleManager::default().is_dark() {
            "* { border-right : 1px solid #454545; } "
        } else {
            "* { border-right : 1px solid #d9dada; } "
        };
        provider.load_from_data(css);
        scroll_left.style_context().add_provider(&provider, 800);

        scroll_left.set_width_request(320);
        let scroll_right = ScrolledWindow::new();
        let list = ListBox::new();

        scroll_left.set_child(Some(&list));
        let row_cols = ListBoxRow::new();
        row_cols.set_activatable(false);
        row_cols.set_selectable(false);

        let must_draw = Rc::new(RefCell::new(HashSet::new()));
        let btn_color = LabeledColorBtn::build("Background color", &RGBA::WHITE);
        let btn_font = LabeledFontBtn::build("Font");

        /* Unless a monospaced font is used, the column name/types will
        not be aligned, since we use whitespace to represent them */
        btn_font.btn.set_filter_func(|family, face| {
            format!("{family}").ends_with("Mono")
        });

        btn_font.btn.set_font("Liberation Mono 12");
        btn_color.btn.set_hexpand(true);
        btn_color.btn.set_halign(Align::End);
        btn_font.btn.set_hexpand(true);
        btn_font.btn.set_halign(Align::End);

        let row_color = ListBoxRow::new();
        let row_text = ListBoxRow::new();
        row_color.set_child(Some(&btn_color.bx));
        row_text.set_child(Some(&btn_font.bx));

        let row_lbl_design = ListBoxRow::new();
        let lbl_design = super::PackedImageLabel::build("larger-brush-symbolic", "Design");
        row_lbl_design.set_child(Some(&lbl_design.bx));

        let row_lbl_tbls = ListBoxRow::new();
        let lbl_tbls = super::PackedImageLabel::build("table-symbolic", "Tables");
        row_lbl_tbls.set_child(Some(&lbl_tbls.bx));
        lbl_tbls.bx.set_margin_top(24);

        for r in [&row_lbl_design, &row_color, &row_text, &row_lbl_tbls] {
            list.append(r);
            r.set_activatable(false);
            r.set_selectable(false);
        }

        scroll_right.set_child(Some(&img));
        bx_outer.append(&scroll_left);

        let bx_btns = Box::new(Orientation::Horizontal, 16);
        bx_btns.set_margin_top(18);
        bx_btns.set_margin_bottom(18);
        bx_btns.set_halign(Align::Center);

        let btn_update = Button::builder().label("Update").build();
        let btn_export = Button::builder().label("Export").build();
        btn_update.set_sensitive(false);
        btn_export.set_sensitive(false);
        bx_btns.append(&btn_export);
        bx_btns.append(&btn_update);
        btn_update.style_context().add_class("pill");
        btn_export.style_context().add_class("pill");
        btn_update.style_context().add_class("suggested-action");

        let bx_inner = Box::new(Orientation::Vertical, 0);
        bx_inner.set_vexpand(true);
        bx_inner.set_valign(Align::Fill);
        bx_inner.append(&scroll_right);
        bx_inner.append(&bx_btns);
        dialog.set_child(Some(&bx_outer));
        bx_outer.append(&bx_inner);

        let model = Rc::new(RefCell::new(None::<DBInfo>));
        btn_update.connect_clicked({
            let model = model.clone();
            let img = img.clone();
            let must_draw = must_draw.clone();
            let btn_export = btn_export.clone();
            let btn_color = btn_color.btn.clone();
            let btn_font = btn_font.btn.clone();
            move |_| {
                if let Some(mut model) = model.borrow().clone() {
                    let must_draw = must_draw.borrow();
                    filter_model(&mut model.schema, &must_draw);
                    let design = read_design(&btn_color, &btn_font, false);
                    let dia = model.diagram(&design);
                    render_to_image(&img, &dia);
                    btn_export.set_sensitive(true);
                } else {
                    render_to_image(&img, NO_DIAGRAM);
                }
            }
        });

        let save_dialog = SaveDialog::build(&[]);
        save_dialog.dialog.set_transient_for(Some(&dialog));
        save_dialog.dialog.set_current_name("Untitled.png");

        btn_export.connect_clicked({
            let save_dialog = save_dialog.clone();
            move |_| {
                save_dialog.dialog.show();
            }
        });
        save_dialog.dialog.connect_response({
            let model = model.clone();
            let must_draw = must_draw.clone();
            let btn_color = btn_color.btn.clone();
            let btn_font = btn_font.btn.clone();
            move |dialog, resp| {
                match resp {
                    ResponseType::Accept => {
                        if let Some(path) = dialog.file().and_then(|f| f.path() ) {
                            if let Some(path) = path.to_str() {
                                if let Some(mut model) = model.borrow().clone() {
                                    let must_draw = must_draw.borrow();
                                    filter_model(&mut model.schema, &must_draw);
                                    let design = read_design(&btn_color, &btn_font, true);
                                    let dia = model.diagram(&design);
                                    render_to_path(path, &dia);
                                }
                            }
                        }
                    },
                    _ => { }
                }
            }
        });
        Self { dialog, img, list, model, btn_update, btn_export, must_draw, save_dialog }
    }

}

fn filter_model(model : &mut Vec<DBObject>, must_draw : &HashSet<(String, String)>) {
    for i in (0..model.len()).rev() {
        match &mut model[i] {
            DBObject::Schema { ref mut children, .. } => {
                filter_model(children, must_draw);
            },
            DBObject::Table { schema, name, .. } => {
                let has_entry = must_draw.iter().find(|k| &k.0[..] == &schema[..] && &k.1[..] == &name[..] ).is_some();
                if !has_entry {
                    model.remove(i);
                }
            },
            _ => { }
        }
    }
}

fn clear_tables_at_list(list : &ListBox) {
    while let Some(r) = list.row_at_index(4) {
        list.remove(&r);
    }
}

fn add_to_list(list : &ListBox, obj : &DBObject, must_draw : &Rc<RefCell<HashSet<(String,String)>>>) {
    let row = ListBoxRow::new();
    row.set_selectable(false);
    row.set_activatable(false);
    match obj {
        DBObject::Schema { name, children, .. } => {
            if name.starts_with("Views (") || name.starts_with("Functions (") {
                return;
            }
            let exp = Expander::with_mnemonic(&name);
            let lbl = PackedImageLabel::build("db-symbolic", &name);
            lbl.bx.set_height_request(32);
            exp.set_label_widget(Some(&lbl.bx));
            let inner_list = ListBox::new();
            for child in children {
                add_to_list(&inner_list, &child, &must_draw);
            }
            exp.set_child(Some(&inner_list));
            row.set_child(Some(&exp));
        },
        DBObject::Table { schema, name, .. } => {
            /* The empty schema is used for all sqlite tables */
            let txt = if schema.is_empty() {
                name.clone()
            } else {
                format!("{schema}.{name}")
            };
            let btn = CheckButton::with_label(&txt);
            let must_draw = must_draw.clone();
            let schema = schema.clone();
            let name = name.clone();
            btn.connect_toggled(move |b| {
                let mut must_draw = must_draw.borrow_mut();
                if b.is_active() {
                    must_draw.insert((schema.clone(), name.clone()));
                } else {
                    must_draw.remove(&(schema.clone(), name.clone()));
                }
            });
            row.set_child(Some(&btn));
        },
        _ => { }
    }
    list.append(&row);
}

impl React<ActiveConnection> for ModelWindow {

    fn react(&self, conn : &ActiveConnection) {
        let model = self.model.clone();
        let must_draw = self.must_draw.clone();
        let list = self.list.clone();
        let btn_update = self.btn_update.clone();
        conn.connect_db_connected(move |(_conn_info, db_info)| {
            clear_tables_at_list(&list);
            if let Some(info) = &db_info {
                for obj in &info.schema {
                    add_to_list(&list, &obj, &must_draw);
                }
            }
            *model.borrow_mut() = db_info;
            btn_update.set_sensitive(true);
        });
        let model = self.model.clone();
        let must_draw = self.must_draw.clone();
        let list = self.list.clone();
        let img = self.img.clone();
        let btn_export = self.btn_export.clone();
        let btn_update = self.btn_update.clone();
        conn.connect_db_disconnected(move |_| {
            clear_tables_at_list(&list);
            *model.borrow_mut() = None;
            render_to_image(&img, NO_DIAGRAM);
            btn_export.set_sensitive(false);
            btn_update.set_sensitive(false);
        });
    }

}

pub fn render_to_path(path : &str, graph : &str) {
    let bytes = render(graph).unwrap();
    if crate::safe_to_write(std::path::Path::new(path)).is_ok() {
        std::fs::write(path, bytes);
    }
}

pub fn render_to_image(img : &Picture, graph : &str) {
    let bytes = render(graph).unwrap();
    match Pixbuf::from_read(VecDeque::from(bytes)) {
        Ok(pxb) => {
            img.set_pixbuf(Some(&pxb));
        },
        Err(e) => {
            println!("{}",e);
        }
    }
}

// cargo test --lib -- simple_rendering --nocapture
/*#[test]
fn simple_graph_rendering() {
    let graph = r#"strict graph {
          a -- b
          a -- b
          b -- a [color=blue]
        }
    "#;
    render(graph).unwrap();
}*/

fn render(graph_dot : &str) -> Result<Vec<u8>, std::boxed::Box<dyn Error>> {
    let graph_dot = CString::new(graph_dot).unwrap();
    let graph_format = CString::new("dot").unwrap();

    unsafe {
        let gv_ctx = graphviz_sys::gvContext();
        if gv_ctx.is_null() {
            Err(String::from("Could not create graphviz context"))?;
        }
        let g : *mut graphviz_sys::Agraph_t = graphviz_sys::agmemread(graph_dot.as_ptr());
        if g.is_null() {
            Err(String::from(("Error parsing graph")))?;
        }
        let ans = graphviz_sys::gvLayout(gv_ctx, g, graph_format.as_ptr());
        if ans != 0 {
            return Err(get_error(ans).into());
        }
        let mut len : u32 = 0;
        let mut buffer : *mut i8 = ptr::null_mut();
        let ans = graphviz_sys::gvRenderData (
            gv_ctx,
            g,
            CString::new("png").unwrap().as_ptr(),
            &mut buffer,
            &mut len as *mut u32
        );
        if len == 0 {
            return Err("Returned length is zero".into());
        }

        let mut result : Vec<u8> = Vec::with_capacity(len as usize);
        result.set_len(len as usize);
        result.copy_from_slice(std::slice::from_raw_parts(buffer as *const u8, len as usize));
        graphviz_sys::gvFreeRenderData(buffer);

        result.truncate(len as usize);
        if ans != 0 {
            return Err(get_error(ans).into());
        }

        let ans = graphviz_sys::gvFreeLayout(gv_ctx, g);
        if ans != 0 {
            return Err(get_error(ans).into());
        }
        let ans = graphviz_sys::agclose(g);
        if ans != 0 {
            return Err(get_error(ans).into());
        }
        let ans = graphviz_sys::gvFreeContext(gv_ctx);
        if ans != 0 {
            return Err(get_error(ans).into());
        }
        Ok(result)
    }
}

unsafe fn get_error(err : c_int) -> String {
    let err_ptr = graphviz_sys::strerror(err) as *mut c_char;
    if let Ok(e) = CStr::from_ptr(err_ptr).to_str() {
        e.to_string()
    } else {
        "Unable to decode graphviz error".to_string()
    }
}


