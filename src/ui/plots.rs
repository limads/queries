/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.
For a copy, see http://www.gnu.org/licenses.*/

use gtk4::*;
use gtk4::prelude::*;
use stateful::React;
use crate::ui::MainMenu;
use libadwaita::ExpanderRow;
use papyri::render::GroupSplit;
use libadwaita::prelude::*;
use papyri::model::MappingType;
use std::rc::Rc;
use std::cell::RefCell;
use gdk::RGBA;
use crate::client::ActiveConnection;
use crate::sql::object::DBObject;

/*
Typical query built:
with t1 as
(select json_build_object('x', array_agg(a), 'y', array_agg(b)) from counts)
,t2 as
(select json_build_object('x', array_agg(balance), 'y', array_agg(client_id)) from account)
select * from t1 cross join t2;
*/

#[derive(Clone, Debug)]
pub struct DesignBox {
    bx : Box,
    grid_thickness_scale : Scale,
    bg_color_btn : ColorButton,
    grid_color_btn : ColorButton,
    font_btn : FontButton,
}

pub struct LabeledScale {
    bx : Box,
    scale : Scale,
    lbl : Label
}

impl LabeledScale {

    pub fn build(name : &str, min : f64, max : f64, step : f64) -> Self {
        let adj = Adjustment::builder().lower(min).upper(max).step_increment(step).build();
        let lbl = Label::new(Some(name));
        let scale = Scale::new(Orientation::Horizontal, Some(&adj));
        let bx = Box::new(Orientation::Horizontal, 0);
        bx.append(&lbl);
        bx.append(&scale);
        scale.set_hexpand(true);
        bx.set_hexpand(true);
        Self { bx, lbl, scale }
    }

}

pub struct LabeledColorBtn {
    bx : Box,
    btn : ColorButton,
    lbl : Label
}

impl LabeledColorBtn {

    pub fn build(name : &str) -> Self {
        let lbl = Label::new(Some(name));
        let btn = ColorButton::with_rgba(&RGBA::BLACK);
        let bx = Box::new(Orientation::Horizontal, 0);
        bx.append(&lbl);
        bx.append(&btn);
        Self { bx, lbl, btn }
    }

}

pub struct LabeledFontBtn {
    bx : Box,
    btn : FontButton,
    lbl : Label
}

impl LabeledFontBtn {

    pub fn build(name : &str) -> Self {
        let lbl = Label::new(Some(name));
        let btn = FontButton::new();
        let bx = Box::new(Orientation::Horizontal, 0);
        bx.append(&lbl);
        bx.append(&btn);
        Self { bx, lbl, btn }
    }

}

pub struct LabeledSwitch {
    bx : Box,
    switch : Switch,
    lbl : Label
}

impl LabeledSwitch {

    pub fn build(name : &str) -> Self {
        let lbl = Label::new(Some(name));
        let switch = Switch::new();
        switch.set_vexpand(false);
        switch.set_valign(Align::Center);
        let bx = Box::new(Orientation::Horizontal, 0);
        bx.append(&lbl);
        bx.append(&switch);
        Self { bx, lbl, switch }
    }

}

#[derive(Debug, Clone)]
pub struct DataEntry {
    entries : Vec<Entry>,
    bx : Box
}

impl DataEntry {

    // Receives pairs of (placeholder, primary icon)
    pub fn build(entries : &[(&str, &str)]) -> Self {
        let entries : Vec<_> = entries
            .iter()
            .map(|(txt, icon)| Entry::builder()
                .primary_icon_name(icon)
                .placeholder_text(txt)
                .build()
            ).collect();
        let bx = Box::new(Orientation::Horizontal, 0);
        bx.style_context().add_class("linked");
        for e in &entries {
            bx.append(e);
            e.set_hexpand(true);
            e.set_halign(Align::Fill);
        }
        Self { entries, bx }
    }

}

impl DesignBox {

    pub fn build() -> DesignBox {
        let bx = Box::new(Orientation::Horizontal, 0);
        let grid_thickness_scale : Scale = Scale::new(Orientation::Horizontal, None::<&Adjustment>);
        let bg_color_btn : ColorButton = ColorButton::new();
        let grid_color_btn : ColorButton = ColorButton::new();
        let font_btn : FontButton = FontButton::new();
        DesignBox {
            bx,
            grid_thickness_scale,
            bg_color_btn,
            grid_color_btn,
            font_btn
        }
    }

}

#[derive(Clone, Debug)]
pub struct ScaleBox {
    bx : Box,
    label_entry : Entry,
    entry_min : Entry,
    entry_max : Entry,
    log_switch : Switch,
    invert_switch : Switch,
    offset_scale : Scale,
    density_scale : Scale
}

impl ScaleBox {

    pub fn build(horizontal : bool) -> ScaleBox {
        let bx_top = Box::new(Orientation::Horizontal, 6);
        let lbl = if horizontal {
            "Horizontal label"
        } else {
            "Vertical label"
        };
        let label_entry = Entry::builder().primary_icon_name("type-text-symbolic").placeholder_text(lbl).build();
        label_entry.set_hexpand(true);
        let limits_bx = Box::new(Orientation::Horizontal, 0);
        limits_bx.style_context().add_class("linked");
        limits_bx.set_margin_end(6);

        let entry_min = Entry::builder().primary_icon_name("scale-inferior-symbolic").placeholder_text("Lower").build();
        let entry_max = Entry::builder().primary_icon_name("scale-superior-symbolic").placeholder_text("Upper").build();
        entry_min.set_max_width_chars(8);
        entry_max.set_max_width_chars(8);
        let log_switch = Switch::new();
        let invert_switch = Switch::new();
        bx_top.append(&label_entry);
        limits_bx.append(&entry_min);
        limits_bx.append(&entry_max);
        bx_top.append(&limits_bx);
        let bx_bottom = Box::new(Orientation::Horizontal, 6);
        let offset_bx = LabeledScale::build("Offset", 0.0, 1.0, 0.05);
        let density_bx = LabeledScale::build("Intervals", 1.0, 20.0, 1.0);
        let log_bx = LabeledSwitch::build("Log scale");
        let invert_bx = LabeledSwitch::build("Invert");
        bx_bottom.append(&log_bx.bx);
        bx_bottom.append(&invert_bx.bx);
        bx_bottom.append(&offset_bx.bx);
        bx_bottom.append(&density_bx.bx);
        let bx = Box::new(Orientation::Vertical, 6);
        bx.append(&bx_top);
        bx.append(&bx_bottom);

        ScaleBox {
            bx,
            label_entry,
            entry_min,
            entry_max,
            log_switch : log_bx.switch,
            invert_switch : invert_bx.switch,
            offset_scale : offset_bx.scale,
            density_scale : density_bx.scale
        }
    }

}

/*
mapping-area-symbolic
mapping-scatter-symbolic
mapping-bar-symbolic
mapping-line-symbolic
mapping-interval-symbolic

layout-four-symbolic
layout-horiz-symbolic
layout-vert-symbolic
layout-three-top-symbolic
layout-three-bottom-symbolic
layout-three-right-symbolic
layout-three-left-symbolic
layout-unique-symbolic

scale-height-symbolic
scale-width-symbolic
scale-horizontal-symbolic
scale-vertical-symbolic
scale-inferior-symbolic
scale-superior-symbolic
*/

fn icon_for_mapping(ty : MappingType) -> &'static str {
    match ty {
        MappingType::Line => "mapping-line-symbolic",
        MappingType::Scatter => "mapping-scatter-symbolic",
        MappingType::Bar => "mapping-bar-symbolic",
        MappingType::Area => "mapping-area-symbolic",
        MappingType::Text => "type-text-symbolic",
        MappingType::Interval => "mapping-interval-symbolic",
    }
}

#[derive(Debug, Clone)]
pub struct PlotRow {
    exp : ExpanderRow,
    add_btn : Button,
    hscale : ScaleBox,
    vscale : ScaleBox,
    design : DesignBox,
    mappings : Rc<RefCell<Vec<MappingRow>>>
}

impl PlotRow {

    pub fn visit_data_entries(&self, f : impl Fn(&Entry)) {
        for m in self.mappings.borrow().iter() {
            for e in m.data.entries.iter() {
                f(e);
            }
        }
    }

    pub fn build(cols_model : &Rc<RefCell<Option<ListStore>>>) -> Self {
        let exp = ExpanderRow::new();
        exp.set_selectable(false);
        exp.set_activatable(false);
        exp.set_icon_name(Some(""));
        exp.set_title("Center");
        exp.set_subtitle("No mappings");
        exp.set_icon_name(Some("roll-symbolic"));
        let add_btn = Button::builder().icon_name("list-add-symbolic").build();
        add_btn.style_context().add_class("flat");
        let mapping_bx = Box::new(Orientation::Horizontal, 0);
        mapping_bx.style_context().add_class("linked");
        let line_btn = Button::builder().icon_name("mapping-line-symbolic").build();
        let scatter_btn = Button::builder().icon_name("mapping-scatter-symbolic").build();
        let text_btn = Button::builder().icon_name("type-text-symbolic").build();
        let area_btn = Button::builder().icon_name("mapping-area-symbolic").build();
        let bar_btn = Button::builder().icon_name("mapping-bar-symbolic").build();
        let interval_btn = Button::builder().icon_name("mapping-interval-symbolic").build();
        mapping_bx.append(&line_btn);
        mapping_bx.append(&bar_btn);
        mapping_bx.append(&scatter_btn);
        mapping_bx.append(&text_btn);
        mapping_bx.append(&interval_btn);
        mapping_bx.append(&area_btn);

        // let action = ActionBar::new();
        // action.set_center_widget(Some(&mapping_bx));
        // action.set_revealed(false);
        mapping_bx.set_visible(false);

        /*let provider = CssProvider::new();
        provider.load_from_data(b"actionbar { background-color : #FFFFFF; }");
        let ctx = action.style_context();
        ctx.add_provider(&provider,800);*/

        let bx = Box::new(Orientation::Horizontal, 0);
        bx.append(&mapping_bx);
        bx.append(&add_btn);
        exp.add_action(&bx);
        add_btn.connect_clicked({
            let mapping_bx = mapping_bx.clone();
            move|_| {
                mapping_bx.set_visible(!mapping_bx.is_visible());
            }
        });
        for btn in [&line_btn, &scatter_btn, &text_btn, &area_btn, &bar_btn, &interval_btn] {
            let mapping_bx = mapping_bx.clone();
            btn.style_context().add_class("flat");
            btn.connect_clicked(move|_|{
                // action.set_revealed(false);
                mapping_bx.set_visible(false);
            });
        }

        let vscale = ScaleBox::build(false);
        let hscale = ScaleBox::build(true);
        let design = DesignBox::build();
        let stack = Stack::new();
        stack.set_valign(Align::Center);
        super::set_margins(&stack, 6, 6);
        stack.add_named(&design.bx, Some("design"));
        stack.add_named(&hscale.bx, Some("hscale"));
        stack.add_named(&vscale.bx, Some("vscale"));
        let design_toggle = ToggleButton::builder().icon_name("larger-brush-symbolic").build();
        let hscale_toggle = ToggleButton::builder().icon_name("scale-horizontal-symbolic").build();
        let vscale_toggle = ToggleButton::builder().icon_name("scale-vertical-symbolic").build();
        hscale_toggle.set_group(Some(&design_toggle));
        vscale_toggle.set_group(Some(&design_toggle));
        stack.set_visible_child_name("hscale");
        hscale_toggle.set_active(true);
        let toggle_bx = Box::new(Orientation::Vertical, 0);
        super::set_margins(&toggle_bx, 6, 6);
        toggle_bx.style_context().add_class("linked");
        for (tgl, nm) in [(&hscale_toggle, "hscale"), (&vscale_toggle, "vscale"), (&design_toggle, "design")] {
            let stack = stack.clone();
            tgl.style_context().add_class("flat");
            tgl.connect_toggled(move|tgl| {
                if tgl.is_active() {
                    stack.set_visible_child_name(nm);
                }
            });
            toggle_bx.append(tgl);
        }
        let plot_row = ListBoxRow::new();
        let bx = Box::new(Orientation::Horizontal, 6);
        bx.append(&toggle_bx);
        bx.append(&stack);
        plot_row.set_child(Some(&bx));
        exp.add_row(&plot_row);
        let mappings : Rc<RefCell<Vec<MappingRow>>> = Rc::new(RefCell::new(Vec::new()));
        let btns = [
            (&line_btn, MappingType::Line),
            (&scatter_btn,MappingType::Scatter),
            (&text_btn,MappingType::Text),
            (&area_btn,MappingType::Area),
            (&bar_btn,MappingType::Bar),
            (&interval_btn, MappingType::Interval)
        ];
        for (btn, ty) in btns {
            let mappings = mappings.clone();
            let exp = exp.clone();
            let cols_model = cols_model.clone();
            btn.clone().connect_clicked(move |_| {
                let row = MappingRow::build(ty);
                if let Some(model) = &*cols_model.borrow() {
                    println!("Added model");
                    for e in &row.data.entries {
                        add_completion(&e, &model);
                    }
                } else {
                    println!("No model to be added");
                }
                exp.add_row(&row.row);
                row.exclude_btn.connect_clicked({
                    let exp = exp.clone();
                    let mappings = mappings.clone();
                    let row = row.row.clone();
                    move |_| {
                        let mut mappings = mappings.borrow_mut();
                        if let Some(pos) = mappings.iter().position(|m| m.row == row ) {
                            exp.remove(&mappings[pos].row);
                            mappings.remove(pos);
                        } else {
                            eprintln!("Row position not found");
                        }
                    }
                });
                mappings.borrow_mut().push(row);
            });
        }

        exp.set_expanded(true);
        Self { exp, add_btn, hscale, vscale, design, mappings }
    }

}

#[derive(Debug, Clone)]
pub enum MappingBox {
    Line(LineBox),
    Scatter(ScatterBox),
    Interval(IntervalBox),
    Area(AreaBox),
    Bar(BarBox),
    Text(TextBox)
}

impl MappingBox {

    pub fn parent_box(&self) -> &Box {
        match self {
            Self::Line(b) => &b.bx,
            Self::Scatter(b) => &b.bx,
            Self::Interval(b) => &b.bx,
            Self::Text(b) => &b.bx,
            Self::Area(b) => &b.bx,
            Self::Bar(b) => &b.bx
        }
    }

}

#[derive(Debug, Clone)]
pub struct MappingRow {
    row : ListBoxRow,
    props : MappingBox,
    data : DataEntry,
    exclude_btn : Button
}

impl MappingRow {

    pub fn build(ty : MappingType) -> Self {
        let row = ListBoxRow::new();
        row.set_activatable(true);
        row.set_selectable(true);
        let bx = Box::new(Orientation::Horizontal, 0);
        let mapping_img = Image::builder().icon_name(icon_for_mapping(ty)).build();
        super::set_margins(&mapping_img, 6, 6);
        mapping_img.set_hexpand(false);
        mapping_img.set_halign(Align::Start);

        // Contains data box on top, mapping specific config on bottom
        let mapping_bx = Box::new(Orientation::Vertical, 0);
        mapping_bx.set_hexpand(true);
        mapping_bx.set_halign(Align::Fill);

        let (data, props) = match ty {

            MappingType::Line => (
                DataEntry::build(&[
                    ("X", "scale-horizontal-symbolic"),
                    ("Y", "scale-vertical-symbolic"),
                ]),
                MappingBox::Line(LineBox::build())
            ),

            MappingType::Scatter => (
                DataEntry::build(&[
                    ("X", "scale-horizontal-symbolic"),
                    ("Y", "scale-vertical-symbolic"),
                ]),
                MappingBox::Scatter(ScatterBox::build())
            ),

            MappingType::Interval => (
                DataEntry::build(&[
                    ("X", "scale-horizontal-symbolic"),
                    ("Lower", "scale-vertical-symbolic"),
                    ("Upper", "scale-vertical-symbolic"),
                ]),
                MappingBox::Interval(IntervalBox::build())
            ),

            MappingType::Text => (
                DataEntry::build(&[
                    ("X", "scale-horizontal-symbolic"),
                    ("Y", "scale-vertical-symbolic"),
                    ("Text", "type-text-symbolic"),
                ]),
                MappingBox::Text(TextBox::build())
            ),

            MappingType::Area => (
                DataEntry::build(&[
                    ("X", "scale-horizontal-symbolic"),
                    ("Lower", "scale-vertical-symbolic"),
                    ("Upper", "scale-vertical-symbolic"),
                ]),
                MappingBox::Area(AreaBox::build())
            ),

            MappingType::Bar => (
                DataEntry::build(&[
                    ("Length", "scale-vertical-symbolic")
                ]),
                MappingBox::Bar(BarBox::build())
            ),
        };
        mapping_bx.append(&data.bx);
        mapping_bx.append(props.parent_box());

        // let exclude_btn = Button::builder().icon_name("").build();
        let exclude_btn = Button::builder().icon_name("user-trash-symbolic").build();
        exclude_btn.set_hexpand(false);
        exclude_btn.set_halign(Align::End);
        exclude_btn.style_context().add_class("flat");
        // exclude_btn.set_visible(false);

        // Account for exclude btn space
        // bx.set_margin_end(34);

        bx.append(&mapping_img);
        bx.append(&mapping_bx);
        props.parent_box().append(&exclude_btn);

        row.set_child(Some(&bx));
        let ev = EventControllerMotion::new();
        ev.connect_enter({
            let exclude_btn = exclude_btn.clone();
            let mapping_bx = mapping_bx.clone();
            let bx = bx.clone();
            move |_, _, _| {
                // exclude_btn.set_icon_name("user-trash-symbolic");
            }
        });
        ev.connect_leave({
            let exclude_btn = exclude_btn.clone();
            let mapping_bx = mapping_bx.clone();
            let bx = bx.clone();
            move |_| {
                // exclude_btn.set_icon_name("");
            }
        });
        row.add_controller(&ev);
        exclude_btn.connect_clicked(move|_| {

        });

        MappingRow { row, props, data, exclude_btn }
    }

}


#[derive(Debug, Clone)]
pub struct GraphWindow {
    win : Window,
    width_entry : Entry,
    height_entry : Entry,
    btn_clear : Button,
    btn_sql : Button,
    btn_plot : Button,
    toggle_unique : ToggleButton,
    toggle_vertical : ToggleButton,
    toggle_horizontal : ToggleButton,
    toggle_three_top : ToggleButton,
    toggle_three_left : ToggleButton,
    toggle_three_right : ToggleButton,
    toggle_three_bottom : ToggleButton,
    toggle_four : ToggleButton,
    plot_rows : Rc<RefCell<Vec<PlotRow>>>,
    objs : Rc<RefCell<Vec<DBObject>>>,
    cols_model : Rc<RefCell<Option<ListStore>>>
}

impl GraphWindow {

    pub fn build() -> Self {
        let win = Window::new();
        win.set_title(Some("Graph editor"));
        win.set_width_request(800);
        win.set_height_request(600);

        let scroll = ScrolledWindow::new();
        let bx = Box::new(Orientation::Vertical, 0);
        scroll.set_child(Some(&bx));
        win.set_child(Some(&scroll));

        let layout_outer_bx = Box::new(Orientation::Vertical, 0);
        let layout_lbl = gtk4::Label::builder()
            .use_markup(true)
            .label("<span font_weight=\"600\" font_size=\"large\" fgalpha=\"60%\">Layout</span>")
            .build();
        layout_lbl.set_margin_top(18);
        layout_lbl.set_margin_bottom(18);
        layout_lbl.set_halign(Align::Center);
        layout_outer_bx.append(&layout_lbl);
        let layout_bx = Box::new(Orientation::Horizontal, 0);
        layout_bx.style_context().add_class("linked");
        let toggle_unique = ToggleButton::builder().icon_name("layout-unique-symbolic").build();
        let toggle_vertical = ToggleButton::builder().icon_name("layout-vert-symbolic").build();
        let toggle_horizontal = ToggleButton::builder().icon_name("layout-horiz-symbolic").build();
        let toggle_three_top = ToggleButton::builder().icon_name("layout-three-top-symbolic").build();
        let toggle_three_left = ToggleButton::builder().icon_name("layout-three-left-symbolic").build();
        let toggle_three_right = ToggleButton::builder().icon_name("layout-three-right-symbolic").build();
        let toggle_three_bottom = ToggleButton::builder().icon_name("layout-three-bottom-symbolic").build();
        let toggle_four = ToggleButton::builder().icon_name("layout-four-symbolic").build();
        let toggles = [
            &toggle_unique,
            &toggle_vertical,
            &toggle_horizontal,
            &toggle_three_top,
            &toggle_three_left,
            &toggle_three_right,
            &toggle_three_bottom,
            &toggle_four
        ];
        for (ix, btn) in toggles.iter().enumerate() {
            layout_bx.append(*btn);
            if ix >= 1 {
                btn.set_group(Some(toggles[0]));
                btn.style_context().add_class("linked");
            }
        }
        toggle_unique.set_active(true);
        layout_outer_bx.append(&layout_bx);

        let dim_outer_bx = Box::new(Orientation::Vertical, 0);
        let dim_lbl = gtk4::Label::builder()
            .use_markup(true)
            .label("<span font_weight=\"600\" font_size=\"large\" fgalpha=\"60%\">Dimensions</span>")
            .build();
        dim_lbl.set_margin_top(18);
        dim_lbl.set_margin_bottom(18);
        dim_lbl.set_halign(Align::Center);
        dim_outer_bx.append(&dim_lbl);
        let dim_bx = Box::new(Orientation::Horizontal, 0);
        dim_bx.style_context().add_class("linked");
        let width_entry = Entry::builder().primary_icon_name("scale-horizontal-symbolic").placeholder_text("Width (px)").build();
        let height_entry = Entry::builder().primary_icon_name("scale-vertical-symbolic").placeholder_text("Height (px)").build();
        dim_bx.append(&width_entry);
        dim_bx.append(&height_entry);
        dim_outer_bx.append(&dim_bx);

        let top_bx = Box::new(Orientation::Horizontal, 64);
        top_bx.set_halign(Align::Center);
        top_bx.append(&layout_outer_bx);
        top_bx.append(&dim_outer_bx);

        let bottom_bx = Box::new(Orientation::Horizontal, 16);
        bottom_bx.set_margin_top(18);
        bottom_bx.set_margin_bottom(18);
        bottom_bx.set_halign(Align::Center);
        let btn_clear = Button::builder().label("Clear").build();
        let btn_sql = Button::builder().label("Copy SQL").build();
        let btn_plot = Button::builder().label("Plot").build();
        for btn in [&btn_clear, &btn_sql, &btn_plot] {
            bottom_bx.append(btn);
            btn.style_context().add_class("pill");
        }
        btn_plot.style_context().add_class("suggested-action");

        let list = ListBox::new();
        list.set_halign(Align::Center);

        let cols_model = Rc::new(RefCell::new(None));
        let pr = PlotRow::build(&cols_model);

        let plot_rows = Rc::new(RefCell::new(Vec::new()));
        plot_rows.borrow_mut().push(pr.clone());

        list.append(&pr.exp);
        crate::ui::configure_list(&list);
        bx.append(&top_bx);
        bx.append(&list);
        bx.append(&bottom_bx);

        Self { win, btn_clear, btn_sql, btn_plot, width_entry, height_entry, toggle_unique,
            toggle_vertical,
            toggle_horizontal,
            toggle_three_top,
            toggle_three_left,
            toggle_three_right,
            toggle_three_bottom,
            toggle_four,
            plot_rows,
            objs : Default::default(),
            cols_model
        }
    }

}

impl React<ActiveConnection> for GraphWindow {

    fn react(&self, conn : &ActiveConnection) {
        conn.connect_db_connected({
            let objs = self.objs.clone();
            let pl_rows = self.plot_rows.clone();
            let cols_model = self.cols_model.clone();
            move |(_, info)| {
                if let Some(info) = info {
                    update_completion_with_schema(objs.clone(), cols_model.clone(), pl_rows.clone(), Some(info.schema));
                }
            }
        });
        conn.connect_schema_update({
            let objs = self.objs.clone();
            let pl_rows = self.plot_rows.clone();
            let cols_model = self.cols_model.clone();
            move |schema| {
                update_completion_with_schema(objs.clone(), cols_model.clone(), pl_rows.clone(), schema);
            }
        });
    }

}

fn update_completion_with_schema(
    objs : Rc<RefCell<Vec<DBObject>>>,
    cols_model : Rc<RefCell<Option<ListStore>>>,
    pl_rows : Rc<RefCell<Vec<PlotRow>>>,
    schema : Option<Vec<DBObject>>
) {
    let mut objs = objs.borrow_mut();
    let mut cols_model = cols_model.borrow_mut();
    objs.clear();
    if let Some(schema) = schema {
        let col_types: [glib::Type; 1] = [glib::Type::STRING];
        let model = ListStore::new(&col_types);
        let mut data = Vec::new();
        for new_obj in &schema {
            match &new_obj {
                DBObject::Schema { children, .. } => {
                    for child in children.iter() {
                        match child {
                            DBObject::Table { name, cols, .. } => {
                                for (c, _, _) in cols.iter() {
                                    data.push(format!("{}.{}", name, c));
                                }
                            },
                            DBObject::View { name, .. } => {

                            },
                            _ =>  { }
                        }
                    }
                },
                _ => { }
            }
        }
        for d in &data {
            model.set(&model.append(), &[(0, d)]);
        }
        for pl in pl_rows.borrow().iter() {
            pl.visit_data_entries(|e| {
                add_completion(&e, &model);
            });
        }
        println!("Updating model with {:?}", data);

        // Any mappings added later will use this information.
        *objs = schema;
        *cols_model = Some(model);
    } else {
        for pl in pl_rows.borrow().iter() {
            pl.visit_data_entries(|e| {
                e.set_completion(None);
            });
        }
        *cols_model = None;
    }
}

fn add_completion(e : &Entry, model : &ListStore) {
    let compl = EntryCompletion::new();
    compl.set_text_column(0);
    compl.set_minimum_key_length(1);
    compl.set_popup_completion(true);
    compl.set_model(Some(model));
    e.set_completion(Some(&compl));
}

impl React<MainMenu> for GraphWindow {

    fn react(&self, menu : &MainMenu) {
        let win = self.win.clone();
        menu.action_graph.connect_activate(move|_,_| {
            win.show();
        });
    }

}

#[derive(Debug, Clone)]
pub struct ScatterBox {
    bx : Box,
    color_btn : ColorButton,
    radius_scale : Scale
}

impl ScatterBox {

    pub fn build() -> Self {
        let color_btn : ColorButton = ColorButton::new();
        let radius_scale : Scale = Scale::new(Orientation::Horizontal, None::<&Adjustment>);
        let bx = Box::new(Orientation::Horizontal, 6);
        Self { color_btn, radius_scale, bx }
    }

}

#[derive(Debug, Clone)]
pub struct LineBox {
    color_btn : ColorButton,
    width_scale : Scale,
    dash_scale : Scale,
    bx : Box
}

impl LineBox {

    pub fn build() -> Self {
        let color_bx = LabeledColorBtn::build("Line color");
        let width_bx = LabeledScale::build("Width", 0.0, 10.0, 1.0);
        let dash_bx = LabeledScale::build("Dash", 0.0, 10.0, 1.0);
        let bx = Box::new(Orientation::Horizontal, 6);
        for b in [&color_bx.bx, &width_bx.bx, &dash_bx.bx] {
            bx.append(b);
        }
        Self { color_btn : color_bx.btn, width_scale : width_bx.scale, dash_scale : dash_bx.scale, bx }
    }

}

#[derive(Debug, Clone)]
pub struct TextBox {
    color_btn : ColorButton,
    font_btn : FontButton,
    bx : Box
}

impl TextBox {

    pub fn build() -> Self {
        let color_btn : ColorButton = ColorButton::new();
        let font_btn : FontButton = FontButton::new();
        let bx = Box::new(Orientation::Horizontal, 6);
        Self { color_btn, font_btn, bx }
    }

}

#[derive(Debug, Clone)]
pub struct AreaBox {
    color_btn : ColorButton,
    opacity_scale : Scale,
    bx : Box
}

impl AreaBox {

    pub fn build() -> Self {
        let color_btn : ColorButton = ColorButton::new();
        let opacity_scale : Scale = Scale::new(Orientation::Horizontal, None::<&Adjustment>);
        let bx = Box::new(Orientation::Horizontal, 6);
        Self { color_btn, opacity_scale, bx }
    }

}

#[derive(Debug, Clone)]
pub struct IntervalBox {
    bx : Box
}

impl IntervalBox {

    pub fn build() -> Self {
        let bx = Box::new(Orientation::Horizontal, 6);
        Self { bx }
    }

}

#[derive(Debug, Clone)]
pub struct BarBox {
    bx : Box,
    color_btn : ColorButton,
    anchor_switch : Switch,
    horizontal_switch : Switch,
    width_scale : Scale,
    origin_x_entry : Entry,
    origin_y_entry : Entry,
    spacing_entry : Entry
}

impl BarBox {

    pub fn build() -> Self {
        let color_btn : ColorButton = ColorButton::new();
        let anchor_switch : Switch = Switch::new();
        let horizontal_switch : Switch = Switch::new();
        let width_scale : Scale = Scale::new(Orientation::Horizontal, None::<&Adjustment>);
        let origin_x_entry : Entry = Entry::new();
        let origin_y_entry : Entry = Entry::new();
        let spacing_entry : Entry = Entry::new();
        let bx = Box::new(Orientation::Horizontal, 6);
        Self {
            bx,
            color_btn,
            anchor_switch,
            horizontal_switch,
            width_scale,
            origin_x_entry,
            origin_y_entry,
            spacing_entry
        }
    }

}



