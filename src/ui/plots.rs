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
use gdk_pixbuf::Pixbuf;
use crate::ui::PackedImageLabel;
use crate::ui::settings::NamedBox;
use std::collections::BTreeMap;
use std::cmp::{Ord, Ordering, PartialEq};

/*
Since CTE names shadow names of tables, use a single CTE that aggregates all data from a single
table that will be required

with usertable as (
    select array_agg(col1name) as col1name, array_agg(col2name) as col2name from usertable
), usertable2 as (
    select array_agg(col1name) as col1name, array_agg(col2name) as col2name from usertable2
) select json_build_object("x" usertable.col1name, "y", usertable.col2name),
    json_build_object("x2" usertable2.col1name, "y", usertable2.col2name)
from usertable cross join usertable2;
*/

#[derive(Clone, Debug)]
pub struct DesignBox {
    bx : Box,
    grid_thickness_scale : Scale,
    bg_color_btn : ColorButton,
    grid_color_btn : ColorButton,
    font_btn : FontButton,
}

impl DesignBox {

    pub fn sql(&self) -> String {
        let bgcolor = color_literal(&self.bg_color_btn);
        let fgcolor = color_literal(&self.grid_color_btn);
        let width = self.grid_thickness_scale.value();
        let font = font_literal(&self.font_btn);
        format!(r#"
            json_build_object(
                'bgcolor', {},
                'fgcolor', {},
                'width', {},
                'font', '{}'
            )"#, bgcolor, fgcolor, width, font
        )
    }

    pub fn build() -> DesignBox {
        let design_title_bx = PackedImageLabel::build("larger-brush-symbolic", "Design");
        configure_title(&design_title_bx.bx);
        let bx = Box::new(Orientation::Vertical, 0);
        bx.set_halign(Align::Center);
        bx.set_hexpand(false);
        bx.set_vexpand(false);
        bx.set_valign(Align::Center);
        bx.append(&design_title_bx.bx);
        let design_list = ListBox::new();
        crate::ui::configure_list(&design_list);
        design_list.set_halign(Align::Center);
        bx.append(&design_list);

        let bg_color_row = ListBoxRow::new();
        let bg_color_btn : ColorButton = ColorButton::new();
        bg_color_row.set_selectable(false);
        bg_color_row.set_activatable(false);
        let bg_color_bx = NamedBox::new("Background", Some("Plot area background color"), bg_color_btn);
        bg_color_row.set_child(Some(&bg_color_bx.bx));
        design_list.append(&bg_color_row);

        let grid_color_row = ListBoxRow::new();
        let grid_color_btn : ColorButton = ColorButton::new();
        grid_color_row.set_selectable(false);
        grid_color_row.set_activatable(false);
        let grid_color_bx = NamedBox::new("Grid", Some("Plot grid color"), grid_color_btn);
        grid_color_row.set_child(Some(&grid_color_bx.bx));
        design_list.append(&grid_color_row);

        let adj = Adjustment::builder().lower(0.0).upper(10.0).step_increment(0.1).build();
        let grid_thickness_scale : Scale = Scale::new(Orientation::Horizontal, Some(&adj));
        grid_thickness_scale.set_hexpand(true);
        let grid_thickness_row = ListBoxRow::new();
        grid_thickness_row.set_selectable(false);
        grid_thickness_row.set_activatable(false);
        let grid_thickness_bx = NamedBox::new("Grid line thickness (in pixels)", Some("Grid thickness"), grid_thickness_scale);
        grid_thickness_row.set_child(Some(&grid_thickness_bx.bx));
        design_list.append(&grid_thickness_row);

        let font_row = ListBoxRow::new();
        let font_btn = FontButton::new();
        font_row.set_selectable(false);
        font_row.set_activatable(false);
        let font_bx = NamedBox::new("Font", Some("Plot font for labels and scale values"), font_btn);
        font_row.set_child(Some(&font_bx.bx));
        design_list.append(&font_row);

        DesignBox {
            bx,
            grid_thickness_scale : grid_thickness_bx.w,
            bg_color_btn : bg_color_bx.w,
            grid_color_btn : grid_color_bx.w,
            font_btn : font_bx.w
        }
    }

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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ColSource {
    schema : Option<String>,
    tbl : String,
}

impl ColSource {

    pub fn to_string(&self) -> String {
        if let Some(s) = &self.schema {
            format!("{}.{}", s, self.tbl)
        } else {
            self.tbl.clone()
        }
    }

}

pub struct MappingCol {
    src : ColSource,
    col : String
}

impl MappingCol {

    pub fn column(&self) -> &str {
        &self.col[..]
    }

    pub fn to_string(&self) -> String {
        if let Some(schema) = &self.src.schema {
            format!("{}.{}.{}", schema, self.src.tbl, self.col)
        } else {
            format!("{}.{}", self.src.tbl, self.col)
        }
    }

}

// Maps CTE names to inner queries.
fn nested_column_sql(nested : &BTreeMap<ColSource, Vec<String>>) -> BTreeMap<String, String> {
    let mut exprs = BTreeMap::new();
    for (src, cols) in nested.iter() {
        let cols_expr : String = cols.iter().map(|c| format!("array_agg({}) as {}", c, c)).collect::<Vec<_>>().join(", ");

        // TODO make sure this is unique (or prefixed with schema if required).
        let agg_name = &src.tbl;

        exprs.insert(
            agg_name.to_string(),
            format!("{} as (SELECT {} FROM {})", agg_name, cols_expr, src.to_string())
        );
    }
    exprs
}

pub fn nested_columns(
    entries : &[DataEntry]
) -> BTreeMap<ColSource, Vec<String>> {
    let mut nested = BTreeMap::new();
    for e in entries {
        let mut vals = e.values();
        for v in vals.drain(..) {
            nested.entry(v.src).or_insert(Vec::default()).push(v.col);
        }
    }
    nested
}

impl DataEntry {

    pub fn values(&self) -> Vec<MappingCol> {
        let mut out = Vec::new();
        for e in &self.entries {
            let txt = e.text();
            let mut txt_iter = txt.split(".");
            if let Some(s1) = txt_iter.next() {
                if let Some(s2) = txt_iter.next() {
                    if let Some(s3) = txt_iter.next() {
                        out.push(MappingCol { src : ColSource { schema : Some(s1.to_string()), tbl : s2.to_string() }, col : s3.to_string() });
                    } else {
                        out.push(MappingCol { src : ColSource { schema : None, tbl : s1.to_string() }, col : s2.to_string() });
                    }
                } else {
                    return Vec::new();
                }
            } else{
                return Vec::new();
            }
        }
        out
    }

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

    pub fn sql(&self) -> String {
        let label = self.label_entry.text();
        let (mut min, mut max) = (self.entry_min.text().to_string(), self.entry_max.text().to_string());
        if min.is_empty() {
            min = String::from("0.0");
        }
        if max.is_empty() {
            max = String::from("1.0");
        }
        let adjust = "'off'";
        let log = bool_literal(&self.log_switch);
        let invert = bool_literal(&self.invert_switch);
        let offset = self.offset_scale.value();
        let density = self.density_scale.value();
        let precision = 2;
        format!(r#"json_build_object(
            'label', '{}',
            'from', {},
            'to', {},
            'adjust', {},
            'log', {},
            'invert', {},
            'offset', {},
            'intervals', {},
            'precision', {}
        )"#, label, min, max, adjust, log, invert, offset, density, precision)
    }

    pub fn build(horizontal : bool) -> ScaleBox {
        let bx_top = Box::new(Orientation::Horizontal, 6);
        let lbl = if horizontal {
            "Horizontal scale"
        } else {
            "Vertical scale"
        };
        let label_entry = Entry::builder().primary_icon_name("type-text-symbolic").placeholder_text(lbl).build();
        label_entry.set_hexpand(true);
        let limits_bx = Box::new(Orientation::Horizontal, 0);
        limits_bx.style_context().add_class("linked");
        limits_bx.set_margin_end(6);

        let icon_start = if horizontal {
            "scale-left-symbolic"
        } else {
            "scale-inferior-symbolic"
        };
        let icon_end = if horizontal {
            "scale-right-symbolic"
        } else {
            "scale-superior-symbolic"
        };
        let entry_min = Entry::builder().primary_icon_name(icon_start).placeholder_text("Lower").build();
        let entry_max = Entry::builder().primary_icon_name(icon_end).placeholder_text("Upper").build();
        entry_min.set_max_width_chars(8);
        entry_max.set_max_width_chars(8);

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
    // exp : ExpanderRow,
    list : ListBox,
    bx : Box,
    add_btn : Button,
    hscale : ScaleBox,
    vscale : ScaleBox,
    mappings : Rc<RefCell<Vec<MappingRow>>>,
    //completions : Rc<RefCell<Vec<EntryCompletion>>>
}

impl PlotRow {

    pub fn sql(&self) -> String {
        let hscale = self.hscale.sql();
        let vscale = self.vscale.sql();
        let mappings = self.mappings.borrow();
        let mut mappings_str = String::new();
        if mappings.len() >= 1 {
            for m in mappings.iter().take(mappings.len()-1) {
                mappings_str += &m.sql();
                mappings_str += ", ";
            }
            if let Some(lst) = mappings.last() {
                mappings_str += &lst.sql();
            }
        }
        format!(r#"
            json_build_object(
                'mappings' : array[{}],
                'x' : {},
                'y' : {}
            )"#, mappings_str, hscale, vscale
        )
    }

    pub fn visit_data_entries(&self, f : impl Fn(&Entry)) {
        for m in self.mappings.borrow().iter() {
            for e in m.data.entries.iter() {
                f(e);
            }
        }
    }

    pub fn build(title : &str, cols_model : &Rc<RefCell<Option<ListStore>>>) -> Self {
        let im = super::PackedImageLabel::build("roll-symbolic", title);
        // im.bx.set_halign(Align::Start);
        // im.bx.set_hexpand(true);

        let title_bx = Box::new(Orientation::Horizontal, 0);
        configure_title(&title_bx);

        title_bx.set_halign(Align::Fill);
        title_bx.set_hexpand(true);
        title_bx.append(&im.bx);

        let list = ListBox::new();
        crate::ui::configure_list(&list);
        list.set_halign(Align::Center);
        // let exp = ExpanderRow::new();

        // exp.set_selectable(false);
        // exp.set_activatable(false);

        // exp.set_title("Center");
        // exp.set_subtitle("No mappings");
        // exp.set_icon_name(Some("roll-symbolic"));

        let add_btn = Button::builder().icon_name("list-add-symbolic").build();
        add_btn.style_context().add_class("flat");
        add_btn.set_hexpand(true);
        add_btn.set_halign(Align::End);
        let mapping_bx = Box::new(Orientation::Horizontal, 0);
        mapping_bx.set_hexpand(true);
        mapping_bx.set_halign(Align::End);
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

        let action_bx = Box::new(Orientation::Horizontal, 0);
        action_bx.set_halign(Align::End);
        action_bx.set_hexpand(false);
        action_bx.append(&mapping_bx);
        action_bx.append(&add_btn);
        title_bx.append(&action_bx);

        // exp.add_action(&bx);
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

        let stack = Stack::new();
        stack.set_valign(Align::Center);
        super::set_margins(&stack, 6, 6);

        stack.add_named(&hscale.bx, Some("hscale"));
        stack.add_named(&vscale.bx, Some("vscale"));

        let hscale_toggle = ToggleButton::builder().icon_name("scale-horizontal-symbolic").build();
        let vscale_toggle = ToggleButton::builder().icon_name("scale-vertical-symbolic").build();
        vscale_toggle.set_group(Some(&hscale_toggle));
        stack.set_visible_child_name("hscale");
        hscale_toggle.set_active(true);
        let toggle_bx = Box::new(Orientation::Vertical, 0);
        super::set_margins(&toggle_bx, 6, 6);
        toggle_bx.style_context().add_class("linked");
        for (tgl, nm) in [(&hscale_toggle, "hscale"), (&vscale_toggle, "vscale")] {
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
        plot_row.set_selectable(false);
        plot_row.set_activatable(false);
        let fst_bx = Box::new(Orientation::Horizontal, 6);
        fst_bx.append(&toggle_bx);
        fst_bx.append(&stack);
        plot_row.set_child(Some(&fst_bx));
        list.append(&plot_row);

        let mappings : Rc<RefCell<Vec<MappingRow>>> = Rc::new(RefCell::new(Vec::new()));
        let btns = [
            (&line_btn, MappingType::Line),
            (&scatter_btn,MappingType::Scatter),
            (&text_btn,MappingType::Text),
            (&area_btn,MappingType::Area),
            (&bar_btn,MappingType::Bar),
            (&interval_btn, MappingType::Interval)
        ];

        //let completions = Rc::new(RefCell::new(Vec::new()));
        for (btn, ty) in btns {
            let mappings = mappings.clone();
            let list = list.clone();
            let cols_model = cols_model.clone();
            //let completions = completions.clone();
            btn.clone().connect_clicked(move |_| {
                let row = MappingRow::build(ty);
                match &row.props {
                    MappingBox::Bar(bar_bx) => {
                        row.data.bx.append(&bar_bx.origin_entry);
                        // row.data.bx.append(&bar_bx.origin_y_entry);
                        row.data.bx.append(&bar_bx.spacing_entry);
                    },
                    _ => { }
                }
                if let Some(model) = &*cols_model.borrow() {
                    // println!("Added model");
                    for e in &row.data.entries {
                        /*completions.borrow_mut().push(*/ add_completion(&e, &model);
                    }
                } else {
                    // println!("No model to be added");
                }
                list.append(&row.row);
                row.exclude_btn.connect_clicked({
                    let list = list.clone();
                    let mappings = mappings.clone();
                    let row = row.row.clone();
                    move |_| {
                        let mut mappings = mappings.borrow_mut();
                        if let Some(pos) = mappings.iter().position(|m| m.row == row ) {
                            list.remove(&mappings[pos].row);
                            mappings.remove(pos);
                        } else {
                            eprintln!("Row position not found");
                        }
                    }
                });
                mappings.borrow_mut().push(row);
            });
        }

        // exp.set_expanded(true);
        let bx = Box::new(Orientation::Vertical, 0);
        bx.set_halign(Align::Center);
        bx.set_hexpand(false);
        bx.set_valign(Align::Center);
        bx.set_vexpand(false);
        bx.append(&title_bx);
        bx.append(&list);

        Self { add_btn, hscale, vscale, mappings, list, bx /*completions*/ }
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

    pub fn sql(&self) -> String {

        let data = self.data.values();
        if data.is_empty() {
            return String::new();
        }

        // TODO make sure source is same for other entries.
        // let tbl = data[0].src.to_string();

        match &self.props {
            MappingBox::Line(line_bx) => {
                let dash = line_bx.dash_scale.value();
                let width = line_bx.width_scale.value();
                let x = &data[0].col;
                let y = &data[1].col;
                let color = color_literal(&line_bx.color_btn);
                format!("
	            json_build_object(
	                'kind', 'line',
	                'map', json_build_object('x', {}, 'y', {}),
	                'color', '{}',
	                'width', {},
	                'spacing', {}
	            )", x, y, color, width, dash)
	        },
	        MappingBox::Scatter(scatter_bx) => {
	            let x = &data[0].col;
                let y = &data[1].col;
                let color = color_literal(&scatter_bx.color_btn);
                let radius = scatter_bx.radius_scale.value();
	            format!("json_build_object(
	                'kind', 'scatter',
	                'map', json_build_object('x', {}, 'y', {}),
	                'color', {},
	                'radius', {}
                )", x, y, color, radius)
            },
            MappingBox::Bar(bar_bx) => {
                let x = &data[0].col;
                let color = color_literal(&bar_bx.color_btn);
                let origin = bar_bx.origin_entry.text();
                let spacing = bar_bx.spacing_entry.text();
                let width = bar_bx.width_scale.value();
                let center = bool_literal(&bar_bx.center_switch);
                let vertical = bool_literal(&bar_bx.vertical_switch);
	            format!("json_build_object(
	                'kind', 'bar',
	                'map', json_build_object('x', {}),
	                'color', {},
	                'origin', {},
	                'spacing', {},
	                'width', {},
	                'center', {},
	                'vertical', {}
	                )", x, color, origin, spacing, width, center, vertical
	            )
	        },
	        MappingBox::Interval(interval_bx) => {
	            let (x, y, z) = (&data[0].col, &data[1].col, &data[2].col);
                let color = color_literal(&interval_bx.color_btn);
                let spacing = interval_bx.spacing_scale.value();
                let limits = interval_bx.limits_scale.value();
                let width = interval_bx.width_scale.value();
                let vertical = bool_literal(&interval_bx.vertical_switch);
                format!(r#"
		            json_build_object(
		                'kind', 'interval',
		                'map', json_build_object('x', {}, 'y', {}, 'z', {}),
		                'color', {},
		                'width', {},
		                'limits', {},
		                'vertical', {},
		                'spacing', {}
		            )"#, x, y, z, color, width, limits, vertical, spacing
		        )
		    },
            MappingBox::Text(text_bx) => {
                let (x, y, z) = (&data[0].col, &data[1].col, &data[2].col);
                let color = color_literal(&text_bx.color_btn);
                let font = font_literal(&text_bx.font_btn);
                format!(r#"
		        json_build_object(
		            'kind', 'text',
		            'map', json_build_object('x', {}, 'y', {}, 'text', {}),
		            'color', {},
		            'font', '{}'
		        )"#, x, y, z, color, font)
		    },
		    MappingBox::Area(area_bx) => {
		        let (x, y, z) = (&data[0].col, &data[1].col, &data[2].col);
		        let color = color_literal(&area_bx.color_btn);
                let opacity = area_bx.opacity_scale.value();
                format!(r#"
		            json_build_object(
		                'kind', 'area',
		                'map', json_build_object('x', {}, 'y', {}, 'z', {}),
		                'color', {},
		                'opacity', {}
		            )"#, x, y, z, color, opacity
		        )
		    }
		}
    }

    pub fn build(ty : MappingType) -> Self {
        let row = ListBoxRow::new();
        row.set_activatable(false);
        row.set_selectable(false);
        let bx = Box::new(Orientation::Horizontal, 0);
        super::set_margins(&bx, 0, 12);

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

fn font_literal(font_btn : &FontButton) -> String {
    font_btn.font().map(|f| f.to_string() ).unwrap_or(String::from("Liberation Sans 20"))
}

fn bool_literal(switch : &Switch) -> &'static str {
    if switch.is_active() {
        "'t'"
    } else {
        "'f'"
    }
}

fn color_literal(color_btn : &ColorButton) -> String {
    format!("'{}'", color_btn.rgba())
}

fn configure_title(bx : &gtk4::Box) {
    bx.set_hexpand(false);
    bx.set_vexpand(false);
    bx.set_margin_top(18);
    bx.set_margin_bottom(6);
    bx.set_halign(Align::Start);
}

#[derive(Debug, Clone)]
pub struct LayoutBox {
    bx : Box,
    width_entry : Entry,
    height_entry : Entry,
    toggle_unique : ToggleButton,
    toggle_vertical : ToggleButton,
    toggle_horizontal : ToggleButton,
    toggle_three_top : ToggleButton,
    toggle_three_left : ToggleButton,
    toggle_three_right : ToggleButton,
    toggle_three_bottom : ToggleButton,
    toggle_four : ToggleButton,
    hratio_scale : Scale,
    vratio_scale : Scale
}

impl LayoutBox {

    pub fn sql(&self) -> String {
        let width = self.width_entry.text();
        let height = self.width_entry.text();
        let split = if self.toggle_unique.is_active() {
            "unique"
        } else if self.toggle_vertical.is_active() {
            "vertical"
        } else if self.toggle_horizontal.is_active() {
            "horizontal"
        } else if self.toggle_three_top.is_active() {
            "three-top"
        } else if self.toggle_three_left.is_active() {
            "three-left"
        } else if self.toggle_three_right.is_active() {
            "three-right"
        } else if self.toggle_three_bottom.is_active() {
            "three-bottom"
        } else if self.toggle_four.is_active() {
            "four"
        } else {
            "unique"
        };
        let hratio = self.hratio_scale.value();
        let vratio = self.vratio_scale.value();
        format!(r#"
            select json_build_object(
                'width', {},
                'height', {},
                'hratio', {},
                'vratio', {},
                'split', '{}'
            )"#, width, height, hratio, vratio, split
        )
    }

    pub fn build() -> Self {
        let layout_title_bx = PackedImageLabel::build("folder-templates-symbolic", "Layout");
        configure_title(&layout_title_bx.bx);

        let bx = Box::new(Orientation::Vertical, 0);
        bx.set_halign(Align::Center);
        bx.set_hexpand(false);
        bx.set_vexpand(false);
        bx.set_valign(Align::Center);
        bx.append(&layout_title_bx.bx);
        let layout_list = ListBox::new();
        crate::ui::configure_list(&layout_list);
        layout_list.set_halign(Align::Center);
        bx.append(&layout_list);
        let dim_row = ListBoxRow::new();
        let layout_toggle_bx = Box::new(Orientation::Horizontal, 0);
        layout_toggle_bx.style_context().add_class("linked");
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
            layout_toggle_bx.append(*btn);
            if ix >= 1 {
                btn.set_group(Some(toggles[0]));
                // btn.style_context().add_class("linked");

            }
            btn.style_context().add_class("flat");
        }
        toggle_unique.set_active(true);

        let split_row = ListBoxRow::new();
        split_row.set_selectable(false);
        split_row.set_activatable(false);
        let split_bx = NamedBox::new("Layout", Some("Spatial distribution of plots through the area"), layout_toggle_bx);

        split_row.set_child(Some(&split_bx.bx));
        layout_list.append(&split_row);

        let dim_bx_inner = Box::new(Orientation::Horizontal, 0);
        dim_bx_inner.style_context().add_class("linked");
        let width_entry = Entry::builder().primary_icon_name("scale-horizontal-symbolic").max_width_chars(10)
            .input_purpose(InputPurpose::Digits).placeholder_text("Width (px)").build();
        let height_entry = Entry::builder().primary_icon_name("scale-vertical-symbolic").max_width_chars(10)
            .input_purpose(InputPurpose::Digits).placeholder_text("Height (px)").build();
        dim_bx_inner.append(&width_entry);
        dim_bx_inner.append(&height_entry);

        let split_bx_inner = Box::new(Orientation::Horizontal, 0);
        split_bx_inner.style_context().add_class("linked");
        let hratio_scale = Scale::new(Orientation::Horizontal, Some(&Adjustment::builder().lower(0.0).upper(1.0).step_increment(0.1).build()));
        let vratio_scale = Scale::new(Orientation::Horizontal, Some(&Adjustment::builder().lower(0.0).upper(1.0).step_increment(0.1).build()));
        for scale in [&hratio_scale, &vratio_scale] {
            scale.set_hexpand(true);
            scale.set_width_request(128);
            scale.set_draw_value(true);
            scale.set_value_pos(PositionType::Left);
            split_bx_inner.append(scale);
        }

        let dim_row = ListBoxRow::new();
        dim_row.set_selectable(false);
        dim_row.set_activatable(false);
        let dim_bx = NamedBox::new("Dimensions", Some("Dimensions (in pixels) of exported plot"), dim_bx_inner);
        dim_row.set_child(Some(&dim_bx.bx));
        layout_list.append(&dim_row);

        let split_row = ListBoxRow::new();
        split_row.set_selectable(false);
        split_row.set_activatable(false);
        let split_bx = NamedBox::new("Separation", Some("Distribution (in %) of vertical and horizontal areas"), split_bx_inner);

        split_row.set_child(Some(&split_bx.bx));
        layout_list.append(&split_row);

        Self {
            bx,
            width_entry,
            height_entry,
            toggle_unique,
            toggle_vertical,
            toggle_horizontal,
            toggle_three_top,
            toggle_three_left,
            toggle_three_right,
            toggle_three_bottom,
            toggle_four,
            hratio_scale,
            vratio_scale
        }
    }

}

#[derive(Debug, Clone)]
pub struct GraphWindow {
    win : Window,
    layout : LayoutBox,
    design : DesignBox,
    btn_clear : Button,
    btn_sql : Button,
    pub btn_plot : Button,
    plot_rows : Rc<RefCell<Vec<PlotRow>>>,
    objs : Rc<RefCell<Vec<DBObject>>>,
    cols_model : Rc<RefCell<Option<ListStore>>>
}

impl GraphWindow {

    pub fn plot_sql(&self) -> String {
        let rows = self.plot_rows.borrow();
        let mut entries = Vec::new();
        for r in rows.iter() {
            for m in r.mappings.borrow().iter() {
                entries.push(m.data.clone());
            }
        }
        let nc = nested_columns(&entries);
        let ns = nested_column_sql(&nc);
        let cte_arg : String = ns.values().cloned().collect::<Vec<_>>().join(",\n");
        let src_expr : String = ns.keys().cloned().collect::<Vec<_>>().join("CROSS JOIN ");
        let layout_expr = self.layout.sql();
        let design_expr = self.design.sql();
        let plots_expr : String = rows.iter().cloned().map(|r| r.sql() ).collect::<Vec<_>>().join(",\n");
        let panel_expr = format!(
            "json_build_object('plots', array[{}], 'design', {}, 'layout', {})",
            plots_expr,
            design_expr,
            layout_expr
        );
        if cte_arg.is_empty() {
            format!("SELECT {};", panel_expr)
        } else {
            format!("WITH {}\nSELECT {}\nFROM {};", cte_arg, panel_expr, src_expr)
        }
    }

    pub fn build() -> Self {
        let win = Window::new();
        super::configure_dialog(&win, false);
        win.set_title(Some("Graph editor"));
        win.set_width_request(800);
        win.set_height_request(600);

        let scroll = ScrolledWindow::new();
        let bx = Box::new(Orientation::Vertical, 0);
        scroll.set_child(Some(&bx));
        win.set_child(Some(&scroll));

        let layout = LayoutBox::build();
        bx.append(&layout.bx);

        let design = DesignBox::build();
        bx.append(&design.bx);

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

        let cols_model = Rc::new(RefCell::new(None));
        let pr = PlotRow::build("Plot", &cols_model);

        let middle_bx = Box::new(Orientation::Vertical, 18);
        middle_bx.append(&pr.bx);

        let plot_rows = Rc::new(RefCell::new(Vec::new()));
        plot_rows.borrow_mut().push(pr.clone());

        let toggles = [
            (&["Plot"][..], &layout.toggle_unique),
            (&["Top plot", "Bottom plot"][..], &layout.toggle_vertical),
            (&["Left plot", "Right plot"][..], &layout.toggle_horizontal),
            (&["Top plot", "Bottom left plot", "Bottom right plot"][..], &layout.toggle_three_top),
            (&["Left plot", "Top right plot", "Bottom right plot"][..], &layout.toggle_three_left),
            (&["Top left plot", "Bottom left plot", "Right plot"][..], &layout.toggle_three_right),
            (&["Top left plot", "Top right plot", "Bottom plot"][..], &layout.toggle_three_bottom),
            (&["Top left plot", "Top right plot", "Bottom left plot", "Bottom right plot"][..], &layout.toggle_four)
        ];

        for (plots, tgl) in toggles {
            let plot_rows = plot_rows.clone();
            let middle_bx = middle_bx.clone();
            let cols_model = cols_model.clone();
            tgl.connect_toggled(move|btn| {
                if btn.is_active() {
                    let mut plot_rows = plot_rows.borrow_mut();

                    // Clear
                    for pl in &*plot_rows {
                        middle_bx.remove(&pl.bx);
                    }
                    plot_rows.clear();

                    // Append new
                    for pl in plots {
                        let pr = PlotRow::build(pl, &cols_model);
                        middle_bx.append(&pr.bx);
                        plot_rows.push(pr);
                    }
                }
            });
        }
        layout.toggle_unique.set_active(true);

        // list.append(&pr.exp);
        // crate::ui::configure_list(&list);
        // bx.append(&top_bx);
        bx.append(&middle_bx);
        bx.append(&bottom_bx);

        Self {
            win,
            btn_clear,
            btn_sql,
            btn_plot,
            layout,
            design,
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
                    update_completion_with_schema(objs.clone(), cols_model.clone(), Some(info.schema));
                    update_plot_rows_with_model(pl_rows.clone(), cols_model.clone());
                }
            }
        });
        conn.connect_schema_update({
            let objs = self.objs.clone();
            let pl_rows = self.plot_rows.clone();
            let cols_model = self.cols_model.clone();
            move |schema| {
                update_completion_with_schema(objs.clone(), cols_model.clone(), schema);
                update_plot_rows_with_model(pl_rows.clone(), cols_model.clone());
            }
        });
    }

}

fn update_plot_rows_with_model(pl_rows : Rc<RefCell<Vec<PlotRow>>>, cols_model : Rc<RefCell<Option<ListStore>>>) {
    if let Some(model) = &*cols_model.borrow() {
        for pl in pl_rows.borrow().iter() {
            pl.visit_data_entries(|e| {
                add_completion(&e, &model);
            });
        }
    } else {
        for pl in pl_rows.borrow().iter() {
            pl.visit_data_entries(|e| {
                e.set_completion(None);
            });
        }
    }
}

pub fn update_completion_with_schema(
    objs : Rc<RefCell<Vec<DBObject>>>,
    cols_model : Rc<RefCell<Option<ListStore>>>,
    schema : Option<Vec<DBObject>>
) {
    let mut objs = objs.borrow_mut();
    let mut cols_model = cols_model.borrow_mut();
    objs.clear();
    if let Some(schema) = schema {
        let col_types: [glib::Type; 2] = [Pixbuf::static_type(), glib::Type::STRING];
        let model = ListStore::new(&col_types);
        let mut data = Vec::new();
        for new_obj in &schema {
            match &new_obj {
                DBObject::Schema { name, children, .. } => {
                    let schema_name = &name;
                    for child in children.iter() {
                        match child {
                            DBObject::Table { name, cols, .. } => {
                                for col in cols.iter() {
                                    if &schema_name[..] == "public" {
                                        data.push(format!("{}.{}", name, col.name));
                                    } else {
                                        data.push(format!("{}.{}.{}", schema_name, name, col.name));
                                    }
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
            model.set(&model.append(), &[
                (0, &Pixbuf::from_resource("/io/github/limads/queries/icons/scalable/actions/table-symbolic.svg").unwrap()),
                (1, d),
            ]);
        }

        // Any mappings added later will use this information.
        *objs = schema;
        *cols_model = Some(model);
    } else {
        *cols_model = None;
    }
}

pub fn add_completion(e : &Entry, model : &ListStore) -> EntryCompletion {
    let compl = EntryCompletion::new();
    let pix_renderer = CellRendererPixbuf::new();
    pix_renderer.set_padding(6, 6);
    compl.pack_start(&pix_renderer, false);
    compl.add_attribute(&pix_renderer, "pixbuf", 0);
    let txt_renderer = CellRendererText::new();
    compl.pack_start(&txt_renderer, true);
    compl.add_attribute(&txt_renderer, "text", 1);
    compl.set_model(Some(model));
    compl.set_property("text-column", 1);
    compl.set_minimum_key_length(1);
    compl.set_popup_completion(true);
    e.set_completion(Some(&compl));
    compl
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
        let color_bx  = LabeledColorBtn::build("Color");
        let radius_bx = LabeledScale::build("Radius", 1.0, 20.0, 1.0);
        let bx = Box::new(Orientation::Horizontal, 6);
        bx.append(&color_bx.bx);
        bx.append(&radius_bx.bx);
        Self { color_btn : color_bx.btn, radius_scale : radius_bx.scale, bx }
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
        let color_bx  = LabeledColorBtn::build("Color");
        let font_bx  = LabeledFontBtn::build("Font");
        let bx = Box::new(Orientation::Horizontal, 6);
        bx.append(&color_bx.bx);
        bx.append(&font_bx.bx);
        Self { color_btn : color_bx.btn, font_btn : font_bx.btn, bx }
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
        let color_bx  = LabeledColorBtn::build("Color");
        let opacity_bx = LabeledScale::build("Opacity", 0.0, 100.0, 1.0);
        let bx = Box::new(Orientation::Horizontal, 6);
        bx.append(&color_bx.bx);
        bx.append(&opacity_bx.bx);
        Self { color_btn : color_bx.btn, opacity_scale : opacity_bx.scale, bx }
    }

}

#[derive(Debug, Clone)]
pub struct IntervalBox {
    bx : Box,
    color_btn : ColorButton,
    width_scale : Scale,
    vertical_switch : Switch,
    spacing_scale : Scale,
    limits_scale : Scale
}

impl IntervalBox {

    pub fn build() -> Self {
        let bx = Box::new(Orientation::Horizontal, 6);
        let color_bx = LabeledColorBtn::build("Line color");
        let width_bx = LabeledScale::build("Width", 0.0, 10.0, 1.0);
        let vertical_bx = LabeledSwitch::build("Vertical");
        let spacing_bx = LabeledScale::build("Spacing", 0.0, 100.0, 1.0);
        let limits_bx = LabeledScale::build("Limits", 0.0, 100.0, 1.0);
	    bx.append(&color_bx.bx);
        bx.append(&width_bx.bx);
        bx.append(&vertical_bx.bx);
        bx.append(&spacing_bx.bx);
        bx.append(&limits_bx.bx);
        Self {
            bx,
            color_btn : color_bx.btn,
            vertical_switch : vertical_bx.switch,
            width_scale : width_bx.scale,
            spacing_scale : spacing_bx.scale,
            limits_scale : limits_bx.scale
        }
    }

}

#[derive(Debug, Clone)]
pub struct BarBox {
    bx : Box,
    //bx_limits : Box,
    color_btn : ColorButton,
    center_switch : Switch,
    vertical_switch : Switch,
    width_scale : Scale,
    origin_entry : Entry,
    // origin_y_entry : Entry,
    spacing_entry : Entry
}

impl BarBox {

    pub fn build() -> Self {
        let color_bx = LabeledColorBtn::build("Color");
        let center_bx = LabeledSwitch::build("Centered");
        let vertical_bx = LabeledSwitch::build("Horizontal");
        let width_bx = LabeledScale::build("Bar width", 0.0, 1.0, 0.1);
        let origin_entry = Entry::builder().placeholder_text("Origin")
            .primary_icon_name("scale-left-symbolic").max_width_chars(8).hexpand(true).build();
        //let origin_y_entry = Entry::builder().placeholder_text("Y origin")
        //    .primary_icon_name("scale-inferior-symbolic").max_width_chars(8).hexpand(true).build();
        let spacing_entry = Entry::builder().placeholder_text("Spacing")
            .primary_icon_name("scale-horizontal-symbolic").max_width_chars(8).hexpand(true).build();
        let bx = Box::new(Orientation::Horizontal, 0);
        bx.append(&color_bx.bx);
        bx.append(&center_bx.bx);
        bx.append(&vertical_bx.bx);
        bx.append(&width_bx.bx);
        //let bx_limits = Box::new(Orientation::Horizontal, 0);
        //bx_limits.style_context().add_class("linked");
        //bx_limits.append(&origin_x_entry);
        //bx_limits.append(&origin_y_entry);
        // bx_limits.append(&spacing_entry);
        Self {
            bx,
            //bx_limits,
            color_btn : color_bx.btn,
            center_switch : center_bx.switch,
            vertical_switch : vertical_bx.switch,
            width_scale : width_bx.scale,
            origin_entry,
            // origin_y_entry,
            spacing_entry
        }
    }

}



