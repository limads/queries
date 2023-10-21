/*Copyright (c) 2023 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.
For a copy, see http://www.gnu.org/licenses.*/

use serde::{Serialize, Deserialize};
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
use papyri::model::*;
use super::plotarea::*;

/* Analyze panel */

// explain (format json) select * from tbl;
// explain analyze select * from tbl;

/* Bar graph will contain relation name at the top/right; then the execution time as the
bar width/height. */

// Parse the output as Vec<Explain>, since the top-level JSON is an array.

/* Graph will contain basic info (node type, relation name (#rows, N bytes) execution time).
TreeView at the side will contain more detailed information hidden under expanders. */

pub struct ExplainPanel {

    tree_view : TreeView,

    dia : Picture,

    plot : PlotView,

    // pub bx : Box
    pub paned : Paned

}

impl ExplainPanel {

    pub fn new(expl : Explain) -> Self {
        // let bx = Box::new(Orientation::Horizontal, 0);
        let paned = Paned::new(Orientation::Horizontal);
        // paned.set_shrink_end_child(true);
        // paned.set_shrink_start_child(false);
        let tree_view = TreeView::new();
        let store = super::schema_tree::configure_tree_view(&tree_view);
        let scroll = ScrolledWindow::new();
        // super::set_margins(&scroll, 32,78);
        scroll.set_width_request(320);
        scroll.set_child(Some(&tree_view));
        scroll.set_hexpand(true);
        scroll.set_vexpand(true);

        /*let provider = CssProvider::new();
        let css = if libadwaita::StyleManager::default().is_dark() {
            "* { border : 1px solid #454545; } "
        } else {
            "* { border : 1px solid #d9dada; } "
        };
        provider.load_from_data(css);
        scroll.style_context().add_provider(&provider, 800);*/

        let bx_upper = Box::new(Orientation::Horizontal, 0);
        bx_upper.set_vexpand(true);
        let dia = Picture::new();

        dia.set_can_shrink(false);
        dia.set_halign(Align::Center);
        dia.set_valign(Align::Center);
        dia.set_vexpand(true);
        dia.set_hexpand(true);

        super::set_margins(&dia, 32,32);
        let plot = PlotView::new_from_panel(papyri::render::Panel::new_from_model(plan_plot(&expl.plan)).unwrap());
        super::set_margins(&plot.parent, 32,32);
        plot.parent.set_width_request(720);
        plot.parent.set_height_request(360);
        plot.parent.set_vexpand(false);
        plot.parent.set_valign(Align::Center);
        plot.parent.set_hexpand(false);
        scroll.set_child(Some(&dia));
        paned.set_start_child(Some(&scroll));
        paned.set_end_child(Some(&plot.parent));

        let mut s = String::new();
        add_node_to_diagram(&mut s, &expl.plan, 0, 0, None);
        let font_color = if libadwaita::StyleManager::default().is_dark() {
            "#f9f9f9".to_string()
        } else {
            "#363636".to_string()
        };
        let fill_color = if libadwaita::StyleManager::default().is_dark() {
            "#404040".to_string()
        } else {
            "#ffffff".to_string()
        };
        let bg_color = if libadwaita::StyleManager::default().is_dark() {
            "#242424".to_string()
        } else {
            "#fafafa".to_string()
        };
        let diagram = format!(r##"
            digraph {{
                dpi=96;
                rank="LR";
                bgcolor="{bg_color}";
                node [
                    fontname = "Ubuntu Mono",
                    style="filled",
                    color="#bbbbbb",
                    fillcolor="{fill_color}",
                    fontcolor="{font_color}",
                    fontsize=12.0,
                    margin=0.025
                ];
                edge [color="#bbbbbb"];
                {s}
            }}
        "##);
        crate::ui::model::render_to_image(&dia, &diagram[..]);
        paned.connect_realize(|p| {
            p.set_position((p.allocation().width() - 720).max(720));
        });
        Self { tree_view, dia, plot, paned }
    }

}

const MIN_SPACE : usize = 8;

fn plan_properties(plan : &Plan) -> String {
    let mut lines = Vec::new();
    lines.push(format!("Rows: {} Columns: {}", plan.rows, plan.width));
    lines.push(format!("Cost: {} (startup: {})", plan.total_cost, plan.startup_cost));
    let inner_unique = plan.inner_unique.map(|p| if p { "Yes".to_string() } else { "No" .to_string() });
    let parallel_aware = plan.parallel_aware.map(|p| if p { "Yes".to_string() } else { "No" .to_string() });
    let opt_keys = [
        ("Index condition", plan.index_cond.clone()),
        ("Hash condition", plan.hash_cond.clone()),
        ("Scan direction", plan.scan_direction.clone()),
        ("Index name", plan.index_name.clone()),
        ("Parallel aware",  parallel_aware),
        ("Sort key", plan.sort_key.clone().map(|s| s.join(", ") )),
        ("Parent relationship", plan.parent_relationship.clone()),
        ("Join type", plan.join_type.clone()),
        ("Inner unique", inner_unique.clone())
    ];
    for (name, key) in opt_keys {
        if let Some(key) = key {
            lines.push(format!("{name}: {key}"));
        }
    }
    lines.join("<br/><br/>")
}

fn add_node_to_diagram(s : &mut String, plan : &Plan, depth : usize, order : usize, parent : Option<&str>) {
    let name = format!("plan_{}_{}", depth, order);
    let qual_name = plan.qualified_name();
    let props = plan_properties(plan);
    let lbl = format!("<b>{qual_name}</b><br/><br/>{props}");
    *s += &format!("{name} [label=<{lbl}>,shape=\"note\"];\n");
    if let Some(parent) = parent {
        *s += &format!("{parent} -> {name};\n");
    }

    if let Some(children) = &plan.plans {
        for (i, child) in children.iter().enumerate() {
            add_node_to_diagram(s, child, depth+1, i, Some(&name));
        }
    }
}

// cost: clock icon
// name (kind) with separate kind icons
// hash join (inner join icon)
// if has relation name or alias, use table icon

fn add_node_to_tree_store(store : &TreeStore, parent : Option<&TreeIter>, node : &Plan) {
    let pxb = Pixbuf::new(gdk_pixbuf::Colorspace::Rgb, true, 8, 16,16).unwrap();
    let iter = store.append(parent);
    store.set(&iter, &[(0, &pxb), (1, &node.qualified_name())]);
    if let Some(plans) = &node.plans {
        for plan in plans {
            add_node_to_tree_store(store, Some(&iter), plan);
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Explain {

    #[serde(rename = "Plan")]
    pub plan : Plan

}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Plan {

    #[serde(rename = "Node Type")]
    pub node_type : String,

    #[serde(rename = "Relation Name")]
    pub relation_name : Option<String>,

    #[serde(rename = "Alias")]
    pub alias : Option<String>,

    #[serde(rename = "Startup Cost")]
    pub startup_cost : f64,

    #[serde(rename = "Total Cost")]
    pub total_cost : f64,

    #[serde(rename = "Plan Rows")]
    pub rows : u32,

    // ?
    // loops : Option<u32>,

    // Row size (bytes)
    #[serde(rename = "Plan Width")]
    pub width : u32,

    #[serde(rename = "Index Cond")]
    pub index_cond : Option<String>,

    #[serde(rename = "Scan Direction")]
    pub scan_direction : Option<String>,

    #[serde(rename = "Index Name")]
    pub index_name : Option<String>,

    #[serde(rename = "Parallel Aware")]
    pub parallel_aware : Option<bool>,

    #[serde(rename = "Sort Key")]
    pub sort_key : Option<Vec<String>>,

    #[serde(rename = "Plans")]
    pub plans : Option<Vec<Plan>>,

    #[serde(rename = "Parent Relationship")]
    pub parent_relationship : Option<String>,

    #[serde(rename = "Join Type")]
    pub join_type : Option<String>,

    #[serde(rename = "Inner Unique")]
    pub inner_unique : Option<bool>,

    #[serde(rename = "Hash Cond")]
    pub hash_cond : Option<String>

}

impl Plan {

    pub fn name(&self) -> String {
        if let Some(r) = &self.relation_name {
            r.clone()
        } else if let Some(a) = &self.alias {
            a.clone()
        } else {
            format!("(unknown)")
        }
    }

    pub fn qualified_name(&self) -> String {
        if self.relation_name.is_some() || self.alias.is_some() {
            format!("{} ({})", self.name(), self.node_type)
        } else {
            format!("{}", self.node_type)
        }
    }

}

fn add_plan_data(
    plan : &Plan,
    cost : &mut Vec<(String, f64, f64)>,
) {
    cost.push((plan.qualified_name(), plan.total_cost, plan.startup_cost));
    if let Some(children) = &plan.plans {
        for child in children {
            add_plan_data(child, cost);
        }
    }
}

fn next_pow10(mut v : u32) -> u32 {
    while v % 10 != 0 {
        v += 1;
    }
    v
}

pub fn plan_plot(plan : &Plan) -> Panel {
    let spacing = 1.0;
    let mut cost = Vec::new();
    add_plan_data(&plan, &mut cost);
    cost.sort_by(|a, b| b.1.total_cmp(&a.1) );
    let names : Vec<_> = cost.iter().map(|c| c.0.clone() ).collect();
    let total_cost : Vec<_> = cost.iter().map(|c| c.1 ).collect();
    let startup_cost : Vec<_> = cost.iter().map(|c| c.2 ).collect();
    let max_cost = *total_cost.iter().max_by(|a,b|a.total_cmp(&b)).unwrap();
    let upper_lim = next_pow10((max_cost + 0.25*max_cost).ceil() as u32).max(1) as f64;
    let n = names.len();
    let label_xs : Vec<_> = (0..n)
	    .map(|i| spacing*(i as f64) + 0.25*spacing )
	    .collect();
	let label_ys : Vec<_> = cost
	    .iter()
	    .map(|c| c.1 + upper_lim*0.05 )
	    .collect();
	let mut labels = papyri::model::Label::builder()
	    .font("Liberation Sans 12".to_owned())
	    .map(label_xs, label_ys, names)
	    .build();
	if libadwaita::StyleManager::default().is_dark() {
	    labels.color = String::from("#f9f9f9ff");
	}
	let total = Bar::builder().width(0.5).color("#92b9d8").map(total_cost).center(false).spacing(spacing).build();
	let startup = Bar::builder().width(0.5).color("#3d6480").map(startup_cost).center(false).spacing(spacing).build();
    let mappings = [
	    Mapping::from(labels),
	    Mapping::from(total),
	    Mapping::from(startup)
    ];
    let sx = papyri::model::Scale::builder()
        .from(-1.0)
        .intervals(0)
        .guide(false)
        .to(spacing*((n+1) as f64))
        .label("Plan node")
        .build();
    let sy = papyri::model::Scale::builder()
        .from(0.0)
        .to(upper_lim)
        .label("Cost")
        .build();
    let mut design = if libadwaita::StyleManager::default().is_dark() {
        papyri::model::Design::default_dark()
    } else {
        papyri::model::Design::default()
    };
    design.font = "Liberation Sans 12".to_string();
    let pl = Plot::builder()
        .x(sx)
        .y(sy)
        .mappings(mappings)
        .build();
    Panel::builder().design(design).plots([pl]).build()
}

