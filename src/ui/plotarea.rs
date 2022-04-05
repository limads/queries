use gtk4::*;
use gtk4::prelude::*;
// use std::rc::{Weak, Rc};
// use crate::plots::PlotSet;
use cairo::Context;
use papyri::render::{Panel, GroupSplit};
use std::collections::HashMap;
// use gtk::prelude::*;

/*struct PlotState {
    plot_group : Panel,
    active_area : usize,
    layout_path : String
}*/

#[derive(Clone)]
pub struct PlotView {

    // pub plot_group : Rc<Panel:,

    //pub plot_area : PlotArea,
    pub parent : gtk4::DrawingArea,

    // active_area : usize,
    // layout_path : String
}

/*pub enum UpdateContent {

    Dimensions(Option<usize>, Option<usize>),

    /// Used to evaluate plot-specific characteristics.
    /// Layout path, Layout value
    Layout(String, String),

    /// Used to evaluate characteristics shared by all plots.
    /// Design path, design value
    Design(String, String),

    /// Mapping name, position values
    Data(String, Vec<Vec<f64>>),

    /// Mapping name, position values, text values
    TextData(String, Vec<Vec<f64>>, Vec<String>),

    /// Mapping name; Mapping type; Mapping source; mapping column names; plot ix
    NewMapping(String, String, String, Vec<String>, usize),

    // Mapping id; Column names;
    ColumnNames(String, Vec<String>),

    // Mapping id; source
    Source(String, String),

    /// Mapping plot index, mapping id
    RemoveMapping(usize, String),

    /// Mapping id; New id; New type; mapping source
    EditMapping(String, String, String, String),

    AspectRatio(Option<f64>, Option<f64>),

    // Pass (old, new) mapping name
    // RenameMapping(String, String),

    /// Clears all data and displays layout at the informed path
    Clear(String),

    /// Clears all data, but preserving the current layout path.
    Erase,

    // Clears only the data, preserving all current plot properties.
    ClearData,

    /// Old plot, old name, new plot
    ReassignPlot((usize, String, usize))
}*/

fn draw_plot(da : &gtk4::DrawingArea, ctx : &Context, group : &mut Panel) {
    let allocation = da.allocation();
    let w = allocation.width;
    let h = allocation.height;
    group.draw_to_context(&ctx, 0, 0, w, h);
    // if let Ok(mut pl) = plot_ref.try_borrow_mut() {
    // }
}

/*/// Use this to bind the draw signal of the DrawArea to a set of
/// plot models, instead of a single one. This is required to avoid
/// having a nested Rc vector. We instead use the allocation of the
/// full plot set, but  in reality we will only use the data of the
/// current plot, which index is assumed constant in the in the set
/// at the momment the signal is bound. Thew new plot view is assumed
/// to not yet have been inserted into weak_set, and insertion is assumed
/// to happen just afterwards.
pub fn connect_draw_to_set(new : &PlotView, weak_set : Weak<RefCell<PlotSet>>) {

    // let weak_set = Rc::downgrade(&plot_set);
    println!("Plot set strong count = {}; Weak count = {}", weak_set.strong_count(), weak_set.weak_count());
    let new_ix = weak_set.upgrade().unwrap().try_borrow().map(|set| set.len() ).unwrap();

    // if let Ok (pl_mut) = plot_view.try_borrow_mut() {
    new.parent.connect_draw(move |da, ctx| {
        if let Some(mut plots) = weak_set.upgrade() {
            if let Some(mut plot) = plots.borrow_mut().get_mut(new_ix) {
                draw_plot(da, ctx, &mut plot.plot_group);
            } else {
                println!("No plot to be drawn at index {}", new_ix);
            }
        } else {
            println!("Unable to upgrade plot view so it can be drawn");
        }
        glib::signal::Inhibit(true)
    });

    // } else {
    //    println!("Error in getting mutable reference to plot_group");
    // }
}*/

/*fn connect_draw(plot_view : &PlotView) {

    /*let da = if let Ok(pl) = plot_view.try_borrow() {
        pl.parent.clone()
    } else {
        panic!("Unable to borow plot view")
    };*/

    // if let Ok (pl_mut) = plot_view.try_borrow_mut() {
    // let plot_view = plot_view.clone();
    let panel = plot_view.plot_group.clone();
    plot_view.parent.connect_draw(move |da, ctx| {
        // if let Ok(mut pl) = plot_view.try_borrow_mut() {
        draw_plot(da, ctx, &mut panel);
        // } else {
        //    println!("Unable to borrow plot view");
        // }
        glib::signal::Inhibit(true)
    });

    // } else {
    //    println!("Error in getting mutable reference to plot_group");
    // }
}*/

impl PlotView {

    // pub fn do_this(self : Weak<Self>) {
    // }

    pub fn redraw(&self) {
        self.parent.queue_draw();
    }

    pub fn new_from_panel(mut panel : Panel) -> Self {
        let parent = gtk4::DrawingArea::new();
        parent.set_draw_func(move |da, ctx, _, _| {
            let allocation = da.allocation();
            let w = allocation.width;
            let h = allocation.height;
            panel.draw_to_context(&ctx, 0, 0, w, h);
        });
        Self { parent }
    }

    pub fn new_from_json(json : &str) -> Result<Self, String> {
        let mut panel = Panel::new_from_json(json)?;
        Ok(Self::new_from_panel(panel))
    }

    /*/* Starts a new PlotView with an enclosed DrawingArea */
    pub fn new(layout_path : &str) -> Rc<RefCell<PlotView>> {
        let draw_area = gtk::DrawingArea::new();
        PlotView::new_with_draw_area(layout_path, draw_area)
    }

    pub fn group_split(&self) -> GroupSplit {
        self.plot_group.group_split()
    }

    pub fn aspect_ratio(&self) -> (f64, f64) {
        self.plot_group.aspect_ratio()
    }

    pub fn view_dimensions(&self) -> (u32, u32) {
        self.plot_group.view_dimensions()
    }

    pub fn n_plots(&self) -> usize {
        self.plot_group.n_plots()
    }

    pub fn change_active_area(&mut self, area : usize) {
        self.active_area = area;
    }

    pub fn get_active_area(&self) -> usize {
        self.active_area
    }

    pub fn set_active_area(&mut self, active : usize) {
        self.active_area = active;
    }

    /* If you want to add the PlotDrawing behavior to an
    already instantiated draw area (i.e. built from glade) */
    pub fn new_with_draw_area(
        layout_path : &str,
        draw_area : gtk::DrawingArea,
    ) -> Rc<RefCell<PlotView>> {
        // println!("Layout path = {}", layout_path);
        // let plot_group = Panel::new(String::from(layout_path)).unwrap();
        let plot_group = Default::default();
        let plot_view = Rc::new(RefCell::new(
            PlotView{plot_group, parent : draw_area, active_area : 0, layout_path : layout_path.into() }));
        connect_draw_to_single(&plot_view);
        plot_view
    }

    /// Returns information for the current active scale as a HashMap of (Property, Value).
    pub fn current_scale_info(&self, scale : &str) -> HashMap<String, String> {
        self.plot_group.scale_info(self.active_area, scale)
    }

    /// For each mapping in the current active area, return a tuple with (name, type, properties).
    pub fn mapping_info(&self) -> Vec<(String, String, HashMap<String,String>)> {
        self.plot_group.mapping_info(self.active_area)
    }

    fn insert_mapping(&mut self, ix : usize, m_name : String, m_type : String, m_source : String, col_names : Vec<String>) {
        /*let maybe_update = self.plot_group.add_mapping(
            ix,
            m_name.to_string(),
            m_type.to_string(),
            m_source.to_string(),
            col_names
        );
        if let Err(e) = maybe_update {
            println!("Error adding new mapping: {}", e);
        }*/
    }

    pub fn update(&mut self, content : &mut UpdateContent) -> Result<(), String> {

        /*//if let Ok(mut ref_area) = self.plot_area.try_borrow_mut() {
        let active = self.active_area;
        match content {
            UpdateContent::Dimensions(w, h) => {
                self.plot_group.set_dimensions(*w, *h);
            },
            UpdateContent::Layout(key, property) => {
                self.plot_group.update_plot_property(active, &key, &property);
                /*if self.plot_area.reload_layout_data().is_err() {
                    println!(
                        "Error updating property {:?} with value {:?}",
                            key, property);
                }*/
                self.parent.queue_draw();
            },
            UpdateContent::Design(key, property) => {
                self.plot_group.update_design(&key, &property);
                self.parent.queue_draw();
            },
            UpdateContent::Data(key, data) => {
                // println!("Key {} at active area {} received new data", key, self.active_area);
                // self.plot_group.update_mapping_with_adjustment(active, key, data.to_vec(), Adjustment::Tight);
                // self.plot_group.update_mapping(active, key, data.to_vec());
                self.parent.queue_draw();
            },
            UpdateContent::TextData(key, pos, text) => {
                //self.plot_group.update_text_mapping_with_adjustment(active, key, pos.to_vec(), text.to_vec(), Adjustment::Tight);
                // self.plot_group.update_text_mapping(active, key, pos.to_vec(), text.to_vec());
                self.parent.queue_draw();
            },
            UpdateContent::ColumnNames(m_name, cols) => {
                if let Err(e) = self.plot_group.update_mapping_columns(active, &m_name, cols.to_vec()) {
                    println!("{}", e);
                }
            },
            UpdateContent::ClearData => {
                self.plot_group.clear_all_data();
            },
            UpdateContent::Source(m_name, source) => {
                if let Err(e) = self.plot_group.update_source(active, &m_name, source.clone()) {
                    println!("{}", e);
                };
            },
            UpdateContent::NewMapping(m_name, m_type, source, col_names, plot_ix) => {
                self.insert_mapping(*plot_ix, m_name.clone(), m_type.clone(), source.clone(), col_names.clone());
                self.parent.queue_draw();
            },
            UpdateContent::EditMapping(m_name, new_name, new_type, source) => {
                // self.plot_group.remove_mapping(active, m_name);
                // self.insert_mapping(active, new_name.clone(), new_type.clone(), source.clone());
                // self.parent.queue_draw();
            },
            UpdateContent::AspectRatio(opt_h, opt_v) => {
                self.plot_group.set_aspect_ratio(*opt_h, *opt_v);
                self.parent.queue_draw();
            }
            UpdateContent::RemoveMapping(pl_ix, m_name) => {
                self.plot_group.remove_mapping(*pl_ix, m_name);
                self.parent.queue_draw();
            },
            //UpdateContent::RenameMapping(old, new) => {
            //},
            UpdateContent::Clear(path) => {
                if let Err(e) = self.plot_group.load_layout(path.clone()) {
                    println!("{}", e);
                } else {
                    self.layout_path = path.to_string();
                }
                self.parent.queue_draw();
            },
            UpdateContent::Erase => {
                if let Err(e) = self.plot_group.load_layout(self.layout_path.clone()) {
                    println!("{}", e);
                }
                self.parent.queue_draw();
            },
            UpdateContent::ReassignPlot((old, name, new)) => {
                if let Err(e) = self.plot_group.reassign_plot(*old, &name[..], *new) {
                    println!("{}", e);
                }
                self.parent.queue_draw();
            }
        }*/
        Ok(())
    }*/
}

