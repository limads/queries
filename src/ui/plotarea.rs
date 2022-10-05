/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use gtk4::*;
use gtk4::prelude::*;
use cairo::Context;
use papyri::render::{Panel};

#[derive(Clone)]
pub struct PlotView {

    pub parent : gtk4::DrawingArea

}

impl PlotView {

    pub fn redraw(&self) {
        self.parent.queue_draw();
    }

    pub fn new_from_panel(mut panel : Panel) -> Self {
        let parent = gtk4::DrawingArea::new();
        parent.set_draw_func(move |da, ctx, _, _| {
            let allocation = da.allocation();
            let w = allocation.width();
            let h = allocation.height();
            if let Err(e) = panel.draw_to_context(&ctx, 0, 0, w, h) {
                eprintln!("{}", e);
            }
        });
        Self { parent }
    }

    pub fn new_from_json(json : &str) -> Result<Self, String> {
        let panel = Panel::new_from_json(json)?;
        Ok(Self::new_from_panel(panel))
    }

}

fn _draw_plot(da : &gtk4::DrawingArea, ctx : &Context, group : &mut Panel) {
    let allocation = da.allocation();
    let w = allocation.width();
    let h = allocation.height();
    if let Err(e) = group.draw_to_context(&ctx, 0, 0, w, h) {
        eprintln!("{}", e);
    }
}

