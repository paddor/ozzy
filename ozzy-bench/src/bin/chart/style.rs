//! Match OMQ's `chart/common.rs` and `chart/pushpull_compression.rs` style.
use ozzy_bench::automation::Result;
use plotters::prelude::*;
use std::{fmt::Write, path::Path};

pub(super) const BACKGROUND: RGBColor = RGBColor(0, 0, 0);
pub(super) const GRID: RGBColor = RGBColor(55, 65, 81);
pub(super) const AXIS: RGBColor = RGBColor(156, 163, 175);
pub(super) const TEXT: RGBColor = RGBColor(229, 231, 235);
pub(super) const MUTED: RGBColor = RGBColor(156, 163, 175);
pub(super) const CONTENT_WIDTH: u32 = 800;
// Leave room for long footer labels beyond the two panel columns.
pub(super) const WIDTH: u32 = CONTENT_WIDTH + 40;
pub(super) const PANEL_HEIGHT: u32 = 280;
pub(super) const ROW_GAP: u32 = 60;
pub(super) const TOP: u32 = 56;

#[derive(Clone, Copy)]
pub(super) struct Series {
    pub id: &'static str,
    pub label: &'static str,
    pub color: RGBColor,
}

pub(super) const SERIES: [Series; 3] = [
    Series {
        id: "iggy/raw",
        label: "Iggy",
        color: RGBColor(250, 204, 21),
    },
    Series {
        id: "redpanda/raw",
        label: "Redpanda",
        color: RGBColor(74, 222, 128),
    },
    Series {
        id: "ozzy/raw",
        label: "Ozzy",
        color: RGBColor(239, 68, 68),
    },
];

pub(super) fn nice_step(max: f64, target: usize) -> f64 {
    if max <= 0.0 {
        return 1.0;
    }
    let raw = max / target as f64;
    let magnitude = 10.0_f64.powf(raw.log10().floor());
    for scale in [1.0, 2.0, 2.5, 5.0, 10.0] {
        let step = scale * magnitude;
        if max / step <= target as f64 + 1.0 {
            return step;
        }
    }
    magnitude * 10.0
}

pub(super) fn size(bytes: u64) -> String {
    if bytes >= 1024 && bytes.is_multiple_of(1024) {
        format!("{} KiB", bytes / 1024)
    } else {
        format!("{bytes} B")
    }
}

pub(super) fn records_per_second(value: f64) -> String {
    if value >= 1e6 {
        let n = value / 1e6;
        if (n - n.round()).abs() < 0.05 {
            format!("{n:.0}M/s")
        } else {
            format!("{n:.1}M/s")
        }
    } else if value >= 1e3 {
        format!("{:.0}K/s", value / 1e3)
    } else {
        format!("{value:.0}/s")
    }
}

fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

pub(super) fn finish(
    path: &Path,
    height: u32,
    title: &str,
    subtitle: &str,
    panels: &[(u32, u32, String)],
    rows: &[&serde_json::Value],
) -> Result<()> {
    let mut svg = std::fs::read_to_string(path)?;
    svg = svg.replacen(
        &format!("<svg width=\"{WIDTH}\" height=\"{height}\" viewBox=\"0 0 {WIDTH} {height}\""),
        &format!("<svg viewBox=\"0 0 {WIDTH} {height}\""),
        1,
    );
    svg = svg.replacen(
        "xmlns=\"http://www.w3.org/2000/svg\"",
        "xmlns=\"http://www.w3.org/2000/svg\" font-family=\"system-ui, -apple-system, sans-serif\"",
        1,
    );
    let mut header = format!(
        "\n<text x=\"400\" y=\"17\" text-anchor=\"middle\" font-family=\"sans-serif\" font-size=\"14\" font-weight=\"bold\" fill=\"#F9FAFB\">{}</text>",
        escape(title)
    );
    write!(
        header,
        "\n<text x=\"400\" y=\"31\" text-anchor=\"middle\" font-family=\"sans-serif\" font-size=\"10\" fill=\"#9CA3AF\">{}</text>",
        escape(subtitle)
    )?;
    let start = svg.find("<rect").ok_or("missing SVG background")?;
    let end = start + svg[start..].find("/>").ok_or("invalid SVG background")? + 2;
    svg.insert_str(end, &header);
    svg = svg.replace("r=\"2\"", "r=\"2.5\"");
    let mut titles = String::new();
    for (x, y, label) in panels {
        write!(
            titles,
            "\n<text x=\"{x}\" y=\"{y}\" text-anchor=\"middle\" font-family=\"sans-serif\" font-size=\"13\" font-weight=\"bold\" fill=\"#F9FAFB\">{}</text>",
            escape(label)
        )?;
    }
    let end = svg.rfind("</svg>").ok_or("missing SVG end")?;
    svg.insert_str(end, &titles);
    super::coverage::stamp(&mut svg, rows)?;
    std::fs::write(path, svg)?;
    Ok(())
}
