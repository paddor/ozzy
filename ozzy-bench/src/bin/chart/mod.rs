//! OMQ-style SVG charts. Saturation and fixed-load charts share their axes,
//! series marks, legend footer and file framing here.
pub(super) mod coverage;
mod fixed;
mod hardware;
mod legend;
mod marks;
pub(super) mod regression;
pub(super) mod replacement;
mod saturation;
mod style;
pub(super) use fixed::render as render_fixed_load;
pub(super) use saturation::render;

use ozzy_bench::automation::{Result, finite};
use plotters::{
    chart::MeshStyle,
    coord::{Shift, types::RangedCoordf64},
    prelude::*,
};
use serde_json::Value;
use std::path::{Path, PathBuf};
use style::{AXIS, BACKGROUND, FOOTER_HEIGHT, GRID, PanelTitle, SERIES, WIDTH};

type Chart<'a> = ChartContext<'a, SVGBackend<'a>, Cartesian2d<RangedCoordf64, RangedCoordf64>>;
type PlotArea<'a> = DrawingArea<SVGBackend<'a>, Cartesian2d<RangedCoordf64, RangedCoordf64>>;
/// A pixel-addressed region of a chart file.
type Region<'a> = DrawingArea<SVGBackend<'a>, Shift>;
/// Mesh settings of a chart drawn over one region.
type Mesh<'a, 'b, 'c> = MeshStyle<'a, 'b, RangedCoordf64, RangedCoordf64, SVGBackend<'c>>;

#[derive(Clone, Copy)]
enum XAxis {
    Size,
    Rate,
}

impl XAxis {
    fn key(self) -> &'static str {
        match self {
            Self::Size => "size",
            Self::Rate => "rate",
        }
    }

    fn label(self, value: u64) -> String {
        match self {
            Self::Size => style::size(value),
            Self::Rate => style::records_per_second(value as f64),
        }
    }

    /// Sizes sit at evenly spaced indexes of `values`; rates on a log scale.
    fn position(self, values: &[u64], value: u64) -> Result<f64> {
        match self {
            Self::Size => values
                .binary_search(&value)
                .map(|index| index as f64)
                .map_err(|_| "record size outside the charted sizes".into()),
            Self::Rate => Ok((value as f64).log10()),
        }
    }

    fn tick(self, values: &[u64], value: f64) -> String {
        match self {
            Self::Rate => style::records_per_second(10.0_f64.powf(value)),
            Self::Size if (value - value.round()).abs() < 0.01 && value >= 0.0 => values
                .get(value as usize)
                .map_or_else(String::new, |v| self.label(*v)),
            Self::Size => String::new(),
        }
    }
}

const LATENCY_MAX_MS: f64 = 400.0;

#[derive(Clone, Copy)]
pub(super) struct Metric {
    key: &'static str,
    p50: Option<&'static str>,
    p999: Option<&'static str>,
    title: &'static str,
    scale: f64,
}

impl Metric {
    /// Latency metrics carry P50 whiskers and use the fixed latency scale.
    fn is_latency(self) -> bool {
        self.p50.is_some()
    }

    /// Record rates draw dashed lines; byte rates and latencies solid ones.
    fn is_record_rate(self) -> bool {
        matches!(self.key, "confirmed_s" | "delivered_s")
    }
}

fn id(row: &Value) -> String {
    format!(
        "{}/{}",
        row["case"]["impl"].as_str().unwrap_or(""),
        row["case"]["codec"].as_str().unwrap_or("")
    )
}

/// One of the four charted deployment modes.
fn is_chart_mode(mode: &str) -> bool {
    matches!(mode, "buffered" | "durable" | "disk-quorum")
}

/// A row of one of the charted series.
fn is_charted_series(row: &Value) -> bool {
    SERIES.iter().any(|series| id(row) == series.id)
}

fn sizes(rows: &[&Value]) -> Result<Vec<u64>> {
    x_values(rows, XAxis::Size)
}

fn x_values(rows: &[&Value], axis: XAxis) -> Result<Vec<u64>> {
    let mut sizes = rows
        .iter()
        .map(|r| {
            r["case"][axis.key()]
                .as_u64()
                .filter(|s| *s > 0)
                .ok_or_else(|| "invalid record size".into())
        })
        .collect::<Result<Vec<_>>>()?;
    sizes.sort_unstable();
    sizes.dedup();
    Ok(sizes)
}

fn chart_path(output: &Path, mode: &str, suffix: &str) -> PathBuf {
    let family = if ozzy_bench::automation::cluster_mode(mode) {
        "cluster"
    } else {
        "single"
    };
    output.join(family).join(format!("{mode}{suffix}.svg"))
}

fn mode_title(mode: &str) -> &'static str {
    match mode {
        "buffered" => "Single broker: buffered writes",
        "durable" => "Single broker: durable confirmations",
        _ => "Three brokers: durable disk confirmation",
    }
}

fn metric_range(rows: &[&Value], metrics: &[Metric; 2]) -> Result<std::ops::Range<f64>> {
    let mut hi = 0.0_f64;
    for row in rows {
        if row.get("failure").is_some() {
            continue;
        }
        for metric in metrics {
            for key in [Some(metric.key), metric.p50, metric.p999]
                .into_iter()
                .flatten()
            {
                if Some(key) == metric.p999 && row["measurements"].get(key).is_none() {
                    continue;
                }
                let values = &row["measurements"][key];
                let _ = finite(&values["minimum"])?;
                hi = hi.max(finite(&values["maximum"])? * metric.scale);
            }
        }
    }
    if metrics[0].is_latency() {
        Ok(0.0..LATENCY_MAX_MS)
    } else {
        let step = style::nice_step(hi, 6);
        Ok(0.0..(hi / step).ceil().max(1.0) * step)
    }
}

/// A panel chart with the shared bottom and left label areas and top and left
/// margins; callers set the right side.
fn chart_builder<'a, 'b, 'c>(area: &'a Region<'c>) -> ChartBuilder<'a, 'b, SVGBackend<'c>> {
    let mut builder = ChartBuilder::on(area);
    builder
        .set_label_area_size(LabelAreaPosition::Bottom, 28)
        .set_label_area_size(LabelAreaPosition::Left, 55)
        .margin_top(26)
        .margin_left(10);
    builder
}

/// The shared mesh look: grid-colored major lines only, axis-colored axes,
/// and Y labels in body text.
fn grid<'m, 'a, 'b, 'c>(mesh: &'m mut Mesh<'a, 'b, 'c>) -> &'m mut Mesh<'a, 'b, 'c> {
    mesh.y_label_style(style::text(10))
        .light_line_style(TRANSPARENT)
        .bold_line_style(GRID)
        .axis_style(AXIS)
}

/// Latency panels: 16 whole-millisecond Y labels over the shared grid.
fn latency_grid<'m, 'a, 'b, 'c>(mesh: &'m mut Mesh<'a, 'b, 'c>) -> &'m mut Mesh<'a, 'b, 'c> {
    grid(mesh).y_labels(16).y_label_formatter(&whole_number)
}

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "plotters label formatters take the value by reference"
)]
fn whole_number(value: &f64) -> String {
    format!("{value:.0}")
}

/// One mode's chart file: panels above the legend footer, then the title,
/// subtitle, panel titles and coverage stamp.
struct Page<'a> {
    data: &'a Value,
    rows: &'a [&'a Value],
    mode: &'a str,
    /// What the mode title describes, such as "at saturation".
    condition: &'static str,
}

impl Page<'_> {
    /// Draw the panels with `body` into the top `body_height` pixels of
    /// `path`; `body` returns their titles.
    fn write(
        &self,
        path: &Path,
        body_height: u32,
        body: impl FnOnce(&Region<'_>) -> Result<Vec<PanelTitle>>,
    ) -> Result<()> {
        std::fs::create_dir_all(path.parent().ok_or("chart path has no directory")?)?;
        let height = body_height + FOOTER_HEIGHT;
        let root = SVGBackend::new(path, (WIDTH, height)).into_drawing_area();
        root.fill(&BACKGROUND)?;
        let panels = body(&root)?;
        legend::draw(
            &root
                .clone()
                .shrink((0, body_height), (WIDTH, FOOTER_HEIGHT)),
            self.data,
            self.rows,
            self.mode,
        )?;
        root.present()?;
        drop(root);
        style::finish(
            path,
            height,
            &style::Labels {
                title: &format!("{} {}", mode_title(self.mode), self.condition),
                subtitle: hardware::subtitle(self.data).as_deref().unwrap_or(""),
                panels: &panels,
            },
            self.rows,
        )?;
        println!("{}", path.display());
        Ok(())
    }
}

/// Offered rates at which only Ozzy has a measurement, in recorded order.
fn ozzy_only_rates(data: &Value) -> Result<Vec<u64>> {
    data["ozzy_only_rates"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|rate| rate.as_u64().ok_or_else(|| "invalid Ozzy-only rate".into()))
        .collect()
}
