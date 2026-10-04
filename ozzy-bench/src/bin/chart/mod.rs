pub(super) mod coverage;
mod fixed;
mod hardware;
pub(super) mod regression;
pub(super) mod replacement;
mod style;
pub(super) use fixed::render as render_fixed_load;

use ozzy_bench::automation::{Result, finite};
use plotters::{
    coord::{Shift, types::RangedCoordf64},
    prelude::*,
    style::text_anchor::{HPos, Pos, VPos},
};
use serde_json::Value;
use std::{collections::BTreeMap, fmt::Write, path::Path};
use style::{
    AXIS, BACKGROUND, CONTENT_WIDTH, GRID, MUTED, PANEL_HEIGHT, ROW_GAP, SERIES, Series, TEXT, TOP,
    WIDTH,
};

type Chart<'a> = ChartContext<'a, SVGBackend<'a>, Cartesian2d<RangedCoordf64, RangedCoordf64>>;
type PlotArea<'a> = DrawingArea<SVGBackend<'a>, Cartesian2d<RangedCoordf64, RangedCoordf64>>;
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
    fn position(self, values: &[u64], value: u64) -> f64 {
        match self {
            Self::Size => values.binary_search(&value).unwrap() as f64,
            Self::Rate => (value as f64).log10(),
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
const FOOTER_HEIGHT: u32 = 144;
const LATENCY_MAX_MS: f64 = 400.0;
#[derive(Clone, Copy)]
pub(super) struct Metric {
    key: &'static str,
    p50: Option<&'static str>,
    p999: Option<&'static str>,
    title: &'static str,
    scale: f64,
}
const RECORD_RATES: [Metric; 2] = [
    Metric {
        key: "confirmed_s",
        p50: None,
        p999: None,
        title: "records/s",
        scale: 1.0,
    },
    Metric {
        key: "delivered_s",
        p50: None,
        p999: None,
        title: "records/s",
        scale: 1.0,
    },
];
const BYTE_RATES: [Metric; 2] = [
    Metric {
        key: "confirmed_mib_s",
        p50: None,
        p999: None,
        title: "MB/s",
        scale: 1.048_576,
    },
    Metric {
        key: "verified_mib_s",
        p50: None,
        p999: None,
        title: "MB/s",
        scale: 1.048_576,
    },
];
const LATENCIES: [Metric; 2] = [
    Metric {
        key: "ack_p99_us",
        p50: Some("ack_p50_us"),
        p999: Some("ack_p999_us"),
        title: "Writer confirmation (ms, 0-400)",
        scale: 0.001,
    },
    Metric {
        key: "delivery_p99_us",
        p50: Some("delivery_p50_us"),
        p999: Some("delivery_p999_us"),
        title: "Verified reader (ms, 0-400)",
        scale: 0.001,
    },
];

fn is_record_rate(key: &str) -> bool {
    matches!(key, "confirmed_s" | "delivered_s")
}

fn id(row: &Value) -> String {
    format!(
        "{}/{}",
        row["case"]["impl"].as_str().unwrap_or(""),
        row["case"]["codec"].as_str().unwrap_or("")
    )
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

fn percentile_key(area: &DrawingArea<SVGBackend<'_>, Shift>, p999: bool) -> Result<()> {
    let stroke = TEXT.stroke_width(1);
    area.draw(&PathElement::new(vec![(76, 4), (76, 20)], stroke))?;
    area.draw(&PathElement::new(vec![(72, 20), (80, 20)], stroke))?;
    if p999 {
        area.draw(&PathElement::new(vec![(72, 4), (80, 4)], stroke))?;
    }
    area.draw(&PathElement::new(
        vec![(66, 11), (86, 11)],
        TEXT.stroke_width(2),
    ))?;
    area.draw(&Circle::new((76, 11), 2, TEXT.filled()))?;
    area.draw_text(
        if p999 {
            "P99; P50-P99.9"
        } else {
            "P99; P50 lower"
        },
        &("sans-serif", 10).into_font().color(&TEXT),
        (96, 5),
    )?;
    Ok(())
}

fn size_range(sizes: &[u64]) -> std::ops::Range<f64> {
    // Leave room for endpoint dots and whisker caps.
    -0.12..(sizes.len().saturating_sub(1) as f64 + 0.12).max(0.88)
}

fn throughput_panel(
    area: &DrawingArea<SVGBackend<'_>, Shift>,
    rows: &[&Value],
    column: usize,
    record_range: std::ops::Range<f64>,
    byte_range: std::ops::Range<f64>,
) -> Result<()> {
    let sizes = sizes(rows)?;
    let xrange = size_range(&sizes);
    let mut chart = ChartBuilder::on(area)
        .set_label_area_size(LabelAreaPosition::Bottom, 28)
        .set_label_area_size(LabelAreaPosition::Left, 55)
        .set_label_area_size(LabelAreaPosition::Right, 55)
        .margin_top(26)
        .margin_left(10)
        .margin_right(10)
        .build_cartesian_2d(xrange.clone(), record_range)?
        .set_secondary_coord(xrange, byte_range);
    chart
        .configure_mesh()
        .x_labels(sizes.len().clamp(2, 7))
        .y_labels(7)
        .x_label_formatter(&|x| XAxis::Size.tick(&sizes, *x))
        .y_label_formatter(&|y| style::records_per_second(*y))
        .y_label_style(("sans-serif", 10).into_font().color(&TEXT))
        .x_label_style(("sans-serif", 10).into_font().color(&TEXT))
        .light_line_style(TRANSPARENT)
        .bold_line_style(GRID)
        .axis_style(AXIS)
        .draw()?;
    chart
        .configure_secondary_axes()
        .y_labels(7)
        .y_label_formatter(&|y| format!("{y:.0}"))
        .label_style(("sans-serif", 10).into_font().color(&TEXT))
        .axis_style(AXIS)
        .draw()?;
    for (x, metric) in [(65, RECORD_RATES[column]), (260, BYTE_RATES[column])] {
        let stroke = TEXT.stroke_width(2);
        if is_record_rate(metric.key) {
            for start in [x, x + 9] {
                area.draw(&PathElement::new(
                    vec![(start, 11), (start + 6, 11)],
                    stroke,
                ))?;
            }
        } else {
            area.draw(&PathElement::new(vec![(x, 11), (x + 15, 11)], stroke))?;
        }
        area.draw_text(
            metric.title,
            &("sans-serif", 10).into_font().color(&TEXT),
            (x + 22, 5),
        )?;
    }
    for series in SERIES {
        draw_series(
            chart.plotting_area(),
            rows,
            (&sizes, XAxis::Size),
            RECORD_RATES[column],
            series,
        )?;
        draw_series(
            chart.secondary_plotting_area(),
            rows,
            (&sizes, XAxis::Size),
            BYTE_RATES[column],
            series,
        )?;
    }
    Ok(())
}

fn panel(
    area: &DrawingArea<SVGBackend<'_>, Shift>,
    rows: &[&Value],
    metric: Metric,
    yrange: std::ops::Range<f64>,
) -> Result<()> {
    let sizes = sizes(rows)?;
    percentile_key(
        area,
        metric.p999.is_some_and(|key| {
            rows.iter()
                .any(|row| row["measurements"].get(key).is_some())
        }),
    )?;
    let mut chart = ChartBuilder::on(area)
        .set_label_area_size(LabelAreaPosition::Bottom, 28)
        .set_label_area_size(LabelAreaPosition::Left, 55)
        .margin_top(26)
        .margin_left(10)
        .margin_right(65)
        .build_cartesian_2d(size_range(&sizes), yrange)?;
    chart
        .configure_mesh()
        .x_labels(sizes.len().clamp(2, 7))
        .y_labels(16)
        .x_label_formatter(&|x| XAxis::Size.tick(&sizes, *x))
        .y_label_formatter(&|y| format!("{y:.0}"))
        .y_label_style(("sans-serif", 10).into_font().color(&TEXT))
        .x_label_style(("sans-serif", 10).into_font().color(&TEXT))
        .light_line_style(TRANSPARENT)
        .bold_line_style(GRID)
        .axis_style(AXIS)
        .draw()?;
    for series in SERIES {
        draw_series(
            chart.plotting_area(),
            rows,
            (&sizes, XAxis::Size),
            metric,
            series,
        )?;
    }
    clipped_latency_labels(chart.plotting_area(), rows, &sizes, &[(metric, false)])?;
    Ok(())
}

fn measurement_range(row: &Value, key: &str, scale: f64) -> Result<(f64, f64, f64)> {
    let m = &row["measurements"][key];
    let (lo, mid, hi) = (
        finite(&m["minimum"])? * scale,
        finite(&m["median"])? * scale,
        finite(&m["maximum"])? * scale,
    );
    if lo > mid || mid > hi {
        return Err("invalid repetition range".into());
    }
    if key.ends_with("_us") && lo <= 0.0 {
        return Err("invalid latency sample".into());
    }
    Ok((lo, mid, hi))
}

/// Latency whiskers use the repetition median of each percentile, never the
/// min/max across runs. Missing historical P99.9 leaves the upper whisker open.
fn latency_span(row: &Value, metric: Metric, p99: f64) -> Result<(f64, Option<f64>)> {
    let p50 = measurement_range(row, metric.p50.ok_or("missing P50 key")?, metric.scale)?.1;
    let p999 = metric
        .p999
        .filter(|key| row["measurements"].get(*key).is_some())
        .map(|key| measurement_range(row, key, metric.scale).map(|(_, mid, _)| mid))
        .transpose()?;
    if p50 > p99 || p999.is_some_and(|upper| upper < p99) {
        return Err("invalid latency percentile order".into());
    }
    Ok((p50, p999))
}

fn draw_series(
    area: &PlotArea<'_>,
    rows: &[&Value],
    x: (&[u64], XAxis),
    metric: Metric,
    series: Series,
) -> Result<()> {
    let mut lines = vec![vec![]];
    let color = series.color;
    let stroke = color.stroke_width(2);
    let mut ordered: Vec<_> = rows.iter().filter(|r| id(r) == series.id).collect();
    ordered.sort_by_key(|r| r["case"][x.1.key()].as_u64());
    for row in ordered {
        if row.get("failure").is_some() {
            // Failed cells create gaps, never invented latency values.
            // A separate failed repeat annotates the completed measurement.
            if row["repeat"] != true {
                lines.push(vec![]);
            }
            continue;
        }
        let x = x.1.position(
            x.0,
            row["case"][x.1.key()].as_u64().ok_or("invalid x value")?,
        );
        let (lo, mid, hi) = measurement_range(row, metric.key, metric.scale)?;
        let capped = |value: f64| {
            if metric.p50.is_some() {
                value.min(LATENCY_MAX_MS)
            } else {
                value
            }
        };
        lines.last_mut().unwrap().push((x, capped(mid)));
        let (lo, upper) = if metric.p50.is_some() {
            latency_span(row, metric, mid)?
        } else {
            (lo, Some(hi))
        };
        let whisker = color.mix(0.7).stroke_width(1);
        area.draw(&PathElement::new(
            vec![(x, capped(lo)), (x, capped(upper.unwrap_or(mid)))],
            whisker,
        ))?;
        for y in [Some(lo), upper].into_iter().flatten() {
            area.draw(
                &(EmptyElement::at((x, capped(y)))
                    + PathElement::new(vec![(-3, 0), (3, 0)], whisker)),
            )?;
        }
    }
    for points in &lines {
        if is_record_rate(metric.key) {
            for element in DashedLineSeries::new(points.iter().copied(), 6, 3, stroke) {
                area.draw(&element)?;
            }
        } else {
            for element in LineSeries::new(points.iter().copied(), stroke) {
                area.draw(&element)?;
            }
        }
    }
    for point in lines.into_iter().flatten() {
        area.draw(&Circle::new(point, 2, color.filled()))?;
    }
    Ok(())
}

/// Place every clipped percentile beside its own marker. Labels at nearby X
/// positions take separate rows so they remain readable at the fixed scale.
pub(super) fn clipped_latency_labels(
    area: &PlotArea<'_>,
    rows: &[&Value],
    values: &[u64],
    metrics: &[(Metric, bool)],
) -> Result<()> {
    let bounds = area.get_pixel_range().0;
    let axis = if rows
        .first()
        .is_some_and(|row| row["case"]["rate"].is_null())
    {
        XAxis::Size
    } else {
        XAxis::Rate
    };
    let mut occupied: Vec<Vec<(i32, i32)>> = Vec::new();
    for &(metric, reader) in metrics {
        for series in SERIES {
            let color = if reader {
                fixed::reader_color(series.color)
            } else {
                series.color
            };
            let mut matching: Vec<_> = rows
                .iter()
                .copied()
                .filter(|row| id(row) == series.id && row.get("failure").is_none())
                .collect();
            matching.sort_by_key(|row| row["case"][axis.key()].as_u64());
            for row in matching {
                let value = row["case"][axis.key()]
                    .as_u64()
                    .ok_or("invalid chart x value")?;
                let x = axis.position(values, value);
                let (_, p99, _) = measurement_range(row, metric.key, metric.scale)?;
                let (_, p999) = latency_span(row, metric, p99)?;
                let median_clipped = p99 > LATENCY_MAX_MS;
                let upper_clipped = p999.filter(|value| *value > LATENCY_MAX_MS);
                if !median_clipped && upper_clipped.is_none() {
                    continue;
                }
                let label = match (median_clipped, upper_clipped) {
                    (true, Some(p999)) => format!("P99 {p99:.0} / P99.9 {p999:.0} ms"),
                    (true, None) => format!("P99 {p99:.0} ms"),
                    (false, Some(p999)) => format!("P99.9 {p999:.0} ms"),
                    (false, None) => unreachable!(),
                };
                let point_x = area.map_coordinate(&(x, LATENCY_MAX_MS)).0;
                let width = i32::try_from(label.len())? * 6 + 8;
                let left = (point_x - width / 2).clamp(
                    bounds.start + 2,
                    (bounds.end - width - 2).max(bounds.start + 2),
                );
                let right = left + width;
                let lane = occupied
                    .iter()
                    .position(|intervals| {
                        intervals
                            .iter()
                            .all(|&(start, end)| right + 6 < start || left > end + 6)
                    })
                    .unwrap_or_else(|| {
                        occupied.push(Vec::new());
                        occupied.len() - 1
                    });
                occupied[lane].push((left, right));
                let marker_offsets: &[i32] = if median_clipped && upper_clipped.is_some() {
                    &[-5, 5]
                } else {
                    &[0]
                };
                for &offset in marker_offsets {
                    area.draw(
                        &(EmptyElement::at((x, LATENCY_MAX_MS))
                            + TriangleMarker::new((offset, 7), 5, color.filled())),
                    )?;
                }
                let dx = left - point_x;
                let dy = 18 + i32::try_from(lane)? * 16;
                area.draw(
                    &(EmptyElement::at((x, LATENCY_MAX_MS))
                        + PathElement::new(
                            vec![(0, 13), (dx + width / 2, dy)],
                            color.mix(0.65).stroke_width(1),
                        )
                        + Rectangle::new([(dx, dy), (dx + width, dy + 14)], BACKGROUND.filled())
                        + Text::new(
                            label,
                            (dx + 4, dy + 2),
                            ("sans-serif", 9)
                                .into_font()
                                .color(&color)
                                .pos(Pos::new(HPos::Left, VPos::Top)),
                        )),
                )?;
            }
        }
    }
    Ok(())
}

fn legend(
    area: &DrawingArea<SVGBackend<'_>, Shift>,
    data: &Value,
    rows: &[&Value],
    mode: &str,
) -> Result<()> {
    let present: Vec<_> = SERIES
        .into_iter()
        .filter(|s| rows.iter().any(|r| id(r) == s.id))
        .collect();
    let text = ("sans-serif", 11).into_font().color(&TEXT);
    let dim = ("sans-serif", 10).into_font().color(&MUTED);
    // Fixed-load panels draw writer and reader latency together.
    let fixed = rows.iter().any(|r| r["case"]["rate"].as_u64().is_some());
    let label_x = if fixed { 118 } else { 98 };
    for (index, series) in present.iter().enumerate() {
        let y = 4 + i32::try_from(index)? * 16;
        area.draw(&PathElement::new(
            vec![(78, y + 6), (92, y + 6)],
            series.color.stroke_width(2),
        ))?;
        if fixed {
            area.draw(&PathElement::new(
                vec![(98, y + 6), (112, y + 6)],
                fixed::reader_color(series.color).stroke_width(2),
            ))?;
        }
        let label = series_label(data, *series, mode)?;
        // Fixed-load panels name their own backlog failures.
        area.draw_text(&label, &text, (label_x, y))?;
    }
    if fixed {
        let mut note = "Writer confirmation: full color; verified reader: lighter shade".to_owned();
        if let Some(rates) = data["ozzy_only_rates"].as_array() {
            for rate in rates {
                let rate = rate.as_u64().ok_or("invalid Ozzy-only rate")?;
                write!(note, "; {} Ozzy only", XAxis::Rate.label(rate))?;
            }
        }
        area.draw_text(&note, &dim, (78, 4 + i32::try_from(present.len())? * 16))?;
    }
    let mut config = data["compatibility"]["configuration"].clone();
    if let Some(records) = data["batch_records"]["ozzy"].as_u64() {
        config["request_records"] = records.into();
    }
    if data["payload_compression"].is_string() {
        config["payload_compression"] = data["payload_compression"].clone();
    }
    config["observed_batch_payload_cap"] = data["writer_payload_caps"][mode].clone();
    let mut topology = format!(
        "{} partitions",
        config["partitions"]
            .as_u64()
            .ok_or("missing partition count")?
    );
    if let Some(workers) = config["producer_workers"].as_u64() {
        write!(topology, ", {workers} writer processes")?;
    }
    if let Some(mib) = config["segment_mib"].as_u64() {
        write!(topology, ", {mib} MiB segments")?;
    }
    area.draw_text(&topology, &text, (380, 4))?;
    let mut reference = data["references"]["iggy"]["compatibility"]["configuration"].clone();
    if let Some(records) = data["batch_records"]["iggy"].as_u64() {
        reference["request_records"] = records.into();
    }
    let records = data["writer_protocols"][mode] == "push-records";
    let captions_y = 4 + 16 * i32::try_from(present.len())? + if fixed { 16 } else { 0 };
    for (index, label) in batch_labels(&config, &reference, &present, records)
        .iter()
        .enumerate()
    {
        area.draw_text(label, &dim, (78, captions_y + 16 * i32::try_from(index)?))?;
    }
    Ok(())
}

fn series_label(data: &Value, series: Series, mode: &str) -> Result<String> {
    let mut label = if let Some(implementation) = series.id.strip_suffix("/raw")
        && implementation != "ozzy"
    {
        let version = data["versions"][implementation]
            .as_str()
            .ok_or("missing recorded broker version")?;
        let name = if implementation == "iggy" {
            "Iggy"
        } else {
            "Redpanda"
        };
        let policy = if mode == "replicated-persisting" {
            " replicated"
        } else {
            ""
        };
        let client = if implementation == "redpanda" {
            " (librdkafka)"
        } else {
            ""
        };
        format!("{name} {version}{client}{policy}")
    } else {
        series.label.to_owned()
    };
    if series.id == "ozzy/raw"
        && data["compatibility"]["configuration"]["broker_omq_on_shard"] == true
    {
        label.push_str(" (shard-owned OMQ)");
    }
    Ok(label)
}

fn batch_labels(
    config: &Value,
    reference: &Value,
    present: &[Series],
    record_lane: bool,
) -> Vec<String> {
    let records = config["request_records"].as_u64().unwrap_or(1024);
    let mut labels = vec![];
    if record_lane && present.iter().any(|s| s.id.starts_with("ozzy/")) {
        labels.push(format!(
            "Ozzy: 1 record/message; {records} queue slots/writer"
        ));
    } else if present.iter().any(|s| s.id.starts_with("ozzy/")) {
        let sdk = if config["payload_compression"] == "adaptive-lz4" {
            "adaptive LZ4 APPENDs"
        } else {
            "SDK batches"
        };
        let mut label = format!("Ozzy: per record; {sdk} up to {records} records");
        if let Some(bytes) = config["observed_batch_payload_cap"].as_u64() {
            write!(label, "; <= {} KiB payload", bytes / 1024).unwrap();
        } else if let Some(bytes) = config["native_batch_target_bytes"].as_u64() {
            write!(label, "; {} MiB target", bytes / (1024 * 1024)).unwrap();
        }
        if let Some(requests) = config["writer_inflight_appends"].as_u64() {
            write!(label, "; {requests} in flight").unwrap();
        }
        labels.push(label);
        if let Some(depth) = config["disk_aio_depth"].as_u64() {
            let mut disk = if config["disk_io_backend"] == "pool" {
                "Ozzy disk: bounded pool".to_owned()
            } else {
                format!("Ozzy disk: AIO depth {depth}")
            };
            if config["disk_direct_io"] == true {
                disk.push_str("; direct writes");
            }
            labels.push(disk);
        }
        if config["live_readers"] == true {
            labels.push("Ozzy readers: live PUB; PEER repair".to_owned());
        }
    }
    if present.iter().any(|s| s.id == "iggy/raw") {
        let records = reference["request_records"].as_u64().unwrap_or(records);
        labels.push(format!("Iggy: explicit batches up to {records} records"));
    }
    if present.iter().any(|s| s.id == "redpanda/raw") {
        let records = reference["request_records"].as_u64().unwrap_or(records);
        labels.push(format!(
            "Redpanda: per record; SDK batches up to {records} records or 4 MiB"
        ));
    }
    labels
}

pub(super) fn render(data: &Value, output: &Path, suffix: &str) -> Result<()> {
    let mut modes = BTreeMap::<&str, Vec<&Value>>::new();
    for row in data["summary"].as_array().ok_or("missing summary")? {
        if !row["case"]["rate"].is_null() {
            return Err("fixed-load rows require scheduled-arrival latency charts; cannot overwrite saturation charts".into());
        }
        let mode = row["case"]["mode"].as_str().ok_or("missing mode")?;
        if !matches!(
            mode,
            "buffered" | "durable" | "disk-quorum" | "replicated-persisting"
        ) {
            return Err("unknown chart mode".into());
        }
        if !SERIES.iter().any(|s| id(row) == s.id) {
            return Err("unknown or incompatible chart series".into());
        }
        modes.entry(mode).or_default().push(row);
    }
    if modes.is_empty() {
        return Err("empty summary".into());
    }
    for (mode, rows) in &modes {
        coverage::check(&chart_path(output, mode, suffix), rows, &BTreeMap::new())?;
    }
    for (mode, rows) in modes {
        render_mode(data, output, suffix, mode, &rows)?;
    }
    Ok(())
}

fn chart_path(output: &Path, mode: &str, suffix: &str) -> std::path::PathBuf {
    let family = if ozzy_bench::automation::cluster_mode(mode) {
        "cluster"
    } else {
        "single"
    };
    output.join(family).join(format!("{mode}{suffix}.svg"))
}

fn render_mode(
    data: &Value,
    output: &Path,
    suffix: &str,
    mode: &str,
    rows: &[&Value],
) -> Result<()> {
    let path = chart_path(output, mode, suffix);
    std::fs::create_dir_all(path.parent().expect("chart directory"))?;
    let body_height = TOP + 2 * PANEL_HEIGHT + ROW_GAP;
    let height = body_height + FOOTER_HEIGHT;
    let root = SVGBackend::new(&path, (WIDTH, height)).into_drawing_area();
    root.fill(&BACKGROUND)?;
    let mut titles = vec![];
    let record_range = metric_range(rows, &RECORD_RATES)?;
    let byte_range = metric_range(rows, &BYTE_RATES)?;
    let latency_range = metric_range(rows, &LATENCIES)?;
    for (column, stage) in ["Writer confirmation", "Verified reader"]
        .iter()
        .enumerate()
    {
        let left = u32::try_from(column)? * CONTENT_WIDTH / 2;
        titles.push((
            left + CONTENT_WIDTH / 4,
            TOP - 6,
            format!("{stage} (higher is better)"),
        ));
        let area = root
            .clone()
            .shrink((left, TOP), (CONTENT_WIDTH / 2, PANEL_HEIGHT));
        throughput_panel(
            &area,
            rows,
            column,
            record_range.clone(),
            byte_range.clone(),
        )?;

        let top = TOP + PANEL_HEIGHT + ROW_GAP;
        let area = root
            .clone()
            .shrink((left, top), (CONTENT_WIDTH / 2, PANEL_HEIGHT));
        titles.push((
            left + CONTENT_WIDTH / 4,
            top - 6,
            LATENCIES[column].title.to_owned(),
        ));
        panel(&area, rows, LATENCIES[column], latency_range.clone())?;
    }
    legend(
        &root
            .clone()
            .shrink((0, body_height), (WIDTH, FOOTER_HEIGHT)),
        data,
        rows,
        mode,
    )?;
    root.present()?;
    drop(root);
    style::finish(
        &path,
        height,
        &format!("{} at saturation", mode_title(mode)),
        hardware::subtitle(data).as_deref().unwrap_or(""),
        &titles,
        rows,
    )?;
    println!("{}", path.display());
    Ok(())
}

fn mode_title(mode: &str) -> &'static str {
    match mode {
        "buffered" => "Single broker: buffered writes",
        "durable" => "Single broker: durable confirmations",
        "replicated-persisting" => "Three brokers: replicated-persisting confirmation",
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
    if metrics[0].p50.is_some() {
        Ok(0.0..LATENCY_MAX_MS)
    } else {
        let step = style::nice_step(hi, 6);
        Ok(0.0..(hi / step).ceil().max(1.0) * step)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn worker_payload_cap_replaces_requested_target_in_caption() {
        let labels = batch_labels(
            &json!({"request_records":2048,"native_batch_target_bytes":4 * 1024 * 1024,
                "observed_batch_payload_cap":832 * 1024}),
            &Value::Null,
            &SERIES,
            false,
        );
        assert!(labels[0].contains("<= 832 KiB payload"));
        assert!(!labels[0].contains("4 MiB target"));
    }

    #[test]
    fn individual_record_lane_does_not_claim_sdk_batching() {
        let labels = batch_labels(
            &json!({"request_records":4096}),
            &Value::Null,
            &SERIES,
            true,
        );
        assert_eq!(labels[0], "Ozzy: 1 record/message; 4096 queue slots/writer");
        assert!(
            labels
                .iter()
                .any(|label| label.contains("Iggy: explicit batches"))
        );
        let labels = batch_labels(
            &json!({"request_records":1024,"payload_compression":"adaptive-lz4",
                "writer_inflight_appends":1,"disk_aio_depth":8}),
            &Value::Null,
            &SERIES,
            false,
        );
        assert_eq!(
            labels[0],
            "Ozzy: per record; adaptive LZ4 APPENDs up to 1024 records; 1 in flight"
        );
        assert_eq!(labels[1], "Ozzy disk: AIO depth 8");
    }

    #[test]
    fn cluster_chart_lays_out_ozzy_and_optional_iggy_series() {
        let temp = tempfile::tempdir().unwrap();
        let measurement = json!({"minimum":1,"median":2,"maximum":3});
        let summary: Vec<_> = [128, 1024, 8192].into_iter().map(|size| {
            json!({"case":{"impl":"ozzy","mode":"replicated-persisting","size":size,"codec":"raw","pattern":"json"},"repetitions":3,"measurements":{"confirmed_s":measurement,"delivered_s":measurement,"confirmed_mib_s":measurement,"verified_mib_s":measurement,"ack_p50_us":measurement,"ack_p99_us":measurement,"delivery_p50_us":measurement,"delivery_p99_us":measurement}})
        }).collect();
        let data = json!({"summary":summary,"run_ids":["fixture"],"compatibility":{"configuration":{"partitions":8,"producer_workers":4,"warmup":1,"duration":3,"broker_cpus":[0],"client_cpus":[1]}}});
        render(&data, temp.path(), "").unwrap();
        let svg =
            std::fs::read_to_string(temp.path().join("cluster/replicated-persisting.svg")).unwrap();
        assert!(!svg.contains("Iggy"));
        assert!(svg.contains("confirmation at saturation</text>"));
        assert_eq!(svg.matches("higher is better").count(), 2);
        assert_eq!(svg.matches("(ms, 0-400)").count(), 2);
        assert!(svg.contains("viewBox=\"0 0 840"));
        assert!(!svg.contains("<svg width="));
        assert!(svg.contains("r=\"2.5\""));
        assert!(svg.contains("x=\"200\" y=\"50\""));
        assert!(svg.contains("x=\"600\" y=\"390\""));
        assert_eq!(svg.matches("P99; P50 lower").count(), 2);
        assert!(!svg.contains("P99.9"));
        assert!(svg.contains("8 partitions, 4 writer processes"));
        assert!(svg.contains("Ozzy: per record; SDK batches up to 1024 records"));
        assert!(!svg.contains("uncompressed"));
        assert_eq!(svg.matches(">\nrecords/s\n</text>").count(), 2);
        assert_eq!(svg.matches(">\nMB/s\n</text>").count(), 2);
        assert_eq!(svg.matches(">\n128 B\n</text>").count(), 4);
        assert_eq!(svg.matches(">\n1 KiB\n</text>").count(), 4);
        assert_eq!(svg.matches(">\n8 KiB\n</text>").count(), 4);
        assert!(!svg.contains("No measured sizes"));
        let mut compared = data.clone();
        let external = compared["summary"]
            .as_array()
            .unwrap()
            .iter()
            .cloned()
            .map(|mut row| {
                row["case"]["impl"] = json!("iggy");
                row
            })
            .collect::<Vec<_>>();
        compared["summary"].as_array_mut().unwrap().extend(external);
        compared["versions"] = json!({"iggy":"0.9.0-rc.1"});
        compared["compatibility"]["configuration"]["request_records"] = json!(8192);
        compared["references"]["iggy"] = json!({
            "compatibility":{"configuration":{"request_records":1024}}
        });
        render(&compared, temp.path(), "").unwrap();
        let svg =
            std::fs::read_to_string(temp.path().join("cluster/replicated-persisting.svg")).unwrap();
        assert!(svg.contains("Iggy 0.9.0-rc.1 replicated"));
        assert!(!svg.contains("(cached reference)"));
        assert!(svg.contains("SDK batches up to 8192 records"));
        assert!(svg.contains("Iggy: explicit batches up to 1024 records"));
        assert!(svg.contains("Three brokers: replicated-persisting confirmation"));

        let mut partial = compared.clone();
        partial["summary"]
            .as_array_mut()
            .unwrap()
            .retain(|row| row["case"]["size"] == 1024);
        assert!(render(&partial, temp.path(), "").is_err());
        assert_eq!(
            std::fs::read_to_string(temp.path().join("cluster/replicated-persisting.svg")).unwrap(),
            svg
        );

        assert!(
            render(&data, temp.path(), "")
                .unwrap_err()
                .to_string()
                .contains("iggy/raw")
        );
        assert_eq!(
            std::fs::read_to_string(temp.path().join("cluster/replicated-persisting.svg")).unwrap(),
            svg
        );
    }

    #[test]
    fn latency_whiskers_use_percentiles_not_repetition_extremes() {
        let mut row = json!({"measurements":{
            "ack_p50_us":{"minimum":100,"median":1000,"maximum":2000},
            "ack_p99_us":{"minimum":4000,"median":10000,"maximum":20000},
            "ack_p999_us":{"minimum":40000,"median":100_000,"maximum":200_000}
        }});
        let metric = LATENCIES[0];
        let p99 = measurement_range(&row, metric.key, metric.scale).unwrap().1;
        assert!((p99 - 10.0).abs() < f64::EPSILON);
        assert_eq!(latency_span(&row, metric, p99).unwrap(), (1.0, Some(100.0)));
        row["measurements"]
            .as_object_mut()
            .unwrap()
            .remove("ack_p999_us");
        assert_eq!(latency_span(&row, metric, p99).unwrap(), (1.0, None));
        row["measurements"]["ack_p50_us"]["median"] = json!(0);
        assert!(latency_span(&row, metric, p99).is_err());
    }

    #[test]
    fn latency_chart_draws_only_p99_lines_with_capped_percentile_whiskers() {
        let rows: Vec<_> = [16, 128, 1024, 8192]
            .into_iter()
            .map(|size| {
                json!({
                    "case":{"impl":"ozzy","codec":"raw","size":size},
                    "measurements":{
                        "ack_p50_us":{"minimum":500,"median":1000,"maximum":2000},
                        "ack_p99_us":{"minimum":4000,"median":10000,"maximum":20000},
                        "ack_p999_us":{"minimum":40000,"median":100_000,"maximum":200_000}
                    }
                })
            })
            .collect();
        let mut svg = String::new();
        {
            let area = SVGBackend::with_string(&mut svg, (400, 280)).into_drawing_area();
            panel(
                &area,
                &rows.iter().collect::<Vec<_>>(),
                LATENCIES[0],
                0.0..LATENCY_MAX_MS,
            )
            .unwrap();
        }
        assert_eq!(svg.matches(r##"fill="#EF4444""##).count(), 4);
        // One connected P99 curve, four vertical stems, eight end caps.
        assert_eq!(
            svg.lines()
                .filter(
                    |line| line.starts_with("<polyline") && line.contains(r##"stroke="#EF4444""##)
                )
                .count(),
            13
        );
        assert_eq!(svg.matches("P99; P50-P99.9").count(), 1);
    }

    #[test]
    fn clipped_latency_labels_leave_boundary_p99_visible() {
        let rows = [json!({
            "case":{"impl":"ozzy","codec":"raw","size":8192},
            "measurements":{
                "ack_p50_us":{"minimum":100_000,"median":100_000,"maximum":100_000},
                "ack_p99_us":{"minimum":400_000,"median":400_000,"maximum":400_000},
                "ack_p999_us":{"minimum":700_000,"median":700_000,"maximum":700_000}
            }
        })];
        let mut svg = String::new();
        {
            let area = SVGBackend::with_string(&mut svg, (400, 280)).into_drawing_area();
            panel(
                &area,
                &rows.iter().collect::<Vec<_>>(),
                LATENCIES[0],
                0.0..LATENCY_MAX_MS,
            )
            .unwrap();
        }
        assert!(svg.contains("P99.9 700 ms"));
        assert!(!svg.contains("P99 400 /"));
        assert!(!svg.contains("labeled triangle"));
    }

    #[test]
    fn byte_rate_axis_converts_mib_to_decimal_mb() {
        let row = json!({"measurements":{
            "confirmed_mib_s":{"minimum":1,"median":2,"maximum":3}
        }});
        let metric = BYTE_RATES[0];
        assert_eq!(metric.title, "MB/s");
        assert!(
            (measurement_range(&row, metric.key, metric.scale).unwrap().1 - 2.097_152).abs()
                < f64::EPSILON
        );
    }

    #[test]
    fn scheduled_rows_cannot_overwrite_saturation_charts() {
        let temp = tempfile::tempdir().unwrap();
        let data = json!({"summary":[{"case":{"rate":1000}}]});
        assert!(render(&data, temp.path(), "").is_err());
        assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 0);
    }
}
