//! Scheduled-arrival latency, kept separate from saturation charts. One chart
//! per mode: a panel per record size, offered load on each X axis, writer
//! confirmation and verified reader latency together in every panel.
use super::{
    AXIS, BACKGROUND, CONTENT_WIDTH, Chart, FOOTER_HEIGHT, GRID, Metric, SERIES, Series, TEXT, TOP,
    WIDTH, XAxis, clipped_latency_labels, draw_series, id, legend, metric_range, percentile_key,
    sizes, style, x_values,
};
use ozzy_bench::automation::Result;
use plotters::{
    prelude::*,
    style::text_anchor::{HPos, Pos, VPos},
};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

const WRITER: Metric = Metric {
    key: "scheduled_ack_p99_us",
    p50: Some("scheduled_ack_p50_us"),
    p999: Some("scheduled_ack_p999_us"),
    title: "Writer confirmation",
    scale: 0.001,
};
const READER: Metric = Metric {
    key: "scheduled_delivery_p99_us",
    p50: Some("scheduled_delivery_p50_us"),
    p999: Some("scheduled_delivery_p999_us"),
    title: "Verified reader",
    scale: 0.001,
};
const METRICS: [Metric; 2] = [WRITER, READER];
const PANEL_HEIGHT: u32 = 280;
const PANEL_GAP: u32 = 34;

/// Reader lines use a lighter shade of their system's writer color.
pub(super) fn reader_color(color: RGBColor) -> RGBColor {
    let lighten = |channel: u8| channel + (255 - channel) / 2;
    RGBColor(lighten(color.0), lighten(color.1), lighten(color.2))
}

pub(crate) fn render(data: &Value, output: &Path, suffix: &str) -> Result<()> {
    let rate_limits = super::coverage::rate_limits(data)?;
    let mut modes = BTreeMap::<&str, Vec<&Value>>::new();
    for row in data["summary"]
        .as_array()
        .ok_or("missing summary")?
        .iter()
        .chain(data["incomplete"].as_array().into_iter().flatten())
    {
        let mode = row["case"]["mode"].as_str().ok_or("missing mode")?;
        if !matches!(
            mode,
            "buffered" | "durable" | "disk-quorum" | "replicated-persisting"
        ) || !SERIES.iter().any(|s| id(row) == s.id)
            || row["case"]["rate"].as_u64().is_none_or(|r| r == 0)
        {
            return Err("fixed-load charts require supported comparison measurements".into());
        }
        modes.entry(mode).or_default().push(row);
    }
    if modes.is_empty() {
        return Err("empty fixed-load summary".into());
    }
    let ozzy_only_rates: BTreeSet<u64> = data["ozzy_only_rates"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|rate| rate.as_u64().ok_or("invalid Ozzy-only rate"))
        .collect::<std::result::Result<Vec<_>, _>>()?
        .into_iter()
        .collect();
    // Validate every panel before writing any chart.
    for (mode, rows) in &modes {
        super::coverage::check(
            &super::chart_path(output, mode, &format!("-fixed-load{suffix}")),
            rows,
            &rate_limits,
        )?;
        for size in sizes(rows)? {
            let rows = of_size(rows, size);
            let rates = x_values(&rows, XAxis::Rate)?;
            for series in SERIES {
                let selected: Vec<_> = rows
                    .iter()
                    .copied()
                    .filter(|r| id(r) == series.id)
                    .collect();
                let expected: Vec<_> = if series.id == "ozzy/raw" {
                    rates.clone()
                } else {
                    rates
                        .iter()
                        .copied()
                        .filter(|rate| !ozzy_only_rates.contains(rate))
                        .collect()
                };
                if !selected.is_empty() && x_values(&selected, XAxis::Rate)? != expected {
                    return Err("fixed-load series has missing offered rates".into());
                }
            }
        }
        metric_range(rows, &METRICS)?;
    }
    for (mode, rows) in modes {
        render_mode(data, output, suffix, mode, &rows)?;
    }
    Ok(())
}

fn of_size<'a>(rows: &[&'a Value], size: u64) -> Vec<&'a Value> {
    rows.iter()
        .copied()
        .filter(|r| r["case"]["size"].as_u64() == Some(size))
        .collect()
}

fn render_mode(
    data: &Value,
    output: &Path,
    suffix: &str,
    mode: &str,
    rows: &[&Value],
) -> Result<()> {
    let path = super::chart_path(output, mode, &format!("-fixed-load{suffix}"));
    std::fs::create_dir_all(path.parent().expect("chart directory"))?;
    let measured = sizes(rows)?;
    let count = u32::try_from(measured.len())?;
    let body_height = TOP + count * PANEL_HEIGHT + (count - 1) * PANEL_GAP + 24;
    let height = body_height + FOOTER_HEIGHT;
    let root = SVGBackend::new(&path, (WIDTH, height)).into_drawing_area();
    root.fill(&BACKGROUND)?;
    let range = metric_range(rows, &METRICS)?;
    let mut titles = vec![];
    for (index, size) in measured.iter().enumerate() {
        let top = TOP + u32::try_from(index)? * (PANEL_HEIGHT + PANEL_GAP);
        let area = root.clone().shrink((0, top), (CONTENT_WIDTH, PANEL_HEIGHT));
        let sized = of_size(rows, *size);
        let rates = x_values(&sized, XAxis::Rate)?;
        panel(&area, &sized, &rates, range.clone())?;
        titles.push((
            CONTENT_WIDTH / 2,
            top - 6,
            format!("{} records (ms, 0-400)", style::size(*size)),
        ));
    }
    root.draw_text(
        "Offered records/s (log, total)",
        &("sans-serif", 11).into_font().color(&TEXT),
        (
            i32::try_from(CONTENT_WIDTH / 2)? - 80,
            i32::try_from(body_height)? - 18,
        ),
    )?;
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
        &format!("{} at fixed load", super::mode_title(mode)),
        super::hardware::subtitle(data).as_deref().unwrap_or(""),
        &titles,
        rows,
    )?;
    println!("{}", path.display());
    Ok(())
}

/// Name the systems that could not sustain a rate in this panel.
fn backlog_note(
    area: &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
    rows: &[&Value],
) -> Result<()> {
    let mut notes = vec![];
    for (repeat, label) in [(false, "backlog limit"), (true, "failed repeat")] {
        let mut seen = BTreeSet::new();
        let failed: Vec<_> = SERIES
            .iter()
            .flat_map(|series| {
                rows.iter()
                    .filter(|row| {
                        id(row) == series.id
                            && row.get("failure").is_some()
                            && (row["repeat"] == true) == repeat
                    })
                    .map(|row| {
                        format!(
                            "{} {}",
                            series.label,
                            XAxis::Rate.label(row["case"]["rate"].as_u64().unwrap_or(0))
                        )
                    })
            })
            .filter(|label| seen.insert(label.clone()))
            .collect();
        if !failed.is_empty() {
            notes.push(format!("{label}: {}", failed.join(", ")));
        }
    }
    if !notes.is_empty() {
        area.draw_text(
            &notes.join("; "),
            &("sans-serif", 10)
                .into_font()
                .color(&super::MUTED)
                .pos(Pos::new(HPos::Right, VPos::Top)),
            (i32::try_from(CONTENT_WIDTH)? - 30, 5),
        )?;
    }
    Ok(())
}

fn panel(
    area: &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
    rows: &[&Value],
    rates: &[u64],
    yrange: std::ops::Range<f64>,
) -> Result<()> {
    let axis = XAxis::Rate;
    let first = axis.position(rates, rates[0]);
    let last = axis.position(rates, *rates.last().unwrap());
    let xrange = if rates.len() == 1 {
        first - 0.5..first + 0.5
    } else {
        first - 0.1..last + 0.1
    };
    percentile_key(
        area,
        rows.iter().any(|row| {
            METRICS.iter().any(|metric| {
                metric
                    .p999
                    .is_some_and(|key| row["measurements"].get(key).is_some())
            })
        }),
    )?;
    backlog_note(area, rows)?;
    let mut chart: Chart<'_> = ChartBuilder::on(area)
        .set_label_area_size(LabelAreaPosition::Bottom, 28)
        .set_label_area_size(LabelAreaPosition::Left, 55)
        .margin_top(26)
        .margin_left(10)
        .margin_right(30)
        .build_cartesian_2d(xrange, yrange.clone())?;
    chart
        .configure_mesh()
        .x_labels(0)
        .disable_x_mesh()
        .y_labels(16)
        .y_label_formatter(&|y| format!("{y:.0}"))
        .y_label_style(("sans-serif", 10).into_font().color(&TEXT))
        .light_line_style(TRANSPARENT)
        .bold_line_style(GRID)
        .axis_style(AXIS)
        .draw()?;
    // Mark each measured load at its actual logarithmic position.
    let (origin_x, origin_y) = area.get_pixel_range();
    let label_style = ("sans-serif", 10)
        .into_font()
        .color(&TEXT)
        .pos(Pos::new(HPos::Center, VPos::Top));
    for rate in rates {
        let position = axis.position(rates, *rate);
        chart.draw_series(std::iter::once(PathElement::new(
            vec![(position, yrange.start), (position, yrange.end)],
            GRID,
        )))?;
        let (x, y) = chart.backend_coord(&(position, yrange.start));
        let (x, y) = (x - origin_x.start, y - origin_y.start);
        area.draw(&PathElement::new(vec![(x, y), (x, y + 5)], AXIS))?;
        area.draw_text(&axis.label(*rate), &label_style, (x, y + 8))?;
    }
    // Readers under writers; all latency lines connect P99 values.
    for metric in [READER, WRITER] {
        for series in SERIES {
            let series = if metric.key == READER.key {
                Series {
                    color: reader_color(series.color),
                    ..series
                }
            } else {
                series
            };
            draw_series(chart.plotting_area(), rows, (rates, axis), metric, series)?;
        }
    }
    clipped_latency_labels(
        chart.plotting_area(),
        rows,
        rates,
        &[(READER, true), (WRITER, false)],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fixture() -> Value {
        let measurement = json!({"minimum":1000,"median":2000,"maximum":3000});
        let mut rows = vec![];
        for size in [128, 1024, 8192] {
            for rate in [100, 1_000, 10_000, 100_000] {
                for implementation in ["ozzy", "iggy", "redpanda"] {
                    rows.push(json!({
                        "case":{"impl":implementation,"mode":"durable","size":size,"codec":"raw","rate":rate},
                        "repetitions":2,"measurements":{"scheduled_ack_p50_us":measurement,"scheduled_ack_p99_us":measurement,
                        "scheduled_delivery_p50_us":measurement,"scheduled_delivery_p99_us":measurement}}));
                }
            }
        }
        json!({"summary":rows,"versions":{"iggy":"0.9.0","redpanda":"26.2.2"},
            "compatibility":{"configuration":{"warmup":5,"partitions":4,
            "request_records":1024,"broker_cpus":[0,1],"client_cpus":[2,3,4,5]}},
            "fixed_load_windows":{"100":{"duration":30},"1000":{"duration":10},"10000":{"duration":10},"100000":{"duration":10}}})
    }

    #[test]
    fn one_chart_per_mode_stacks_sizes_and_overlays_writer_and_reader_latency() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::create_dir(temp.path().join("single")).unwrap();
        let saturation = temp.path().join("single/durable.svg");
        std::fs::write(&saturation, "existing saturation chart").unwrap();
        let mut data = fixture();
        render(&data, temp.path(), "").unwrap();
        assert_eq!(
            std::fs::read_to_string(&saturation).unwrap(),
            "existing saturation chart"
        );
        let path = temp.path().join("single/durable-fixed-load.svg");
        let svg = std::fs::read_to_string(&path).unwrap();
        assert!(svg.contains("confirmations at fixed load</text>"));
        for label in ["100/s", "1K/s", "10K/s", "100K/s"] {
            assert_eq!(svg.matches(&format!(">\n{label}\n</text>")).count(), 3);
        }
        assert!(svg.contains("128 B records (ms, 0-400)"));
        assert!(svg.contains("1 KiB records (ms, 0-400)"));
        assert!(svg.contains("8 KiB records (ms, 0-400)"));
        assert_eq!(svg.matches("Offered records/s (log, total)").count(), 1);
        assert!(svg.contains("Writer confirmation: full color; verified reader: lighter shade"));
        // Ozzy writer red and its lighter reader shade share each panel.
        assert!(svg.contains("#EF4444") && svg.contains("#F7A1A1"));
        assert!(svg.contains("Iggy 0.9.0") && svg.contains("Redpanda 26.2.2 (librdkafka)"));
        assert_eq!(svg.matches("P99; P50 lower").count(), 3);
        assert!(!svg.contains("P99.9"));

        let mut native_only = data.clone();
        native_only["summary"]
            .as_array_mut()
            .unwrap()
            .retain(|row| row["case"]["impl"] == "ozzy");
        assert!(
            render(&native_only, temp.path(), "")
                .unwrap_err()
                .to_string()
                .contains("iggy/raw")
        );
        assert_eq!(std::fs::read_to_string(&path).unwrap(), svg);

        for (field, value) in [("size", 1024), ("rate", 100_000)] {
            let mut partial = data.clone();
            partial["summary"]
                .as_array_mut()
                .unwrap()
                .retain(|row| row["case"][field] == value);
            assert!(render(&partial, temp.path(), "").is_err());
            assert_eq!(std::fs::read_to_string(&path).unwrap(), svg);
        }

        let mut cluster = data.clone();
        for row in cluster["summary"].as_array_mut().unwrap() {
            row["case"]["mode"] = json!("replicated-persisting");
        }
        render(&cluster, temp.path(), "").unwrap();
        let cluster_svg = std::fs::read_to_string(
            temp.path()
                .join("cluster/replicated-persisting-fixed-load.svg"),
        )
        .unwrap();
        assert!(cluster_svg.contains("Three brokers: replicated-persisting confirmation"));
        assert!(cluster_svg.contains("Iggy 0.9.0 replicated"));

        for row in data["summary"].as_array_mut().unwrap() {
            row["measurements"]["scheduled_ack_p999_us"] =
                json!({"minimum":10000,"median":20000,"maximum":30000});
            row["measurements"]["scheduled_delivery_p999_us"] =
                json!({"minimum":10000,"median":20000,"maximum":30000});
        }
        data["incomplete"] = json!(["ozzy", "iggy", "redpanda"].map(|implementation| json!({
            "case":{"impl":implementation,"mode":"durable","size":128,"codec":"raw","rate":1_000_000},
            "failure":"scheduled backlog limit exceeded"})));
        data["fixed_load_windows"]["1000000"] = json!({"duration":10});
        render(&data, temp.path(), "").unwrap();
        let svg = std::fs::read_to_string(&path).unwrap();
        assert_eq!(svg.matches("P99; P50-P99.9").count(), 3);
        assert!(svg.contains("opacity=\"0.7\""));
        assert!(svg.contains("backlog limit: Iggy 1M/s, Redpanda 1M/s, Ozzy 1M/s"));
        assert_eq!(svg.matches(">\n1M/s\n</text>").count(), 1);
        assert_eq!(svg.matches(">\n100K/s\n</text>").count(), 3);

        let mut gap = fixture();
        gap["summary"]
            .as_array_mut()
            .unwrap()
            .retain(|row| row["case"]["impl"] != "iggy" || row["case"]["rate"] != 100_000);
        assert!(render(&gap, temp.path(), "").is_err());
        data["summary"][0]["measurements"]["scheduled_ack_p99_us"] = Value::Null;
        assert!(render(&data, temp.path(), "").is_err());
    }

    #[test]
    fn fixed_load_labels_clipped_writer_and_reader_percentiles() {
        let mut data = fixture();
        let row = data["summary"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|row| {
                row["case"]["impl"] == "ozzy"
                    && row["case"]["size"] == 8192
                    && row["case"]["rate"] == 100_000
            })
            .unwrap();
        row["measurements"]["scheduled_ack_p99_us"] =
            json!({"minimum":500_000,"median":500_000,"maximum":500_000});
        row["measurements"]["scheduled_ack_p999_us"] =
            json!({"minimum":700_000,"median":700_000,"maximum":700_000});
        row["measurements"]["scheduled_delivery_p999_us"] =
            json!({"minimum":450_000,"median":450_000,"maximum":450_000});
        let temp = tempfile::tempdir().unwrap();
        render(&data, temp.path(), "").unwrap();
        let svg =
            std::fs::read_to_string(temp.path().join("single/durable-fixed-load.svg")).unwrap();
        assert!(svg.contains("P99 500 / P99.9 700 ms"));
        assert!(svg.contains("P99.9 450 ms"));
    }

    #[test]
    fn failed_repeat_keeps_the_completed_runs_latency_lines() {
        let temp = tempfile::tempdir().unwrap();
        let mut data = fixture();
        render(&data, temp.path(), "").unwrap();
        let path = temp.path().join("single/durable-fixed-load.svg");
        let before = std::fs::read_to_string(&path).unwrap();
        let case = data["summary"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| {
                row["case"]["impl"] == "ozzy"
                    && row["case"]["size"] == 1024
                    && row["case"]["rate"] == 10_000
            })
            .unwrap()["case"]
            .clone();
        data["incomplete"] =
            json!([{"case":case,"failure":"scheduled backlog limit exceeded","repeat":true}]);
        render(&data, temp.path(), "").unwrap();
        let after = std::fs::read_to_string(path).unwrap();
        let lines = |svg: &str| -> Vec<String> {
            svg.lines()
                .filter(|line| {
                    line.contains("stroke=\"#EF4444\"") || line.contains("stroke=\"#F7A1A1\"")
                })
                .map(str::to_owned)
                .collect()
        };
        assert_eq!(lines(&before), lines(&after));
        assert!(after.contains("failed repeat: Ozzy 10K/s"));
    }
}
