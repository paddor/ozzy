//! Saturation charts: per mode, writer and reader throughput over record size
//! above writer and reader latency.
use super::{
    Metric, Page, Region, XAxis, chart_builder, chart_path, coverage, grid, is_chart_mode,
    is_charted_series, latency_grid,
    marks::{clipped_latency_labels, draw_series, percentile_key},
    metric_range, sizes,
    style::{self, AXIS, CONTENT_WIDTH, PANEL_HEIGHT, PanelTitle, ROW_GAP, SERIES, TEXT, TOP},
    whole_number,
};
use ozzy_bench::automation::Result;
use plotters::prelude::*;
use serde_json::Value;
use std::{collections::BTreeMap, path::Path};

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

pub(crate) fn render(data: &Value, output: &Path, suffix: &str) -> Result<()> {
    let mut modes = BTreeMap::<&str, Vec<&Value>>::new();
    for row in data["summary"].as_array().ok_or("missing summary")? {
        if !row["case"]["rate"].is_null() {
            return Err("fixed-load rows require scheduled-arrival latency charts; cannot overwrite saturation charts".into());
        }
        let mode = row["case"]["mode"].as_str().ok_or("missing mode")?;
        if !is_chart_mode(mode) {
            return Err("unknown chart mode".into());
        }
        if !is_charted_series(row) {
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
        let page = Page {
            data,
            rows: &rows,
            mode,
            condition: "at saturation",
        };
        page.write(
            &chart_path(output, mode, suffix),
            TOP + 2 * PANEL_HEIGHT + ROW_GAP,
            |root| panels(root, &rows),
        )?;
    }
    Ok(())
}

/// Throughput on the top row and latency below it, writer left, reader right.
fn panels(root: &Region<'_>, rows: &[&Value]) -> Result<Vec<PanelTitle>> {
    let mut titles = vec![];
    let record_range = metric_range(rows, &RECORD_RATES)?;
    let byte_range = metric_range(rows, &BYTE_RATES)?;
    let latency_range = metric_range(rows, &LATENCIES)?;
    for (column, stage) in ["Writer confirmation", "Verified reader"]
        .iter()
        .enumerate()
    {
        let left = u32::try_from(column)? * CONTENT_WIDTH / 2;
        titles.push(PanelTitle {
            x: left + CONTENT_WIDTH / 4,
            y: TOP - 6,
            text: format!("{stage} (higher is better)"),
        });
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
        titles.push(PanelTitle {
            x: left + CONTENT_WIDTH / 4,
            y: top - 6,
            text: LATENCIES[column].title.to_owned(),
        });
        latency_panel(&area, rows, LATENCIES[column], latency_range.clone())?;
    }
    Ok(titles)
}

fn size_range(sizes: &[u64]) -> std::ops::Range<f64> {
    // Leave room for endpoint dots and whisker caps.
    -0.12..(sizes.len().saturating_sub(1) as f64 + 0.12).max(0.88)
}

/// Record rates on the left axis and byte rates on the right, over size.
fn throughput_panel(
    area: &Region<'_>,
    rows: &[&Value],
    column: usize,
    record_range: std::ops::Range<f64>,
    byte_range: std::ops::Range<f64>,
) -> Result<()> {
    let sizes = sizes(rows)?;
    let xrange = size_range(&sizes);
    let mut chart = chart_builder(area)
        .set_label_area_size(LabelAreaPosition::Right, 55)
        .margin_right(10)
        .build_cartesian_2d(xrange.clone(), record_range)?
        .set_secondary_coord(xrange, byte_range);
    grid(&mut chart.configure_mesh())
        .x_labels(sizes.len().clamp(2, 7))
        .y_labels(7)
        .x_label_formatter(&|x| XAxis::Size.tick(&sizes, *x))
        .y_label_formatter(&|y| style::records_per_second(*y))
        .x_label_style(style::text(10))
        .draw()?;
    chart
        .configure_secondary_axes()
        .y_labels(7)
        .y_label_formatter(&whole_number)
        .label_style(style::text(10))
        .axis_style(AXIS)
        .draw()?;
    rate_keys(area, column)?;
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

/// Line keys for the dashed record-rate and solid byte-rate axes.
fn rate_keys(area: &Region<'_>, column: usize) -> Result<()> {
    for (x, metric) in [(65, RECORD_RATES[column]), (260, BYTE_RATES[column])] {
        let stroke = TEXT.stroke_width(2);
        if metric.is_record_rate() {
            for start in [x, x + 9] {
                area.draw(&PathElement::new(
                    vec![(start, 11), (start + 6, 11)],
                    stroke,
                ))?;
            }
        } else {
            area.draw(&PathElement::new(vec![(x, 11), (x + 15, 11)], stroke))?;
        }
        area.draw_text(metric.title, &style::text(10), (x + 22, 5))?;
    }
    Ok(())
}

/// One latency metric over size, on the fixed latency scale.
fn latency_panel(
    area: &Region<'_>,
    rows: &[&Value],
    metric: Metric,
    yrange: std::ops::Range<f64>,
) -> Result<()> {
    let sizes = sizes(rows)?;
    percentile_key(area, rows, &[metric])?;
    let mut chart = chart_builder(area)
        .margin_right(65)
        .build_cartesian_2d(size_range(&sizes), yrange)?;
    latency_grid(&mut chart.configure_mesh())
        .x_labels(sizes.len().clamp(2, 7))
        .x_label_formatter(&|x| XAxis::Size.tick(&sizes, *x))
        .x_label_style(style::text(10))
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

#[cfg(test)]
mod tests {
    use super::super::{
        LATENCY_MAX_MS,
        marks::{latency_span, measurement_range},
    };
    use super::*;
    use serde_json::json;

    #[test]
    fn cluster_chart_lays_out_ozzy_and_optional_iggy_series() {
        let temp = tempfile::tempdir().unwrap();
        let measurement = json!({"minimum":1,"median":2,"maximum":3});
        let summary: Vec<_> = [128, 1024, 8192].into_iter().map(|size| {
            json!({"case":{"impl":"ozzy","mode":"disk-quorum","size":size,"codec":"raw","pattern":"json"},"repetitions":3,"measurements":{"confirmed_s":measurement,"delivered_s":measurement,"confirmed_mib_s":measurement,"verified_mib_s":measurement,"ack_p50_us":measurement,"ack_p99_us":measurement,"delivery_p50_us":measurement,"delivery_p99_us":measurement}})
        }).collect();
        let data = json!({"summary":summary,"run_ids":["fixture"],"compatibility":{"configuration":{"partitions":8,"producer_workers":4,"warmup":1,"duration":3,"broker_cpus":[0],"client_cpus":[1]}}});
        render(&data, temp.path(), "").unwrap();
        let svg = std::fs::read_to_string(temp.path().join("cluster/disk-quorum.svg")).unwrap();
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
        let svg = std::fs::read_to_string(temp.path().join("cluster/disk-quorum.svg")).unwrap();
        assert!(svg.contains("Iggy 0.9.0-rc.1"));
        assert!(!svg.contains("(cached reference)"));
        assert!(svg.contains("SDK batches up to 8192 records"));
        assert!(svg.contains("Iggy: explicit batches up to 1024 records"));
        assert!(svg.contains("Three brokers: durable disk confirmation"));

        let mut partial = compared.clone();
        partial["summary"]
            .as_array_mut()
            .unwrap()
            .retain(|row| row["case"]["size"] == 1024);
        assert!(render(&partial, temp.path(), "").is_err());
        assert_eq!(
            std::fs::read_to_string(temp.path().join("cluster/disk-quorum.svg")).unwrap(),
            svg
        );

        assert!(
            render(&data, temp.path(), "")
                .unwrap_err()
                .to_string()
                .contains("iggy/raw")
        );
        assert_eq!(
            std::fs::read_to_string(temp.path().join("cluster/disk-quorum.svg")).unwrap(),
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
            latency_panel(
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
            latency_panel(
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
