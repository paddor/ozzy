//! Data marks shared by saturation and fixed-load panels: series lines with
//! repetition or percentile whiskers, the percentile key, and labels for
//! latencies above the fixed scale.
use super::{
    LATENCY_MAX_MS, Metric, PlotArea, Region, XAxis, fixed, id,
    style::{self, BACKGROUND, SERIES, Series, TEXT},
};
use ozzy_bench::automation::{Result, finite};
use plotters::{
    prelude::*,
    style::text_anchor::{HPos, Pos, VPos},
};
use serde_json::Value;

/// The whisker key; it names P99.9 when any row of `metrics` recorded it.
pub(super) fn percentile_key(area: &Region<'_>, rows: &[&Value], metrics: &[Metric]) -> Result<()> {
    let p999 = rows.iter().any(|row| {
        metrics.iter().any(|metric| {
            metric
                .p999
                .is_some_and(|key| row["measurements"].get(key).is_some())
        })
    });
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
        &style::text(10),
        (96, 5),
    )?;
    Ok(())
}

pub(super) fn measurement_range(row: &Value, key: &str, scale: f64) -> Result<(f64, f64, f64)> {
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
pub(super) fn latency_span(row: &Value, metric: Metric, p99: f64) -> Result<(f64, Option<f64>)> {
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

pub(super) fn draw_series(
    area: &PlotArea<'_>,
    rows: &[&Value],
    x: (&[u64], XAxis),
    metric: Metric,
    series: Series,
) -> Result<()> {
    // Failed cells split the line into segments, never invented values.
    let mut lines = Vec::new();
    let mut segment = Vec::new();
    let color = series.color;
    let stroke = color.stroke_width(2);
    let mut ordered: Vec<_> = rows.iter().filter(|r| id(r) == series.id).collect();
    ordered.sort_by_key(|r| r["case"][x.1.key()].as_u64());
    for row in ordered {
        if row.get("failure").is_some() {
            // A separate failed repeat annotates the completed measurement.
            if row["repeat"] != true {
                lines.push(std::mem::take(&mut segment));
            }
            continue;
        }
        let x = x.1.position(
            x.0,
            row["case"][x.1.key()].as_u64().ok_or("invalid x value")?,
        )?;
        let (lo, mid, hi) = measurement_range(row, metric.key, metric.scale)?;
        let capped = |value: f64| {
            if metric.is_latency() {
                value.min(LATENCY_MAX_MS)
            } else {
                value
            }
        };
        segment.push((x, capped(mid)));
        let (lo, upper) = if metric.is_latency() {
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
    lines.push(segment);
    for points in &lines {
        if metric.is_record_rate() {
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
    let axis = if rows
        .first()
        .is_some_and(|row| row["case"]["rate"].is_null())
    {
        XAxis::Size
    } else {
        XAxis::Rate
    };
    let mut label_rows = LabelRows::default();
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
                let x = axis.position(values, value)?;
                let (_, p99, _) = measurement_range(row, metric.key, metric.scale)?;
                let (_, p999) = latency_span(row, metric, p99)?;
                if let Some(label) = Clipped::of(p99, p999) {
                    label.draw(area, x, color, &mut label_rows)?;
                }
            }
        }
    }
    Ok(())
}

/// Percentiles of one cell above the fixed latency scale.
struct Clipped {
    text: String,
    /// Marker offsets from the cell X: one per clipped percentile.
    markers: &'static [i32],
}

impl Clipped {
    /// The label for a cell, if its P99 or P99.9 exceeds the scale.
    fn of(p99: f64, p999: Option<f64>) -> Option<Self> {
        let upper = p999.filter(|value| *value > LATENCY_MAX_MS);
        let (text, markers): (_, &'static [i32]) = match (p99 > LATENCY_MAX_MS, upper) {
            (true, Some(p999)) => (format!("P99 {p99:.0} / P99.9 {p999:.0} ms"), &[-5, 5]),
            (true, None) => (format!("P99 {p99:.0} ms"), &[0]),
            (false, Some(p999)) => (format!("P99.9 {p999:.0} ms"), &[0]),
            (false, None) => return None,
        };
        Some(Self { text, markers })
    }

    /// Markers at the top edge above `x`, and the label in the first free
    /// row below them with a leader line.
    fn draw(
        self,
        area: &PlotArea<'_>,
        x: f64,
        color: RGBColor,
        label_rows: &mut LabelRows,
    ) -> Result<()> {
        let bounds = area.get_pixel_range().0;
        let point_x = area.map_coordinate(&(x, LATENCY_MAX_MS)).0;
        let width = i32::try_from(self.text.len())? * 6 + 8;
        let left = (point_x - width / 2).clamp(
            bounds.start + 2,
            (bounds.end - width - 2).max(bounds.start + 2),
        );
        let row = label_rows.place(left, left + width);
        for &offset in self.markers {
            area.draw(
                &(EmptyElement::at((x, LATENCY_MAX_MS))
                    + TriangleMarker::new((offset, 7), 5, color.filled())),
            )?;
        }
        let dx = left - point_x;
        let dy = 18 + i32::try_from(row)? * 16;
        area.draw(
            &(EmptyElement::at((x, LATENCY_MAX_MS))
                + PathElement::new(
                    vec![(0, 13), (dx + width / 2, dy)],
                    color.mix(0.65).stroke_width(1),
                )
                + Rectangle::new([(dx, dy), (dx + width, dy + 14)], BACKGROUND.filled())
                + Text::new(
                    self.text,
                    (dx + 4, dy + 2),
                    ("sans-serif", 9)
                        .into_font()
                        .color(&color)
                        .pos(Pos::new(HPos::Left, VPos::Top)),
                )),
        )?;
        Ok(())
    }
}

/// Pixel spans of the clipped labels in each row below the top edge.
#[derive(Default)]
struct LabelRows(Vec<Vec<std::ops::Range<i32>>>);

impl LabelRows {
    /// The first row where `left..right` keeps 6 pixels from every label.
    fn place(&mut self, left: i32, right: i32) -> usize {
        let row = self
            .0
            .iter()
            .position(|spans| {
                spans
                    .iter()
                    .all(|span| right + 6 < span.start || left > span.end + 6)
            })
            .unwrap_or_else(|| {
                self.0.push(Vec::new());
                self.0.len() - 1
            });
        self.0[row].push(left..right);
        row
    }
}
