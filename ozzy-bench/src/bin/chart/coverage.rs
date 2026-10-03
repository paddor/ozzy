//! Refuse to replace an existing comparison with fewer measured series.
use super::{SERIES, id};
use ozzy_bench::automation::Result;
use serde_json::Value;
use std::{collections::BTreeSet, fs, path::Path};

const ATTRIBUTE: &str = "data-ozzy-series=\"";

fn selected(rows: &[&Value]) -> BTreeSet<String> {
    rows.iter().map(|row| id(row)).collect()
}

fn previous(svg: &str) -> Result<BTreeSet<String>> {
    if let Some((_, value)) = svg.split_once(ATTRIBUTE) {
        let (value, _) = value
            .split_once('"')
            .ok_or("invalid chart series metadata")?;
        if value.is_empty() {
            return Err("empty chart series metadata".into());
        }
        return Ok(value
            .split(',')
            .map(|series| {
                if series == "ozzy/lz4" {
                    "ozzy/raw"
                } else {
                    series
                }
            })
            .map(str::to_owned)
            .collect());
    }
    // Existing generated charts predate the machine-readable series list.
    // Read only legend text, including versioned external implementations.
    Ok(SERIES
        .iter()
        .filter(|series| {
            svg.lines().map(str::trim).any(|line| {
                line == series.label
                    || line.starts_with(&format!("{} ", series.label))
                    || (series.id == "ozzy/raw" && line.starts_with("Ozzy RAM copies ("))
            })
        })
        .map(|series| series.id.to_owned())
        .collect())
}

pub(super) fn check(path: &Path, rows: &[&Value]) -> Result<()> {
    let svg = match fs::read_to_string(path) {
        Ok(svg) => svg,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let existing = previous(&svg)?;
    let selected = selected(rows);
    let missing: Vec<_> = existing.difference(&selected).map(String::as_str).collect();
    if !missing.is_empty() {
        return Err(format!(
            "refusing to remove {} from {}; include compatible baseline runs or use --suffix for a separate diagnostic chart",
            missing.join(", "), path.display()
        ).into());
    }
    Ok(())
}

pub(super) fn stamp(svg: &mut String, rows: &[&Value]) -> Result<()> {
    let start = svg.find("<svg ").ok_or("missing SVG root")? + 5;
    let ids = selected(rows).into_iter().collect::<Vec<_>>().join(",");
    svg.insert_str(start, &format!("{ATTRIBUTE}{ids}\" "));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn legacy_legends_and_stable_ids_protect_comparison_series() {
        let legacy = "<text>\nIggy 0.9.0-rc.1 replicated\n</text>\n<text>\nRedpanda 26.2.2 + librdkafka 2.12.1\n</text>\n<text>\nOzzy RAM copies (three brokers)\n</text>\n<text>\nOzzy packed\n</text>";
        let expected = SERIES.iter().map(|series| series.id.to_owned()).collect();
        assert_eq!(previous(legacy).unwrap(), expected);
        let row = json!({"case":{"impl":"ozzy","codec":"raw"}});
        let mut svg = "<svg viewBox=\"0 0 10 10\"></svg>".to_owned();
        stamp(&mut svg, &[&row]).unwrap();
        assert_eq!(previous(&svg).unwrap(), BTreeSet::from(["ozzy/raw".into()]));
        assert!(previous("<svg data-ozzy-series=\"").is_err());
        assert!(previous("<svg data-ozzy-series=\"\">").is_err());
    }
}
