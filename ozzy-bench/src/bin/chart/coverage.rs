//! Preserve chart coverage except for explicitly excluded offered rates.
use super::{SERIES, id};
use ozzy_bench::automation::Result;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

const ATTRIBUTE: &str = "data-ozzy-series=\"";
const CASES: &str = "data-ozzy-cases=\"";

pub(crate) fn limit_rates(data: &mut Value, limits: &[String]) -> Result<()> {
    if limits.is_empty() {
        return Ok(());
    }
    let mut parsed = BTreeMap::new();
    for limit in limits {
        let (size, rate) = limit.split_once(':').ok_or("expected SIZE:RATE")?;
        let (size, rate) = (size.parse::<u64>()?, rate.parse::<u64>()?);
        if size == 0 || rate == 0 || parsed.insert(size, rate).is_some() {
            return Err("rate limits require distinct positive sizes and rates".into());
        }
    }
    for key in ["summary", "incomplete"] {
        if let Some(rows) = data[key].as_array_mut() {
            rows.retain(|row| {
                let size = row["case"]["size"].as_u64().unwrap_or(0);
                let rate = row["case"]["rate"].as_u64().unwrap_or(0);
                !excluded(size, rate, &parsed)
            });
        }
    }
    data["rate_limits"] = serde_json::to_value(parsed)?;
    Ok(())
}

pub(super) fn rate_limits(data: &Value) -> Result<BTreeMap<u64, u64>> {
    if data["rate_limits"].is_null() {
        return Ok(BTreeMap::new());
    }
    let limits: BTreeMap<u64, u64> = serde_json::from_value(data["rate_limits"].clone())?;
    if limits.iter().any(|(size, rate)| *size == 0 || *rate == 0) {
        return Err("invalid chart rate limit".into());
    }
    Ok(limits)
}

fn excluded(size: u64, rate: u64, limits: &BTreeMap<u64, u64>) -> bool {
    limits.get(&size).is_some_and(|limit| rate > *limit)
}

fn excluded_case(case: &str, limits: &BTreeMap<u64, u64>) -> bool {
    let mut fields = case.rsplit('/');
    match (fields.next(), fields.next()) {
        (Some(rate), Some(size)) => match (size.parse(), rate.parse()) {
            (Ok(size), Ok(rate)) => excluded(size, rate, limits),
            _ => false,
        },
        _ => false,
    }
}

fn selected(rows: &[&Value]) -> BTreeSet<String> {
    rows.iter().map(|row| id(row)).collect()
}

fn cases(rows: &[&Value]) -> BTreeSet<String> {
    rows.iter()
        .map(|row| {
            format!(
                "{}/{}/{}",
                id(row),
                row["case"]["size"].as_u64().unwrap_or(0),
                row["case"]["rate"].as_u64().unwrap_or(0)
            )
        })
        .collect()
}

fn metadata(svg: &str, attribute: &str) -> Result<Option<BTreeSet<String>>> {
    let Some((_, value)) = svg.split_once(attribute) else {
        return Ok(None);
    };
    let (value, _) = value
        .split_once('"')
        .ok_or("invalid chart coverage metadata")?;
    if value.is_empty() {
        return Err("empty chart coverage metadata".into());
    }
    Ok(Some(value.split(',').map(str::to_owned).collect()))
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

pub(super) fn check(path: &Path, rows: &[&Value], rate_limits: &BTreeMap<u64, u64>) -> Result<()> {
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
    if let Some(existing) = metadata(&svg, CASES)? {
        let selected = cases(rows);
        let missing: Vec<_> = existing
            .difference(&selected)
            .filter(|case| !excluded_case(case, rate_limits))
            .map(String::as_str)
            .collect();
        if !missing.is_empty() {
            return Err(format!(
                "refusing to remove chart cases {} from {}; retain other sizes/rates or use --suffix",
                missing.join(", "), path.display()
            ).into());
        }
    } else {
        // Earlier SVGs have size ticks or panel titles, but no case metadata.
        for size in [128, 1024, 8192] {
            let label = super::style::size(size);
            if (svg.contains(&format!(">\n{label}\n</text>"))
                || svg.contains(&format!("{label} records (ms,")))
                && !rows.iter().any(|row| row["case"]["size"] == size)
            {
                return Err(format!("refusing to remove {label} from {}", path.display()).into());
            }
        }
    }
    Ok(())
}

pub(super) fn stamp(svg: &mut String, rows: &[&Value]) -> Result<()> {
    let start = svg.find("<svg ").ok_or("missing SVG root")? + 5;
    let ids = selected(rows).into_iter().collect::<Vec<_>>().join(",");
    let cases = cases(rows).into_iter().collect::<Vec<_>>().join(",");
    svg.insert_str(start, &format!("{ATTRIBUTE}{ids}\" {CASES}{cases}\" "));
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

    #[test]
    fn explicit_rate_ceiling_preserves_other_cases_and_comparison_series() {
        let mut data = json!({"summary":[
            {"case":{"impl":"ozzy","codec":"raw","size":128,"rate":100_000}},
            {"case":{"impl":"ozzy","codec":"raw","size":128,"rate":1_000_000}},
            {"case":{"impl":"iggy","codec":"raw","size":128,"rate":100_000}},
            {"case":{"impl":"iggy","codec":"raw","size":8192,"rate":10000}}
        ]});
        let original: Vec<_> = data["summary"].as_array().unwrap().iter().collect();
        let mut svg = "<svg ></svg>".to_owned();
        stamp(&mut svg, &original).unwrap();
        let file = tempfile::NamedTempFile::new().unwrap();
        fs::write(file.path(), svg).unwrap();
        limit_rates(&mut data, &["128:100000".into()]).unwrap();
        let limits = rate_limits(&data).unwrap();
        let rows: Vec<_> = data["summary"].as_array().unwrap().iter().collect();
        assert_eq!(rows.len(), 3);
        assert!(check(file.path(), &rows, &limits).is_ok());
        assert!(check(file.path(), &rows, &BTreeMap::new()).is_err());
        assert!(check(file.path(), &rows[..2], &limits).is_err());
        assert!(check(file.path(), &rows[..1], &limits).is_err());
        assert!(limit_rates(&mut data, &["128:0".into()]).is_err());
        assert!(limit_rates(&mut data, &["128:100".into(), "128:1000".into()]).is_err());
    }
}
