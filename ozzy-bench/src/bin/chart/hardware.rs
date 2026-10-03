//! Recorded CPU model with optional OMQ-compatible local hardware labels.
use serde_json::Value;

pub(super) fn subtitle(data: &Value) -> Option<String> {
    let config = std::fs::read_to_string(ozzy_bench::automation::root().join(".chart_hw"))
        .unwrap_or_default();
    label(&data["compatibility"]["environment"], &config)
}

fn label(environment: &Value, config: &str) -> Option<String> {
    let cpu = environment["cpu"]
        .as_array()?
        .iter()
        .filter_map(Value::as_str)
        .find_map(|line| {
            let (key, value) = line.split_once(':')?;
            (key.trim() == "model name").then(|| value.trim())
        })?
        .replace("(R)", "")
        .replace("(TM)", "")
        .replace(" CPU", "");
    let (mut prefix, mut postfix) = ("", "");
    for line in config.lines().map(str::trim) {
        if line.starts_with('#') {
            continue;
        }
        if let Some((key, value)) = line.split_once('=') {
            match key.trim() {
                "prefix" => prefix = value.trim(),
                "postfix" => postfix = value.trim(),
                _ => {}
            }
        }
    }
    Some(
        [prefix, &cpu, postfix]
            .into_iter()
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join(", "),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn recorded_cpu_stays_between_optional_omq_affixes() {
        let environment = json!({"cpu":[
            "processor\t: 0",
            "model name\t: Intel(R) Core(TM) i7-8700B CPU @ 3.20GHz"
        ]});
        let cpu = "Intel Core i7-8700B @ 3.20GHz";
        for (config, expected) in [
            ("", cpu.to_owned()),
            ("prefix=Linux VM", format!("Linux VM, {cpu}")),
            ("postfix=6 cores", format!("{cpu}, 6 cores")),
            (
                "# local labels\n prefix = Linux VM \npostfix=6 cores\nunused=x\n",
                format!("Linux VM, {cpu}, 6 cores"),
            ),
        ] {
            assert_eq!(label(&environment, config).as_deref(), Some(&*expected));
        }
        assert_eq!(label(&Value::Null, "prefix=Linux VM"), None);
    }
}
