use crate::{ConfigError, invalid, validate::require};
use serde::{Serialize, de::DeserializeOwned};

pub(crate) fn encode(kind: &str, value: &impl Serialize) -> Result<String, ConfigError> {
    let body = toml::to_string(value)?;
    Ok(format!(
        "# Ozzy {kind} identity\n# xxh3-128: {:032x}\n{body}",
        checksum(kind, &body)
    ))
}

pub(crate) fn decode<T: DeserializeOwned>(kind: &str, input: &str) -> Result<T, ConfigError> {
    let (header, rest) = input
        .split_once('\n')
        .ok_or_else(|| invalid("identity", "missing record header"))?;
    let (digest, body) = rest
        .split_once('\n')
        .ok_or_else(|| invalid("identity", "missing record checksum"))?;
    require(
        header == format!("# Ozzy {kind} identity")
            && digest == format!("# xxh3-128: {:032x}", checksum(kind, body)),
        "identity",
        "invalid header or identity checksum",
    )?;
    Ok(toml::from_str(body)?)
}

fn checksum(kind: &str, body: &str) -> u128 {
    use xxhash_rust::xxh3::{xxh3_64, xxh3_128_with_seed};
    xxh3_128_with_seed(
        body.as_bytes(),
        xxh3_64(format!("ozzy {kind} identity").as_bytes()),
    )
}
