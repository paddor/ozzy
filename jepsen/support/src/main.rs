//! Test-only IPC observations and production-pool durability qualification.

mod admin;
mod storage;

use std::{error::Error, path::Path, time::Duration};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let result = tokio::time::timeout(Duration::from_secs(30), run(&args)).await??;
    println!("{result}");
    Ok(())
}

async fn run(args: &[String]) -> Result<serde_json::Value> {
    match args {
        [command, endpoint] if command == "admin" => admin::snapshot(endpoint).await,
        [command, path] if command == "identity" => {
            let text = std::fs::read_to_string(path)?;
            Ok(serde_json::to_value(
                ozzy_config::DeploymentIdentity::decode(&text)?,
            )?)
        }
        [command, path] if command == "prepare" => storage::prepare(Path::new(path)).await,
        [command, path] if command == "verify" => storage::verify(Path::new(path)),
        _ => Err(
            "usage: ozzy_jepsen_probe admin ENDPOINT | identity FILE | prepare DIR | verify DIR"
                .into(),
        ),
    }
}
