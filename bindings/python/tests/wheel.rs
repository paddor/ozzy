//! Installed wheel in a Python process talking TCP to memory-backed real brokers.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use ozzy_config::DeploymentMode;
use ozzy_sim::broker::network as support;
use std::process::Command;

fn roundtrip(mode: DeploymentMode, codec: &str, transport: &str) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let cluster = runtime.block_on(support::cluster(mode, transport, 256 * 1024 * 1024, None));
    let brokers = support::addresses(&cluster);
    let output = Command::new(
        std::env::var_os("OZZY_TEST_PYTHON")
            .expect("run scripts/check-python.sh to install the wheel"),
    )
    .arg("-I")
    .arg(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/wheel_roundtrip.py"
    ))
    .env(
        "OZZY_TEST_BROKERS",
        serde_json::to_string(&brokers).unwrap(),
    )
    .env(
        "OZZY_TEST_POLICY",
        if mode == DeploymentMode::Single {
            "local_durable"
        } else {
            "quorum_durable"
        },
    )
    .env_remove("PYTHONPATH")
    .env("OZZY_TEST_CODEC", codec)
    .output()
    .unwrap();
    runtime.block_on(cluster.shutdown());
    assert!(
        output.status.success(),
        "wheel process: {:?}\n{}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

mod tests {
    use super::*;
    mod stress {
        use super::*;
        macro_rules! cases {
            ($($name:ident: $mode:expr, $codec:expr, $transport:expr;)+) => {
                $(#[test] fn $name() { roundtrip($mode, $codec, $transport); })+
            };
        }
        cases! {
            wheel_tcp_single_raw: DeploymentMode::Single, "raw", "tcp";
            wheel_tcp_single_lz4: DeploymentMode::Single, "lz4", "tcp";
            wheel_tcp_quorum_raw: DeploymentMode::Three, "raw", "tcp";
            wheel_tcp_quorum_lz4: DeploymentMode::Three, "lz4", "tcp";
            wheel_lz4_tcp_single_raw: DeploymentMode::Single, "raw", "lz4+tcp";
            wheel_lz4_tcp_single_lz4: DeploymentMode::Single, "lz4", "lz4+tcp";
            wheel_lz4_tcp_quorum_raw: DeploymentMode::Three, "raw", "lz4+tcp";
            wheel_lz4_tcp_quorum_lz4: DeploymentMode::Three, "lz4", "lz4+tcp";
        }
    }
}
