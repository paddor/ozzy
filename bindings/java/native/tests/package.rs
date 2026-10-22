//! Packaged JNI/FFM library in JVM processes, with TCP memory-backed real brokers.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use ozzy_config::DeploymentMode;
use ozzy_sim::broker::network;
use std::process::Command;
fn roundtrip(mode: DeploymentMode, codec: &str, transport: &str) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let cluster = runtime.block_on(network::cluster(mode, transport, 256 * 1024 * 1024, None));
    let jar = std::env::var("OZZY_JAVA_JAR").expect("run scripts/check-java.sh");
    let smoke = std::env::var("OZZY_JAVA_SMOKE").expect("run scripts/check-java.sh");
    let modular = mode == DeploymentMode::Single && codec == "raw" && transport == "tcp";
    let classpath = if modular {
        smoke
    } else {
        format!("{jar}:{smoke}")
    };
    let extraction = tempfile::tempdir().unwrap();
    let mut command =
        Command::new(std::env::var_os("OZZY_TEST_JAVA").unwrap_or_else(|| "java".into()));
    command.arg(format!("-Djava.io.tmpdir={}", extraction.path().display()));
    if modular {
        command.args(["--module-path", &jar, "--add-modules", "io.ozzy"]);
    }
    command.args([
        if modular {
            "--enable-native-access=io.ozzy"
        } else {
            "--enable-native-access=ALL-UNNAMED"
        },
        "-Xcheck:jni",
        "-Xmx256m",
        "-XX:+UseSerialGC",
        "-XX:ActiveProcessorCount=2",
        "-cp",
        &classpath,
        "io.ozzy.smoke.Smoke",
        "main",
        codec,
    ]);
    for (id, peer, data) in network::addresses(&cluster) {
        command.args([id, peer, data]);
    }
    let output = command.output().unwrap();
    runtime.block_on(cluster.shutdown());
    assert!(
        output.status.success(),
        "JVM failed: {}\n{}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "JVM diagnostics: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(std::fs::read_dir(extraction.path()).unwrap().count(), 0);
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
            tcp_single_raw: DeploymentMode::Single, "raw", "tcp";
            tcp_single_lz4: DeploymentMode::Single, "lz4", "tcp";
            tcp_quorum_raw: DeploymentMode::Three, "raw", "tcp";
            tcp_quorum_lz4: DeploymentMode::Three, "lz4", "tcp";
            lz4_tcp_single_raw: DeploymentMode::Single, "raw", "lz4+tcp";
            lz4_tcp_single_lz4: DeploymentMode::Single, "lz4", "lz4+tcp";
            lz4_tcp_quorum_raw: DeploymentMode::Three, "raw", "lz4+tcp";
            lz4_tcp_quorum_lz4: DeploymentMode::Three, "lz4", "lz4+tcp";
        }
    }
}
