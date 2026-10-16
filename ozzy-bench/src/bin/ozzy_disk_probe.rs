//! Linux filesystem probe. These results are not Ozzy confirmation measurements.
#![forbid(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable,
        clippy::too_many_lines,
        clippy::too_many_arguments,
        reason = "product code is linted without cfg(test)"
    )
)]

#[cfg(target_os = "linux")]
mod disk_probe;

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    #[cfg(target_os = "linux")]
    return disk_probe::run();
    #[cfg(not(target_os = "linux"))]
    Err("ozzy_disk_probe requires Linux".into())
}
