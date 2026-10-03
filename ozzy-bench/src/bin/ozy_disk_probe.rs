//! Linux filesystem probe. These results are not Ozzy confirmation measurements.
#![forbid(unsafe_code)]

#[cfg(target_os = "linux")]
mod disk_probe;

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    #[cfg(target_os = "linux")]
    return disk_probe::run();
    #[cfg(not(target_os = "linux"))]
    Err("ozy_disk_probe requires Linux".into())
}
