//! Concurrent language SDK benchmark with verified records and isolated processes.
#[path = "../sdk_pipeline/mod.rs"]
mod sdk_pipeline;
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    sdk_pipeline::main()
}
