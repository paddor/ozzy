//! Functional benchmark storage follows Cargo's artifact directory.

pub(crate) fn storage(prefix: &str) -> tempfile::TempDir {
    let executable = std::env::current_exe().expect("integration test executable path");
    let directory = executable.parent().expect("integration test directory");
    tempfile::Builder::new()
        .prefix(prefix)
        .tempdir_in(directory)
        .expect("benchmark fixture beside the integration test executable")
}
