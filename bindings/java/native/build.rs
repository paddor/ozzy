//! Compile the Java API and broker scenarios into Cargo-owned artifacts.
use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};
fn sources(root: &Path, output: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in fs::read_dir(root)? {
        let path = entry?.path();
        if path.is_dir() {
            sources(&path, output)?;
        } else if path.extension().is_some_and(|s| s == "java") {
            output.push(path);
        }
    }
    Ok(())
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?).join("..");
    let classes = PathBuf::from(env::var("OUT_DIR")?).join("classes");
    fs::create_dir_all(&classes)?;
    let mut files = Vec::new();
    for directory in ["src/main/java", "src/test/java"] {
        println!("cargo:rerun-if-changed={}", root.join(directory).display());
        sources(&root.join(directory), &mut files)?;
    }
    let status = Command::new("javac")
        .args(["--release", "22", "-Xlint:all", "-Werror", "-d"])
        .arg(&classes)
        .args(files)
        .status()?;
    if !status.success() {
        return Err("Java compilation failed; select JDK 22+".into());
    }
    println!(
        "cargo:rustc-env=OZZY_JAVA_TEST_CLASSES={}",
        classes.display()
    );
    Ok(())
}
