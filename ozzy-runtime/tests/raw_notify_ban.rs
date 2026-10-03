//! Keep raw notification mechanics inside the persistent signal primitives.

use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn runtime_notification_mechanics_stay_inside_signal_module() {
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut violations = Vec::new();
    check_directory(&source, &source, &mut violations);
    assert!(
        violations.is_empty(),
        "use crate::signal instead of raw Tokio notifications:\n{}",
        violations.join("\n")
    );
}

fn check_directory(root: &Path, directory: &Path, violations: &mut Vec<String>) {
    let mut entries = fs::read_dir(directory)
        .expect("read runtime source directory")
        .map(|entry| entry.expect("read source entry").path())
        .collect::<Vec<PathBuf>>();
    entries.sort();
    for path in entries {
        if path == root.join("signal.rs") || path == root.join("signal") {
            continue;
        }
        if path.is_dir() {
            check_directory(root, &path, violations);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            let source = fs::read_to_string(&path).expect("read Rust source");
            for (number, line) in source.lines().enumerate() {
                if contains_raw_notify(line) {
                    violations.push(format!(
                        "{}:{}: {}",
                        path.strip_prefix(root).unwrap().display(),
                        number + 1,
                        line.trim()
                    ));
                }
            }
        }
    }
}

fn contains_raw_notify(line: &str) -> bool {
    // Whole identifiers catch qualified paths and renamed imports alike. Check
    // method calls too, since a helper can return Notify without naming its type.
    // Comments/doc examples stay covered so copied examples cannot bypass this.
    if line
        .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .any(|token| matches!(token, "Notify" | "Notified" | "OwnedNotified"))
    {
        return true;
    }
    let compact = line
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>();
    [
        "notified",
        "notified_owned",
        "notify_one",
        "notify_last",
        "notify_waiters",
    ]
    .into_iter()
    .any(|method| compact.contains(&format!(".{method}(")))
}

#[test]
fn guard_rejects_aliases_paths_and_raw_methods_without_matching_signal_names() {
    for source in [
        "use tokio::sync::Notify as Wake;",
        "let wake: tokio::sync::Notify;",
        "use tokio::sync::futures::{Notified, OwnedNotified};",
        "wake.notified().await;",
        "wake.notified_owned().await;",
        "wake.notify_one();",
        "wake.notify_waiters();",
        "wake.notify_last();",
    ] {
        assert!(contains_raw_notify(source), "must reject {source}");
    }
    for source in [
        "use crate::signal::{DataSignal, StateSignal, CloseSignal};",
        "signal.notify_changed();",
        "signal.ready().await;",
        "signal.changed_after(seen).await;",
        "signal.closed().await;",
        "mod notified;",
        "use notified::{NotifiedReceiver, NotifiedSender};",
    ] {
        assert!(!contains_raw_notify(source), "must allow {source}");
    }
}
