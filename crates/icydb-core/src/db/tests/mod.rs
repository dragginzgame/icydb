//! Module: db::tests
//! Covers db-level structural guard behavior that spans multiple subsystems.
//! Does not own: runtime value conversion or storage encoding.
//! Boundary: keeps cross-subsystem source-shape invariants at the db root.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

fn collect_rust_sources(root: &Path, out: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(root)
        .unwrap_or_else(|err| panic!("failed to read source directory {}: {err}", root.display()));

    for entry in entries {
        let entry = entry.unwrap_or_else(|err| {
            panic!(
                "failed to read source directory entry under {}: {err}",
                root.display()
            )
        });
        let path = entry.path();
        if path.is_dir() {
            collect_rust_sources(path.as_path(), out);
            continue;
        }
        if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

fn strip_cfg_test_items(source: &str) -> String {
    let mut output = String::new();
    let mut pending_cfg_test = false;
    let mut skip_depth = 0usize;

    for line in source.lines() {
        let trimmed = line.trim();
        if skip_depth > 0 {
            skip_depth = skip_depth
                .saturating_add(line.matches('{').count())
                .saturating_sub(line.matches('}').count());
            continue;
        }

        if trimmed.starts_with("#[cfg(test)]") {
            pending_cfg_test = true;
            continue;
        }
        if pending_cfg_test {
            let opens = line.matches('{').count();
            let closes = line.matches('}').count();
            if opens > 0 {
                skip_depth = opens.saturating_sub(closes);
            }
            pending_cfg_test = false;
            continue;
        }

        output.push_str(line);
        output.push('\n');
    }

    output
}

fn runtime_surface_needles() -> [String; 2] {
    [
        ["Value", "Surface", "Encode"].concat(),
        ["Value", "Surface", "Decode"].concat(),
    ]
}

fn forbidden_runtime_surface_references() -> BTreeMap<String, Vec<String>> {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db");
    let mut sources = Vec::new();
    collect_rust_sources(source_root.as_path(), &mut sources);
    sources.sort();

    let forbidden_prefixes = ["src/db/commit/", "src/db/data/", "src/db/index/"];
    let needles = runtime_surface_needles();
    let mut violations = BTreeMap::new();

    for source_path in sources {
        if source_path
            .components()
            .any(|part| part.as_os_str() == "tests")
            || source_path
                .file_name()
                .is_some_and(|name| name == "tests.rs")
        {
            continue;
        }

        let relative = source_path
            .strip_prefix(Path::new(env!("CARGO_MANIFEST_DIR")))
            .unwrap_or_else(|err| {
                panic!(
                    "failed to compute relative source path for {}: {err}",
                    source_path.display()
                )
            })
            .to_string_lossy()
            .replace('\\', "/");

        if !forbidden_prefixes
            .iter()
            .any(|prefix| relative.starts_with(prefix))
        {
            continue;
        }

        let source = fs::read_to_string(&source_path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
        let runtime_source = strip_cfg_test_items(source.as_str());
        let matched = needles
            .iter()
            .filter(|needle| runtime_source.contains(needle.as_str()))
            .cloned()
            .collect::<Vec<_>>();

        if !matched.is_empty() {
            violations.insert(relative, matched);
        }
    }

    violations
}

#[test]
fn runtime_surface_traits_stay_out_of_storage_boundaries() {
    let violations = forbidden_runtime_surface_references();

    assert!(
        violations.is_empty(),
        "runtime value-surface traits must not appear in persistence or storage encoding code; violations: {violations:?}",
    );
}
