use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

// Walk one source tree and collect every Rust source path deterministically.
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

// Strip top-level `#[cfg(test)]` items from source text using a lightweight
// brace-depth scanner so runtime-only guard scans ignore inline test modules.
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

// Detect raw `AccessPath::` tokens while excluding prefixed identifiers
// such as `ExplainAccessPath::`.
fn contains_raw_access_path_token(source: &str) -> bool {
    let needle = "AccessPath::";
    let mut offset = 0usize;
    while let Some(found) = source[offset..].find(needle) {
        let absolute = offset + found;
        let previous = if absolute == 0 {
            None
        } else {
            source[..absolute].chars().next_back()
        };
        let preceded_by_identifier =
            previous.is_some_and(|ch| ch.is_ascii_alphanumeric() || ch == '_');
        if !preceded_by_identifier {
            return true;
        }

        offset = absolute + needle.len();
    }

    false
}

#[test]
fn runtime_raw_access_path_references_stay_within_access_boundary() {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db");
    let mut sources = Vec::new();
    collect_rust_sources(source_root.as_path(), &mut sources);
    sources.sort();

    let allowed: BTreeSet<&str> = BTreeSet::from([
        "src/db/access/canonical.rs",
        "src/db/access/dispatch.rs",
        "src/db/access/execution_contract/mod.rs",
        "src/db/access/lowering.rs",
        "src/db/access/path.rs",
        "src/db/access/plan.rs",
    ]);

    let mut violations = Vec::new();
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

        let source = fs::read_to_string(&source_path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
        let runtime_source = strip_cfg_test_items(source.as_str());
        if !contains_raw_access_path_token(runtime_source.as_str()) {
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

        if !allowed.contains(relative.as_str()) {
            violations.push(relative);
        }
    }

    assert!(
        violations.is_empty(),
        "runtime AccessPath variant references must stay access-boundary-local; violations: {}",
        violations.join(", "),
    );
}
