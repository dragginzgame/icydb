use super::*;

#[test]
fn stream_access_module_limits_direct_store_traversal_to_scan_boundary() {
    let access_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor/stream/access");
    let mut sources = Vec::new();
    collect_rust_sources(access_root.as_path(), &mut sources);
    sources.sort();

    let allowed = ["scan.rs"];
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

        if source_path
            .file_name()
            .is_some_and(|name| allowed.contains(&name.to_string_lossy().as_ref()))
        {
            continue;
        }

        let source = fs::read_to_string(&source_path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
        assert!(
            !source_uses_direct_store_or_registry_access(source.as_str()),
            "stream access file {} must not directly traverse store/registry; only scan boundary adapters may do so",
            source_path.display(),
        );
    }
}

#[test]
fn physical_module_has_no_direct_store_traversal() {
    let source_path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor/stream/access/physical.rs");
    let source = fs::read_to_string(&source_path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));

    assert!(
        !source_uses_direct_store_or_registry_access(source.as_str()),
        "stream access physical resolver must request access via PrimaryScan/IndexScan adapters, not direct store handles",
    );
}

#[test]
fn executor_runtime_modules_have_no_raw_access_path_variant_matching() {
    let executor_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor");
    let mut sources = Vec::new();
    collect_rust_sources(executor_root.as_path(), &mut sources);
    sources.sort();

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
        if runtime_source.contains("AccessPath::") {
            violations.push(source_path);
        }
    }

    assert!(
        violations.is_empty(),
        "executor runtime modules must not pattern-match raw AccessPath variants; violations: {}",
        join_display_paths(&violations),
    );
}
