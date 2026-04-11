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

#[test]
fn runtime_as_inner_calls_are_limited_to_boundary_adapters() {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db");
    let mut sources = Vec::new();
    collect_rust_sources(source_root.as_path(), &mut sources);
    sources.sort();

    let allowed: BTreeSet<String> = BTreeSet::new();
    let mut actual = BTreeSet::new();

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
        if !runtime_source.contains(".as_inner(") {
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
        actual.insert(relative);
    }

    assert_eq!(
        actual, allowed,
        "runtime .as_inner() call sites must remain boundary-local; update allowlist only for intentional boundary changes",
    );
}

#[test]
fn runtime_route_capability_helpers_are_not_reintroduced() {
    let executor_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor");
    let mut sources = Vec::new();
    collect_rust_sources(executor_root.as_path(), &mut sources);
    sources.sort();

    let forbidden = [
        "supports_pk_stream_access_executable_path",
        "primary_scan_fetch_hint_for_executable_access_path",
        "secondary_extrema_probe_fetch_hint(",
        "aggregate_secondary_extrema_probe_fetch_hint",
    ];
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
        if forbidden
            .iter()
            .any(|symbol| runtime_source.contains(symbol))
        {
            violations.push(source_path);
        }
    }

    assert!(
        violations.is_empty(),
        "executor runtime must consume direct capability snapshots instead of reintroducing route-capability forwarding helpers. Violations: {}",
        join_display_paths(&violations),
    );
}
