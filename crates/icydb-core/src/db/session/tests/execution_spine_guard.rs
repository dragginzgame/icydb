use std::{
    fs,
    path::{Path, PathBuf},
};

#[test]
fn production_session_surfaces_do_not_direct_plan_outside_shared_cache() {
    let manifest_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let checked_roots = [
        manifest_root.join("src/db/session"),
        manifest_root.join("src/db/query/fluent"),
    ];
    let allowed = [
        "src/db/session/query/cache.rs",
        "src/db/session/tests/execution_spine_guard.rs",
    ];
    let forbidden = [
        "build_plan_with_visible_indexes(",
        "build_plan_with_visible_indexes_from_scalar_planning_state(",
    ];
    let mut sources = Vec::new();
    for root in checked_roots {
        collect_rust_sources(root.as_path(), &mut sources);
    }
    sources.sort();

    // Scan only production session/fluent sources. The query cache is allowed
    // to call the low-level planner because it is the canonical cache-fill
    // owner; tests are allowed to inspect direct planner APIs explicitly.
    let mut violations = Vec::new();
    for source_path in sources {
        let relative = relative_rust_source_path(manifest_root, source_path.as_path());
        if allowed.contains(&relative.as_str()) || relative.contains("/tests/") {
            continue;
        }

        let source = fs::read_to_string(&source_path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
        if forbidden.iter().any(|symbol| source.contains(symbol)) {
            violations.push(relative);
        }
    }

    assert!(
        violations.is_empty(),
        "production session SQL/fluent paths must route direct planning through shared query cache only. Violations: {}",
        violations.join(", "),
    );
}

#[test]
fn fluent_terminal_surfaces_do_not_reintroduce_shared_strategy_enums() {
    let manifest_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let checked_roots = [
        manifest_root.join("src/db/query/builder/aggregate/strategy"),
        manifest_root.join("src/db/query/fluent/load"),
    ];
    let checked_files = [manifest_root.join("src/db/session/query/fluent.rs")];
    let forbidden = [
        "ExistingRowsTerminalStrategy",
        "ScalarTerminalStrategy",
        "OrderSensitiveTerminalStrategy",
        "NumericFieldStrategy",
        "ProjectionStrategy",
        "ExistingRowsRequest",
        "ScalarTerminalRequest",
        "OrderRequest",
        "NumericFieldRequest",
        "ProjectionRequest",
        "FluentScalarTerminalOutput",
        "FluentProjectionTerminalOutput",
        "execute_fluent_existing_rows_terminal",
        "execute_fluent_scalar_terminal",
        "execute_fluent_order_sensitive_terminal",
        "execute_fluent_numeric_field_terminal",
        "execute_fluent_projection_terminal",
    ];
    let mut sources = Vec::new();
    for root in checked_roots {
        collect_rust_sources(root.as_path(), &mut sources);
    }
    sources.extend(checked_files);
    sources.sort();

    // Fluent terminal descriptors are intentionally monomorphic. This guard
    // keeps the old shared strategy/request/transport names from returning in
    // the production terminal surfaces where they would reintroduce dispatch
    // decisions that now belong to concrete descriptor types.
    let mut violations = Vec::new();
    for source_path in sources {
        let relative = relative_rust_source_path(manifest_root, source_path.as_path());
        let source = fs::read_to_string(&source_path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
        let symbols: Vec<&str> = forbidden
            .iter()
            .copied()
            .filter(|symbol| source.contains(symbol))
            .collect();

        if !symbols.is_empty() {
            violations.push(format!("{} ({})", relative, symbols.join(", ")));
        }
    }

    assert!(
        violations.is_empty(),
        "fluent terminal execution must stay descriptor-based with no shared strategy/request/transport enums. Violations: {}",
        violations.join("; "),
    );
}

#[test]
fn sql_ddl_frontend_does_not_take_schema_store_or_generated_index_authority() {
    let manifest_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let checked_files = [
        manifest_root.join("src/db/sql/ddl.rs"),
        manifest_root.join("src/db/session/sql/mod.rs"),
    ];
    let forbidden = [
        "SchemaStore",
        "with_schema_mut(",
        "latest_persisted_snapshot(",
        "publish_accepted_snapshot(",
        "compiled_schema_proposal_for_model",
        ".indexes()",
        "model.indexes()",
        "MODEL.indexes",
    ];
    let mut violations = Vec::new();

    // SQL DDL frontend code may bind accepted catalog facts and call the
    // schema-owned mutation runner, but schema-store publication and generated
    // index metadata authority must stay outside this frontend boundary.
    for source_path in checked_files {
        let relative = relative_rust_source_path(manifest_root, source_path.as_path());
        let source = fs::read_to_string(&source_path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
        let symbols: Vec<&str> = forbidden
            .iter()
            .copied()
            .filter(|symbol| source.contains(symbol))
            .collect();

        if !symbols.is_empty() {
            violations.push(format!("{} ({})", relative, symbols.join(", ")));
        }
    }

    assert!(
        violations.is_empty(),
        "SQL DDL frontend code must route publication through schema-owned mutation runners and must not source index authority from generated metadata. Violations: {}",
        violations.join("; "),
    );
}

// Walk one source tree and collect Rust files deterministically for the
// production-path guardrail above.
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

// Normalize paths relative to the crate root so assertion output is stable
// across machines and path separators.
fn relative_rust_source_path(manifest_root: &Path, source_path: &Path) -> String {
    source_path
        .strip_prefix(manifest_root)
        .unwrap_or_else(|err| {
            panic!(
                "failed to compute relative source path for {}: {err}",
                source_path.display()
            )
        })
        .to_string_lossy()
        .replace('\\', "/")
}
