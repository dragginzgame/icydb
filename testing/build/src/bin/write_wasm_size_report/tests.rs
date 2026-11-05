use std::{collections::BTreeMap, env, fs};

use clap::Parser;

use ic_build::{
    artifacts::CargoBuildFacts,
    cargo::selection::{CargoFeatureSelection, CargoSelection},
};

use icydb_testing_build::{
    CanisterBuildOptions, CanisterBuildProfile, CanisterSqlMode, CanisterWasmProfile,
    ResolvedCanisterBuildConfiguration, resolve_fixture_canister_build_configuration,
};

use super::{
    Args, FileMeta, WasmInfo, command_text, endpoint_surface, file_meta, pipeline,
    reduction_basis_points, validate_final_wasm_features, wasm_call_indirect_count, wasm_structure,
};

#[test]
fn reduction_keeps_floor_rounding_and_handles_full_width_artifact_sizes() {
    let artifact = |bytes| FileMeta {
        path: "fixture.wasm".into(),
        bytes,
        sha256: String::new(),
    };
    assert_eq!(
        reduction_basis_points(&artifact(3), &artifact(2)).unwrap(),
        3_333
    );
    assert_eq!(
        reduction_basis_points(&artifact(10), &artifact(10)).unwrap(),
        0
    );
    assert_eq!(
        reduction_basis_points(&artifact(u64::MAX), &artifact(0)).unwrap(),
        10_000
    );
    assert_eq!(
        reduction_basis_points(&artifact(u64::MAX), &artifact(u64::MAX - 1)).unwrap(),
        0
    );
    assert!(reduction_basis_points(&artifact(0), &artifact(0)).is_err());
    assert!(reduction_basis_points(&artifact(10), &artifact(11)).is_err());
}

#[test]
fn report_capture_keeps_native_arguments_utf8_and_refuses_failed_or_oversized_output() {
    let current_dir = env::current_dir().unwrap();
    let shell = std::path::Path::new("sh");
    assert_eq!(
        command_text(
            &current_dir,
            shell,
            &["-c", "printf '  %s  \\n' \"$1\"", "report", "spaced value"]
        )
        .unwrap(),
        "spaced value"
    );
    for script in [
        "printf '\\377'",
        "printf 'partial output'; printf 'producer failure' >&2; exit 7",
        "i=0; while [ \"$i\" -lt 1025 ]; do printf '%01024d' 0; i=$((i + 1)); done",
        "i=0; while [ \"$i\" -lt 1025 ]; do printf '%01024d' 0 >&2; i=$((i + 1)); done",
    ] {
        assert!(command_text(&current_dir, shell, &["-c", script]).is_err());
    }
}

#[cfg(unix)]
#[test]
fn report_capture_keeps_feature_and_metrics_parsing_for_spaced_artifact_paths() {
    use ic_host_process::{
        testing::write_executable_script,
        tool::{AdmittedTool, ExecutionContext, VersionSpec},
    };

    let root =
        ic_host_fs::testing::create_fixture_directory(&env::temp_dir(), "icydb report capture")
            .unwrap();
    let tool = root.join("fixture tool");
    let wasm = root.join("selected artifact.wasm");
    fs::write(&wasm, b"\0asm\x01\0\0\0").unwrap();
    let features = crate::WASM_OPT_OUTPUT_FEATURES.join("\n");
    let script = format!(
        "#!/bin/sh\nif [ \"$1\" = --version ]; then echo fixture; exit 0; fi\n[ -f \"$1\" ] || exit 9\ncase \"$2\" in\n--metrics) [ \"$3\" = --all-features ] || exit 6; printf 'CallIndirect : 7\\n' >&2 ;;\n--enable-bulk-memory) [ \"$6\" = --print-features ] || exit 5; printf '%s\\n' '{features}' ;;\n*) exit 8 ;;\nesac\n"
    );
    // Keep writable executable descriptors out of parallel parent forks.
    write_executable_script(&tool, script.as_bytes()).unwrap();
    let selected_tool = fs::canonicalize(&tool).unwrap();
    let optimizer = AdmittedTool::admit_version(
        &VersionSpec {
            executable: &selected_tool,
            executable_bytes: u64::MAX,
            version_arguments: &["--version".into()],
        },
        "fixture",
        &ExecutionContext {
            current_dir: &root,
            environment: &[],
        },
        crate::REPORT_OUTPUT_LIMITS,
    )
    .unwrap();
    let tools = super::capture_tools(&root, &tool, &optimizer).unwrap();
    assert_eq!(tools.wasm_opt_version, optimizer.version_identity());
    assert_eq!(
        tools.wasm_opt_sha256,
        optimizer.identity().sha256.to_string()
    );
    assert_eq!(
        validate_final_wasm_features(&root, &optimizer, &wasm).unwrap(),
        crate::WASM_OPT_OUTPUT_FEATURES
    );
    assert_eq!(wasm_call_indirect_count(&wasm, &optimizer).unwrap(), 7);
    // Queries must retain admission authority even if the pathname is reused.
    // Keep otherwise-valid output: failure must come from executable drift,
    // rather than a malformed feature/metric response masking execution.
    let mut changed_tool = fs::read(&tool).unwrap();
    changed_tool.extend_from_slice(b"\n# changed executable identity\n");
    write_executable_script(&tool, &changed_tool).unwrap();
    assert!(validate_final_wasm_features(&root, &optimizer, &wasm).is_err());
    assert!(wasm_call_indirect_count(&wasm, &optimizer).is_err());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn wasm_structure_reports_defined_functions_and_complete_code_payload_bytes() {
    let mut bytes = b"\0asm\x01\0\0\0".to_vec();
    let info = wasm_structure(&bytes, "", 0).unwrap();
    assert_eq!(
        (info.defined_function_count, info.code_section_bytes),
        (0, 0)
    );
    assert_eq!(info.function_count, Some(0));

    // Imported functions do not count as defined functions.
    bytes.extend([1, 4, 1, 0x60, 0, 0, 2, 7, 1, 1, b'm', 1, b'f', 0, 0]);
    let info = wasm_structure(&bytes, "Number of functions: 999", 0).unwrap();
    assert_eq!(
        (info.defined_function_count, info.code_section_bytes),
        (0, 0)
    );
    assert_eq!(info.function_count, Some(1));

    // Two defined functions: one body crosses the one-byte LEB128 boundary.
    // The code payload includes its vector count and both body lengths,
    // while excluding the section ID and the section-length prefix.
    bytes.extend([3, 3, 2, 0, 0, 10, 0x88, 0x01, 2, 0x82, 0x01, 0]);
    bytes.extend(std::iter::repeat_n(0x01, 128));
    bytes.extend([0x0b, 2, 0, 0x0b, 0, 2, 1, b'x']);
    let info = wasm_structure(&bytes, "", 7).unwrap();
    assert_eq!(
        (info.defined_function_count, info.code_section_bytes),
        (2, 136)
    );
    assert_eq!(info.function_count, Some(3));
    assert_eq!(info.call_indirect_count, 7);
}

#[test]
fn wasm_structure_rejects_malformed_framing_and_inconsistent_function_counts() {
    for tail in [
        &[3, 2, 1, 0][..],
        &[3, 2, 1, 0, 10, 1, 0][..],
        &[10, 1, 1][..],
        &[1, 0x80, 0x80, 0x80, 0x80, 0x10][..],
    ] {
        let bytes = [b"\0asm\x01\0\0\0".as_slice(), tail].concat();
        assert!(wasm_structure(&bytes, "", 0).is_err());
    }
}

#[test]
fn artifact_identity_keeps_report_fields_and_hashes_complete_files() {
    let root =
        ic_host_fs::testing::create_fixture_directory(&env::temp_dir(), "icydb-report-artifact")
            .unwrap();
    let path = root.join("artifact.wasm");
    let streamed_bytes = vec![0x80; 16 * 1024 + 1];
    for (bytes, digest) in [
        (
            b"".as_slice(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        ),
        (
            b"abc".as_slice(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        ),
        (
            streamed_bytes.as_slice(),
            "06d3b72e7e774a68bba5660dc7225971c8069732da200495a9a5869d074cd8a8",
        ),
    ] {
        fs::write(&path, bytes).expect("report artifact should be writable");
        let report = serde_json::to_value(file_meta(&path).expect("artifact should be readable"))
            .expect("artifact metadata should serialize");
        assert_eq!(
            report,
            serde_json::json!({
                "path": path.display().to_string(),
                "bytes": bytes.len(),
                "sha256": digest,
            })
        );
    }
    fs::remove_dir_all(root).expect("report fixture should be removable");
    assert!(file_meta(&path).is_err());
}

fn resolved(
    canister: &str,
    profile: CanisterWasmProfile,
    sql_mode: CanisterSqlMode,
) -> ResolvedCanisterBuildConfiguration {
    resolve_fixture_canister_build_configuration(
        canister,
        CanisterBuildOptions {
            profile,
            sql_mode,
            build_profile: CanisterBuildProfile::Production,
        },
    )
    .expect("maintained canister build configuration should resolve")
}

fn build_facts(
    canister: &str,
    profile: CanisterWasmProfile,
    sql_mode: CanisterSqlMode,
) -> CargoBuildFacts {
    let config = resolved(canister, profile, sql_mode);
    CargoBuildFacts {
        selection: CargoSelection {
            profile: Some(profile.as_str().to_string()),
            features: CargoFeatureSelection {
                features: config
                    .features()
                    .iter()
                    .map(|name| (*name).to_owned())
                    .collect(),
                default_features: !config.no_default_features(),
                all_features: false,
            },
            locked: true,
            offline: false,
        },
        target: "wasm32-unknown-unknown".to_string(),
        environment: BTreeMap::default(),
        cargo_identity: b"retained cargo\n".to_vec(),
        rustc_identity: b"retained compiler\n".to_vec(),
    }
}

#[test]
fn reporting_uses_retained_observations_without_exposing_the_build_environment() {
    let mut facts = build_facts(
        "sql_perf",
        CanisterWasmProfile::Debug,
        CanisterSqlMode::Disabled,
    );
    facts
        .selection
        .features
        .features
        .insert("observed-feature".to_string());
    facts.selection.features.default_features = true;
    facts
        .environment
        .insert("SECRET".into(), "private-build-input".into());
    let workspace = super::workspace_root().unwrap();
    let provenance = super::capture_provenance(&workspace, &facts).unwrap();
    assert_eq!(
        provenance.rust_toolchain,
        "sha256:ce5ce60b780e1e91986400e36ac372c4f2ecf49a6c5eaf6acc1041f3b9dda81a"
    );
    let json = serde_json::to_value(&provenance).unwrap();
    assert!(!json.to_string().contains("private-build-input"));
    let build = endpoint_surface(&facts, &wasm_info(&[]));
    assert_eq!(build.exact_features, ["observed-feature"]);
    assert_eq!(build.target, "wasm32-unknown-unknown");
    let config = resolved(
        "sql_perf",
        CanisterWasmProfile::Debug,
        CanisterSqlMode::Disabled,
    );
    assert!(!pipeline(&config, &facts).no_default_features);
    facts.rustc_identity = vec![0xff];
    let other = super::capture_provenance(&workspace, &facts).unwrap();
    assert_ne!(other.rust_toolchain, provenance.rust_toolchain);
}

fn wasm_info(exported_methods: &[&str]) -> WasmInfo {
    use ic_host_artifacts::wasm::{Export, ExportKind, InspectionLimits, inspect};

    let mut facts = inspect(
        b"\0asm\x01\0\0\0",
        InspectionLimits {
            module_bytes: 8,
            sections: 0,
            exports: 0,
            custom_sections: 0,
        },
    )
    .unwrap();
    for name in exported_methods {
        facts.exports.insert(
            name,
            Export {
                kind: ExportKind::Function,
                index: 0,
            },
        );
    }
    // A namespace-looking non-function export must not become an endpoint.
    facts.exports.insert(
        "canister_query icydb_schema",
        Export {
            kind: ExportKind::Memory,
            index: 0,
        },
    );
    super::report_wasm_facts(&facts, "Exported methods: [\"wrong\"]", 0).unwrap()
}

#[test]
fn endpoint_inventory_preserves_exact_names_and_ignores_non_methods() {
    let info = wasm_info(&[
        "canister_composite_query icydb_query",
        "canister_query spaced name (with parentheses)",
        "canister_update quoted\"name",
        "canister_init",
        "ordinary_helper",
    ]);
    let build = endpoint_surface(
        &build_facts(
            "sql",
            CanisterWasmProfile::WasmRelease,
            CanisterSqlMode::Enabled,
        ),
        &info,
    );
    assert!(build.generated_endpoint_surface.sql_readonly);
    assert!(!build.generated_endpoint_surface.schema);
    assert_eq!(
        build.custom_exports,
        ["spaced name (with parentheses)", "quoted\"name"]
    );
    assert_eq!(info.exported_method_count, 5);
    assert!(info.exported_methods.contains(&"canister_init".to_string()));
    assert!(
        info.exported_methods
            .contains(&"ordinary_helper".to_string())
    );
    let json = serde_json::to_value(&info).unwrap();
    assert!(json.get("method_names").is_none());
    assert_eq!(
        json["exported_methods"],
        serde_json::json!(info.exported_methods)
    );
}

#[test]
fn post_link_contract_preserves_modes_lifecycle_exports_and_complete_names() {
    let mut arguments = [
        "--canister",
        "sql",
        "--profile",
        "wasm-release",
        "--build-profile",
        "production",
        "--sql-mode",
        "on",
    ]
    .map(str::to_string)
    .to_vec();
    for flag in [
        "--compiler-wasm",
        "--final-wasm",
        "--final-gz",
        "--compiler-info",
        "--final-info",
        "--report-json",
        "--summary-md",
        "--ic-wasm-bin",
        "--wasm-opt-bin",
    ] {
        arguments.extend([flag.to_string(), "unused".to_string()]);
    }
    let args = Args::try_parse_from(
        std::iter::once("write_wasm_size_report".to_string()).chain(arguments),
    )
    .unwrap();
    let before = super::FileMeta {
        path: "before".to_string(),
        bytes: 100,
        sha256: String::new(),
    };
    let after = super::FileMeta {
        path: "after".to_string(),
        bytes: 1,
        sha256: String::new(),
    };
    let resolved = build_facts(
        "sql",
        CanisterWasmProfile::WasmRelease,
        CanisterSqlMode::Enabled,
    );
    let compiler = wasm_info(&[
        "canister_query spaced name",
        "canister_init",
        "ordinary_helper",
    ]);
    assert!(
        super::validate_post_link_contract(&args, &before, &after, &compiler, &compiler, &resolved)
            .is_ok()
    );
    for changed in [
        [
            "canister_update spaced name",
            "canister_init",
            "ordinary_helper",
        ],
        [
            "canister_query spaced other",
            "canister_init",
            "ordinary_helper",
        ],
        [
            "canister_query spaced name",
            "canister_pre_upgrade",
            "ordinary_helper",
        ],
        [
            "canister_query spaced name",
            "canister_init",
            "other_helper",
        ],
    ] {
        assert!(
            super::validate_post_link_contract(
                &args,
                &before,
                &after,
                &compiler,
                &wasm_info(&changed),
                &resolved
            )
            .is_err()
        );
    }
}

#[test]
fn data_payload_metric_keeps_its_meaning_separate_from_encoded_section_bytes() {
    // One passive segment containing three bytes; encoded payload is six bytes.
    let bytes = b"\0asm\x01\0\0\0\x0b\x06\x01\x01\x03abc";
    let info = wasm_structure(
        bytes,
        "Number of data sections: 999\nSize of data sections: 3 bytes\nNumber of callbacks: 2",
        0,
    )
    .unwrap();
    assert_eq!(info.data_section_count, Some(1));
    assert_eq!(info.data_section_bytes, Some(3));
    assert_eq!(info.callback_count, Some(2));
}

#[test]
fn endpoint_surface_reports_absent_generated_sql_update_endpoint() {
    let resolved = build_facts(
        "sql",
        CanisterWasmProfile::WasmRelease,
        CanisterSqlMode::Enabled,
    );
    let build = endpoint_surface(
        &resolved,
        &wasm_info(&[
            "canister_query icydb_query",
            "canister_update icydb_ddl",
            "canister_update icydb_fixtures_reset",
            "canister_update icydb_fixtures_load",
        ]),
    );

    assert!(build.generated_endpoint_surface.sql_readonly);
    assert!(build.generated_endpoint_surface.sql_ddl);
    assert!(build.generated_endpoint_surface.sql_fixtures);
    assert!(!build.generated_endpoint_surface.sql_update);
    assert!(!build.generated_endpoint_surface.sql_integrity);
    assert!(build.custom_exports.is_empty());
}

#[test]
fn endpoint_surface_reports_generated_sql_update_endpoint() {
    let resolved = build_facts(
        "sql",
        CanisterWasmProfile::WasmRelease,
        CanisterSqlMode::Enabled,
    );
    let build = endpoint_surface(
        &resolved,
        &wasm_info(&[
            "canister_query icydb_query",
            "canister_update icydb_update",
            "canister_update icydb_integrity",
            "canister_update icydb_schema_migrate",
            "canister_query icydb_schema_migration",
            "canister_query custom_query",
        ]),
    );

    assert!(build.generated_endpoint_surface.sql_update);
    assert!(build.generated_endpoint_surface.sql_integrity);
    assert!(build.generated_endpoint_surface.schema);
    assert_eq!(build.custom_exports, ["custom_query"]);
}

#[test]
fn production_feature_identity_is_exact_and_variant_sensitive() {
    let sql_on = endpoint_surface(
        &build_facts(
            "sql_perf",
            CanisterWasmProfile::WasmRelease,
            CanisterSqlMode::Enabled,
        ),
        &wasm_info(&[]),
    );
    assert_eq!(sql_on.exact_features, ["sql"]);

    let sql_off = endpoint_surface(
        &build_facts(
            "sql_perf",
            CanisterWasmProfile::WasmRelease,
            CanisterSqlMode::Disabled,
        ),
        &wasm_info(&[]),
    );
    assert!(sql_off.exact_features.is_empty());
}

#[test]
fn pipeline_identity_comes_from_resolved_build_configuration() {
    let debug = resolved(
        "sql_perf",
        CanisterWasmProfile::Debug,
        CanisterSqlMode::Disabled,
    );
    let pipeline = pipeline(
        &debug,
        &build_facts(
            "sql_perf",
            CanisterWasmProfile::Debug,
            CanisterSqlMode::Disabled,
        ),
    );

    assert_eq!(pipeline.build_profile, "production");
    assert!(pipeline.no_default_features);
    assert_eq!(pipeline.path_remapping, "disabled");
}

#[test]
fn report_arguments_reject_untyped_build_labels() {
    assert!(Args::try_parse_from(["write_wasm_size_report", "--profile", "profile-ish"]).is_err());
    assert!(Args::try_parse_from(["write_wasm_size_report", "--sql-mode", "maybe"]).is_err());
}
