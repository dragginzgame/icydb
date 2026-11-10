use std::{collections::BTreeSet, fs};

use super::{
    CanisterMethod, MAINTAINED_CANISTER_POLICIES, MethodMode, application_wasm_methods,
    inspect_canister_artifacts, inspect_wasm_methods, inspect_wire_methods, read_wasm_artifact,
    render_schema_migration_endpoint_contracts,
};
use ic_host_artifacts::wasm::{CustomSection, encode_custom_section};
use icydb_testing_wasm_helpers::wire::DynamicRowsQuery;

#[test]
fn wire_declarations_require_one_known_current_identity() {
    let mut wasm = wasm_with_exports(&[("canister_query query_one_entity_dynamic_rows", 0)]);
    assert!(inspect_wire_methods(&wasm).unwrap().is_empty());
    let declaration = encode_custom_section(
        CustomSection {
            name: "icydb:wire",
            data: &DynamicRowsQuery::ID.to_le_bytes(),
        },
        64,
    )
    .unwrap();
    wasm.extend(&declaration);
    assert_eq!(
        inspect_wire_methods(&wasm).unwrap(),
        BTreeSet::from([CanisterMethod::new(
            DynamicRowsQuery::NAME,
            MethodMode::Query
        ),])
    );
    wasm.extend(&declaration);
    assert!(inspect_wire_methods(&wasm).is_err());
    let mut unknown = b"\0asm\x01\0\0\0".to_vec();
    unknown.extend(
        encode_custom_section(
            CustomSection {
                name: "icydb:wire",
                data: &[0; 8],
            },
            64,
        )
        .unwrap(),
    );
    assert!(inspect_wire_methods(&unknown).is_err());
}

#[test]
fn wire_declarations_admit_multiple_methods_and_reject_partial_identities() {
    let mut wasm = wasm_with_exports(&[
        ("canister_query icydb_metrics", 0),
        ("canister_update icydb_metrics_reset", 0),
    ]);
    let identities = [
        icydb::__macro::RpcMethod::Metrics.id().to_le_bytes(),
        icydb::__macro::RpcMethod::MetricsReset.id().to_le_bytes(),
    ]
    .concat();
    wasm.extend(
        encode_custom_section(
            CustomSection {
                name: "icydb:wire",
                data: &identities,
            },
            64,
        )
        .unwrap(),
    );
    assert_eq!(
        inspect_wire_methods(&wasm).unwrap(),
        BTreeSet::from([
            CanisterMethod::new("icydb_metrics", MethodMode::Query),
            CanisterMethod::new("icydb_metrics_reset", MethodMode::Update),
        ])
    );
    wasm.extend(
        encode_custom_section(
            CustomSection {
                name: "icydb:wire",
                data: &[1],
            },
            64,
        )
        .unwrap(),
    );
    assert!(inspect_wire_methods(&wasm).is_err());
}

#[test]
fn wire_artifact_agreement_and_publication_preserve_failure_boundaries() {
    let root = ic_host_fs::testing::create_fixture_directory(
        &std::env::temp_dir(),
        "icydb wire publication",
    )
    .unwrap();
    let input = root.join("input.wasm");
    let mut wasm = wasm_with_exports(&[("canister_query query_one_entity_dynamic_rows", 0)]);
    fs::write(&input, &wasm).unwrap();
    assert!(inspect_canister_artifacts(&input).is_err());
    wasm.extend(
        encode_custom_section(
            CustomSection {
                name: "icydb:wire",
                data: &DynamicRowsQuery::ID.to_le_bytes(),
            },
            64,
        )
        .unwrap(),
    );
    fs::write(&input, &wasm).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&input, fs::Permissions::from_mode(0o640)).unwrap();
    }
    let manifest = inspect_canister_artifacts(&input).unwrap();
    assert_eq!(manifest.wire_methods, manifest.wasm_methods);
    let staged =
        crate::stage_canister_artifact_paths(&input, &input, &root.join("staged"), "probe")
            .unwrap();
    assert_eq!(fs::read(&staged).unwrap(), wasm);
    assert_eq!(
        fs::read(staged.with_file_name("probe.compiler.wasm")).unwrap(),
        wasm
    );
    assert_eq!(fs::read(&input).unwrap(), wasm);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(&staged).unwrap().permissions().mode() & 0o777,
            0o640
        );
    }
    assert!(crate::publish_artifact_copy(&root, &staged).is_err());
    assert_eq!(fs::read(&staged).unwrap(), wasm);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn artifact_reads_preserve_complete_regular_files_and_follow_selected_links() {
    use ic_host_artifacts::artifact::ArtifactError;

    let root =
        ic_host_fs::testing::create_fixture_directory(&std::env::temp_dir(), "icydb wasm reads")
            .unwrap();
    let path = root.join("complete.wasm");
    for bytes in [Vec::new(), vec![0x80; 16 * 1024 + 1]] {
        fs::write(&path, &bytes).unwrap();
        assert_eq!(read_wasm_artifact(&path).unwrap(), bytes);
    }
    #[cfg(unix)]
    {
        let link = root.join("selected-link.wasm");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert_eq!(read_wasm_artifact(&link).unwrap(), fs::read(&path).unwrap());
    }
    assert!(matches!(
        read_wasm_artifact(&root),
        Err(ArtifactError::NotRegularFile)
    ));
    assert!(matches!(
        read_wasm_artifact(&root.join("missing.wasm")),
        Err(ArtifactError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound
    ));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn raw_wasm_inspection_reads_only_ic_function_exports() {
    let wasm = wasm_with_exports(&[
        ("canister_composite_query composed", 0),
        ("canister_query read", 0),
        ("canister_update write", 0),
        ("canister_update <ic-timers internal> dispatch", 0),
        ("ordinary_helper", 0),
        ("canister_query not_a_function", 3),
    ]);

    let observed = inspect_wasm_methods(&wasm).expect("Wasm should inspect");
    let expected = BTreeSet::from([
        CanisterMethod::new("composed", MethodMode::CompositeQuery),
        CanisterMethod::new("<ic-timers internal> dispatch", MethodMode::Update),
        CanisterMethod::new("read", MethodMode::Query),
        CanisterMethod::new("write", MethodMode::Update),
    ]);
    assert_eq!(observed, expected);
}

#[test]
fn raw_wasm_inspection_rejects_duplicate_exports_and_malformed_sections() {
    let duplicate = wasm_with_exports(&[("canister_query read", 0), ("canister_query read", 0)]);
    assert!(inspect_wasm_methods(&duplicate).is_err());
    for tail in [
        &[7, 2, 1][..],
        &[7, 0x80, 0x80, 0x80, 0x80, 0x10][..],
        &[7, 2, 0, 0][..],
    ] {
        let wasm = [b"\0asm\x01\0\0\0".as_slice(), tail].concat();
        assert!(inspect_wasm_methods(&wasm).is_err());
    }
}

#[test]
fn declaration_agreement_excludes_only_reserved_timer_runtime_export() {
    let methods = BTreeSet::from([
        CanisterMethod::new("<ic-timers internal> dispatch", MethodMode::Update),
        CanisterMethod::new("<ic-cdk internal> unexpected", MethodMode::Update),
        CanisterMethod::new("read", MethodMode::Query),
        CanisterMethod::new("write", MethodMode::Update),
    ]);

    assert_eq!(
        application_wasm_methods(&methods),
        BTreeSet::from([
            CanisterMethod::new("<ic-cdk internal> unexpected", MethodMode::Update,),
            CanisterMethod::new("read", MethodMode::Query),
            CanisterMethod::new("write", MethodMode::Update),
        ]),
    );
}

#[test]
fn maintained_policy_identity_is_unique_and_policy_is_deterministic() {
    let names = MAINTAINED_CANISTER_POLICIES
        .iter()
        .map(|policy| policy.canister)
        .collect::<BTreeSet<_>>();
    let packages = MAINTAINED_CANISTER_POLICIES
        .iter()
        .map(|policy| policy.package)
        .collect::<BTreeSet<_>>();
    assert_eq!(names.len(), MAINTAINED_CANISTER_POLICIES.len());
    assert_eq!(packages.len(), MAINTAINED_CANISTER_POLICIES.len());
    for policy in MAINTAINED_CANISTER_POLICIES {
        assert!(policy.production_features.is_sorted());
        assert!(policy.local_test_features.is_sorted());
        assert!(policy.local_test_icydb_methods.is_sorted());
        assert!(policy.production_icydb_methods.is_sorted());
        assert!(
            policy
                .production_fixture_updates
                .windows(2)
                .all(|pair| pair[0] < pair[1])
        );
        assert!(
            policy
                .production_fixture_updates
                .iter()
                .all(|name| !name.starts_with("icydb_"))
        );
    }
}

#[test]
fn schema_migration_endpoint_contracts_match_golden() {
    assert_eq!(
        render_schema_migration_endpoint_contracts(),
        include_str!("../contracts/0.277/schema-migration-endpoints.json")
    );
}

fn wasm_with_exports(exports: &[(&str, u8)]) -> Vec<u8> {
    let mut section = Vec::new();
    push_u32_leb(
        &mut section,
        u32::try_from(exports.len()).expect("fixture count fits"),
    );
    for (index, (name, kind)) in exports.iter().enumerate() {
        push_u32_leb(
            &mut section,
            u32::try_from(name.len()).expect("fixture name length fits"),
        );
        section.extend_from_slice(name.as_bytes());
        section.push(*kind);
        push_u32_leb(
            &mut section,
            u32::try_from(index).expect("fixture index fits"),
        );
    }

    let mut wasm = b"\0asm\x01\0\0\0".to_vec();
    wasm.push(7);
    push_u32_leb(
        &mut wasm,
        u32::try_from(section.len()).expect("fixture section length fits"),
    );
    wasm.extend_from_slice(&section);
    wasm
}

fn push_u32_leb(output: &mut Vec<u8>, mut value: u32) {
    loop {
        let mut byte = u8::try_from(value & 0x7f).expect("seven bits fit u8");
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        output.push(byte);
        if value == 0 {
            return;
        }
    }
}
