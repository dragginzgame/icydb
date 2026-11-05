use super::*;

#[test]
fn wrong_version_and_unknown_fields_fail_closed() {
    let wrong_version = br#"{
        "format":"icydb-diagnostic-schema",
        "version":2,
        "provenance":{"kind":"deployment","environment":"demo","canister":"app"},
        "entities":[]
    }"#;
    let artifact: DiagnosticSchemaArtifact =
        serde_json::from_slice(wrong_version).expect("shape should decode");
    assert!(artifact.validate().is_err());

    let unknown_field = br#"{
        "format":"icydb-diagnostic-schema",
        "version":1,
        "provenance":{"kind":"deployment","environment":"demo","canister":"app"},
        "entities":[],
        "legacy":true
    }"#;
    assert!(serde_json::from_slice::<DiagnosticSchemaArtifact>(unknown_field).is_err());
}

#[test]
fn exact_fingerprint_and_entity_tag_are_required_for_resolution() {
    let artifact = DiagnosticSchemaArtifact::test_fixture();

    assert!(artifact.resolve(1, [7; 16], 42, Some(3)).is_some());
    assert!(artifact.resolve(2, [7; 16], 42, Some(3)).is_none());
    assert!(artifact.resolve(1, [8; 16], 42, Some(3)).is_none());
    assert!(artifact.resolve(1, [7; 16], 43, Some(3)).is_none());
    assert!(artifact.resolve(1, [7; 16], 42, Some(4)).is_none());
}

fn ordered_identity_fixture(order: [u32; 3]) -> DiagnosticSchemaArtifact {
    let mut artifact = DiagnosticSchemaArtifact::test_fixture();
    let entity = &mut artifact.entities[0];
    for (label, identities) in [
        ("field", &mut entity.fields),
        ("index", &mut entity.indexes),
        ("relation", &mut entity.relations),
    ] {
        *identities = order
            .map(|id| DiagnosticArtifactIdentity {
                id,
                name: format!("{label}_{id}"),
            })
            .into();
    }
    entity.constraints = order
        .map(|id| DiagnosticArtifactConstraint {
            id,
            name: format!("constraint_{id}"),
            kind: "unique".to_string(),
            field_ids: vec![id],
            index_id: Some(id),
            relation_id: Some(id),
        })
        .into();
    artifact
}

#[test]
fn artifact_labels_resolve_independently_of_json_list_order() {
    let root = ic_host_fs::testing::create_fixture_directory(
        &std::env::temp_dir(),
        "icydb-artifact-list-order",
    )
    .unwrap();
    let path = root.join("artifact.json");
    for order in [
        [3, 5, 7],
        [3, 7, 5],
        [5, 3, 7],
        [5, 7, 3],
        [7, 3, 5],
        [7, 5, 3],
    ] {
        let deployment = ordered_identity_fixture(order);
        let source = deployment
            .clone()
            .bind_to_source("schema/account.rs")
            .unwrap();
        for artifact in [deployment, source] {
            std::fs::write(&path, serde_json::to_vec(&artifact).unwrap()).unwrap();
            let decoded = match &artifact.provenance {
                DiagnosticArtifactOwner::Deployment { .. } => {
                    DiagnosticSchemaArtifact::read_deployment(&path)
                }
                DiagnosticArtifactOwner::Source { .. } => {
                    DiagnosticSchemaArtifact::read_source_metadata(&path)
                }
            }
            .unwrap();
            assert_eq!(decoded, artifact);
            let resolved = decoded.resolve(1, [7; 16], 42, None).unwrap();
            for id in [3, 5, 7] {
                assert_eq!(
                    resolved.field_name(id),
                    Some(format!("field_{id}").as_str())
                );
                assert_eq!(
                    resolved.index_name(id),
                    Some(format!("index_{id}").as_str())
                );
                assert_eq!(
                    resolved.relation_name(id),
                    Some(format!("relation_{id}").as_str())
                );
                let constraint = decoded.resolve(1, [7; 16], 42, Some(id)).unwrap();
                assert_eq!(
                    constraint.constraint_name(),
                    Some(format!("constraint_{id}").as_str())
                );
                assert_eq!(constraint.constraint_kind(), Some("unique"));
            }
            for id in [0, 4, 8] {
                assert_eq!(resolved.field_name(id), None);
                assert_eq!(resolved.index_name(id), None);
                assert_eq!(resolved.relation_name(id), None);
                assert!(decoded.resolve(1, [7; 16], 42, Some(id)).is_none());
            }
            assert!(decoded.resolve(2, [7; 16], 42, Some(3)).is_none());
            assert!(decoded.resolve(1, [8; 16], 42, Some(3)).is_none());
            assert!(decoded.resolve(1, [7; 16], 43, Some(3)).is_none());
        }
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn duplicate_artifact_ids_are_rejected_in_every_identity_list() {
    for list in ["fields", "indexes", "relations", "constraints"] {
        let mut json = serde_json::to_value(ordered_identity_fixture([7, 3, 5])).unwrap();
        json["entities"][0][list][2]["id"] = serde_json::json!(3);
        let artifact: DiagnosticSchemaArtifact = serde_json::from_value(json).unwrap();
        assert!(artifact.validate().is_err(), "{list}");
    }
}

#[test]
fn artifact_identity_is_projected_from_the_accepted_description() {
    let report = [icydb::db::EntitySchemaDescription::new(
        "schema::Account".to_string(),
        "Account".to_string(),
        42,
        1,
        [7; 16],
        "id".to_string(),
        vec!["id".to_string()],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        1,
        1,
    )];
    let artifact = DiagnosticSchemaArtifact::from_report("demo", "app", &report)
        .expect("accepted description should export");

    assert!(artifact.provenance_matches("demo", "app"));
    let resolved = artifact
        .resolve(1, [7; 16], 42, None)
        .expect("exact accepted identity should resolve");
    assert_eq!(resolved.entity_name(), "Account");
    assert_eq!(resolved.entity_path(), "schema::Account");
}

#[test]
fn current_artifact_roundtrips_without_overwriting_existing_output() {
    let artifact = DiagnosticSchemaArtifact::test_fixture();
    let root = ic_host_fs::testing::create_fixture_directory(
        &std::env::temp_dir(),
        "icydb-diagnostic-artifact",
    )
    .unwrap();
    let path = root.join("artifact.json");

    artifact
        .write_new(path.as_path())
        .expect("current artifact should write");
    assert_eq!(
        DiagnosticSchemaArtifact::read_deployment(path.as_path())
            .expect("current artifact should read"),
        artifact
    );
    assert!(artifact.write_new(path.as_path()).is_err());

    std::fs::remove_dir_all(root).expect("test fixture should be removable");
}

#[test]
#[cfg(unix)]
fn artifact_publication_preserves_parent_and_create_only_policy() {
    use std::os::unix::fs::{PermissionsExt as _, symlink};
    let root = ic_host_fs::testing::create_fixture_directory(
        &std::env::temp_dir(),
        "icydb-artifact-publication",
    )
    .unwrap();
    let artifact = DiagnosticSchemaArtifact::test_fixture();
    assert!(
        artifact
            .write_new(&root.join("missing/output.json"))
            .is_err()
    );
    assert!(!root.join("missing").exists());
    let parent = root.join("parent");
    std::fs::create_dir(&parent).unwrap();
    let linked_parent = root.join("linked-parent");
    symlink(&parent, &linked_parent).unwrap();
    let target = linked_parent.join("output.json");
    artifact.write_new(&target).unwrap();
    let expected = serde_json::to_vec_pretty(&artifact).unwrap();
    assert_eq!(std::fs::read(&target).unwrap(), expected);
    assert_eq!(std::fs::read_dir(&parent).unwrap().count(), 1);
    // Compare against ordinary creation under the current umask without
    // changing process-global permissions during parallel tests.
    let ordinary = parent.join("ordinary");
    std::fs::write(&ordinary, b"preserved").unwrap();
    assert_eq!(
        std::fs::metadata(&target).unwrap().permissions().mode() & 0o777,
        std::fs::metadata(&ordinary).unwrap().permissions().mode() & 0o777
    );
    let link = parent.join("link.json");
    symlink(&ordinary, &link).unwrap();
    assert!(artifact.write_new(&link).is_err());
    assert_eq!(std::fs::read(&ordinary).unwrap(), b"preserved");
    assert!(
        std::fs::symlink_metadata(&link)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert!(artifact.write_new(&target).is_err());
    assert_eq!(std::fs::read(&target).unwrap(), expected);
    assert_eq!(std::fs::read_dir(&parent).unwrap().count(), 3);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
#[cfg(unix)]
fn artifact_publication_rejects_directory_required_targets() {
    let root = ic_host_fs::testing::create_fixture_directory(
        &std::env::temp_dir(),
        "icydb-artifact-target-suffix",
    )
    .unwrap();
    let existing = root.join("existing");
    std::fs::write(&existing, b"preserved").unwrap();
    let artifact = DiagnosticSchemaArtifact::test_fixture();
    for name in ["existing", "absent", "missing/child"] {
        for suffix in ["/", "//", "/.", "/./.", "/.//"] {
            let mut raw = root.join(name).into_os_string();
            raw.push(suffix);
            assert!(artifact.write_new(std::path::Path::new(&raw)).is_err());
            assert_eq!(std::fs::read(&existing).unwrap(), b"preserved");
            assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
        }
    }
    artifact.write_new(&root.join("./ordinary")).unwrap();
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 2);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn artifact_reads_preserve_exact_limits_and_reject_invalid_files() {
    let root = ic_host_fs::testing::create_fixture_directory(
        &std::env::temp_dir(),
        "icydb-diagnostic-read-boundary",
    )
    .unwrap();
    let path = root.join("artifact.json");
    let artifact = DiagnosticSchemaArtifact::test_fixture();
    let mut bytes = serde_json::to_vec(&artifact).unwrap();
    bytes.resize(super::MAX_DIAGNOSTIC_ARTIFACT_BYTES, b' ');
    std::fs::write(&path, &bytes).unwrap();
    assert_eq!(
        DiagnosticSchemaArtifact::read_deployment(&path).unwrap(),
        artifact
    );
    bytes.push(b' ');
    std::fs::write(&path, &bytes).unwrap();
    assert!(DiagnosticSchemaArtifact::read_deployment(&path).is_err());
    std::fs::write(&path, b"{invalid JSON").unwrap();
    assert!(DiagnosticSchemaArtifact::read_deployment(&path).is_err());
    std::fs::remove_file(&path).unwrap();
    assert!(DiagnosticSchemaArtifact::read_deployment(&path).is_err());
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(unix)]
#[test]
fn artifact_reads_preserve_symlink_selection_and_reject_special_files() {
    let root = ic_host_fs::testing::create_fixture_directory(
        &std::env::temp_dir(),
        "icydb-diagnostic-read-link",
    )
    .unwrap();
    let path = root.join("artifact.json");
    let link = path.with_extension("link");
    let artifact = DiagnosticSchemaArtifact::test_fixture();
    std::fs::write(&path, serde_json::to_vec(&artifact).unwrap()).unwrap();
    std::os::unix::fs::symlink(&path, &link).unwrap();
    assert_eq!(
        DiagnosticSchemaArtifact::read_deployment(&link).unwrap(),
        artifact
    );
    assert!(DiagnosticSchemaArtifact::read_deployment(Path::new("/dev/null")).is_err());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn deployment_and_source_provenance_are_not_interchangeable() {
    let deployment = DiagnosticSchemaArtifact::test_fixture();
    let source = DiagnosticSchemaArtifact::test_source_fixture();
    let root = ic_host_fs::testing::create_fixture_directory(
        &std::env::temp_dir(),
        "icydb-diagnostic-provenance",
    )
    .unwrap();
    let deployment_path = root.join("deployment.json");
    let source_path = root.join("source.json");

    deployment
        .write_new(deployment_path.as_path())
        .expect("deployment artifact should write");
    source
        .write_new(source_path.as_path())
        .expect("source metadata should write");

    assert!(
        DiagnosticSchemaArtifact::read_deployment(source_path.as_path()).is_err(),
        "source metadata must not impersonate a deployment artifact"
    );
    assert!(
        DiagnosticSchemaArtifact::read_source_metadata(deployment_path.as_path()).is_err(),
        "deployment provenance must not impersonate source metadata"
    );
    assert_eq!(
        DiagnosticSchemaArtifact::read_source_metadata(source_path.as_path())
            .expect("source metadata should read"),
        source
    );

    std::fs::remove_dir_all(root).expect("test fixture should be removable");
}

#[test]
fn source_binding_preserves_the_exact_accepted_artifact_mapping() {
    let deployment = DiagnosticSchemaArtifact::test_fixture();
    let source = deployment
        .clone()
        .bind_to_source("schema/account.rs")
        .expect("valid source identity should bind");

    assert!(!source.provenance_matches("demo", "app"));
    assert_eq!(source.entities, deployment.entities);
    assert!(source.resolve(1, [7; 16], 42, Some(3)).is_some());
    assert!(source.resolve(1, [7; 16], 42, Some(4)).is_none());
    assert!(deployment.bind_to_source("").is_err());
}
