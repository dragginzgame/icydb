use std::{env, fs, path::PathBuf, process::Command};

use super::{
    ArtifactCacheOutcome, CACHE_MAX_AGE, CACHE_MAX_BYTES, CARGO_CONFIGURATION_LIMITS,
    CargoSelection, CargoWasmBatchEntry, CargoWasmCacheRequest, PostLinkBatchEntry,
    PostLinkCacheRequest, SHARED_INCREMENTAL_TARGET_MAX_BYTES, artifact_cache_prune_policy,
    build_cached_cargo_wasm, build_cached_cargo_wasm_batch, cache_post_link_wasm,
    cache_post_link_wasm_batch, cargo_wasm_batch_specs, cargo_wasm_spec,
    shared_incremental_target_prune_policy,
};

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("build crate should live below the workspace root")
        .to_path_buf()
}

#[test]
fn encoded_flags_reach_single_and_batch_builds_and_cache_identity() {
    let root = ic_host_fs::testing::create_fixture_directory(
        &env::temp_dir(),
        "icydb encoded-flags space",
    )
    .unwrap();
    fs::create_dir(root.join("src")).unwrap();
    fs::create_dir(root.join(".cargo")).unwrap();
    let config = root.join(".cargo/config.toml");
    // An exact-limit, valid comment qualifies the selected budget through
    // real single/batch acquisition, not only construction of the spec.
    fs::write(&config, "#".repeat(CARGO_CONFIGURATION_LIMITS.file_bytes)).unwrap();
    fs::write(
        root.join("Cargo.toml"),
        "[package]\nname = \"cache_flags_probe\"\nversion = \"0.0.0\"\nedition = \"2024\"\n[lib]\ncrate-type = [\"cdylib\"]\n[workspace]\n[lints.rust]\nunexpected_cfgs = { level = \"warn\", check-cfg = [\"cfg(caller_cache)\"] }\n",
    ).unwrap();
    let source = root.join("src/absolute.rs");
    fs::write(
        &source,
        "const MARKER: &str = if cfg!(caller_cache) { concat!(\"caller-preserved|\", file!()) } else { concat!(\"caller-missing|\", file!()) };\n",
    ).unwrap();
    fs::write(
        root.join("src/lib.rs"),
        format!("include!({source:?});\n#[unsafe(no_mangle)]\npub extern \"C\" fn marker_ptr() -> *const u8 {{ MARKER.as_ptr() }}\n#[unsafe(no_mangle)]\npub extern \"C\" fn marker_len() -> usize {{ MARKER.len() }}\n"),
    ).unwrap();
    let lock = Command::new("cargo")
        .current_dir(&root)
        .args(["generate-lockfile", "--offline"])
        .output()
        .unwrap();
    assert!(
        lock.status.success(),
        "{}",
        String::from_utf8_lossy(&lock.stderr)
    );
    let target = root.join("target");
    let flags = format!(
        "--cfg\x1fcaller_cache\x1f--remap-path-prefix={}=/first remap",
        root.display()
    );
    let changed_flags = format!(
        "--cfg\x1fcaller_cache\x1f--remap-path-prefix={}=/second remap",
        root.display()
    );
    let single = build_cached_cargo_wasm(&CargoWasmCacheRequest {
        context: "flags-single",
        workspace_root: &root,
        target_dir: &target,
        packages: &["cache_flags_probe"],
        selection: &CargoSelection::default(),
        encoded_rustflags: Some(&flags),
    })
    .unwrap();
    assert!(!single.is_reused());
    assert!(
        !single
            .record()
            .facts()
            .environment
            .contains_key(std::ffi::OsStr::new("CARGO_TARGET_DIR"))
    );
    let specs = cargo_wasm_batch_specs(
        &root,
        &target,
        &[
            CargoWasmBatchEntry {
                context: "flags-same",
                package: "cache_flags_probe",
                selection: &CargoSelection::default(),
                encoded_rustflags: Some(&flags),
            },
            CargoWasmBatchEntry {
                context: "flags-changed",
                package: "cache_flags_probe",
                selection: &CargoSelection::default(),
                encoded_rustflags: Some(&changed_flags),
            },
        ],
    );
    let batch = build_cached_cargo_wasm_batch(&specs).unwrap();
    assert_eq!(batch.batch.failures().count(), 0, "{batch:?}");
    let same = batch.batch.entries()[0].result().unwrap().record();
    assert!(batch.batch.entries()[0].result().unwrap().is_reused());
    let changed = batch.batch.entries()[1].result().unwrap().record();
    assert_eq!(single.record().fingerprint(), same.fingerprint());
    assert_ne!(same.fingerprint(), changed.fingerprint());
    for (record, prefix) in [
        (single.record(), "/first remap"),
        (same, "/first remap"),
        (changed, "/second remap"),
    ] {
        let facts = record.facts();
        let expected_flags = if prefix == "/first remap" {
            &flags
        } else {
            &changed_flags
        };
        assert_eq!(
            facts
                .environment
                .get(std::ffi::OsStr::new("CARGO_ENCODED_RUSTFLAGS")),
            Some(&std::ffi::OsString::from(expected_flags)),
        );
        let wasm = fs::read(&record.artifacts()[0]).unwrap();
        let expected = format!("caller-preserved|{prefix}/src/absolute.rs");
        assert!(
            wasm.windows(expected.len())
                .any(|bytes| bytes == expected.as_bytes())
        );
    }
    drop(single);
    drop(batch);
    fs::write(
        &config,
        "#".repeat(CARGO_CONFIGURATION_LIMITS.file_bytes + 1),
    )
    .unwrap();
    let single_spec = cargo_wasm_spec(
        &root,
        &target,
        &["cache_flags_probe"],
        &CargoSelection::default(),
        Some(&flags),
    );
    for spec in [&single_spec, specs[0].spec()] {
        let error = ic_build::artifacts::resolve_cargo_build_inputs(spec).unwrap_err();
        let ic_build::artifacts::WasmBuildError::InvalidCargoConfiguration { path, .. } = error
        else {
            panic!("configuration admission must retain its typed error: {error}");
        };
        assert_eq!(
            fs::canonicalize(path).unwrap(),
            fs::canonicalize(&config).unwrap()
        );
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cargo_cache_retention_bounds_exact_and_incremental_state() {
    let artifact_policy = artifact_cache_prune_policy();
    assert_eq!(artifact_policy.max_age(), Some(CACHE_MAX_AGE));
    assert_eq!(artifact_policy.max_size_bytes(), Some(CACHE_MAX_BYTES));

    let shared_policy = shared_incremental_target_prune_policy();
    assert_eq!(shared_policy.max_age(), Some(CACHE_MAX_AGE));
    assert_eq!(
        shared_policy.max_size_bytes(),
        Some(SHARED_INCREMENTAL_TARGET_MAX_BYTES)
    );
}

#[test]
fn cargo_cache_maintenance_accepts_missing_target() {
    let root = ic_host_fs::testing::create_fixture_directory(
        &env::temp_dir(),
        "icydb missing cache target",
    )
    .unwrap();
    let target = root.join("missing-target");
    let spec = cargo_wasm_spec(&root, &target, &["probe"], &CargoSelection::default(), None);
    assert!(
        ic_build::artifacts::maintain_shared_incremental_target(
            &spec,
            shared_incremental_target_prune_policy(),
        )
        .unwrap()
        .is_none()
    );
    assert!(!target.exists());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn single_post_link_reuses_batch_identity_and_preserves_output_on_failure() {
    let root =
        ic_host_fs::testing::create_fixture_directory(&env::temp_dir(), "icydb single post-link")
            .unwrap();
    let input = root.join("input.wasm");
    let output = root.join("output.wasm");
    let cache = root.join("cache");
    let workspace = workspace_root();
    fs::write(&input, b"\0asm\x01\0\0\0").unwrap();
    let entries = [PostLinkBatchEntry {
        context: "same post-link recipe",
        compiler_emitted: &input,
        final_deployable: &output,
    }];
    let batch = cache_post_link_wasm_batch(&workspace, &cache, &entries).unwrap();
    let request = PostLinkCacheRequest {
        workspace_root: &workspace,
        cache_root: &cache,
        coordination_scope: entries[0].context,
        compiler_emitted: &input,
        final_deployable: &output,
    };
    let single = cache_post_link_wasm(&request).unwrap();
    assert!(single.is_reused());
    assert_eq!(
        single.record().key(),
        batch
            .batch
            .outcomes()
            .next()
            .unwrap()
            .outcome()
            .unwrap()
            .record()
            .key()
    );
    fs::write(&input, b"invalid wasm").unwrap();
    assert!(cache_post_link_wasm(&request).is_err());
    assert_eq!(fs::read(&output).unwrap(), b"\0asm\x01\0\0\0");
    drop(batch);
    fs::remove_file(&output).unwrap();
    assert_eq!(
        fs::read(single.record().artifacts()[0].path()).unwrap(),
        b"\0asm\x01\0\0\0"
    );
    drop(single);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn post_link_batch_reports_failure_and_finishes_later_artifact() {
    let root =
        ic_host_fs::testing::create_fixture_directory(&env::temp_dir(), "icydb post-link-batch")
            .unwrap();
    let invalid_input = root.join("invalid.wasm");
    let valid_input = root.join("valid.wasm");
    let invalid_output = root.join("invalid.final.wasm");
    let valid_output = root.join("valid.final.wasm");
    fs::write(&invalid_input, b"not wasm").expect("write invalid Wasm fixture");
    fs::write(&valid_input, b"\0asm\x01\0\0\0").expect("write valid Wasm fixture");
    let entries = [
        PostLinkBatchEntry {
            context: "invalid post-link fixture",
            compiler_emitted: &invalid_input,
            final_deployable: &invalid_output,
        },
        PostLinkBatchEntry {
            context: "valid post-link fixture",
            compiler_emitted: &valid_input,
            final_deployable: &valid_output,
        },
    ];
    let report =
        cache_post_link_wasm_batch(&workspace_root(), &root.join("artifact-cache"), &entries)
            .expect("post-link batch setup should succeed");

    assert_eq!(report.batch.outcomes().count(), 1);
    assert_eq!(report.batch.outcomes().next().unwrap().index(), 1);
    assert_eq!(report.batch.failures().count(), 1);
    assert!(!invalid_output.exists());
    assert!(valid_output.is_file());
    // Consume the upstream report just like the coordinator: the moved
    // record must retain the exact artifact after other entries are dropped.
    let mut entries = report.batch.into_entries();
    let (_, _, outcome, _) = entries.pop().unwrap().into_parts();
    let record = match outcome.unwrap() {
        ArtifactCacheOutcome::Built(record) | ArtifactCacheOutcome::Reused(record) => record,
    };
    drop(entries);
    let retained = record.artifacts()[0].path();
    assert_ne!(retained, valid_output);
    fs::remove_file(&valid_output).expect("remove mutable materialization");
    assert_eq!(
        fs::read(retained).expect("read retained success"),
        b"\0asm\x01\0\0\0"
    );
    drop(record);
    fs::remove_dir_all(root).expect("remove post-link batch fixture");
}
