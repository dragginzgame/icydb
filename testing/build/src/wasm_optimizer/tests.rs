use super::{
    POST_LINK_PIPELINE_IDENTITY, WASM_OPT_FLAGS, WASM_OPT_OUTPUT_FEATURES, WASM_OPT_VERSION,
    optimize_deployable_wasm_with_optimizer, pinned_wasm_optimizer, wasm_opt_sha256,
};

#[test]
fn post_link_optimizer_contract_is_exact_and_available() {
    assert_eq!(WASM_OPT_VERSION, "wasm-opt version 133 (version_133)");
    assert_eq!(wasm_opt_sha256().unwrap().len(), 64);
    assert_eq!(
        WASM_OPT_FLAGS,
        [
            "-Oz",
            "--enable-bulk-memory",
            "--enable-sign-ext",
            "--enable-nontrapping-float-to-int",
            "--one-caller-inline-max-function-size=0",
        ]
    );
    assert_eq!(
        WASM_OPT_OUTPUT_FEATURES,
        [
            "--enable-bulk-memory",
            "--enable-bulk-memory-opt",
            "--enable-mutable-globals",
            "--enable-nontrapping-float-to-int",
            "--enable-sign-ext",
        ]
    );
    assert_eq!(
        POST_LINK_PIPELINE_IDENTITY,
        "binaryen-133-oz+bulk-memory+sign-ext+nontrapping-float-to-int+one-caller-inline-max-0/v1"
    );
    let optimizer = pinned_wasm_optimizer().expect("pinned optimizer should admit");
    assert_eq!(
        optimizer.identity().sha256.to_string(),
        wasm_opt_sha256().unwrap()
    );
    assert_eq!(optimizer.version_identity(), WASM_OPT_VERSION);
}

#[test]
fn admitted_optimizer_preserves_outputs_on_failure_and_handles_spaced_paths() {
    let root = ic_host_fs::testing::create_fixture_directory(
        &std::env::temp_dir(),
        "icydb optimizer host",
    )
    .unwrap();
    let input = root.join("compiler input.wasm");
    let output = root.join("final output.wasm");
    let optimizer = pinned_wasm_optimizer().unwrap();
    let wasm = b"\0asm\x01\0\0\0";
    std::fs::write(&input, wasm).unwrap();
    optimize_deployable_wasm_with_optimizer(&input, &output, &optimizer).unwrap();
    assert_eq!(std::fs::read(&output).unwrap(), wasm);
    std::fs::write(&input, b"invalid wasm").unwrap();
    assert!(optimize_deployable_wasm_with_optimizer(&input, &output, &optimizer).is_err());
    assert_eq!(std::fs::read(&output).unwrap(), wasm);
    assert_eq!(std::fs::read(&input).unwrap(), b"invalid wasm");
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 2);
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(unix)]
#[test]
fn producer_requires_successful_exit_and_wasm_header_before_publication() {
    use ic_host_process::tool::{AdmittedTool, ExecutionContext, VersionSpec};
    use std::{ffi::OsString, os::unix::fs::PermissionsExt};

    let root = ic_host_fs::testing::create_fixture_directory(
        &std::env::temp_dir(),
        "icydb empty optimizer",
    )
    .unwrap();
    let input = root.join("input.wasm");
    let output = root.join("output.wasm");
    let previous = b"\0asm\x01\0\0\0";
    std::fs::write(&input, previous).unwrap();
    std::fs::write(&output, previous).unwrap();
    for (case, producer) in [
        ":",
        "printf 'short' > \"$output\"",
        "printf 'not-wasm' > \"$output\"",
        "printf '\\000asm\\001\\000\\000\\000' > \"$output\"; printf 'producer failure' >&2; exit 7",
    ]
    .into_iter()
    .enumerate()
    {
        // Keep each executed inode immutable across producer cases.
        let executable = root.join(format!("producer-{case}"));
        std::fs::write(&executable, format!(
            "#!/bin/sh\nif [ \"$1\" = --version ]; then echo fixture; exit 0; fi\nwhile [ \"$1\" != -o ]; do shift; done\noutput=$2\n[ -f \"$output\" ] && [ ! -s \"$output\" ] || exit 9\n{producer}\n"
        )).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let executable = std::fs::canonicalize(executable).unwrap();
        let current_dir = std::env::current_dir().unwrap();
        let optimizer = AdmittedTool::admit_version(
            &VersionSpec {
                executable: &executable,
                executable_bytes: u64::MAX,
                version_arguments: &[OsString::from("--version")],
            },
            "fixture",
            &ExecutionContext {
                current_dir: &current_dir,
                environment: &[],
            },
            super::OPTIMIZER_OUTPUT_LIMITS,
        )
        .unwrap();
        assert!(optimize_deployable_wasm_with_optimizer(&input, &output, &optimizer).is_err());
        assert_eq!(std::fs::read(&output).unwrap(), previous);
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 3);
        std::fs::remove_file(&executable).unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}
