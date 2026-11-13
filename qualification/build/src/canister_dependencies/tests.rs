use super::admit;
use crate::canister_build_cache::{CargoWasmCacheRequest, build_cached_cargo_wasm};
use ic_build::cargo::selection::CargoSelection;
use std::{env, fs, path::Path, process::Command};

fn package(root: &Path, name: &str, dependencies: &str) {
    fs::create_dir_all(root.join("src")).unwrap();
    fs::write(root.join("src/lib.rs"), "pub fn fixture() {}\n").unwrap();
    fs::write(
        root.join("Cargo.toml"),
        format!("[package]\nname = {name:?}\nversion = '0.0.0'\nedition = '2024'\n{dependencies}"),
    )
    .unwrap();
}

// Exercise real Cargo's alias, target and feature graph selection. These tiny
// local packages model identities only, never compatibility with real Canic.
#[test]
fn retained_artifact_admission_uses_exact_target_features_and_source() {
    let root = ic_host_fs::testing::create_fixture_directory(
        &env::temp_dir(),
        "icydb dependency admission",
    )
    .unwrap();
    package(&root.join("excluded"), "ic-testkit", "");
    package(&root.join("native"), "proptest", "");
    package(
        &root.join("allowed"),
        "canic-icydb",
        "[dependencies]\ntooling = { package = 'ic-testkit', path = '../excluded', optional = true }\n[features]\nexcluded = ['dep:tooling']\n",
    );
    package(
        &root,
        "dependency_probe",
        "[workspace]\nmembers = ['allowed', 'excluded', 'native']\n[lib]\ncrate-type = ['cdylib']\n[dependencies]\ncanic = { package = 'canic-icydb', path = 'allowed' }\nrenamed = { package = 'ic-testkit', path = 'excluded', optional = true }\n[features]\ndirect = ['dep:renamed']\ntransitive = ['canic/excluded']\n[dev-dependencies]\nproptest = { path = 'native' }\n[target.'cfg(target_os = \"linux\")'.dependencies]\nproptest = { path = 'native' }\n",
    );
    assert!(
        Command::new("cargo")
            .args(["generate-lockfile", "--offline"])
            .current_dir(&root)
            .status()
            .unwrap()
            .success()
    );
    let repository = crate::workspace_root();
    let target = root.join("target");
    let packages = ["dependency_probe"];
    let mut selection = CargoSelection {
        locked: true,
        offline: true,
        ..Default::default()
    };
    selection.features.default_features = false;
    for features in [vec![], vec!["direct"], vec!["transitive"]] {
        selection.features.features = features.iter().map(|s| (*s).to_owned()).collect();
        let request = CargoWasmCacheRequest {
            context: "dependency admission fixture",
            workspace_root: &root,
            target_dir: &target,
            packages: &packages,
            selection: &selection,
            encoded_rustflags: None,
        };
        let outcome = build_cached_cargo_wasm(&request).unwrap();
        let record = outcome.record();
        let [wasm] = record.artifacts() else {
            panic!("one fixture artifact expected");
        };
        let admission = admit(&repository, &request, record, wasm);
        if features.is_empty() {
            let proof = admission.unwrap();
            assert_eq!(proof["target"], "wasm32-unknown-unknown");
            assert_eq!(proof["features"], serde_json::json!([]));
            assert_eq!(proof["compiler_sha256"], proof["deployable_sha256"]);
            let reused = build_cached_cargo_wasm(&request).unwrap();
            assert!(matches!(
                reused,
                ic_build::artifacts::WasmBuildOutcome::Reused(_)
            ));
            assert_eq!(
                proof,
                admit(&repository, &request, reused.record(), wasm).unwrap()
            );
            // A different final artifact gets a different binding; no prior
            // admission stamp is looked up or reused for these bytes.
            let changed = target.join("changed.wasm");
            fs::write(&changed, b"\0asm\x01\0\0\0").unwrap();
            let changed_proof = admit(&repository, &request, record, &changed).unwrap();
            assert_ne!(
                proof["deployable_sha256"],
                changed_proof["deployable_sha256"]
            );
            fs::write(root.join("src/lib.rs"), "pub fn changed_source() {}\n").unwrap();
            assert!(admit(&repository, &request, record, wasm).is_err());
            fs::write(root.join("src/lib.rs"), "pub fn fixture() {}\n").unwrap();
            let mut changed_selection = selection.clone();
            changed_selection.profile = Some("release".to_owned());
            let changed_request = CargoWasmCacheRequest {
                selection: &changed_selection,
                ..request
            };
            assert!(admit(&repository, &changed_request, record, wasm).is_err());
            changed_selection.profile = None;
            changed_selection
                .features
                .features
                .insert("direct".to_owned());
            let changed_request = CargoWasmCacheRequest {
                selection: &changed_selection,
                ..request
            };
            assert!(admit(&repository, &changed_request, record, wasm).is_err());
        } else {
            assert!(admission.is_err());
        }
    }
    // A selected build dependency is also excluded, despite executing on the
    // host. This policy deliberately differs from permitted schema/DTO inputs.
    let manifest = root.join("Cargo.toml");
    let mut source = fs::read_to_string(&manifest).unwrap();
    source.push_str(
        "\n[build-dependencies]\nrenamed_build = { package = 'ic-testkit', path = 'excluded' }\n",
    );
    // Cargo does not permit two different aliases of the same dependency in
    // one package's active graph. Remove the optional direct alias for this case.
    source = source.replace(
        "renamed = { package = 'ic-testkit', path = 'excluded', optional = true }\n",
        "",
    );
    source = source.replace("direct = ['dep:renamed']\n", "");
    fs::write(&manifest, source).unwrap();
    selection.features.features.clear();
    let request = CargoWasmCacheRequest {
        context: "build dependency admission fixture",
        workspace_root: &root,
        target_dir: &target,
        packages: &packages,
        selection: &selection,
        encoded_rustflags: None,
    };
    let outcome = build_cached_cargo_wasm(&request).unwrap();
    let record = outcome.record();
    assert!(admit(&repository, &request, record, &record.artifacts()[0]).is_err());
    fs::remove_dir_all(&root).unwrap();
}
