//! Module: ICP command and manifest tests.
//! Responsibility: exercise local `icp` command construction and fallback manifest parsing.
//! Does not own: CLI argument parsing or observability payload decoding.
//! Boundary: test-only assertions over process command arguments and parser helpers.

use icydb::__macro::RpcMethod;

use std::path::Path;

use crate::icp::support_tests::{
    build_command, canister_status_id_command, canister_status_json_command, deploy_command,
    fixtures_load_command, hex_response_bytes, icp_query_command, icp_update_command,
    install_upgrade_command, parse_manifest_canisters, status_command, top_up_command,
};

#[test]
fn manifest_canister_fallback_matches_environment_names_exactly() {
    let manifest = r"
environments:
  - name: demo-extra
    canisters: [wrong]
  - name: demo
    canisters: [demo_rpg, minimal]
";

    assert_eq!(
        parse_manifest_canisters(manifest, "demo").unwrap(),
        vec!["demo_rpg".to_string(), "minimal".to_string()],
    );
}

#[test]
fn manifest_canister_fallback_ignores_top_level_canister_names() {
    let manifest = r"
canisters:
  - name: demo
    type: rust
environments:
  - name: demo
    canisters: [demo_rpg]
";

    assert_eq!(
        parse_manifest_canisters(manifest, "demo").unwrap(),
        vec!["demo_rpg".to_string()],
    );
}

#[test]
fn manifest_canister_fallback_accepts_quoted_inline_names() {
    let manifest = r#"
environments:
  - name: test
    canisters: ["ten_complex", 'one_simple']
"#;

    assert_eq!(
        parse_manifest_canisters(manifest, "test").unwrap(),
        vec!["one_simple".to_string(), "ten_complex".to_string()],
    );
}

#[test]
fn manifest_canister_fallback_accepts_quoted_environment_names() {
    let manifest = r#"
environments:
  - name: "test"
    canisters: [test_sql]
  - name: 'demo'
    canisters: [demo_rpg]
"#;

    assert_eq!(
        parse_manifest_canisters(manifest, "demo").unwrap(),
        vec!["demo_rpg".to_string()],
    );
    assert_eq!(
        parse_manifest_canisters(manifest, "test").unwrap(),
        vec!["test_sql".to_string()],
    );
}

#[test]
fn icp_query_command_targets_environment_and_hex_query_output() {
    let command = icp_query_command(
        "demo",
        "demo_rpg",
        RpcMethod::Query.name(),
        Path::new("arguments.bin"),
    );
    let args = command
        .get_args()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect::<Vec<_>>();

    assert_eq!(command.get_program().to_string_lossy(), "icp");
    assert_eq!(
        args,
        vec![
            "canister",
            "call",
            "demo_rpg",
            "icydb_query",
            "--args-file",
            "arguments.bin",
            "--args-format",
            "bin",
            "--query",
            "--output",
            "hex",
            "--environment",
            "demo",
        ],
    );
}

#[test]
fn icp_update_command_targets_environment_without_query_flag() {
    let command = icp_update_command(
        "demo",
        "demo_rpg",
        RpcMethod::Ddl.name(),
        Path::new("arguments.bin"),
    );
    let args = command
        .get_args()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect::<Vec<_>>();

    assert_eq!(command.get_program().to_string_lossy(), "icp");
    assert_eq!(
        args,
        vec![
            "canister",
            "call",
            "demo_rpg",
            "icydb_ddl",
            "--args-file",
            "arguments.bin",
            "--args-format",
            "bin",
            "--output",
            "hex",
            "--environment",
            "demo",
        ],
    );
}

#[test]
fn fixtures_load_command_targets_fixed_generated_endpoint() {
    let command = fixtures_load_command("demo", "demo_rpg", std::path::Path::new("arguments.bin"));
    let args = command
        .get_args()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect::<Vec<_>>();

    assert_eq!(command.get_program().to_string_lossy(), "icp");
    assert_eq!(
        args,
        vec![
            "canister",
            "call",
            "demo_rpg",
            "icydb_fixtures_load",
            "--args-file",
            "arguments.bin",
            "--args-format",
            "bin",
            "--output",
            "hex",
            "--environment",
            "demo",
        ],
    );
}

#[test]
fn install_upgrade_command_preserves_stable_memory() {
    let command = install_upgrade_command(
        "demo",
        "demo_rpg",
        Some(Path::new("selected artifact.wasm")),
    );
    let args = command
        .get_args()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect::<Vec<_>>();

    assert_eq!(command.get_program().to_string_lossy(), "icp");
    assert_eq!(
        args,
        vec![
            "canister",
            "install",
            "demo_rpg",
            "--mode",
            "upgrade",
            "--wasm",
            "selected artifact.wasm",
            "--environment",
            "demo",
        ],
    );
}

#[test]
fn lifecycle_commands_target_selected_environment() {
    for (command, expected) in [
        (
            deploy_command("demo", "demo_rpg"),
            vec!["deploy", "demo_rpg", "--environment", "demo"],
        ),
        (
            build_command("demo", "demo_rpg"),
            vec!["build", "demo_rpg", "--environment", "demo"],
        ),
        (
            status_command("demo", "demo_rpg"),
            vec!["canister", "status", "demo_rpg", "--environment", "demo"],
        ),
        (
            top_up_command("demo", "demo_rpg", "1t"),
            vec![
                "canister",
                "top-up",
                "--amount",
                "1t",
                "demo_rpg",
                "--environment",
                "demo",
            ],
        ),
    ] {
        let args = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();

        assert_eq!(command.get_program().to_string_lossy(), "icp");
        assert_eq!(args, expected);
    }
}

#[test]
fn canister_status_probe_commands_target_selected_environment() {
    for (command, expected) in [
        (
            canister_status_json_command("demo", "demo_rpg"),
            vec![
                "canister",
                "status",
                "demo_rpg",
                "--json",
                "--environment",
                "demo",
            ],
        ),
        (
            canister_status_id_command("demo", "demo_rpg"),
            vec![
                "canister",
                "status",
                "demo_rpg",
                "--id-only",
                "--environment",
                "demo",
            ],
        ),
    ] {
        let args = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();

        assert_eq!(command.get_program().to_string_lossy(), "icp");
        assert_eq!(args, expected);
    }
}

#[test]
fn hex_response_bytes_accepts_plain_or_labeled_icp_hex_output() {
    assert_eq!(
        hex_response_bytes("4449444c00017f").expect("plain hex should parse"),
        vec![0x44, 0x49, 0x44, 0x4c, 0x00, 0x01, 0x7f],
    );
    assert_eq!(
        hex_response_bytes("response (hex): 44 49 44 4c").expect("labeled hex should parse"),
        vec![0x44, 0x49, 0x44, 0x4c],
    );
    assert_eq!(
        hex_response_bytes("call completed\nresponse (hex): 4A\u{2003}ff\n00")
            .expect("CLI presentation should normalize before shared decoding"),
        vec![0x4a, 0xff, 0x00],
    );
}

#[test]
fn hex_response_bytes_rejects_malformed_icp_hex_output() {
    for output in ["", "response (hex):", "123", "12 zz"] {
        assert!(
            hex_response_bytes(output).is_err(),
            "malformed hex output should fail: {output:?}",
        );
    }
}

#[test]
fn hex_response_diagnostics_are_control_free() {
    for byte in (0_u8..=31).chain([127]) {
        let character = char::from(byte);
        if character.is_whitespace() {
            continue;
        }
        // Keep an even-length body so the invalid byte reaches the shared
        // decoder rather than the earlier odd-length refusal.
        let response = format!("00{character}x");
        let error = hex_response_bytes(&response).unwrap_err();
        assert!(!error.chars().any(char::is_control), "byte {byte}");
    }
}

#[test]
fn manifest_missing_or_empty_canisters_preserve_environment_boundaries() {
    let manifest = r"
canisters: [outside]
environments:
  - name: missing-keys
  - name: empty-keys
    canisters: []
  - name: populated
    canisters: [inside]
";
    for environment in ["missing-keys", "empty-keys", "absent"] {
        assert!(
            parse_manifest_canisters(manifest, environment)
                .unwrap()
                .is_empty()
        );
    }
    assert_eq!(
        parse_manifest_canisters(manifest, "populated").unwrap(),
        vec!["inside"]
    );
}

#[test]
fn manifest_yaml_projection_preserves_literal_names_and_ignores_unrelated_tags() {
    let manifest = r#"
canisters: [{name: outside, build: !script [false]}]
environments:
  - name: "demo\\name"
    canisters: ['first,second', 'literal''quote', "line\nname", '', z, z]
  - name: other
    canisters: [outside]
"#;
    assert_eq!(
        parse_manifest_canisters(manifest, "demo\\name").unwrap(),
        ["first,second", "line\nname", "literal'quote", "z", "z"]
    );
}

#[test]
fn manifest_yaml_projection_refuses_malformed_documents_and_wrong_shapes() {
    for manifest in [
        "[",
        "[]",
        "not a project",
        "environments: fixture",
        "environments: [{name: fixture, canisters: 42}]",
        "environments: [{name: fixture, canisters: [{name: one}]}]",
        "environments: [{name: fixture, canisters: [], canisters: [later]}]",
        "environments: []\n---\nenvironments: []",
    ] {
        assert!(parse_manifest_canisters(manifest, "fixture").is_err());
    }
}

#[test]
fn manifest_yaml_projection_preserves_empty_and_first_environment_selections() {
    for manifest in [
        "",
        "{}",
        "environments: null",
        "environments: []",
        "environments: [{name: fixture}]",
        "environments: [{name: fixture, canisters: null}]",
        "environments: [{name: fixture, canisters: []}, {name: fixture, canisters: [later]}]",
    ] {
        assert!(
            parse_manifest_canisters(manifest, "fixture")
                .unwrap()
                .is_empty()
        );
    }
}
