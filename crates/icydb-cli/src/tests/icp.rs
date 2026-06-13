//! Module: ICP command and manifest tests.
//! Responsibility: exercise local `icp` command construction and fallback manifest parsing.
//! Does not own: CLI argument parsing or observability payload decoding.
//! Boundary: test-only assertions over process command arguments and parser helpers.

use std::path::Path;

use icydb_config::ICYDB_BUILD_TARGET_ENV;

use crate::{
    config::{SQL_DDL_ENDPOINT, SQL_QUERY_ENDPOINT},
    icp::test_support::{
        build_command, canister_status_check_command, canister_status_id_command, deploy_command,
        fixtures_load_command, hex_response_bytes, icp_query_command, icp_update_command,
        install_upgrade_command, parse_canister_cycles, parse_manifest_canisters,
        parse_manifest_environment_network, status_command, top_up_command,
        unreachable_network_hint,
    },
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
        parse_manifest_canisters(manifest, "demo"),
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
        parse_manifest_canisters(manifest, "demo"),
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
        parse_manifest_canisters(manifest, "test"),
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
        parse_manifest_canisters(manifest, "demo"),
        vec!["demo_rpg".to_string()],
    );
    assert_eq!(
        parse_manifest_canisters(manifest, "test"),
        vec!["test_sql".to_string()],
    );
}

#[test]
fn icp_query_command_targets_environment_and_hex_query_output() {
    let command = icp_query_command(
        "demo",
        "demo_rpg",
        SQL_QUERY_ENDPOINT.method(),
        "(\"SELECT 1\")",
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
            "__icydb_query",
            "(\"SELECT 1\")",
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
        SQL_DDL_ENDPOINT.method(),
        "(\"CREATE INDEX name_idx\")",
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
            "__icydb_ddl",
            "(\"CREATE INDEX name_idx\")",
            "--output",
            "hex",
            "--environment",
            "demo",
        ],
    );
}

#[test]
fn fixtures_load_command_targets_fixed_generated_endpoint() {
    let command = fixtures_load_command("demo", "demo_rpg");
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
            "__icydb_fixtures_load",
            "()",
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
        Path::new(".icp/local/canisters/demo_rpg/demo_rpg.wasm").to_path_buf(),
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
            ".icp/local/canisters/demo_rpg/demo_rpg.wasm",
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
fn lifecycle_build_commands_set_generated_build_target() {
    for (command, expected) in [
        (deploy_command("local", "demo_rpg"), "local"),
        (build_command("local", "demo_rpg"), "local"),
        (deploy_command("ic", "demo_rpg"), "ic"),
        (build_command("demo", "demo_rpg"), "ic"),
    ] {
        assert_eq!(
            command_env(&command, ICYDB_BUILD_TARGET_ENV).as_deref(),
            Some(expected)
        );
    }
}

fn command_env(command: &std::process::Command, key: &str) -> Option<String> {
    command
        .get_envs()
        .find(|(name, _)| name.to_string_lossy() == key)
        .and_then(|(_, value)| value.map(|value| value.to_string_lossy().into_owned()))
}

#[test]
fn canister_status_probe_commands_target_selected_environment() {
    for (command, expected) in [
        (
            canister_status_check_command("demo", "demo_rpg"),
            vec!["canister", "status", "demo_rpg", "--environment", "demo"],
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
fn canister_status_cycles_parser_accepts_underscored_cycle_balance() {
    let status = "\
Canister Status Report:
  Memory size: 9_515_742
  Cycles: 1_418_380_664_222
  Reserved cycles: 0
";

    assert_eq!(parse_canister_cycles(status), Some(1_418_380_664_222));
    assert_eq!(parse_canister_cycles("Status: Running"), None);
}

#[test]
fn manifest_environment_network_parser_detects_local_targets() {
    let contents = r"
environments:
  - name: demo
    network: local
    canisters: [demo_rpg]

  - name: ic
    network: ic
    canisters: [demo_rpg]
";

    assert_eq!(
        parse_manifest_environment_network(contents, "demo"),
        Some("local")
    );
    assert_eq!(
        parse_manifest_environment_network(contents, "ic"),
        Some("ic")
    );
    assert_eq!(
        parse_manifest_environment_network(contents, "missing"),
        None
    );
}

#[test]
fn unreachable_network_hint_recognizes_local_icp_connection_failures() {
    for message in [
        "connection refused while calling local replica",
        "failed to connect to local network",
        "PocketIC transport is unavailable",
        "network is not running",
    ] {
        assert!(
            unreachable_network_hint(message).is_some(),
            "local ICP network failure should produce guidance: {message}",
        );
    }

    assert!(
        unreachable_network_hint("canister demo_rpg not found").is_none(),
        "ordinary canister lifecycle errors should not be reported as network reachability",
    );
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
