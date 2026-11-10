use super::*;

#[test]
fn fixture_arguments_preserve_defaults_and_explicit_build_selection() {
    let defaults = Args::try_parse_from(["build_fixture_canister"]).unwrap();
    assert_eq!(defaults.canister, "demo_rpg");
    assert_eq!(defaults.build_profile, CanisterBuildProfile::LocalTest);
    assert_eq!(defaults.profile, CanisterWasmProfile::Debug);
    assert_eq!(defaults.sql_mode, CanisterSqlMode::Enabled);
    let explicit = Args::try_parse_from([
        "build_fixture_canister",
        "sql",
        "--build-profile",
        "production",
        "--profile",
        "wasm-attribution",
        "--sql-mode",
        "sql-off",
    ])
    .unwrap();
    assert_eq!(explicit.canister, "sql");
    assert_eq!(explicit.build_profile, CanisterBuildProfile::Production);
    assert_eq!(explicit.profile, CanisterWasmProfile::WasmAttribution);
    assert_eq!(explicit.sql_mode, CanisterSqlMode::Disabled);
    assert!(Args::try_parse_from(["build_fixture_canister", "--profile", "unknown"]).is_err());
}
