//! Scenario-owned feature recipes for retained fixture actor variants.
//!
//! Build support owns Cargo, post-link and artifact retention; these recipes
//! own actor roles and their returned source/successor ordering.

use icydb_testing_build::{
    CanisterBuildOptions, CanisterWasmProfile, build_fixture_variant_wasm_with_profile,
};

/// Build the full and omitted-store logical-memory test actors, in that order.
///
/// Both use the ordinary retained Cargo/post-link pipeline. Read each result
/// while its owner is alive, before another variant can replace shared outputs.
/// These actors expose test-only controls and must not host application data.
///
/// # Errors
/// Returns a build, post-link or retained-artifact read failure.
pub fn build_logical_memory_fixture_wasms() -> Result<(Vec<u8>, Vec<u8>), String> {
    Ok((
        build_fixture_variant_wasm("canister_test_logical_memory", "", "logical-memory-full")?,
        build_fixture_variant_wasm(
            "canister_test_logical_memory",
            "omit-retiring-store",
            "logical-memory-omitted",
        )?,
    ))
}

/// Build the maintained source and successor schema-migration actors.
///
/// Both source shapes use the current library and storage format. Artifact
/// retention lasts through each read, before the next variant is built.
///
/// # Errors
/// Returns a build, post-link or retained-artifact read failure.
pub fn build_schema_migration_fixture_wasms() -> Result<(Vec<u8>, Vec<u8>), String> {
    Ok((
        build_fixture_variant_wasm(
            "canister_test_sql",
            "test-admin-api,local-sql-query,schema-migration-api",
            "schema-migration-source",
        )?,
        build_fixture_variant_wasm(
            "canister_test_sql",
            "test-admin-api,local-sql-query,schema-migration-v2",
            "schema-migration-successor",
        )?,
    ))
}

/// Build the populated entity-rename source and successor using the SQL actor.
/// Both actors use current formats and the same durable namespace/store keys.
///
/// # Errors
/// Returns a build, post-link or retained-artifact read failure.
pub fn build_entity_rename_fixture_wasms() -> Result<(Vec<u8>, Vec<u8>), String> {
    Ok((
        build_fixture_variant_wasm(
            "canister_test_sql",
            "test-admin-api,local-sql-query,entity-rename",
            "entity-rename-source",
        )?,
        build_fixture_variant_wasm(
            "canister_test_sql",
            "test-admin-api,local-sql-query,entity-rename-successor",
            "entity-rename-successor",
        )?,
    ))
}

/// Build populated source, additive, metadata and physical creation actors.
/// Every actor uses current formats and the same durable namespace/store keys.
///
/// # Errors
/// Returns a build, post-link or retained-artifact read failure.
pub fn build_entity_creation_fixture_wasms() -> Result<[Vec<u8>; 4], String> {
    Ok([
        build_fixture_variant_wasm(
            "canister_test_sql",
            "test-admin-api,local-sql-query,entity-rename",
            "entity-creation-source",
        )?,
        build_fixture_variant_wasm(
            "canister_test_sql",
            "test-admin-api,local-sql-query,entity-creation",
            "entity-creation-additive",
        )?,
        build_fixture_variant_wasm(
            "canister_test_sql",
            "test-admin-api,local-sql-query,entity-creation,entity-rename-successor",
            "entity-creation-metadata",
        )?,
        build_fixture_variant_wasm(
            "canister_test_sql",
            "test-admin-api,local-sql-query,entity-creation-physical",
            "entity-creation-physical",
        )?,
    ])
}

/// Build matched source, unchanged-schema upgrade control and additive actors.
///
/// The control repeats the source contract without a schema change; all actors
/// retain the same storage namespace and use the selected canonical profile.
///
/// # Errors
/// Returns a build, post-link or retained-artifact read failure.
pub fn build_entity_creation_lifecycle_fixture_wasms(
    profile: CanisterWasmProfile,
) -> Result<[Vec<u8>; 3], String> {
    let build = |features, label| {
        build_fixture_variant_wasm_with_profile("canister_test_sql", features, label, profile)
    };
    Ok([
        build(
            "test-admin-api,local-sql-query,entity-rename",
            "creation-lifecycle-source",
        )?,
        build(
            "test-admin-api,local-sql-query,entity-rename",
            "creation-lifecycle-control",
        )?,
        build(
            "test-admin-api,local-sql-query,entity-creation",
            "creation-lifecycle-additive",
        )?,
    ])
}

fn build_fixture_variant_wasm(
    package: &str,
    features: &str,
    label: &str,
) -> Result<Vec<u8>, String> {
    build_fixture_variant_wasm_with_profile(
        package,
        features,
        label,
        CanisterBuildOptions::default().profile,
    )
}
