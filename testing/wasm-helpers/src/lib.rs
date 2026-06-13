//! Shared helpers for wasm fixture schema and canister builds.

///
/// build_configured_canister
///
/// Emit generated actor glue for one fixture canister using local
/// `icydb.toml` switches.
///
#[macro_export]
macro_rules! build_configured_canister {
    ($canister_ty:ty, $canister_path:literal, $canister_name:literal) => {{
        let _ = std::any::TypeId::of::<$canister_ty>();

        let config = icydb_config::emit_config_for_build_script()?;
        let options = icydb::build::BuildOptions::default()
            .with_sql_readonly_enabled(config.canister_sql_readonly_enabled($canister_name))
            .with_sql_ddl_enabled(config.canister_sql_ddl_enabled($canister_name))
            .with_sql_fixtures_enabled(config.canister_sql_fixtures_enabled($canister_name))
            .with_sql_update_policy(match config.canister_sql_update_policy($canister_name) {
                Some(icydb_config::GeneratedSqlUpdatePolicy::PublicPrimaryKeyOnly) => {
                    Some(icydb::build::BuildSqlUpdatePolicy::PublicPrimaryKeyOnly)
                }
                Some(icydb_config::GeneratedSqlUpdatePolicy::PublicBoundedDeterministic) => {
                    Some(icydb::build::BuildSqlUpdatePolicy::PublicBoundedDeterministic)
                }
                None => None,
            })
            .with_metrics_enabled(config.canister_metrics_enabled($canister_name))
            .with_metrics_extended_enabled(config.canister_metrics_extended_enabled($canister_name))
            .with_snapshot_enabled(config.canister_snapshot_enabled($canister_name))
            .with_schema_enabled(config.canister_schema_enabled($canister_name));
        icydb::build_with_options!($canister_path, options);
    }};
}

///
/// define_fixture_canister
///
/// Generate the repeated canister declaration used by wasm fixture schema
/// crates.
///
/// `memory_min`, `memory_max`, and `commit_memory_id` are canister-level
/// stable-memory manager configuration. Per-store memory IDs live in
/// `define_fixture_store!(storage(...))`.
///
#[macro_export]
macro_rules! define_fixture_canister {
    (
        $canister:ident = $canister_name:literal,
        namespace = $namespace:literal,
        memory_min = $memory_min:literal,
        memory_max = $memory_max:literal,
        commit_memory_id = $commit_memory_id:literal $(,)?
    ) => {
        #[doc = ""]
        #[doc = stringify!($canister)]
        #[doc = ""]
        #[doc = "Canister model used by wasm SQL fixtures."]
        #[doc = ""]
        #[canister(memory_namespace = $namespace, memory_min = $memory_min, memory_max = $memory_max, commit_memory_id = $commit_memory_id)]
        pub struct $canister {}
    };
}

///
/// define_fixture_store
///
/// Generate the repeated store declaration used by wasm fixture schema crates.
///
#[macro_export]
macro_rules! define_fixture_store {
    (
        $store:ident = $store_ident:literal,
        canister = $canister_name:literal,
        storage(stable(
            data_memory_id = $data_memory_id:literal,
            index_memory_id = $index_memory_id:literal,
            schema_memory_id = $schema_memory_id:literal,
        )) $(,)?
    ) => {
        #[doc = ""]
        #[doc = stringify!($store)]
        #[doc = ""]
        #[doc = "Main store model used by wasm SQL fixtures."]
        #[doc = ""]
        #[store(ident = $store_ident, store_name = "main", canister = $canister_name, storage(stable(data_memory_id = $data_memory_id, index_memory_id = $index_memory_id, schema_memory_id = $schema_memory_id)))]
        pub struct $store {}
    };
    (
        $store:ident = $store_ident:literal,
        canister = $canister_name:literal,
        storage(journaled(
            data_memory_id = $data_memory_id:literal,
            index_memory_id = $index_memory_id:literal,
            schema_memory_id = $schema_memory_id:literal,
            journal_memory_id = $journal_memory_id:literal,
        )) $(,)?
    ) => {
        #[doc = ""]
        #[doc = stringify!($store)]
        #[doc = ""]
        #[doc = "Main store model used by wasm SQL fixtures."]
        #[doc = ""]
        #[store(ident = $store_ident, store_name = "main", canister = $canister_name, storage(journaled(data_memory_id = $data_memory_id, index_memory_id = $index_memory_id, schema_memory_id = $schema_memory_id, journal_memory_id = $journal_memory_id)))]
        pub struct $store {}
    };
}

///
/// define_simple_audit_entities
///
/// Generate one or more repeated simple audit entities for wasm-size fixtures.
///
#[macro_export]
macro_rules! define_simple_audit_entities {
    ($store:literal; $($entity:ident),+ $(,)?) => {
        $(
            #[doc = ""]
            #[doc = stringify!($entity)]
            #[doc = ""]
            #[doc = "Repeated simple audit entity used to measure base per-entity wasm cost."]
            #[doc = ""]
            #[entity(
                store = $store,
                version = 1,
                pk(fields = ["id"]),
                fields(
                    field(ident = "id", value(item(prim = "Ulid")), generated(insert = "Ulid::generate")),
                    field(ident = "name", value(item(prim = "Text", unbounded)))
                )
            )]
            pub struct $entity {}
        )+
    };
}
