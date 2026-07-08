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
        icydb::build::build_configured_canister!($canister_ty, $canister_path, $canister_name);
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
