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

        let config = icydb_config_build::emit_config_for_build_script()?;
        let options = icydb::build::BuildOptions::default()
            .with_sql_readonly_enabled(config.canister_sql_readonly_enabled($canister_name))
            .with_sql_ddl_enabled(config.canister_sql_ddl_enabled($canister_name))
            .with_sql_fixtures_enabled(config.canister_sql_fixtures_enabled($canister_name))
            .with_metrics_enabled(config.canister_metrics_enabled($canister_name))
            .with_metrics_reset_enabled(config.canister_metrics_reset_enabled($canister_name))
            .with_snapshot_enabled(config.canister_snapshot_enabled($canister_name))
            .with_schema_enabled(config.canister_schema_enabled($canister_name));
        icydb::build_with_options!($canister_path, options);
    }};
}

///
/// define_fixture_canister_store
///
/// Generate the repeated canister and main-store declarations used by wasm
/// fixture schema crates.
///
#[macro_export]
macro_rules! define_fixture_canister_store {
    (
        $canister:ident = $canister_name:literal,
        $store:ident = $store_ident:literal,
        namespace = $namespace:literal,
        memory_min = $memory_min:literal,
        memory_max = $memory_max:literal,
        commit_memory_id = $commit_memory_id:literal,
        data_memory_id = $data_memory_id:literal,
        index_memory_id = $index_memory_id:literal,
        schema_memory_id = $schema_memory_id:literal $(,)?
    ) => {
        #[doc = ""]
        #[doc = stringify!($canister)]
        #[doc = ""]
        #[doc = "Canister model used by wasm SQL fixtures."]
        #[doc = ""]
        #[canister(memory_namespace = $namespace, memory_min = $memory_min, memory_max = $memory_max, commit_memory_id = $commit_memory_id)]
        pub struct $canister {}

        #[doc = ""]
        #[doc = stringify!($store)]
        #[doc = ""]
        #[doc = "Main store model used by wasm SQL fixtures."]
        #[doc = ""]
        #[store(ident = $store_ident, store_name = "main", canister = $canister_name, data_memory_id = $data_memory_id, index_memory_id = $index_memory_id, schema_memory_id = $schema_memory_id)]
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

///
/// define_complex_audit_types
///
/// Generate the shared structured helper types used by complex wasm-size audit
/// entities.
///
#[macro_export]
macro_rules! define_complex_audit_types {
    () => {
        #[doc = ""]
        #[doc = "AuditText"]
        #[doc = ""]
        #[doc = "Lower-cased text newtype reused across complex audit entities."]
        #[doc = ""]
        #[newtype(
            primitive = "Text",
            item(prim = "Text", unbounded),
            ty(
                sanitizer(path = "base::sanitizer::text::case::Lower"),
                validator(path = "base::validator::text::case::Lower")
            )
        )]
        pub struct AuditText {}

        #[doc = ""]
        #[doc = "AuditCode"]
        #[doc = ""]
        #[doc = "Upper-snake text newtype reused across complex audit entities."]
        #[doc = ""]
        #[newtype(
            primitive = "Text",
            item(prim = "Text", unbounded),
            ty(validator(path = "base::validator::text::case::UpperSnake"))
        )]
        pub struct AuditCode {}

        #[doc = ""]
        #[doc = "AuditScoreList"]
        #[doc = ""]
        #[doc = "Bounded numeric list reused across complex audit entities."]
        #[doc = ""]
        #[list(
            item(
                prim = "Nat32",
                validator(path = "base::validator::num::Lte", args(1000))
            ),
            ty(validator(path = "base::validator::len::Max", args(16)))
        )]
        pub struct AuditScoreList {}

        #[doc = ""]
        #[doc = "AuditAliasList"]
        #[doc = ""]
        #[doc = "Normalized alias list reused across complex audit entities."]
        #[doc = ""]
        #[list(item(is = "AuditText"))]
        pub struct AuditAliasList {}

        #[doc = ""]
        #[doc = "AuditTagSet"]
        #[doc = ""]
        #[doc = "Bounded normalized set reused across complex audit entities."]
        #[doc = ""]
        #[set(
            item(is = "AuditText"),
            ty(validator(path = "base::validator::len::Max", args(16)))
        )]
        pub struct AuditTagSet {}

        #[doc = ""]
        #[doc = "AuditAttributeMap"]
        #[doc = ""]
        #[doc = "Bounded map reused across complex audit entities."]
        #[doc = ""]
        #[map(
            key(
                prim = "Text",
                unbounded,
                validator(path = "base::validator::text::case::Lower")
            ),
            value(item(is = "AuditText")),
            ty(validator(path = "base::validator::len::Max", args(16)))
        )]
        pub struct AuditAttributeMap {}

        #[doc = ""]
        #[doc = "AuditProfile"]
        #[doc = ""]
        #[doc = "Embedded record reused across complex audit entities."]
        #[doc = ""]
        #[record(fields(
            field(ident = "display_name", value(item(prim = "Text", unbounded))),
            field(ident = "nickname", value(opt, item(is = "AuditText"))),
            field(ident = "scores", value(item(is = "AuditScoreList"))),
            field(ident = "tags", value(item(is = "AuditTagSet"))),
            field(ident = "attributes", value(item(is = "AuditAttributeMap")))
        ))]
        pub struct AuditProfile {}

        #[doc = ""]
        #[doc = "AuditState"]
        #[doc = ""]
        #[doc = "Payload-bearing enum reused across complex audit entities."]
        #[doc = ""]
        #[enum_(
            variant(ident = "Draft", default),
            variant(ident = "Published", value(item(is = "AuditProfile"))),
            variant(ident = "Archived", value(item(is = "AuditText")))
        )]
        pub struct AuditState {}
    };
}

///
/// define_complex_audit_entities
///
/// Generate one or more repeated complex audit entities for wasm-size
/// fixtures.
///
#[macro_export]
macro_rules! define_complex_audit_entities {
    ($store:literal, $anchor:literal; $($entity:ident),+ $(,)?) => {
        $(
            #[doc = ""]
            #[doc = stringify!($entity)]
            #[doc = ""]
            #[doc = "Repeated complex audit entity used to measure macro-heavy per-entity wasm cost."]
            #[doc = ""]
            #[entity(
                store = $store,
                pk(fields = ["id"]),
                fields(
                    field(ident = "id", value(item(prim = "Ulid")), generated(insert = "Ulid::generate")),
                    field(ident = "slug", value(item(is = "AuditText"))),
                    field(ident = "code", value(item(is = "AuditCode"))),
                    field(ident = "profile", value(item(is = "AuditProfile"))),
                    field(ident = "profile_opt", value(opt, item(is = "AuditProfile"))),
                    field(ident = "state", value(item(is = "AuditState"))),
                    field(ident = "tags", value(item(is = "AuditTagSet"))),
                    field(ident = "scores", value(item(is = "AuditScoreList"))),
                    field(ident = "attributes", value(item(is = "AuditAttributeMap"))),
                    field(ident = "aliases", value(item(is = "AuditAliasList"))),
                    field(
                        ident = "owner_id",
                        value(opt, item(rel = $anchor, prim = "Ulid"))
                    ),
                    field(
                        ident = "sibling_ids",
                        value(many, item(rel = $anchor, prim = "Ulid"))
                    )
                )
            )]
            pub struct $entity {}
        )+
    };
}
