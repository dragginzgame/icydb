//! Schema-only consumer of the standalone `icydb-model` package.
//!
//! This crate intentionally has no dependency on `icydb` or `icydb-core`.

use model_api::{base::types::web::Url, prelude::*};

#[canister(
    memory_namespace = "model_schema_only",
    memory_min = 100,
    memory_max = 110,
    commit_memory_id = 110
)]
pub struct SchemaOnlyCanister {}

#[store(
    ident = "SCHEMA_ONLY_STORE",
    store_name = "schema_only",
    canister = "SchemaOnlyCanister",
    storage(heap())
)]
pub struct SchemaOnlyStore {}

#[record(
    source_key = "fixture/model-schema-only/profile",
    fields(field(source_key = "website", ident = "website", value(item(is = "Url"))))
)]
pub struct SchemaOnlyProfile {}

#[enum_(
    source_key = "fixture/model-schema-only/state",
    variant(source_key = "active", ident = "Active"),
    variant(source_key = "disabled", ident = "Disabled")
)]
pub struct SchemaOnlyState {}

#[entity(
    source_key = "fixture/model-schema-only/entity",
    store = "SchemaOnlyStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(
            source_key = "id",
            ident = "id",
            value(item(prim = "Nat64"))
        ),
        field(
            source_key = "state",
            ident = "state",
            value(item(is = "SchemaOnlyState"))
        ),
        field(
            source_key = "profile",
            ident = "profile",
            value(item(is = "SchemaOnlyProfile"))
        )
    )
)]
pub struct SchemaOnlyEntity {}

#[cfg(test)]
mod tests {
    use model_api::build::{BuildOptions, generate_with_options, get_schema};
    use model_api::schema::{decode_schema_fragment, encode_schema_fragment};

    use super::SchemaOnlyCanister;
    use model_api::Path as _;

    #[test]
    fn renamed_schema_only_dependency_emits_fragment_and_actor_tokens() {
        let canister_path = SchemaOnlyCanister::PATH;
        let schema = get_schema().expect("schema-only fixture graph should validate");
        let fragment = schema
            .schema_fragment_for_canister(canister_path)
            .expect("schema-only fixture should lower one complete fragment");
        assert_eq!(fragment.entities().len(), 1);
        assert_eq!(fragment.types().len(), 3);
        let encoded =
            encode_schema_fragment(&fragment).expect("fixture fragment should encode canonically");
        assert_eq!(
            decode_schema_fragment(&encoded).expect("encoded fixture fragment should decode"),
            fragment,
        );
        drop(schema);

        let actor = generate_with_options(
            canister_path,
            BuildOptions::default()
                .with_metrics_enabled(false)
                .with_icydb_crate_path("runtime_api"),
        );
        assert!(actor.contains("runtime_api"));
        assert!(!actor.contains(":: icydb ::"));
    }
}
