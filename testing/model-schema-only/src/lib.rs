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

#[newtype(
    source_key = "fixture/model-schema-only/field-key",
    primitive = "Text",
    item(prim = "Text", max_len = 64)
)]
pub struct FieldKey {}

#[map(
    source_key = "fixture/model-schema-only/values",
    key(is = "FieldKey"),
    value(item(is = "FieldValue"))
)]
pub struct Values {}

#[enum_(
    source_key = "fixture/model-schema-only/field-value",
    variant(source_key = "one", ident = "One", value(item(indirect, is = "Value"))),
    variant(
        source_key = "many",
        ident = "Many",
        value(many, item(indirect, is = "Value"))
    )
)]
pub struct FieldValue {}

#[enum_(
    source_key = "fixture/model-schema-only/value",
    variant(
        source_key = "text",
        ident = "Text",
        value(item(prim = "Text", max_len = 128))
    ),
    variant(
        source_key = "record",
        ident = "Record",
        value(item(indirect, is = "Values"))
    )
)]
pub struct Value {}

#[newtype(
    source_key = "fixture/model-schema-only/tokens",
    primitive = "Nat64",
    item(prim = "Nat64")
)]
pub struct Tokens {}

#[newtype(
    source_key = "fixture/model-schema-only/token-amount",
    primitive = "Nat64",
    item(prim = "Nat64")
)]
pub struct TokenAmount {}

#[newtype(
    source_key = "fixture/model-schema-only/tier",
    primitive = "Text",
    item(prim = "Text", max_len = 32)
)]
pub struct Tier {}

#[enum_(
    source_key = "fixture/model-schema-only/claim-cost",
    variant(source_key = "free", ident = "Free"),
    variant(source_key = "icp", ident = "Icp", value(item(is = "crate::Tokens"))),
    variant(
        source_key = "icrc1",
        ident = "Icrc1",
        value(item(is = "crate::TokenAmount"))
    )
)]
pub struct ClaimCost {}

#[map(
    source_key = "fixture/model-schema-only/claim-cost-tiers",
    key(is = "Tier"),
    value(item(is = "crate::ClaimCost"))
)]
pub struct ClaimCostTiers {}

#[record(
    source_key = "fixture/model-schema-only/collection-policy",
    fields(
        field(source_key = "values", ident = "values", value(item(is = "Values"))),
        field(
            source_key = "fallback",
            ident = "fallback",
            value(opt, item(is = "Value"))
        ),
        field(
            source_key = "claim-cost-tiers",
            ident = "claim_cost_tiers",
            value(item(is = "ClaimCostTiers"))
        )
    )
)]
pub struct CollectionPolicy {}

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
        ),
        field(
            source_key = "policy",
            ident = "policy",
            value(item(is = "CollectionPolicy"))
        )
    )
)]
pub struct SchemaOnlyEntity {}

#[cfg(test)]
mod tests {
    use model_api::build::{BuildOptions, generate_with_options, get_schema};
    use model_api::schema::{
        FieldType, NamedTypeFragment, decode_schema_fragment, encode_schema_fragment,
    };

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
        let named_type = |source_key: &str| {
            fragment
                .types()
                .iter()
                .find(|candidate| candidate.source_key().as_str() == source_key)
                .expect("reachable named type should be present")
        };
        let NamedTypeFragment::Enum(field_value) =
            named_type("fixture/model-schema-only/field-value")
        else {
            panic!("FieldValue should remain an enum")
        };
        let many = field_value
            .variants()
            .iter()
            .find(|variant| variant.name().as_str() == "Many")
            .and_then(model_api::schema::EnumVariantFragment::payload)
            .expect("FieldValue::Many should retain its payload");
        assert!(matches!(
            many,
            FieldType::List(item)
                if matches!(
                    item.as_ref(),
                    FieldType::Named(source)
                        if source.as_str() == "fixture/model-schema-only/value"
                )
        ));
        let NamedTypeFragment::Enum(value) = named_type("fixture/model-schema-only/value") else {
            panic!("Value should remain an enum")
        };
        assert!(matches!(
            value
                .variants()
                .iter()
                .find(|variant| variant.name().as_str() == "Record")
                .and_then(model_api::schema::EnumVariantFragment::payload),
            Some(FieldType::Named(source))
                if source.as_str() == "fixture/model-schema-only/values"
        ));
        let NamedTypeFragment::Enum(claim_cost) =
            named_type("fixture/model-schema-only/claim-cost")
        else {
            panic!("ClaimCost should remain an enum")
        };
        for (variant_name, payload_source) in [
            ("Icp", "fixture/model-schema-only/tokens"),
            ("Icrc1", "fixture/model-schema-only/token-amount"),
        ] {
            assert!(matches!(
                claim_cost
                    .variants()
                    .iter()
                    .find(|variant| variant.name().as_str() == variant_name)
                    .and_then(model_api::schema::EnumVariantFragment::payload),
                Some(FieldType::Named(source)) if source.as_str() == payload_source
            ));
        }
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
