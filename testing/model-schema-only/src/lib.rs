//! Schema-only consumer of the standalone `icydb-model` package.
//!
//! This crate intentionally has no dependency on `icydb` or `icydb-core`.

use model_api::{
    base::types::{num::Degrees, web::Url},
    prelude::*,
};

#[canister(
    memory_namespace = "model_schema_only",
    memory_min = 100,
    memory_max = 110,
    commit_memory_id = 110
)]
pub struct SchemaOnlyCanister {}

#[store(canister = "SchemaOnlyCanister", storage(heap()))]
pub struct SchemaOnlyStore {}

#[record(fields(field(name = "website", value(item(is = "Url")))))]
pub struct SchemaOnlyProfile {}

#[enum_(variant(name = "Active"), variant(name = "Disabled"))]
pub struct SchemaOnlyState {}

#[newtype(primitive = "Text", item(prim = "Text", max_len = 64))]
pub struct FieldKey {}

#[map(key(is = "FieldKey"), value(item(is = "FieldValue")))]
pub struct Values {}

#[enum_(
    variant(name = "One", value(item(indirect, is = "Value"))),
    variant(name = "Many", value(many, item(indirect, is = "Value")))
)]
pub struct FieldValue {}

#[enum_(
    name = "SchemaOnlyValue",
    variant(name = "Text", value(item(prim = "Text", max_len = 128))),
    variant(name = "Degrees", value(item(is = "Degrees"))),
    variant(name = "Record", value(item(indirect, is = "Values")))
)]
pub struct Value {}

#[newtype(primitive = "Nat64", item(prim = "Nat64"))]
pub struct Tokens {}

#[newtype(
    name = "SchemaOnlyTokenAmount",
    primitive = "Nat64",
    item(prim = "Nat64")
)]
pub struct TokenAmount {}

#[newtype(primitive = "Text", item(prim = "Text", max_len = 32))]
pub struct Tier {}

#[enum_(
    variant(name = "Free"),
    variant(name = "Icp", value(item(is = "crate::Tokens"))),
    variant(name = "Icrc1", value(item(is = "crate::TokenAmount")))
)]
pub struct ClaimCost {}

#[map(key(is = "Tier"), value(item(is = "crate::ClaimCost")))]
pub struct ClaimCostTiers {}

#[record(fields(
    field(name = "values", value(item(is = "Values"))),
    field(name = "fallback", value(opt, item(is = "Value"))),
    field(name = "claim_cost_tiers", value(item(is = "ClaimCostTiers")))
))]
pub struct CollectionPolicy {}

#[entity(
    store = "SchemaOnlyStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(
            name = "id",
            value(item(prim = "Nat64"))
        ),
        field(
            name = "state",
            value(item(is = "SchemaOnlyState"))
        ),
        field(
            name = "profile",
            value(item(is = "SchemaOnlyProfile"))
        ),
        field(
            name = "degrees",
            value(item(is = "Degrees"))
        ),
        field(
            name = "policy",
            value(item(is = "CollectionPolicy"))
        )
    )
)]
pub struct SchemaOnlyEntity {}

#[cfg(test)]
mod tests {
    use model_api::build::{BuildOptions, generate_with_options, get_schema};
    use model_api::schema::{
        ConstraintFragmentKind, ConstraintSourceKey, FieldSourceKey, FieldType, NamedTypeFragment,
        RuleSourceKey, SchemaFragment, SourceRuleOperation, TypeSourceKey, decode_schema_fragment,
        encode_schema_fragment,
    };

    use super::SchemaOnlyCanister;
    use model_api::Path as _;

    fn assert_targeted_degrees_rules(fragment: &SchemaFragment) {
        let degrees_type = TypeSourceKey::try_new("Degrees").expect("type source");
        let rule_source = RuleSourceKey::try_new("range").expect("base rule source should admit");
        for (root, source) in ["degrees", "policy"].map(|root| {
            let field = FieldSourceKey::try_new(root).expect("fixture field source should admit");
            let source =
                ConstraintSourceKey::for_targeted_field_rule(&field, &degrees_type, &rule_source);
            (root, source)
        }) {
            let constraint = fragment.entities()[0]
                .constraints()
                .iter()
                .find(|constraint| constraint.source_key() == &source)
                .expect("direct and recursively nested Degrees rules should both lower");
            let ConstraintFragmentKind::TargetedRule(rule) = constraint.kind() else {
                panic!("source rule should use the sole targeted-rule proposal path")
            };
            assert_eq!(rule.root().as_str(), root);
            assert_eq!(rule.target_type(), &degrees_type);
            assert!(matches!(
                rule.operation(),
                SourceRuleOperation::NumericRangeInclusive { .. }
            ));
        }
    }

    #[test]
    fn renamed_schema_only_dependency_emits_fragment_and_actor_tokens() {
        let canister_path = SchemaOnlyCanister::PATH;
        let schema = get_schema().expect("schema-only fixture graph should validate");
        let fragment = schema
            .schema_fragment_for_canister(canister_path)
            .expect("schema-only fixture should lower one complete fragment");
        assert_eq!(fragment.entities().len(), 1);
        assert_targeted_degrees_rules(&fragment);
        let named_type = |source_key: &str| {
            fragment
                .types()
                .iter()
                .find(|candidate| candidate.source_key().as_str() == source_key)
                .expect("reachable named type should be present")
        };
        let NamedTypeFragment::Enum(field_value) = named_type("FieldValue") else {
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
                        if source.as_str() == "SchemaOnlyValue"
                )
        ));
        let NamedTypeFragment::Enum(value) = named_type("SchemaOnlyValue") else {
            panic!("Value should remain an enum")
        };
        assert!(matches!(
            value
                .variants()
                .iter()
                .find(|variant| variant.name().as_str() == "Record")
                .and_then(model_api::schema::EnumVariantFragment::payload),
            Some(FieldType::Named(source))
                if source.as_str() == "Values"
        ));
        let NamedTypeFragment::Enum(claim_cost) = named_type("ClaimCost") else {
            panic!("ClaimCost should remain an enum")
        };
        for (variant_name, payload_source) in
            [("Icp", "Tokens"), ("Icrc1", "SchemaOnlyTokenAmount")]
        {
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
