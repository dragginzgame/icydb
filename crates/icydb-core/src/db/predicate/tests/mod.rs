//! Module: db::predicate::tests
//! Covers predicate subsystem contract behavior that spans capability
//! classification, model fixtures, and index compile targets.
//! Boundary: keeps predicate-owned cross-file tests at the subsystem root.

use crate::{
    db::predicate::{
        CoercionId, CoercionSpec, CompareOp, ExecutableComparePredicate, ExecutablePredicate,
        IndexCompileTarget, IndexPredicateCapability, PredicateCapabilityContext,
        ScalarPredicateCapability, classify_index_compare_component, classify_index_compare_target,
        classify_predicate_capabilities, classify_predicate_capabilities_for_targets,
        lower_index_compare_literal_for_target, lower_index_starts_with_prefix_for_target,
    },
    model::{
        entity::EntityModel,
        field::{FieldKind, FieldModel},
        index::{IndexExpression, IndexKeyItem},
    },
    value::Value,
};

static CAPABILITY_FIELDS: [FieldModel; 3] = [
    FieldModel::generated("score", FieldKind::Int),
    FieldModel::generated("name", FieldKind::Text { max_len: None }),
    FieldModel::generated("tags", FieldKind::List(&FieldKind::Text { max_len: None })),
];
static CAPABILITY_MODEL: EntityModel = EntityModel::generated(
    "PredicateCapabilityEntity",
    "PredicateCapabilityEntity",
    &CAPABILITY_FIELDS[0],
    0,
    &CAPABILITY_FIELDS,
    &[],
);

#[test]
fn strict_scalar_compare_is_scalar_safe_and_indexable_when_indexed() {
    let predicate = ExecutablePredicate::Compare(ExecutableComparePredicate::field_literal(
        Some(0),
        CompareOp::Eq,
        Value::Int(7),
        CoercionSpec::new(CoercionId::Strict),
    ));
    let scalar_profile = classify_predicate_capabilities(
        &predicate,
        PredicateCapabilityContext::runtime_for_model_only(&CAPABILITY_MODEL),
    );
    let index_profile = classify_predicate_capabilities(
        &predicate,
        PredicateCapabilityContext::index_compile(&[0]),
    );

    assert_eq!(
        scalar_profile.scalar(),
        ScalarPredicateCapability::ScalarSafe
    );
    assert_eq!(
        index_profile.index(),
        IndexPredicateCapability::FullyIndexable
    );
}

#[test]
fn scalar_text_contains_requires_full_scan() {
    let predicate = ExecutablePredicate::TextContainsCi {
        field_slot: Some(1),
        value: Value::Text("alp".to_string()),
    };
    let scalar_profile = classify_predicate_capabilities(
        &predicate,
        PredicateCapabilityContext::runtime_for_model_only(&CAPABILITY_MODEL),
    );
    let index_profile = classify_predicate_capabilities(
        &predicate,
        PredicateCapabilityContext::index_compile(&[1]),
    );

    assert_eq!(
        scalar_profile.scalar(),
        ScalarPredicateCapability::ScalarSafe
    );
    assert_eq!(
        index_profile.index(),
        IndexPredicateCapability::RequiresFullScan
    );
}

#[test]
fn mixed_and_tree_is_partially_indexable_but_not_fully_indexable() {
    let predicate = ExecutablePredicate::And(vec![
        ExecutablePredicate::Compare(ExecutableComparePredicate::field_literal(
            Some(0),
            CompareOp::Eq,
            Value::Int(7),
            CoercionSpec::new(CoercionId::Strict),
        )),
        ExecutablePredicate::TextContainsCi {
            field_slot: Some(1),
            value: Value::Text("alp".to_string()),
        },
    ]);
    let profile = classify_predicate_capabilities(
        &predicate,
        PredicateCapabilityContext::index_compile(&[0]),
    );

    assert_eq!(
        profile.index(),
        IndexPredicateCapability::PartiallyIndexable
    );
}

#[test]
fn index_compare_component_requires_strict_supported_projection() {
    let strict = ExecutableComparePredicate::field_literal(
        Some(0),
        CompareOp::In,
        Value::List(vec![Value::Int(1), Value::Int(2)]),
        CoercionSpec::new(CoercionId::Strict),
    );
    let non_strict = ExecutableComparePredicate::field_literal(
        Some(0),
        CompareOp::Eq,
        Value::Int(7),
        CoercionSpec::new(CoercionId::NumericWiden),
    );

    assert_eq!(classify_index_compare_component(&strict, &[0]), Some(0));
    assert_eq!(classify_index_compare_component(&strict, &[1]), None);
    assert_eq!(classify_index_compare_component(&non_strict, &[0]), None);
}

#[test]
fn text_casefold_expression_range_is_fully_indexable_for_compile_targets() {
    let predicate = ExecutablePredicate::And(vec![
        ExecutablePredicate::Compare(ExecutableComparePredicate::field_literal(
            Some(1),
            CompareOp::Gte,
            Value::Text("br".to_string()),
            CoercionSpec::new(CoercionId::TextCasefold),
        )),
        ExecutablePredicate::Compare(ExecutableComparePredicate::field_literal(
            Some(1),
            CompareOp::Lt,
            Value::Text("bs".to_string()),
            CoercionSpec::new(CoercionId::TextCasefold),
        )),
    ]);
    let compile_targets = [IndexCompileTarget {
        component_index: 0,
        field_slot: 1,
        key_item: IndexKeyItem::Expression(IndexExpression::Lower("name")),
    }];
    let profile = classify_predicate_capabilities_for_targets(&predicate, &compile_targets);

    assert_eq!(profile.index(), IndexPredicateCapability::FullyIndexable);
}

#[test]
fn text_casefold_expression_compare_target_lowers_canonical_text_bytes() {
    let cmp = ExecutableComparePredicate::field_literal(
        Some(1),
        CompareOp::StartsWith,
        Value::Text("BR".to_string()),
        CoercionSpec::new(CoercionId::TextCasefold),
    );
    let compile_target = IndexCompileTarget {
        component_index: 0,
        field_slot: 1,
        key_item: IndexKeyItem::Expression(IndexExpression::Lower("name")),
    };

    assert_eq!(
        classify_index_compare_target(&cmp, &[compile_target]),
        Some(compile_target),
    );
    assert_eq!(
        lower_index_compare_literal_for_target(
            compile_target,
            &Value::Text("BR".to_string()),
            CoercionId::TextCasefold,
        ),
        Some(Value::Text("br".to_string())),
    );
    assert_eq!(
        lower_index_starts_with_prefix_for_target(
            compile_target,
            cmp.right_literal().expect("starts-with test literal"),
            CoercionId::TextCasefold,
        ),
        Some("br".to_string()),
    );
}
