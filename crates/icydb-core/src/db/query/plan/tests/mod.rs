//! Module: db::query::plan::tests
//! Responsibility: module-local ownership and contracts for db::query::plan::tests.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

mod error_mapping;
mod group;
mod pushdown;
mod semantics;
mod structural_guards;

use crate::{
    db::access::{AccessPath, AccessPlan},
    db::predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
    db::query::plan::plan_access,
    db::schema::SchemaInfo,
    model::{entity::EntityModel, field::FieldKind, index::IndexModel},
    traits::EntitySchema,
    types::Ulid,
    value::Value,
};
use std::ops::Bound;

const INDEX_FIELDS: [&str; 1] = ["tag"];
const INDEX_MODEL: IndexModel = IndexModel::new(
    "plan_tests::idx_tag",
    "plan_tests::IndexStore",
    &INDEX_FIELDS,
    false,
);
const RANGE_INDEX_FIELDS: [&str; 3] = ["a", "b", "c"];
const RANGE_INDEX_MODEL: IndexModel = IndexModel::new(
    "plan_tests::idx_abc",
    "plan_tests::RangeIndexStore",
    &RANGE_INDEX_FIELDS,
    false,
);

crate::test_entity! {
    ident = PlanModelEntity,
    id = Ulid,
    entity_name = "PlanEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("tag", FieldKind::Text),
    ],
    indexes = [&INDEX_MODEL],
}

crate::test_entity! {
    ident = PlanRangeEntity,
    id = Ulid,
    entity_name = "PlanRangeEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("a", FieldKind::Uint),
        ("b", FieldKind::Uint),
        ("c", FieldKind::Uint),
    ],
    indexes = [&RANGE_INDEX_MODEL],
}

// Helper for tests that need the indexed model derived from a typed schema.
fn model_with_index() -> &'static EntityModel {
    <PlanModelEntity as EntitySchema>::MODEL
}

fn model_with_range_index() -> &'static EntityModel {
    <PlanRangeEntity as EntitySchema>::MODEL
}

fn compare_strict(field: &str, op: CompareOp, value: Value) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        field,
        op,
        value,
        CoercionId::Strict,
    ))
}

fn compare_numeric_widen(field: &str, op: CompareOp, value: Value) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        field,
        op,
        value,
        CoercionId::NumericWiden,
    ))
}

type IndexRangeView<'a> = (
    &'a IndexModel,
    &'a [Value],
    &'a Bound<Value>,
    &'a Bound<Value>,
);

fn find_index_range(plan: &'_ AccessPlan<Value>) -> Option<IndexRangeView<'_>> {
    match plan {
        AccessPlan::Path(path) => match path.as_ref() {
            AccessPath::IndexRange { spec } => Some((
                spec.index(),
                spec.prefix_values(),
                spec.lower(),
                spec.upper(),
            )),
            _ => None,
        },
        AccessPlan::Union(children) | AccessPlan::Intersection(children) => {
            children.iter().find_map(find_index_range)
        }
    }
}

fn visit_access_paths<'a>(plan: &'a AccessPlan<Value>, f: &mut impl FnMut(&'a AccessPath<Value>)) {
    match plan {
        AccessPlan::Path(path) => f(path.as_ref()),
        AccessPlan::Union(children) | AccessPlan::Intersection(children) => {
            for child in children {
                visit_access_paths(child, f);
            }
        }
    }
}

#[test]
fn plan_access_full_scan_without_predicate() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let plan = plan_access(model, &schema, None).expect("plan should build");

    assert_eq!(plan, AccessPlan::full_scan());
}

#[test]
fn plan_access_primary_key_is_null_lowers_to_empty_by_keys() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::IsNull {
        field: "id".to_string(),
    };

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::path(AccessPath::ByKeys(Vec::new())),
        "primary_key IS NULL is unsatisfiable and should lower to explicit empty access shape",
    );
}

#[test]
fn plan_access_secondary_is_null_retains_full_scan_fallback() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::IsNull {
        field: "tag".to_string(),
    };

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::full_scan(),
        "non-primary IS NULL remains full-scan until nullable/index-aware pushdown is available",
    );
}

#[test]
fn plan_access_primary_key_is_null_or_secondary_eq_collapses_to_secondary_branch() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::Or(vec![
        Predicate::IsNull {
            field: "id".to_string(),
        },
        compare_strict("tag", CompareOp::Eq, Value::Text("alpha".to_string())),
    ]);

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::path(AccessPath::IndexPrefix {
            index: INDEX_MODEL,
            values: vec![Value::Text("alpha".to_string())],
        }),
        "primary_key IS NULL is an empty OR-identity and should not widen the surviving branch",
    );
}

#[test]
fn plan_access_primary_key_is_null_or_primary_key_is_null_stays_empty() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::Or(vec![
        Predicate::IsNull {
            field: "id".to_string(),
        },
        Predicate::IsNull {
            field: "id".to_string(),
        },
    ]);

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::path(AccessPath::ByKeys(Vec::new())),
        "OR over only impossible primary_key IS NULL branches should remain explicit empty access",
    );
}

#[test]
fn plan_access_uses_primary_key_lookup() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let key = Ulid::generate();
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "id",
        CompareOp::Eq,
        Value::Ulid(key),
        CoercionId::Strict,
    ));

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");

    assert_eq!(plan, AccessPlan::path(AccessPath::ByKey(Value::Ulid(key))));
}

#[test]
fn plan_access_uses_index_prefix_for_exact_match() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "tag",
        CompareOp::Eq,
        Value::Text("alpha".to_string()),
        CoercionId::Strict,
    ));

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::path(AccessPath::IndexPrefix {
            index: INDEX_MODEL,
            values: vec![Value::Text("alpha".to_string())],
        })
    );
}

#[test]
fn plan_access_uses_index_multi_lookup_for_secondary_in() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = compare_strict(
        "tag",
        CompareOp::In,
        Value::List(vec![
            Value::Text("alpha".to_string()),
            Value::Text("beta".to_string()),
        ]),
    );

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::path(AccessPath::IndexMultiLookup {
            index: INDEX_MODEL,
            values: vec![
                Value::Text("alpha".to_string()),
                Value::Text("beta".to_string()),
            ],
        }),
    );
}

#[test]
fn plan_access_stability_secondary_in_permutation_and_duplicates_are_canonical() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate_a = compare_strict(
        "tag",
        CompareOp::In,
        Value::List(vec![
            Value::Text("beta".to_string()),
            Value::Text("alpha".to_string()),
            Value::Text("beta".to_string()),
        ]),
    );
    let predicate_b = compare_strict(
        "tag",
        CompareOp::In,
        Value::List(vec![
            Value::Text("alpha".to_string()),
            Value::Text("beta".to_string()),
        ]),
    );

    let plan_a = plan_access(model, &schema, Some(&predicate_a)).expect("plan should build");
    let plan_b = plan_access(model, &schema, Some(&predicate_b)).expect("plan should build");

    assert_eq!(
        plan_a, plan_b,
        "equivalent secondary IN predicates should canonicalize to identical access plans",
    );
    assert_eq!(
        plan_a,
        AccessPlan::path(AccessPath::IndexMultiLookup {
            index: INDEX_MODEL,
            values: vec![
                Value::Text("alpha".to_string()),
                Value::Text("beta".to_string()),
            ],
        }),
        "secondary IN canonicalization should sort and deduplicate lookup values",
    );
}

#[test]
fn plan_access_stability_primary_key_in_permutation_and_duplicates_are_canonical() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate_a = compare_strict(
        "id",
        CompareOp::In,
        Value::List(vec![
            Value::Ulid(Ulid::from_u128(3)),
            Value::Ulid(Ulid::from_u128(1)),
            Value::Ulid(Ulid::from_u128(3)),
        ]),
    );
    let predicate_b = compare_strict(
        "id",
        CompareOp::In,
        Value::List(vec![
            Value::Ulid(Ulid::from_u128(1)),
            Value::Ulid(Ulid::from_u128(3)),
        ]),
    );

    let plan_a = plan_access(model, &schema, Some(&predicate_a)).expect("plan should build");
    let plan_b = plan_access(model, &schema, Some(&predicate_b)).expect("plan should build");

    assert_eq!(
        plan_a, plan_b,
        "equivalent primary-key IN predicates should canonicalize to identical access plans",
    );
    assert_eq!(
        plan_a,
        AccessPlan::path(AccessPath::ByKeys(vec![
            Value::Ulid(Ulid::from_u128(1)),
            Value::Ulid(Ulid::from_u128(3)),
        ])),
        "primary-key IN canonicalization should sort and deduplicate key lists",
    );
}

#[test]
fn plan_access_secondary_in_singleton_collapses_to_index_prefix() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = compare_strict(
        "tag",
        CompareOp::In,
        Value::List(vec![Value::Text("alpha".to_string())]),
    );

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::path(AccessPath::IndexPrefix {
            index: INDEX_MODEL,
            values: vec![Value::Text("alpha".to_string())],
        }),
    );
}

#[test]
fn plan_access_stability_secondary_or_eq_lowers_to_index_multi_lookup() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::Or(vec![
        compare_strict("tag", CompareOp::Eq, Value::Text("beta".to_string())),
        compare_strict("tag", CompareOp::Eq, Value::Text("alpha".to_string())),
        compare_strict("tag", CompareOp::Eq, Value::Text("beta".to_string())),
    ]);

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::path(AccessPath::IndexMultiLookup {
            index: INDEX_MODEL,
            values: vec![
                Value::Text("alpha".to_string()),
                Value::Text("beta".to_string()),
            ],
        }),
        "same-field strict OR equality should canonicalize through bounded IN planning",
    );
}

#[test]
fn plan_access_stability_primary_key_or_eq_lowers_to_by_keys() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::Or(vec![
        compare_strict("id", CompareOp::Eq, Value::Ulid(Ulid::from_u128(3))),
        compare_strict("id", CompareOp::Eq, Value::Ulid(Ulid::from_u128(1))),
        compare_strict("id", CompareOp::Eq, Value::Ulid(Ulid::from_u128(3))),
    ]);

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::path(AccessPath::ByKeys(vec![
            Value::Ulid(Ulid::from_u128(1)),
            Value::Ulid(Ulid::from_u128(3)),
        ])),
        "same-field strict OR equality over primary key should canonicalize into ByKeys",
    );
}

#[test]
fn plan_access_secondary_or_eq_with_non_strict_branch_stays_fail_closed() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::Or(vec![
        compare_strict("tag", CompareOp::Eq, Value::Text("alpha".to_string())),
        Predicate::Compare(ComparePredicate::with_coercion(
            "tag",
            CompareOp::Eq,
            Value::Text("beta".to_string()),
            CoercionId::TextCasefold,
        )),
    ]);

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::full_scan(),
        "OR rewrite must remain fail-closed when any branch is non-strict",
    );
}

#[test]
fn plan_access_secondary_in_empty_lowers_to_empty_by_keys() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = compare_strict("tag", CompareOp::In, Value::List(Vec::new()));

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::path(AccessPath::ByKeys(Vec::new())),
        "strict secondary IN [] should lower to an explicit empty by-keys access shape",
    );
}

#[test]
fn plan_access_secondary_in_empty_remains_distinct_from_false_before_constant_folding() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let secondary_in_empty = compare_strict("tag", CompareOp::In, Value::List(Vec::new()));

    let plan_from_empty_in =
        plan_access(model, &schema, Some(&secondary_in_empty)).expect("plan should build");
    let plan_from_false =
        plan_access(model, &schema, Some(&Predicate::False)).expect("plan should build");

    assert_eq!(
        plan_from_empty_in,
        AccessPlan::path(AccessPath::ByKeys(Vec::new())),
        "strict secondary IN [] should lower to explicit empty by-keys shape at access planning",
    );
    assert_eq!(
        plan_from_false,
        AccessPlan::full_scan(),
        "constant FALSE folding remains a higher-level planning concern outside direct access planning",
    );
    assert_ne!(
        plan_from_empty_in, plan_from_false,
        "strict secondary IN [] and FALSE should remain distinct at direct access-planning boundary",
    );
}

#[test]
fn plan_access_secondary_in_empty_in_and_group_collapses_to_empty_by_keys() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::And(vec![
        compare_strict("tag", CompareOp::In, Value::List(Vec::new())),
        compare_strict("tag", CompareOp::Eq, Value::Text("alpha".to_string())),
    ]);

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::path(AccessPath::ByKeys(Vec::new())),
        "AND groups that include strict IN [] should collapse to one explicit empty access shape",
    );
}

#[test]
fn plan_access_secondary_in_mixed_literal_types_stays_fail_closed() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = compare_strict(
        "tag",
        CompareOp::In,
        Value::List(vec![Value::Text("alpha".to_string()), Value::Uint(7)]),
    );

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");

    assert_eq!(
        plan,
        AccessPlan::full_scan(),
        "strict IN pushdown must fail closed when any literal is schema-incompatible",
    );
}

#[test]
fn plan_access_emits_index_range_for_single_field_between_equivalent_bounds() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::And(vec![
        compare_strict("tag", CompareOp::Gte, Value::Text("alpha".to_string())),
        compare_strict("tag", CompareOp::Lte, Value::Text("omega".to_string())),
    ]);

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");
    let (index, prefix, lower, upper) =
        find_index_range(&plan).expect("plan should include index range");

    assert_eq!(index.name(), INDEX_MODEL.name());
    assert!(
        prefix.is_empty(),
        "single-field ranges should use empty prefixes"
    );
    assert_eq!(lower, &Bound::Included(Value::Text("alpha".to_string())));
    assert_eq!(upper, &Bound::Included(Value::Text("omega".to_string())));
}

#[test]
fn plan_access_stability_single_field_between_equal_bounds_and_eq_share_identical_access_plan() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let between_equal_bounds = Predicate::And(vec![
        compare_strict("tag", CompareOp::Gte, Value::Text("alpha".to_string())),
        compare_strict("tag", CompareOp::Lte, Value::Text("alpha".to_string())),
    ]);
    let strict_eq = compare_strict("tag", CompareOp::Eq, Value::Text("alpha".to_string()));

    let between_plan =
        plan_access(model, &schema, Some(&between_equal_bounds)).expect("plan should build");
    let eq_plan = plan_access(model, &schema, Some(&strict_eq)).expect("plan should build");

    assert_eq!(
        between_plan, eq_plan,
        "single-field equal-bounds BETWEEN shapes should canonicalize to the same access plan as strict equality",
    );
}

#[test]
fn plan_access_emits_index_range_for_text_starts_with() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = compare_strict("tag", CompareOp::StartsWith, Value::Text("foo".to_string()));

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");
    let (index, prefix, lower, upper) =
        find_index_range(&plan).expect("plan should include index range");

    assert_eq!(index.name(), INDEX_MODEL.name());
    assert!(
        prefix.is_empty(),
        "starts-with index range should use an empty equality prefix"
    );
    assert_eq!(lower, &Bound::Included(Value::Text("foo".to_string())));
    assert_eq!(upper, &Bound::Excluded(Value::Text("fop".to_string())));
}

#[test]
fn plan_access_starts_with_empty_prefix_falls_back_to_full_scan() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = compare_strict("tag", CompareOp::StartsWith, Value::Text(String::new()));

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");

    assert_eq!(plan, AccessPlan::full_scan());
}

#[test]
fn plan_access_starts_with_high_unicode_prefix_skips_surrogate_gap_in_upper_bound() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let prefix = format!("foo{}", char::from_u32(0xD7FF).expect("valid scalar"));
    let predicate = compare_strict("tag", CompareOp::StartsWith, Value::Text(prefix.clone()));

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");
    let (_, _, lower, upper) = find_index_range(&plan).expect("plan should include index range");

    assert_eq!(lower, &Bound::Included(Value::Text(prefix)));
    assert_eq!(
        upper,
        &Bound::Excluded(Value::Text(format!(
            "foo{}",
            char::from_u32(0xE000).expect("valid scalar")
        ))),
    );
}

#[test]
fn plan_access_starts_with_max_unicode_prefix_uses_unbounded_upper() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let prefix = char::from_u32(0x10_FFFF).expect("valid scalar").to_string();
    let predicate = compare_strict("tag", CompareOp::StartsWith, Value::Text(prefix.clone()));

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");
    let (_, _, lower, upper) = find_index_range(&plan).expect("plan should include index range");

    assert_eq!(lower, &Bound::Included(Value::Text(prefix)));
    assert_eq!(upper, &Bound::Unbounded);
}

#[test]
fn plan_access_stability_starts_with_and_equivalent_range_share_identical_access_plan() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let starts_with = compare_strict("tag", CompareOp::StartsWith, Value::Text("foo".to_string()));
    let equivalent_range = Predicate::And(vec![
        compare_strict("tag", CompareOp::Gte, Value::Text("foo".to_string())),
        compare_strict("tag", CompareOp::Lt, Value::Text("fop".to_string())),
    ]);

    let starts_with_plan =
        plan_access(model, &schema, Some(&starts_with)).expect("starts_with plan should build");
    let equivalent_range_plan = plan_access(model, &schema, Some(&equivalent_range))
        .expect("equivalent range plan should build");

    assert_eq!(
        starts_with_plan, equivalent_range_plan,
        "equivalent prefix and bounded-range predicates should canonicalize to identical access plans",
    );
}

#[test]
fn plan_access_stability_max_unicode_starts_with_and_equivalent_lower_bound_share_plan() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let prefix = char::from_u32(0x10_FFFF).expect("valid scalar").to_string();
    let starts_with = compare_strict("tag", CompareOp::StartsWith, Value::Text(prefix.clone()));
    let equivalent_lower_bound = compare_strict("tag", CompareOp::Gte, Value::Text(prefix));

    let starts_with_plan =
        plan_access(model, &schema, Some(&starts_with)).expect("starts_with plan should build");
    let lower_bound_plan = plan_access(model, &schema, Some(&equivalent_lower_bound))
        .expect("lower-bound plan should build");

    assert_eq!(
        starts_with_plan, lower_bound_plan,
        "max-unicode prefix has no strict upper bound and should match equivalent lower-bound range planning",
    );
}

#[test]
fn plan_access_emits_index_range_for_single_field_gt() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = compare_strict("tag", CompareOp::Gt, Value::Text("alpha".to_string()));

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");
    let (index, prefix, lower, upper) =
        find_index_range(&plan).expect("plan should include index range");

    assert_eq!(index.name(), INDEX_MODEL.name());
    assert!(
        prefix.is_empty(),
        "single-field ranges should use empty prefixes"
    );
    assert_eq!(lower, &Bound::Excluded(Value::Text("alpha".to_string())));
    assert_eq!(upper, &Bound::Unbounded);
}

#[test]
fn plan_access_emits_index_range_for_single_field_lte() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = compare_strict("tag", CompareOp::Lte, Value::Text("omega".to_string()));

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");
    let (index, prefix, lower, upper) =
        find_index_range(&plan).expect("plan should include index range");

    assert_eq!(index.name(), INDEX_MODEL.name());
    assert!(
        prefix.is_empty(),
        "single-field ranges should use empty prefixes"
    );
    assert_eq!(lower, &Bound::Unbounded);
    assert_eq!(upper, &Bound::Included(Value::Text("omega".to_string())));
}

#[test]
fn plan_access_ignores_non_strict_predicates() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "tag",
        CompareOp::Eq,
        Value::Text("alpha".to_string()),
        CoercionId::TextCasefold,
    ));

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");

    assert_eq!(plan, AccessPlan::full_scan());
}

#[test]
fn plan_access_emits_index_range_for_prefix_plus_range() {
    let model = model_with_range_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::And(vec![
        compare_strict("a", CompareOp::Eq, Value::Uint(7)),
        compare_strict("b", CompareOp::Gte, Value::Uint(100)),
        compare_strict("b", CompareOp::Lt, Value::Uint(200)),
    ]);

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");
    let (index, prefix, lower, upper) =
        find_index_range(&plan).expect("plan should include index range");

    assert_eq!(index.name(), RANGE_INDEX_MODEL.name());
    assert_eq!(prefix, [Value::Uint(7)].as_slice());
    assert_eq!(lower, &Bound::Included(Value::Uint(100)));
    assert_eq!(upper, &Bound::Excluded(Value::Uint(200)));
}

#[test]
fn plan_access_emits_only_one_composite_index_range_for_and_eq_plus_gt() {
    let model = model_with_range_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::And(vec![
        compare_strict("a", CompareOp::Eq, Value::Uint(1)),
        compare_strict("b", CompareOp::Gt, Value::Uint(5)),
    ]);

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");
    let AccessPlan::Path(path) = &plan else {
        panic!("composite eq+range predicate should emit a single access path");
    };
    let AccessPath::IndexRange { spec } = path.as_ref() else {
        panic!("composite eq+range predicate should emit IndexRange");
    };
    let index = spec.index();
    let prefix = spec.prefix_values();
    let lower = spec.lower();
    let upper = spec.upper();

    assert_eq!(index.name(), RANGE_INDEX_MODEL.name());
    assert_eq!(prefix, [Value::Uint(1)].as_slice());
    assert_eq!(lower, &Bound::Excluded(Value::Uint(5)));
    assert_eq!(upper, &Bound::Unbounded);

    let mut index_range_count = 0usize;
    let mut index_prefix_count = 0usize;
    let mut single_field_index_range_count = 0usize;
    visit_access_paths(&plan, &mut |access| match access {
        AccessPath::IndexRange { spec } => {
            index_range_count = index_range_count.saturating_add(1);
            if spec.prefix_values().is_empty() {
                single_field_index_range_count = single_field_index_range_count.saturating_add(1);
            }
        }
        AccessPath::IndexPrefix { .. } => {
            index_prefix_count = index_prefix_count.saturating_add(1);
        }
        _ => {}
    });

    assert_eq!(
        index_range_count, 1,
        "exactly one IndexRange should be emitted"
    );
    assert_eq!(
        index_prefix_count, 0,
        "composite IndexRange should not carry IndexPrefix siblings"
    );
    assert_eq!(
        single_field_index_range_count, 0,
        "composite IndexRange should not carry single-field IndexRange siblings"
    );
}

#[test]
fn plan_access_emits_index_range_for_between_equivalent_bounds() {
    let model = model_with_range_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::And(vec![
        compare_strict("a", CompareOp::Eq, Value::Uint(7)),
        compare_strict("b", CompareOp::Gte, Value::Uint(100)),
        compare_strict("b", CompareOp::Lte, Value::Uint(200)),
    ]);

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");
    let (index, prefix, lower, upper) =
        find_index_range(&plan).expect("plan should include index range");

    assert_eq!(index.name(), RANGE_INDEX_MODEL.name());
    assert_eq!(prefix, [Value::Uint(7)].as_slice());
    assert_eq!(lower, &Bound::Included(Value::Uint(100)));
    assert_eq!(upper, &Bound::Included(Value::Uint(200)));
}

#[test]
fn plan_access_emits_index_range_for_prefix_plus_range_edge_bounds() {
    let model = model_with_range_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::And(vec![
        compare_strict("a", CompareOp::Eq, Value::Uint(7)),
        compare_strict("b", CompareOp::Gte, Value::Uint(0)),
        compare_strict("b", CompareOp::Lt, Value::Uint(u64::from(u32::MAX))),
    ]);

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");
    let (index, prefix, lower, upper) =
        find_index_range(&plan).expect("plan should include index range");

    assert_eq!(index.name(), RANGE_INDEX_MODEL.name());
    assert_eq!(prefix, [Value::Uint(7)].as_slice());
    assert_eq!(lower, &Bound::Included(Value::Uint(0)));
    assert_eq!(upper, &Bound::Excluded(Value::Uint(u64::from(u32::MAX))));
}

#[test]
fn plan_access_emits_index_range_for_between_equivalent_edge_bounds() {
    let model = model_with_range_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::And(vec![
        compare_strict("a", CompareOp::Eq, Value::Uint(7)),
        compare_strict("b", CompareOp::Gte, Value::Uint(0)),
        compare_strict("b", CompareOp::Lte, Value::Uint(u64::from(u32::MAX))),
    ]);

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");
    let (index, prefix, lower, upper) =
        find_index_range(&plan).expect("plan should include index range");

    assert_eq!(index.name(), RANGE_INDEX_MODEL.name());
    assert_eq!(prefix, [Value::Uint(7)].as_slice());
    assert_eq!(lower, &Bound::Included(Value::Uint(0)));
    assert_eq!(upper, &Bound::Included(Value::Uint(u64::from(u32::MAX))));
}

#[test]
fn plan_access_rejects_trailing_equality_after_range() {
    let model = model_with_range_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::And(vec![
        compare_strict("a", CompareOp::Eq, Value::Uint(7)),
        compare_strict("b", CompareOp::Gte, Value::Uint(100)),
        compare_strict("c", CompareOp::Eq, Value::Uint(3)),
    ]);

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");
    assert!(
        find_index_range(&plan).is_none(),
        "range path should be rejected when equality appears after range field"
    );
}

#[test]
fn plan_access_rejects_range_with_missing_prefix_component() {
    let model = model_with_range_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::And(vec![
        compare_strict("a", CompareOp::Eq, Value::Uint(7)),
        compare_strict("c", CompareOp::Gte, Value::Uint(100)),
    ]);

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");
    assert!(
        find_index_range(&plan).is_none(),
        "range path should be rejected when first non-equality component is skipped"
    );
}

#[test]
fn plan_access_rejects_range_before_prefix_equality() {
    let model = model_with_range_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::And(vec![
        compare_strict("a", CompareOp::Gte, Value::Uint(7)),
        compare_strict("b", CompareOp::Eq, Value::Uint(3)),
    ]);

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");
    assert!(
        find_index_range(&plan).is_none(),
        "range path should be rejected when equality appears after a leading range field"
    );
}

#[test]
fn plan_access_merges_duplicate_lower_bounds_to_stricter_value() {
    let model = model_with_range_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::And(vec![
        compare_strict("a", CompareOp::Eq, Value::Uint(7)),
        compare_strict("b", CompareOp::Gte, Value::Uint(50)),
        compare_strict("b", CompareOp::Gt, Value::Uint(80)),
        compare_strict("b", CompareOp::Lte, Value::Uint(200)),
    ]);

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");
    let (_, _, lower, upper) = find_index_range(&plan).expect("plan should include index range");
    assert_eq!(lower, &Bound::Excluded(Value::Uint(80)));
    assert_eq!(upper, &Bound::Included(Value::Uint(200)));
}

#[test]
fn plan_access_stability_equivalent_predicates_share_identical_access_plan() {
    let model = model_with_range_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate_a = Predicate::And(vec![
        compare_strict("a", CompareOp::Eq, Value::Uint(7)),
        compare_strict("b", CompareOp::Gte, Value::Uint(100)),
        compare_strict("b", CompareOp::Lte, Value::Uint(100)),
    ]);
    let predicate_b = Predicate::And(vec![
        compare_strict("b", CompareOp::Eq, Value::Uint(100)),
        compare_strict("a", CompareOp::Eq, Value::Uint(7)),
    ]);

    let plan_a = plan_access(model, &schema, Some(&predicate_a)).expect("plan should build");
    let plan_b = plan_access(model, &schema, Some(&predicate_b)).expect("plan should build");

    assert_eq!(
        plan_a, plan_b,
        "equivalent canonical predicate shapes must lower to identical access plans",
    );
}

#[test]
fn plan_access_stability_contradictory_and_predicate_matches_constant_false_shape() {
    let model = model_with_range_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let contradictory = Predicate::And(vec![
        compare_strict("a", CompareOp::Eq, Value::Uint(7)),
        compare_strict("b", CompareOp::Gt, Value::Uint(100)),
        compare_strict("b", CompareOp::Lt, Value::Uint(100)),
    ]);

    let plan_from_contradiction =
        plan_access(model, &schema, Some(&contradictory)).expect("plan should build");
    let plan_from_false =
        plan_access(model, &schema, Some(&Predicate::False)).expect("plan should build");

    assert_eq!(
        plan_from_contradiction, plan_from_false,
        "contradictory conjunctions should canonicalize to the same access shape as false",
    );
}

#[test]
fn plan_access_rejects_empty_exclusive_interval() {
    let model = model_with_range_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::And(vec![
        compare_strict("a", CompareOp::Eq, Value::Uint(7)),
        compare_strict("b", CompareOp::Gt, Value::Uint(100)),
        compare_strict("b", CompareOp::Lt, Value::Uint(100)),
    ]);

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");
    assert!(
        find_index_range(&plan).is_none(),
        "exclusive equal bounds should be rejected as empty interval"
    );
}

#[test]
fn plan_access_rejects_non_strict_numeric_widen_for_range_bounds() {
    let model = model_with_range_index();
    let schema = SchemaInfo::from_entity_model(model).expect("schema should validate");
    let predicate = Predicate::And(vec![
        compare_strict("a", CompareOp::Eq, Value::Uint(7)),
        compare_numeric_widen("b", CompareOp::Gte, Value::Int(100)),
        compare_numeric_widen("b", CompareOp::Lte, Value::Uint(200)),
    ]);

    let plan = plan_access(model, &schema, Some(&predicate)).expect("plan should build");
    assert!(
        find_index_range(&plan).is_none(),
        "non-strict numeric widen predicates must not compile into index range access paths"
    );
}
