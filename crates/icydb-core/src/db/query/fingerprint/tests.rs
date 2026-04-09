//! Module: db::query::fingerprint::tests
//! Responsibility: module-local ownership and contracts for db::query::fingerprint::tests.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        access::AccessPath,
        codec::cursor::encode_cursor,
        predicate::{CoercionId, CompareOp, ComparePredicate, MissingRowPolicy, Predicate},
        query::{
            builder::{field::FieldRef, sum},
            explain::{ExplainGroupedStrategy, ExplainGrouping},
            fingerprint::{
                finalize_sha256_digest, hash_parts, new_continuation_signature_hasher_v1,
                new_plan_fingerprint_hasher_v1,
            },
            intent::{KeyAccess, build_access_plan_from_keys},
            plan::{
                AccessPlannedQuery, AggregateKind, DeleteLimitSpec, DeleteSpec, FieldSlot,
                GroupAggregateSpec, GroupSpec, GroupedExecutionConfig, LoadSpec, LogicalPlan,
                PageSpec, QueryMode, ScalarPlan,
                expr::{
                    Alias, BinaryOp, Expr, FieldId, ProjectionField, ProjectionSelection,
                    ProjectionSpec,
                },
            },
        },
    },
    model::index::IndexModel,
    types::{Decimal, Ulid},
    value::Value,
};
use sha2::{Digest, Sha256};
use std::ops::Bound;

fn fingerprint_with_projection(plan: &AccessPlannedQuery, projection: &ProjectionSpec) -> [u8; 32] {
    let explain = plan.explain();
    let mut hasher = new_plan_fingerprint_hasher_v1();
    hash_explain_plan_profile_with_projection(
        &mut hasher,
        &explain,
        hash_parts::ExplainHashProfile::FingerprintV1,
        projection,
    );

    finalize_sha256_digest(hasher)
}

fn hash_explain_plan_profile_with_projection(
    hasher: &mut Sha256,
    plan: &crate::db::query::explain::ExplainPlan,
    profile: hash_parts::ExplainHashProfile<'_>,
    projection: &ProjectionSpec,
) {
    hash_parts::hash_explain_plan_profile_internal(hasher, plan, profile, Some(projection));
}

fn full_scan_query() -> AccessPlannedQuery {
    AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
}

fn index_prefix_query(index: IndexModel, values: Vec<Value>) -> AccessPlannedQuery {
    AccessPlannedQuery::new(
        AccessPath::IndexPrefix { index, values },
        MissingRowPolicy::Ignore,
    )
}

fn index_range_query(
    index: IndexModel,
    prefix: Vec<Value>,
    lower: Bound<Value>,
    upper: Bound<Value>,
) -> AccessPlannedQuery {
    AccessPlannedQuery::new(
        AccessPath::index_range(index, prefix, lower, upper),
        MissingRowPolicy::Ignore,
    )
}

fn grouped_query_with_fixed_shape() -> AccessPlannedQuery {
    AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore).into_grouped(
        GroupSpec {
            group_fields: vec![FieldSlot::from_parts_for_test(1, "rank")],
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: None,
                distinct: false,
            }],
            execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
        },
    )
}

fn grouped_explain_with_fixed_shape() -> crate::db::query::explain::ExplainPlan {
    grouped_query_with_fixed_shape().explain()
}

#[test]
fn plan_fingerprint_hasher_profile_seed_matches_manual_contract() {
    let mut helper = new_plan_fingerprint_hasher_v1();
    helper.update(b"payload");

    let mut manual = Sha256::new();
    manual.update(b"planfp:v1");
    manual.update(b"payload");

    assert_eq!(helper.finalize(), manual.finalize());
}

#[test]
fn continuation_signature_hasher_profile_seed_matches_manual_contract() {
    let mut helper = new_continuation_signature_hasher_v1();
    helper.update(b"payload");

    let mut manual = Sha256::new();
    manual.update(b"contsig:v1");
    manual.update(b"payload");

    assert_eq!(helper.finalize(), manual.finalize());
}

#[test]
fn fingerprint_is_deterministic_for_equivalent_predicates() {
    let id = Ulid::default();

    let predicate_a = Predicate::And(vec![
        FieldRef::new("id").eq(id),
        FieldRef::new("other").eq(Value::Text("x".to_string())),
    ]);
    let predicate_b = Predicate::And(vec![
        FieldRef::new("other").eq(Value::Text("x".to_string())),
        FieldRef::new("id").eq(id),
    ]);

    let mut plan_a: AccessPlannedQuery = full_scan_query();
    plan_a.scalar_plan_mut().predicate = Some(predicate_a);

    let mut plan_b: AccessPlannedQuery = full_scan_query();
    plan_b.scalar_plan_mut().predicate = Some(predicate_b);

    assert_eq!(plan_a.fingerprint(), plan_b.fingerprint());
}

#[test]
fn fingerprint_and_signature_are_stable_for_reordered_and_non_canonical_map_predicates() {
    let map_a = Value::Map(vec![
        (Value::Text("z".to_string()), Value::Int(9)),
        (Value::Text("a".to_string()), Value::Int(1)),
    ]);
    let map_b = Value::Map(vec![
        (Value::Text("a".to_string()), Value::Int(1)),
        (Value::Text("z".to_string()), Value::Int(9)),
    ]);

    let predicate_a = Predicate::And(vec![
        FieldRef::new("other").eq(Value::Text("x".to_string())),
        Predicate::Compare(ComparePredicate::eq("meta".to_string(), map_a)),
    ]);
    let predicate_b = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::eq("meta".to_string(), map_b)),
        FieldRef::new("other").eq(Value::Text("x".to_string())),
    ]);

    let mut plan_a: AccessPlannedQuery = full_scan_query();
    plan_a.scalar_plan_mut().predicate = Some(predicate_a);

    let mut plan_b: AccessPlannedQuery = full_scan_query();
    plan_b.scalar_plan_mut().predicate = Some(predicate_b);

    assert_eq!(plan_a.fingerprint(), plan_b.fingerprint());
    assert_eq!(
        plan_a.continuation_signature("tests::Entity"),
        plan_b.continuation_signature("tests::Entity")
    );
}

#[test]
fn fingerprint_and_signature_treat_equivalent_decimal_predicate_literals_as_identical() {
    let predicate_a = Predicate::Compare(ComparePredicate::eq(
        "rank".to_string(),
        Value::Decimal(Decimal::new(10, 1)),
    ));
    let predicate_b = Predicate::Compare(ComparePredicate::eq(
        "rank".to_string(),
        Value::Decimal(Decimal::new(100, 2)),
    ));

    let mut plan_a: AccessPlannedQuery = full_scan_query();
    plan_a.scalar_plan_mut().predicate = Some(predicate_a);

    let mut plan_b: AccessPlannedQuery = full_scan_query();
    plan_b.scalar_plan_mut().predicate = Some(predicate_b);

    assert_eq!(plan_a.fingerprint(), plan_b.fingerprint());
    assert_eq!(
        plan_a.continuation_signature("tests::Entity"),
        plan_b.continuation_signature("tests::Entity")
    );
}

#[test]
fn fingerprint_and_signature_treat_equivalent_in_list_predicates_as_identical() {
    let predicate_a = Predicate::Compare(ComparePredicate::in_(
        "rank".to_string(),
        vec![Value::Uint(3), Value::Uint(1), Value::Uint(2)],
    ));
    let predicate_b = Predicate::Compare(ComparePredicate::in_(
        "rank".to_string(),
        vec![Value::Uint(1), Value::Uint(2), Value::Uint(3)],
    ));

    let mut plan_a: AccessPlannedQuery = full_scan_query();
    plan_a.scalar_plan_mut().predicate = Some(predicate_a);

    let mut plan_b: AccessPlannedQuery = full_scan_query();
    plan_b.scalar_plan_mut().predicate = Some(predicate_b);

    assert_eq!(plan_a.fingerprint(), plan_b.fingerprint());
    assert_eq!(
        plan_a.continuation_signature("tests::Entity"),
        plan_b.continuation_signature("tests::Entity")
    );
}

#[test]
fn fingerprint_and_signature_treat_same_field_or_eq_and_in_as_identical() {
    let predicate_or_eq = Predicate::Or(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::Eq,
            Value::Uint(3),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::Eq,
            Value::Uint(1),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::Eq,
            Value::Uint(3),
            CoercionId::Strict,
        )),
    ]);
    let predicate_in = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::In,
        Value::List(vec![Value::Uint(1), Value::Uint(3)]),
        CoercionId::Strict,
    ));

    let mut plan_or_eq: AccessPlannedQuery = full_scan_query();
    plan_or_eq.scalar_plan_mut().predicate = Some(predicate_or_eq);

    let mut plan_in: AccessPlannedQuery = full_scan_query();
    plan_in.scalar_plan_mut().predicate = Some(predicate_in);

    assert_eq!(plan_or_eq.fingerprint(), plan_in.fingerprint());
    assert_eq!(
        plan_or_eq.continuation_signature("tests::Entity"),
        plan_in.continuation_signature("tests::Entity")
    );
}

#[test]
fn fingerprint_and_signature_treat_equivalent_in_list_duplicate_literals_as_identical() {
    let predicate_a = Predicate::Compare(ComparePredicate::in_(
        "rank".to_string(),
        vec![
            Value::Uint(3),
            Value::Uint(1),
            Value::Uint(3),
            Value::Uint(2),
        ],
    ));
    let predicate_b = Predicate::Compare(ComparePredicate::in_(
        "rank".to_string(),
        vec![Value::Uint(1), Value::Uint(2), Value::Uint(3)],
    ));

    let mut plan_a: AccessPlannedQuery = full_scan_query();
    plan_a.scalar_plan_mut().predicate = Some(predicate_a);

    let mut plan_b: AccessPlannedQuery = full_scan_query();
    plan_b.scalar_plan_mut().predicate = Some(predicate_b);

    assert_eq!(plan_a.fingerprint(), plan_b.fingerprint());
    assert_eq!(
        plan_a.continuation_signature("tests::Entity"),
        plan_b.continuation_signature("tests::Entity")
    );
}

#[test]
fn fingerprint_and_signature_treat_implicit_and_explicit_strict_coercion_as_identical() {
    let predicate_a = Predicate::Compare(ComparePredicate::eq("rank".to_string(), Value::Int(7)));
    let predicate_b = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Eq,
        Value::Int(7),
        CoercionId::Strict,
    ));

    let mut plan_a: AccessPlannedQuery = full_scan_query();
    plan_a.scalar_plan_mut().predicate = Some(predicate_a);

    let mut plan_b: AccessPlannedQuery = full_scan_query();
    plan_b.scalar_plan_mut().predicate = Some(predicate_b);

    assert_eq!(plan_a.fingerprint(), plan_b.fingerprint());
    assert_eq!(
        plan_a.continuation_signature("tests::Entity"),
        plan_b.continuation_signature("tests::Entity")
    );
}

#[test]
fn fingerprint_and_signature_distinguish_different_coercion_ids() {
    let predicate_strict = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Eq,
        Value::Int(7),
        CoercionId::Strict,
    ));
    let predicate_numeric_widen = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Eq,
        Value::Int(7),
        CoercionId::NumericWiden,
    ));

    let mut strict_plan: AccessPlannedQuery = full_scan_query();
    strict_plan.scalar_plan_mut().predicate = Some(predicate_strict);

    let mut numeric_widen_plan: AccessPlannedQuery = full_scan_query();
    numeric_widen_plan.scalar_plan_mut().predicate = Some(predicate_numeric_widen);

    assert_ne!(strict_plan.fingerprint(), numeric_widen_plan.fingerprint());
    assert_ne!(
        strict_plan.continuation_signature("tests::Entity"),
        numeric_widen_plan.continuation_signature("tests::Entity")
    );
}

#[test]
fn fingerprint_and_signature_treat_numeric_widen_equivalent_literal_subtypes_as_identical() {
    let predicate_int = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Eq,
        Value::Int(1),
        CoercionId::NumericWiden,
    ));
    let predicate_decimal = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Eq,
        Value::Decimal(Decimal::new(10, 1)),
        CoercionId::NumericWiden,
    ));

    let mut int_plan: AccessPlannedQuery = full_scan_query();
    int_plan.scalar_plan_mut().predicate = Some(predicate_int);

    let mut decimal_plan: AccessPlannedQuery = full_scan_query();
    decimal_plan.scalar_plan_mut().predicate = Some(predicate_decimal);

    assert_eq!(int_plan.fingerprint(), decimal_plan.fingerprint());
    assert_eq!(
        int_plan.continuation_signature("tests::Entity"),
        decimal_plan.continuation_signature("tests::Entity")
    );
}

#[test]
fn fingerprint_and_signature_treat_text_casefold_case_only_literals_as_identical() {
    let predicate_lower = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::Eq,
        Value::Text("ada".to_string()),
        CoercionId::TextCasefold,
    ));
    let predicate_upper = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::Eq,
        Value::Text("ADA".to_string()),
        CoercionId::TextCasefold,
    ));

    let mut lower_plan: AccessPlannedQuery = full_scan_query();
    lower_plan.scalar_plan_mut().predicate = Some(predicate_lower);

    let mut upper_plan: AccessPlannedQuery = full_scan_query();
    upper_plan.scalar_plan_mut().predicate = Some(predicate_upper);

    assert_eq!(lower_plan.fingerprint(), upper_plan.fingerprint());
    assert_eq!(
        lower_plan.continuation_signature("tests::Entity"),
        upper_plan.continuation_signature("tests::Entity")
    );
}

#[test]
fn fingerprint_and_signature_keep_strict_text_case_variants_distinct() {
    let predicate_lower = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::Eq,
        Value::Text("ada".to_string()),
        CoercionId::Strict,
    ));
    let predicate_upper = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::Eq,
        Value::Text("ADA".to_string()),
        CoercionId::Strict,
    ));

    let mut lower_plan: AccessPlannedQuery = full_scan_query();
    lower_plan.scalar_plan_mut().predicate = Some(predicate_lower);

    let mut upper_plan: AccessPlannedQuery = full_scan_query();
    upper_plan.scalar_plan_mut().predicate = Some(predicate_upper);

    assert_ne!(lower_plan.fingerprint(), upper_plan.fingerprint());
    assert_ne!(
        lower_plan.continuation_signature("tests::Entity"),
        upper_plan.continuation_signature("tests::Entity")
    );
}

#[test]
fn fingerprint_and_signature_treat_text_casefold_in_list_case_variants_as_identical() {
    let predicate_mixed = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::In,
        Value::List(vec![
            Value::Text("ADA".to_string()),
            Value::Text("ada".to_string()),
            Value::Text("Bob".to_string()),
        ]),
        CoercionId::TextCasefold,
    ));
    let predicate_canonical = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::In,
        Value::List(vec![
            Value::Text("ada".to_string()),
            Value::Text("bob".to_string()),
        ]),
        CoercionId::TextCasefold,
    ));

    let mut mixed_plan: AccessPlannedQuery = full_scan_query();
    mixed_plan.scalar_plan_mut().predicate = Some(predicate_mixed);

    let mut canonical_plan: AccessPlannedQuery = full_scan_query();
    canonical_plan.scalar_plan_mut().predicate = Some(predicate_canonical);

    assert_eq!(mixed_plan.fingerprint(), canonical_plan.fingerprint());
    assert_eq!(
        mixed_plan.continuation_signature("tests::Entity"),
        canonical_plan.continuation_signature("tests::Entity")
    );
}

#[test]
fn fingerprint_and_signature_distinguish_strict_from_text_casefold_coercion() {
    let predicate_strict = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::Eq,
        Value::Text("ada".to_string()),
        CoercionId::Strict,
    ));
    let predicate_casefold = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::Eq,
        Value::Text("ada".to_string()),
        CoercionId::TextCasefold,
    ));

    let mut strict_plan: AccessPlannedQuery = full_scan_query();
    strict_plan.scalar_plan_mut().predicate = Some(predicate_strict);

    let mut casefold_plan: AccessPlannedQuery = full_scan_query();
    casefold_plan.scalar_plan_mut().predicate = Some(predicate_casefold);

    assert_ne!(strict_plan.fingerprint(), casefold_plan.fingerprint());
    assert_ne!(
        strict_plan.continuation_signature("tests::Entity"),
        casefold_plan.continuation_signature("tests::Entity")
    );
}

#[test]
fn fingerprint_and_signature_distinguish_strict_from_collection_element_coercion() {
    let predicate_strict = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Eq,
        Value::Int(7),
        CoercionId::Strict,
    ));
    let predicate_collection_element = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Eq,
        Value::Int(7),
        CoercionId::CollectionElement,
    ));

    let mut strict_plan: AccessPlannedQuery = full_scan_query();
    strict_plan.scalar_plan_mut().predicate = Some(predicate_strict);

    let mut collection_plan: AccessPlannedQuery = full_scan_query();
    collection_plan.scalar_plan_mut().predicate = Some(predicate_collection_element);

    assert_ne!(strict_plan.fingerprint(), collection_plan.fingerprint());
    assert_ne!(
        strict_plan.continuation_signature("tests::Entity"),
        collection_plan.continuation_signature("tests::Entity")
    );
}

#[test]
fn fingerprint_is_deterministic_for_by_keys() {
    let a = Ulid::from_u128(1);
    let b = Ulid::from_u128(2);

    let access_a = build_access_plan_from_keys(&KeyAccess::Many(vec![a, b, a]));
    let access_b = build_access_plan_from_keys(&KeyAccess::Many(vec![b, a]));

    let plan_a: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: access_a,
        projection_selection: ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
    };
    let plan_b: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: access_b,
        projection_selection: ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
    };

    assert_eq!(plan_a.fingerprint(), plan_b.fingerprint());
}

#[test]
fn fingerprint_changes_with_index_choice() {
    const INDEX_FIELDS: [&str; 1] = ["idx_a"];
    const INDEX_A: IndexModel = IndexModel::new(
        "fingerprint::idx_a",
        "fingerprint::store",
        &INDEX_FIELDS,
        false,
    );
    const INDEX_B: IndexModel = IndexModel::new(
        "fingerprint::idx_b",
        "fingerprint::store",
        &INDEX_FIELDS,
        false,
    );

    let plan_a: AccessPlannedQuery =
        index_prefix_query(INDEX_A, vec![Value::Text("alpha".to_string())]);
    let plan_b: AccessPlannedQuery =
        index_prefix_query(INDEX_B, vec![Value::Text("alpha".to_string())]);

    assert_ne!(plan_a.fingerprint(), plan_b.fingerprint());
}

#[test]
fn fingerprint_changes_with_pagination() {
    let mut plan_a: AccessPlannedQuery = full_scan_query();
    let mut plan_b: AccessPlannedQuery = full_scan_query();
    plan_a.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(10),
        offset: 0,
    });
    plan_b.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(10),
        offset: 1,
    });

    assert_ne!(plan_a.fingerprint(), plan_b.fingerprint());
}

#[test]
fn fingerprint_changes_with_delete_limit() {
    let mut plan_a: AccessPlannedQuery = full_scan_query();
    let mut plan_b: AccessPlannedQuery = full_scan_query();
    plan_a.scalar_plan_mut().mode = QueryMode::Delete(DeleteSpec::new());
    plan_b.scalar_plan_mut().mode = QueryMode::Delete(DeleteSpec::new());
    plan_a.scalar_plan_mut().delete_limit = Some(DeleteLimitSpec { max_rows: 2 });
    plan_b.scalar_plan_mut().delete_limit = Some(DeleteLimitSpec { max_rows: 3 });

    assert_ne!(plan_a.fingerprint(), plan_b.fingerprint());
}

#[test]
fn fingerprint_changes_with_distinct_flag() {
    let plan_a: AccessPlannedQuery = full_scan_query();
    let mut plan_b: AccessPlannedQuery = full_scan_query();
    plan_b.scalar_plan_mut().distinct = true;

    assert_ne!(plan_a.fingerprint(), plan_b.fingerprint());
}

#[test]
fn fingerprint_numeric_projection_alias_only_change_does_not_invalidate() {
    let plan: AccessPlannedQuery = full_scan_query();
    let numeric_projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: crate::db::query::plan::expr::BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Literal(Value::Int(1))),
        },
        alias: None,
    }]);
    let alias_only_numeric_projection =
        ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
            expr: Expr::Alias {
                expr: Box::new(Expr::Binary {
                    op: crate::db::query::plan::expr::BinaryOp::Add,
                    left: Box::new(Expr::Field(FieldId::new("rank"))),
                    right: Box::new(Expr::Literal(Value::Int(1))),
                }),
                name: Alias::new("rank_plus_one_expr"),
            },
            alias: Some(Alias::new("rank_plus_one")),
        }]);

    let semantic_fingerprint = fingerprint_with_projection(&plan, &numeric_projection);
    let alias_fingerprint = fingerprint_with_projection(&plan, &alias_only_numeric_projection);

    assert_eq!(
        semantic_fingerprint, alias_fingerprint,
        "numeric projection alias wrappers must not affect fingerprint identity",
    );
}

#[test]
fn fingerprint_numeric_projection_semantic_change_invalidates() {
    let plan: AccessPlannedQuery = full_scan_query();
    let projection_add_one = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: crate::db::query::plan::expr::BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Literal(Value::Int(1))),
        },
        alias: None,
    }]);
    let projection_mul_one = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: crate::db::query::plan::expr::BinaryOp::Mul,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Literal(Value::Int(1))),
        },
        alias: None,
    }]);

    let add_fingerprint = fingerprint_with_projection(&plan, &projection_add_one);
    let mul_fingerprint = fingerprint_with_projection(&plan, &projection_mul_one);

    assert_ne!(
        add_fingerprint, mul_fingerprint,
        "numeric projection semantic changes must invalidate fingerprint identity",
    );
}

#[test]
fn fingerprint_numeric_literal_decimal_scale_is_canonicalized() {
    let plan: AccessPlannedQuery = full_scan_query();
    let decimal_one_scale_1 = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Literal(Value::Decimal(Decimal::new(10, 1))),
        alias: None,
    }]);
    let decimal_one_scale_2 = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Literal(Value::Decimal(Decimal::new(100, 2))),
        alias: None,
    }]);

    assert_eq!(
        fingerprint_with_projection(&plan, &decimal_one_scale_1),
        fingerprint_with_projection(&plan, &decimal_one_scale_2),
        "decimal scale-only literal changes must not fragment fingerprint identity",
    );
}

#[test]
fn fingerprint_literal_numeric_subtype_remains_significant_when_observable() {
    let plan: AccessPlannedQuery = full_scan_query();
    let int_literal = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Literal(Value::Int(1)),
        alias: None,
    }]);
    let decimal_literal = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Literal(Value::Decimal(Decimal::new(10, 1))),
        alias: None,
    }]);

    assert_ne!(
        fingerprint_with_projection(&plan, &int_literal),
        fingerprint_with_projection(&plan, &decimal_literal),
        "top-level literal subtype remains observable and identity-significant",
    );
}

#[test]
fn fingerprint_numeric_promotion_paths_do_not_fragment() {
    let plan: AccessPlannedQuery = full_scan_query();
    let int_plus_int = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Literal(Value::Int(1))),
            right: Box::new(Expr::Literal(Value::Int(2))),
        },
        alias: None,
    }]);
    let int_plus_decimal = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Literal(Value::Int(1))),
            right: Box::new(Expr::Literal(Value::Decimal(Decimal::new(20, 1)))),
        },
        alias: None,
    }]);
    let decimal_plus_int = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Literal(Value::Decimal(Decimal::new(10, 1)))),
            right: Box::new(Expr::Literal(Value::Int(2))),
        },
        alias: None,
    }]);

    let fingerprint_int_plus_int = fingerprint_with_projection(&plan, &int_plus_int);
    let fingerprint_int_plus_decimal = fingerprint_with_projection(&plan, &int_plus_decimal);
    let fingerprint_decimal_plus_int = fingerprint_with_projection(&plan, &decimal_plus_int);

    assert_eq!(fingerprint_int_plus_int, fingerprint_int_plus_decimal);
    assert_eq!(fingerprint_int_plus_int, fingerprint_decimal_plus_int);
}

#[test]
fn fingerprint_commutative_operand_order_remains_significant_without_ast_normalization() {
    let plan: AccessPlannedQuery = full_scan_query();
    let rank_plus_score = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Field(FieldId::new("score"))),
        },
        alias: None,
    }]);
    let score_plus_rank = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("score"))),
            right: Box::new(Expr::Field(FieldId::new("rank"))),
        },
        alias: None,
    }]);

    assert_ne!(
        fingerprint_with_projection(&plan, &rank_plus_score),
        fingerprint_with_projection(&plan, &score_plus_rank),
        "fingerprint preserves AST operand order for commutative operators in v2",
    );
}

#[test]
fn fingerprint_aggregate_numeric_target_field_remains_significant() {
    let plan: AccessPlannedQuery = full_scan_query();
    let sum_rank = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Aggregate(sum("rank")),
        alias: None,
    }]);
    let sum_score = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Aggregate(sum("score")),
        alias: None,
    }]);

    assert_ne!(
        fingerprint_with_projection(&plan, &sum_rank),
        fingerprint_with_projection(&plan, &sum_score),
        "aggregate target field changes must invalidate fingerprint identity",
    );
}

#[test]
fn fingerprint_distinct_numeric_noop_paths_stay_stable() {
    let plan: AccessPlannedQuery = full_scan_query();
    let sum_distinct_plus_int_zero =
        ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
            expr: Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(Expr::Aggregate(sum("rank").distinct())),
                right: Box::new(Expr::Literal(Value::Int(0))),
            },
            alias: None,
        }]);
    let sum_distinct_plus_decimal_zero =
        ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
            expr: Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(Expr::Aggregate(sum("rank").distinct())),
                right: Box::new(Expr::Literal(Value::Decimal(Decimal::new(0, 1)))),
            },
            alias: None,
        }]);

    assert_eq!(
        fingerprint_with_projection(&plan, &sum_distinct_plus_int_zero),
        fingerprint_with_projection(&plan, &sum_distinct_plus_decimal_zero),
        "distinct numeric no-op literal subtype differences must not fragment fingerprint identity",
    );
}

#[test]
fn fingerprint_is_stable_for_full_scan() {
    let plan: AccessPlannedQuery = full_scan_query();
    let fingerprint_a = plan.fingerprint();
    let fingerprint_b = plan.fingerprint();
    assert_eq!(fingerprint_a, fingerprint_b);
}

#[test]
fn fingerprint_is_stable_for_equivalent_index_range_bounds() {
    const INDEX_FIELDS: [&str; 2] = ["group", "rank"];
    const INDEX: IndexModel = IndexModel::new(
        "fingerprint::group_rank",
        "fingerprint::store",
        &INDEX_FIELDS,
        false,
    );

    let plan_a: AccessPlannedQuery = index_range_query(
        INDEX,
        vec![Value::Uint(7)],
        Bound::Included(Value::Uint(100)),
        Bound::Excluded(Value::Uint(200)),
    );
    let plan_b: AccessPlannedQuery = index_range_query(
        INDEX,
        vec![Value::Uint(7)],
        Bound::Included(Value::Uint(100)),
        Bound::Excluded(Value::Uint(200)),
    );

    assert_eq!(plan_a.fingerprint(), plan_b.fingerprint());
}

#[test]
fn fingerprint_changes_when_index_range_bound_discriminant_changes() {
    const INDEX_FIELDS: [&str; 2] = ["group", "rank"];
    const INDEX: IndexModel = IndexModel::new(
        "fingerprint::group_rank",
        "fingerprint::store",
        &INDEX_FIELDS,
        false,
    );

    let plan_included: AccessPlannedQuery = index_range_query(
        INDEX,
        vec![Value::Uint(7)],
        Bound::Included(Value::Uint(100)),
        Bound::Excluded(Value::Uint(200)),
    );
    let plan_excluded: AccessPlannedQuery = index_range_query(
        INDEX,
        vec![Value::Uint(7)],
        Bound::Excluded(Value::Uint(100)),
        Bound::Excluded(Value::Uint(200)),
    );

    assert_ne!(plan_included.fingerprint(), plan_excluded.fingerprint());
}

#[test]
fn fingerprint_changes_when_index_range_bound_value_changes() {
    const INDEX_FIELDS: [&str; 2] = ["group", "rank"];
    const INDEX: IndexModel = IndexModel::new(
        "fingerprint::group_rank",
        "fingerprint::store",
        &INDEX_FIELDS,
        false,
    );

    let plan_low_100: AccessPlannedQuery = index_range_query(
        INDEX,
        vec![Value::Uint(7)],
        Bound::Included(Value::Uint(100)),
        Bound::Excluded(Value::Uint(200)),
    );
    let plan_low_101: AccessPlannedQuery = index_range_query(
        INDEX,
        vec![Value::Uint(7)],
        Bound::Included(Value::Uint(101)),
        Bound::Excluded(Value::Uint(200)),
    );

    assert_ne!(plan_low_100.fingerprint(), plan_low_101.fingerprint());
}

#[test]
fn explain_fingerprint_grouped_strategy_only_change_does_not_invalidate() {
    let mut hash_strategy = grouped_explain_with_fixed_shape();
    let mut ordered_strategy = hash_strategy.clone();

    let ExplainGrouping::Grouped {
        strategy: hash_value,
        ..
    } = &mut hash_strategy.grouping
    else {
        panic!("grouped explain fixture must produce grouped explain shape");
    };
    *hash_value = ExplainGroupedStrategy::HashGroup;
    let ExplainGrouping::Grouped {
        strategy: ordered_value,
        ..
    } = &mut ordered_strategy.grouping
    else {
        panic!("grouped explain fixture must produce grouped explain shape");
    };
    *ordered_value = ExplainGroupedStrategy::OrderedGroup;

    assert_eq!(
        hash_strategy.fingerprint(),
        ordered_strategy.fingerprint(),
        "execution strategy hints are explain/runtime metadata and must not affect semantic fingerprint identity",
    );
}

#[test]
fn grouped_fingerprint_identity_projection_remains_stable() {
    let plan = grouped_query_with_fixed_shape();
    let identity_projection = plan.projection_spec_for_identity();

    assert_eq!(
        plan.fingerprint().as_hex(),
        encode_cursor(&fingerprint_with_projection(&plan, &identity_projection)),
        "grouped fingerprint identity must stay stable across plan-owned and explain-owned grouped projection seams",
    );
}
