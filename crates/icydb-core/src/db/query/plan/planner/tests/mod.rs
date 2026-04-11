//! Module: db::query::plan::planner::tests
//! Covers planner access-choice and deterministic planning behavior.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use super::*;
use crate::{
    db::{
        access::{AccessPath, SemanticIndexRangeSpec, normalize_access_plan_value},
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate, normalize},
        query::{
            intent::{KeyAccess, build_access_plan_from_keys},
            plan::{OrderDirection, OrderSpec},
        },
    },
    model::{
        field::{FieldKind, FieldModel},
        index::{IndexExpression, IndexKeyItem, IndexModel, IndexPredicateMetadata},
    },
    testing::entity_model_from_static,
    types::Ulid,
};
use std::{ops::Bound, sync::LazyLock};

static ACTIVE_TRUE_PREDICATE: LazyLock<Predicate> =
    LazyLock::new(|| Predicate::eq("active".to_string(), true.into()));

fn active_true_predicate() -> &'static Predicate {
    &ACTIVE_TRUE_PREDICATE
}

const fn active_true_predicate_metadata() -> IndexPredicateMetadata {
    IndexPredicateMetadata::generated("active = true", active_true_predicate)
}

static PLANNER_CANONICAL_FIELDS: [FieldModel; 1] = [FieldModel::generated("id", FieldKind::Ulid)];
static PLANNER_CANONICAL_INDEXES: [&IndexModel; 0] = [];
static PLANNER_CANONICAL_MODEL: EntityModel = entity_model_from_static(
    "planner::canonical_test_entity",
    "PlannerCanonicalTestEntity",
    &PLANNER_CANONICAL_FIELDS[0],
    0,
    &PLANNER_CANONICAL_FIELDS,
    &PLANNER_CANONICAL_INDEXES,
);

static PLANNER_IN_EMPTY_FIELDS: [FieldModel; 2] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated("email", FieldKind::Text),
];
static PLANNER_IN_EMPTY_INDEX_FIELDS: [&str; 1] = ["email"];
static PLANNER_IN_EMPTY_INDEXES: [IndexModel; 1] = [IndexModel::generated(
    "email_idx",
    "planner::in_empty_test_entity",
    &PLANNER_IN_EMPTY_INDEX_FIELDS,
    false,
)];
static PLANNER_IN_EMPTY_INDEX_REFS: [&IndexModel; 1] = [&PLANNER_IN_EMPTY_INDEXES[0]];
static PLANNER_IN_EMPTY_MODEL: EntityModel = entity_model_from_static(
    "planner::in_empty_test_entity",
    "PlannerInEmptyTestEntity",
    &PLANNER_IN_EMPTY_FIELDS[0],
    0,
    &PLANNER_IN_EMPTY_FIELDS,
    &PLANNER_IN_EMPTY_INDEX_REFS,
);
static PLANNER_ORDER_FIELDS: [FieldModel; 3] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated("name", FieldKind::Text),
    FieldModel::generated("active", FieldKind::Bool),
];
static PLANNER_ORDER_INDEX_FIELDS: [&str; 1] = ["name"];
static PLANNER_ORDER_INDEXES: [IndexModel; 1] = [IndexModel::generated(
    "name_idx",
    "planner::order_test_entity",
    &PLANNER_ORDER_INDEX_FIELDS,
    false,
)];
static PLANNER_ORDER_INDEX_REFS: [&IndexModel; 1] = [&PLANNER_ORDER_INDEXES[0]];
static PLANNER_ORDER_MODEL: EntityModel = entity_model_from_static(
    "planner::order_test_entity",
    "PlannerOrderTestEntity",
    &PLANNER_ORDER_FIELDS[0],
    0,
    &PLANNER_ORDER_FIELDS,
    &PLANNER_ORDER_INDEX_REFS,
);
static PLANNER_ORDER_FILTERED_COMPOSITE_FIELDS: [FieldModel; 5] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated("name", FieldKind::Text),
    FieldModel::generated("active", FieldKind::Bool),
    FieldModel::generated("tier", FieldKind::Text),
    FieldModel::generated("handle", FieldKind::Text),
];
static PLANNER_ORDER_FILTERED_COMPOSITE_INDEX_FIELDS: [&str; 2] = ["tier", "handle"];
static PLANNER_ORDER_FILTERED_COMPOSITE_INDEXES: [IndexModel; 1] =
    [IndexModel::generated_with_predicate(
        "tier_handle_idx_active_only",
        "planner::order_filtered_composite_test_entity",
        &PLANNER_ORDER_FILTERED_COMPOSITE_INDEX_FIELDS,
        false,
        Some(active_true_predicate_metadata()),
    )];
static PLANNER_ORDER_FILTERED_COMPOSITE_INDEX_REFS: [&IndexModel; 1] =
    [&PLANNER_ORDER_FILTERED_COMPOSITE_INDEXES[0]];
static PLANNER_ORDER_FILTERED_COMPOSITE_MODEL: EntityModel = entity_model_from_static(
    "planner::order_filtered_composite_test_entity",
    "PlannerOrderFilteredCompositeTestEntity",
    &PLANNER_ORDER_FILTERED_COMPOSITE_FIELDS[0],
    0,
    &PLANNER_ORDER_FILTERED_COMPOSITE_FIELDS,
    &PLANNER_ORDER_FILTERED_COMPOSITE_INDEX_REFS,
);
static PLANNER_ORDER_COMPOSITE_FIELDS: [FieldModel; 4] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated("code", FieldKind::Text),
    FieldModel::generated("serial", FieldKind::Uint),
    FieldModel::generated("note", FieldKind::Text),
];
static PLANNER_ORDER_COMPOSITE_INDEX_FIELDS: [&str; 2] = ["code", "serial"];
static PLANNER_ORDER_COMPOSITE_INDEXES: [IndexModel; 1] = [IndexModel::generated(
    "code_serial_idx",
    "planner::order_composite_test_entity",
    &PLANNER_ORDER_COMPOSITE_INDEX_FIELDS,
    false,
)];
static PLANNER_ORDER_COMPOSITE_INDEX_REFS: [&IndexModel; 1] = [&PLANNER_ORDER_COMPOSITE_INDEXES[0]];
static PLANNER_ORDER_COMPOSITE_MODEL: EntityModel = entity_model_from_static(
    "planner::order_composite_test_entity",
    "PlannerOrderCompositeTestEntity",
    &PLANNER_ORDER_COMPOSITE_FIELDS[0],
    0,
    &PLANNER_ORDER_COMPOSITE_FIELDS,
    &PLANNER_ORDER_COMPOSITE_INDEX_REFS,
);
static PLANNER_ORDER_FILTERED_COMPOSITE_EXPRESSION_KEY_ITEMS: [IndexKeyItem; 2] = [
    IndexKeyItem::Field("tier"),
    IndexKeyItem::Expression(IndexExpression::Lower("handle")),
];
static PLANNER_ORDER_FILTERED_COMPOSITE_EXPRESSION_INDEXES: [IndexModel; 1] =
    [IndexModel::generated_with_key_items_and_predicate(
        "tier_handle_lower_idx_active_only",
        "planner::order_filtered_composite_expression_test_entity",
        &PLANNER_ORDER_FILTERED_COMPOSITE_INDEX_FIELDS,
        Some(&PLANNER_ORDER_FILTERED_COMPOSITE_EXPRESSION_KEY_ITEMS),
        false,
        Some(active_true_predicate_metadata()),
    )];
static PLANNER_ORDER_FILTERED_COMPOSITE_EXPRESSION_INDEX_REFS: [&IndexModel; 1] =
    [&PLANNER_ORDER_FILTERED_COMPOSITE_EXPRESSION_INDEXES[0]];
static PLANNER_ORDER_FILTERED_COMPOSITE_EXPRESSION_MODEL: EntityModel = entity_model_from_static(
    "planner::order_filtered_composite_expression_test_entity",
    "PlannerOrderFilteredCompositeExpressionTestEntity",
    &PLANNER_ORDER_FILTERED_COMPOSITE_FIELDS[0],
    0,
    &PLANNER_ORDER_FILTERED_COMPOSITE_FIELDS,
    &PLANNER_ORDER_FILTERED_COMPOSITE_EXPRESSION_INDEX_REFS,
);
static PLANNER_RANKING_FIELDS: [FieldModel; 4] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated("tier", FieldKind::Text),
    FieldModel::generated("handle", FieldKind::Text),
    FieldModel::generated("label", FieldKind::Text),
];
static PLANNER_RANKING_LABEL_INDEX_FIELDS: [&str; 2] = ["tier", "label"];
static PLANNER_RANKING_HANDLE_INDEX_FIELDS: [&str; 2] = ["tier", "handle"];
static PLANNER_RANKING_INDEXES: [IndexModel; 2] = [
    IndexModel::generated(
        "a_tier_label_idx",
        "planner::ranking_test_entity",
        &PLANNER_RANKING_LABEL_INDEX_FIELDS,
        false,
    ),
    IndexModel::generated(
        "z_tier_handle_idx",
        "planner::ranking_test_entity",
        &PLANNER_RANKING_HANDLE_INDEX_FIELDS,
        false,
    ),
];
static PLANNER_RANKING_INDEX_REFS: [&IndexModel; 2] =
    [&PLANNER_RANKING_INDEXES[0], &PLANNER_RANKING_INDEXES[1]];
static PLANNER_RANKING_MODEL: EntityModel = entity_model_from_static(
    "planner::ranking_test_entity",
    "PlannerRankingTestEntity",
    &PLANNER_RANKING_FIELDS[0],
    0,
    &PLANNER_RANKING_FIELDS,
    &PLANNER_RANKING_INDEX_REFS,
);
static PLANNER_RANGE_RANKING_FIELDS: [FieldModel; 5] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated("tier", FieldKind::Text),
    FieldModel::generated("score", FieldKind::Uint),
    FieldModel::generated("handle", FieldKind::Text),
    FieldModel::generated("label", FieldKind::Text),
];
static PLANNER_RANGE_RANKING_HANDLE_INDEX_FIELDS: [&str; 3] = ["tier", "score", "handle"];
static PLANNER_RANGE_RANKING_LABEL_INDEX_FIELDS: [&str; 3] = ["tier", "score", "label"];
static PLANNER_RANGE_RANKING_INDEXES: [IndexModel; 2] = [
    IndexModel::generated(
        "a_tier_score_handle_idx",
        "planner::range_ranking_test_entity",
        &PLANNER_RANGE_RANKING_HANDLE_INDEX_FIELDS,
        false,
    ),
    IndexModel::generated(
        "z_tier_score_label_idx",
        "planner::range_ranking_test_entity",
        &PLANNER_RANGE_RANKING_LABEL_INDEX_FIELDS,
        false,
    ),
];
static PLANNER_RANGE_RANKING_INDEX_REFS: [&IndexModel; 2] = [
    &PLANNER_RANGE_RANKING_INDEXES[0],
    &PLANNER_RANGE_RANKING_INDEXES[1],
];
static PLANNER_RANGE_RANKING_MODEL: EntityModel = entity_model_from_static(
    "planner::range_ranking_test_entity",
    "PlannerRangeRankingTestEntity",
    &PLANNER_RANGE_RANKING_FIELDS[0],
    0,
    &PLANNER_RANGE_RANKING_FIELDS,
    &PLANNER_RANGE_RANKING_INDEX_REFS,
);
static PLANNER_ORDER_ONLY_RANKING_FIELDS: [FieldModel; 3] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated("alpha", FieldKind::Text),
    FieldModel::generated("beta", FieldKind::Text),
];
static PLANNER_ORDER_ONLY_RANKING_BETA_INDEX_FIELDS: [&str; 1] = ["beta"];
static PLANNER_ORDER_ONLY_RANKING_ALPHA_INDEX_FIELDS: [&str; 1] = ["alpha"];
static PLANNER_ORDER_ONLY_RANKING_INDEXES: [IndexModel; 2] = [
    IndexModel::generated(
        "a_beta_idx",
        "planner::order_only_ranking_test_entity",
        &PLANNER_ORDER_ONLY_RANKING_BETA_INDEX_FIELDS,
        false,
    ),
    IndexModel::generated(
        "z_alpha_idx",
        "planner::order_only_ranking_test_entity",
        &PLANNER_ORDER_ONLY_RANKING_ALPHA_INDEX_FIELDS,
        false,
    ),
];
static PLANNER_ORDER_ONLY_RANKING_INDEX_REFS: [&IndexModel; 2] = [
    &PLANNER_ORDER_ONLY_RANKING_INDEXES[0],
    &PLANNER_ORDER_ONLY_RANKING_INDEXES[1],
];
static PLANNER_ORDER_ONLY_RANKING_MODEL: EntityModel = entity_model_from_static(
    "planner::order_only_ranking_test_entity",
    "PlannerOrderOnlyRankingTestEntity",
    &PLANNER_ORDER_ONLY_RANKING_FIELDS[0],
    0,
    &PLANNER_ORDER_ONLY_RANKING_FIELDS,
    &PLANNER_ORDER_ONLY_RANKING_INDEX_REFS,
);

fn plan_access_for_test(
    model: &EntityModel,
    schema: &SchemaInfo,
    predicate: Option<&Predicate>,
) -> Result<AccessPlan<Value>, PlannerError> {
    let normalized = predicate.map(normalize);

    plan_access(model, model.indexes(), schema, normalized.as_ref())
}

fn plan_access_for_test_with_order(
    model: &EntityModel,
    schema: &SchemaInfo,
    predicate: Option<&Predicate>,
    order: Option<OrderSpec>,
) -> Result<AccessPlan<Value>, PlannerError> {
    let normalized = predicate.map(normalize);

    plan_access_with_order(
        model,
        model.indexes(),
        schema,
        normalized.as_ref(),
        order.as_ref(),
    )
}

fn canonical_order(fields: &[(&str, OrderDirection)]) -> OrderSpec {
    OrderSpec {
        fields: fields
            .iter()
            .map(|(field, direction)| ((*field).to_string(), *direction))
            .collect(),
    }
}

fn assert_order_only_fallback_index_range(
    model: &EntityModel,
    order: &[(&str, OrderDirection)],
    expected_index: &IndexModel,
    context: &str,
) {
    let schema = SchemaInfo::cached_for_entity_model(model);
    let planner_shape =
        plan_access_for_test_with_order(model, schema, None, Some(canonical_order(order)))
            .unwrap_or_else(|err| panic!("{context} should succeed: {err}"));

    assert_eq!(
        planner_shape,
        AccessPlan::index_range(SemanticIndexRangeSpec::new(
            *expected_index,
            vec![0usize],
            Vec::new(),
            Bound::Unbounded,
            Bound::Unbounded,
        )),
        "{context} should fall back to one whole-index range",
    );
}

fn assert_order_compatible_prefix_choice(
    model: &EntityModel,
    predicate: &Predicate,
    order: &[(&str, OrderDirection)],
    expected_index_name: &str,
    expected_values: &[Value],
    context: &str,
) {
    let schema = SchemaInfo::cached_for_entity_model(model);
    let planner_shape = plan_access_for_test_with_order(
        model,
        schema,
        Some(predicate),
        Some(canonical_order(order)),
    )
    .unwrap_or_else(|err| panic!("{context} should succeed: {err}"));

    let AccessPlan::Path(path) = planner_shape else {
        panic!("{context} should lower to one index-prefix path");
    };
    let AccessPath::IndexPrefix { index, values } = path.as_ref() else {
        panic!("{context} should lower to one index-prefix path");
    };

    assert_eq!(
        index.name(),
        expected_index_name,
        "{context} should keep the order-compatible index when rank ties",
    );
    assert_eq!(
        values, expected_values,
        "{context} should preserve the canonical equality prefix on the selected route",
    );
}

// This assertion helper stays flat so the order/range expectation matrix stays
// readable at each call site without introducing another test-only wrapper type.
#[expect(
    clippy::too_many_arguments,
    reason = "table-driven planner tests keep the expectation shape explicit at call sites"
)]
fn assert_order_compatible_range_choice(
    model: &EntityModel,
    predicate: &Predicate,
    order: &[(&str, OrderDirection)],
    expected_index_name: &str,
    expected_prefix_values: &[Value],
    expected_lower: Bound<Value>,
    expected_upper: Bound<Value>,
    context: &str,
) {
    let schema = SchemaInfo::cached_for_entity_model(model);
    let planner_shape = plan_access_for_test_with_order(
        model,
        schema,
        Some(predicate),
        Some(canonical_order(order)),
    )
    .unwrap_or_else(|err| panic!("{context} should succeed: {err}"));

    let AccessPlan::Path(path) = planner_shape else {
        panic!("{context} should lower to one index range");
    };
    let AccessPath::IndexRange { spec } = path.as_ref() else {
        panic!("{context} should lower to one index range");
    };

    assert_eq!(
        spec.index().name(),
        expected_index_name,
        "{context} should keep the order-compatible range index when rank ties",
    );
    assert_eq!(
        spec.prefix_values(),
        expected_prefix_values,
        "{context} should preserve the equality-bound prefix on the selected index range",
    );
    assert_eq!(spec.lower(), &expected_lower);
    assert_eq!(spec.upper(), &expected_upper);
}

fn assert_order_compatible_order_only_choice(
    model: &EntityModel,
    order: &[(&str, OrderDirection)],
    expected_index_name: &str,
    context: &str,
) {
    let schema = SchemaInfo::cached_for_entity_model(model);
    let planner_shape =
        plan_access_for_test_with_order(model, schema, None, Some(canonical_order(order)))
            .unwrap_or_else(|err| panic!("{context} should succeed: {err}"));

    let AccessPlan::Path(path) = planner_shape else {
        panic!("{context} should lower to one index range");
    };
    let AccessPath::IndexRange { spec } = path.as_ref() else {
        panic!("{context} should lower to one index range");
    };

    assert_eq!(
        spec.index().name(),
        expected_index_name,
        "{context} should keep the order-compatible fallback index when rank ties",
    );
    assert!(spec.prefix_values().is_empty());
    assert_eq!(spec.lower(), &Bound::Unbounded);
    assert_eq!(spec.upper(), &Bound::Unbounded);
}

#[test]
fn normalize_union_dedups_identical_paths() {
    let key = Value::Ulid(Ulid::from_u128(1));
    let plan = AccessPlan::Union(vec![
        AccessPlan::by_key(key.clone()),
        AccessPlan::by_key(key),
    ]);

    let normalized = normalize_access_plan_value(plan);

    assert_eq!(
        normalized,
        AccessPlan::by_key(Value::Ulid(Ulid::from_u128(1)))
    );
}

#[test]
fn normalize_union_sorts_by_key() {
    let a = Value::Ulid(Ulid::from_u128(1));
    let b = Value::Ulid(Ulid::from_u128(2));
    let plan = AccessPlan::Union(vec![
        AccessPlan::by_key(b.clone()),
        AccessPlan::by_key(a.clone()),
    ]);

    let normalized = normalize_access_plan_value(plan);
    let AccessPlan::Union(children) = normalized else {
        panic!("expected union");
    };

    assert_eq!(children.len(), 2);
    assert_eq!(children[0], AccessPlan::by_key(a));
    assert_eq!(children[1], AccessPlan::by_key(b));
}

#[test]
fn normalize_intersection_removes_full_scan() {
    let key = Value::Ulid(Ulid::from_u128(7));
    let plan = AccessPlan::Intersection(vec![AccessPlan::full_scan(), AccessPlan::by_key(key)]);

    let normalized = normalize_access_plan_value(plan);

    assert_eq!(
        normalized,
        AccessPlan::by_key(Value::Ulid(Ulid::from_u128(7)))
    );
}

#[test]
fn planner_and_intent_access_canonicalization_match_for_single_key_set() {
    let key = Ulid::from_u128(42);
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "id",
        CompareOp::In,
        Value::List(vec![Value::Ulid(key)]),
        CoercionId::Strict,
    ));
    let schema = SchemaInfo::cached_for_entity_model(&PLANNER_CANONICAL_MODEL);

    let planner_shape = plan_access_for_test(&PLANNER_CANONICAL_MODEL, schema, Some(&predicate))
        .expect("planner access shape should build for strict single-key IN predicate");
    let intent_shape = build_access_plan_from_keys(&KeyAccess::Many(vec![key]));

    assert_eq!(
        planner_shape, intent_shape,
        "planner and intent canonical access shape should agree for one-key sets",
    );
    assert_eq!(
        planner_shape,
        AccessPlan::by_key(Value::Ulid(key)),
        "one-key set canonicalization should collapse to ByKey",
    );
}

#[test]
fn planner_non_pk_in_empty_lowers_to_empty_by_keys() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "email",
        CompareOp::In,
        Value::List(Vec::new()),
        CoercionId::Strict,
    ));
    let schema = SchemaInfo::cached_for_entity_model(&PLANNER_IN_EMPTY_MODEL);

    let planner_shape = plan_access_for_test(&PLANNER_IN_EMPTY_MODEL, schema, Some(&predicate))
        .expect("planner access shape should build for strict IN-empty predicate");

    assert_eq!(
        planner_shape,
        AccessPlan::by_keys(Vec::new()),
        "IN-empty predicates must lower to an empty access shape instead of full scan",
    );
}

#[test]
fn planner_order_only_single_field_index_falls_back_to_index_range() {
    assert_order_only_fallback_index_range(
        &PLANNER_ORDER_MODEL,
        &[("name", OrderDirection::Asc), ("id", OrderDirection::Asc)],
        &PLANNER_ORDER_INDEXES[0],
        "canonical order-only secondary scans",
    );
}

#[test]
fn planner_order_only_single_field_desc_index_falls_back_to_index_range() {
    assert_order_only_fallback_index_range(
        &PLANNER_ORDER_MODEL,
        &[("name", OrderDirection::Desc), ("id", OrderDirection::Desc)],
        &PLANNER_ORDER_INDEXES[0],
        "canonical descending order-only secondary scans",
    );
}

#[test]
fn planner_order_only_composite_index_falls_back_to_index_range() {
    assert_order_only_fallback_index_range(
        &PLANNER_ORDER_COMPOSITE_MODEL,
        &[
            ("code", OrderDirection::Asc),
            ("serial", OrderDirection::Asc),
            ("id", OrderDirection::Asc),
        ],
        &PLANNER_ORDER_COMPOSITE_INDEXES[0],
        "canonical composite order-only scans",
    );
}

#[test]
fn planner_order_only_composite_desc_index_falls_back_to_index_range() {
    assert_order_only_fallback_index_range(
        &PLANNER_ORDER_COMPOSITE_MODEL,
        &[
            ("code", OrderDirection::Desc),
            ("serial", OrderDirection::Desc),
            ("id", OrderDirection::Desc),
        ],
        &PLANNER_ORDER_COMPOSITE_INDEXES[0],
        "canonical descending composite order-only scans",
    );
}

#[test]
fn planner_prefix_selection_prefers_order_compatible_index_over_name_order_tie() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "tier",
        CompareOp::Eq,
        Value::Text("gold".to_string()),
        CoercionId::Strict,
    ));
    assert_order_compatible_prefix_choice(
        &PLANNER_RANKING_MODEL,
        &predicate,
        &[("handle", OrderDirection::Asc), ("id", OrderDirection::Asc)],
        "z_tier_handle_idx",
        &[Value::Text("gold".to_string())],
        "ranking test prefix predicate",
    );
}

#[test]
fn planner_range_selection_prefers_order_compatible_index_over_name_order_tie() {
    let predicate = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "tier",
            CompareOp::Eq,
            Value::Text("gold".to_string()),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "score",
            CompareOp::Gt,
            Value::Uint(10),
            CoercionId::Strict,
        )),
    ]);
    assert_order_compatible_range_choice(
        &PLANNER_RANGE_RANKING_MODEL,
        &predicate,
        &[
            ("score", OrderDirection::Asc),
            ("label", OrderDirection::Asc),
            ("id", OrderDirection::Asc),
        ],
        "z_tier_score_label_idx",
        &[Value::Text("gold".to_string())],
        Bound::Excluded(Value::Uint(10)),
        Bound::Unbounded,
        "range ranking predicate",
    );
}

#[test]
fn planner_range_selection_desc_prefers_order_compatible_index_over_name_order_tie() {
    let predicate = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "tier",
            CompareOp::Eq,
            Value::Text("gold".to_string()),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "score",
            CompareOp::Gt,
            Value::Uint(10),
            CoercionId::Strict,
        )),
    ]);
    assert_order_compatible_range_choice(
        &PLANNER_RANGE_RANKING_MODEL,
        &predicate,
        &[
            ("score", OrderDirection::Desc),
            ("label", OrderDirection::Desc),
            ("id", OrderDirection::Desc),
        ],
        "z_tier_score_label_idx",
        &[Value::Text("gold".to_string())],
        Bound::Excluded(Value::Uint(10)),
        Bound::Unbounded,
        "descending range ranking predicate",
    );
}

#[test]
fn planner_equality_prefix_suffix_order_prefers_order_compatible_index_over_name_order_tie() {
    let predicate = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "tier",
            CompareOp::Eq,
            Value::Text("gold".to_string()),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "score",
            CompareOp::Eq,
            Value::Uint(20),
            CoercionId::Strict,
        )),
    ]);
    assert_order_compatible_prefix_choice(
        &PLANNER_RANGE_RANKING_MODEL,
        &predicate,
        &[("label", OrderDirection::Asc), ("id", OrderDirection::Asc)],
        "z_tier_score_label_idx",
        &[Value::Text("gold".to_string()), Value::Uint(20)],
        "equality-prefix suffix-order predicate",
    );
}

#[test]
fn planner_equality_prefix_suffix_order_desc_prefers_order_compatible_index_over_name_order_tie() {
    let predicate = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "tier",
            CompareOp::Eq,
            Value::Text("gold".to_string()),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "score",
            CompareOp::Eq,
            Value::Uint(20),
            CoercionId::Strict,
        )),
    ]);
    assert_order_compatible_prefix_choice(
        &PLANNER_RANGE_RANKING_MODEL,
        &predicate,
        &[
            ("label", OrderDirection::Desc),
            ("id", OrderDirection::Desc),
        ],
        "z_tier_score_label_idx",
        &[Value::Text("gold".to_string()), Value::Uint(20)],
        "descending equality-prefix suffix-order predicate",
    );
}

#[test]
fn planner_order_only_selection_prefers_order_compatible_index_over_name_order_tie() {
    assert_order_compatible_order_only_choice(
        &PLANNER_ORDER_ONLY_RANKING_MODEL,
        &[("alpha", OrderDirection::Asc), ("id", OrderDirection::Asc)],
        "z_alpha_idx",
        "order-only ranking fallback",
    );
}

#[test]
fn planner_composite_order_only_selection_prefers_order_compatible_index_over_name_order_tie() {
    assert_order_compatible_order_only_choice(
        &PLANNER_RANKING_MODEL,
        &[
            ("tier", OrderDirection::Asc),
            ("handle", OrderDirection::Asc),
            ("id", OrderDirection::Asc),
        ],
        "z_tier_handle_idx",
        "composite order-only ranking fallback",
    );
}

#[test]
fn planner_composite_order_only_selection_desc_prefers_order_compatible_index_over_name_order_tie()
{
    assert_order_compatible_order_only_choice(
        &PLANNER_RANKING_MODEL,
        &[
            ("tier", OrderDirection::Desc),
            ("handle", OrderDirection::Desc),
            ("id", OrderDirection::Desc),
        ],
        "z_tier_handle_idx",
        "descending composite order-only ranking fallback",
    );
}

#[test]
fn planner_filtered_composite_index_accepts_guarded_text_prefix_with_equality_prefix() {
    let schema = SchemaInfo::cached_for_entity_model(&PLANNER_ORDER_FILTERED_COMPOSITE_MODEL);
    let predicate = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "active",
            CompareOp::Eq,
            Value::Bool(true),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "tier",
            CompareOp::Eq,
            Value::Text("gold".to_string()),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "handle",
            CompareOp::StartsWith,
            Value::Text("br".to_string()),
            CoercionId::Strict,
        )),
    ]);
    let order = canonical_order(&[("handle", OrderDirection::Asc), ("id", OrderDirection::Asc)]);

    let planner_shape = plan_access_for_test_with_order(
        &PLANNER_ORDER_FILTERED_COMPOSITE_MODEL,
        schema,
        Some(&predicate),
        Some(order),
    )
    .expect("guarded filtered composite strict text-prefix access planning should succeed");

    let AccessPlan::Path(path) = planner_shape else {
        panic!(
            "guarded filtered composite strict text-prefix predicate should lower to one index path"
        );
    };
    let AccessPath::IndexRange { spec } = path.as_ref() else {
        panic!(
            "guarded filtered composite strict text-prefix predicate should lower to one index range"
        );
    };

    assert_eq!(spec.index().name(), "tier_handle_idx_active_only");
    assert_eq!(spec.prefix_values(), &[Value::Text("gold".to_string())]);
    assert_eq!(
        spec.lower(),
        &Bound::Included(Value::Text("br".to_string()))
    );
    assert_eq!(
        spec.upper(),
        &Bound::Excluded(Value::Text("bs".to_string()))
    );
}

#[test]
fn planner_single_field_index_accepts_strict_text_prefix_predicate() {
    let schema = SchemaInfo::cached_for_entity_model(&PLANNER_ORDER_MODEL);
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::StartsWith,
        Value::Text("sam".to_string()),
        CoercionId::Strict,
    ));
    let order = canonical_order(&[("name", OrderDirection::Asc), ("id", OrderDirection::Asc)]);

    let planner_shape = plan_access_for_test_with_order(
        &PLANNER_ORDER_MODEL,
        schema,
        Some(&predicate),
        Some(order),
    )
    .expect("strict text-prefix order-only access planning should succeed");

    let AccessPlan::Path(path) = planner_shape else {
        panic!("strict raw-field text-prefix predicate should lower to one index path");
    };
    let AccessPath::IndexRange { spec } = path.as_ref() else {
        panic!("strict raw-field text-prefix predicate should lower to one index range");
    };

    assert_eq!(spec.index().name(), "name_idx");
    assert!(spec.prefix_values().is_empty());
    assert_eq!(
        spec.lower(),
        &Bound::Included(Value::Text("sam".to_string()))
    );
    assert_eq!(
        spec.upper(),
        &Bound::Excluded(Value::Text("san".to_string()))
    );
}

#[test]
fn planner_filtered_composite_expression_text_range_uses_index_range_when_query_implies_guard() {
    let schema =
        SchemaInfo::cached_for_entity_model(&PLANNER_ORDER_FILTERED_COMPOSITE_EXPRESSION_MODEL);
    let predicate = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "active",
            CompareOp::Eq,
            Value::Bool(true),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "tier",
            CompareOp::Eq,
            Value::Text("gold".to_string()),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "handle",
            CompareOp::Gte,
            Value::Text("BR".to_string()),
            CoercionId::TextCasefold,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "handle",
            CompareOp::Lt,
            Value::Text("BS".to_string()),
            CoercionId::TextCasefold,
        )),
    ]);
    let order = canonical_order(&[
        ("LOWER(handle)", OrderDirection::Asc),
        ("id", OrderDirection::Asc),
    ]);

    let planner_shape = plan_access_for_test_with_order(
        &PLANNER_ORDER_FILTERED_COMPOSITE_EXPRESSION_MODEL,
        schema,
        Some(&predicate),
        Some(order),
    )
    .expect("filtered composite expression text range access planning should succeed");

    assert_eq!(
        planner_shape,
        AccessPlan::index_range(SemanticIndexRangeSpec::new(
            PLANNER_ORDER_FILTERED_COMPOSITE_EXPRESSION_INDEXES[0],
            vec![0usize, 1usize],
            vec![Value::Text("gold".to_string())],
            Bound::Included(Value::Text("br".to_string())),
            Bound::Excluded(Value::Text("bs".to_string())),
        )),
        "guarded equality-prefix LOWER(field) ordered text bounds should lower onto the composite expression index range",
    );
}
