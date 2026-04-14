//! Module: db::query::plan::tests
//! Collects query-plan tests across semantics, policy, guards, and pushdown
//! behavior.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

mod access;
mod continuation;
mod covering;
mod error_mapping;
mod group;
mod group_having;
mod planner;
mod pushdown;
mod semantics;
mod structural_guards;

use crate::{
    db::access::{AccessPath, AccessPlan, SemanticIndexRangeSpec},
    db::predicate::{
        CoercionId, CompareOp, ComparePredicate, Predicate, PredicateProgram, normalize,
    },
    db::query::plan::plan_access,
    db::schema::SchemaInfo,
    model::{
        entity::EntityModel,
        field::FieldKind,
        index::{IndexExpression, IndexKeyItem, IndexModel, IndexPredicateMetadata},
    },
    traits::EntitySchema,
    types::Ulid,
    value::Value,
};
use std::{ops::Bound, sync::LazyLock};

static ACTIVE_TRUE_PREDICATE: LazyLock<Predicate> =
    LazyLock::new(|| Predicate::eq("active".to_string(), true.into()));
static SCORE_GTE_10_PREDICATE: LazyLock<Predicate> =
    LazyLock::new(|| Predicate::gte("score".to_string(), 10u64.into()));

fn active_true_predicate() -> &'static Predicate {
    &ACTIVE_TRUE_PREDICATE
}

fn score_gte_10_predicate() -> &'static Predicate {
    &SCORE_GTE_10_PREDICATE
}

const fn active_true_predicate_metadata() -> IndexPredicateMetadata {
    IndexPredicateMetadata::generated("active = true", active_true_predicate)
}

const fn score_gte_10_predicate_metadata() -> IndexPredicateMetadata {
    IndexPredicateMetadata::generated("score >= 10", score_gte_10_predicate)
}

const INDEX_FIELDS: [&str; 1] = ["tag"];
const INDEX_MODEL: IndexModel = IndexModel::generated(
    "plan_tests::idx_tag",
    "plan_tests::IndexStore",
    &INDEX_FIELDS,
    false,
);
const RANGE_INDEX_FIELDS: [&str; 3] = ["a", "b", "c"];
const RANGE_INDEX_MODEL: IndexModel = IndexModel::generated(
    "plan_tests::idx_abc",
    "plan_tests::RangeIndexStore",
    &RANGE_INDEX_FIELDS,
    false,
);
const FILTERED_INDEX_FIELDS: [&str; 1] = ["tag"];
const FILTERED_INDEX_MODEL: IndexModel = IndexModel::generated_with_predicate(
    "plan_tests::idx_tag_active_only",
    "plan_tests::FilteredIndexStore",
    &FILTERED_INDEX_FIELDS,
    false,
    Some(active_true_predicate_metadata()),
);
const FILTERED_NUMERIC_INDEX_FIELDS: [&str; 1] = ["score"];
const FILTERED_NUMERIC_INDEX_MODEL: IndexModel = IndexModel::generated_with_predicate(
    "plan_tests::idx_score_ge_10",
    "plan_tests::FilteredNumericIndexStore",
    &FILTERED_NUMERIC_INDEX_FIELDS,
    false,
    Some(score_gte_10_predicate_metadata()),
);
const EXPRESSION_CASEFOLD_INDEX_FIELDS: [&str; 1] = ["email"];
const EXPRESSION_CASEFOLD_INDEX_KEY_ITEMS: [IndexKeyItem; 1] =
    [IndexKeyItem::Expression(IndexExpression::Lower("email"))];
const EXPRESSION_CASEFOLD_INDEX_MODEL: IndexModel = IndexModel::generated_with_key_items(
    "plan_tests::idx_email_lower",
    "plan_tests::ExpressionCasefoldIndexStore",
    &EXPRESSION_CASEFOLD_INDEX_FIELDS,
    &EXPRESSION_CASEFOLD_INDEX_KEY_ITEMS,
    false,
);
const EXPRESSION_UPPER_INDEX_FIELDS: [&str; 1] = ["email"];
const EXPRESSION_UPPER_INDEX_KEY_ITEMS: [IndexKeyItem; 1] =
    [IndexKeyItem::Expression(IndexExpression::Upper("email"))];
const EXPRESSION_UPPER_INDEX_MODEL: IndexModel = IndexModel::generated_with_key_items(
    "plan_tests::idx_email_upper",
    "plan_tests::ExpressionUpperIndexStore",
    &EXPRESSION_UPPER_INDEX_FIELDS,
    &EXPRESSION_UPPER_INDEX_KEY_ITEMS,
    false,
);
const EXPRESSION_UNSUPPORTED_INDEX_FIELDS: [&str; 1] = ["email"];
const EXPRESSION_UNSUPPORTED_INDEX_KEY_ITEMS: [IndexKeyItem; 1] = [IndexKeyItem::Expression(
    IndexExpression::LowerTrim("email"),
)];
const EXPRESSION_UNSUPPORTED_INDEX_MODEL: IndexModel = IndexModel::generated_with_key_items(
    "plan_tests::idx_email_lower_trim",
    "plan_tests::ExpressionUnsupportedIndexStore",
    &EXPRESSION_UNSUPPORTED_INDEX_FIELDS,
    &EXPRESSION_UNSUPPORTED_INDEX_KEY_ITEMS,
    false,
);
const FILTERED_EXPRESSION_CASEFOLD_INDEX_FIELDS: [&str; 1] = ["email"];
const FILTERED_EXPRESSION_CASEFOLD_INDEX_KEY_ITEMS: [IndexKeyItem; 1] =
    [IndexKeyItem::Expression(IndexExpression::Lower("email"))];
const FILTERED_EXPRESSION_CASEFOLD_INDEX_MODEL: IndexModel =
    IndexModel::generated_with_key_items_and_predicate(
        "plan_tests::idx_email_lower_active_only",
        "plan_tests::FilteredExpressionCasefoldIndexStore",
        &FILTERED_EXPRESSION_CASEFOLD_INDEX_FIELDS,
        Some(&FILTERED_EXPRESSION_CASEFOLD_INDEX_KEY_ITEMS),
        false,
        Some(active_true_predicate_metadata()),
    );

crate::test_entity! {
    ident = PlanModelEntity,
    id = Ulid,
    entity_name = "PlanEntity",
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
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("a", FieldKind::Uint),
        ("b", FieldKind::Uint),
        ("c", FieldKind::Uint),
    ],
    indexes = [&RANGE_INDEX_MODEL],
}

crate::test_entity! {
    ident = PlanFilteredEntity,
    id = Ulid,
    entity_name = "PlanFilteredEntity",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("tag", FieldKind::Text),
        ("active", FieldKind::Bool),
    ],
    indexes = [&FILTERED_INDEX_MODEL],
}

crate::test_entity! {
    ident = PlanFilteredNumericEntity,
    id = Ulid,
    entity_name = "PlanFilteredNumericEntity",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("score", FieldKind::Uint),
    ],
    indexes = [&FILTERED_NUMERIC_INDEX_MODEL],
}

crate::test_entity! {
    ident = PlanExpressionCasefoldEntity,
    id = Ulid,
    entity_name = "PlanExpressionCasefoldEntity",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("email", FieldKind::Text),
    ],
    indexes = [&EXPRESSION_CASEFOLD_INDEX_MODEL],
}

crate::test_entity! {
    ident = PlanFilteredExpressionCasefoldEntity,
    id = Ulid,
    entity_name = "PlanFilteredExpressionCasefoldEntity",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("email", FieldKind::Text),
        ("active", FieldKind::Bool),
    ],
    indexes = [&FILTERED_EXPRESSION_CASEFOLD_INDEX_MODEL],
}

crate::test_entity! {
    ident = PlanExpressionUpperEntity,
    id = Ulid,
    entity_name = "PlanExpressionUpperEntity",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("email", FieldKind::Text),
    ],
    indexes = [&EXPRESSION_UPPER_INDEX_MODEL],
}

crate::test_entity! {
    ident = PlanExpressionUnsupportedEntity,
    id = Ulid,
    entity_name = "PlanExpressionUnsupportedEntity",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("email", FieldKind::Text),
    ],
    indexes = [&EXPRESSION_UNSUPPORTED_INDEX_MODEL],
}

// Helper for tests that need the indexed model derived from a typed schema.
fn model_with_index() -> &'static EntityModel {
    <PlanModelEntity as EntitySchema>::MODEL
}

fn model_with_range_index() -> &'static EntityModel {
    <PlanRangeEntity as EntitySchema>::MODEL
}

fn model_with_filtered_index() -> &'static EntityModel {
    <PlanFilteredEntity as EntitySchema>::MODEL
}

fn model_with_filtered_numeric_index() -> &'static EntityModel {
    <PlanFilteredNumericEntity as EntitySchema>::MODEL
}

fn model_with_expression_casefold_index() -> &'static EntityModel {
    <PlanExpressionCasefoldEntity as EntitySchema>::MODEL
}

fn model_with_filtered_expression_casefold_index() -> &'static EntityModel {
    <PlanFilteredExpressionCasefoldEntity as EntitySchema>::MODEL
}

fn model_with_expression_upper_index() -> &'static EntityModel {
    <PlanExpressionUpperEntity as EntitySchema>::MODEL
}

fn model_with_expression_unsupported_index() -> &'static EntityModel {
    <PlanExpressionUnsupportedEntity as EntitySchema>::MODEL
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

fn compare_text_casefold(field: &str, op: CompareOp, value: Value) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        field,
        op,
        value,
        CoercionId::TextCasefold,
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

fn assert_single_field_text_index_range(
    plan: &AccessPlan<Value>,
    expected_lower: Bound<Value>,
    expected_upper: Bound<Value>,
) {
    let (index, prefix, lower, upper) =
        find_index_range(plan).expect("plan should include one text index range");

    assert_eq!(index.name(), INDEX_MODEL.name());
    assert!(
        prefix.is_empty(),
        "single-field text ranges should not carry equality prefix values",
    );
    assert_eq!(lower, &expected_lower);
    assert_eq!(upper, &expected_upper);
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

fn plan_access_for_test(
    model: &EntityModel,
    schema: &SchemaInfo,
    predicate: Option<&Predicate>,
) -> Result<AccessPlan<Value>, crate::db::query::plan::PlannerError> {
    let normalized = predicate.map(normalize);

    plan_access(model, model.indexes(), schema, normalized.as_ref())
}

// Compile the runtime predicate program against one model so access-planning
// tests can lock the intended separation between planner and execution.
fn compile_runtime_predicate_for_test(
    model: &'static EntityModel,
    predicate: &Predicate,
) -> PredicateProgram {
    PredicateProgram::compile_with_model(model, predicate)
}
