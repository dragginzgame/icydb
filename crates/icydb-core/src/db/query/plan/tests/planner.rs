//! Module: db::query::plan::tests::planner
//! Covers planner route-selection behavior at the query-plan owner boundary.
//! Does not own: planner-local normalization internals.
//! Boundary: keeps cross-module planner semantics in the subsystem test suite.

use super::{
    EXPRESSION_CASEFOLD_INDEX_MODEL, FILTERED_EXPRESSION_CASEFOLD_INDEX_MODEL,
    FILTERED_INDEX_MODEL, compare_strict, compare_text_casefold,
    model_with_expression_casefold_index, model_with_filtered_expression_casefold_index,
    model_with_filtered_index,
};
use crate::{
    db::{
        access::{AccessPlan, SemanticIndexRangeSpec},
        predicate::{Predicate, normalize},
        query::plan::{
            OrderDirection, OrderSpec,
            planner::{PlannerError, plan_access_with_order},
        },
        schema::SchemaInfo,
    },
    model::entity::EntityModel,
    value::Value,
};
use std::ops::Bound;

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
        false,
    )
}

fn canonical_order(fields: &[(&str, OrderDirection)]) -> OrderSpec {
    OrderSpec {
        fields: fields
            .iter()
            .map(|(field, direction)| crate::db::query::plan::OrderTerm::field(*field, *direction))
            .collect(),
    }
}

#[test]
fn planner_order_only_filtered_index_fails_closed_without_guard_predicate() {
    let model = model_with_filtered_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let order = canonical_order(&[("tag", OrderDirection::Asc), ("id", OrderDirection::Asc)]);

    let planner_shape = plan_access_for_test_with_order(model, schema, None, Some(order))
        .expect("filtered order-only access planning should succeed");

    assert_eq!(
        planner_shape,
        AccessPlan::full_scan(),
        "filtered indexes must not satisfy order-only access when the query does not imply the guard",
    );
}

#[test]
fn planner_order_only_filtered_index_uses_index_range_when_query_implies_guard() {
    let model = model_with_filtered_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = compare_strict(
        "active",
        crate::db::predicate::CompareOp::Eq,
        Value::Bool(true),
    );
    let order = canonical_order(&[("tag", OrderDirection::Asc), ("id", OrderDirection::Asc)]);

    let planner_shape =
        plan_access_for_test_with_order(model, schema, Some(&predicate), Some(order))
            .expect("guarded filtered order-only access planning should succeed");

    assert_eq!(
        planner_shape,
        AccessPlan::index_range(SemanticIndexRangeSpec::new(
            FILTERED_INDEX_MODEL,
            vec![0usize],
            Vec::new(),
            Bound::Unbounded,
            Bound::Unbounded,
        )),
        "filtered indexes should satisfy order-only access once the query implies their guard",
    );
}

#[test]
fn planner_filtered_index_accepts_strict_text_prefix_when_query_implies_guard() {
    let model = model_with_filtered_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = Predicate::And(vec![
        compare_strict(
            "active",
            crate::db::predicate::CompareOp::Eq,
            Value::Bool(true),
        ),
        compare_strict(
            "tag",
            crate::db::predicate::CompareOp::StartsWith,
            Value::Text("br".to_string()),
        ),
    ]);
    let order = canonical_order(&[("tag", OrderDirection::Asc), ("id", OrderDirection::Asc)]);

    let planner_shape =
        plan_access_for_test_with_order(model, schema, Some(&predicate), Some(order))
            .expect("guarded filtered strict text-prefix access planning should succeed");

    assert_eq!(
        planner_shape,
        AccessPlan::index_range(SemanticIndexRangeSpec::new(
            FILTERED_INDEX_MODEL,
            vec![0usize],
            Vec::new(),
            Bound::Included(Value::Text("br".to_string())),
            Bound::Excluded(Value::Text("bs".to_string())),
        )),
        "filtered indexes should support strict text-prefix planning once the query implies the guard",
    );
}

#[test]
fn planner_order_only_expression_index_falls_back_to_index_range() {
    let model = model_with_expression_casefold_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let order = canonical_order(&[
        ("LOWER(email)", OrderDirection::Asc),
        ("id", OrderDirection::Asc),
    ]);

    let planner_shape = plan_access_for_test_with_order(model, schema, None, Some(order))
        .expect("expression order-only access planning should succeed");

    assert_eq!(
        planner_shape,
        AccessPlan::index_range(SemanticIndexRangeSpec::new(
            EXPRESSION_CASEFOLD_INDEX_MODEL,
            vec![0usize],
            Vec::new(),
            Bound::Unbounded,
            Bound::Unbounded,
        )),
        "canonical LOWER(field) order-only scans should use the matching expression index range",
    );
}

#[test]
fn planner_order_only_expression_desc_index_falls_back_to_index_range() {
    let model = model_with_expression_casefold_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let order = canonical_order(&[
        ("LOWER(email)", OrderDirection::Desc),
        ("id", OrderDirection::Desc),
    ]);

    let planner_shape = plan_access_for_test_with_order(model, schema, None, Some(order))
        .expect("descending expression order-only access planning should succeed");

    assert_eq!(
        planner_shape,
        AccessPlan::index_range(SemanticIndexRangeSpec::new(
            EXPRESSION_CASEFOLD_INDEX_MODEL,
            vec![0usize],
            Vec::new(),
            Bound::Unbounded,
            Bound::Unbounded,
        )),
        "canonical descending LOWER(field) order-only scans should use the matching expression index range",
    );
}

#[test]
fn planner_order_only_expression_index_fails_closed_for_raw_field_order() {
    let model = model_with_expression_casefold_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let order = canonical_order(&[("email", OrderDirection::Asc), ("id", OrderDirection::Asc)]);

    let planner_shape = plan_access_for_test_with_order(model, schema, None, Some(order))
        .expect("expression order-only access planning should succeed");

    assert_eq!(
        planner_shape,
        AccessPlan::full_scan(),
        "raw field ORDER BY must not silently treat expression-key indexes as field-order-compatible",
    );
}

#[test]
fn planner_order_only_filtered_expression_index_fails_closed_without_guard_predicate() {
    let model = model_with_filtered_expression_casefold_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let order = canonical_order(&[
        ("LOWER(email)", OrderDirection::Asc),
        ("id", OrderDirection::Asc),
    ]);

    let planner_shape = plan_access_for_test_with_order(model, schema, None, Some(order))
        .expect("filtered expression order-only access planning should succeed");

    assert_eq!(
        planner_shape,
        AccessPlan::full_scan(),
        "filtered expression ORDER BY must fail closed when the query does not imply the guard",
    );
}

#[test]
fn planner_order_only_filtered_expression_index_uses_index_range_when_query_implies_guard() {
    let model = model_with_filtered_expression_casefold_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = compare_strict(
        "active",
        crate::db::predicate::CompareOp::Eq,
        Value::Bool(true),
    );
    let order = canonical_order(&[
        ("LOWER(email)", OrderDirection::Asc),
        ("id", OrderDirection::Asc),
    ]);

    let planner_shape =
        plan_access_for_test_with_order(model, schema, Some(&predicate), Some(order))
            .expect("filtered expression order-only access planning should succeed");

    assert_eq!(
        planner_shape,
        AccessPlan::index_range(SemanticIndexRangeSpec::new(
            FILTERED_EXPRESSION_CASEFOLD_INDEX_MODEL,
            vec![0usize],
            Vec::new(),
            Bound::Unbounded,
            Bound::Unbounded,
        )),
        "guarded filtered LOWER(field) order-only scans should use the matching expression index range",
    );
}

#[test]
fn planner_order_only_filtered_expression_desc_index_uses_index_range_when_query_implies_guard() {
    let model = model_with_filtered_expression_casefold_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = compare_strict(
        "active",
        crate::db::predicate::CompareOp::Eq,
        Value::Bool(true),
    );
    let order = canonical_order(&[
        ("LOWER(email)", OrderDirection::Desc),
        ("id", OrderDirection::Desc),
    ]);

    let planner_shape =
        plan_access_for_test_with_order(model, schema, Some(&predicate), Some(order))
            .expect("descending filtered expression order-only access planning should succeed");

    assert_eq!(
        planner_shape,
        AccessPlan::index_range(SemanticIndexRangeSpec::new(
            FILTERED_EXPRESSION_CASEFOLD_INDEX_MODEL,
            vec![0usize],
            Vec::new(),
            Bound::Unbounded,
            Bound::Unbounded,
        )),
        "guarded descending LOWER(field) order-only scans should use the matching expression index range",
    );
}

#[test]
fn planner_expression_text_range_uses_expression_index_range() {
    let model = model_with_expression_casefold_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let predicate = compare_text_casefold(
        "email",
        crate::db::predicate::CompareOp::Gte,
        Value::Text("BR".to_string()),
    );

    let normalized = normalize(&predicate);
    let planner_shape = super::plan_access(model, model.indexes(), schema, Some(&normalized))
        .expect("expression text range access planning should succeed");

    assert_eq!(
        planner_shape,
        AccessPlan::index_range(SemanticIndexRangeSpec::new(
            EXPRESSION_CASEFOLD_INDEX_MODEL,
            vec![0usize],
            Vec::new(),
            Bound::Included(Value::Text("br".to_string())),
            Bound::Unbounded,
        )),
        "canonical LOWER(field) ordered text bounds should lower onto the matching expression index range",
    );
}
