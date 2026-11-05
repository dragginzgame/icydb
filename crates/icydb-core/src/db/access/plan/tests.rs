use crate::{
    db::access::{
        AccessPlan, SemanticIndexAccessContract, SemanticIndexKeyItem,
        path::SemanticIndexAccessContractInner,
    },
    value::Value,
};
use std::sync::Arc;

fn accepted_index_contract() -> SemanticIndexAccessContract {
    SemanticIndexAccessContract {
        inner: Arc::new(SemanticIndexAccessContractInner {
            ordinal: 1,
            physical_generation: 1,
            name: "by_label".to_string(),
            store_path: "test::Store".to_string(),
            key_items: vec![SemanticIndexKeyItem::Field("label".to_string())],
            unique: false,
            predicate_semantics: None,
        }),
    }
}

#[test]
fn union_constructor_flattens_and_collapses_single_child() {
    let plan: AccessPlan<u64> =
        AccessPlan::union(vec![AccessPlan::union(vec![AccessPlan::by_key(7)])]);

    assert_eq!(plan, AccessPlan::by_key(7));
}

#[test]
fn intersection_constructor_flattens_and_collapses_single_child() {
    let plan: AccessPlan<u64> =
        AccessPlan::intersection(vec![AccessPlan::intersection(vec![AccessPlan::by_key(9)])]);

    assert_eq!(plan, AccessPlan::by_key(9));
}

#[test]
fn union_constructor_empty_collapses_to_full_scan() {
    let plan: AccessPlan<u64> = AccessPlan::union(Vec::new());

    assert_eq!(plan, AccessPlan::full_scan());
}

#[test]
fn union_constructor_explicit_empty_is_identity_for_non_empty_children() {
    let plan: AccessPlan<u64> =
        AccessPlan::union(vec![AccessPlan::by_key(7), AccessPlan::by_keys(Vec::new())]);

    assert_eq!(plan, AccessPlan::by_key(7));
}

#[test]
fn union_constructor_only_explicit_empty_children_stays_explicit_empty() {
    let plan: AccessPlan<u64> = AccessPlan::union(vec![
        AccessPlan::by_keys(Vec::new()),
        AccessPlan::by_keys(Vec::new()),
    ]);

    assert_eq!(plan, AccessPlan::by_keys(Vec::new()));
}

#[test]
fn intersection_constructor_empty_collapses_to_full_scan() {
    let plan: AccessPlan<u64> = AccessPlan::intersection(Vec::new());

    assert_eq!(plan, AccessPlan::full_scan());
}

#[test]
fn intersection_constructor_explicit_empty_annihilates_children() {
    let plan: AccessPlan<u64> =
        AccessPlan::intersection(vec![AccessPlan::by_key(9), AccessPlan::by_keys(Vec::new())]);

    assert_eq!(plan, AccessPlan::by_keys(Vec::new()));
}

#[test]
fn intersection_constructor_nested_explicit_empty_annihilates_children() {
    let plan: AccessPlan<u64> = AccessPlan::intersection(vec![AccessPlan::intersection(vec![
        AccessPlan::by_key(9),
        AccessPlan::by_keys(Vec::new()),
    ])]);

    assert_eq!(plan, AccessPlan::by_keys(Vec::new()));
}

#[test]
fn composite_plan_reports_nested_accepted_index_access() {
    let plan = AccessPlan::intersection(vec![
        AccessPlan::by_key(Value::Unit),
        AccessPlan::index_prefix_from_contract(
            accepted_index_contract(),
            vec![Value::Text("singleton".to_string())],
        ),
    ]);

    assert!(plan.has_selected_index_access_path());
    assert!(!AccessPlan::<Value>::by_key(Value::Unit).has_selected_index_access_path());
}

#[test]
fn grouped_strategy_charges_index_prefix_comparisons_before_order_proof() {
    use crate::db::{
        RequestExecutionRoot,
        predicate::MissingRowPolicy,
        query::{
            builder::aggregate::count,
            plan::{
                AccessPlannedQuery, FieldSlot, GroupAggregateSpec, GroupFieldSet, GroupPlan,
                GroupSpec, GroupedExecutionConfig, LogicalPlan, grouped_plan_strategy,
                grouped_plan_strategy_for_explain,
            },
            preparation::PreparationWork,
        },
        resource::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        schema::AcceptedFieldKind,
    };
    use icydb_diagnostic_code::{
        DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
        DiagnosticFactTag,
    };

    for equality_prefix in [false, true] {
        let mut index = accepted_index_contract();
        let values = if equality_prefix {
            Arc::get_mut(&mut index.inner)
                .unwrap()
                .key_items
                .insert(0, SemanticIndexKeyItem::Field("tenant".into()));
            vec![Value::Nat64(7)]
        } else {
            vec![]
        };
        let mut query = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
        query.access = AccessPlan::index_prefix_from_contract(index, values);
        query.logical = LogicalPlan::Grouped(GroupPlan {
            scalar: query.scalar_plan().clone(),
            group: GroupSpec {
                group_fields: GroupFieldSet::Direct(vec![FieldSlot::from_test_accepted_kind(
                    0,
                    "label",
                    AcceptedFieldKind::Text { max_len: Some(64) },
                )]),
                aggregates: vec![GroupAggregateSpec::from_aggregate_expr(count())],
                execution: GroupedExecutionConfig::planner_default_bounded(),
            },
            having_expr: None,
        });
        let residual = crate::db::query::preparation::with_preparation_work(|work| {
            query.prepare_residual_filter_shape(work)
        })
        .unwrap();
        let expected = grouped_plan_strategy(&query, || Ok(residual))
            .unwrap()
            .unwrap();
        assert!(expected.is_ordered_group());
        // Five fixed visits, plus one label comparison per examined key.
        let used = 5 + 6 * if equality_prefix { 2 } else { 1 };
        for limit in [used - 1, used] {
            let root = RequestExecutionRoot::new_for_tests(
                HardExecutionBudget::uniform_for_tests(
                    16_000_000,
                    HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                )
                .with_limit_for_tests(Resource::PredicateExpressionSteps, limit),
            );
            let result = PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
                grouped_plan_strategy_for_explain(&query, query.grouped_plan().unwrap(), work)
            });
            if limit == used {
                assert_eq!(result.unwrap(), expected);
                assert_eq!(root.observed(Resource::PredicateExpressionSteps), used);
            } else {
                assert!(result.unwrap_err().diagnostic_facts().contains(&(
                    DiagnosticFactTag::BudgetResource,
                    Resource::PredicateExpressionSteps.raw(),
                )));
            }
            assert_eq!(root.observed(Resource::TemporaryBytes), 0);
            assert_eq!(root.observed(Resource::RowsVisited), 0);
        }
    }
}
