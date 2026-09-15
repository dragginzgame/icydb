//! Compact continuation state retains identity independently of access payload size.

use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        access::{AccessPath, AccessPlan, SemanticIndexRangeSpec},
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        predicate::MissingRowPolicy,
        query::{
            plan::{AccessPlannedQuery, VisibleIndexes, pipeline::tests::exact_metadata_schema},
            preparation::{PreparationWork, with_preparation_work},
        },
    },
    retained::RetainedBytes,
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
};
use std::ops::Bound;

#[test]
fn continuation_retention_is_independent_of_access_payload_size() {
    let mut retained_bytes = None;
    for size in [1, 1024, 65_536] {
        let plan = AccessPlannedQuery::new(
            AccessPath::ByKey(Value::Text("x".repeat(size))),
            MissingRowPolicy::Ignore,
        );
        let request = RequestExecutionRoot::new_for_tests(
            HardExecutionBudget::uniform_for_tests(
                16_000_000,
                HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
            )
            .with_limit_for_tests(Resource::TemporaryBytes, 0),
        );
        let expected =
            with_preparation_work(|work| plan.continuation_signature("tests::Entity", work))
                .unwrap();
        let contract = PreparationWork::run(&request.scope(), Lane::PublicRead, |work| {
            plan.planned_continuation_contract_with_accepted_identity("tests::Entity", None, work)
                .map_err(QueryError::execute)
        })
        .unwrap()
        .unwrap();
        assert_eq!(contract.continuation_signature(), expected);
        let bytes = RetainedBytes::measure(&contract, usize::MAX).unwrap();
        assert_eq!(*retained_bytes.get_or_insert(bytes), bytes);
        assert_eq!(request.observed(Resource::TemporaryBytes), 0);
        assert_eq!(request.observed(Resource::RowsVisited), 0);
        assert_eq!(request.observed(Resource::QueryExecutions), 0);
        drop(plan);
        assert_eq!(contract.clone().continuation_signature(), expected);
    }
}

#[test]
fn continuation_identity_binds_access_variants_without_owning_the_source_plan() {
    let schema = exact_metadata_schema(&[("by_age_rank", &["age", "rank", "id"])], &[]);
    let index = VisibleIndexes::accepted_schema_visible(&schema)
        .unwrap()
        .accepted_semantic_index_contracts()[0]
        .clone();
    let paths = vec![
        AccessPlan::by_key(Value::Nat64(3)),
        AccessPlan::by_key(Value::Nat64(9)),
        AccessPlan::by_keys(vec![Value::Nat64(3), Value::Nat64(9)]),
        AccessPlan::key_range(Value::Nat64(3), Value::Nat64(9)),
        AccessPlan::index_prefix_from_contract(index.clone(), vec![Value::Nat64(3)]),
        AccessPlan::index_multi_lookup_from_contract(index.clone(), vec![Value::Nat64(3)]),
        AccessPlan::index_branch_set_from_contract(
            index.clone(),
            vec![Value::Nat64(3)],
            vec![Value::Nat64(9)],
        ),
        AccessPlan::index_range(SemanticIndexRangeSpec::from_access_contract(
            index.clone(),
            vec![1],
            vec![],
            Bound::Included(Value::Nat64(3)),
            Bound::Unbounded,
        )),
        AccessPlan::index_range(SemanticIndexRangeSpec::from_access_contract(
            index,
            vec![1],
            vec![],
            Bound::Excluded(Value::Nat64(3)),
            Bound::Unbounded,
        )),
        AccessPlan::Union(vec![
            AccessPlan::by_key(Value::Nat64(3)),
            AccessPlan::by_key(Value::Nat64(9)),
        ]),
        AccessPlan::Intersection(vec![
            AccessPlan::by_key(Value::Nat64(3)),
            AccessPlan::by_key(Value::Nat64(9)),
        ]),
        AccessPlan::full_scan(),
    ];
    let mut signatures = Vec::new();
    for access in paths {
        let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
        plan.access = access;
        let expected =
            with_preparation_work(|work| plan.continuation_signature("tests::Entity", work))
                .unwrap();
        let contract = with_preparation_work(|work| {
            plan.planned_continuation_contract_with_accepted_identity("tests::Entity", None, work)
        })
        .unwrap()
        .unwrap();
        drop(plan);
        assert_eq!(contract.continuation_signature(), expected);
        assert!(!signatures.contains(&expected));
        signatures.push(expected);
    }
}
