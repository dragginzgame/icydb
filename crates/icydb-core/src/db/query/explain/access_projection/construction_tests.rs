//! Access DTO admission happens at the collecting visitor, before child work.

use super::*;
use crate::db::{
    RequestExecutionRoot,
    executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    query::plan::project_explain_access_path,
};
use icydb_diagnostic_code::{DiagnosticExecutionLane as Lane, DiagnosticFactTag};

fn request(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

fn project(
    access: &AccessPlan<Value>,
    root: &RequestExecutionRoot,
) -> Result<ExplainAccessPath, QueryError> {
    PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
        explain_access_plan(access, work)
    })
}

#[test]
fn access_projection_admits_composite_backing_before_visiting_children() {
    for access in [
        AccessPlan::Union(vec![AccessPlan::by_keys(vec![]); 4]),
        AccessPlan::Intersection(vec![AccessPlan::by_keys(vec![]); 4]),
    ] {
        let bytes = 4 * size_of::<ExplainAccessPath>() as u64;
        let short = request(Resource::TemporaryBytes, bytes - 1);
        let error = project(&access, &short).unwrap_err();
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::TemporaryBytes.raw(),
        )));
        assert_eq!(short.observed(Resource::PredicateExpressionSteps), 1);
        assert_eq!(short.observed(Resource::NestedValueSteps), 0);
        let exact = request(Resource::TemporaryBytes, bytes);
        assert!(project(&access, &exact).is_ok());
        assert_eq!(exact.observed(Resource::TemporaryBytes), bytes);
        assert_eq!(exact.observed(Resource::PredicateExpressionSteps), 5);
    }
}

#[test]
fn access_projection_stops_at_failed_child_without_visiting_later_siblings() {
    let access = AccessPlan::Union(vec![
        AccessPlan::by_keys(vec![]),
        AccessPlan::by_keys(vec![Value::Text("a".into())]),
        AccessPlan::by_keys(vec![Value::Text("unvisited".into())]),
    ]);
    let short = request(
        Resource::TemporaryBytes,
        3 * size_of::<ExplainAccessPath>() as u64,
    );
    assert!(project(&access, &short).is_err());
    assert_eq!(short.observed(Resource::PredicateExpressionSteps), 3);
    assert_eq!(short.observed(Resource::NestedValueSteps), 0);
}

#[test]
fn access_projection_repeated_calls_preserve_values_and_depth() {
    let mut access = AccessPlan::by_keys(vec![Value::List(vec![Value::Text("payload".into())])]);
    for _ in 1..MAX_EXPLAIN_ACCESS_DEPTH {
        access = AccessPlan::Union(vec![access]);
    }
    let generous = request(Resource::TemporaryBytes, 16_000_000);
    let expected = project(&access, &generous).unwrap();
    let bytes = generous.observed(Resource::TemporaryBytes);
    let exact = request(Resource::TemporaryBytes, bytes * 2);
    assert_eq!(project(&access, &exact).unwrap(), expected);
    assert_eq!(project(&access, &exact).unwrap(), expected);
    assert!(project(&access, &exact).is_err());
    assert_eq!(
        generous.observed(Resource::PredicateExpressionSteps),
        (MAX_EXPLAIN_ACCESS_DEPTH + "payload".len()) as u64
    );
    assert_eq!(generous.observed(Resource::RowsVisited), 0);
}

#[test]
fn access_projection_rejects_depth_before_child_backing_and_payload() {
    let mut access = AccessPlan::by_keys(vec![Value::Text("unvisited".into())]);
    for level in 0..MAX_EXPLAIN_ACCESS_DEPTH {
        access = if level % 2 == 0 {
            AccessPlan::Union(vec![access])
        } else {
            AccessPlan::Intersection(vec![access])
        };
    }
    let root = request(Resource::TemporaryBytes, 16_000_000);
    for attempt in 1..=2 {
        let error = project(&access, &root).unwrap_err();
        assert_eq!(
            error.diagnostic(),
            InternalError::query_explain_depth_exceeded(128, 129).diagnostic(),
        );
        assert!(
            error
                .diagnostic_facts()
                .contains(&(DiagnosticFactTag::Limit, 128))
        );
        assert!(
            error
                .diagnostic_facts()
                .contains(&(DiagnosticFactTag::Actual, 129))
        );
        assert_eq!(
            root.observed(Resource::PredicateExpressionSteps),
            attempt * 128
        );
        assert_eq!(
            root.observed(Resource::TemporaryBytes),
            attempt * 127 * size_of::<ExplainAccessPath>() as u64,
        );
        assert_eq!(root.observed(Resource::NestedValueSteps), 0);
        assert_eq!(root.observed(Resource::RowsVisited), 0);
    }
    assert_eq!(
        project(&AccessPlan::full_scan(), &root).unwrap(),
        ExplainAccessPath::FullScan,
    );
}

#[test]
fn access_projection_depth_is_per_branch_and_allows_empty_terminal_composites() {
    for leaf in [
        AccessPlan::full_scan(),
        AccessPlan::Union(vec![]),
        AccessPlan::Intersection(vec![]),
    ] {
        let mut branch = leaf;
        for _ in 2..MAX_EXPLAIN_ACCESS_DEPTH {
            branch = AccessPlan::Union(vec![branch]);
        }
        let access = AccessPlan::Intersection(vec![branch.clone(), branch]);
        let root = request(Resource::TemporaryBytes, 16_000_000);
        assert!(project(&access, &root).is_ok());
        assert_eq!(root.observed(Resource::PredicateExpressionSteps), 255);
    }
}

#[test]
fn access_projection_preserves_every_dto_shape_and_charges_branch_field_copy() {
    let fields = vec!["owner".into(), "amount".into()];
    let values = vec![Value::Text("λ".into())];
    let cases = [
        ExplainAccessPath::ByKey {
            key: Value::Nat64(3),
        },
        ExplainAccessPath::ByKeys {
            keys: values.clone(),
        },
        ExplainAccessPath::KeyRange {
            start: Value::Nat64(1),
            end: Value::Nat64(9),
        },
        ExplainAccessPath::FullScan,
        ExplainAccessPath::IndexPrefix {
            name: "i".into(),
            fields: fields.clone(),
            prefix_len: 1,
            values: values.clone(),
        },
        ExplainAccessPath::IndexMultiLookup {
            name: "i".into(),
            fields: fields.clone(),
            values: values.clone(),
        },
        ExplainAccessPath::IndexBranchSet {
            name: "i".into(),
            fields: fields.clone(),
            fixed_values: values.clone(),
            branch_values: vec![Value::Nat64(5)],
            branch_field: Some("amount".into()),
        },
        ExplainAccessPath::IndexRange {
            name: "i".into(),
            fields,
            prefix_len: 1,
            prefix: values,
            lower: Bound::Included(Value::Nat64(3)),
            upper: Bound::Excluded(Value::Nat64(9)),
        },
        ExplainAccessPath::IndexRange {
            name: "i".into(),
            fields: vec![],
            prefix_len: 0,
            prefix: vec![],
            lower: Bound::Excluded(Value::Nat64(3)),
            upper: Bound::Included(Value::Nat64(9)),
        },
        ExplainAccessPath::IndexRange {
            name: "i".into(),
            fields: vec![],
            prefix_len: 0,
            prefix: vec![],
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        },
    ];
    for source in cases {
        let root = request(Resource::TemporaryBytes, 16_000_000);
        let actual = PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            project_explain_access_path(&source, &mut ExplainAccessProjection { work, depth: 1 })
        })
        .unwrap();
        assert_eq!(actual, source);
        if matches!(source, ExplainAccessPath::IndexBranchSet { .. }) {
            let bytes =
                1 + 2 * size_of::<String>() + 5 + 6 + 6 + 2 * size_of::<Value>() + "λ".len();
            assert_eq!(root.observed(Resource::TemporaryBytes), bytes as u64);
        }
    }
}
