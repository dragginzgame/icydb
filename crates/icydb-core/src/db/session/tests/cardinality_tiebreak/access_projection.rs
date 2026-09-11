//! Project current accepted field/expression index contracts under one request.

use super::*;
use crate::db::{
    RequestExecutionRoot,
    access::{AccessPlan, SemanticIndexAccessContract, SemanticIndexRangeSpec},
    executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    query::{
        explain::{ExplainAccessPath, explain_access_plan},
        preparation::PreparationWork,
    },
};
use crate::value::Value;
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
};
use std::ops::Bound;

#[test]
fn decision_projection_matches_first_selected_identity_on_a_warm_plan() {
    let session = initialize();
    seed_rows(&session);
    let request = selective_dynamic_query();
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let query = PreparationWork::run(
        session.db.request_execution_scope(),
        Lane::TrustedRead,
        |work| {
            StructuralQuery::new(MissingRowPolicy::Ignore).filter_for_schema(
                catalog.accepted_schema_info(),
                request.filter_expr().unwrap(),
                work,
            )
        },
    )
    .unwrap()
    .order_spec(OrderSpec {
        fields: vec![asc("id").lower()],
    })
    .select_fields(["id"])
    .limit(3);
    let prepare = || {
        session
            .structural_projection_prepared_plan_for_accepted_authority(
                &query,
                catalog.accepted_entity_authority(),
                catalog.snapshot(),
                Lane::TrustedRead,
            )
            .unwrap()
            .0
    };
    let (cold, warm) = (prepare(), prepare());
    let original = warm.logical_plan();
    assert_eq!(original, cold.logical_plan());
    let signature = original.continuation_signature(ENTITY_NAME);
    let mut planned = original.clone();
    let selected_name = planned
        .access
        .selected_index_contract()
        .unwrap()
        .name()
        .to_owned();
    let first = planned
        .access_choice
        .candidates
        .iter()
        .find(|candidate| candidate.index_name() == selected_name)
        .unwrap()
        .clone();
    let mut later = first.clone();
    later.residual_predicate_terms = first.residual_predicate_terms + 7;
    planned.access_choice.candidates = vec![first.clone(), later];
    let planned = crate::db::query::preparation::with_preparation_work(|work| {
        crate::db::executor::SharedPreparedExecutionPlan::from_plan(
            warm.authority(),
            planned,
            warm.authority_ref().accepted_schema_fingerprint(),
            work,
        )
    })
    .unwrap();
    let generous = RequestExecutionRoot::__new_runtime_root();
    let expected = PreparationWork::run(&generous.scope(), Lane::Diagnostic, |work| {
        planned.explain(work)
    })
    .unwrap();
    assert_eq!(
        expected.access_decision().selected.index_name.as_deref(),
        Some(selected_name.as_str())
    );
    assert_eq!(
        expected.access_decision().residual.residual_predicate_count,
        first.residual_predicate_terms
    );
    assert_eq!(expected.access_decision().candidates.len(), 2);
    for resource in [Resource::TemporaryBytes, Resource::PredicateExpressionSteps] {
        let exact = RequestExecutionRoot::new_for_tests(
            HardExecutionBudget::uniform_for_tests(
                16_000_000,
                HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
            )
            .with_limit_for_tests(resource, generous.observed(resource)),
        );
        let run = || {
            PreparationWork::run(&exact.scope(), Lane::Diagnostic, |work| {
                planned.explain(work)
            })
        };
        assert_eq!(run().unwrap(), expected);
        assert!(run().is_err());
        assert_eq!(exact.observed(Resource::RowsVisited), 0);
    }
    assert_eq!(original.continuation_signature(ENTITY_NAME), signature);
    assert_eq!(prepare().logical_plan(), original);
    assert_eq!(projection_rows(&session,
        "SELECT id FROM PlannerRow WHERE common = 'everyone' AND rare = 'group-a' ORDER BY id LIMIT 3"
    ).len(), 3);
}

#[test]
fn access_projection_keeps_accepted_expression_index_source_fields() {
    let session = initialize();
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let accepted = catalog
        .accepted_schema_info()
        .expression_indexes()
        .iter()
        .find(|index| index.name() == "zz_lower_common_idx")
        .unwrap();
    let access = AccessPlan::index_prefix_from_contract(
        SemanticIndexAccessContract::from_accepted_expression_index(accepted),
        vec![Value::Text("everyone".into())],
    );
    let projected = crate::db::query::preparation::with_preparation_work(|work| {
        explain_access_plan(&access, work)
    })
    .unwrap();
    assert_eq!(
        projected,
        ExplainAccessPath::IndexPrefix {
            name: "zz_lower_common_idx".into(),
            fields: vec!["common".into()],
            prefix_len: 1,
            values: vec![Value::Text("everyone".into())],
        }
    );
}

#[test]
fn access_projection_keeps_accepted_index_fields_and_successful_bytes() {
    let session = initialize();
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let schema = catalog.accepted_schema_info();
    let accepted = schema
        .field_path_indexes()
        .iter()
        .find(|index| index.name() == "b_wide_branch_idx")
        .unwrap();
    let index = SemanticIndexAccessContract::from_accepted_field_path_index(accepted);
    let values = vec![Value::Text("all".into())];
    let cases = [
        AccessPlan::index_prefix_from_contract(index.clone(), values.clone()),
        AccessPlan::index_multi_lookup_from_contract(index.clone(), values.clone()),
        AccessPlan::index_branch_set_from_contract(
            index.clone(),
            values.clone(),
            vec![Value::Text("x".into())],
        ),
        AccessPlan::index_range(SemanticIndexRangeSpec::from_access_contract(
            index,
            vec![3, 4],
            values,
            Bound::Included(Value::Text("a".into())),
            Bound::Unbounded,
        )),
    ];
    for access in cases {
        let root = RequestExecutionRoot::new_for_tests(HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        ));
        let projected = PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            explain_access_plan(&access, work)
        })
        .unwrap();
        let (ExplainAccessPath::IndexPrefix { name, fields, .. }
        | ExplainAccessPath::IndexMultiLookup { name, fields, .. }
        | ExplainAccessPath::IndexBranchSet { name, fields, .. }
        | ExplainAccessPath::IndexRange { name, fields, .. }) = &projected
        else {
            panic!("index DTO required")
        };
        assert_eq!(name, "b_wide_branch_idx");
        assert_eq!(fields, &["wide_fixed", "wide_branch"]);
        let bytes = root.observed(Resource::TemporaryBytes);
        let exact = RequestExecutionRoot::new_for_tests(
            HardExecutionBudget::uniform_for_tests(
                16_000_000,
                HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
            )
            .with_limit_for_tests(Resource::TemporaryBytes, bytes),
        );
        assert_eq!(
            PreparationWork::run(
                &exact.scope(),
                Lane::Diagnostic,
                |work| explain_access_plan(&access, work)
            )
            .unwrap(),
            projected
        );
        assert!(
            PreparationWork::run(
                &exact.scope(),
                Lane::Diagnostic,
                |work| explain_access_plan(&access, work)
            )
            .is_err()
        );
        assert_eq!(exact.observed(Resource::RowsVisited), 0);
    }
}
