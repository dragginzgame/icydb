//! Candidate-list construction shares the request; warm template reuse does not
//! allocate another candidate list or publish an incomplete cold artifact.

use super::*;
use crate::db::{
    MissingRowPolicy, RequestExecutionRoot,
    executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    query::{
        intent::StructuralQuery,
        plan::{OrderSpec, VisibleIndexes},
        preparation::PreparationWork,
    },
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane,
};

fn request(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

#[test]
fn template_candidate_construction_rejects_before_publication_and_shares_warm_authority() {
    let setup = initialize();
    seed_singleton(&setup);
    let catalog = setup
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let schema = catalog.accepted_schema_info();
    let visible = VisibleIndexes::accepted_schema_visible(schema);
    let count = visible.accepted_semantic_index_contracts().len();
    assert!(count > 0, "exercise nonempty accepted index authority");
    let bytes =
        (size_of_val(visible.accepted_semantic_index_contracts()) + 2 * size_of::<usize>()) as u64;
    let query = |label: &str| {
        PreparationWork::run(
            setup.db.request_execution_scope(),
            DiagnosticExecutionLane::TrustedRead,
            |work| {
                StructuralQuery::new(MissingRowPolicy::Ignore).filter_for_schema(
                    schema,
                    &FieldRef::new("label").eq(label),
                    work,
                )
            },
        )
        .unwrap()
        .order_spec(OrderSpec {
            fields: vec![asc("id").lower()],
        })
        .limit(1)
    };
    let queries = [query("singleton"), query("missing"), query("singleton")];
    for lane in [
        DiagnosticExecutionLane::PublicRead,
        DiagnosticExecutionLane::TrustedRead,
    ] {
        for (resource, exact) in [
            (Resource::TemporaryBytes, bytes),
            (Resource::PredicateExpressionSteps, count as u64),
        ] {
            setup.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
            let rejected = request(resource, exact - 1);
            let session = new_request_session_with_root(&rejected);
            for attempt in 1..=2 {
                let error = session
                    .cached_shared_query_plan_for_accepted_authority_with_catalog_and_reuse(
                        catalog.accepted_entity_authority().clone(),
                        &catalog,
                        &queries[0],
                        lane,
                    )
                    .unwrap_err();
                assert!(
                    error
                        .diagnostic_facts()
                        .contains(&(DiagnosticFactTag::BudgetResource, resource.raw(),)),
                    "construction should reject {resource:?}: {error:?}, {:?}",
                    error.diagnostic_facts(),
                );
                assert_eq!(rejected.observed(resource), exact * attempt);
                assert!(
                    error
                        .diagnostic_facts()
                        .contains(&(DiagnosticFactTag::ExecutionLane, lane.raw()))
                );
                assert_eq!(rejected.observed(Resource::RowsVisited), 0);
                assert_eq!(rejected.observed(Resource::QueryExecutions), 0);
                assert_eq!(setup.shared_query_cache_usage_for_tests(), (0, 0));
            }
            let admitted = request(resource, exact);
            let session = new_request_session_with_root(&admitted);
            let (_, reuse) = session
                .cached_shared_query_plan_for_accepted_authority_with_catalog_and_reuse(
                    catalog.accepted_entity_authority().clone(),
                    &catalog,
                    &queries[0],
                    lane,
                )
                .unwrap();
            assert!(!reuse.is_hit());
            assert_eq!(admitted.observed(resource), exact);
            assert_eq!(setup.shared_query_cache_usage_for_tests().0, 1);
            // Both identical memo hits and A/B/A rebinding retain the original
            // candidate array. These limits cover this owner, not all planner work.
            let warm = request(resource, 0);
            let session = new_request_session_with_root(&warm);
            for query in &queries {
                let (_, reuse) = session
                    .cached_shared_query_plan_for_accepted_authority_with_catalog_and_reuse(
                        catalog.accepted_entity_authority().clone(),
                        &catalog,
                        query,
                        lane,
                    )
                    .unwrap();
                assert!(reuse.is_hit());
                assert_eq!(warm.observed(resource), 0);
                assert_eq!(warm.observed(Resource::PlanCompilations), 0);
                assert_eq!(setup.shared_query_cache_usage_for_tests().0, 1);
            }
        }
    }
}
