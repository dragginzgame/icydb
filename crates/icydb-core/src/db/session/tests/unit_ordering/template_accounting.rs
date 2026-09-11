//! Candidate-list construction shares the request; warm template reuse does not
//! allocate another candidate list or publish an incomplete cold artifact.

use super::*;
use crate::db::{
    MissingRowPolicy, RequestExecutionRoot,
    executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    query::{
        intent::StructuralQuery,
        plan::{OrderSpec, OrderTerm, VisibleIndexes},
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
    let queries = ["singleton", "missing", "singleton"].map(|label| indexed_query(&setup, label));
    for lane in [
        DiagnosticExecutionLane::PublicRead,
        DiagnosticExecutionLane::TrustedRead,
    ] {
        let costs = construction_costs(&setup, &queries, lane);
        let (access_order_bytes, access_order_steps) = access_order_cost();
        // A cold plan and the rebound A have identical operands. Only cold
        // preparation constructs candidate backing and selects its initial order.
        assert_eq!(costs[0].0 - costs[2].0, bytes + access_order_bytes);
        assert_eq!(costs[0].1 - costs[2].1, count as u64 + access_order_steps);
        for (resource, exact, rebound) in [
            (
                Resource::TemporaryBytes,
                costs[0].0,
                [costs[1].0, costs[2].0],
            ),
            (
                Resource::PredicateExpressionSteps,
                costs[0].1,
                [costs[1].1, costs[2].1],
            ),
        ] {
            setup.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
            let rejected = request(resource, exact - 1);
            let session = new_request_session_with_root(&rejected);
            let mut previous = 0;
            for _ in 0..2 {
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
                // A retry can exhaust at an earlier construction owner because
                // the same request has already consumed its budget.
                let observed = rejected.observed(resource);
                assert!(observed >= exact && observed > previous);
                previous = observed;
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
            // A memo hit skips construction. A/B/A rebinding rebuilds static
            // metadata but retains the candidate array instead of copying it.
            let warm = request(resource, rebound.iter().sum());
            let session = new_request_session_with_root(&warm);
            let mut expected = 0;
            for (position, query) in queries.iter().enumerate() {
                let (_, reuse) = session
                    .cached_shared_query_plan_for_accepted_authority_with_catalog_and_reuse(
                        catalog.accepted_entity_authority().clone(),
                        &catalog,
                        query,
                        lane,
                    )
                    .unwrap();
                assert!(reuse.is_hit());
                if position > 0 {
                    expected += rebound[position - 1];
                }
                assert_eq!(warm.observed(resource), expected);
                assert_eq!(warm.observed(Resource::PlanCompilations), 0);
                assert_eq!(setup.shared_query_cache_usage_for_tests().0, 1);
            }
        }
    }
}

// Observe shared preparation owners instead of maintaining a second formula for
// projection, metadata, operand copies and access lowering. The test independently
// pins cold-only candidate/order costs, zero-charge memo hits and cache publication
// at the observed exact boundary (including rejection one unit below it).
fn construction_costs(
    setup: &DbSession<TestCanister>,
    queries: &[StructuralQuery; 3],
    lane: DiagnosticExecutionLane,
) -> [(u64, u64); 3] {
    setup.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
    let catalog = setup
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    std::array::from_fn(|position| {
        let root = request(Resource::TemporaryBytes, 16_000_000);
        let session = new_request_session_with_root(&root);
        let (_, reuse) = session
            .cached_shared_query_plan_for_accepted_authority_with_catalog_and_reuse(
                catalog.accepted_entity_authority(),
                &catalog,
                &queries[position],
                lane,
            )
            .unwrap();
        assert_eq!(reuse.is_hit(), position > 0);
        assert_eq!(root.observed(Resource::RowsVisited), 0);
        assert_eq!(root.observed(Resource::QueryExecutions), 0);
        (
            root.observed(Resource::TemporaryBytes),
            root.observed(Resource::PredicateExpressionSteps),
        )
    })
}

const fn access_order_cost() -> (u64, u64) {
    // Initial access selection also copies and canonicalizes the id order.
    // Template rebinding retains its existing topology and skips this work.
    ((size_of::<OrderTerm>() + "id".len()) as u64, 4 + 4)
}

fn indexed_query(setup: &DbSession<TestCanister>, label: &str) -> StructuralQuery {
    let catalog = setup
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    PreparationWork::run(
        setup.db.request_execution_scope(),
        DiagnosticExecutionLane::TrustedRead,
        |work| {
            StructuralQuery::new(MissingRowPolicy::Ignore).filter_for_schema(
                catalog.accepted_schema_info(),
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
}
