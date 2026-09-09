//! Finalized secondary order stays consistent across cached covering consumers.

use super::*;
use crate::db::{
    direction::Direction,
    query::plan::{
        CoveringProjectionOrder, OrderDirection, OrderTerm,
        covering_read_execution_plan_with_schema_info,
    },
};

#[test]
fn cached_secondary_order_preserves_covering_and_distinct_seek_contracts() {
    let session = initialize();
    seed_rows(&session);
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    for (direction, physical) in [
        (OrderDirection::Asc, Direction::Asc),
        (OrderDirection::Desc, Direction::Desc),
    ] {
        for distinct in [false, true] {
            let mut query = StructuralQuery::new(MissingRowPolicy::Ignore)
                .select_fields(["rare"])
                .order_spec(OrderSpec {
                    fields: vec![OrderTerm::field("rare", direction)],
                })
                .limit(2);
            if distinct {
                query = query.distinct();
            }
            // Exercise the real finalizer and the warm resident, including the
            // implicit primary-key suffix rather than a hand-built route profile.
            for _ in 0..2 {
                let (prepared, _) = session
                    .cached_shared_query_plan_for_accepted_authority_with_catalog_and_reuse(
                        catalog.accepted_entity_authority(),
                        &catalog,
                        &query,
                        DiagnosticExecutionLane::TrustedRead,
                    )
                    .unwrap();
                let plan = prepared.logical_plan();
                let order = plan
                    .planner_route_profile()
                    .secondary_order_contract()
                    .unwrap();
                assert_eq!(order.non_primary_key_terms(), ["rare"]);
                assert_eq!(order.direction(), direction);
                let covering = covering_read_execution_plan_with_schema_info(
                    catalog.accepted_schema_info(),
                    plan,
                    true,
                )
                .unwrap();
                assert_eq!(
                    covering.order_contract,
                    CoveringProjectionOrder::IndexOrder(physical)
                );
                let seek = covering.ordered_distinct_group_seek_contract();
                assert_eq!(seek.is_some(), distinct);
                if let Some(seek) = seek {
                    assert_eq!(seek.direction(), physical);
                    assert_eq!(seek.output_window(), (0, 2));
                }
            }
        }
    }
}

#[test]
fn secondary_order_reuse_preserves_cold_and_warm_projection_results() {
    let session = initialize();
    seed_rows(&session);
    for (direction, first, second) in [
        ("ASC", "group-a", "group-b"),
        ("DESC", "group-b", "group-a"),
    ] {
        for distinct in [false, true] {
            let modifier = if distinct { "DISTINCT " } else { "" };
            let sql =
                format!("SELECT {modifier}rare FROM PlannerRow ORDER BY rare {direction} LIMIT 2");
            let expected = vec![
                vec![OutputValue::text(first.to_string())],
                vec![OutputValue::text(
                    if distinct { second } else { first }.to_string(),
                )],
            ];
            for _ in 0..2 {
                assert_eq!(projection_rows(&session, &sql), expected);
                let SqlStatementResult::Explain(explain) = session
                    .execute_trusted_sql_query(&format!("EXPLAIN EXECUTION {sql}"))
                    .unwrap()
                else {
                    panic!("execution explain payload required");
                };
                assert!(explain.contains("OrderByAccessSatisfied"), "{explain}");
                assert!(!explain.contains("OrderByMaterializedSort"), "{explain}");
            }
        }
    }
}
