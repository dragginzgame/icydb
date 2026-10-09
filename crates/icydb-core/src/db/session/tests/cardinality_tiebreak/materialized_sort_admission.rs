//! Public admission and diagnostics share the canonical scalar sort requirement.

use super::*;
use crate::db::{
    OrderTerm, desc,
    query::{
        admission::{
            QueryAdmissionAccessKind, QueryAdmissionLane, QueryAdmissionPolicy,
            QueryAdmissionRejection, QueryAdmissionSummary, QueryBoundKind,
        },
        preparation::with_preparation_work,
    },
};
use icydb_diagnostic_code::{DiagnosticDetail, QueryReadAdmissionCode};

pub(super) fn summary(
    session: &DbSession<TestCanister>,
    query: &DynamicQuery,
) -> QueryAdmissionSummary {
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let mut structural = StructuralQuery::new(MissingRowPolicy::Ignore);
    if let Some(filter) = query.filter_expr() {
        structural = with_preparation_work(|work| {
            structural.filter_for_schema(catalog.accepted_schema_info(), filter, work)
        })
        .unwrap();
    }
    structural = structural.select_fields(["id"]);
    // Public pages insert accepted primary-key order when none is authored.
    structural = structural.order_spec(OrderSpec {
        fields: if query.order_terms().is_empty() {
            catalog
                .accepted_schema_info()
                .primary_key_names()
                .iter()
                .map(|name| asc(name.as_str()).lower())
                .collect()
        } else {
            query
                .order_terms()
                .iter()
                .cloned()
                .map(OrderTerm::lower)
                .collect()
        },
    });
    if let Some(limit) = query.row_limit() {
        structural = structural.limit(limit);
    }
    let prepared = session
        .cached_shared_query_plan_for_accepted_authority_with_catalog(
            catalog.accepted_entity_authority(),
            &catalog,
            &structural,
            DiagnosticExecutionLane::PublicRead,
        )
        .unwrap();
    QueryAdmissionSummary::from_plan(QueryAdmissionLane::PublicRead, prepared.logical_plan())
        .unwrap()
}

fn secondary_query(filter: FilterExpr) -> DynamicQuery {
    DynamicQuery::new(ENTITY_NAME)
        .filter(filter)
        .select(["id"])
        .order_by(asc("wide_branch"))
        .limit(5)
}

#[test]
fn public_sort_admission_rejects_secondary_materialization_on_cold_and_warm_plans() {
    let session = initialize();
    seed_rows(&session);
    let cases = [
        (
            secondary_query(FieldRef::new("rare").eq(InputValue::text("group-a".into()))),
            QueryAdmissionAccessKind::IndexPrefix,
        ),
        (
            secondary_query(FieldRef::new("rare").gt(InputValue::text("group-a".into()))),
            QueryAdmissionAccessKind::IndexRange,
        ),
        (
            secondary_query(FieldRef::new("rare").in_list([
                InputValue::text("group-a".into()),
                InputValue::text("group-b".into()),
            ])),
            QueryAdmissionAccessKind::IndexMultiLookup,
        ),
    ];
    for (query, kind) in cases {
        for _ in 0..2 {
            let facts = summary(&session, &query);
            assert_eq!(facts.selected_access(), kind);
            assert!(facts.materialization().materialized_sort());
            assert_eq!(facts.materialization().materialized_rows(), None);
            assert_eq!(
                facts.materialization().row_bound_kind(),
                QueryBoundKind::Unavailable
            );
            assert_eq!(
                QueryAdmissionPolicy::default_bounded_read()
                    .evaluate(facts)
                    .rejection(),
                Some(QueryAdmissionRejection::SortRequiresMaterialization),
            );
            let error = session.execute_public_live_page(&query, None).unwrap_err();
            assert_eq!(
                error.diagnostic().detail(),
                Some(&DiagnosticDetail::QueryReadAdmission {
                    reason: QueryReadAdmissionCode::SortRequiresMaterialization,
                }),
            );
            assert!(session.execute_trusted_live_page(&query, None).is_ok());
        }
    }
}

#[test]
fn public_sort_admission_preserves_residual_filters_on_ordered_and_materialized_routes() {
    let session = initialize();
    seed_rows(&session);
    let query = secondary_query(FilterExpr::and(vec![
        FieldRef::new("rare").eq(InputValue::text("group-a".into())),
        FieldRef::new("wide_fixed").eq(InputValue::text("all".into())),
    ]));
    for _ in 0..2 {
        let facts = summary(&session, &query);
        assert_eq!(facts.selected_index(), Some("b_wide_branch_idx"));
        assert!(!facts.materialization().materialized_sort());
        let public = session.execute_public_live_page(&query, None).unwrap();
        let trusted = session.execute_trusted_live_page(&query, None).unwrap();
        assert_eq!(
            public.rows,
            vec![vec![OutputValue::nat64(0)], vec![OutputValue::nat64(2)]]
        );
        assert_eq!(public.rows, trusted.rows);
        // The same residual predicate cannot make mixed-direction ordering
        // streamable through these uniformly ordered indexes.
        let unsupported = query.clone().order_by(desc("id"));
        let facts = summary(&session, &unsupported);
        assert_eq!(
            facts.selected_access(),
            QueryAdmissionAccessKind::IndexPrefix
        );
        assert!(facts.materialization().materialized_sort());
        assert_eq!(
            QueryAdmissionPolicy::default_bounded_read()
                .evaluate(facts)
                .rejection(),
            Some(QueryAdmissionRejection::SortRequiresMaterialization),
        );
        let error = session
            .execute_public_live_page(&unsupported, None)
            .unwrap_err();
        assert_eq!(
            error.diagnostic().detail(),
            Some(&DiagnosticDetail::QueryReadAdmission {
                reason: QueryReadAdmissionCode::SortRequiresMaterialization,
            })
        );
    }
}

#[test]
fn public_sort_admission_keeps_exact_primary_key_candidates_bounded() {
    let session = initialize();
    seed_rows(&session);
    let single = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("id").eq(InputValue::nat64(1)))
        .select(["id"])
        .order_by(asc("wide_branch"));
    let multiple = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("id").in_list((0..4).map(InputValue::nat64)))
        .select(["id"])
        .order_by(asc("wide_branch"));
    for (query, kind, bound) in [
        (single, QueryAdmissionAccessKind::ByKey, 1),
        (multiple.clone(), QueryAdmissionAccessKind::ByKeys, 4),
        (multiple.limit(1), QueryAdmissionAccessKind::ByKeys, 4),
    ] {
        for _ in 0..2 {
            let facts = summary(&session, &query);
            assert_eq!(facts.selected_access(), kind);
            assert_eq!(facts.scan_bound(), Some(u64::from(bound)));
            assert!(facts.materialization().materialized_sort());
            assert_eq!(facts.materialization().materialized_rows(), Some(bound));
            assert_eq!(
                facts.materialization().row_bound_kind(),
                QueryBoundKind::Exact
            );
            assert_eq!(
                QueryAdmissionPolicy::default_bounded_read()
                    .evaluate(facts)
                    .rejection(),
                None
            );
            assert!(session.execute_public_live_page(&query, None).is_ok());
        }
    }
}

#[test]
fn public_sort_admission_preserves_ordered_access_and_materialized_boundary_controls() {
    let session = initialize();
    seed_rows(&session);
    let no_order = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("rare").eq(InputValue::text("group-a".into())))
        .select(["id"])
        .limit(5);
    let primary_order = no_order.clone().order_by(asc("id"));
    let secondary_order = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("wide_fixed").eq(InputValue::text("all".into())))
        .select(["id"])
        .order_by(asc("wide_fixed"))
        .order_by(asc("wide_branch"))
        .limit(5);
    let descending_boundary = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("wide_fixed").eq(InputValue::text("all".into())))
        .select(["id"])
        .order_by(desc("wide_fixed"))
        .order_by(desc("wide_branch"))
        .limit(5);
    for query in [
        no_order,
        primary_order,
        secondary_order,
        descending_boundary,
    ] {
        let facts = summary(&session, &query);
        assert!(!facts.materialization().materialized_sort());
        assert_eq!(
            QueryAdmissionPolicy::default_bounded_read()
                .evaluate(facts)
                .rejection(),
            None
        );
        assert!(session.execute_public_live_page(&query, None).is_ok());
    }
}

#[test]
fn public_sort_admission_explain_reports_sort_and_candidate_bounds() {
    let session = initialize();
    seed_rows(&session);
    for (sql, rows) in [
        (
            "SELECT id FROM PlannerRow WHERE rare = 'group-a' ORDER BY wide_branch LIMIT 5",
            "none",
        ),
        (
            "SELECT id FROM PlannerRow WHERE id IN (0, 1, 2, 3) ORDER BY wide_branch LIMIT 1",
            "4",
        ),
    ] {
        for _ in 0..2 {
            let SqlStatementResult::Explain(text) = session
                .execute_trusted_sql_query(&format!("EXPLAIN EXECUTION VERBOSE {sql}"))
                .unwrap()
            else {
                panic!("expected text EXPLAIN");
            };
            assert!(text.contains("materialized_sort=true"));
            assert!(text.contains(&format!("materialized_rows={rows}")));
            let SqlStatementResult::Explain(json) = session
                .execute_trusted_sql_query(&format!("EXPLAIN EXECUTION JSON {sql}"))
                .unwrap()
            else {
                panic!("expected JSON EXPLAIN");
            };
            assert!(json.contains("\"materialized_sort\":true"));
            let json_rows = if rows == "none" { "null" } else { rows };
            assert!(json.contains(&format!("\"materialized_rows\":{json_rows}")));
        }
    }
}
