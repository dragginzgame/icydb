//! Finalized secondary order stays consistent across cached covering consumers.

use super::*;
use crate::db::{
    RequestExecutionRoot, desc,
    direction::Direction,
    query::plan::{
        CoveringProjectionOrder, CoveringReadFieldSource, OrderDirection, OrderTerm,
        covering_hybrid_projection_execution_plan_with_schema_info,
        covering_read_execution_plan_with_schema_info,
    },
};
use crate::value::PublicValue;
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

#[test]
fn execution_explain_projects_the_same_order_pushdown_fact_on_cold_and_warm_calls() {
    let _setup = initialize_long_secondary_branch();
    let cases = [
        (
            "SELECT id FROM PlannerRow WHERE common = 'everyone' ORDER BY common, id LIMIT 5",
            "eligible(index=a_common_idx,prefix_len=1)",
        ),
        (
            "SELECT id FROM PlannerRow WHERE common = 'everyone' ORDER BY common DESC, id DESC LIMIT 5",
            "eligible(index=a_common_idx,prefix_len=1)",
        ),
        (
            "SELECT id FROM PlannerRow WHERE rare = 'b' ORDER BY wide_branch LIMIT 5",
            "rejected(OrderFieldsDoNotMatchIndex(",
        ),
        (
            "SELECT id FROM PlannerRow WHERE rare > 'a' ORDER BY wide_branch LIMIT 5",
            "rejected(AccessPathIndexRangeUnsupported(",
        ),
        (
            "SELECT id FROM PlannerRow WHERE common = 'everyone' ORDER BY id LIMIT 5",
            "not_applicable",
        ),
        (
            "SELECT id FROM PlannerRow WHERE common = 'everyone'",
            "not_applicable",
        ),
        (
            "SELECT common, COUNT(*) FROM PlannerRow WHERE common = 'everyone' GROUP BY common ORDER BY common LIMIT 5",
            "not_applicable",
        ),
    ];
    let root = RequestExecutionRoot::__new_runtime_root();
    let reader = new_request_session(&root);
    for (query, expected) in cases {
        for _ in 0..2 {
            let SqlStatementResult::Explain(text) = reader
                .execute_trusted_sql_query(&format!("EXPLAIN EXECUTION VERBOSE {query}"))
                .unwrap_or_else(|error| panic!("execution EXPLAIN {query}: {error:?}"))
            else {
                panic!("expected verbose execution explanation");
            };
            let logical = text
                .lines()
                .find_map(|line| line.strip_prefix("diag.p.order_pushdown="))
                .unwrap();
            let route = text
                .lines()
                .find_map(|line| line.strip_prefix("diag.r.secondary_order_pushdown="))
                .unwrap();
            assert_eq!(logical, route, "{query}");
            assert!(route.starts_with(expected), "{query}: {route}");
            assert_eq!(root.observed(Resource::RowsVisited), 0);
        }
    }
    // Diagnostic preparation must leave the maintained ordered execution lane intact.
    let query = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("common").eq("everyone"))
        .select(["id"])
        .order_by(asc("common"))
        .order_by(asc("id"))
        .limit(5);
    let (control, _) = collect_secondary_pages(&query, false, None);
    let SqlStatementResult::Projection { rows, .. } =
        reader.execute_trusted_sql_query(cases[0].0).unwrap()
    else {
        panic!("expected stored projection");
    };
    assert_eq!(rows.len(), 5);
    assert_eq!(rows, control);
}

fn initialize_long_secondary_branch() -> DbSession<TestCanister> {
    let session = initialize();
    for id in 0..10 {
        let rare = match id {
            0 => "a",
            9 => "c",
            _ => "b",
        };
        insert_row(&session, id, "everyone", rare);
    }
    session
}

fn secondary_membership() -> FilterExpr {
    FieldRef::new("rare").in_list(["c", "b", "missing", "a", "b"])
}

// Replay the real authenticated tokens, including boundaries inside the long
// branch. Each page gets a fresh request budget and the same registry identity.
fn collect_secondary_pages(
    query: &DynamicQuery,
    public: bool,
    mut cursor: Option<String>,
) -> (Vec<Vec<OutputValue>>, Vec<(String, usize)>) {
    let mut rows = Vec::new();
    let mut tokens = Vec::new();
    for _ in 0..16 {
        let root = RequestExecutionRoot::__new_runtime_root();
        let session = new_request_session(&root);
        let page = if public {
            session.execute_public_live_page(query, cursor.as_deref())
        } else {
            session.execute_trusted_live_page(query, cursor.as_deref())
        }
        .unwrap_or_else(|error| panic!("public={public}, query={query:?}: {error:?}"));
        rows.extend(page.rows);
        let Some(next) = page.continuation else {
            return (rows, tokens);
        };
        assert_ne!(cursor.as_ref(), Some(&next));
        assert!(!tokens.iter().any(|(token, _)| token == &next));
        tokens.push((next.clone(), rows.len()));
        cursor = Some(next);
    }
    panic!("secondary pages must exhaust within the bounded page count");
}

#[test]
fn secondary_in_pages_preserve_long_branches_and_every_resume_suffix() {
    use crate::db::query::admission::QueryAdmissionAccessKind;

    let session = initialize_long_secondary_branch();
    for descending in [false, true] {
        for row_backed in [false, true] {
            let fields = if row_backed {
                vec!["id", "common"]
            } else {
                vec!["id"]
            };
            let query = DynamicQuery::new(ENTITY_NAME)
                .filter(secondary_membership())
                .select(fields)
                .order_by(if descending {
                    desc("rare")
                } else {
                    asc("rare")
                });
            assert_eq!(
                super::materialized_sort_admission::summary(&session, &query).selected_access(),
                QueryAdmissionAccessKind::IndexMultiLookup
            );
            let mut expected = (0..10)
                .map(|id| {
                    let mut row = vec![OutputValue::nat64(id)];
                    if row_backed {
                        row.push(OutputValue::text("everyone".into()));
                    }
                    row
                })
                .collect::<Vec<_>>();
            if descending {
                expected.reverse();
            }
            for capacity in [0, 4 * 1024 * 1024] {
                session.clear_shared_query_cache_for_tests(capacity);
                for public in [false, true] {
                    for _ in 0..2 {
                        let (rows, tokens) = collect_secondary_pages(&query, public, None);
                        assert_eq!(rows, expected);
                        assert!(tokens.len() >= 4);
                        for (token, offset) in tokens {
                            assert_eq!(
                                collect_secondary_pages(&query, public, Some(token)).0,
                                expected[offset..]
                            );
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn secondary_in_pages_match_range_and_residual_window_controls() {
    use crate::db::query::admission::QueryAdmissionAccessKind;
    let setup = initialize_long_secondary_branch();
    for descending in [false, true] {
        let base = DynamicQuery::new(ENTITY_NAME)
            .select(["id", "common"])
            .order_by(if descending {
                desc("rare")
            } else {
                asc("rare")
            });
        let range = FilterExpr::and(vec![
            FieldRef::new("rare").gte("a"),
            FieldRef::new("rare").lte("c"),
        ]);
        for public in [false, true] {
            let control =
                collect_secondary_pages(&base.clone().filter(range.clone()), public, None).0;
            assert_eq!(control.len(), 10);
            for (filter, limit, expected) in [
                (secondary_membership(), 5, control[..5].to_vec()),
                (
                    FilterExpr::and(vec![
                        secondary_membership(),
                        // Without wide_fixed, this composite suffix is a
                        // residual rather than a competing primary-key range.
                        FieldRef::new("wide_branch").eq("y"),
                    ]),
                    10,
                    control
                        .iter()
                        .filter(
                            |row| matches!(row[0].as_public(), PublicValue::Nat64(id) if !id.is_multiple_of(2)),
                        )
                        .cloned()
                        .collect(),
                ),
            ] {
                let query = base.clone().filter(filter).limit(limit);
                assert_eq!(
                    super::materialized_sort_admission::summary(&setup, &query).selected_access(),
                    QueryAdmissionAccessKind::IndexMultiLookup
                );
                assert_eq!(collect_secondary_pages(&query, public, None).0, expected);
            }
        }
    }
}

#[test]
fn primary_ordered_index_sets_keep_bounded_resume_reads() {
    use crate::db::query::admission::QueryAdmissionAccessKind;

    let setup = initialize_long_secondary_branch();
    for descending in [false, true] {
        for (filter, kind) in [
            (
                // Two merged branches fit this harness's four-entry page
                // envelope; the wider family needs a larger indivisible unit.
                FieldRef::new("rare").in_list(["b", "a", "b"]),
                QueryAdmissionAccessKind::IndexMultiLookup,
            ),
            (
                FilterExpr::and(vec![
                    FieldRef::new("wide_fixed").eq("all"),
                    FieldRef::new("wide_branch").in_list(["x", "y"]),
                ]),
                QueryAdmissionAccessKind::IndexBranchSet,
            ),
        ] {
            // The maintained branch-set planner proves only ascending PK
            // order; descending selects an ordinary prefix instead.
            if descending && kind == QueryAdmissionAccessKind::IndexBranchSet {
                continue;
            }
            let query = DynamicQuery::new(ENTITY_NAME)
                .filter(filter)
                .select(["id", "common"])
                .order_by(if descending { desc("id") } else { asc("id") });
            assert_eq!(
                super::materialized_sort_admission::summary(&setup, &query).selected_access(),
                kind
            );
            for public in [false, true] {
                let mut cursor = None;
                let mut rows = Vec::new();
                for _ in 0..16 {
                    let root = RequestExecutionRoot::__new_runtime_root();
                    let session = new_request_session(&root);
                    let page = if public {
                        session.execute_public_live_page(&query, cursor.as_deref())
                    } else {
                        session.execute_trusted_live_page(&query, cursor.as_deref())
                    }
                    .unwrap_or_else(|error| {
                        panic!("descending={descending}, kind={kind:?}, public={public}, emitted={}: {error:?}", rows.len())
                    });
                    assert!(root.observed(Resource::RowsVisited) <= 3);
                    rows.extend(page.rows);
                    cursor = page.continuation;
                    if cursor.is_none() {
                        break;
                    }
                }
                assert!(cursor.is_none());
                let end = if kind == QueryAdmissionAccessKind::IndexMultiLookup {
                    9
                } else {
                    10
                };
                let mut expected = (0..end)
                    .map(|id| vec![OutputValue::nat64(id), OutputValue::text("everyone".into())])
                    .collect::<Vec<_>>();
                if descending {
                    expected.reverse();
                }
                assert_eq!(rows, expected);
            }
        }
    }
}

#[test]
fn primary_ordered_index_merge_rejects_oversized_page_unit() {
    use icydb_diagnostic_code::{DiagnosticFactTag, ErrorCode};

    initialize_long_secondary_branch();
    let query = DynamicQuery::new(ENTITY_NAME)
        .filter(secondary_membership())
        .select(["id", "common"])
        .order_by(asc("id"));
    for public in [false, true] {
        let root = RequestExecutionRoot::__new_runtime_root();
        let session = new_request_session(&root);
        let error = if public {
            session.execute_public_live_page(&query, None)
        } else {
            session.execute_trusted_live_page(&query, None)
        }
        .unwrap_err();
        assert_eq!(
            error.diagnostic().error_code(),
            ErrorCode::RUNTIME_BOUNDARY_PAGE_UNIT_TOO_LARGE
        );
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::KeyIndexEntriesVisited.raw()
        )));
        assert_eq!(root.observed(Resource::RowsVisited), 0);
    }
}

// Page size is two in this maintained session harness. A three-row budget
// admits the page plus lookahead, but cannot admit rereading a consumed prefix.
pub(super) fn bounded_secondary_request(rows: u64) -> RequestExecutionRoot {
    use crate::db::executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom};
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::RowsVisited, rows),
    )
}

fn collect_bounded_secondary_pages(
    query: &DynamicQuery,
    public: bool,
    mut cursor: Option<String>,
) -> (Vec<Vec<OutputValue>>, Vec<(String, usize)>) {
    let mut rows = Vec::new();
    let mut tokens = Vec::new();
    for _ in 0..16 {
        let root = bounded_secondary_request(3);
        let session = new_request_session(&root);
        let page = if public {
            session.execute_public_live_page(query, cursor.as_deref())
        } else {
            session.execute_trusted_live_page(query, cursor.as_deref())
        }
        .unwrap();
        assert!(root.observed(Resource::RowsVisited) <= 3);
        rows.extend(page.rows);
        let Some(next) = page.continuation else {
            return (rows, tokens);
        };
        assert_ne!(cursor.as_ref(), Some(&next));
        tokens.push((next.clone(), rows.len()));
        cursor = Some(next);
    }
    panic!("bounded secondary pages must exhaust");
}

#[test]
fn secondary_index_resume_seeks_before_row_budget_and_replays_every_suffix() {
    use crate::db::query::admission::QueryAdmissionAccessKind;
    let setup = initialize_long_secondary_branch();
    for descending in [false, true] {
        for (filter, field, kind, mut ids) in [
            (
                secondary_membership(),
                "rare",
                QueryAdmissionAccessKind::IndexMultiLookup,
                (0..10).collect::<Vec<u64>>(),
            ),
            (
                FieldRef::new("rare").eq("b"),
                "rare",
                QueryAdmissionAccessKind::IndexPrefix,
                (1..9).collect(),
            ),
            (
                FilterExpr::and(vec![
                    FieldRef::new("rare").gte("a"),
                    FieldRef::new("rare").lte("c"),
                ]),
                "rare",
                QueryAdmissionAccessKind::IndexRange,
                (0..10).collect(),
            ),
            (
                FieldRef::new("wide_fixed").eq("all"),
                "wide_branch",
                QueryAdmissionAccessKind::IndexPrefix,
                vec![0, 2, 4, 6, 8, 1, 3, 5, 7, 9],
            ),
            (
                FilterExpr::and(vec![
                    FieldRef::new("wide_fixed").eq("all"),
                    FieldRef::new("wide_branch").gte("x"),
                ]),
                "wide_branch",
                QueryAdmissionAccessKind::IndexRange,
                vec![0, 2, 4, 6, 8, 1, 3, 5, 7, 9],
            ),
        ] {
            if descending {
                ids.reverse();
            }
            let expected = ids
                .into_iter()
                .map(|id| vec![OutputValue::nat64(id), OutputValue::text("everyone".into())])
                .collect::<Vec<_>>();
            let query = DynamicQuery::new(ENTITY_NAME)
                .filter(filter)
                .select(["id", "common"])
                .order_by(if descending { desc(field) } else { asc(field) });
            assert_eq!(
                super::materialized_sort_admission::summary(&setup, &query).selected_access(),
                kind
            );
            for capacity in [0, 4 * 1024 * 1024] {
                setup.clear_shared_query_cache_for_tests(capacity);
                for public in [false, true] {
                    for _ in 0..2 {
                        let (rows, tokens) = collect_bounded_secondary_pages(&query, public, None);
                        assert_eq!(rows, expected);
                        assert!(!tokens.is_empty());
                        for (token, offset) in tokens {
                            assert_eq!(
                                collect_bounded_secondary_pages(&query, public, Some(token)).0,
                                expected[offset..]
                            );
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn secondary_index_resume_keeps_typed_budget_rejection() {
    use icydb_diagnostic_code::{DiagnosticFactTag, ErrorCode};
    initialize_long_secondary_branch();
    let query = DynamicQuery::new(ENTITY_NAME)
        .filter(secondary_membership())
        .select(["id", "common"])
        .order_by(asc("rare"));
    for public in [false, true] {
        // Cursor envelopes authenticate the issuing lane; resume a token from
        // the same lane to reach row-budget admission instead of token rejection.
        let first_session = new_request_session(&bounded_secondary_request(3));
        let first = if public {
            first_session.execute_public_live_page(&query, None)
        } else {
            first_session.execute_trusted_live_page(&query, None)
        }
        .unwrap();
        let root = bounded_secondary_request(0);
        let session = new_request_session(&root);
        let error = if public {
            session.execute_public_live_page(&query, first.continuation.as_deref())
        } else {
            session.execute_trusted_live_page(&query, first.continuation.as_deref())
        }
        .unwrap_err();
        assert_eq!(
            error.diagnostic().error_code(),
            ErrorCode::RUNTIME_BOUNDARY_EXECUTION_BUDGET_EXCEEDED,
            "public={public}: {error:?}, {:?}",
            error.diagnostic_facts()
        );
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::RowsVisited.raw()
        )));
    }
}

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
fn cached_hybrid_admission_preserves_row_backed_projection_results() {
    let session = initialize();
    seed_rows(&session);
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    for fields in [
        vec!["rare"],
        vec!["rare", "common"],
        vec!["rare", "common", "wide_branch"],
    ] {
        let query = StructuralQuery::new(MissingRowPolicy::Ignore)
            .select_fields(fields.clone())
            .order_spec(OrderSpec {
                fields: vec![OrderTerm::field("rare", OrderDirection::Asc)],
            });
        let sql = format!(
            "SELECT {} FROM PlannerRow ORDER BY rare ASC",
            fields.join(", ")
        );
        let expected: Vec<_> = (0u64..12)
            .map(|id| {
                let values = [
                    if id < 6 { "group-a" } else { "group-b" },
                    "everyone",
                    if id.is_multiple_of(2) { "x" } else { "y" },
                ];
                values[..fields.len()]
                    .iter()
                    .map(|value| OutputValue::text((*value).to_string()))
                    .collect::<Vec<_>>()
            })
            .collect();
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
            let hybrid = covering_hybrid_projection_execution_plan_with_schema_info(
                catalog.accepted_schema_info(),
                plan,
                true,
            );
            if fields.len() == 1 {
                assert!(hybrid.is_none());
                assert!(
                    covering_read_execution_plan_with_schema_info(
                        catalog.accepted_schema_info(),
                        plan,
                        true,
                    )
                    .is_some()
                );
            } else {
                assert_eq!(
                    hybrid
                        .unwrap()
                        .fields
                        .iter()
                        .filter(|field| matches!(field.source, CoveringReadFieldSource::RowField))
                        .count(),
                    fields.len() - 1
                );
            }
            assert_eq!(projection_rows(&session, &sql), expected);
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
