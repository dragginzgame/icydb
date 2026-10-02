//! Disjoint multi-lookup access preserves projected DISTINCT and page progress.

use super::*;
use crate::db::{
    RequestExecutionRoot, desc,
    executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
};
use crate::types::Decimal;
use icydb_diagnostic_code::{
    DiagnosticDetail, DiagnosticExecutionBudgetResource as Resource, DiagnosticFactTag,
    RuntimeBoundaryCode,
};

fn initialize_branches() -> DbSession<TestCanister> {
    let session = initialize();
    for (id, common, rare) in [
        (10, "a", "same"),
        (11, "a", "same"),
        (12, "a", "same"),
        (3, "b", "same"),
        (4, "b", "same"),
        (20, "c", "other"),
        (1, "d", "excluded"),
    ] {
        insert_row(&session, id, common, rare);
    }
    session
}

fn expected_groups(descending: bool) -> Vec<Vec<OutputValue>> {
    let mut rows = [("a", "same"), ("b", "same"), ("c", "other")]
        .map(|(common, rare)| {
            vec![
                OutputValue::text(common.into()),
                OutputValue::text(rare.into()),
            ]
        })
        .to_vec();
    if descending {
        rows.reverse();
    }
    rows
}

fn query(descending: bool) -> DynamicQuery {
    DynamicQuery::new(ENTITY_NAME)
        .filter(
            FieldRef::new("common")
                .in_list(["c", "a", "b", "a"].map(|name| InputValue::text(name.into()))),
        )
        .select(["common", "rare"])
        .order_by(if descending {
            desc("common")
        } else {
            asc("common")
        })
        .distinct_for_internal_execution()
}

#[test]
fn branch_distinct_sql_preserves_rows_windows_and_repeated_literals() {
    let session = initialize_branches();
    for (direction, descending) in [("ASC", false), ("DESC", true)] {
        let mut plain = [
            ("a", "same"),
            ("a", "same"),
            ("a", "same"),
            ("b", "same"),
            ("b", "same"),
            ("c", "other"),
        ]
        .map(|(common, rare)| {
            vec![
                OutputValue::text(common.into()),
                OutputValue::text(rare.into()),
            ]
        })
        .to_vec();
        if descending {
            plain.reverse();
        }
        let expected = expected_groups(descending);
        for literals in ["'a','b','c'", "'c','a','b','a'"] {
            let source =
                format!("FROM PlannerRow WHERE common IN ({literals}) ORDER BY common {direction}");
            for _ in 0..2 {
                assert_eq!(
                    projection_rows(&session, &format!("SELECT common, rare {source}")),
                    plain
                );
                for (window, offset, limit) in [
                    ("", 0, 3),
                    (" LIMIT 1", 0, 1),
                    (" LIMIT 1 OFFSET 1", 1, 1),
                    (" LIMIT 5 OFFSET 1", 1, 2),
                ] {
                    assert_eq!(
                        projection_rows(
                            &session,
                            &format!("SELECT DISTINCT common, rare {source}{window}")
                        ),
                        expected[offset..offset + limit]
                    );
                }
                let SqlStatementResult::Explain(explain) = session
                    .execute_trusted_sql_query(&format!(
                        "EXPLAIN EXECUTION SELECT DISTINCT common, rare {source}"
                    ))
                    .unwrap()
                else {
                    panic!("execution descriptor required")
                };
                assert!(explain.contains("IndexMultiLookup"), "{explain}");
                assert!(explain.contains("materialized_sort=false"), "{explain}");
            }
        }
    }
}

#[test]
fn branch_distinct_expression_projection_replays_canonical_duplicates() {
    let session = initialize_branches();
    for (direction, expected) in [
        (
            "ASC",
            vec![("a", 0_u64), ("a", 1), ("b", 1), ("b", 0), ("c", 0)],
        ),
        (
            "DESC",
            vec![("c", 0), ("b", 0), ("b", 1), ("a", 0), ("a", 1)],
        ),
    ] {
        let rows = expected
            .into_iter()
            .map(|(name, value)| {
                vec![
                    OutputValue::text(name.into()),
                    OutputValue::decimal(Decimal::from(value)),
                ]
            })
            .collect::<Vec<_>>();
        for _ in 0..2 {
            assert_eq!(
                projection_rows(
                    &session,
                    &format!(
                        "SELECT DISTINCT common, MOD(id, 2) FROM PlannerRow WHERE common IN ('c','a','b','a') ORDER BY common {direction}"
                    )
                ),
                rows
            );
            assert_eq!(
                projection_rows(
                    &session,
                    &format!(
                        "SELECT DISTINCT common, MOD(id, 2) FROM PlannerRow WHERE common IN ('c','a','b','a') ORDER BY common {direction} LIMIT 2 OFFSET 1"
                    )
                ),
                rows[1..3]
            );
        }
    }
}

fn collect_live(
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
        .unwrap();
        rows.extend(page.rows);
        let Some(next) = page.continuation else {
            return (rows, tokens);
        };
        assert_ne!(cursor.as_ref(), Some(&next));
        assert!(!tokens.iter().any(|(token, _)| token == &next));
        tokens.push((next.clone(), rows.len()));
        cursor = Some(next);
    }
    panic!("branch DISTINCT must exhaust within the bounded page count");
}

#[test]
fn branch_distinct_live_resume_preserves_page_union_and_every_suffix() {
    initialize_branches();
    for descending in [false, true] {
        let query = query(descending);
        let expected = expected_groups(descending);
        for public in [false, true] {
            for _ in 0..2 {
                let (rows, tokens) = collect_live(&query, public, None);
                assert_eq!(rows, expected);
                assert!(!tokens.is_empty());
                for (token, offset) in tokens {
                    assert_eq!(
                        collect_live(&query, public, Some(token)).0,
                        expected[offset..]
                    );
                }
                assert_eq!(
                    collect_live(&query.clone().limit(1), public, None).0,
                    expected[..1]
                );
            }
        }
    }
}

#[test]
fn branch_distinct_exhaustive_resume_preserves_proof_and_results() {
    initialize_branches();
    for descending in [false, true] {
        let query = query(descending);
        for public in [false, true] {
            let mut cursor = None;
            let mut proof = None;
            let mut rows = Vec::new();
            let mut continued = false;
            for _ in 0..16 {
                let root = RequestExecutionRoot::__new_runtime_root();
                let session = new_request_session(&root);
                let page = if public {
                    session.execute_public_exhaustive_page(
                        &query,
                        cursor.as_deref(),
                        proof.as_ref(),
                    )
                } else {
                    session.execute_trusted_exhaustive_page(
                        &query,
                        cursor.as_deref(),
                        proof.as_ref(),
                    )
                }
                .unwrap();
                rows.extend(page.rows);
                proof = Some(page.proof);
                let Some(next) = page.continuation else {
                    cursor = None;
                    break;
                };
                assert_ne!(cursor.as_ref(), Some(&next));
                cursor = Some(next);
                continued = true;
            }
            assert!(cursor.is_none());
            assert!(continued);
            assert_eq!(rows, expected_groups(descending));
        }
    }
}

#[test]
fn branch_distinct_preserves_typed_state_and_access_budget_failures() {
    initialize_branches();
    for resource in [
        Resource::GroupDistinctEntries,
        Resource::GroupDistinctStateBytes,
        Resource::RowsVisited,
        Resource::StoredBytesRead,
    ] {
        let root = RequestExecutionRoot::new_for_tests(
            HardExecutionBudget::uniform_for_tests(
                16_000_000,
                HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
            )
            .with_limit_for_tests(resource, 0),
        );
        let error = new_request_session(&root)
            .execute_trusted_live_page(&query(false), None)
            .unwrap_err();
        assert!(
            error
                .diagnostic_facts()
                .contains(&(DiagnosticFactTag::BudgetResource, resource.raw(),))
        );
        assert!(
            matches!(
                error.diagnostic().detail(),
                Some(DiagnosticDetail::RuntimeBoundary {
                    boundary: RuntimeBoundaryCode::ExecutionBudgetExceeded,
                })
            ),
            "{resource:?}: {error:?}"
        );
    }
}

#[test]
fn branch_distinct_preserves_identity_primary_order_and_composite_controls() {
    let session = initialize_branches();
    for order in ["common ASC", "common DESC", "id ASC", "id DESC"] {
        let sql = format!(
            "SELECT id, common FROM PlannerRow WHERE common IN ('c','a','b','a') ORDER BY {order}"
        );
        let expected = projection_rows(&session, &sql);
        assert_eq!(expected.len(), 6);
        for _ in 0..2 {
            assert_eq!(
                projection_rows(&session, &sql.replacen("SELECT", "SELECT DISTINCT", 1)),
                expected
            );
        }
    }
    for (direction, descending) in [("ASC", false), ("DESC", true)] {
        let expected = if descending {
            // DESC also reverses each branch's primary-key suffix. Global
            // projected DISTINCT keeps the first representative in that order.
            vec![
                ("y", "a"),
                ("y", "b"),
                ("y", "d"),
                ("x", "c"),
                ("x", "a"),
                ("x", "b"),
            ]
        } else {
            vec![
                ("x", "b"),
                ("x", "a"),
                ("x", "c"),
                ("y", "d"),
                ("y", "b"),
                ("y", "a"),
            ]
        };
        let expected = expected
            .into_iter()
            .map(|(branch, common)| {
                vec![
                    OutputValue::text("all".into()),
                    OutputValue::text(branch.into()),
                    OutputValue::text(common.into()),
                ]
            })
            .collect::<Vec<_>>();
        for _ in 0..2 {
            assert_eq!(
                projection_rows(
                    &session,
                    &format!(
                        "SELECT DISTINCT wide_fixed, wide_branch, common FROM PlannerRow WHERE wide_fixed = 'all' AND wide_branch IN ('x','y','x') ORDER BY wide_branch {direction}"
                    )
                ),
                expected
            );
        }
    }
}

// Each accepted fixture publishes one current schema identity. Exercise unique
// and non-unique catalog content in independent tests, rather than replacing
// content beneath a retained identity/cache within one request thread.
fn numeric_prefixes(unique: bool) {
    super::scalar_page_limits::initialize_payload_schema(
        vec![
            field(1, "id", 0, AcceptedFieldKind::Nat64),
            field(2, "code", 1, AcceptedFieldKind::Nat64),
            field(3, "payload", 2, AcceptedFieldKind::Text { max_len: None }),
        ],
        vec![PersistedIndexSnapshot::new(
            SchemaIndexId::new(1).unwrap(),
            1,
            "code_idx".into(),
            STORE_PATH.into(),
            unique,
            PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                FieldId::new(2),
                SchemaFieldSlot::new(1),
                vec!["code".into()],
                AcceptedFieldKind::Nat64,
                false,
            )]),
            None,
        )],
    );
    let session = new_request_session(&RequestExecutionRoot::__new_runtime_root());
    for (id, code, payload) in [(10, 1, "same"), (3, 2, "same"), (20, 3, "other")] {
        session
            .execute_trusted_dynamic_insert_batch(
                ENTITY_NAME,
                vec![DynamicStructuralPatch::new(vec![
                    ("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))),
                    (
                        "code".into(),
                        DynamicWriteCell::Value(InputValue::nat64(code)),
                    ),
                    (
                        "payload".into(),
                        DynamicWriteCell::Value(InputValue::text(payload.into())),
                    ),
                ])],
            )
            .unwrap();
    }
    for (direction, descending) in [("ASC", false), ("DESC", true)] {
        let mut expected = [(1, "same"), (2, "same"), (3, "other")]
            .map(|(code, payload)| {
                vec![OutputValue::nat64(code), OutputValue::text(payload.into())]
            })
            .to_vec();
        if descending {
            expected.reverse();
        }
        for _ in 0..2 {
            assert_eq!(
                projection_rows(
                    &session,
                    &format!(
                        "SELECT DISTINCT code, payload FROM PlannerRow WHERE code IN (3,1,2,1) ORDER BY code {direction}"
                    )
                ),
                expected
            );
        }
    }
}

#[test]
fn branch_distinct_preserves_unique_numeric_prefixes() {
    numeric_prefixes(true);
}

#[test]
fn branch_distinct_preserves_nonunique_numeric_prefixes() {
    numeric_prefixes(false);
}
