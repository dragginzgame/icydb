//! Unordered DISTINCT shares canonical projection state without inventing order.

use super::*;
use crate::{
    db::{
        RequestExecutionRoot,
        executor::{
            SharedPreparedExecutionPlan, StructuralProjectionRequest,
            StructuralProjectionScanBudget,
            budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
            execute_structural_projection_page, execute_structural_projection_rows,
        },
        query::preparation::with_preparation_work,
        schema::empty_accepted_enum_catalog_for_tests,
    },
    error::ErrorClass,
    types::Decimal,
    value::PublicValue,
};
use icydb_diagnostic_code::{
    DiagnosticCode, DiagnosticDetail, DiagnosticExecutionBudgetResource as Resource,
    DiagnosticFactTag, RuntimeBoundaryCode, SqlWriteBoundaryCode,
};

fn initialize_components() -> DbSession<TestCanister> {
    hybrid_components::initialize_component_schema(
        AcceptedFieldKind::Nat64,
        empty_accepted_enum_catalog_for_tests(),
    );
    let session = new_request_session(&RequestExecutionRoot::__new_runtime_root());
    for (id, category, operand, label) in [
        (1, 3, 7, "alpha"),
        (2, 3, 7, "beta"),
        (3, 3, 9, "alpha"),
        (4, 3, 7, "alpha"),
        (5, 9, 9, "excluded"),
    ] {
        session
            .execute_trusted_dynamic_insert_batch(
                ENTITY_NAME,
                vec![DynamicStructuralPatch::new(vec![
                    ("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))),
                    (
                        "category".into(),
                        DynamicWriteCell::Value(InputValue::nat64(category)),
                    ),
                    (
                        "operand".into(),
                        DynamicWriteCell::Value(InputValue::nat64(operand)),
                    ),
                    (
                        "label".into(),
                        DynamicWriteCell::Value(InputValue::text(label.into())),
                    ),
                ])],
            )
            .unwrap();
    }
    session
}

// Unordered SQL promises a set, not a traversal order. Length plus membership
// checks prove exact coverage and no duplicates without constraining the route.
fn assert_unordered(actual: &[Vec<OutputValue>], expected: &[Vec<OutputValue>]) {
    assert_eq!(actual.len(), expected.len());
    for row in expected {
        assert!(actual.contains(row), "missing {row:?} from {actual:?}");
    }
}

fn prepared(session: &DbSession<TestCanister>, fields: &[&str]) -> SharedPreparedExecutionPlan {
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let query = with_preparation_work(|work| {
        StructuralQuery::new(MissingRowPolicy::Ignore)
            .select_fields(fields.iter().copied())
            .distinct()
            .filter_for_schema(
                catalog.accepted_schema_info(),
                &FieldRef::new("category").eq(InputValue::nat64(3)),
                work,
            )
    })
    .unwrap();
    let plan = session
        .cached_shared_query_plan_for_accepted_authority_with_catalog(
            catalog.accepted_entity_authority(),
            &catalog,
            &query,
            DiagnosticExecutionLane::TrustedRead,
        )
        .unwrap();
    assert!(plan.logical_plan().resolved_order().is_none());
    plan
}

#[test]
fn unordered_distinct_covering_hybrid_and_row_projections_preserve_windows() {
    let session = initialize_components();
    for (projection, expected) in [
        (
            "operand, category",
            vec![
                vec![OutputValue::nat64(7), OutputValue::nat64(3)],
                vec![OutputValue::nat64(9), OutputValue::nat64(3)],
            ],
        ),
        (
            "operand, label",
            vec![
                vec![OutputValue::nat64(7), OutputValue::text("alpha".into())],
                vec![OutputValue::nat64(7), OutputValue::text("beta".into())],
                vec![OutputValue::nat64(9), OutputValue::text("alpha".into())],
            ],
        ),
        (
            "label",
            vec![
                vec![OutputValue::text("alpha".into())],
                vec![OutputValue::text("beta".into())],
            ],
        ),
    ] {
        let sql = format!("SELECT DISTINCT {projection} FROM PlannerRow WHERE category = 3");
        for _ in 0..2 {
            assert_unordered(&projection_rows(&session, &sql), &expected);
            for (window, offset, limit) in [
                (" LIMIT 0", 0, 0),
                (" LIMIT 1", 0, 1),
                (" LIMIT 1 OFFSET 1", 1, 1),
                (" LIMIT 9 OFFSET 1", 1, 9),
                (" LIMIT 1 OFFSET 99", 99, 1),
            ] {
                let error = session
                    .execute_trusted_sql_query(&format!("{sql}{window}"))
                    .unwrap_err();
                assert_eq!(
                    error.diagnostic_code(),
                    DiagnosticCode::QueryUnorderedPagination
                );
                let rows =
                    projection_rows(&session, &format!("{sql} ORDER BY {projection}{window}"));
                assert_eq!(rows.len(), expected.len().saturating_sub(offset).min(limit));
                assert!(rows.iter().all(|row| expected.contains(row)));
                for (position, row) in rows.iter().enumerate() {
                    assert!(!rows[..position].contains(row));
                }
            }
        }
    }
    let plain = projection_rows(&session, "SELECT * FROM PlannerRow WHERE category = 3");
    assert_eq!(plain.len(), 4);
    for _ in 0..2 {
        assert_unordered(
            &projection_rows(
                &session,
                "SELECT DISTINCT * FROM PlannerRow WHERE category = 3",
            ),
            &plain,
        );
        assert!(
            projection_rows(
                &session,
                "SELECT DISTINCT operand, label FROM PlannerRow WHERE category = 99"
            )
            .is_empty()
        );
        assert_unordered(
            &projection_rows(
                &session,
                "SELECT DISTINCT operand, label FROM PlannerRow WHERE category = 3 AND label = 'alpha'",
            ),
            &[
                vec![OutputValue::nat64(7), OutputValue::text("alpha".into())],
                vec![OutputValue::nat64(9), OutputValue::text("alpha".into())],
            ],
        );
    }
}

#[test]
fn unordered_distinct_expression_and_null_keys_keep_canonical_equality() {
    let session = initialize_components();
    for (expression, expected) in [
        (
            "MOD(operand, 2)",
            vec![vec![OutputValue::decimal(Decimal::from(1_u64))]],
        ),
        (
            "NULLIF(label, 'alpha')",
            vec![
                vec![OutputValue::null()],
                vec![OutputValue::text("beta".into())],
            ],
        ),
        (
            "CASE WHEN operand = 7 THEN label ELSE 'other' END",
            vec![
                vec![OutputValue::text("alpha".into())],
                vec![OutputValue::text("beta".into())],
                vec![OutputValue::text("other".into())],
            ],
        ),
    ] {
        for _ in 0..2 {
            assert_unordered(
                &projection_rows(
                    &session,
                    &format!(
                        "SELECT DISTINCT {expression} AS result FROM PlannerRow WHERE category = 3"
                    ),
                ),
                &expected,
            );
        }
    }
}

#[test]
fn unordered_distinct_nullable_collection_projection_preserves_owned_keys() {
    let kind = AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat64));
    let decode = FieldStorageDecode::ByKind;
    let codec = kind.leaf_codec_for_storage(decode);
    scalar_page_limits::initialize_payload_schema(
        vec![
            field(1, "id", 0, AcceptedFieldKind::Nat64),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(2),
                "items".into(),
                SchemaFieldSlot::new(1),
                kind,
                vec![],
                true,
                SchemaInsertDefault::None,
                decode,
                codec,
            ),
        ],
        vec![],
    );
    let session = new_request_session(&RequestExecutionRoot::__new_runtime_root());
    for (id, values) in [
        (1, Some(vec![1, 2])),
        (2, None),
        (3, Some(vec![2, 1])),
        (4, Some(vec![1, 2])),
        (5, None),
        (6, Some(vec![])),
    ] {
        let cell = values.map_or(DynamicWriteCell::Null, |values| {
            DynamicWriteCell::Value(InputValue::list(
                values.into_iter().map(InputValue::nat64).collect(),
            ))
        });
        session
            .execute_trusted_dynamic_insert_batch(
                ENTITY_NAME,
                vec![DynamicStructuralPatch::new(vec![
                    ("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))),
                    ("items".into(), cell),
                ])],
            )
            .unwrap();
    }
    let list = |values: &[u64]| {
        OutputValue::list(values.iter().copied().map(PublicValue::Nat64).collect())
    };
    let expected = vec![
        vec![list(&[1, 2])],
        vec![OutputValue::null()],
        vec![list(&[2, 1])],
        vec![list(&[])],
    ];
    for _ in 0..2 {
        assert_unordered(
            &projection_rows(&session, "SELECT DISTINCT items FROM PlannerRow"),
            &expected,
        );
        assert_eq!(
            projection_rows(&session, "SELECT DISTINCT * FROM PlannerRow").len(),
            6
        );
    }
}

#[test]
fn unordered_distinct_structural_reads_keep_order_optional_and_cursor_order_required() {
    initialize_components();
    let root = RequestExecutionRoot::__new_runtime_root();
    let session = new_request_session(&root);
    for _ in 0..2 {
        let plan = prepared(&session, &["operand", "category"]);
        let error = execute_structural_projection_page(
            &session.db,
            StructuralProjectionRequest::new(plan, DiagnosticExecutionLane::TrustedRead)
                .with_cursor_emission(2),
        )
        .err()
        .unwrap();
        assert_eq!(error.class(), ErrorClass::InvariantViolation);
        assert_eq!(root.observed(Resource::RowsVisited), 0);
    }
    let rows = execute_structural_projection_rows(
        &session.db,
        StructuralProjectionRequest::new(
            prepared(&session, &["operand", "category"]),
            DiagnosticExecutionLane::TrustedRead,
        ),
    )
    .unwrap()
    .into_value_rows();
    assert_eq!(rows.len(), 2);
}

fn default_cursor_query() -> DynamicQuery {
    // A bounded exact-key control admits both lanes without borrowing a sort
    // promise from the unrelated composite index. Paging supplies accepted PK order.
    DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("id").in_list([4, 2, 1, 3].map(InputValue::nat64)))
        .select(["operand", "label"])
        .distinct_for_internal_execution()
}

fn collect_live(
    query: &DynamicQuery,
    public: bool,
    mut cursor: Option<String>,
) -> (Vec<Vec<OutputValue>>, Vec<(String, usize)>) {
    let mut rows = vec![];
    let mut tokens = vec![];
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
    panic!("default-order DISTINCT must exhaust within the bounded page count");
}

#[test]
fn unordered_distinct_default_cursor_order_preserves_live_and_exhaustive_results() {
    initialize_components();
    let query = default_cursor_query();
    assert!(query.order_terms().is_empty());
    let expected = vec![
        vec![OutputValue::nat64(7), OutputValue::text("alpha".into())],
        vec![OutputValue::nat64(7), OutputValue::text("beta".into())],
        vec![OutputValue::nat64(9), OutputValue::text("alpha".into())],
    ];
    for public in [false, true] {
        let (rows, tokens) = collect_live(&query, public, None);
        assert_eq!(rows, expected);
        assert!(!tokens.is_empty());
        for (token, offset) in tokens {
            assert_eq!(
                collect_live(&query, public, Some(token)).0,
                expected[offset..]
            );
        }
        let mut cursor = None;
        let mut proof = None;
        let mut rows = vec![];
        let mut continued = false;
        for _ in 0..16 {
            let root = RequestExecutionRoot::__new_runtime_root();
            let session = new_request_session(&root);
            let page = if public {
                session.execute_public_exhaustive_page(&query, cursor.as_deref(), proof.as_ref())
            } else {
                session.execute_trusted_exhaustive_page(&query, cursor.as_deref(), proof.as_ref())
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
        assert!(cursor.is_none() && continued);
        assert_eq!(rows, expected);
    }
}

#[test]
fn unordered_distinct_preserves_typed_state_storage_and_scan_limits() {
    initialize_components();
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
            .execute_trusted_sql_query(
                "SELECT DISTINCT operand, category FROM PlannerRow WHERE category = 3",
            )
            .unwrap_err();
        assert!(
            error
                .diagnostic_facts()
                .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
        );
        assert!(matches!(
            error.diagnostic().detail(),
            Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::ExecutionBudgetExceeded,
            })
        ));
    }
    for limit in [3, 4] {
        let root = RequestExecutionRoot::__new_runtime_root();
        let session = new_request_session(&root);
        let result = execute_structural_projection_rows(
            &session.db,
            StructuralProjectionRequest::new(
                prepared(&session, &["operand", "category"]),
                DiagnosticExecutionLane::Mutation,
            )
            .with_scan_budget(StructuralProjectionScanBudget::try_new(limit).unwrap()),
        );
        if limit == 4 {
            assert_eq!(result.unwrap().row_count(), 2);
        } else {
            let error = result.err().unwrap();
            assert!(matches!(
                error.diagnostic().detail(),
                Some(DiagnosticDetail::SqlWriteBoundary {
                    boundary: SqlWriteBoundaryCode::WriteScanBudgetExceeded,
                })
            ));
        }
    }
}
