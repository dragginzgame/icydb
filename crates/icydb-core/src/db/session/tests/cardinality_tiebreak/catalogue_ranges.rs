//! Complete catalogue ranges retain public admission, budgets and paging.

use super::{materialized_sort_admission::summary, scalar_page_limits, *};
use crate::db::{
    RequestExecutionRoot, desc,
    executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    query::admission::{QueryAdmissionAccessKind, QueryBoundKind},
};
use icydb_diagnostic_code::{
    DiagnosticDetail, DiagnosticExecutionBudgetResource as Resource, DiagnosticFactTag,
    QueryReadAdmissionCode,
};

fn seed_catalogue(count: u16, unique: bool) -> Vec<Vec<OutputValue>> {
    scalar_page_limits::initialize_payload_schema(
        vec![
            field(1, "id", 0, AcceptedFieldKind::Nat64),
            field(2, "catalog_order", 1, AcceptedFieldKind::Nat16),
        ],
        vec![PersistedIndexSnapshot::new(
            SchemaIndexId::new(1).unwrap(),
            1,
            "catalog_order_idx".into(),
            STORE_PATH.into(),
            unique,
            PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                FieldId::new(2),
                SchemaFieldSlot::new(1),
                vec!["catalog_order".into()],
                AcceptedFieldKind::Nat16,
                false,
            )]),
            None,
        )],
    );
    let mut expected = Vec::new();
    for offset in 0..count {
        // Cover both type boundaries, gaps, and unexpected high catalogue orders.
        // Reverse IDs so primary order cannot masquerade as catalogue order.
        let id = u64::from(count - offset);
        let order = if offset + 1 == count {
            u16::MAX
        } else if unique {
            offset * 2
        } else {
            offset / 2
        };
        new_request_session(&RequestExecutionRoot::__new_runtime_root())
            .execute_trusted_dynamic_insert_batch(
                ENTITY_NAME,
                vec![DynamicStructuralPatch::new(vec![
                    ("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))),
                    (
                        "catalog_order".into(),
                        DynamicWriteCell::Value(InputValue::nat64(u64::from(order))),
                    ),
                ])],
            )
            .unwrap();
        expected.push((order, id));
    }
    expected.sort_unstable();
    expected
        .into_iter()
        .map(|(order, id)| vec![OutputValue::nat64(id), OutputValue::nat64(u64::from(order))])
        .collect()
}

fn catalogue_query(descending: bool) -> DynamicQuery {
    DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("catalog_order").gte(InputValue::nat64(0)))
        .select(["id", "catalog_order"])
        .order_by(if descending {
            desc("catalog_order")
        } else {
            asc("catalog_order")
        })
        .order_by(if descending { desc("id") } else { asc("id") })
}

fn collect_catalogue(query: &DynamicQuery, trusted: bool) -> Vec<Vec<OutputValue>> {
    let root = RequestExecutionRoot::__new_runtime_root();
    let session = new_request_session(&root);
    let mut rows = Vec::new();
    let mut continuation = None;
    // The harness deliberately reduces physical pages to two rows. A logical
    // LIMIT must not stand in for that page bound or for complete-set exhaustion.
    for _ in 0..100 {
        let page = if trusted {
            session.execute_trusted_live_page(query, continuation.as_deref())
        } else {
            session.execute_public_live_page(query, continuation.as_deref())
        }
        .unwrap();
        assert!(page.rows.len() <= 2);
        rows.extend(page.rows);
        let Some(next) = page.continuation else {
            return rows;
        };
        assert_ne!(Some(&next), continuation.as_ref());
        continuation = Some(next);
    }
    panic!("bounded catalogue fixture must exhaust");
}

#[test]
fn complete_nat16_catalogue_range_preserves_empty_order_overflow_and_resume() {
    for unique in [true, false] {
        for count in [0, 64, 65, 128, 129] {
            // Startup control retains accepted authority across live-store
            // replacement. Isolate each fixture's complete thread-local state.
            std::thread::spawn(move || {
                let expected = seed_catalogue(count, unique);
                for descending in [false, true] {
                    let query = catalogue_query(descending);
                    let mut expected = expected.clone();
                    if descending {
                        expected.reverse();
                    }
                    for _ in 0..2 {
                        let session =
                            new_request_session(&RequestExecutionRoot::__new_runtime_root());
                        let facts = summary(&session, &query);
                        assert_eq!(
                            facts.selected_access(),
                            QueryAdmissionAccessKind::IndexRange
                        );
                        assert_eq!(facts.selected_index(), Some("catalog_order_idx"));
                        // A complete type range supplies indexed access, not a
                        // numerical row-count proof. Runtime budgets still own work.
                        assert_eq!(facts.scan_bound_kind(), QueryBoundKind::Unavailable);
                        assert!(!facts.materialization().materialized_sort());
                        let actual = collect_catalogue(&query, false);
                        assert_eq!(actual, expected);
                        assert_eq!(actual, collect_catalogue(&query, true));
                        // The sentinel, not disappearance of a continuation at the
                        // authored total limit, detects a catalogue exceeding its cap.
                        let sentinel = collect_catalogue(&query.clone().limit(65), false);
                        assert_eq!(sentinel, expected[..expected.len().min(65)]);
                        assert_eq!(sentinel.len() > 64, count > 64);
                    }
                }
            })
            .join()
            .unwrap();
        }
    }
}

#[test]
fn complete_catalogue_range_keeps_work_budgets_and_unconstrained_rejection() {
    seed_catalogue(3, true);
    let query = catalogue_query(false);
    let root = RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::KeyIndexEntriesVisited, 0),
    );
    let error = new_request_session(&root)
        .execute_public_live_page(&query, None)
        .unwrap_err();
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::KeyIndexEntriesVisited.raw(),
    )));
    let root = RequestExecutionRoot::__new_runtime_root();
    let unconstrained = DynamicQuery::new(ENTITY_NAME)
        .order_by(asc("catalog_order"))
        .limit(65);
    let error = new_request_session(&root)
        .execute_public_live_page(&unconstrained, None)
        .unwrap_err();
    assert_eq!(
        error.diagnostic().detail(),
        Some(&DiagnosticDetail::QueryReadAdmission {
            reason: QueryReadAdmissionCode::UnboundedFullScanRejected,
        }),
    );
    assert_eq!(root.observed(Resource::RowsVisited), 0);
}

#[test]
fn complete_text_catalogue_range_preserves_empty_unicode_and_duplicate_keys() {
    let setup = initialize();
    let query = |descending| {
        DynamicQuery::new(ENTITY_NAME)
            .filter(FieldRef::new("common").gte(InputValue::text(String::new())))
            .select(["id"])
            .order_by(if descending {
                desc("common")
            } else {
                asc("common")
            })
            .order_by(if descending { desc("id") } else { asc("id") })
    };
    assert!(collect_catalogue(&query(false), false).is_empty());
    let mut expected = vec![
        ("\0", 9),
        ("", 8),
        ("\u{10ffff}", 7),
        ("a\0", 6),
        ("unexpected", 5),
        ("a", 4),
        ("a", 3),
    ];
    for &(text, id) in &expected {
        insert_row(&setup, id, text, "catalogue");
    }
    expected.sort_unstable();
    let expected = expected
        .into_iter()
        .map(|(_, id)| vec![OutputValue::nat64(id)])
        .collect::<Vec<_>>();
    for descending in [false, true] {
        let query = query(descending);
        let mut expected = expected.clone();
        if descending {
            expected.reverse();
        }
        for _ in 0..2 {
            let facts = summary(&setup, &query);
            assert_eq!(
                facts.selected_access(),
                QueryAdmissionAccessKind::IndexRange
            );
            assert_eq!(facts.selected_index(), Some("a_common_idx"));
            let actual = collect_catalogue(&query, false);
            assert_eq!(actual, expected);
            assert_eq!(actual, collect_catalogue(&query, true));
        }
    }
}
