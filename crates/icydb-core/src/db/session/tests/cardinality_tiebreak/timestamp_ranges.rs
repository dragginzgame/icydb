//! Signed timestamp indexes agree with stored values across the epoch.

use super::*;
use crate::{
    db::{RequestExecutionRoot, desc},
    types::Timestamp,
};

fn seed_timestamps(unique: bool) -> Vec<(i64, u64)> {
    scalar_page_limits::initialize_payload_schema(
        vec![
            field(1, "id", 0, AcceptedFieldKind::Nat64),
            field(2, "stamp", 1, AcceptedFieldKind::Timestamp),
        ],
        vec![PersistedIndexSnapshot::new(
            SchemaIndexId::new(1).unwrap(),
            1,
            "stamp_idx".into(),
            STORE_PATH.into(),
            unique,
            PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                FieldId::new(2),
                SchemaFieldSlot::new(1),
                vec!["stamp".into()],
                AcceptedFieldKind::Timestamp,
                false,
            )]),
            None,
        )],
    );
    let mut millis = vec![
        i64::MIN,
        i64::MIN + 1,
        -1_000,
        -2,
        -1,
        0,
        1,
        2,
        1_000,
        i64::MAX - 1,
        i64::MAX,
    ];
    if !unique {
        millis.extend([-1, 0, 1]);
    }
    let mut rows = Vec::new();
    for (offset, stamp) in millis.into_iter().enumerate() {
        // Reverse row identity so primary order cannot accidentally prove index order.
        let id = 100 - u64::try_from(offset).unwrap();
        new_request_session(&RequestExecutionRoot::__new_runtime_root())
            .execute_trusted_dynamic_insert_batch(
                ENTITY_NAME,
                vec![DynamicStructuralPatch::new(vec![
                    ("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))),
                    (
                        "stamp".into(),
                        DynamicWriteCell::Value(InputValue::timestamp(Timestamp::from_millis(
                            stamp,
                        ))),
                    ),
                ])],
            )
            .unwrap();
        rows.push((stamp, id));
    }
    rows.sort_unstable();
    rows
}

fn collect_pages(
    query: &DynamicQuery,
    mut cursor: Option<String>,
) -> (Vec<Vec<OutputValue>>, Vec<(String, usize)>) {
    let mut rows = Vec::new();
    let mut tokens = Vec::new();
    for _ in 0..32 {
        let page = new_request_session(&RequestExecutionRoot::__new_runtime_root())
            .execute_trusted_live_page(query, cursor.as_deref())
            .unwrap();
        rows.extend(page.rows);
        let Some(next) = page.continuation else {
            return (rows, tokens);
        };
        assert_ne!(Some(&next), cursor.as_ref());
        assert!(!tokens.iter().any(|(token, _)| token == &next));
        tokens.push((next.clone(), rows.len()));
        cursor = Some(next);
    }
    panic!("timestamp range must exhaust within the bounded page count");
}

fn assert_index_range(query: &DynamicQuery, descending: bool) {
    let root = RequestExecutionRoot::__new_runtime_root();
    let session = new_request_session(&root);
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let structural = crate::db::query::preparation::with_preparation_work(|work| {
        StructuralQuery::new(MissingRowPolicy::Ignore).filter_for_schema(
            catalog.accepted_schema_info(),
            query.filter_expr().unwrap(),
            work,
        )
    })
    .unwrap()
    .order_spec(OrderSpec {
        fields: vec![if descending {
            desc("stamp").lower()
        } else {
            asc("stamp").lower()
        }],
    });
    let (prepared, _) = session
        .cached_shared_query_plan_for_accepted_authority_with_catalog_and_reuse(
            catalog.accepted_entity_authority(),
            &catalog,
            &structural,
            DiagnosticExecutionLane::TrustedRead,
        )
        .unwrap();
    assert!(
        prepared
            .logical_plan()
            .access
            .as_index_range_path()
            .is_some()
    );
}

fn assert_timestamp_index_reads(unique: bool) {
    let rows = seed_timestamps(unique);
    for &(stamp, _) in &rows {
        let query = DynamicQuery::new(ENTITY_NAME)
            .filter(FieldRef::new("stamp").eq(InputValue::timestamp(Timestamp::from_millis(stamp))))
            .order_by(asc("stamp"));
        let expected = rows
            .iter()
            .filter(|(value, _)| *value == stamp)
            .map(|(value, id)| {
                vec![
                    OutputValue::nat64(*id),
                    OutputValue::timestamp(Timestamp::from_millis(*value)),
                ]
            })
            .collect::<Vec<_>>();
        assert_eq!(collect_pages(&query, None).0, expected);
    }
    for (lower, upper, inclusive) in [
        (i64::MIN, i64::MAX, true),
        (-1_000, 1_000, false),
        (-2, 2, true),
        (-2, 2, false),
        (i64::MIN, -1, true),
        (0, i64::MAX, true),
        (-1, -1, true),
        (-1, -1, false),
    ] {
        for descending in [false, true] {
            let lo = InputValue::timestamp(Timestamp::from_millis(lower));
            let hi = InputValue::timestamp(Timestamp::from_millis(upper));
            let query = DynamicQuery::new(ENTITY_NAME)
                .filter(FilterExpr::and(vec![
                    if inclusive {
                        FieldRef::new("stamp").gte(lo)
                    } else {
                        FieldRef::new("stamp").gt(lo)
                    },
                    if inclusive {
                        FieldRef::new("stamp").lte(hi)
                    } else {
                        FieldRef::new("stamp").lt(hi)
                    },
                ]))
                .order_by(if descending {
                    desc("stamp")
                } else {
                    asc("stamp")
                });
            if lower < upper {
                assert_index_range(&query, descending);
            }
            let mut expected = rows
                .iter()
                .filter(|(stamp, _)| {
                    if inclusive {
                        lower <= *stamp && *stamp <= upper
                    } else {
                        lower < *stamp && *stamp < upper
                    }
                })
                .map(|(stamp, id)| {
                    vec![
                        OutputValue::nat64(*id),
                        OutputValue::timestamp(Timestamp::from_millis(*stamp)),
                    ]
                })
                .collect::<Vec<_>>();
            if descending {
                expected.reverse();
            }
            let (actual, tokens) = collect_pages(&query, None);
            assert_eq!(
                actual, expected,
                "unique={unique} bounds={lower}..{upper} inclusive={inclusive} descending={descending}"
            );
            if expected.len() > 2 {
                assert!(!tokens.is_empty());
            }
            for (token, offset) in tokens {
                assert_eq!(collect_pages(&query, Some(token)).0, expected[offset..]);
            }
            // Reuse the cached preparation with a total result limit.
            let limited = new_request_session(&RequestExecutionRoot::__new_runtime_root())
                .execute_trusted_live_page(&query.limit(1), None)
                .unwrap();
            assert_eq!(limited.rows, expected[..expected.len().min(1)]);
            assert!(limited.continuation.is_none());
        }
    }
}

#[test]
fn nonunique_timestamp_index_preserves_equality_ranges_and_every_resume_suffix() {
    assert_timestamp_index_reads(false);
}

#[test]
fn unique_timestamp_index_preserves_equality_ranges_and_every_resume_suffix() {
    assert_timestamp_index_reads(true);
}
