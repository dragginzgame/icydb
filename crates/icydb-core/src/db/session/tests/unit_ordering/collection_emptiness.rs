//! Stored emptiness predicates preserve the accepted collection capability boundary.

use super::{bindings_parity::publish_operand_schema, *};
use crate::db::{
    predicate::Predicate, query::predicate::validate_predicate, schema::ValidateError,
};

fn with_stored_operand(
    kind: AcceptedFieldKind,
    input: InputValue,
    check: impl FnOnce(DbSession<TestCanister>) + Send + 'static,
) {
    // Each shape gets a fresh accepted catalog and request state before any write.
    std::thread::spawn(move || {
        let session = initialize();
        publish_operand_schema(&session, kind);
        session
            .execute_trusted_dynamic_insert_batch(
                ENTITY_NAME,
                vec![DynamicStructuralPatch::new(vec![
                    ("id".into(), DynamicWriteCell::Value(InputValue::unit())),
                    ("operand".into(), DynamicWriteCell::Value(input)),
                ])],
            )
            .expect("accepted stored operand");
        check(session);
    })
    .join()
    .expect("collection predicate fixture");
}

#[test]
fn collection_emptiness_rejects_stored_maps_at_the_accepted_query_boundary() {
    for entries in [
        Vec::new(),
        vec![(InputValue::nat64(1), InputValue::nat64(2))],
    ] {
        with_stored_operand(
            AcceptedFieldKind::Map {
                key: Box::new(AcceptedFieldKind::Nat64),
                value: Box::new(AcceptedFieldKind::Nat64),
            },
            InputValue::map(entries),
            |session| {
                let catalog = session
                    .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
                    .unwrap();
                for (predicate, filter) in [
                    (
                        Predicate::IsEmpty {
                            field: "operand".into(),
                        },
                        FilterExpr::is_empty("operand"),
                    ),
                    (
                        Predicate::IsNotEmpty {
                            field: "operand".into(),
                        },
                        FilterExpr::is_not_empty("operand"),
                    ),
                ] {
                    assert!(matches!(
                        validate_predicate(catalog.accepted_schema_info(), &predicate),
                        Err(ValidateError::MapPredicateUnsupported { field }) if field == "operand"
                    ));
                    let query = DynamicQuery::new(ENTITY_NAME).select(["id"]).filter(filter);
                    session
                        .execute_trusted_live_page(&query, None)
                        .expect_err("map predicates reject before runtime");
                }
                assert_sql_length_queries(&session, None);
            },
        );
    }
}

#[test]
fn collection_emptiness_matches_stored_text_list_and_set_cardinality() {
    for empty in [true, false] {
        let items = if empty {
            Vec::new()
        } else {
            vec![InputValue::nat64(1)]
        };
        for (kind, input) in [
            (
                AcceptedFieldKind::Text { max_len: None },
                InputValue::text(if empty { "" } else { "value" }.into()),
            ),
            (
                AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat64)),
                InputValue::list(items.clone()),
            ),
            (
                AcceptedFieldKind::Set(Box::new(AcceptedFieldKind::Nat64)),
                InputValue::list(items.clone()),
            ),
        ] {
            let text = matches!(kind, AcceptedFieldKind::Text { .. });
            with_stored_operand(kind, input, move |session| {
                for (filter, matches) in [
                    (FilterExpr::is_empty("operand"), empty),
                    (FilterExpr::is_not_empty("operand"), !empty),
                ] {
                    let query = DynamicQuery::new(ENTITY_NAME).select(["id"]).filter(filter);
                    let structural = session.execute_trusted_live_page(&query, None).unwrap();
                    assert_eq!(structural.row_count, u32::from(matches));
                }
                assert_sql_length_queries(&session, text.then_some(empty));
            });
        }
    }
}

// SQL LENGTH measures text, not collection cardinality. Positive text controls
// distinguish accepted SQL syntax from the collection type-admission rejection.
fn assert_sql_length_queries(session: &DbSession<TestCanister>, text_empty: Option<bool>) {
    for (operator, positive) in [("=", true), ("<>", false)] {
        let sql = format!("SELECT id FROM Singleton WHERE LENGTH(operand) {operator} 0");
        let dispatch = sql_statement_dispatch(&sql).expect("maintained SQL syntax");
        let direct = session.execute_trusted_sql_query(&sql);
        let parsed = session.execute_trusted_sql_query_with_entity_name(&dispatch, &[]);
        if let Some(empty) = text_empty {
            let SqlStatementResult::Projection { rows: direct, .. } = direct.unwrap() else {
                panic!("projection")
            };
            let (SqlStatementResult::Projection { rows: parsed, .. }, _) = parsed.unwrap() else {
                panic!("projection")
            };
            assert_eq!(direct, parsed);
            assert_eq!(direct.len(), usize::from(empty == positive));
        } else {
            let direct = direct.expect_err("LENGTH does not admit collections");
            let parsed = parsed.expect_err("parsed LENGTH has the same admission");
            assert_eq!(direct.diagnostic(), parsed.diagnostic());
        }
    }
}
