//! Stored nested projection parity across dynamic pages and fixed SQL.

use super::{bindings_parity::publish_operand_schema, *};

#[test]
fn stored_nested_direct_projection_preserves_unique_and_repeated_outputs() {
    for items in [0, 4] {
        std::thread::spawn(move || {
            let session = initialize();
            publish_operand_schema(
                &session,
                AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Map {
                    key: Box::new(AcceptedFieldKind::Text { max_len: Some(8) }),
                    value: Box::new(AcceptedFieldKind::Blob { max_len: Some(128) }),
                })),
            );
            let input = InputValue::list(
                (0..items)
                    .map(|_| {
                        InputValue::map(vec![(
                            InputValue::text("payload".into()),
                            InputValue::blob(vec![7; 128]),
                        )])
                    })
                    .collect(),
            );
            let expected = OutputValue::list(
                (0..items)
                    .map(|_| {
                        OutputValue::map(vec![(
                            OutputValue::text("payload".into()).into_public(),
                            OutputValue::blob(vec![7; 128]).into_public(),
                        )])
                        .into_public()
                    })
                    .collect(),
            );
            session
                .execute_trusted_dynamic_insert_batch(
                    ENTITY_NAME,
                    vec![DynamicStructuralPatch::new(vec![
                        ("id".into(), DynamicWriteCell::Value(InputValue::unit())),
                        ("operand".into(), DynamicWriteCell::Value(input)),
                    ])],
                )
                .unwrap();
            for (fields, sql, expected_row) in [
                (
                    vec!["operand"],
                    "SELECT operand FROM Singleton",
                    vec![expected.clone()],
                ),
                (
                    vec!["operand", "id", "operand"],
                    "SELECT operand, id, operand FROM Singleton",
                    vec![expected.clone(), OutputValue::unit(), expected],
                ),
            ] {
                let query = DynamicQuery::new(ENTITY_NAME).select(fields.clone());
                let page = new_request_session()
                    .execute_trusted_live_page(&query, None)
                    .unwrap();
                assert_eq!(page.rows, vec![expected_row.clone()]);
                assert_eq!(page.continuation, None);
                let SqlStatementResult::Projection { rows, .. } = new_request_session()
                    .execute_trusted_sql_query(sql)
                    .unwrap()
                else {
                    panic!("projection");
                };
                assert_eq!(rows, page.rows);
                // A residual collection filter materializes the projected root
                // before retained output; both accepted and rejected rows keep
                // the same unique/repeated projection semantics.
                for (filter, matches) in [
                    (FilterExpr::is_empty("operand"), items == 0),
                    (FilterExpr::is_not_empty("operand"), items != 0),
                ] {
                    let query = DynamicQuery::new(ENTITY_NAME)
                        .select(fields.clone())
                        .filter(filter);
                    let filtered = new_request_session()
                        .execute_trusted_live_page(&query, None)
                        .unwrap();
                    assert_eq!(
                        filtered.rows,
                        if matches {
                            vec![expected_row.clone()]
                        } else {
                            vec![]
                        }
                    );
                    assert_eq!(filtered.continuation, None);
                }
            }
        })
        .join()
        .expect("stored direct projection fixture");
    }
}
