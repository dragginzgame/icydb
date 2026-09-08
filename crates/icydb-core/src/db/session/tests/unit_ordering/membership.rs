//! Compact membership keeps stored-row, NULL and current-binding behavior.

use super::*;

#[test]
fn typed_membership_matches_sql_for_nulls_duplicates_and_negation() {
    let session = initialize();
    seed_singleton(&session);
    let cases = [
        (
            vec![InputValue::text("singleton".into())],
            "('singleton')",
            1,
            0,
        ),
        (
            vec![InputValue::text("missing".into())],
            "('missing')",
            0,
            1,
        ),
        (vec![InputValue::null()], "(NULL)", 0, 0),
        (
            vec![InputValue::text("missing".into()), InputValue::null()],
            "('missing', NULL)",
            0,
            0,
        ),
        (
            vec![
                InputValue::text("singleton".into()),
                InputValue::null(),
                InputValue::text("singleton".into()),
            ],
            "('singleton', NULL, 'singleton')",
            1,
            0,
        ),
    ];
    for (values, literals, positive_count, negative_count) in cases {
        for negated in [false, true] {
            let filter = if negated {
                FilterExpr::not_in("label", values.clone())
            } else {
                FilterExpr::in_list("label", values.clone())
            };
            let query = DynamicQuery::new(ENTITY_NAME)
                .select(["label"])
                .filter(filter);
            let actual = session
                .execute_trusted_live_page(&query, None)
                .expect("typed membership");
            let operator = if negated { "NOT IN" } else { "IN" };
            let expected = sql_rows(
                &session,
                &format!("SELECT label FROM Singleton WHERE label {operator} {literals}"),
            );
            assert_eq!(actual.rows, expected);
            assert_eq!(
                actual.rows.len(),
                if negated {
                    negative_count
                } else {
                    positive_count
                }
            );
        }
    }
    for (filter, count) in [
        (FilterExpr::in_list("label", Vec::<InputValue>::new()), 0),
        (FilterExpr::not_in("label", Vec::<InputValue>::new()), 1),
    ] {
        let query = DynamicQuery::new(ENTITY_NAME)
            .select(["label"])
            .filter(filter);
        assert_eq!(
            session
                .execute_trusted_live_page(&query, None)
                .expect("empty typed set")
                .rows
                .len(),
            count
        );
    }
}

#[test]
fn compact_membership_keeps_schema_rejection_after_a_warm_call() {
    let session = initialize();
    seed_singleton(&session);
    let query = |filter| {
        DynamicQuery::new(ENTITY_NAME)
            .select(["label"])
            .filter(filter)
    };
    session
        .execute_trusted_live_page(&query(FilterExpr::in_list("label", ["singleton"])), None)
        .expect("warm valid membership");
    for filter in [
        FilterExpr::in_list("label", [InputValue::boolean(true)]),
        FilterExpr::in_list(
            "label",
            [
                InputValue::text("singleton".into()),
                InputValue::boolean(true),
            ],
        ),
        FilterExpr::in_list("missing", [InputValue::text("singleton".into())]),
    ] {
        assert!(
            session
                .execute_trusted_live_page(&query(filter), None)
                .is_err()
        );
    }
}
