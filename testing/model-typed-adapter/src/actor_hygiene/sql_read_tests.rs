//! Execute generated public/trusted SQL capabilities without an endpoint host.

use super::generated;
use crate::actor_schema::{ActorHygieneEntity as Row, ActorHygieneEntityInsert as Insert};
use runtime_api::{
    db::{
        TypedWriteAdapter,
        sql::{SqlQueryPageResult, SqlQueryResult},
    },
    value::OutputValue,
};

fn seed(rows: usize) {
    generated::__drive_native_database_for_tests().unwrap();
    runtime_api::db::with_request_execution(|| {
        let session = generated::db().unwrap();
        let binding = Row::typed_binding(&session).unwrap();
        let inputs = (0..rows)
            .map(|index| {
                Insert::from(Row {
                    id: 999,
                    label: format!("row-{index}"),
                    nickname: None,
                })
                .encode_write(&binding)
                .unwrap()
            })
            .collect();
        session.execute_trusted_typed_write_batch(inputs).unwrap();
    });
}

#[test]
fn generated_public_sql_uses_the_production_hundred_row_page_and_total_limit() {
    seed(105);
    // An exact finite key set carries public admission proof; an arbitrary
    // primary-key range does not satisfy the public index requirement.
    let keys = (1..=105)
        .map(|key| key.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT id, label, nickname FROM ActorHygieneEntity WHERE id IN ({keys}) ORDER BY id ASC LIMIT 103"
    );
    let mut cursor = None;
    let mut ids = Vec::new();
    for expected in [100, 3] {
        let result = runtime_api::db::with_request_execution(|| {
            generated::endpoint_handlers::sql_query_public::<false>(sql.clone(), cursor.clone())
        })
        .unwrap();
        // The new page, receipt and token all survive the actual wire boundary.
        let encoded = runtime_api::wire::encode_one(&result).unwrap();
        let decoded: SqlQueryPageResult = runtime_api::wire::decode_one(&encoded).unwrap();
        assert_eq!(result, decoded);
        let SqlQueryPageResult::Scalar {
            projection,
            continuation,
            work,
        } = result
        else {
            panic!("public scalar SELECT must return page progress");
        };
        assert_eq!(projection.row_count, expected);
        assert_eq!(work.result_rows, expected);
        assert!(work.entries_visited <= 10_000);
        ids.extend(projection.rows.into_iter().map(|row| {
            assert_eq!(row[2], OutputValue::null());
            row[0].clone()
        }));
        cursor = continuation;
        assert_eq!(cursor.is_some(), expected == 100);
    }
    assert_eq!(ids, (1..=103).map(OutputValue::nat64).collect::<Vec<_>>());
    let result = runtime_api::db::with_request_execution(|| {
        generated::endpoint_handlers::sql_query_trusted::<false>(
            "SELECT id FROM ActorHygieneEntity ORDER BY id ASC".into(),
        )
    })
    .unwrap();
    let SqlQueryResult::Projection(rows) = result else {
        panic!("operator SQL remains unpaged")
    };
    assert_eq!(rows.row_count, 105);
}

#[test]
fn generated_public_sql_keeps_introspection_and_mutation_gates() {
    seed(1);
    runtime_api::db::with_request_execution(|| {
        for introspection in [false, true] {
            let read = || {
                if introspection {
                    generated::endpoint_handlers::sql_query_public::<true>(
                        "SELECT id FROM ActorHygieneEntity LIMIT 1".into(),
                        None,
                    )
                } else {
                    generated::endpoint_handlers::sql_query_public::<false>(
                        "SELECT id FROM ActorHygieneEntity LIMIT 1".into(),
                        None,
                    )
                }
            };
            // Introspection approval cannot turn a full scan into a public read.
            assert!(read().is_err());
        }
        assert!(
            generated::endpoint_handlers::sql_query_public::<false>("SHOW ENTITIES".into(), None,)
                .is_err()
        );
        assert!(matches!(
            generated::endpoint_handlers::sql_query_public::<true>("SHOW ENTITIES".into(), None,)
                .unwrap(),
            SqlQueryPageResult::Statement(SqlQueryResult::ShowEntities { .. })
        ));
        assert!(
            generated::endpoint_handlers::sql_query_public::<true>(
                "DELETE FROM ActorHygieneEntity WHERE id = 1".into(),
                None,
            )
            .is_err()
        );
    });
    let trusted = runtime_api::db::with_request_execution(|| {
        generated::endpoint_handlers::sql_query_trusted::<false>(
            "SELECT id FROM ActorHygieneEntity".into(),
        )
    })
    .unwrap();
    assert!(matches!(trusted, SqlQueryResult::Projection(rows) if rows.row_count == 1));
}

#[test]
fn parsed_public_sql_binds_current_operands_without_reusing_trusted_authority() {
    seed(3);
    let dispatch =
        runtime_api::db::sql_statement_dispatch("SELECT id FROM ActorHygieneEntity WHERE id = ?")
            .unwrap();
    for id in [1, 3, 999] {
        runtime_api::db::with_request_execution(|| {
            let session = generated::db().unwrap();
            let bindings = [runtime_api::value::InputValue::nat64(id)];
            let _ = session
                .execute_trusted_sql_query_dispatch(&dispatch, &bindings)
                .unwrap();
            let page = session
                .execute_public_sql_query_page_dispatch(&dispatch, &bindings, None)
                .unwrap();
            let SqlQueryPageResult::Scalar {
                projection,
                continuation,
                ..
            } = page
            else {
                panic!("bound SELECT must retain page output");
            };
            assert_eq!(projection.row_count, u32::from(id != 999));
            assert!(continuation.is_none());
        });
    }
}

#[test]
fn generated_public_sql_retains_aliased_round_display_scale() {
    seed(1);
    let page = runtime_api::db::with_request_execution(|| {
        generated::endpoint_handlers::sql_query_public::<false>(
            "SELECT ROUND(id / 3, 3) AS fraction FROM ActorHygieneEntity WHERE id = 1".into(),
            None,
        )
    })
    .unwrap();
    let SqlQueryPageResult::Scalar { projection, .. } = page else {
        panic!("expression projection must retain the scalar page");
    };
    assert_eq!(projection.columns, ["fraction"]);
    assert_eq!(projection.rows, [vec![OutputValue::text("0.333".into())]]);
}
