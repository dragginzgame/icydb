//! Typed SQL versus the maintained structural query surface on accepted values.

use super::*;
use crate::types::{
    Account, Date, Decimal, Duration, Float32, Float64, IntBig, NatBig, Principal, Subaccount,
    Timestamp, Ulid,
};
use icydb_diagnostic_code::DiagnosticCode;
use std::slice;

#[test]
fn sql_multiplication_reads_padded_decimal_fields() {
    let session = initialize();
    publish_operand_schema(&session, AcceptedFieldKind::Decimal { scale: 18 });
    session
        .execute_trusted_dynamic_insert_batch(
            ENTITY_NAME,
            vec![DynamicStructuralPatch::new(vec![
                ("id".into(), DynamicWriteCell::Value(InputValue::unit())),
                (
                    "operand".into(),
                    DynamicWriteCell::Value(InputValue::decimal(Decimal::new(20, 0))),
                ),
            ])],
        )
        .expect("accepted decimal write");
    let stored = new_request_session()
        .execute_trusted_live_page(&DynamicQuery::new(ENTITY_NAME).select(["operand"]), None)
        .expect("stored decimal read");
    let crate::value::PublicValue::Decimal(value) = stored.rows[0][0].as_public() else {
        panic!("decimal operand");
    };
    assert_eq!(value.scale(), 18);
    assert_eq!(value.mantissa(), 20_000_000_000_000_000_000);

    let dispatch =
        sql_statement_dispatch("SELECT operand * operand FROM Singleton").expect("fixed syntax");
    for _ in 0..2 {
        let (result, _) = new_request_session()
            .execute_trusted_sql_query_with_entity_name(&dispatch, &[])
            .expect("padded decimal multiplication");
        let SqlStatementResult::Projection { rows, .. } = result else {
            panic!("projection");
        };
        assert_eq!(rows, vec![vec![OutputValue::decimal(Decimal::new(400, 0))]]);
    }
}

#[test]
fn bound_scalar_families_match_structural_reads_of_stored_values() {
    let cases = [
        (
            AcceptedFieldKind::Account,
            InputValue::account(Account::from_owner_and_subaccount(
                Principal::anonymous(),
                Some(Subaccount::from_array([7; 32])),
            )),
        ),
        (
            AcceptedFieldKind::Principal,
            InputValue::principal(Principal::anonymous()),
        ),
        (
            AcceptedFieldKind::Subaccount,
            InputValue::subaccount(Subaccount::from_array([5; 32])),
        ),
        (
            AcceptedFieldKind::Blob { max_len: None },
            InputValue::blob(vec![0, 1, 255]),
        ),
        (AcceptedFieldKind::Bool, InputValue::boolean(true)),
        (AcceptedFieldKind::Date, InputValue::date(Date::EPOCH)),
        (
            AcceptedFieldKind::Decimal { scale: 2 },
            InputValue::decimal(Decimal::from(2_u64)),
        ),
        (
            AcceptedFieldKind::Duration,
            InputValue::duration(Duration::from_millis(7)),
        ),
        (
            AcceptedFieldKind::Float32,
            InputValue::float32(Float32::default()),
        ),
        (
            AcceptedFieldKind::Float64,
            InputValue::float64(Float64::default()),
        ),
        (AcceptedFieldKind::Int64, InputValue::int64(-3)),
        (AcceptedFieldKind::Int128, InputValue::int128(i128::MAX)),
        (
            AcceptedFieldKind::IntBig { max_bytes: 64 },
            InputValue::int_big(
                "170141183460469231731687303715884105727"
                    .parse::<IntBig>()
                    .expect("integer"),
            ),
        ),
        (AcceptedFieldKind::Nat64, InputValue::nat64(3)),
        (AcceptedFieldKind::Nat128, InputValue::nat128(u128::MAX)),
        (
            AcceptedFieldKind::NatBig { max_bytes: 64 },
            InputValue::nat_big(
                "340282366920938463463374607431768211455"
                    .parse::<NatBig>()
                    .expect("natural"),
            ),
        ),
        (AcceptedFieldKind::U256, InputValue::u256(U256::MAX)),
        (
            AcceptedFieldKind::Text { max_len: None },
            InputValue::text("'?; -- opaque operand".into()),
        ),
        (
            AcceptedFieldKind::Timestamp,
            InputValue::timestamp(Timestamp::from_millis(5)),
        ),
        (
            AcceptedFieldKind::Ulid,
            InputValue::ulid(Ulid::from_u128(7)),
        ),
    ];
    for (kind, input) in cases {
        // Each accepted schema gets a fresh thread-local registry and cache,
        // rather than replacing storage beneath a retained runtime root.
        std::thread::spawn(move || assert_stored_operand_parity(kind, input))
            .join()
            .expect("scalar parity case");
    }
}

#[test]
fn sql_decimal_precision_rounding_preserves_stored_values_and_typed_overflow() {
    for (scale, stored, expression, expected) in [
        (15, "0.000000000000001", "operand * operand", Some("0")),
        (
            18,
            "1.123456789012345678",
            "operand * operand",
            Some("1.2621551567779301925279682998"),
        ),
        (
            28,
            "0.0000000000000000000000000001",
            "operand * 0.5",
            Some("0.0000000000000000000000000001"),
        ),
        (
            28,
            "-0.0000000000000000000000000001",
            "operand * 0.5",
            Some("-0.0000000000000000000000000001"),
        ),
        (
            28,
            "17014118346.0469231731687303715884105727",
            "operand * operand",
            Some("289480223093290488558.92746252171976963"),
        ),
        (
            1,
            "1.1",
            "POWER(operand, 30)",
            Some("17.4494022688864073185588037538"),
        ),
        (
            0,
            "170141183460469231731687303715884105727",
            "operand * 2",
            None,
        ),
        (
            18,
            "-170141183460469231731.687303715884105728",
            "operand / -1",
            Some("170141183460469231731.68730371588410573"),
        ),
    ] {
        assert_stored_decimal_expression(
            scale,
            stored,
            expression,
            expected.ok_or(DiagnosticCode::QueryNumericOverflow),
        );
    }
}

#[test]
fn sql_decimal_arithmetic_qualifies_fitting_results_and_typed_failures() {
    for (scale, stored, expression, expected) in [
        (
            0,
            "1000000000000000000000000000000",
            "operand + 0.0000000000000000000000000001",
            Ok("1000000000000000000000000000000"),
        ),
        (
            0,
            "-1000000000000000000000000000000",
            "operand - -0.0000000000000000000000000001",
            Ok("-1000000000000000000000000000000"),
        ),
        (
            28,
            "-17014118346.0469231731687303715884105728",
            "operand - operand",
            Ok("0"),
        ),
        (
            0,
            "20000000000000000000000000000000000000",
            "operand - 10000000000000000000000000000000000000.0",
            Ok("10000000000000000000000000000000000000"),
        ),
        (
            0,
            "170141183460469231731687303715884105727",
            "operand / 17014118346.0469231731687303715884105727",
            Ok("10000000000000000000000000000"),
        ),
        (
            0,
            "20000000000000000000000000000000000000",
            "SUM(operand - 10000000000000000000000000000000000000.0)",
            Ok("10000000000000000000000000000000000000"),
        ),
        (
            0,
            "170141183460469231731687303715884105727",
            "AVG(operand / 17014118346.0469231731687303715884105727)",
            Ok("10000000000000000000000000000"),
        ),
        (
            0,
            "170141183460469231731687303715884105727",
            "operand + 1",
            Err(DiagnosticCode::QueryNumericOverflow),
        ),
        (
            0,
            "-170141183460469231731687303715884105728",
            "operand / -1",
            Err(DiagnosticCode::QueryNumericOverflow),
        ),
        (
            0,
            "170141183460469231731687303715884105727",
            "operand * 17014118346.0469231731687303715884105727",
            Err(DiagnosticCode::QueryNumericOverflow),
        ),
        (
            0,
            "170141183460469231731687303715884105727",
            "operand / 0",
            Err(DiagnosticCode::QueryNumericNotRepresentable),
        ),
        (
            28,
            "0.0000000000000000000000000001",
            "operand / 10",
            Ok("0"),
        ),
    ] {
        assert_stored_decimal_expression(scale, stored, expression, expected);
    }
}

fn assert_stored_decimal_expression(
    scale: u32,
    stored: &'static str,
    expression: &'static str,
    expected: Result<&'static str, DiagnosticCode>,
) {
    // Each accepted scale owns fresh storage; repeated requests qualify
    // cold/warm compiled plans without replacing a retained schema root.
    std::thread::spawn(move || {
        let session = initialize();
        publish_operand_schema(&session, AcceptedFieldKind::Decimal { scale });
        session
            .execute_trusted_dynamic_insert_batch(
                ENTITY_NAME,
                vec![DynamicStructuralPatch::new(vec![
                    ("id".into(), DynamicWriteCell::Value(InputValue::unit())),
                    (
                        "operand".into(),
                        DynamicWriteCell::Value(InputValue::decimal(stored.parse().unwrap())),
                    ),
                ])],
            )
            .unwrap();
        let sql = format!("SELECT {expression} FROM Singleton");
        for _ in 0..2 {
            let result = new_request_session().execute_trusted_sql_query(&sql);
            match expected {
                Ok(expected) => {
                    let SqlStatementResult::Projection { rows, .. } = result.unwrap() else {
                        panic!("projection");
                    };
                    assert_eq!(
                        rows,
                        vec![vec![OutputValue::decimal(expected.parse().unwrap())]]
                    );
                }
                Err(expected) => assert_eq!(result.unwrap_err().diagnostic_code(), expected),
            }
        }
    })
    .join()
    .unwrap();
}

#[test]
fn sql_remainder_decimal_scale_alignment_preserves_stored_results() {
    for (scale, stored, divisor, expected) in [
        (
            28,
            "5.0000000000000000000000000001",
            "10000000000000",
            Some("5.0000000000000000000000000001"),
        ),
        (
            28,
            "5.0000000000000000000000000001",
            "-10000000000000",
            Some("5.0000000000000000000000000001"),
        ),
        (
            28,
            "-5.0000000000000000000000000001",
            "10000000000000",
            Some("-5.0000000000000000000000000001"),
        ),
        (
            28,
            "-5.0000000000000000000000000001",
            "-10000000000000",
            Some("-5.0000000000000000000000000001"),
        ),
        (
            0,
            "170141183460469231731687303715884105727",
            "0.0000000000000000000000000003",
            Some("0.0000000000000000000000000001"),
        ),
        (
            0,
            "-170141183460469231731687303715884105728",
            "-1",
            Some("0"),
        ),
        (28, "0", "10000000000000", Some("0")),
        (28, "5.0000000000000000000000000001", "0", None),
    ] {
        // Fresh accepted storage per scale; repeated requests cover cold/warm
        // plans while reading the maintained padded decimal field encoding.
        std::thread::spawn(move || {
            let session = initialize();
            publish_operand_schema(&session, AcceptedFieldKind::Decimal { scale });
            session
                .execute_trusted_dynamic_insert_batch(
                    ENTITY_NAME,
                    vec![DynamicStructuralPatch::new(vec![
                        ("id".into(), DynamicWriteCell::Value(InputValue::unit())),
                        (
                            "operand".into(),
                            DynamicWriteCell::Value(InputValue::decimal(stored.parse().unwrap())),
                        ),
                    ])],
                )
                .unwrap();
            let sql = format!("SELECT MOD(operand, {divisor}) FROM Singleton");
            for _ in 0..2 {
                let result = new_request_session().execute_trusted_sql_query(&sql);
                if let Some(expected) = expected {
                    let SqlStatementResult::Projection { rows, .. } = result.unwrap() else {
                        panic!("projection");
                    };
                    assert_eq!(
                        rows,
                        vec![vec![OutputValue::decimal(expected.parse().unwrap())]]
                    );
                } else {
                    assert_eq!(
                        result.unwrap_err().diagnostic_code(),
                        icydb_diagnostic_code::DiagnosticCode::QueryNumericNotRepresentable,
                    );
                }
                let SqlStatementResult::Projection { rows, .. } = new_request_session()
                    .execute_trusted_sql_query(
                        "SELECT MOD(NULL, 0), MOD(operand, NULL) FROM Singleton",
                    )
                    .unwrap()
                else {
                    panic!("null projection");
                };
                assert_eq!(rows, vec![vec![OutputValue::null(), OutputValue::null()]]);
            }
        })
        .join()
        .unwrap();
    }
}

fn assert_stored_operand_parity(kind: AcceptedFieldKind, input: InputValue) {
    let dispatch = sql_statement_dispatch("SELECT operand FROM Singleton WHERE operand = ?")
        .expect("fixed syntax");
    let session = initialize();
    publish_operand_schema(&session, kind);
    session
        .execute_trusted_dynamic_insert_batch(
            ENTITY_NAME,
            vec![DynamicStructuralPatch::new(vec![
                ("id".into(), DynamicWriteCell::Value(InputValue::unit())),
                ("operand".into(), DynamicWriteCell::Value(input.clone())),
            ])],
        )
        .expect("typed write through accepted authority");
    let query = DynamicQuery::new(ENTITY_NAME)
        .select(["operand"])
        .filter(FieldRef::new("operand").eq(input.clone()));
    let expected = new_request_session()
        .execute_trusted_live_page(&query, None)
        .expect("structural control");
    assert_eq!(expected.row_count, 1, "control must match the stored value");
    let (actual, _) = new_request_session()
        .execute_trusted_sql_query_with_entity_name(&dispatch, slice::from_ref(&input))
        .expect("typed SQL");
    let SqlStatementResult::Projection { rows, .. } = actual else {
        panic!("projection");
    };
    assert_eq!(rows, expected.rows);

    // Membership shares the same accepted scalar conversion and stored-row
    // semantics across typed and SQL callers, including duplicate operands.
    let membership =
        sql_statement_dispatch("SELECT operand FROM Singleton WHERE operand IN (?, ?)")
            .expect("fixed membership syntax");
    let query = DynamicQuery::new(ENTITY_NAME)
        .select(["operand"])
        .filter(FilterExpr::in_list(
            "operand",
            [input.clone(), input.clone()],
        ));
    assert_eq!(
        new_request_session()
            .execute_trusted_live_page(&query, None)
            .expect("typed membership")
            .rows,
        expected.rows
    );
    let (actual, _) = new_request_session()
        .execute_trusted_sql_query_with_entity_name(&membership, &[input.clone(), input])
        .expect("SQL membership");
    let SqlStatementResult::Projection { rows, .. } = actual else {
        panic!("projection");
    };
    assert_eq!(rows, expected.rows);
}

// Reuse the heap-only fixture, replacing its empty accepted catalog before
// writes. No generated model supplies runtime authority or operand meaning.
pub(super) fn publish_operand_schema(session: &DbSession<TestCanister>, kind: AcceptedFieldKind) {
    let fields = vec![
        field(1, "id", 0, AcceptedFieldKind::Unit),
        field(2, "operand", 1, kind),
    ];
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        ENTITY_SOURCE.to_string(),
        ENTITY_NAME.to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    );
    let candidate = accepted_schema_candidate_with_field_bindings_for_tests(
        STORE_PATH,
        AcceptedSchemaRevision::new(2),
        BTreeMap::from([(ENTITY_TAG, snapshot)]),
        BTreeMap::from([
            ((ENTITY_TAG, field_source(ID_SOURCE)), FieldId::new(1)),
            ((ENTITY_TAG, field_source(LABEL_SOURCE)), FieldId::new(2)),
        ]),
    );
    let store = session.db.store_handle(STORE_PATH).expect("store");
    crate::db::commit::publish_accepted_schema_candidate(
        STORE_PATH,
        store,
        AcceptedSchemaRevision::INITIAL,
        &candidate,
    )
    .expect("accepted operand schema");
}
