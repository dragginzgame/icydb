//! Stored aggregate domains and shared result/error contracts.

use super::{bindings_parity::publish_operand_schema, *};
use crate::{
    db::{
        avg,
        query::plan::expr::{Expr, ExprType, NumericSubtype, infer_expr_type},
    },
    types::{Decimal, IntBig, NatBig},
};
use icydb_diagnostic_code::DiagnosticCode;

fn with_operand(
    kind: AcceptedFieldKind,
    value: InputValue,
    check: impl FnOnce(DbSession<TestCanister>) + Send + 'static,
) {
    // Each accepted shape gets an isolated registry, runtime root and request budget.
    std::thread::spawn(move || {
        let session = initialize();
        publish_operand_schema(&session, kind);
        session
            .execute_trusted_dynamic_insert_batch(
                ENTITY_NAME,
                vec![DynamicStructuralPatch::new(vec![
                    ("id".into(), DynamicWriteCell::Value(InputValue::unit())),
                    ("operand".into(), DynamicWriteCell::Value(value)),
                ])],
            )
            .expect("accepted numeric write");
        check(session);
    })
    .join()
    .expect("aggregate contract fixture");
}

#[test]
fn aggregate_numeric_domain_failures_keep_typed_diagnostics_across_frontends() {
    let cases = [
        (AcceptedFieldKind::Nat128, InputValue::nat128(u128::MAX)),
        (
            AcceptedFieldKind::NatBig { max_bytes: 64 },
            InputValue::nat_big(NatBig::from(2_u32)),
        ),
        (
            AcceptedFieldKind::IntBig { max_bytes: 64 },
            InputValue::int_big(IntBig::from(-2_i32)),
        ),
        (
            AcceptedFieldKind::NatBig { max_bytes: 64 },
            InputValue::nat_big(u128::MAX.to_string().parse::<NatBig>().unwrap()),
        ),
        (
            AcceptedFieldKind::IntBig { max_bytes: 64 },
            InputValue::int_big(
                "-340282366920938463463374607431768211455"
                    .parse::<IntBig>()
                    .unwrap(),
            ),
        ),
    ];
    for (kind, input) in cases {
        with_operand(kind, input, |session| {
            for operation in ["SUM", "AVG"] {
                for (projection, suffix) in [("", ""), ("operand, ", " GROUP BY operand")] {
                    let sql =
                        format!("SELECT {projection}{operation}(operand) FROM Singleton{suffix}");
                    let error = session
                        .execute_trusted_sql_query(&sql)
                        .expect_err("out of Decimal range");
                    assert_eq!(
                        error.diagnostic().code(),
                        DiagnosticCode::QueryNumericNotRepresentable,
                        "{sql}"
                    );
                    let (compiled, _) = session
                        .compile_sql_query_for_tests(&sql)
                        .expect("numeric input admits");
                    let prepared_error = session
                        .execute_compiled_sql_query_context(&compiled)
                        .expect_err("same numeric domain");
                    assert_eq!(error.diagnostic(), prepared_error.diagnostic());
                }
                let empty = format!("SELECT {operation}(operand) FROM Singleton WHERE FALSE");
                assert_eq!(sql_rows(&session, &empty), vec![vec![OutputValue::null()]]);
            }
            for aggregate in [sum("operand"), avg("operand")] {
                let query = DynamicQuery::new(ENTITY_NAME)
                    .group_by("operand")
                    .aggregate(aggregate)
                    .grouped_limits(1, 4096)
                    .limit(1);
                let error = session
                    .execute_trusted_dynamic_grouped_query(&query)
                    .expect_err("same structural numeric domain");
                assert_eq!(
                    error.diagnostic().code(),
                    DiagnosticCode::QueryNumericNotRepresentable
                );
            }
        });
    }
}

#[test]
fn aggregate_result_inference_matches_decimal_and_u256_outputs() {
    for (kind, input, expected) in [
        (
            AcceptedFieldKind::Nat64,
            InputValue::nat64(2),
            ExprType::Numeric(NumericSubtype::Decimal),
        ),
        (
            AcceptedFieldKind::Int128,
            InputValue::int128(2),
            ExprType::Numeric(NumericSubtype::Decimal),
        ),
        (
            AcceptedFieldKind::U256,
            InputValue::u256(U256::from(2_u64)),
            ExprType::U256,
        ),
    ] {
        with_operand(kind, input, move |session| {
            let catalog = session
                .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
                .unwrap();
            for aggregate in [sum("operand"), avg("operand")] {
                let is_average = aggregate.kind() == crate::db::query::plan::AggregateKind::Avg;
                let inferred =
                    infer_expr_type(&Expr::Aggregate(aggregate), catalog.accepted_schema_info());
                if is_average && expected == ExprType::U256 {
                    assert!(inferred.is_err());
                    continue;
                }
                assert_eq!(inferred.unwrap(), expected);
                let operation = if is_average { "AVG" } else { "SUM" };
                let value = if expected == ExprType::U256 {
                    OutputValue::u256(U256::from(2_u64))
                } else {
                    OutputValue::decimal(Decimal::from(2_u64))
                };
                let sql = format!("SELECT {operation}(operand) FROM Singleton");
                let result = session
                    .execute_trusted_sql_query(&sql)
                    .unwrap_or_else(|error| panic!("{sql} ({expected:?}): {error:?}"));
                let SqlStatementResult::Projection { rows, .. } = result else {
                    panic!("aggregate projection")
                };
                assert_eq!(rows, vec![vec![value]]);
            }
        });
    }
}

#[test]
fn aggregate_having_mixed_u256_comparison_is_typed_and_valid_literal_still_works() {
    with_operand(
        AcceptedFieldKind::U256,
        InputValue::u256(U256::from(2_u64)),
        |session| {
            for (projection, grouping) in [("", ""), ("operand, ", " GROUP BY operand")] {
                for operator in ["=", "<", ">="] {
                    let sql = format!(
                        "SELECT {projection}SUM(operand) FROM Singleton{grouping} HAVING SUM(operand) {operator} 2"
                    );
                    let error = session
                        .execute_trusted_sql_query(&sql)
                        .expect_err("no implicit U256 coercion");
                    assert_eq!(
                        error.diagnostic().code(),
                        DiagnosticCode::QueryUnsupportedProjection
                    );
                    let (compiled, _) = session
                        .compile_sql_query_for_tests(&sql)
                        .expect("runtime comparison boundary");
                    let prepared_error = session
                        .execute_compiled_sql_query_context(&compiled)
                        .expect_err("same typed failure");
                    assert_eq!(error.diagnostic(), prepared_error.diagnostic());
                }
            }
            let result = session.execute_trusted_sql_query("SELECT operand, SUM(operand) FROM Singleton GROUP BY operand HAVING SUM(operand) = U256 '2'").expect("exact U256 comparison");
            let SqlStatementResult::Grouped { rows, .. } = result else {
                panic!("grouped result")
            };
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].aggregate_values(),
                &[OutputValue::u256(U256::from(2_u64))]
            );
        },
    );
}
