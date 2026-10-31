//! Typed float spelling preserves the existing finite atoms across SQL parsing.

use crate::{
    db::{
        predicate::{
            CoercionId, CompareOp, ComparePredicate, Predicate, parse_sql_predicate,
            render_sql_predicate,
        },
        sql_shared::{SqlParseError, SqlTokenCursor, render_scalar_sql_value, tokenize_sql},
    },
    types::{Float32, Float64},
    value::Value,
};

fn float_atoms() -> Vec<Value> {
    let mut values = Vec::new();
    for value in [
        0.0_f32,
        -0.0,
        1.0,
        -1.25,
        f32::EPSILON,
        f32::from_bits(1),
        f32::MIN_POSITIVE,
        f32::MAX,
        f32::MIN,
    ] {
        values.push(Value::Float32(Float32::try_new(value).unwrap()));
    }
    for value in [
        0.0_f64,
        -0.0,
        1.0,
        -1.25,
        1.234_567_890_123_456_7e-20,
        f64::EPSILON,
        f64::from_bits(1),
        f64::MIN_POSITIVE,
        f64::MAX,
        f64::MIN,
    ] {
        values.push(Value::Float64(Float64::try_new(value).unwrap()));
    }
    values
}

#[test]
fn typed_floats_reject_malformed_and_nonfinite_payloads() {
    for prefix in ["FLOAT32", "float64"] {
        for value in [
            "", "NaN", "inf", "-inf", "1e9999", "1_000", "0x1", "1.2.3", "no",
        ] {
            let sql = format!("{prefix} '{value}'");
            let mut cursor = SqlTokenCursor::new(tokenize_sql(&sql).unwrap());
            assert_eq!(
                cursor.parse_literal(),
                Err(SqlParseError::invalid_numeric_literal()),
                "{sql}"
            );
            assert_eq!(
                parse_sql_predicate(&format!("score = {sql}")),
                Err(SqlParseError::invalid_numeric_literal())
            );
        }
    }
    let mut overflow = SqlTokenCursor::new(tokenize_sql("FLOAT32 '3.5e38'").unwrap());
    assert_eq!(
        overflow.parse_literal(),
        Err(SqlParseError::invalid_numeric_literal())
    );
}

#[test]
fn typed_numeric_lookahead_preserves_fields_membership_and_between() {
    for name in ["float32", "FLOAT64", "u256"] {
        for sql in [format!("{name} = score"), format!("score = {name}")] {
            assert!(matches!(
                parse_sql_predicate(&sql),
                Ok(Predicate::CompareFields(_))
            ));
        }
    }
    let value = Value::Float64(Float64::try_new(1.25e-30).unwrap());
    let literal = render_scalar_sql_value(&value).unwrap();
    for (word, op) in [("IN", CompareOp::In), ("NOT IN", CompareOp::NotIn)] {
        let expected = Predicate::Compare(ComparePredicate::with_coercion(
            "score",
            op,
            Value::List(vec![value.clone(), Value::Null]),
            CoercionId::Strict,
        ));
        assert_eq!(
            parse_sql_predicate(&format!("score {word} ({literal}, NULL)")),
            Ok(expected)
        );
    }
    let Predicate::And(bounds) =
        parse_sql_predicate(&format!("score BETWEEN {literal} AND {literal}")).unwrap()
    else {
        panic!("BETWEEN should retain its pair of bounds");
    };
    assert_eq!(
        bounds,
        [
            Predicate::Compare(ComparePredicate::with_coercion(
                "score",
                CompareOp::Gte,
                value.clone(),
                CoercionId::NumericWiden
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "score",
                CompareOp::Lte,
                value,
                CoercionId::NumericWiden
            )),
        ]
    );
    let expected = Predicate::Compare(ComparePredicate::with_coercion(
        "balance",
        CompareOp::Eq,
        Value::U256(crate::types::U256::MAX),
        CoercionId::Strict,
    ));
    assert_eq!(
        parse_sql_predicate(&format!("balance = U256 '{}'", crate::types::U256::MAX)),
        Ok(expected)
    );
}

#[cfg(feature = "sql")]
#[test]
fn full_sql_retains_float_atoms_in_select_insert_update_and_expression_positions() {
    use crate::db::sql::parser::{
        SqlExpr, SqlExprBinaryOp, SqlInsertSource, SqlProjection, SqlSelectItem, SqlStatement,
        SqlWriteValue, parse_sql,
    };
    use icydb_diagnostic_code::SqlFeatureCode;

    for value in float_atoms() {
        let literal = render_scalar_sql_value(&value).unwrap();
        assert_eq!(
            parse_sql(&format!("SELECT {literal} FROM Measurement")),
            Err(SqlParseError::UnsupportedFeature {
                feature: SqlFeatureCode::StandaloneLiteralProjectionItem
            })
        );
        let SqlStatement::Select(select) =
            parse_sql(&format!("SELECT score + {literal} FROM Measurement")).unwrap()
        else {
            panic!("expected SELECT");
        };
        assert_eq!(
            select.projection,
            SqlProjection::Items(vec![SqlSelectItem::Expr(SqlExpr::Binary {
                left: Box::new(SqlExpr::Field("score".into())),
                op: SqlExprBinaryOp::Add,
                right: Box::new(SqlExpr::Literal(value.clone())),
            })])
        );
        let SqlStatement::Insert(insert) = parse_sql(&format!(
            "INSERT INTO Measurement (score) VALUES ({literal})"
        ))
        .unwrap() else {
            panic!("expected INSERT");
        };
        assert_eq!(
            insert.source,
            SqlInsertSource::Values(vec![vec![SqlWriteValue::Literal(value.clone())]])
        );
        for sql in [
            format!("UPDATE Measurement SET score = {literal}"),
            format!("SELECT score + {literal} FROM Measurement"),
            format!("SELECT id FROM Measurement WHERE score >= {literal}"),
            format!("SELECT id FROM Measurement WHERE score BETWEEN {literal} AND {literal}"),
        ] {
            assert!(parse_sql(&sql).is_ok(), "{sql}");
        }
    }
    assert!(
        parse_sql("SELECT float32, float64, u256 FROM Measurement WHERE float64 = float32").is_ok()
    );
}

#[test]
fn canonical_float_sql_preserves_exact_finite_atoms() {
    for expected in float_atoms() {
        let sql = render_scalar_sql_value(&expected).unwrap();
        let mut cursor = SqlTokenCursor::new(tokenize_sql(&sql).unwrap());
        assert_eq!(cursor.parse_literal(), Ok(expected), "{sql}");
        assert!(cursor.is_eof());
    }
}

#[test]
fn standalone_float_predicates_preserve_both_operand_orders_and_rendering() {
    for value in float_atoms() {
        let literal = render_scalar_sql_value(&value).unwrap();
        for (op, text, reverse, coercion) in [
            (CompareOp::Eq, "=", "=", CoercionId::Strict),
            (CompareOp::Ne, "!=", "!=", CoercionId::Strict),
            (CompareOp::Lt, "<", ">", CoercionId::NumericWiden),
            (CompareOp::Lte, "<=", ">=", CoercionId::NumericWiden),
            (CompareOp::Gt, ">", "<", CoercionId::NumericWiden),
            (CompareOp::Gte, ">=", "<=", CoercionId::NumericWiden),
        ] {
            let expected = Predicate::Compare(ComparePredicate::with_coercion(
                "score",
                op,
                value.clone(),
                coercion,
            ));
            for sql in [
                format!("score {text} {literal}"),
                format!("{literal} {reverse} score"),
            ] {
                let parsed = parse_sql_predicate(&sql).unwrap();
                assert_eq!(parsed, expected, "{sql}");
                let rendered = render_sql_predicate(&parsed).unwrap();
                assert_eq!(
                    parse_sql_predicate(&rendered),
                    Ok(expected.clone()),
                    "{rendered}"
                );
            }
        }
    }
}
