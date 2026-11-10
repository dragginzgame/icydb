//! Canonical float CHECK SQL binds back to the exact accepted expression bytes.

use super::*;
use crate::types::{Float32, Float64};

#[test]
fn accepted_float_checks_preserve_literals_through_direct_and_named_fields() {
    for (kind, literal) in [
        (
            AcceptedFieldKind::Float32,
            InputValue::from(Float32::try_new(f32::from_bits(1)).unwrap()),
        ),
        (
            AcceptedFieldKind::Float32,
            InputValue::from(Float32::try_new(f32::MAX).unwrap()),
        ),
        (
            AcceptedFieldKind::Float64,
            InputValue::from(Float64::try_new(1.234_567_890_123_456_7e-20).unwrap()),
        ),
        (
            AcceptedFieldKind::Float64,
            InputValue::from(Float64::try_new(f64::from_bits(1)).unwrap()),
        ),
        (
            AcceptedFieldKind::Float64,
            InputValue::from(Float64::try_new(f64::MAX).unwrap()),
        ),
    ] {
        for wrapped in [false, true] {
            let (schema, catalog) = numeric_check_schema(&kind, wrapped, "tests::Float");
            for op in [
                AcceptedCheckCompareOpV1::Eq,
                AcceptedCheckCompareOpV1::Ne,
                AcceptedCheckCompareOpV1::Lt,
                AcceptedCheckCompareOpV1::Lte,
                AcceptedCheckCompareOpV1::Gt,
                AcceptedCheckCompareOpV1::Gte,
            ] {
                for literal_first in [false, true] {
                    let field = CheckValueExprV1Input::Field("score".into());
                    let literal = CheckValueExprV1Input::Literal(literal.clone());
                    let (left, right) = if literal_first {
                        (literal, field)
                    } else {
                        (field, literal)
                    };
                    let expected = bind_check_expr_v1(
                        CheckExprV1Input::Compare { left, op, right },
                        &schema,
                        catalog.enum_catalog(),
                        catalog.composite_catalog(),
                    )
                    .unwrap();
                    let sql = render_accepted_check_expr_sql(&expected, &schema, &catalog).unwrap();
                    assert_eq!(
                        bind_sql_check_for_tests(
                            &sql,
                            &schema,
                            catalog.enum_catalog(),
                            catalog.composite_catalog()
                        ),
                        Ok(expected),
                        "{sql}"
                    );
                }
            }
        }
    }
}
