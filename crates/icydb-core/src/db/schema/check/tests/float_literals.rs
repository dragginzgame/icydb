//! Canonical float CHECK SQL binds back to the exact accepted expression bytes.

use super::*;
use crate::types::{Float32, Float64};

fn float_schema(
    kind: &AcceptedFieldKind,
    wrapped: bool,
) -> (PersistedSchemaSnapshot, AcceptedValueCatalogHandle) {
    let enums = empty_accepted_enum_catalog_for_tests();
    let type_id = CompositeTypeId::new(1).unwrap();
    let composites = if wrapped {
        AcceptedCompositeCatalog::from_initial_definitions(
            BTreeMap::from([(
                type_id,
                (
                    "tests::Float".into(),
                    AcceptedCompositeShape::Newtype(AcceptedCompositeElement::new(
                        kind.clone(),
                        false,
                    )),
                ),
            )]),
            &enums,
        )
        .unwrap()
    } else {
        AcceptedCompositeCatalog::empty()
    };
    let mut fields = snapshot().fields().to_vec();
    fields[1] = field(
        2,
        1,
        "score",
        if wrapped {
            AcceptedFieldKind::Composite { type_id }
        } else {
            kind.clone()
        },
        false,
        LeafCodec::Structural,
    );
    let schema = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::Compass".into(),
        "Compass".into(),
        FieldId::new(1),
        SchemaRowLayout::initial(fields.iter().map(|f| (f.id(), f.slot())).collect()),
        fields,
    );
    let catalog = AcceptedValueCatalogHandle::new_for_tests(
        enums,
        composites,
        AcceptedSchemaRevision::INITIAL,
    );
    (schema, catalog)
}

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
            let (schema, catalog) = float_schema(&kind, wrapped);
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
