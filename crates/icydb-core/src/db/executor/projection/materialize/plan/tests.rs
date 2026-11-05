use super::{ProjectionSpec, direct_projection_slots_from_projection};
use crate::{
    db::query::plan::expr::{Expr, FieldId, ProjectionField},
    value::Value,
};

#[test]
fn direct_slot_metadata_preserves_order_repeats_and_expression_rejection() {
    let projection = ProjectionSpec::from_fields_for_test(
        ["payload", "id", "payload"]
            .into_iter()
            .map(|name| ProjectionField::Scalar {
                expr: Expr::Field(FieldId::new(name)),
                alias: None,
            })
            .collect(),
    );
    let prepared = direct_projection_slots_from_projection(&projection, Some(&[7, 2, 7])).unwrap();
    assert!(prepared.has_repeated_source());
    assert_eq!(
        prepared
            .projections()
            .iter()
            .map(|slot| (slot.source_slot(), slot.previous_projection_index()))
            .collect::<Vec<_>>(),
        vec![(7, None), (2, None), (7, Some(0))]
    );
    assert!(direct_projection_slots_from_projection(&projection, None).is_none());

    let expression = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Literal(Value::Nat64(1)),
        alias: None,
    }]);
    assert!(direct_projection_slots_from_projection(&expression, Some(&[0])).is_none());
    let empty =
        direct_projection_slots_from_projection(&ProjectionSpec::default(), Some(&[])).unwrap();
    assert_eq!(empty.len(), 0);
    assert!(!empty.has_repeated_source());
}
