use super::group_fields_support_borrowed_group_probe;
use crate::db::{query::plan::FieldSlot, schema::AcceptedFieldKind};

#[test]
fn borrowed_group_probe_requires_accepted_scalar_authority() {
    let accepted =
        FieldSlot::from_test_accepted_kind(0, "status", AcceptedFieldKind::Text { max_len: None });
    let accepted_decimal =
        FieldSlot::from_test_accepted_kind(1, "amount", AcceptedFieldKind::Decimal { scale: 2 });
    let unresolved = FieldSlot::unresolved(0, "status");
    let accepted_enum = FieldSlot::from_test_accepted_kind(
        0,
        "status",
        AcceptedFieldKind::Enum {
            type_id: crate::value::EnumTypeId::new(1).expect("non-zero type ID"),
        },
    );

    assert!(group_fields_support_borrowed_group_probe(&[
        accepted.clone(),
        accepted_decimal,
    ]));
    assert!(!group_fields_support_borrowed_group_probe(&[unresolved]));
    assert!(!group_fields_support_borrowed_group_probe(&[accepted_enum]));

    for kind in [
        AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat64)),
        AcceptedFieldKind::Map {
            key: Box::new(AcceptedFieldKind::Text { max_len: None }),
            value: Box::new(AcceptedFieldKind::Nat64),
        },
        AcceptedFieldKind::Unit,
    ] {
        let field = FieldSlot::from_test_accepted_kind(1, "detail", kind);
        assert!(!group_fields_support_borrowed_group_probe(&[
            accepted.clone(),
            field,
        ]));
    }
}
