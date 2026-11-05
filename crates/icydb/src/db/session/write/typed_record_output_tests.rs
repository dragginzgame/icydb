use super::{TypedAdapterError, exact_record_output_values};
use crate::value::PublicValue;

#[test]
fn typed_bindings_and_output_rows_remain_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<super::TypedEntityBinding>();
    assert_send_sync::<super::OutputRow>();
}

fn text(value: &str) -> PublicValue {
    PublicValue::Text(value.to_string())
}

#[test]
fn exact_record_output_reorders_accepted_members() {
    let value = PublicValue::Map(vec![
        (text("count"), PublicValue::Nat64(7)),
        (text("label"), text("Ada")),
    ]);

    let values = exact_record_output_values(&["label", "count"], value)
        .expect("exact accepted record output should decode");

    assert!(matches!(&values[0], PublicValue::Text(value) if value == "Ada"));
    assert_eq!(values[1], PublicValue::Nat64(7));
}

#[test]
fn exact_record_output_rejects_duplicate_missing_and_unknown_members() {
    let malformed = [
        PublicValue::Map(vec![
            (text("label"), text("Ada")),
            (text("label"), text("Grace")),
        ]),
        PublicValue::Map(vec![(text("label"), text("Ada"))]),
        PublicValue::Map(vec![
            (text("label"), text("Ada")),
            (text("other"), PublicValue::Nat64(7)),
        ]),
        PublicValue::Map(vec![
            (PublicValue::Nat64(1), text("Ada")),
            (text("count"), PublicValue::Nat64(7)),
        ]),
        PublicValue::List(Vec::new()),
    ];

    for value in malformed.clone() {
        assert_eq!(
            exact_record_output_values(&["label", "count"], value),
            Err(TypedAdapterError::ValueShapeMismatch),
        );
    }
    assert_eq!(
        exact_record_output_values(&["label", "label"], malformed[0].clone()),
        Err(TypedAdapterError::ValueShapeMismatch),
    );
}

#[test]
fn exact_record_output_preserves_every_member_permutation_and_null() {
    let members = [
        (text("label"), text("Ada")),
        (text("count"), PublicValue::Nat64(7)),
        (text("note"), PublicValue::Null),
    ];
    for order in [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ] {
        let record = PublicValue::Map(
            order
                .into_iter()
                .map(|index| members[index].clone())
                .collect(),
        );
        assert_eq!(
            exact_record_output_values(&["label", "count", "note"], record),
            Ok(vec![text("Ada"), PublicValue::Nat64(7), PublicValue::Null]),
        );
    }
    assert_eq!(
        exact_record_output_values(&[], PublicValue::Map(Vec::new())),
        Ok(Vec::new())
    );
}

#[test]
fn typed_input_failures_retain_source_and_shape_classes() {
    assert_eq!(
        TypedAdapterError::from(icydb_model::TypedValueError::SourceUnavailable),
        TypedAdapterError::FieldUnavailable,
    );
    assert_eq!(
        TypedAdapterError::from(icydb_model::TypedValueError::ShapeMismatch),
        TypedAdapterError::ValueShapeMismatch,
    );
}
