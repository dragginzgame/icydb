//! Renames preserve values through every accepted container shape.

use super::*;
use crate::db::schema::{
    AcceptedCompositeCatalog, AcceptedSchemaRevision, AcceptedValueContract, CompositeFieldId,
    FieldStorageDecode, TestEnumDefinition, TestEnumVariant, ValueAdmissionBudget,
    build_accepted_enum_catalog_for_tests,
    composite_catalog::{AcceptedCompositeElement, AcceptedCompositeField},
    empty_accepted_enum_catalog_for_tests,
    enum_catalog::admit_canonical_value,
};
use std::collections::BTreeMap;

fn composite(id: u32) -> AcceptedFieldKind {
    AcceptedFieldKind::Composite {
        type_id: CompositeTypeId::new(id).unwrap(),
    }
}

fn catalogs() -> (AcceptedValueCatalogHandle, AcceptedValueCatalogHandle) {
    let record = AcceptedCompositeShape::Record(vec![
        AcceptedCompositeField::new(
            CompositeFieldId::new(1).unwrap(),
            "a".into(),
            AcceptedCompositeElement::new(AcceptedFieldKind::Nat64, false),
        ),
        AcceptedCompositeField::new(
            CompositeFieldId::new(2).unwrap(),
            "z".into(),
            AcceptedCompositeElement::new(AcceptedFieldKind::Nat64, false),
        ),
    ]);
    let composites = AcceptedCompositeCatalog::from_initial_definitions(
        BTreeMap::from([
            (CompositeTypeId::new(1).unwrap(), ("Record".into(), record)),
            (
                CompositeTypeId::new(2).unwrap(),
                (
                    "Tuple".into(),
                    AcceptedCompositeShape::Tuple(vec![AcceptedCompositeElement::new(
                        composite(1),
                        false,
                    )]),
                ),
            ),
            (
                CompositeTypeId::new(3).unwrap(),
                (
                    "Wrapper".into(),
                    AcceptedCompositeShape::Newtype(AcceptedCompositeElement::new(
                        composite(1),
                        true,
                    )),
                ),
            ),
        ]),
        &empty_accepted_enum_catalog_for_tests(),
    )
    .unwrap();
    let enums = build_accepted_enum_catalog_for_tests(&[TestEnumDefinition::new(
        "Envelope",
        vec![TestEnumVariant::payload(
            "Some",
            composite(1),
            FieldStorageDecode::CatalogValue,
        )],
    )])
    .unwrap();
    let renamed = composites
        .clone()
        .with_renamed_record_field(
            CompositeTypeId::new(1).unwrap(),
            CompositeFieldId::new(1).unwrap(),
            "zz".into(),
            &enums,
        )
        .unwrap();
    (
        AcceptedValueCatalogHandle::new_for_tests(
            enums.clone(),
            composites,
            AcceptedSchemaRevision::INITIAL,
        ),
        AcceptedValueCatalogHandle::new_for_tests(enums, renamed, AcceptedSchemaRevision::new(2)),
    )
}

fn record(a: u64, z: u64, renamed: bool) -> Value {
    let mut entries = vec![
        (
            Value::Text(if renamed { "zz" } else { "a" }.into()),
            Value::Nat64(a),
        ),
        (Value::Text("z".into()), Value::Nat64(z)),
    ];
    entries.sort_unstable_by(|left, right| Value::canonical_cmp(&left.0, &right.0));
    Value::Map(entries)
}

fn assert_admitted(catalog: &AcceptedValueCatalogHandle, kind: &AcceptedFieldKind, value: Value) {
    let contract =
        AcceptedValueContract::from_accepted_field(catalog, kind, FieldStorageDecode::CatalogValue)
            .unwrap();
    admit_canonical_value(
        catalog,
        &contract,
        false,
        value,
        &mut ValueAdmissionBudget::standard(),
    )
    .unwrap();
}

#[test]
fn record_rename_preserves_nested_values_and_restores_container_order() {
    let (before, after) = catalogs();
    let a = record(1, 9, false);
    let b = record(2, 0, false);
    let new_a = record(1, 9, true);
    let new_b = record(2, 0, true);
    let enum_id = before.enum_catalog().type_id("Envelope").unwrap();
    let variant_id = before
        .enum_catalog()
        .enum_type(enum_id)
        .unwrap()
        .variant_id("Some")
        .unwrap();
    let wrap = |value| {
        Value::Enum(ValueEnum::new(
            enum_id,
            variant_id,
            CanonicalEnumBody::Payload(Box::new(value)),
        ))
    };
    let cases = [
        (composite(1), a.clone(), new_a.clone()),
        (
            composite(2),
            Value::List(vec![a.clone()]),
            Value::List(vec![new_a.clone()]),
        ),
        (composite(3), a.clone(), new_a.clone()),
        (composite(3), Value::Null, Value::Null),
        (
            AcceptedFieldKind::Enum { type_id: enum_id },
            wrap(a.clone()),
            wrap(new_a.clone()),
        ),
        (
            AcceptedFieldKind::List(Box::new(composite(1))),
            Value::List(vec![a.clone(), b.clone()]),
            Value::List(vec![new_a.clone(), new_b.clone()]),
        ),
        (
            AcceptedFieldKind::Set(Box::new(composite(1))),
            Value::List(vec![a.clone(), b.clone()]),
            Value::List(vec![new_b.clone(), new_a.clone()]),
        ),
        (
            AcceptedFieldKind::Map {
                key: Box::new(composite(1)),
                value: Box::new(composite(1)),
            },
            Value::Map(vec![(a.clone(), b.clone()), (b, a)]),
            Value::Map(vec![(new_b.clone(), new_a.clone()), (new_a, new_b)]),
        ),
    ];
    for (kind, value, expected) in cases {
        // Nullability of the newtype's inner element is part of its contract.
        if !matches!(value, Value::Null) {
            assert_admitted(&before, &kind, value.clone());
        }
        let rewritten = rewrite_value(
            value,
            &kind,
            &before,
            &after,
            0,
            &mut (MAX_ACCEPTED_VALUE_BYTES as usize),
        )
        .unwrap();
        assert_eq!(rewritten, expected);
        if !matches!(rewritten, Value::Null) {
            assert_admitted(&after, &kind, rewritten);
        }
    }
}

#[test]
fn record_rename_rejects_missing_candidate_identity_and_excess_depth() {
    let (before, after) = catalogs();
    let empty = AcceptedValueCatalogHandle::new_for_tests(
        empty_accepted_enum_catalog_for_tests(),
        AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::new(2),
    );
    assert!(
        rewrite_value(
            record(1, 2, false),
            &composite(1),
            &before,
            &empty,
            0,
            &mut (MAX_ACCEPTED_VALUE_BYTES as usize)
        )
        .is_err()
    );
    assert!(
        rewrite_value(
            record(1, 2, false),
            &composite(1),
            &before,
            &after,
            0,
            &mut 0
        )
        .is_err()
    );
    assert!(
        rewrite_value(
            record(1, 2, false),
            &composite(1),
            &before,
            &after,
            MAX_ACCEPTED_RECURSIVE_DEPTH + 1,
            &mut (MAX_ACCEPTED_VALUE_BYTES as usize),
        )
        .is_err()
    );
}
