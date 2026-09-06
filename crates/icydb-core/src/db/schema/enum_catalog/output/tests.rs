//! Public output owns admitted payloads and resolves enum labels at the catalog.

use super::*;
use crate::{
    db::schema::{
        AcceptedFieldKind, FieldStorageDecode, TestEnumDefinition, TestEnumVariant,
        build_accepted_enum_catalog_for_tests,
    },
    types::{IntBig, NatBig},
    value::ValueEnum,
};

const PATH: &str = "tests::PublicOutput";

fn catalog() -> AcceptedEnumCatalog {
    build_accepted_enum_catalog_for_tests(&[TestEnumDefinition::new(
        PATH,
        vec![
            TestEnumVariant::unit("Unit"),
            TestEnumVariant::payload(
                "Payload",
                AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Map {
                    key: Box::new(AcceptedFieldKind::Text { max_len: None }),
                    value: Box::new(AcceptedFieldKind::Blob { max_len: None }),
                })),
                FieldStorageDecode::CatalogValue,
            ),
        ],
    )])
    .unwrap()
}

fn nested(items: usize) -> Value {
    Value::List(
        (0..items)
            .map(|_| {
                Value::Map(vec![(
                    Value::Text("payload".into()),
                    Value::Blob(vec![7; 1024]),
                )])
            })
            .collect(),
    )
}

fn enum_value(catalog: &AcceptedEnumCatalog, name: &str, payload: Option<Value>) -> Value {
    let type_id = catalog.type_id(PATH).unwrap();
    let variant_id = catalog
        .enum_type(type_id)
        .unwrap()
        .variant_id(name)
        .unwrap();
    Value::Enum(ValueEnum::new(
        type_id,
        variant_id,
        match payload {
            None => CanonicalEnumBody::Unit,
            Some(value) => CanonicalEnumBody::Payload(Box::new(value)),
        },
    ))
}

fn runtime_backing(value: &Value, pointers: &mut Vec<*const u8>) {
    match value {
        Value::Blob(v) => pointers.push(v.as_ptr()),
        Value::Text(v) => pointers.push(v.as_ptr()),
        Value::List(v) => v.iter().for_each(|v| runtime_backing(v, pointers)),
        Value::Map(v) => v.iter().for_each(|(k, v)| {
            runtime_backing(k, pointers);
            runtime_backing(v, pointers);
        }),
        Value::Enum(v) => {
            if let Some(v) = v.payload() {
                runtime_backing(v, pointers);
            }
        }
        _ => {}
    }
}

fn public_backing(value: &PublicValue, pointers: &mut Vec<*const u8>) {
    match value {
        PublicValue::Blob(v) => pointers.push(v.as_ptr()),
        PublicValue::Text(v) => pointers.push(v.as_ptr()),
        PublicValue::List(v) => v.iter().for_each(|v| public_backing(v, pointers)),
        PublicValue::Map(v) => v.iter().for_each(|(k, v)| {
            public_backing(k, pointers);
            public_backing(v, pointers);
        }),
        PublicValue::Enum(v) => {
            if let Some(v) = v.payload() {
                public_backing(v, pointers);
            }
        }
        _ => {}
    }
}

#[test]
fn public_output_moves_heap_payloads_and_preserves_public_wire() {
    let catalog = catalog();
    for value in [
        Value::Blob(vec![8; 65536]),
        Value::Text("é".repeat(1024)),
        nested(3),
        Value::List(vec![]),
        Value::Map(vec![]),
        Value::Null,
        Value::Bool(true),
        Value::IntBig(i128::MIN.to_string().parse::<IntBig>().unwrap()),
        Value::NatBig(u128::MAX.to_string().parse::<NatBig>().unwrap()),
    ] {
        let expected =
            OutputValue::from_public(PublicValue::try_from_runtime_non_enum(&value).unwrap());
        let expected_wire = candid::encode_one(&expected).unwrap();
        let mut before = Vec::new();
        runtime_backing(&value, &mut before);
        let output = output_value_from_runtime(&catalog, value).unwrap();
        let mut after = Vec::new();
        public_backing(output.as_public(), &mut after);
        assert_eq!(output, expected);
        assert_eq!(candid::encode_one(&output).unwrap(), expected_wire);
        assert_eq!(before, after);
    }
}

#[test]
fn public_output_moves_enum_payload_and_owns_catalog_labels() {
    let catalog = catalog();
    let mut outputs = Vec::new();
    for payload in [None, Some(nested(2)), Some(nested(0))] {
        let name = if payload.is_some() { "Payload" } else { "Unit" };
        let expected =
            OutputValue::from_public(PublicValue::Enum(PublicEnumValue::from_catalog_parts(
                name,
                PATH,
                payload
                    .as_ref()
                    .map(|v| PublicValue::try_from_runtime_non_enum(v).unwrap()),
            )));
        let value = enum_value(&catalog, name, payload);
        let mut before = Vec::new();
        runtime_backing(&value, &mut before);
        let output = output_value_from_runtime(&catalog, value).unwrap();
        let mut after = Vec::new();
        public_backing(output.as_public(), &mut after);
        assert_eq!(before, after);
        assert_eq!(output, expected);
        assert_eq!(
            candid::encode_one(&output).unwrap(),
            candid::encode_one(&expected).unwrap()
        );
        outputs.push(output);
    }
    drop(catalog);
    for output in outputs {
        let PublicValue::Enum(value) = output.as_public() else {
            panic!("enum")
        };
        assert_eq!(value.path(), Some(PATH));
    }
}

#[test]
fn public_output_preserves_enum_failure_and_traversal_order() {
    let catalog = catalog();
    let type_id = catalog.type_id(PATH).unwrap().get();
    let unknown_type = || Value::Enum(ValueEnum::test_unit(u32::MAX, 1));
    let unknown_variant = || Value::Enum(ValueEnum::test_unit(type_id, u32::MAX));
    for (value, error) in [
        (unknown_type(), EnumValueResolutionError::UnknownType),
        (unknown_variant(), EnumValueResolutionError::UnknownVariant),
        (
            Value::List(vec![unknown_variant(), unknown_type()]),
            EnumValueResolutionError::UnknownVariant,
        ),
        (
            Value::Map(vec![(unknown_variant(), unknown_type())]),
            EnumValueResolutionError::UnknownVariant,
        ),
        (
            Value::Enum(ValueEnum::test_payload(type_id, u32::MAX, unknown_type())),
            EnumValueResolutionError::UnknownVariant,
        ),
        (
            enum_value(
                &catalog,
                "Payload",
                Some(Value::List(vec![nested(1), unknown_type()])),
            ),
            EnumValueResolutionError::UnknownType,
        ),
    ] {
        assert_eq!(
            output_value_from_runtime(&catalog, value).unwrap_err(),
            error
        );
    }
}
