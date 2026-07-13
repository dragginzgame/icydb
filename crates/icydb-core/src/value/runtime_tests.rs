use super::{RuntimeEnumContext, RuntimeEnumSelection, RuntimeValueDecode};
use crate::value::{Value, ValueEnum};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct CollapsedNat(u8);

impl RuntimeValueDecode for CollapsedNat {
    fn from_value(value: &Value) -> Option<Self> {
        let Value::Nat64(value) = value else {
            return None;
        };

        Some(Self(u8::from(value % 2 != 0)))
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ResolvedEnum(u8);

impl RuntimeValueDecode for ResolvedEnum {
    fn from_value(_value: &Value) -> Option<Self> {
        None
    }

    fn from_value_with_enum_context(
        value: &Value,
        context: &dyn RuntimeEnumContext,
    ) -> Option<Self> {
        let Value::Enum(value) = value else {
            return None;
        };
        let selection = context.resolve_enum(value)?;

        (selection.path == "test::Collapsed" && selection.variant == "Only").then_some(Self(0))
    }
}

struct CollapsingEnumContext;

impl RuntimeEnumContext for CollapsingEnumContext {
    fn resolve_enum<'a>(&'a self, value: &'a ValueEnum) -> Option<RuntimeEnumSelection<'a>> {
        matches!(value.variant_id().get(), 1 | 2).then_some(RuntimeEnumSelection {
            path: "test::Collapsed",
            variant: "Only",
            payload: value.payload(),
        })
    }
}

fn enum_value(variant_id: u32) -> Value {
    Value::Enum(ValueEnum::test_unit(1, variant_id))
}

#[test]
fn ordinary_set_decode_rejects_colliding_decoded_items() {
    let value = Value::List(vec![Value::Nat64(1), Value::Nat64(3)]);

    assert!(BTreeSet::<CollapsedNat>::from_value(&value).is_none());
}

#[test]
fn ordinary_map_decode_rejects_colliding_decoded_keys() {
    let value = Value::Map(vec![
        (Value::Nat64(1), Value::Nat64(10)),
        (Value::Nat64(3), Value::Nat64(30)),
    ]);

    assert!(BTreeMap::<CollapsedNat, u64>::from_value(&value).is_none());
}

#[test]
fn contextual_set_decode_rejects_colliding_enum_items() {
    let value = Value::List(vec![enum_value(1), enum_value(2)]);

    assert!(
        BTreeSet::<ResolvedEnum>::from_value_with_enum_context(&value, &CollapsingEnumContext)
            .is_none(),
    );
}

#[test]
fn contextual_map_decode_rejects_colliding_enum_keys() {
    let value = Value::Map(vec![
        (enum_value(1), Value::Nat64(10)),
        (enum_value(2), Value::Nat64(20)),
    ]);

    assert!(
        BTreeMap::<ResolvedEnum, u64>::from_value_with_enum_context(
            &value,
            &CollapsingEnumContext,
        )
        .is_none(),
    );
}

#[test]
fn contextual_map_decode_rejects_noncanonical_enum_containing_entries() {
    let value = Value::Map(vec![
        (Value::Nat64(2), enum_value(1)),
        (Value::Nat64(1), enum_value(1)),
    ]);

    assert!(
        BTreeMap::<u64, ResolvedEnum>::from_value_with_enum_context(
            &value,
            &CollapsingEnumContext,
        )
        .is_none(),
    );
}
