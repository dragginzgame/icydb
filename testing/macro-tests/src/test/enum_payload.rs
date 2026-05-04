#[cfg(test)]
use crate::prelude::*;

pub use icydb_testing_test_fixtures::macro_test::enum_payload::*;

///
/// TESTS
///

#[cfg(test)]
pub mod test {
    use super::*;
    use base::types::ic::icp::Tokens;
    use icydb::{
        __macro::{
            Value, decode_persisted_structured_slot_payload,
            encode_persisted_structured_slot_payload, runtime_value_to_value,
        },
        db::query::FilterExpr,
        value::InputValue,
    };

    #[entity(
        store = "TestStore",
        pk(field = "id"),
        fields(
            field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
            field(ident = "cost", value(item(is = "EnumWithPayload")))
        )
    )]
    pub struct EnumEntityHarness {}

    ///
    /// PrimitiveEnumWithPayload
    ///
    /// Holds a primitive-backed enum payload so the persisted custom-slot
    /// helpers can exercise the direct enum bytes lane without falling back to
    /// a nested structured wrapper codec.
    ///

    #[enum_(
        variant(unspecified, default),
        variant(ident = "Loaded", value(item(prim = "Nat32"))),
        variant(ident = "Named", value(item(prim = "Text", unbounded)))
    )]
    pub struct PrimitiveEnumWithPayload {}

    ///
    /// Stage
    ///
    /// Local unit enum used to lock the generated `InputValue` bridge without
    /// coupling filter ergonomics to payload enum behavior.
    ///

    #[enum_(variant(ident = "Draft", default), variant(ident = "Live"))]
    pub struct Stage {}

    #[test]
    fn enum_runtime_value_carries_payload() {
        let v = EnumWithPayload::Icp(Tokens::from(123_u64));

        match runtime_value_to_value(&v) {
            Value::Enum(e) => {
                assert_eq!(e.variant(), "Icp");
                assert_eq!(
                    e.payload(),
                    Some(&runtime_value_to_value(&Tokens::from(123_u64)))
                );
            }
            other => panic!("expected Value::Enum with payload, got {other:?}"),
        }
    }

    #[test]
    fn vec_box_runtime_value_roundtrips() {
        let value = Value::Uint(5);
        let vec: Vec<Box<Value>> = vec![Box::new(value.clone())];
        let list = runtime_value_to_value(&vec);
        assert_eq!(list, Value::List(vec![value]));
    }

    #[test]
    fn option_runtime_value_handles_some_and_none() {
        let some_val: Option<Value> = Some(Value::Uint(7));
        let none_val: Option<Value> = None;

        assert_eq!(runtime_value_to_value(&some_val), Value::Uint(7));
        assert_eq!(runtime_value_to_value(&none_val), Value::Null);
    }

    #[test]
    fn primitive_enum_structured_slot_payload_roundtrips_through_storage_helpers() {
        let value = PrimitiveEnumWithPayload::Loaded(7);
        let payload = encode_persisted_structured_slot_payload(&value, "status")
            .expect("encode enum payload");
        let decoded = decode_persisted_structured_slot_payload::<PrimitiveEnumWithPayload>(
            payload.as_slice(),
            "status",
        )
        .expect("decode enum payload");

        assert_eq!(decoded, value);
    }

    #[test]
    fn generated_unit_enum_lowers_to_input_value() {
        assert_eq!(Stage::LIVE, "Live");

        let input = InputValue::from(Stage::Live);

        match input {
            InputValue::Enum(value) => {
                assert_eq!(value.variant(), "Live");
                assert_eq!(value.path(), Some(<Stage as icydb::traits::Path>::PATH));
                assert_eq!(value.payload(), None);
            }
            other => panic!("expected InputValue::Enum, got {other:?}"),
        }
    }

    #[test]
    fn generated_enum_reference_lowers_to_input_value() {
        let value = Stage::Draft;
        let input = InputValue::from(&value);

        match input {
            InputValue::Enum(value) => {
                assert_eq!(value.variant(), "Draft");
                assert_eq!(value.path(), Some(<Stage as icydb::traits::Path>::PATH));
                assert_eq!(value.payload(), None);
            }
            other => panic!("expected InputValue::Enum, got {other:?}"),
        }
    }

    #[test]
    fn generated_unit_enum_can_be_used_as_fluent_filter_literal() {
        let expr = FilterExpr::eq("stage", Stage::Live);

        assert!(matches!(expr, FilterExpr::Eq { .. }));
    }
}
