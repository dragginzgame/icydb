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
        __macro::{Value, runtime_value_to_value},
        db::{decode_persisted_custom_slot_payload, encode_persisted_custom_slot_payload},
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
        variant(ident = "Named", value(item(prim = "Text")))
    )]
    pub struct PrimitiveEnumWithPayload {}

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
    fn primitive_enum_custom_slot_payload_roundtrips_through_storage_helpers() {
        let value = PrimitiveEnumWithPayload::Loaded(7);
        let payload =
            encode_persisted_custom_slot_payload(&value, "status").expect("encode enum payload");
        let decoded = decode_persisted_custom_slot_payload::<PrimitiveEnumWithPayload>(
            payload.as_slice(),
            "status",
        )
        .expect("decode enum payload");

        assert_eq!(decoded, value);
    }
}
