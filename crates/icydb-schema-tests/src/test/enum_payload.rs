use crate::prelude::*;

///
/// EnumWithPayload
///

#[enum_(
    variant(unspecified, default),
    variant(ident = "Icp", value(item(is = "base::types::ic::icp::Tokens")))
)]
pub struct EnumWithPayload {}

///
/// EnumEntity
///

#[entity(
    store = "TestDataStore",
    pk = "id",
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "cost", value(item(is = "EnumWithPayload")))
    )
)]
pub struct EnumEntity {}

///
/// TESTS
///

#[cfg(test)]
pub mod test {
    use super::*;
    use base::types::ic::icp::Tokens;
    use icydb::{deserialize, serialize};

    #[test]
    fn enum_field_value_carries_payload() {
        let v = EnumWithPayload::Icp(Tokens::from(123_u64));

        match icydb::traits::FieldValue::to_value(&v) {
            icydb::value::Value::Enum(e) => {
                assert_eq!(e.variant, "Icp");
                assert_eq!(
                    e.payload.as_deref(),
                    Some(&icydb::traits::FieldValue::to_value(&Tokens::from(123_u64)))
                );
            }
            other => panic!("expected Value::Enum with payload, got {other:?}"),
        }
    }

    #[test]
    fn enum_with_tokens_roundtrips_via_serialize() {
        let entity = EnumEntity {
            id: Id::new(Ulid::generate()),
            cost: EnumWithPayload::Icp(Tokens::from(42_u64)),
            ..Default::default()
        };

        let bytes = serialize(&entity).expect("serialize enum with payload");
        let decoded: EnumEntity = deserialize(&bytes).expect("deserialize enum with payload");

        assert_eq!(entity, decoded);
    }

    #[test]
    fn vec_box_value_field_value() {
        use icydb::traits::FieldValue;

        let value = icydb::value::Value::Uint(5);
        let vec: Vec<Box<icydb::value::Value>> = vec![Box::new(value.clone())];
        let list = FieldValue::to_value(&vec);
        assert_eq!(list, icydb::value::Value::List(vec![value]));
    }

    #[test]
    fn option_field_value_handles_some_and_none() {
        use icydb::{traits::FieldValue, value::Value};

        let some_val: Option<Value> = Some(Value::Uint(7));
        let none_val: Option<Value> = None;

        assert_eq!(FieldValue::to_value(&some_val), Value::Uint(7));
        assert_eq!(FieldValue::to_value(&none_val), Value::None);
    }
}
