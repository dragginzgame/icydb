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
    use icydb::__macro::{FieldValue, Value};

    #[entity(
        store = "TestStore",
        pk(field = "id"),
        fields(
            field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
            field(ident = "cost", value(item(is = "EnumWithPayload")))
        )
    )]
    pub struct EnumEntityHarness {}

    #[test]
    fn enum_field_value_carries_payload() {
        let v = EnumWithPayload::Icp(Tokens::from(123_u64));

        match FieldValue::to_value(&v) {
            Value::Enum(e) => {
                assert_eq!(e.variant(), "Icp");
                assert_eq!(
                    e.payload(),
                    Some(&FieldValue::to_value(&Tokens::from(123_u64)))
                );
            }
            other => panic!("expected Value::Enum with payload, got {other:?}"),
        }
    }

    #[test]
    fn vec_box_value_field_value() {
        let value = Value::Uint(5);
        let vec: Vec<Box<Value>> = vec![Box::new(value.clone())];
        let list = FieldValue::to_value(&vec);
        assert_eq!(list, Value::List(vec![value]));
    }

    #[test]
    fn option_field_value_handles_some_and_none() {
        let some_val: Option<Value> = Some(Value::Uint(7));
        let none_val: Option<Value> = None;

        assert_eq!(FieldValue::to_value(&some_val), Value::Uint(7));
        assert_eq!(FieldValue::to_value(&none_val), Value::Null);
    }
}
