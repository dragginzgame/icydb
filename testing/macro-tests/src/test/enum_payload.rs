#[cfg(test)]
use crate::prelude::*;

pub use icydb_testing_test_fixtures::macro_test::enum_payload::*;

///
/// TESTS
///

#[cfg(test)]
pub mod test {
    use super::*;
    use icydb::{
        __macro::{Value, runtime_value_to_value},
        db::query::FilterExpr,
        traits::EntityDeclaration,
        value::InputValue,
    };

    #[entity(source_key = "testing/macro-tests/src/test/enum_payload.rs::entity::nested::1",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
        store = "TestStore",
        version = 1,
        pk(fields = ["id"]),
        fields(
            field(source_key = "id", ident = "id",
                value(item(prim = "Ulid")),
                generated(insert = "Ulid::generate")
            ),
            field(source_key = "cost", ident = "cost", value(item(is = "EnumWithPayload")))
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
        source_key = "testing/macro-tests/src/test/enum_payload.rs::enum_::nested::1",
        variant(source_key = "Loaded", ident = "Loaded", value(item(prim = "Nat32"))),
        variant(
            source_key = "Named",
            ident = "Named",
            value(item(prim = "Text", unbounded))
        )
    )]
    pub struct PrimitiveEnumWithPayload {}

    ///
    /// Stage
    ///
    /// Local unit enum used to lock the generated `InputValue` bridge without
    /// coupling filter ergonomics to payload enum behavior.
    ///

    #[enum_(
        source_key = "testing/macro-tests/src/test/enum_payload.rs::enum_::nested::2",
        variant(source_key = "Draft", ident = "Draft"),
        variant(source_key = "Live", ident = "Live")
    )]
    pub struct Stage {}

    #[entity(source_key = "testing/macro-tests/src/test/enum_payload.rs::entity::nested::2",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
        store = "TestStore",
        version = 1,
        pk(fields = ["id"]),
        fields(
            field(source_key = "id", ident = "id",
                value(item(prim = "Ulid")),
                generated(insert = "Ulid::generate")
            ),
            field(source_key = "stage", ident = "stage",
                value(item(is = "Stage")),
                default = "Stage::Draft"
            )
        )
    )]
    pub struct EnumDefaultEntityHarness {}

    #[test]
    fn generated_unit_enum_default_remains_authored_model_metadata() {
        let default = EnumDefaultEntityHarness::MODEL
            .fields()
            .iter()
            .find(|field| field.name() == "stage")
            .expect("generated enum field should exist")
            .database_default();

        assert_eq!(
            default,
            icydb::model::field::FieldDatabaseDefault::AuthoredEnumUnit {
                enum_path: <Stage as icydb::traits::Path>::PATH,
                variant: "Draft",
            },
        );
    }

    #[test]
    fn vec_box_runtime_value_roundtrips() {
        let value = Value::Nat64(5);
        let vec: Vec<Box<Value>> = vec![Box::new(value.clone())];
        let list = runtime_value_to_value(&vec);
        assert_eq!(list, Value::List(vec![value]));
    }

    #[test]
    fn option_runtime_value_handles_some_and_none() {
        let some_val: Option<Value> = Some(Value::Nat64(7));
        let none_val: Option<Value> = None;

        assert_eq!(runtime_value_to_value(&some_val), Value::Nat64(7));
        assert_eq!(runtime_value_to_value(&none_val), Value::Null);
    }

    #[test]
    fn generated_payload_enum_lowers_to_recursive_input_value() {
        let input = InputValue::from(PrimitiveEnumWithPayload::Loaded(7));

        match input {
            InputValue::Enum(value) => {
                assert_eq!(value.variant(), "Loaded");
                assert_eq!(
                    value.path(),
                    Some(<PrimitiveEnumWithPayload as icydb::traits::Path>::PATH),
                );
                assert_eq!(value.payload(), Some(&InputValue::Nat64(7)));
            }
            other => panic!("expected InputValue::Enum, got {other:?}"),
        }
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

        std::assert_matches!(expr, FilterExpr::Eq { .. });
    }
}
