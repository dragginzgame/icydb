/*
///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::SaveExecutor;
    use crate::{
        error::{ErrorClass, ErrorOrigin},
        traits::{
            FieldValues, SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, View,
            Visitable,
        },
        types::Ulid,
        value::Value,
    };
    use serde::{Deserialize, Serialize};

    /// Deliberately violates `key() == primary_key()` for invariant testing.
    #[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
    struct BadKeyEntity {
        id: Ulid,
        other: Ulid,
    }

    crate::test_entity! {
        entity BadKeyEntity {
            path: "save_invariant_test::BadKeyEntity",
            pk: id: Ulid,

            fields { id: Ulid }
        }
    }

    /// Deliberately returns an inconsistent primary key field value.
    #[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
    struct BadFieldEntity {
        id: Ulid,
        other: Ulid,
    }

    crate::test_entity! {
        entity BadFieldEntity {
            path: "save_invariant_test::BadFieldEntity",
            pk: id: Ulid,

            fields { id: Ulid }
        }
    }

    /// Deliberately returns an invalid value type for the primary key field.
    #[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
    struct BadTypeEntity {
        id: Ulid,
    }

    crate::test_entity! {
        entity BadTypeEntity {
            path: "save_invariant_test::BadTypeEntity",
            pk: id: Ulid,

            fields { id: Ulid }
        }
    }

    impl View for BadKeyEntity {
        type ViewType = Self;

        fn to_view(&self) -> Self::ViewType {
            self.clone()
        }

        fn from_view(view: Self::ViewType) -> Self {
            view
        }
    }

    impl View for BadFieldEntity {
        type ViewType = Self;

        fn to_view(&self) -> Self::ViewType {
            self.clone()
        }

        fn from_view(view: Self::ViewType) -> Self {
            view
        }
    }

    impl View for BadTypeEntity {
        type ViewType = Self;

        fn to_view(&self) -> Self::ViewType {
            self.clone()
        }

        fn from_view(view: Self::ViewType) -> Self {
            view
        }
    }

    impl SanitizeAuto for BadKeyEntity {}
    impl SanitizeCustom for BadKeyEntity {}
    impl ValidateAuto for BadKeyEntity {}
    impl ValidateCustom for BadKeyEntity {}
    impl Visitable for BadKeyEntity {}

    impl SanitizeAuto for BadFieldEntity {}
    impl SanitizeCustom for BadFieldEntity {}
    impl ValidateAuto for BadFieldEntity {}
    impl ValidateCustom for BadFieldEntity {}
    impl Visitable for BadFieldEntity {}

    impl SanitizeAuto for BadTypeEntity {}
    impl SanitizeCustom for BadTypeEntity {}
    impl ValidateAuto for BadTypeEntity {}
    impl ValidateCustom for BadTypeEntity {}
    impl Visitable for BadTypeEntity {}

    impl FieldValues for BadKeyEntity {
        fn get_value(&self, field: &str) -> Option<Value> {
            match field {
                "id" => Some(Value::Ulid(self.other)),
                _ => None,
            }
        }
    }

    impl FieldValues for BadFieldEntity {
        fn get_value(&self, field: &str) -> Option<Value> {
            match field {
                "id" => None,
                _ => Some(Value::Ulid(self.other)),
            }
        }
    }

    impl FieldValues for BadTypeEntity {
        fn get_value(&self, field: &str) -> Option<Value> {
            match field {
                "id" => Some(Value::Text(self.id.to_string())),
                _ => None,
            }
        }
    }

    #[test]
    fn validate_entity_invariants_rejects_key_mismatch() {
        let entity = BadKeyEntity {
            id: Ulid::from_u128(1),
            other: Ulid::from_u128(2),
        };
        let schema = SaveExecutor::<BadKeyEntity>::schema_info().expect("schema");
        let err = SaveExecutor::<BadKeyEntity>::validate_entity_invariants(&entity, schema)
            .expect_err("expected key mismatch to fail");

        assert_eq!(err.class, ErrorClass::InvariantViolation);
        assert_eq!(err.origin, ErrorOrigin::Executor);
    }

    #[test]
    fn validate_entity_invariants_rejects_field_mismatch() {
        let entity = BadFieldEntity {
            id: Ulid::from_u128(1),
            other: Ulid::from_u128(2),
        };
        let schema = SaveExecutor::<BadFieldEntity>::schema_info().expect("schema");
        let err = SaveExecutor::<BadFieldEntity>::validate_entity_invariants(&entity, schema)
            .expect_err("expected field mismatch to fail");

        assert_eq!(err.class, ErrorClass::InvariantViolation);
        assert_eq!(err.origin, ErrorOrigin::Executor);
    }

    #[test]
    fn validate_entity_invariants_rejects_type_mismatch() {
        let entity = BadTypeEntity {
            id: Ulid::from_u128(1),
        };
        let schema = SaveExecutor::<BadTypeEntity>::schema_info().expect("schema");
        let err = SaveExecutor::<BadTypeEntity>::validate_entity_invariants(&entity, schema)
            .expect_err("expected type mismatch to fail");

        assert_eq!(err.class, ErrorClass::InvariantViolation);
        assert_eq!(err.origin, ErrorOrigin::Executor);
    }
}
*/
