#[cfg(test)]
use crate::prelude::*;

pub use icydb_testing_test_fixtures::macro_test::field_projection_order::*;

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::{
        __macro::{FieldProjection, FieldValue, Value},
        traits::EntitySchema,
    };

    #[entity(
        store = "TestStore",
        pk(field = "id"),
        fields(
            field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
            field(ident = "title", value(item(prim = "Text"))),
            field(ident = "score", value(item(prim = "Nat32"))),
            field(ident = "nickname", value(opt, item(prim = "Text"))),
            field(ident = "tags", value(many, item(prim = "Text")))
        )
    )]
    pub struct ProjectionOrderEntityHarness {}

    #[test]
    fn field_projection_slot_order_matches_entity_model_field_order() {
        let entity = ProjectionOrderEntityHarness {
            id: Ulid::from_parts(100, 1),
            title: "alpha".to_string(),
            score: 42,
            nickname: Some("nick".to_string()),
            tags: vec!["one".to_string(), "two".to_string()],
            ..Default::default()
        };

        let expected = [
            ("id", entity.id.to_value()),
            ("title", entity.title.to_value()),
            ("score", entity.score.to_value()),
            (
                "nickname",
                entity
                    .nickname
                    .as_ref()
                    .map_or(Value::Null, FieldValue::to_value),
            ),
            (
                "tags",
                Value::List(entity.tags.iter().map(FieldValue::to_value).collect()),
            ),
            ("created_at", entity.created_at.to_value()),
            ("updated_at", entity.updated_at.to_value()),
        ];

        for (slot, (name, expected_value)) in expected.iter().enumerate() {
            let model_field = &ProjectionOrderEntityHarness::MODEL.fields()[slot];
            assert_eq!(model_field.name(), *name);

            let projected = entity
                .get_value_by_index(slot)
                .expect("all declared fields must project by slot index");
            assert_eq!(projected, *expected_value);
        }

        assert_eq!(entity.get_value_by_index(expected.len()), None);
    }

    #[test]
    fn field_projection_optional_none_is_null_in_declared_slot() {
        let entity = ProjectionOrderEntityHarness {
            id: Ulid::from_parts(101, 1),
            title: "beta".to_string(),
            score: 7,
            nickname: None,
            tags: Vec::new(),
            ..Default::default()
        };

        // Slot 3 is `nickname` in declared schema order.
        assert_eq!(entity.get_value_by_index(3), Some(Value::Null));
    }
}
