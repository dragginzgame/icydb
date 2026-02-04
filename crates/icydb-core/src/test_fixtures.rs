use crate::model::{entity::EntityModel, field::EntityFieldModel, index::IndexModel};

///
/// LegacyTestEntityModel
///
/// Legacy test-only helper for constructing `EntityModel` directly.
/// This bypasses typed entities intentionally.
///

pub struct LegacyTestEntityModel;

impl LegacyTestEntityModel {
    ///
    /// from_fields
    ///
    /// Build a legacy test `EntityModel` with default identity and no indexes.
    /// Leaks field storage to satisfy static lifetime requirements.
    ///
    pub fn from_fields(fields: Vec<EntityFieldModel>, pk_index: usize) -> EntityModel {
        Self::from_fields_and_indexes("test_fixtures::Entity", "TestEntity", fields, pk_index, &[])
    }

    ///
    /// from_fields_and_indexes
    ///
    /// Build a legacy test `EntityModel` with explicit identity and indexes.
    /// Leaks field storage to satisfy static lifetime requirements.
    ///
    pub fn from_fields_and_indexes(
        path: &'static str,
        entity_name: &'static str,
        fields: Vec<EntityFieldModel>,
        pk_index: usize,
        indexes: &'static [&'static IndexModel],
    ) -> EntityModel {
        // Leak the fields to satisfy the static lifetime required by EntityModel.
        let fields: &'static [EntityFieldModel] = Box::leak(fields.into_boxed_slice());
        let primary_key = &fields[pk_index];

        EntityModel {
            path,
            entity_name,
            primary_key,
            fields,
            indexes,
        }
    }

    ///
    /// from_static
    ///
    /// Build a legacy test `EntityModel` from pre-allocated static slices.
    ///
    pub const fn from_static(
        path: &'static str,
        entity_name: &'static str,
        primary_key: &'static EntityFieldModel,
        fields: &'static [EntityFieldModel],
        indexes: &'static [&'static IndexModel],
    ) -> EntityModel {
        EntityModel {
            path,
            entity_name,
            primary_key,
            fields,
            indexes,
        }
    }
}
