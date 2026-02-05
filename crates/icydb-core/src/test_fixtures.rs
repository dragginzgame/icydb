use crate::model::{entity::EntityModel, field::EntityFieldModel, index::IndexModel};

///
/// LegacyTestEntityModel
///
/// Legacy test-only helper for constructing `EntityModel` directly.
/// Prefer `test_entity_schema!` for valid models; reserve this for
/// intentionally invalid schemas or tests that must bypass typed entities.
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

///
/// test_entity_schema
///
/// Test-only helper to define a typed entity schema and derived model.
/// Prefer this over `LegacyTestEntityModel` when the model is valid.
///
#[macro_export]
macro_rules! test_entity_schema {
    (
        $name:ident,
        id = $id_ty:ty,
        path = $path:expr,
        entity_name = $entity_name:expr,
        primary_key = $primary_key:expr,
        pk_index = $pk_index:expr,
        fields = [ $( ($field_name:expr, $field_kind:expr) ),+ $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
    ) => {
        struct $name;

        impl $name {
            const FIELD_MODELS: [$crate::model::field::EntityFieldModel;
                $crate::test_entity_schema!(@count $( $field_name ),+)
            ] = [
                $(
                    $crate::model::field::EntityFieldModel {
                        name: $field_name,
                        kind: $field_kind,
                    },
                )+
            ];
            const FIELD_NAMES: [&'static str;
                $crate::test_entity_schema!(@count $( $field_name ),+)
            ] = [
                $( $field_name, )+
            ];
            const INDEXES_DEF: [&'static $crate::model::index::IndexModel;
                $crate::test_entity_schema!(@count $( $index ),*)
            ] = [
                $( $index, )*
            ];
            const MODEL_DEF: $crate::model::entity::EntityModel =
                $crate::test_fixtures::LegacyTestEntityModel::from_static(
                    $path,
                    $entity_name,
                    &Self::FIELD_MODELS[$pk_index],
                    &Self::FIELD_MODELS,
                    &Self::INDEXES_DEF,
                );
        }

        impl $crate::traits::EntityIdentity for $name {
            type Id = $id_ty;

            const ENTITY_NAME: &'static str = $entity_name;
            const PRIMARY_KEY: &'static str = $primary_key;
        }

        impl $crate::traits::EntitySchema for $name {
            const MODEL: &'static $crate::model::entity::EntityModel = &Self::MODEL_DEF;
            const FIELDS: &'static [&'static str] = &Self::FIELD_NAMES;
            const INDEXES: &'static [&'static $crate::model::index::IndexModel] =
                &Self::INDEXES_DEF;
        }
    };
    (@count $( $value:expr ),* ) => {
        <[()]>::len(&[ $( $crate::test_entity_schema!(@unit $value) ),* ])
    };
    (@unit $value:expr) => {
        ()
    };
}
