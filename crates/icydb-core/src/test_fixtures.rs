use crate::model::{entity::EntityModel, field::FieldModel, index::IndexModel};

/// Construct a test `EntityModel` from static components.
pub const fn entity_model_from_static(
    path: &'static str,
    entity_name: &'static str,
    primary_key: &'static FieldModel,
    fields: &'static [FieldModel],
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

///
/// InvalidEntityModelBuilder
///
/// Test-only helper for constructing intentionally invalid `EntityModel` values.
/// Use this for negative tests that must bypass schema invariants.
///

pub struct InvalidEntityModelBuilder;

impl InvalidEntityModelBuilder {
    ///
    /// from_fields
    ///
    /// Build an invalid test `EntityModel` with default identity and no indexes.
    /// Leaks field storage to satisfy static lifetime requirements.
    ///
    pub fn from_fields(fields: Vec<FieldModel>, pk_index: usize) -> EntityModel {
        Self::from_fields_and_indexes("test_fixtures::Entity", "TestEntity", fields, pk_index, &[])
    }

    ///
    /// from_fields_and_indexes
    ///
    /// Build an invalid test `EntityModel` with explicit identity and indexes.
    /// Leaks field storage to satisfy static lifetime requirements.
    ///
    pub fn from_fields_and_indexes(
        path: &'static str,
        entity_name: &'static str,
        fields: Vec<FieldModel>,
        pk_index: usize,
        indexes: &'static [&'static IndexModel],
    ) -> EntityModel {
        // Leak the fields to satisfy the static lifetime required by EntityModel.
        let fields: &'static [FieldModel] = Box::leak(fields.into_boxed_slice());
        let primary_key = &fields[pk_index];

        entity_model_from_static(path, entity_name, primary_key, fields, indexes)
    }

    ///
    /// from_static
    ///
    /// Build an invalid test `EntityModel` from pre-allocated static slices.
    ///
    pub const fn from_static(
        path: &'static str,
        entity_name: &'static str,
        primary_key: &'static FieldModel,
        fields: &'static [FieldModel],
        indexes: &'static [&'static IndexModel],
    ) -> EntityModel {
        entity_model_from_static(path, entity_name, primary_key, fields, indexes)
    }
}

///
/// test_entity_schema
///
/// Test-only helper to define a typed entity schema and derived model.
/// Prefer this over `InvalidEntityModelBuilder` when the model is valid.
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
            const FIELD_MODELS: [$crate::model::field::FieldModel;
                $crate::test_entity_schema!(@count $( $field_name ),+)
            ] = [
                $(
                    $crate::model::field::FieldModel {
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
                $crate::test_fixtures::entity_model_from_static(
                    $path,
                    $entity_name,
                    &Self::FIELD_MODELS[$pk_index],
                    &Self::FIELD_MODELS,
                    &Self::INDEXES_DEF,
                );
        }

        impl $crate::traits::EntityKey for $name {
            type Key = $id_ty;
        }

        impl $crate::traits::EntityIdentity for $name {
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
