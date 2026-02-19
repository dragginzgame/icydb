use crate::model::{entity::EntityModel, field::FieldModel, index::IndexModel};

/// Construct a test `EntityModel` from static components.
pub(crate) const fn entity_model_from_static(
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

pub(crate) struct InvalidEntityModelBuilder;

impl InvalidEntityModelBuilder {
    ///
    /// from_fields
    ///
    /// Build an invalid test `EntityModel` with default identity and no indexes.
    /// Leaks field storage to satisfy static lifetime requirements.
    ///
    pub(crate) fn from_fields(fields: Vec<FieldModel>, pk_index: usize) -> EntityModel {
        Self::from_fields_and_indexes("test_fixtures::Entity", "TestEntity", fields, pk_index, &[])
    }

    ///
    /// from_fields_and_indexes
    ///
    /// Build an invalid test `EntityModel` with explicit identity and indexes.
    /// Leaks field storage to satisfy static lifetime requirements.
    ///
    pub(crate) fn from_fields_and_indexes(
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
    pub(crate) const fn from_static(
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
/// impl_test_entity_markers
///
/// Test-only helper macro for the common marker-trait boilerplate used by
/// test entity definitions.
///
#[macro_export]
macro_rules! impl_test_entity_markers {
    ($entity:ty) => {
        impl $crate::traits::SanitizeAuto for $entity {}
        impl $crate::traits::SanitizeCustom for $entity {}
        impl $crate::traits::ValidateAuto for $entity {}
        impl $crate::traits::ValidateCustom for $entity {}
        impl $crate::traits::Visitable for $entity {}
    };
}

///
/// impl_test_entity_view_markers
///
/// Test-only helper macro for common trivial `AsView` + marker trait boilerplate.
///
#[macro_export]
macro_rules! impl_test_entity_view_markers {
    ($entity:ty) => {
        impl $crate::traits::AsView for $entity {
            type ViewType = Self;

            fn as_view(&self) -> Self::ViewType {
                self.clone()
            }

            fn from_view(view: Self::ViewType) -> Self {
                view
            }
        }

        $crate::impl_test_entity_markers!($entity);
    };
}

///
/// test_canister
///
/// Test-only helper to define a canister marker type with a static path.
///
#[macro_export]
macro_rules! test_canister {
    (
        ident = $canister:ident $(,)?
    ) => {
        struct $canister;

        impl $crate::traits::Path for $canister {
            const PATH: &'static str = concat!(module_path!(), "::", stringify!($canister));
        }

        impl $crate::traits::CanisterKind for $canister {}
    };
}

///
/// test_store
///
/// Test-only helper to define a store marker type with a static path and
/// associated canister binding.
///
#[macro_export]
macro_rules! test_store {
    (
        ident = $store:ident,
        canister = $canister:ty $(,)?
    ) => {
        struct $store;

        impl $crate::traits::Path for $store {
            const PATH: &'static str = concat!(module_path!(), "::", stringify!($store));
        }

        impl $crate::traits::StoreKind for $store {
            type Canister = $canister;
        }
    };
}

///
/// test_entity
///
/// Test-only helper to define a test entity type and derive its schema model.
/// Prefer this over `InvalidEntityModelBuilder` when the model is valid.
///
#[macro_export]
macro_rules! test_entity {
    (
        ident = $name:ident,
        id = $id_ty:ty,
        entity_name = $entity_name:expr,
        primary_key = $primary_key:expr,
        pk_index = $pk_index:expr,
        fields = [ $( ($field_name:expr, $field_kind:expr) ),+ $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
    ) => {
        struct $name;

        impl $name {
            const FIELD_MODELS: [$crate::model::field::FieldModel;
                $crate::test_entity!(@count $( $field_name ),+)
            ] = [
                $(
                    $crate::model::field::FieldModel {
                        name: $field_name,
                        kind: $field_kind,
                    },
                )+
            ];
            const FIELD_NAMES: [&'static str;
                $crate::test_entity!(@count $( $field_name ),+)
            ] = [
                $( $field_name, )+
            ];
            const INDEXES_DEF: [&'static $crate::model::index::IndexModel;
                $crate::test_entity!(@count $( $index ),*)
            ] = [
                $( $index, )*
            ];
            const MODEL_DEF: $crate::model::entity::EntityModel =
                $crate::test_support::entity_model_from_static(
                    concat!(module_path!(), "::", stringify!($name)),
                    $entity_name,
                    &Self::FIELD_MODELS[$pk_index],
                    &Self::FIELD_MODELS,
                    &Self::INDEXES_DEF,
                );
        }

        impl $crate::traits::EntityKey for $name {
            type Key = $id_ty;
        }

        impl $crate::traits::Path for $name {
            const PATH: &'static str = concat!(module_path!(), "::", stringify!($name));
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
        <[()]>::len(&[ $( $crate::test_entity!(@unit $value) ),* ])
    };
    (@unit $value:expr) => {
        ()
    };
}

///
/// test_entity_schema
///
/// Test-only helper to attach typed entity schema/placement/value traits
/// to an existing test entity struct.
///
#[macro_export]
macro_rules! test_entity_schema {
    (
        ident = $entity:ident,
        id = $id_ty:ty,
        entity_name = $entity_name:expr,
        primary_key = $primary_key:expr,
        pk_index = $pk_index:expr,
        fields = [ $( ($field_name:expr, $field_kind:expr) ),+ $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
    ) => {
        impl $crate::traits::AsView for $entity {
            type ViewType = Self;

            fn as_view(&self) -> Self::ViewType {
                self.clone()
            }

            fn from_view(view: Self::ViewType) -> Self {
                view
            }
        }

        $crate::impl_test_entity_markers!($entity);

        impl $entity {
            const TEST_FIELD_MODELS: [$crate::model::field::FieldModel;
                $crate::test_entity_schema!(@count $( $field_name ),+)
            ] = [
                $(
                    $crate::model::field::FieldModel {
                        name: $field_name,
                        kind: $field_kind,
                    },
                )+
            ];
            const TEST_FIELD_NAMES: [&'static str;
                $crate::test_entity_schema!(@count $( $field_name ),+)
            ] = [
                $( $field_name, )+
            ];
            const TEST_INDEXES_DEF: [&'static $crate::model::index::IndexModel;
                $crate::test_entity_schema!(@count $( $index ),*)
            ] = [
                $( $index, )*
            ];
            const TEST_MODEL_DEF: $crate::model::entity::EntityModel =
                $crate::test_support::entity_model_from_static(
                    concat!(module_path!(), "::", stringify!($entity)),
                    $entity_name,
                    &Self::TEST_FIELD_MODELS[$pk_index],
                    &Self::TEST_FIELD_MODELS,
                    &Self::TEST_INDEXES_DEF,
                );
        }

        impl $crate::traits::EntityKey for $entity {
            type Key = $id_ty;
        }

        impl $crate::traits::Path for $entity {
            const PATH: &'static str = concat!(module_path!(), "::", stringify!($entity));
        }

        impl $crate::traits::EntityIdentity for $entity {
            const ENTITY_NAME: &'static str = $entity_name;
            const PRIMARY_KEY: &'static str = $primary_key;
        }

        impl $crate::traits::EntitySchema for $entity {
            const MODEL: &'static $crate::model::entity::EntityModel = &Self::TEST_MODEL_DEF;
            const FIELDS: &'static [&'static str] = &Self::TEST_FIELD_NAMES;
            const INDEXES: &'static [&'static $crate::model::index::IndexModel] =
                &Self::TEST_INDEXES_DEF;
        }
    };
    (
        ident = $entity:ident,
        id = $id_ty:ty,
        entity_name = $entity_name:expr,
        primary_key = $primary_key:expr,
        pk_index = $pk_index:expr,
        fields = [ $( ($field_name:expr, $field_kind:expr) ),+ $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
        store = $store_ty:ty,
        canister = $canister_ty:ty,
    ) => {
        $crate::test_entity_schema! {
            ident = $entity,
            id = $id_ty,
            entity_name = $entity_name,
            primary_key = $primary_key,
            pk_index = $pk_index,
            fields = [ $( ($field_name, $field_kind) ),+ ],
            indexes = [ $( $index ),* ],
        }

        impl $crate::traits::EntityPlacement for $entity {
            type Store = $store_ty;
            type Canister = $canister_ty;
        }

        impl $crate::traits::EntityKind for $entity {}
    };
    (
        ident = $entity:ident,
        id = $id_ty:ty,
        id_field = $id_field:ident,
        entity_name = $entity_name:expr,
        primary_key = $primary_key:expr,
        pk_index = $pk_index:expr,
        fields = [ $( ($field_name:expr, $field_kind:expr) ),+ $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
        store = $store_ty:ty,
        canister = $canister_ty:ty,
    ) => {
        $crate::test_entity_schema! {
            ident = $entity,
            id = $id_ty,
            entity_name = $entity_name,
            primary_key = $primary_key,
            pk_index = $pk_index,
            fields = [ $( ($field_name, $field_kind) ),+ ],
            indexes = [ $( $index ),* ],
            store = $store_ty,
            canister = $canister_ty,
        }

        impl $crate::traits::EntityValue for $entity {
            #[allow(clippy::unit_arg)]
            fn id(&self) -> $crate::types::Id<Self> {
                $crate::types::Id::from_key(self.$id_field)
            }
        }
    };
    (
        ident = $entity:ident,
        id = $id_ty:ty,
        id_field = $id_field:ident,
        singleton = true,
        entity_name = $entity_name:expr,
        primary_key = $primary_key:expr,
        pk_index = $pk_index:expr,
        fields = [ $( ($field_name:expr, $field_kind:expr) ),+ $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
        store = $store_ty:ty,
        canister = $canister_ty:ty,
    ) => {
        $crate::test_entity_schema! {
            ident = $entity,
            id = $id_ty,
            id_field = $id_field,
            entity_name = $entity_name,
            primary_key = $primary_key,
            pk_index = $pk_index,
            fields = [ $( ($field_name, $field_kind) ),+ ],
            indexes = [ $( $index ),* ],
            store = $store_ty,
            canister = $canister_ty,
        }

        impl $crate::traits::SingletonEntity for $entity {}
    };
    (@count $( $value:expr ),* ) => {
        <[()]>::len(&[ $( $crate::test_entity_schema!(@unit $value) ),* ])
    };
    (@unit $value:expr) => {
        ()
    };
}
