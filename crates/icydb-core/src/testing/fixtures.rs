//! Module: testing::fixtures
//! Responsibility: module-local ownership and contracts for testing::fixtures.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::model::{entity::EntityModel, field::FieldModel, index::IndexModel};

/// Construct a test `EntityModel` from static components.
pub(crate) const fn entity_model_from_static(
    path: &'static str,
    entity_name: &'static str,
    primary_key: &'static FieldModel,
    primary_key_slot: usize,
    fields: &'static [FieldModel],
    indexes: &'static [&'static IndexModel],
) -> EntityModel {
    EntityModel::generated(
        path,
        entity_name,
        primary_key,
        primary_key_slot,
        fields,
        indexes,
    )
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
/// test_canister
///
/// Test-only helper to define a canister marker type with a static path.
///
#[macro_export]
macro_rules! test_canister {
    (
        ident = $canister:ident $(,)?
    ) => {
        compile_error!("test_canister! requires `commit_memory_id = <u8>`");
    };
    (
        ident = $canister:ident,
        commit_memory_id = $commit_memory_id:expr $(,)?
    ) => {
        struct $canister;

        impl $crate::traits::Path for $canister {
            const PATH: &'static str = concat!(module_path!(), "::", stringify!($canister));
        }

        impl $crate::traits::CanisterKind for $canister {
            const COMMIT_MEMORY_ID: u8 = $commit_memory_id;
        }
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

/// Hidden helper that keeps the common runtime trait surface for test entities
/// in one place so the two public test helper macros cannot drift.
#[doc(hidden)]
#[macro_export]
macro_rules! impl_test_entity_runtime_surface {
    ($entity:ident, $id_ty:ty, $entity_name:expr, $model_ident:ident) => {
        impl $crate::traits::EntityKey for $entity {
            type Key = $id_ty;
        }

        impl $crate::traits::Path for $entity {
            const PATH: &'static str = concat!(module_path!(), "::", stringify!($entity));
        }

        impl $crate::traits::EntitySchema for $entity {
            const NAME: &'static str = $entity_name;
            const MODEL: &'static $crate::model::entity::EntityModel = &Self::$model_ident;
        }
    };
}

/// Hidden helper that builds the shared static model storage used by both test
/// entity helper macros.
#[doc(hidden)]
#[macro_export]
macro_rules! impl_test_entity_model_storage {
    (
        $entity:ident,
        $entity_name:expr,
        $pk_index:expr,
        fields = [ $( $field_model:expr ),+ $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
    ) => {
        impl $entity {
            const FIELD_MODELS: [$crate::model::field::FieldModel;
                $crate::impl_test_entity_model_storage!(@count $( $field_model ),+)
            ] = [
                $( $field_model, )+
            ];
            const INDEXES_DEF: [&'static $crate::model::index::IndexModel;
                $crate::impl_test_entity_model_storage!(@count $( $index ),*)
            ] = [
                $( $index, )*
            ];
            const MODEL_DEF: $crate::model::entity::EntityModel =
                $crate::testing::entity_model_from_static(
                    concat!(module_path!(), "::", stringify!($entity)),
                    $entity_name,
                    &Self::FIELD_MODELS[$pk_index],
                    $pk_index,
                    &Self::FIELD_MODELS,
                    &Self::INDEXES_DEF,
                );
        }
    };
    (@count $( $value:expr ),* ) => {
        <[()]>::len(&[ $( $crate::impl_test_entity_model_storage!(@unit $value) ),* ])
    };
    (@unit $value:expr) => {
        ()
    };
}

///
/// test_entity
///
/// Test-only helper to define a test entity type and derive its schema model.
///
#[macro_export]
macro_rules! test_entity {
    (
        ident = $name:ident,
        id = $id_ty:ty,
        entity_name = $entity_name:expr,
        pk_index = $pk_index:expr,
        fields = [ $( ($field_name:expr, $field_kind:expr) ),+ $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
    ) => {
        struct $name;

        $crate::impl_test_entity_model_storage!(
            $name,
            $entity_name,
            $pk_index,
            fields = [
                $(
                    $crate::model::field::FieldModel::generated($field_name, $field_kind)
                ),+
            ],
            indexes = [ $( $index ),* ],
        );

        $crate::impl_test_entity_runtime_surface!($name, $id_ty, $entity_name, MODEL_DEF);
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
    (@field_model $field_name:expr, $field_kind:expr) => {
        $crate::model::field::FieldModel::generated($field_name, $field_kind)
    };
    (@field_model $field_name:expr, $field_kind:expr, $field_decode:expr) => {
        $crate::model::field::FieldModel::generated_with_storage_decode(
            $field_name,
            $field_kind,
            $field_decode,
        )
    };
    (
        ident = $entity:ident,
        id = $id_ty:ty,
        entity_name = $entity_name:expr,
        pk_index = $pk_index:expr,
        fields = [ $( ($field_name:expr, $field_kind:expr $(, $field_decode:expr )? ) ),+ $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
    ) => {
        $crate::impl_test_entity_markers!($entity);

        $crate::impl_test_entity_model_storage!(
            $entity,
            $entity_name,
            $pk_index,
            fields = [
                $(
                    $crate::test_entity_schema!(@field_model $field_name, $field_kind $(, $field_decode)?)
                ),+
            ],
            indexes = [ $( $index ),* ],
        );

        $crate::impl_test_entity_runtime_surface!($entity, $id_ty, $entity_name, MODEL_DEF);
    };
    (
        ident = $entity:ident,
        id = $id_ty:ty,
        entity_name = $entity_name:expr,
        entity_tag = $entity_tag:expr,
        pk_index = $pk_index:expr,
        fields = [ $( ($field_name:expr, $field_kind:expr $(, $field_decode:expr )? ) ),+ $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
        store = $store_ty:ty,
        canister = $canister_ty:ty,
    ) => {
        $crate::test_entity_schema! {
            ident = $entity,
            id = $id_ty,
            entity_name = $entity_name,
            pk_index = $pk_index,
            fields = [ $( ($field_name, $field_kind $(, $field_decode )? ) ),+ ],
            indexes = [ $( $index ),* ],
        }

        impl $crate::traits::EntityPlacement for $entity {
            type Store = $store_ty;
            type Canister = $canister_ty;
        }

        impl $crate::traits::EntityKind for $entity {
            const ENTITY_TAG: $crate::types::EntityTag = $entity_tag;
        }
    };
    (
        ident = $entity:ident,
        id = $id_ty:ty,
        id_field = $id_field:ident,
        entity_name = $entity_name:expr,
        entity_tag = $entity_tag:expr,
        pk_index = $pk_index:expr,
        fields = [ $( ($field_name:expr, $field_kind:expr $(, $field_decode:expr )? ) ),+ $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
        store = $store_ty:ty,
        canister = $canister_ty:ty,
    ) => {
        $crate::test_entity_schema! {
            ident = $entity,
            id = $id_ty,
            entity_name = $entity_name,
            entity_tag = $entity_tag,
            pk_index = $pk_index,
            fields = [ $( ($field_name, $field_kind $(, $field_decode )? ) ),+ ],
            indexes = [ $( $index ),* ],
            store = $store_ty,
            canister = $canister_ty,
        }

        impl $crate::traits::EntityValue for $entity {
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
        entity_tag = $entity_tag:expr,
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
            entity_tag = $entity_tag,
            pk_index = $pk_index,
            fields = [ $( ($field_name, $field_kind) ),+ ],
            indexes = [ $( $index ),* ],
            store = $store_ty,
            canister = $canister_ty,
        }

        impl $crate::traits::SingletonEntity for $entity {}
    };
}
