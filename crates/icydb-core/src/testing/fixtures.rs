//! Module: testing::fixtures
//! Responsibility: shared fixture constructors and test helper macros.
//! Does not own: stable entity-tag assignment or production schema metadata.
//! Boundary: internal test-support utilities reused across `icydb-core` tests.

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

const fn str_eq(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();
    if left.len() != right.len() {
        return false;
    }

    let mut index = 0;
    while index < left.len() {
        if left[index] != right[index] {
            return false;
        }
        index += 1;
    }
    true
}

/// Resolve one field model by name from a static fixture field list.
///
/// Test relation helpers use this so relation declarations reference field
/// names, not hard-coded slots.
#[doc(hidden)]
pub(crate) const fn field_model_by_name(
    fields: &'static [FieldModel],
    name: &'static str,
) -> &'static FieldModel {
    let mut index = 0;
    while index < fields.len() {
        if str_eq(fields[index].name(), name) {
            return &fields[index];
        }
        index += 1;
    }
    panic!("test relation references a field that is not declared on the entity")
}

/// Resolve one field model slot by name from a static fixture field list.
#[doc(hidden)]
pub(crate) const fn field_model_index_by_name(
    fields: &'static [FieldModel],
    name: &'static str,
) -> usize {
    let mut index = 0;
    while index < fields.len() {
        if str_eq(fields[index].name(), name) {
            return index;
        }
        index += 1;
    }
    panic!("test primary key references a field that is not declared on the entity")
}

/// Hidden helper for the common marker-trait boilerplate used by test entity
/// definitions.
#[doc(hidden)]
#[macro_export]
macro_rules! __icydb_test_entity_markers {
    ($entity:ty) => {
        impl $crate::traits::SanitizeAuto for $entity {}
        impl $crate::traits::SanitizeCustom for $entity {}
        impl $crate::traits::ValidateAuto for $entity {}
        impl $crate::traits::ValidateCustom for $entity {}
        impl $crate::traits::Visitable for $entity {}
    };
}

/// Hidden canister-level helper used by `test_canister!`.
#[doc(hidden)]
#[macro_export]
macro_rules! __icydb_test_canister_model {
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
            const COMMIT_STABLE_KEY: &'static str = "icydb.test.commit.v1";
        }
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
        $crate::__icydb_test_canister_model! {
            ident = $canister,
            commit_memory_id = $commit_memory_id,
        }
    };
}

/// Hidden store-level helper used by `test_store!`.
#[doc(hidden)]
#[macro_export]
macro_rules! __icydb_test_store_model {
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
        $crate::__icydb_test_store_model! {
            ident = $store,
            canister = $canister,
        }
    };
}

/// Hidden helper that keeps the common runtime trait surface for test entities
/// in one place so the two public test helper macros cannot drift.
#[doc(hidden)]
#[macro_export]
macro_rules! __icydb_test_entity_runtime_surface {
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
macro_rules! __icydb_test_entity_model {
    (
        $entity:ident,
        $entity_name:expr,
        primary_key = fields([ $pk_field:ident ]),
        fields = [ $( $field_model:expr ),+ $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
        relations = [ $( $relation:expr ),* $(,)? ],
    ) => {
        impl $entity {
            const FIELD_MODELS: [$crate::model::field::FieldModel;
                $crate::__icydb_test_entity_model!(@count $( $field_model ),+)
            ] = [
                $( $field_model, )+
            ];
            const INDEXES_DEF: [&'static $crate::model::index::IndexModel;
                $crate::__icydb_test_entity_model!(@count $( $index ),*)
            ] = [
                $( $index, )*
            ];
            const RELATIONS_DEF: [$crate::model::entity::RelationEdgeModel;
                $crate::__icydb_test_entity_model!(@count $( $relation ),*)
            ] = [
                $( $relation, )*
            ];
            const MODEL_DEF: $crate::model::entity::EntityModel =
                $crate::model::entity::EntityModel::generated_with_primary_key_model_and_relations(
                    concat!(module_path!(), "::", stringify!($entity)),
                    $entity_name,
                    $crate::model::entity::PrimaryKeyModel::scalar(
                        $crate::testing::field_model_by_name(
                            &Self::FIELD_MODELS,
                            stringify!($pk_field),
                        ),
                    ),
                    $crate::testing::field_model_index_by_name(
                        &Self::FIELD_MODELS,
                        stringify!($pk_field),
                    ),
                    &Self::FIELD_MODELS,
                    &Self::INDEXES_DEF,
                    &Self::RELATIONS_DEF,
                );
        }
    };
    (
        $entity:ident,
        $entity_name:expr,
        primary_key = fields([ $pk_field_0:ident, $( $pk_field_rest:ident ),+ $(,)? ]),
        fields = [ $( $field_model:expr ),+ $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
        relations = [ $( $relation:expr ),* $(,)? ],
    ) => {
        impl $entity {
            const FIELD_MODELS: [$crate::model::field::FieldModel;
                $crate::__icydb_test_entity_model!(@count $( $field_model ),+)
            ] = [
                $( $field_model, )+
            ];
            const PRIMARY_KEY_FIELDS: [&'static $crate::model::field::FieldModel;
                $crate::__icydb_test_entity_model!(@count $pk_field_0, $( $pk_field_rest ),+)
            ] = [
                $crate::testing::field_model_by_name(
                    &Self::FIELD_MODELS,
                    stringify!($pk_field_0),
                ),
                $(
                    $crate::testing::field_model_by_name(
                        &Self::FIELD_MODELS,
                        stringify!($pk_field_rest),
                    ),
                )+
            ];
            const INDEXES_DEF: [&'static $crate::model::index::IndexModel;
                $crate::__icydb_test_entity_model!(@count $( $index ),*)
            ] = [
                $( $index, )*
            ];
            const RELATIONS_DEF: [$crate::model::entity::RelationEdgeModel;
                $crate::__icydb_test_entity_model!(@count $( $relation ),*)
            ] = [
                $( $relation, )*
            ];
            const MODEL_DEF: $crate::model::entity::EntityModel =
                $crate::model::entity::EntityModel::generated_with_primary_key_model_and_relations(
                    concat!(module_path!(), "::", stringify!($entity)),
                    $entity_name,
                    $crate::model::entity::PrimaryKeyModel::ordered(&Self::PRIMARY_KEY_FIELDS),
                    0,
                    &Self::FIELD_MODELS,
                    &Self::INDEXES_DEF,
                    &Self::RELATIONS_DEF,
                );
        }
    };
    (@count $( $value:expr ),* ) => {
        <[()]>::len(&[ $( $crate::__icydb_test_entity_model!(@unit $value) ),* ])
    };
    (@unit $value:expr) => {
        ()
    };
}

/// Hidden helper that emits the common test entity runtime traits.
#[doc(hidden)]
#[macro_export]
macro_rules! __icydb_test_entity_traits {
    (
        ident = $entity:ident,
        entity_name = $entity_name:expr,
        tag = $entity_tag:expr,
        store = $store_ty:ty,
        canister = $canister_ty:ty,
        key_type = $key_ty:ty,
        model = $model_ident:ident,
        entity_value = { $($entity_value:tt)+ } $(,)?
    ) => {
        $crate::__icydb_test_entity_runtime_surface!($entity, $key_ty, $entity_name, $model_ident);

        impl $crate::traits::EntityPlacement for $entity {
            type Store = $store_ty;
            type Canister = $canister_ty;
        }

        impl $crate::traits::EntityKind for $entity {
            const ENTITY_TAG: $crate::types::EntityTag = $entity_tag;
        }

        $crate::__icydb_test_entity_value!($entity, $($entity_value)+);
    };
}

/// Hidden helper that emits the optional `EntityValue` implementation for test
/// entities.
#[doc(hidden)]
#[macro_export]
macro_rules! __icydb_test_entity_value {
    ($entity:ident, none) => {};
    ($entity:ident, id_field($id_field:ident)) => {
        impl $crate::traits::EntityValue for $entity {
            fn id(&self) -> $crate::types::Id<Self> {
                $crate::types::Id::from_key(self.$id_field)
            }
        }
    };
    ($entity:ident, key($key_expr:expr)) => {
        impl $crate::traits::EntityValue for $entity {
            fn id(&self) -> $crate::types::Id<Self> {
                $crate::types::Id::from_key(($key_expr)(self))
            }
        }
    };
}

/// Hidden helper that emits optional singleton behavior for test entities.
#[doc(hidden)]
#[macro_export]
macro_rules! __icydb_test_entity_singleton {
    ($entity:ident, true) => {
        impl $crate::traits::SingletonEntity for $entity {}
    };
    ($entity:ident, false) => {};
}

/// Hidden field-level parser used by `test_field!`.
#[doc(hidden)]
#[macro_export]
macro_rules! __icydb_test_field_model {
    ($field_name:expr, $field_kind:expr $(,)?) => {
        $crate::__icydb_test_field_model! {
            @emit
            name = $field_name,
            kind = $field_kind,
            decode = $crate::model::field::FieldStorageDecode::ByKind,
            nullable = false,
            generated = None,
            managed = None,
            database_default = $crate::model::field::FieldDatabaseDefault::None,
            nested = &[],
        }
    };
    ($field_name:expr, $field_kind:expr, $($metadata:tt)+) => {
        $crate::__icydb_test_field_model! {
            @parse
            name = $field_name,
            kind = $field_kind,
            decode = $crate::model::field::FieldStorageDecode::ByKind,
            nullable = false,
            generated = None,
            managed = None,
            database_default = $crate::model::field::FieldDatabaseDefault::None,
            nested = &[],
            remaining = [ $($metadata)+, ],
        }
    };
    (@emit
        name = $field_name:expr,
        kind = $field_kind:expr,
        decode = $field_decode:expr,
        nullable = $field_nullable:expr,
        generated = $field_generation:expr,
        managed = $field_management:expr,
        database_default = $database_default:expr,
        nested = $nested_fields:expr,
    ) => {
        $crate::model::field::FieldModel::generated_with_storage_decode_nullability_write_policies_database_default_and_nested_fields(
            $field_name,
            $field_kind,
            $field_decode,
            $field_nullable,
            $field_generation,
            $field_management,
            $database_default,
            $nested_fields,
        )
    };
    (@parse
        name = $field_name:expr,
        kind = $field_kind:expr,
        decode = $field_decode:expr,
        nullable = $field_nullable:expr,
        generated = $field_generation:expr,
        managed = $field_management:expr,
        database_default = $database_default:expr,
        nested = $nested_fields:expr,
        remaining = [],
    ) => {
        $crate::__icydb_test_field_model! {
            @emit
            name = $field_name,
            kind = $field_kind,
            decode = $field_decode,
            nullable = $field_nullable,
            generated = $field_generation,
            managed = $field_management,
            database_default = $database_default,
            nested = $nested_fields,
        }
    };
    (@parse
        name = $field_name:expr,
        kind = $field_kind:expr,
        decode = $field_decode:expr,
        nullable = $field_nullable:expr,
        generated = $field_generation:expr,
        managed = $field_management:expr,
        database_default = $database_default:expr,
        nested = $nested_fields:expr,
        remaining = [, $($rest:tt)*],
    ) => {
        $crate::__icydb_test_field_model! {
            @parse
            name = $field_name,
            kind = $field_kind,
            decode = $field_decode,
            nullable = $field_nullable,
            generated = $field_generation,
            managed = $field_management,
            database_default = $database_default,
            nested = $nested_fields,
            remaining = [ $($rest)* ],
        }
    };
    (@parse
        name = $field_name:expr,
        kind = $field_kind:expr,
        decode = $field_decode:expr,
        nullable = $field_nullable:expr,
        generated = $field_generation:expr,
        managed = $field_management:expr,
        database_default = $database_default:expr,
        nested = $nested_fields:expr,
        remaining = [decode = $next_decode:expr, $($rest:tt)*],
    ) => {
        $crate::__icydb_test_field_model! {
            @parse
            name = $field_name,
            kind = $field_kind,
            decode = $next_decode,
            nullable = $field_nullable,
            generated = $field_generation,
            managed = $field_management,
            database_default = $database_default,
            nested = $nested_fields,
            remaining = [ $($rest)* ],
        }
    };
    (@parse
        name = $field_name:expr,
        kind = $field_kind:expr,
        decode = $field_decode:expr,
        nullable = $field_nullable:expr,
        generated = $field_generation:expr,
        managed = $field_management:expr,
        database_default = $database_default:expr,
        nested = $nested_fields:expr,
        remaining = [nullable = $next_nullable:expr, $($rest:tt)*],
    ) => {
        $crate::__icydb_test_field_model! {
            @parse
            name = $field_name,
            kind = $field_kind,
            decode = $field_decode,
            nullable = $next_nullable,
            generated = $field_generation,
            managed = $field_management,
            database_default = $database_default,
            nested = $nested_fields,
            remaining = [ $($rest)* ],
        }
    };
    (@parse
        name = $field_name:expr,
        kind = $field_kind:expr,
        decode = $field_decode:expr,
        nullable = $field_nullable:expr,
        generated = $field_generation:expr,
        managed = $field_management:expr,
        database_default = $database_default:expr,
        nested = $nested_fields:expr,
        remaining = [generated = $next_generation:expr, $($rest:tt)*],
    ) => {
        $crate::__icydb_test_field_model! {
            @parse
            name = $field_name,
            kind = $field_kind,
            decode = $field_decode,
            nullable = $field_nullable,
            generated = Some($next_generation),
            managed = $field_management,
            database_default = $database_default,
            nested = $nested_fields,
            remaining = [ $($rest)* ],
        }
    };
    (@parse
        name = $field_name:expr,
        kind = $field_kind:expr,
        decode = $field_decode:expr,
        nullable = $field_nullable:expr,
        generated = $field_generation:expr,
        managed = $field_management:expr,
        database_default = $database_default:expr,
        nested = $nested_fields:expr,
        remaining = [managed = $next_management:expr, $($rest:tt)*],
    ) => {
        $crate::__icydb_test_field_model! {
            @parse
            name = $field_name,
            kind = $field_kind,
            decode = $field_decode,
            nullable = $field_nullable,
            generated = $field_generation,
            managed = Some($next_management),
            database_default = $database_default,
            nested = $nested_fields,
            remaining = [ $($rest)* ],
        }
    };
    (@parse
        name = $field_name:expr,
        kind = $field_kind:expr,
        decode = $field_decode:expr,
        nullable = $field_nullable:expr,
        generated = $field_generation:expr,
        managed = $field_management:expr,
        database_default = $database_default:expr,
        nested = $nested_fields:expr,
        remaining = [database_default = $next_default:expr, $($rest:tt)*],
    ) => {
        $crate::__icydb_test_field_model! {
            @parse
            name = $field_name,
            kind = $field_kind,
            decode = $field_decode,
            nullable = $field_nullable,
            generated = $field_generation,
            managed = $field_management,
            database_default = $next_default,
            nested = $nested_fields,
            remaining = [ $($rest)* ],
        }
    };
    (@parse
        name = $field_name:expr,
        kind = $field_kind:expr,
        decode = $field_decode:expr,
        nullable = $field_nullable:expr,
        generated = $field_generation:expr,
        managed = $field_management:expr,
        database_default = $database_default:expr,
        nested = $nested_fields:expr,
        remaining = [nested = $next_nested_fields:expr, $($rest:tt)*],
    ) => {
        $crate::__icydb_test_field_model! {
            @parse
            name = $field_name,
            kind = $field_kind,
            decode = $field_decode,
            nullable = $field_nullable,
            generated = $field_generation,
            managed = $field_management,
            database_default = $database_default,
            nested = $next_nested_fields,
            remaining = [ $($rest)* ],
        }
    };
    (@parse
        name = $field_name:expr,
        kind = $field_kind:expr,
        decode = $field_decode:expr,
        nullable = $field_nullable:expr,
        generated = $field_generation:expr,
        managed = $field_management:expr,
        database_default = $database_default:expr,
        nested = $nested_fields:expr,
        remaining = [@generated $next_generation:expr, $($rest:tt)*],
    ) => {
        $crate::__icydb_test_field_model! {
            @parse
            name = $field_name,
            kind = $field_kind,
            decode = $field_decode,
            nullable = $field_nullable,
            generated = Some($next_generation),
            managed = $field_management,
            database_default = $database_default,
            nested = $nested_fields,
            remaining = [ $($rest)* ],
        }
    };
    (@parse
        name = $field_name:expr,
        kind = $field_kind:expr,
        decode = $field_decode:expr,
        nullable = $field_nullable:expr,
        generated = $field_generation:expr,
        managed = $field_management:expr,
        database_default = $database_default:expr,
        nested = $nested_fields:expr,
        remaining = [@managed $next_management:expr, $($rest:tt)*],
    ) => {
        $crate::__icydb_test_field_model! {
            @parse
            name = $field_name,
            kind = $field_kind,
            decode = $field_decode,
            nullable = $field_nullable,
            generated = $field_generation,
            managed = Some($next_management),
            database_default = $database_default,
            nested = $nested_fields,
            remaining = [ $($rest)* ],
        }
    };
    (@parse
        name = $field_name:expr,
        kind = $field_kind:expr,
        decode = $field_decode:expr,
        nullable = $field_nullable:expr,
        generated = $field_generation:expr,
        managed = $field_management:expr,
        database_default = $database_default:expr,
        nested = $nested_fields:expr,
        remaining = [$next_decode:expr, $($rest:tt)*],
    ) => {
        $crate::__icydb_test_field_model! {
            @parse
            name = $field_name,
            kind = $field_kind,
            decode = $next_decode,
            nullable = $field_nullable,
            generated = $field_generation,
            managed = $field_management,
            database_default = $database_default,
            nested = $nested_fields,
            remaining = [ $($rest)* ],
        }
    };
}

///
/// test_field
///
/// Test-only helper to construct one field model. This is the only test
/// fixture macro that owns field metadata parsing.
///
#[macro_export]
macro_rules! test_field {
    (
        name = $field_name:expr,
        ty = $field_ty:ty,
        kind = $field_kind:expr $(,)?
    ) => {
        $crate::__icydb_test_field_model!($field_name, $field_kind)
    };
    (
        name = $field_name:expr,
        ty = $field_ty:ty,
        kind = $field_kind:expr,
        $($metadata:tt)+
    ) => {
        $crate::__icydb_test_field_model!($field_name, $field_kind, $($metadata)+)
    };
    ($field:ident : $field_ty:ty => $field_kind:expr $(,)?) => {
        $crate::__icydb_test_field_model!(stringify!($field), $field_kind)
    };
    ($field:ident : $field_ty:ty => $field_kind:expr, $($metadata:tt)+) => {
        $crate::__icydb_test_field_model!(stringify!($field), $field_kind, $($metadata)+)
    };
}

///
/// test_relation
///
/// Test-only helper to construct one relation edge from declared local field
/// names. This macro is intended to be used inside `test_entity!` where
/// `Self::FIELD_MODELS` is in scope.
///
#[macro_export]
macro_rules! test_relation {
    (
        name = $relation_name:expr,
        target = $relation_target:ty,
        fields = [ $( $relation_field:ident ),+ $(,)? ] $(,)?
    ) => {
        $crate::model::entity::RelationEdgeModel::generated(
            $relation_name,
            <$relation_target as $crate::traits::Path>::PATH,
            &[
                $(
                    $crate::testing::field_model_by_name(
                        &Self::FIELD_MODELS,
                        stringify!($relation_field),
                    ),
                )+
            ],
        )
    };
}

/// Hidden normalized entity emitter used by `test_entity!`.
#[doc(hidden)]
#[macro_export]
macro_rules! __icydb_test_emit_entity {
    (
        ident = $entity:ident,
        entity_name = $entity_name:expr,
        tag = $entity_tag:expr,
        store = $store_ty:ty,
        canister = $canister_ty:ty,
        runtime = full,
        singleton = $singleton:tt,
        key_type = $key_ty:ty,
        primary_key = [ $( $pk_field:ident ),+ $(,)? ],
        fields = [ $( $field_model:expr ),+ $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
        relations = [ $( $relation:expr ),* $(,)? ],
        entity_value = { $($entity_value:tt)+ } $(,)?
    ) => {
        $crate::__icydb_test_entity_markers!($entity);

        $crate::__icydb_test_entity_model!(
            $entity,
            $entity_name,
            primary_key = fields([ $( $pk_field ),+ ]),
            fields = [ $( $field_model ),+ ],
            indexes = [ $( $index ),* ],
            relations = [ $( $relation ),* ],
        );

        $crate::__icydb_test_entity_traits! {
            ident = $entity,
            entity_name = $entity_name,
            tag = $entity_tag,
            store = $store_ty,
            canister = $canister_ty,
            key_type = $key_ty,
            model = MODEL_DEF,
            entity_value = { $($entity_value)+ },
        }

        $crate::__icydb_test_entity_singleton!($entity, $singleton);
    };
    (
        ident = $entity:ident,
        entity_name = $entity_name:expr,
        runtime = schema_only,
        key_type = $key_ty:ty,
        primary_key = [ $( $pk_field:ident ),+ $(,)? ],
        fields = [ $( $field_model:expr ),+ $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
        relations = [ $( $relation:expr ),* $(,)? ],
    ) => {
        struct $entity;

        $crate::__icydb_test_entity_model!(
            $entity,
            $entity_name,
            primary_key = fields([ $( $pk_field ),+ ]),
            fields = [ $( $field_model ),+ ],
            indexes = [ $( $index ),* ],
            relations = [ $( $relation ),* ],
        );

        $crate::__icydb_test_entity_runtime_surface!($entity, $key_ty, $entity_name, MODEL_DEF);
    };
}

/// Hidden compatibility parser used by `test_entity!`.
#[doc(hidden)]
#[macro_export]
macro_rules! __icydb_test_parse_entity {
    (
        ident = $entity:ident,
        entity_name = $entity_name:expr,
        tag = $entity_tag:expr,
        store = $store_ty:ty,
        canister = $canister_ty:ty,
        key_type = $key_ty:ty,
        primary_key = [ $( $pk_field:ident ),+ $(,)? ],
        fields = [ $( $field_model:expr ),+ $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
        relations = [ $( $relation:expr ),* $(,)? ],
        entity_value = none $(,)?
    ) => {
        $crate::__icydb_test_emit_entity! {
            ident = $entity,
            entity_name = $entity_name,
            tag = $entity_tag,
            store = $store_ty,
            canister = $canister_ty,
            runtime = full,
            singleton = false,
            key_type = $key_ty,
            primary_key = [ $( $pk_field ),+ ],
            fields = [ $( $field_model ),+ ],
            indexes = [ $( $index ),* ],
            relations = [ $( $relation ),* ],
            entity_value = { none },
        }
    };
    (
        ident = $entity:ident,
        entity_name = $entity_name:expr,
        tag = $entity_tag:expr,
        store = $store_ty:ty,
        canister = $canister_ty:ty,
        key_type = $key_ty:ty,
        primary_key = [ $( $pk_field:ident ),+ $(,)? ],
        fields = [ $( $field_model:expr ),+ $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
        relations = [ $( $relation:expr ),* $(,)? ],
        entity_value = id_field($id_field:ident) $(,)?
    ) => {
        $crate::__icydb_test_emit_entity! {
            ident = $entity,
            entity_name = $entity_name,
            tag = $entity_tag,
            store = $store_ty,
            canister = $canister_ty,
            runtime = full,
            singleton = false,
            key_type = $key_ty,
            primary_key = [ $( $pk_field ),+ ],
            fields = [ $( $field_model ),+ ],
            indexes = [ $( $index ),* ],
            relations = [ $( $relation ),* ],
            entity_value = { id_field($id_field) },
        }
    };
    (
        ident = $entity:ident,
        entity_name = $entity_name:expr,
        tag = $entity_tag:expr,
        store = $store_ty:ty,
        canister = $canister_ty:ty,
        key_type = $key_ty:ty,
        primary_key = [ $( $pk_field:ident ),+ $(,)? ],
        fields = [ $( $field_model:expr ),+ $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
        relations = [ $( $relation:expr ),* $(,)? ],
        entity_value = key($key_expr:expr) $(,)?
    ) => {
        $crate::__icydb_test_emit_entity! {
            ident = $entity,
            entity_name = $entity_name,
            tag = $entity_tag,
            store = $store_ty,
            canister = $canister_ty,
            runtime = full,
            singleton = false,
            key_type = $key_ty,
            primary_key = [ $( $pk_field ),+ ],
            fields = [ $( $field_model ),+ ],
            indexes = [ $( $index ),* ],
            relations = [ $( $relation ),* ],
            entity_value = { key($key_expr) },
        }
    };
    (
        ident = $entity:ident,
        entity_name = $entity_name:expr,
        tag = $entity_tag:expr,
        store = $store_ty:ty,
        canister = $canister_ty:ty,
        key_type = $key_ty:ty,
        primary_key = [ $pk_field:ident ],
        fields = [ $( $field_model:expr ),+ $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
        relations = [ $( $relation:expr ),* $(,)? ],
    ) => {
        $crate::__icydb_test_emit_entity! {
            ident = $entity,
            entity_name = $entity_name,
            tag = $entity_tag,
            store = $store_ty,
            canister = $canister_ty,
            runtime = full,
            singleton = false,
            key_type = $key_ty,
            primary_key = [ $pk_field ],
            fields = [ $( $field_model ),+ ],
            indexes = [ $( $index ),* ],
            relations = [ $( $relation ),* ],
            entity_value = { id_field($pk_field) },
        }
    };
    (
        ident = $entity:ident,
        entity_name = $entity_name:expr,
        tag = $entity_tag:expr,
        store = $store_ty:ty,
        canister = $canister_ty:ty,
        key_type = $key_ty:ty,
        primary_key = [ $pk_field:ident ],
        fields = [ $( $field_model:expr ),+ $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
    ) => {
        $crate::__icydb_test_parse_entity! {
            ident = $entity,
            entity_name = $entity_name,
            tag = $entity_tag,
            store = $store_ty,
            canister = $canister_ty,
            key_type = $key_ty,
            primary_key = [ $pk_field ],
            fields = [ $( $field_model ),+ ],
            indexes = [ $( $index ),* ],
            relations = [],
        }
    };
    (
        ident = $entity:ident,
        entity_name = $entity_name:expr,
        tag = $entity_tag:expr,
        store = $store_ty:ty,
        canister = $canister_ty:ty,
        singleton = true,
        key_type = $key_ty:ty,
        primary_key = [ $pk_field:ident ],
        fields = [ $( $field_model:expr ),+ $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
    ) => {
        $crate::__icydb_test_emit_entity! {
            ident = $entity,
            entity_name = $entity_name,
            tag = $entity_tag,
            store = $store_ty,
            canister = $canister_ty,
            runtime = full,
            singleton = true,
            key_type = $key_ty,
            primary_key = [ $pk_field ],
            fields = [ $( $field_model ),+ ],
            indexes = [ $( $index ),* ],
            relations = [],
            entity_value = { id_field($pk_field) },
        }
    };
    (
        ident = $entity:ident,
        entity_name = $entity_name:expr,
        tag = $entity_tag:expr,
        store = $store_ty:ty,
        canister = $canister_ty:ty,
        singleton = true,
        primary_key(fields = [ $pk_field:ident : $pk_ty:ty => $pk_kind:expr $(, $pk_meta_key:ident = $pk_meta_value:expr)* $(,)? ]),
        fields = [ $( $field_model:expr ),* $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
    ) => {
        $crate::__icydb_test_emit_entity! {
            ident = $entity,
            entity_name = $entity_name,
            tag = $entity_tag,
            store = $store_ty,
            canister = $canister_ty,
            runtime = full,
            singleton = true,
            key_type = $pk_ty,
            primary_key = [ $pk_field ],
            fields = [
                $crate::test_field! { $pk_field : $pk_ty => $pk_kind $(, $pk_meta_key = $pk_meta_value)* },
                $( $field_model, )*
            ],
            indexes = [ $( $index ),* ],
            relations = [],
            entity_value = { id_field($pk_field) },
        }
    };
    (
        ident = $entity:ident,
        entity_name = $entity_name:expr,
        tag = $entity_tag:expr,
        store = $store_ty:ty,
        canister = $canister_ty:ty,
        value = none,
        primary_key(fields = [ $pk_field:ident : $pk_ty:ty => $pk_kind:expr $(, $pk_meta_key:ident = $pk_meta_value:expr)* $(,)? ]),
        fields = [ $( $field_model:expr ),* $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
    ) => {
        $crate::__icydb_test_emit_entity! {
            ident = $entity,
            entity_name = $entity_name,
            tag = $entity_tag,
            store = $store_ty,
            canister = $canister_ty,
            runtime = full,
            singleton = false,
            key_type = $pk_ty,
            primary_key = [ $pk_field ],
            fields = [
                $crate::test_field! { $pk_field : $pk_ty => $pk_kind $(, $pk_meta_key = $pk_meta_value)* },
                $( $field_model, )*
            ],
            indexes = [ $( $index ),* ],
            relations = [],
            entity_value = { none },
        }
    };
    (
        ident = $entity:ident,
        entity_name = $entity_name:expr,
        tag = $entity_tag:expr,
        store = $store_ty:ty,
        canister = $canister_ty:ty,
        entity_value(key_type = $key_ty:ty, key = $key_expr:expr),
        primary_key(fields = [
            $pk_field_0:ident : $pk_ty_0:ty => $pk_kind_0:expr,
            $pk_field_1:ident : $pk_ty_1:ty => $pk_kind_1:expr $(,)?
        ]),
        fields = [ $( $field_model:expr ),* $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
    ) => {
        $crate::__icydb_test_parse_entity! {
            ident = $entity,
            entity_name = $entity_name,
            tag = $entity_tag,
            store = $store_ty,
            canister = $canister_ty,
            entity_value(key_type = $key_ty, key = $key_expr),
            primary_key(fields = [
                $pk_field_0: $pk_ty_0 => $pk_kind_0,
                $pk_field_1: $pk_ty_1 => $pk_kind_1,
            ]),
            fields = [ $( $field_model ),* ],
            indexes = [ $( $index ),* ],
            relations = [],
        }
    };
    (
        ident = $entity:ident,
        entity_name = $entity_name:expr,
        tag = $entity_tag:expr,
        store = $store_ty:ty,
        canister = $canister_ty:ty,
        entity_value(key_type = $key_ty:ty, key = $key_expr:expr),
        primary_key(fields = [
            $pk_field_0:ident : $pk_ty_0:ty => $pk_kind_0:expr,
            $pk_field_1:ident : $pk_ty_1:ty => $pk_kind_1:expr $(,)?
        ]),
        fields = [ $( $field_model:expr ),* $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
        relations = [ $( $relation:expr ),* $(,)? ],
    ) => {
        $crate::__icydb_test_emit_entity! {
            ident = $entity,
            entity_name = $entity_name,
            tag = $entity_tag,
            store = $store_ty,
            canister = $canister_ty,
            runtime = full,
            singleton = false,
            key_type = $key_ty,
            primary_key = [ $pk_field_0, $pk_field_1 ],
            fields = [
                $crate::test_field! { $pk_field_0 : $pk_ty_0 => $pk_kind_0 },
                $crate::test_field! { $pk_field_1 : $pk_ty_1 => $pk_kind_1 },
                $( $field_model, )*
            ],
            indexes = [ $( $index ),* ],
            relations = [ $( $relation ),* ],
            entity_value = { key($key_expr) },
        }
    };
    (
        ident = $entity:ident,
        entity_name = $entity_name:expr,
        tag = $entity_tag:expr,
        store = $store_ty:ty,
        canister = $canister_ty:ty,
        primary_key(fields = [ $pk_field:ident : $pk_ty:ty => $pk_kind:expr $(, $pk_meta_key:ident = $pk_meta_value:expr)* $(,)? ]),
        fields = [ $( $field_model:expr ),* $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
        relations = [ $( $relation:expr ),* $(,)? ],
    ) => {
        $crate::__icydb_test_emit_entity! {
            ident = $entity,
            entity_name = $entity_name,
            tag = $entity_tag,
            store = $store_ty,
            canister = $canister_ty,
            runtime = full,
            singleton = false,
            key_type = $pk_ty,
            primary_key = [ $pk_field ],
            fields = [
                $crate::test_field! { $pk_field : $pk_ty => $pk_kind $(, $pk_meta_key = $pk_meta_value)* },
                $( $field_model, )*
            ],
            indexes = [ $( $index ),* ],
            relations = [ $( $relation ),* ],
            entity_value = { id_field($pk_field) },
        }
    };
    (
        ident = $entity:ident,
        entity_name = $entity_name:expr,
        tag = $entity_tag:expr,
        store = $store_ty:ty,
        canister = $canister_ty:ty,
        entity_value(id_field = $id_field:ident),
        primary_key(fields = [ $pk_field:ident : $pk_ty:ty => $pk_kind:expr $(, $pk_meta_key:ident = $pk_meta_value:expr)* $(,)? ]),
        fields = [ $( $field_model:expr ),* $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
    ) => {
        $crate::__icydb_test_emit_entity! {
            ident = $entity,
            entity_name = $entity_name,
            tag = $entity_tag,
            store = $store_ty,
            canister = $canister_ty,
            runtime = full,
            singleton = false,
            key_type = $pk_ty,
            primary_key = [ $pk_field ],
            fields = [
                $crate::test_field! { $pk_field : $pk_ty => $pk_kind $(, $pk_meta_key = $pk_meta_value)* },
                $( $field_model, )*
            ],
            indexes = [ $( $index ),* ],
            relations = [],
            entity_value = { id_field($id_field) },
        }
    };
    (
        ident = $entity:ident,
        entity_name = $entity_name:expr,
        tag = $entity_tag:expr,
        store = $store_ty:ty,
        canister = $canister_ty:ty,
        primary_key(fields = [ $pk_field:ident : $pk_ty:ty => $pk_kind:expr $(, $pk_meta_key:ident = $pk_meta_value:expr)* $(,)? ]),
        fields = [ $( $field_model:expr ),* $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
    ) => {
        $crate::__icydb_test_parse_entity! {
            ident = $entity,
            entity_name = $entity_name,
            tag = $entity_tag,
            store = $store_ty,
            canister = $canister_ty,
            primary_key(fields = [ $pk_field : $pk_ty => $pk_kind $(, $pk_meta_key = $pk_meta_value)* ]),
            fields = [ $( $field_model ),* ],
            indexes = [ $( $index ),* ],
            relations = [],
        }
    };
    (
        ident = $entity:ident,
        entity_name = $entity_name:expr,
        runtime = schema_only,
        key_type = $key_ty:ty,
        primary_key = [ $( $pk_field:ident ),+ $(,)? ],
        fields = [ $( $field_model:expr ),+ $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
        relations = [ $( $relation:expr ),* $(,)? ],
    ) => {
        $crate::__icydb_test_emit_entity! {
            ident = $entity,
            entity_name = $entity_name,
            runtime = schema_only,
            key_type = $key_ty,
            primary_key = [ $( $pk_field ),+ ],
            fields = [ $( $field_model ),+ ],
            indexes = [ $( $index ),* ],
            relations = [ $( $relation ),* ],
        }
    };
    (
        ident = $entity:ident,
        entity_name = $entity_name:expr,
        runtime = schema_only,
        key_type = $key_ty:ty,
        primary_key = [ $( $pk_field:ident ),+ $(,)? ],
        fields = [ $( $field_model:expr ),+ $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
    ) => {
        $crate::__icydb_test_emit_entity! {
            ident = $entity,
            entity_name = $entity_name,
            runtime = schema_only,
            key_type = $key_ty,
            primary_key = [ $( $pk_field ),+ ],
            fields = [ $( $field_model ),+ ],
            indexes = [ $( $index ),* ],
            relations = [],
        }
    };
    (
        ident = $entity:ident,
        entity_name = $entity_name:expr,
        runtime = schema_only,
        primary_key(fields = [ $pk_field:ident : $pk_ty:ty => $pk_kind:expr $(, $pk_meta_key:ident = $pk_meta_value:expr)* $(,)? ]),
        fields = [ $( $field_model:expr ),* $(,)? ],
        indexes = [ $( $index:expr ),* $(,)? ],
    ) => {
        $crate::__icydb_test_emit_entity! {
            ident = $entity,
            entity_name = $entity_name,
            runtime = schema_only,
            key_type = $pk_ty,
            primary_key = [ $pk_field ],
            fields = [
                $crate::test_field! { $pk_field : $pk_ty => $pk_kind $(, $pk_meta_key = $pk_meta_value)* },
                $( $field_model, )*
            ],
            indexes = [ $( $index ),* ],
            relations = [],
        }
    };
}

///
/// test_entity
///
/// Test-only helper to define a test entity type and derive its schema model.
///
#[macro_export]
macro_rules! test_entity {
    ( $($input:tt)* ) => {
        $crate::__icydb_test_parse_entity! { $($input)* }
    };
}
