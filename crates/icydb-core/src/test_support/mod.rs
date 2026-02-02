use crate::{
    model::{
        entity::EntityModel,
        field::{EntityFieldKind, EntityFieldModel},
        index::IndexModel,
    },
    traits::{CanisterKind, DataStoreKind, EntityKey, EntityKind, FieldValues, Path, TypeKind},
};

/// Default test canister path for core-only test entities.
pub const TEST_CANISTER_PATH: &str = "icydb_core::test_support::TestCanister";

/// Default test data store path for core-only test entities.
pub const TEST_DATA_STORE_PATH: &str = "icydb_core::test_support::TestDataStore";

/// Default test index store path for core-only test entities.
pub const TEST_INDEX_STORE_PATH: &str = "icydb_core::test_support::TestIndexStore";

///
/// TestCanister
///
/// Shared test-only canister marker for core tests.
/// Use this for EntityKind implementations in test support.
///

#[derive(Clone, Copy)]
pub struct TestCanister;

impl Path for TestCanister {
    const PATH: &'static str = TEST_CANISTER_PATH;
}

impl CanisterKind for TestCanister {}

///
/// TestDataStore
///
/// Shared test-only data store marker for core tests.
/// Use this for EntityKind implementations in test support.
///

pub struct TestDataStore;

impl Path for TestDataStore {
    const PATH: &'static str = TEST_DATA_STORE_PATH;
}

impl DataStoreKind for TestDataStore {
    type Canister = TestCanister;
}

///
/// EntitySpec
///
/// Runtime-only entity description used by test-only helpers.
/// This keeps test models centralized and consistent with EntityKind metadata.
///

pub struct EntitySpec {
    pub path: &'static str,
    pub entity_name: &'static str,
    pub primary_key: &'static str,
    pub primary_key_index: usize,
    pub fields: &'static [EntityFieldModel],
    pub field_names: &'static [&'static str],
    pub indexes: &'static [&'static IndexModel],
}

/// Build a runtime field model for test entities.
#[must_use]
pub const fn field(name: &'static str, kind: EntityFieldKind) -> EntityFieldModel {
    EntityFieldModel { name, kind }
}

/// Build an EntityModel for a test entity spec.
#[must_use]
pub const fn entity_model(spec: &EntitySpec) -> EntityModel {
    EntityModel {
        path: spec.path,
        entity_name: spec.entity_name,
        primary_key: &spec.fields[spec.primary_key_index],
        fields: spec.fields,
        indexes: spec.indexes,
    }
}

/// Test-only entity metadata for central EntityKind implementation.
pub trait TestEntitySpec: FieldValues + TypeKind {
    type Id: EntityKey;
    type DataStore: DataStoreKind;
    type Canister: CanisterKind;

    const SPEC: &'static EntitySpec;
    const MODEL: &'static EntityModel;

    fn id(&self) -> Self::Id;
    fn set_id(&mut self, id: Self::Id);
}

impl<T> EntityKind for T
where
    T: TestEntitySpec,
{
    type Id = T::Id;
    type DataStore = T::DataStore;
    type Canister = T::Canister;

    const ENTITY_NAME: &'static str = T::SPEC.entity_name;
    const PRIMARY_KEY: &'static str = T::SPEC.primary_key;
    const FIELDS: &'static [&'static str] = T::SPEC.field_names;
    const INDEXES: &'static [&'static IndexModel] = T::SPEC.indexes;
    const MODEL: &'static EntityModel = T::MODEL;

    fn id(&self) -> Self::Id {
        <T as TestEntitySpec>::id(self)
    }

    fn set_id(&mut self, id: Self::Id) {
        <T as TestEntitySpec>::set_id(self, id);
    }
}

#[macro_export]
macro_rules! test_entity {
    (
        $(#[$meta:meta])*
        struct $entity:ident {
            $($struct_field:ident : $struct_ty:ty),* $(,)?
        }

        path: $path:literal,
        pk: $pk:ident : $pk_kind:tt,

        fields { $($field:ident : $kind:tt),* $(,)? }

        $(indexes { $($indexes:tt)* })?
    ) => {
        $(#[$meta])*
        struct $entity {
            $($struct_field : $struct_ty),*
        }

        $crate::test_entity! {
            entity $entity {
                path: $path,
                pk: $pk : $pk_kind,
                fields { $($field : $kind),* }
                $(indexes { $($indexes)* })?
            }
        }
    };

    (
        entity $entity:ident {
            path: $path:literal,
            pk: $pk:ident : $pk_kind:tt,

            fields { $($field:ident : $kind:tt),* $(,)? }

            $(indexes { $($indexes:tt)* })?
        }
    ) => {
        impl $entity {
            const __TEST_FIELD_NAMES: [&'static str; $crate::test_entity!(@count $($field),*)] = [
                $(stringify!($field)),*
            ];
            const __TEST_FIELDS: [$crate::model::field::EntityFieldModel; $crate::test_entity!(@count $($field),*)] = [
                $($crate::test_support::field(stringify!($field), $crate::test_entity!(@kind $kind))),*
            ];
            const __TEST_PRIMARY_KEY_INDEX: usize =
                $crate::test_entity!(@pk_index $pk, 0usize, $($field : $kind),*);

            $crate::test_entity!(@indexes $path, $($($indexes)*)?);

            const __TEST_SPEC: $crate::test_support::EntitySpec = $crate::test_support::EntitySpec {
                path: $path,
                entity_name: stringify!($entity),
                primary_key: stringify!($pk),
                primary_key_index: Self::__TEST_PRIMARY_KEY_INDEX,
                fields: &Self::__TEST_FIELDS,
                field_names: &Self::__TEST_FIELD_NAMES,
                indexes: &Self::__TEST_INDEXES,
            };
            const __TEST_MODEL: $crate::model::entity::EntityModel =
                $crate::test_support::entity_model(&Self::__TEST_SPEC);
        }

        impl $crate::traits::Path for $entity {
            const PATH: &'static str = $path;
        }

        impl $crate::test_support::TestEntitySpec for $entity {
            type Id = $crate::test_entity!(@id_type $pk_kind);
            type DataStore = $crate::test_support::TestDataStore;
            type Canister = $crate::test_support::TestCanister;

            const SPEC: &'static $crate::test_support::EntitySpec = &Self::__TEST_SPEC;
            const MODEL: &'static $crate::model::entity::EntityModel = &Self::__TEST_MODEL;

            fn id(&self) -> Self::Id {
                self.$pk
            }

            fn set_id(&mut self, id: Self::Id) {
                self.$pk = id;
            }
        }
    };

    (@indexes $path:literal, ) => {
        const __TEST_INDEXES: [&'static $crate::model::index::IndexModel; 0] = [];
    };
    (@indexes $path:literal, $(index $name:ident ( $($field:ident),+ ) $(unique)?;)+) => {
        $(
            const $name: $crate::model::index::IndexModel = $crate::model::index::IndexModel::new(
                concat!($path, "::", stringify!($name)),
                $crate::test_support::TEST_INDEX_STORE_PATH,
                &[$(stringify!($field)),+],
                $crate::test_entity!(@index_unique $($unique)?),
            );
        )+

        const __TEST_INDEXES: [&'static $crate::model::index::IndexModel; $crate::test_entity!(@count $($name),+)] = [
            $(&$name),+
        ];
    };
    (@index_unique unique) => { true };
    (@index_unique) => { false };

    (@pk_index $pk:ident, $idx:expr, $pk:ident : $kind:tt $(, $rest:tt)*) => { $idx };
    (@pk_index $pk:ident, $idx:expr, $field:ident : $kind:tt $(, $rest:tt)*) => {
        $crate::test_entity!(@pk_index $pk, $idx + 1usize, $($rest),*)
    };
    (@pk_index $pk:ident, $idx:expr,) => {
        compile_error!(concat!("test_entity primary key not found in fields: ", stringify!($pk)))
    };

    (@id_type Ulid) => { $crate::types::Ulid };
    (@id_type Unit) => { $crate::types::Unit };
    (@id_type Timestamp) => { $crate::types::Timestamp };
    (@id_type $other:tt) => {
        compile_error!(concat!("unsupported test entity id type: ", stringify!($other)))
    };

    (@kind Ulid) => { $crate::model::field::EntityFieldKind::Ulid };
    (@kind Unit) => { $crate::model::field::EntityFieldKind::Unit };
    (@kind Timestamp) => { $crate::model::field::EntityFieldKind::Timestamp };
    (@kind Text) => { $crate::model::field::EntityFieldKind::Text };
    (@kind String) => { $crate::model::field::EntityFieldKind::Text };
    (@kind Int) => { $crate::model::field::EntityFieldKind::Int };
    (@kind Enum) => { $crate::model::field::EntityFieldKind::Enum };
    (@kind Unsupported) => { $crate::model::field::EntityFieldKind::Unsupported };
    (@kind Ref<$target:ty>) => {
        $crate::model::field::EntityFieldKind::Ref {
            target_path: <$target as $crate::traits::Path>::PATH,
            key_kind: &<$target as $crate::traits::EntityKind>::MODEL.primary_key.kind,
        }
    };
    (@kind List<$inner:tt>) => {
        $crate::model::field::EntityFieldKind::List(&$crate::test_entity!(@kind $inner))
    };
    (@kind $other:tt) => {
        compile_error!(concat!("unsupported test field kind: ", stringify!($other)))
    };

    (@count) => { 0usize };
    (@count $head:ident $(, $tail:ident)*) => { 1usize + $crate::test_entity!(@count $($tail),*) };
}
