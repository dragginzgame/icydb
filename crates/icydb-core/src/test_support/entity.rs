use crate::model::{entity::EntityModel, field::EntityFieldModel, index::IndexModel};

///
/// EntitySpec
///
/// Compile-time scaffolding for building an EntityModel in tests.
/// Not used at runtime.
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

/// macro helper
#[doc(hidden)]
pub const fn __test_entity_pk_index(fields: &[&str], pk: &str) -> usize {
    let pk_bytes = pk.as_bytes();

    let mut i = 0;
    while i < fields.len() {
        let field_bytes = fields[i].as_bytes();

        if field_bytes.len() == pk_bytes.len() {
            let mut j = 0;
            let mut eq = true;
            while j < pk_bytes.len() {
                if field_bytes[j] != pk_bytes[j] {
                    eq = false;
                    break;
                }
                j += 1;
            }
            if eq {
                return i;
            }
        }

        i += 1;
    }

    panic!("test_entity: primary key field not found in fields list");
}

///
/// Macro
///

#[macro_export]
macro_rules! test_entity {
    // =============================================================
    // Entry: struct + entity
    // =============================================================
    (
        $(#[$meta:meta])*
        $vis:vis struct $entity:ident {
            $($struct_field:ident : $struct_ty:ty),* $(,)?
        }

        path: $path:literal,
        pk: $pk:ident,

        fields { $($field:ident : $kind:tt),* $(,)? }

        $(indexes { $($indexes:tt)* })?
        $(impls { $($impls:tt),* $(,)? })?
    ) => {
        $(#[$meta])*
        #[derive(::icydb_derive::FieldValues)]
        $vis struct $entity {
            $($struct_field : $struct_ty),*
        }

        impl $entity {
            const __TEST_FIELD_NAMES: [&'static str;
                $crate::test_entity!(@count $($field),*)
            ] = [
                $( stringify!($field) ),*
            ];

            const __TEST_FIELDS: [$crate::model::field::EntityFieldModel;
                $crate::test_entity!(@count $($field),*)
            ] = [
                $(
                    $crate::test_support::field(
                        stringify!($field),
                        $crate::test_entity!(@kind $kind),
                    )
                ),*
            ];

            const __TEST_PRIMARY_KEY_INDEX: usize = {
                $crate::test_support::entity::__test_entity_pk_index(
                    &Self::__TEST_FIELD_NAMES,
                    stringify!($pk),
                )
            };

            const __TEST_STRUCT_FIELD_NAMES: [&'static str;
                $crate::test_entity!(@count $($struct_field),*)
            ] = [
                $( stringify!($struct_field) ),*
            ];

            const __TEST_STRUCT_PK_INDEX: usize = {
                $crate::test_support::entity::__test_entity_pk_index(
                    &Self::__TEST_STRUCT_FIELD_NAMES,
                    stringify!($pk),
                )
            };

            $crate::test_entity!(@indexes $path, $($($indexes)*)?);

            const __TEST_SPEC: $crate::test_support::entity::EntitySpec =
                $crate::test_support::entity::EntitySpec {
                    path: $path,
                    entity_name: stringify!($entity),
                    primary_key: stringify!($pk),
                    primary_key_index: Self::__TEST_PRIMARY_KEY_INDEX,
                    fields: &Self::__TEST_FIELDS,
                    field_names: &Self::__TEST_FIELD_NAMES,
                    indexes: &Self::__TEST_INDEXES,
                };

            const __TEST_MODEL: $crate::model::entity::EntityModel =
                $crate::test_support::entity::entity_model(&Self::__TEST_SPEC);
        }

        impl $crate::traits::Path for $entity {
            const PATH: &'static str = $path;
        }

        const _: () = {
            const _: usize = $entity::__TEST_STRUCT_PK_INDEX;

            /// Maps struct field idents to their Rust types for PK extraction.
            #[allow(non_camel_case_types)]
            trait __TestEntityStructFields {
                $( type $struct_field; )*
            }

            #[allow(non_camel_case_types)]
            impl __TestEntityStructFields for $entity {
                $( type $struct_field = $struct_ty; )*
            }

            impl $crate::traits::EntityKind for $entity {
                type Id = <Self as __TestEntityStructFields>::$pk;
                type DataStore = $crate::test_support::TestDataStore;
                type Canister = $crate::test_support::TestCanister;

                const ENTITY_NAME: &'static str = stringify!($entity);
                const PRIMARY_KEY: &'static str = stringify!($pk);
                const FIELDS: &'static [&'static str] = &Self::__TEST_FIELD_NAMES;
                const INDEXES: &'static [&'static $crate::model::index::IndexModel] =
                    &Self::__TEST_INDEXES;
                const MODEL: &'static $crate::model::entity::EntityModel =
                    &Self::__TEST_MODEL;
            }

            impl $crate::traits::EntityValue for $entity {
                fn id(&self) -> Self::Id {
                    self.$pk
                }

                fn set_id(&mut self, id: Self::Id) {
                    self.$pk = id;
                }
            }
        };

        $(
            $crate::test_entity!(@emit_impls $entity, $($impls),*);
        )?
    };

    // =============================================================
    // Index handling
    // =============================================================
    (@indexes $path:literal,) => {
        const __TEST_INDEXES: [&'static $crate::model::index::IndexModel; 0] = [];
    };

    (@indexes $path:literal,
        $(index $name:ident ( $($field:ident),+ ) unique;)+
    ) => {
        $(
            #[allow(non_upper_case_globals)]
            const $name: $crate::model::index::IndexModel =
                $crate::model::index::IndexModel::new(
                    concat!($path, "::", stringify!($name)),
                    $crate::test_support::TEST_INDEX_STORE_PATH,
                    &[ $( stringify!($field) ),+ ],
                    true, // or false
                );
        )+

        const __TEST_INDEXES:
            [&'static $crate::model::index::IndexModel;
             $crate::test_entity!(@count $($name),+)
            ] = [ $( &Self::$name ),+ ];
    };

    (@indexes $path:literal,
        $(index $name:ident ( $($field:ident),+ );)+
    ) => {
        $(
            #[allow(non_upper_case_globals)]
            const $name: $crate::model::index::IndexModel =
                $crate::model::index::IndexModel::new(
                    concat!($path, "::", stringify!($name)),
                    $crate::test_support::TEST_INDEX_STORE_PATH,
                    &[ $( stringify!($field) ),+ ],
                    true, // or false
                );
        )+

        const __TEST_INDEXES:
            [&'static $crate::model::index::IndexModel;
             $crate::test_entity!(@count $($name),+)
            ] = [ $( &Self::$name ),+ ];
    };

    // =============================================================
    // impls { ... }
    // =============================================================
    (@emit_impl $entity:ident, ViewClone) => {
        impl $crate::traits::View for $entity {
            type ViewType = Self;
            fn to_view(&self) -> Self { self.clone() }
            fn from_view(view: Self) -> Self { view }
        }
    };

    (@emit_impl $entity:ident, SanitizeAuto) =>
        { impl $crate::traits::SanitizeAuto for $entity {} };

    (@emit_impl $entity:ident, SanitizeCustom) =>
        { impl $crate::traits::SanitizeCustom for $entity {} };

    (@emit_impl $entity:ident, ValidateAuto) =>
        { impl $crate::traits::ValidateAuto for $entity {} };

    (@emit_impl $entity:ident, ValidateCustom) =>
        { impl $crate::traits::ValidateCustom for $entity {} };

    (@emit_impl $entity:ident, Visitable) =>
        { impl $crate::traits::Visitable for $entity {} };

    // =============================================================
    // Field kind mapping
    // =============================================================
    (@kind Ulid) => { $crate::model::field::EntityFieldKind::Ulid };
    (@kind Unit) => { $crate::model::field::EntityFieldKind::Unit };
    (@kind Timestamp) => { $crate::model::field::EntityFieldKind::Timestamp };
    (@kind Text) => { $crate::model::field::EntityFieldKind::Text };
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
        $crate::model::field::EntityFieldKind::List(
            &$crate::test_entity!(@kind $inner)
        )
    };

    // Entry for zero impls
    (@emit_impls $entity:ident) => {};
    (@emit_impls $entity:ident,) => {};

    // Entry for one or more impls
    (@emit_impls $entity:ident, $head:tt $(, $tail:tt)*) => {
        $crate::test_entity!(@emit_impl $entity, $head);
        $crate::test_entity!(@emit_impls $entity $(, $tail)*);
    };

    // =============================================================
    // Utilities
    // =============================================================
    (@count) => { 0usize };
    (@count $head:ident $(, $tail:ident)*) => {
        1usize + $crate::test_entity!(@count $($tail),*)
    };
}
