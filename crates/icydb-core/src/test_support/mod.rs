#[macro_export]
macro_rules! test_entity {
    // ─────────────────────────────────────────────────────────────
    // Entry: define struct + entity
    // ─────────────────────────────────────────────────────────────
    (
        $(#[$meta:meta])*
        struct $entity:ident {
            $($struct_field:ident : $struct_ty:ty),* $(,)?
        }

        path: $path:literal,
        pk: $pk:ident,

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
                pk: $pk,
                fields { $($field : $kind),* }
                $(indexes { $($indexes)* })?
            }
        }
    };

    // ─────────────────────────────────────────────────────────────
    // Entry: entity only
    // ─────────────────────────────────────────────────────────────
    (
        entity $entity:ident {
            path: $path:literal,
            pk: $pk:ident,

            fields { $($field:ident : $kind:tt),* $(,)? }

            $(indexes { $($indexes:tt)* })?
        }
    ) => {
        impl $entity {
            const __TEST_FIELD_NAMES: [&'static str; $crate::test_entity!(@count $($field),*)] = [
                $(stringify!($field)),*
            ];

            const __TEST_FIELDS: [$crate::model::field::EntityFieldModel;
                $crate::test_entity!(@count $($field),*)
            ] = [
                $(
                    $crate::test_support::field(
                        stringify!($field),
                        $crate::test_entity!(@kind $kind)
                    )
                ),*
            ];

            const __TEST_PRIMARY_KEY_INDEX: usize =
                $crate::test_entity!(@pk_index $pk, 0usize, $($field : $kind),*);

            $crate::test_entity!(@indexes $path, $($($indexes)*)?);

            const __TEST_SPEC: $crate::test_support::EntitySpec =
                $crate::test_support::EntitySpec {
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
            type Id = $crate::test_entity!(
                @id_from_kind
                $crate::test_entity!(@pk_kind $pk, $($field : $kind),*)
            );

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

    // ─────────────────────────────────────────────────────────────
    // Index handling
    // ─────────────────────────────────────────────────────────────
    (@indexes $path:literal, ) => {
        const __TEST_INDEXES: [&'static $crate::model::index::IndexModel; 0] = [];
    };

    (@indexes $path:literal,
        $(index $name:ident ( $($field:ident),+ ) $(unique)?;)+
    ) => {
        $(
            const $name: $crate::model::index::IndexModel =
                $crate::model::index::IndexModel::new(
                    concat!($path, "::", stringify!($name)),
                    $crate::test_support::TEST_INDEX_STORE_PATH,
                    &[$(stringify!($field)),+],
                    $crate::test_entity!(@index_unique $($unique)?),
                );
        )+

        const __TEST_INDEXES:
            [&'static $crate::model::index::IndexModel;
             $crate::test_entity!(@count $($name),+)
            ] = [
                $(&$name),+
            ];
    };

    (@index_unique unique) => { true };
    (@index_unique) => { false };

    // ─────────────────────────────────────────────────────────────
    // Primary key helpers
    // ─────────────────────────────────────────────────────────────
    (@pk_index $pk:ident, $idx:expr, $field:ident : $kind:tt $(, $rest:tt)*) => {
        $crate::test_entity!(@pk_index_match $pk, $field, $idx, $kind $(, $rest)*)
    };
    (@pk_index $pk:ident, $idx:expr, $field:ident : $kind:tt $(, $rest:tt)*) => {
        $crate::test_entity!(@pk_index $pk, $idx + 1usize, $($rest),*)
    };
    (@pk_index $pk:ident, $idx:expr,) => {
        compile_error!(concat!(
            "test_entity primary key not found in fields: ",
            stringify!($pk)
        ))
    };

    (@pk_kind $pk:ident, $field:ident : $kind:tt $(, $rest:tt)*) => {
        $crate::test_entity!(@pk_kind_match $pk, $field, $kind $(, $rest)*)
    };
    (@pk_kind $pk:ident, $field:ident : $kind:tt $(, $rest:tt)*) => {
        $crate::test_entity!(@pk_kind $pk, $($rest),*)
    };
    (@pk_kind $pk:ident,) => {
        compile_error!(concat!(
            "test_entity primary key not found in fields: ",
            stringify!($pk)
        ))
    };

    (@id_from_kind Ulid) => { $crate::types::Ulid };
    (@id_from_kind Unit) => { $crate::types::Unit };
    (@id_from_kind Timestamp) => { $crate::types::Timestamp };
    (@id_from_kind $other:tt) => {
        compile_error!(concat!(
            "unsupported primary key field kind: ",
            stringify!($other)
        ))
    };

    // ─────────────────────────────────────────────────────────────
    // Field kind mapping
    // ─────────────────────────────────────────────────────────────
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
        $crate::model::field::EntityFieldKind::List(
            &$crate::test_entity!(@kind $inner)
        )
    };

    (@kind $other:tt) => {
        compile_error!(concat!(
            "unsupported test field kind: ",
            stringify!($other)
        ))
    };

    // ─────────────────────────────────────────────────────────────
    // Utilities
    // ─────────────────────────────────────────────────────────────
    (@count) => { 0usize };
    (@count $head:ident $(, $tail:ident)*) => {
        1usize + $crate::test_entity!(@count $($tail),*)
    };
}
