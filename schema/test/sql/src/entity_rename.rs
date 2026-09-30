//! Current-format source/successor declarations for the populated rename rehearsal.
//! The store and canister owners remain the ordinary SQL fixture.

use crate::sql::SqlTestStore;
use icydb_model::prelude::*;

// Creation qualification reuses this populated source with either a metadata
// rename or one physical field addition. Unchanged companions stay at version 1.
macro_rules! define_rename_entities {
    ($item:ident, $target:literal, $version:literal, $holder_version:literal, [$($additional_fields:tt)*]) => {
        /// Indexed item with a nullable self-reference for rename qualification.
        #[entity(
                    store = "SqlTestStore",
                    version = $version,
                    pk(field = "id"),
                    index(field = "key", unique),
                    fields(
                        field(name = "id", value(item(prim = "Nat64"))),
                        field(name = "key", value(item(prim = "Nat64"))),
                        field(name = "label", value(item(prim = "Nat64"))),
                        field(name = "parent_id", value(opt, item(rel = $target, prim = "Nat64"))),
                        $($additional_fields)*
                    )
                )]
        pub struct $item {}

        /// Unrenamed inbound owner; its relation dependency advances explicitly.
        #[entity(
                    store = "SqlTestStore",
                    version = $holder_version,
                    pk(field = "id"),
                    fields(
                        field(name = "id", value(item(prim = "Nat64"))),
                        field(name = "item_id", value(item(rel = $target, prim = "Nat64")))
                    )
                )]
        pub struct Holder {}
    };
}

#[cfg(not(any(
    feature = "entity-rename-successor",
    feature = "entity-creation-physical"
)))]
define_rename_entities!(Item, "Item", 1, 1, []);
#[cfg(feature = "entity-rename-successor")]
define_rename_entities!(CatalogItem, "CatalogItem", 2, 2, []);
#[cfg(all(
    feature = "entity-creation-physical",
    not(feature = "entity-rename-successor")
))]
define_rename_entities!(
    Item,
    "Item",
    2,
    1,
    [field(name = "coins", value(item(prim = "Nat64")))]
);
