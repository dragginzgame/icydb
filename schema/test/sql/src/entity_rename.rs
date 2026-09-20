//! Current-format source/successor declarations for the populated rename rehearsal.
//! The store and canister owners remain the ordinary SQL fixture.

use crate::sql::SqlTestStore;
use icydb_model::prelude::*;

// The two build variants differ only in source versions and entity/target names.
macro_rules! define_rename_entities {
    ($item:ident, $target:literal, $version:literal) => {
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
                        field(name = "parent_id", value(opt, item(rel = $target, prim = "Nat64")))
                    )
                )]
        pub struct $item {}

        /// Unrenamed inbound owner; its relation dependency advances explicitly.
        #[entity(
                    store = "SqlTestStore",
                    version = $version,
                    pk(field = "id"),
                    fields(
                        field(name = "id", value(item(prim = "Nat64"))),
                        field(name = "item_id", value(item(rel = $target, prim = "Nat64")))
                    )
                )]
        pub struct Holder {}
    };
}

#[cfg(not(feature = "entity-rename-successor"))]
define_rename_entities!(Item, "Item", 1);
#[cfg(feature = "entity-rename-successor")]
define_rename_entities!(CatalogItem, "CatalogItem", 2);
