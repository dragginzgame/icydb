//! Added declarations share the populated rename fixture's accepted store.

use crate::sql::SqlTestStore;
use icydb_model::prelude::*;

/// New enum authority published with Quest.

#[enum_(variant(name = "Active"), variant(name = "Done"))]
pub struct QuestState {}

/// New record authority published with Quest.

#[record(fields(
    field(name = "label", value(item(prim = "Text", max_len = 64))),
    field(name = "reward", value(item(prim = "Nat64")))
))]
pub struct QuestDetails {}

macro_rules! define_quest {
    ($target:literal) => {
        /// New indexed entity with a restrictive link to populated Item data.

        #[entity(
                            store = "SqlTestStore",
                            version = 1,
                            pk(field = "id"),
                            index(field = "code", unique),
                            fields(
                                field(name = "id", value(item(prim = "Nat64"))),
                                field(name = "item_id", value(item(rel = $target, prim = "Nat64"))),
                                field(name = "code", value(item(prim = "Nat64"))),
                                field(name = "state", value(item(is = "QuestState"))),
                                field(name = "details", value(item(is = "QuestDetails")))
                            )
                        )]
        pub struct Quest {}
    };
}

#[cfg(not(feature = "entity-rename-successor"))]
define_quest!("crate::entity_rename::Item");
#[cfg(feature = "entity-rename-successor")]
define_quest!("crate::entity_rename::CatalogItem");
