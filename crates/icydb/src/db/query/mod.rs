pub mod expr;

pub use expr::{FilterExpr, OrderDirection, SortExpr};
pub use icydb_core::db::{FieldRef, MissingRowPolicy, Predicate, Query};

/// Field-reference helpers exposed by the facade query API.
pub mod builder {
    pub use icydb_core::db::FieldRef;
}

/// Predicate type exposed at the facade query boundary.
pub mod predicate {
    pub use icydb_core::db::Predicate;
}
