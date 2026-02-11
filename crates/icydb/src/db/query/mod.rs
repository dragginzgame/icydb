pub mod expr;

pub use expr::{FilterExpr, OrderDirection, SortExpr};
pub use icydb_core::db::query::{Query, ReadConsistency};

/// Field-reference helpers exposed by the facade query API.
pub mod builder {
    pub use icydb_core::db::query::builder::FieldRef;
}

/// Predicate type exposed at the facade query boundary.
pub mod predicate {
    pub use icydb_core::db::query::predicate::Predicate;
}

pub use builder::FieldRef;
pub use predicate::Predicate;
