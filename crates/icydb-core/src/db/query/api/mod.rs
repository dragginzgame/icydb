//! Query API helpers that live at the query/session boundary.
//! Boundary rule: cardinality semantics belong here, not on transport DTOs
//! from `db::response`.

mod result_ext;

pub use result_ext::ResponseCardinalityExt;
