//! Query Builder v2 modules.
//!
//! Predicate semantics are defined in `docs/QUERY_BUILDER_V2.md` and are the
//! canonical contract for evaluation, coercion, and normalization.
pub mod builder;
pub mod plan;
pub mod predicate;
