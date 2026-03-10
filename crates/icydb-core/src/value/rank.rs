//! Module: value::rank
//! Responsibility: module-local ownership and contracts for value::rank.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::value::{Value, tag};

///
/// Canonical Value Rank
///
/// Stable rank used for cross-variant ordering.
///
/// IMPORTANT:
/// Rank order is part of deterministic query behavior and must remain fixed
/// after 0.7 unless an intentional breaking migration is performed.
///
#[must_use]
pub(super) const fn canonical_rank(value: &Value) -> u8 {
    // Tags are 1-based for wire/hash stability; rank is 0-based.
    tag::canonical_tag(value).to_u8() - 1
}
