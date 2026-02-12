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
pub const fn canonical_rank(value: &Value) -> u8 {
    // Tags are 1-based for wire/hash stability; rank is 0-based.
    tag::canonical_tag(value).to_u8() - 1
}
