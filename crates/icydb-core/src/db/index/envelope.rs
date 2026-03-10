//! Module: index::envelope
//! Responsibility: canonical bound-envelope containment helpers for index-domain operations.
//! Does not own: cursor continuation advancement/resume semantics.
//! Boundary: index-owned bound containment primitives consumed by index/runtime layers.

use std::ops::Bound;

/// key_within_envelope
///
/// Validate that one key is contained by one canonical bound envelope.
/// This centralizes inclusive/exclusive bound semantics under index authority.
#[must_use]
pub(in crate::db) fn key_within_envelope<K: Ord + Clone>(
    key: &K,
    lower: &Bound<K>,
    upper: &Bound<K>,
) -> bool {
    KeyEnvelope::new(lower.clone(), upper.clone()).contains(key)
}

///
/// KeyEnvelope
///
/// Canonical raw-key envelope with inclusive/exclusive bound semantics.
/// This type models containment only; cursor continuation advancement semantics
/// are intentionally owned by `db::cursor`.
///

pub(in crate::db) struct KeyEnvelope<K> {
    lower: Bound<K>,
    upper: Bound<K>,
}

impl<K> KeyEnvelope<K>
where
    K: Ord,
{
    pub(in crate::db) const fn new(lower: Bound<K>, upper: Bound<K>) -> Self {
        Self { lower, upper }
    }

    pub(in crate::db) fn contains(&self, key: &K) -> bool {
        // Envelope containment is purely bound-based and direction-agnostic.
        let lower_ok = match &self.lower {
            Bound::Unbounded => true,
            Bound::Included(boundary) => key >= boundary,
            Bound::Excluded(boundary) => key > boundary,
        };
        let upper_ok = match &self.upper {
            Bound::Unbounded => true,
            Bound::Included(boundary) => key <= boundary,
            Bound::Excluded(boundary) => key < boundary,
        };

        lower_ok && upper_ok
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{KeyEnvelope, key_within_envelope};
    use std::ops::Bound;

    #[test]
    fn key_envelope_contains_respects_inclusive_and_exclusive_bounds() {
        let envelope = KeyEnvelope::new(Bound::Included(10_u8), Bound::Excluded(20_u8));

        assert!(envelope.contains(&10));
        assert!(envelope.contains(&19));
        assert!(!envelope.contains(&9));
        assert!(!envelope.contains(&20));
    }

    #[test]
    fn key_envelope_contains_handles_unbounded_edges() {
        let lower_unbounded = KeyEnvelope::new(Bound::Unbounded::<u8>, Bound::Included(3_u8));
        assert!(lower_unbounded.contains(&0));
        assert!(lower_unbounded.contains(&3));
        assert!(!lower_unbounded.contains(&4));

        let upper_unbounded = KeyEnvelope::new(Bound::Excluded(5_u8), Bound::Unbounded::<u8>);
        assert!(!upper_unbounded.contains(&5));
        assert!(upper_unbounded.contains(&6));
    }

    #[test]
    fn key_within_envelope_matches_key_envelope_contains() {
        let lower = Bound::Excluded(100_u16);
        let upper = Bound::Included(120_u16);
        let key = 120_u16;

        assert_eq!(
            key_within_envelope(&key, &lower, &upper),
            KeyEnvelope::new(lower, upper).contains(&key),
            "free helper should delegate to envelope semantics",
        );
    }
}
