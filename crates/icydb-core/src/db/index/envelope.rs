//! Module: index::envelope
//! Responsibility: directional continuation advancement and envelope checks.
//! Does not own: range encoding or cursor-token signature policy.
//! Boundary: shared by index range and cursor resume paths.

use crate::db::direction::Direction;
use std::{cmp::Ordering, ops::Bound};

///
/// DirectionComparator
///
/// Direction-aware key comparator used by cursor resume and continuation checks.
/// Keeps strict "after anchor" semantics in one place.
///

struct DirectionComparator {
    direction: Direction,
}

impl DirectionComparator {
    const fn new(direction: Direction) -> Self {
        Self { direction }
    }

    fn is_strictly_after<K: Ord>(&self, candidate: &K, anchor: &K) -> bool {
        continuation_advances(self.direction, anchor, candidate)
    }
}

///
/// continuation_advances_from_ordering
///
/// Shared strict continuation predicate from one precomputed ordering.
/// Centralizes the strict "after" rule (`Greater` only) across cursor layers.
///
#[must_use]
pub(in crate::db) const fn continuation_advances_from_ordering(ordering: Ordering) -> bool {
    ordering.is_gt()
}

///
/// continuation_advances
///
/// Shared directional strict-advancement comparator for continuation checks.
/// `candidate` advances only when it is strictly after `anchor` under direction.
///
#[must_use]
pub(in crate::db) fn continuation_advances<K: Ord>(
    direction: Direction,
    anchor: &K,
    candidate: &K,
) -> bool {
    let ordering = match direction {
        Direction::Asc => candidate.cmp(anchor),
        Direction::Desc => anchor.cmp(candidate),
    };

    continuation_advances_from_ordering(ordering)
}

///
/// continuation_advanced
///
/// Validate strict monotonic advancement relative to one continuation anchor.
///
#[must_use]
pub(in crate::db) fn continuation_advanced<K: Ord>(
    direction: Direction,
    candidate: &K,
    anchor: &K,
) -> bool {
    KeyEnvelope::new(direction, Bound::Unbounded, Bound::Unbounded)
        .continuation_advanced(candidate, anchor)
}

///
/// resume_bounds_from_refs
///
/// Rewrite continuation bounds while cloning only retained bound edges.
///
#[must_use]
pub(in crate::db) fn resume_bounds_from_refs<K: Clone + Ord>(
    direction: Direction,
    lower: &Bound<K>,
    upper: &Bound<K>,
    anchor: &K,
) -> (Bound<K>, Bound<K>) {
    #[cfg(debug_assertions)]
    {
        let envelope = KeyEnvelope::new(direction, lower.clone(), upper.clone());

        debug_assert!(envelope.contains(anchor), "cursor anchor escaped envelope",);

        let bounds_key_ordered = match (lower, upper) {
            (Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
            (
                Bound::Included(lower_key) | Bound::Excluded(lower_key),
                Bound::Included(upper_key) | Bound::Excluded(upper_key),
            ) => lower_key <= upper_key,
        };
        debug_assert!(
            bounds_key_ordered,
            "index envelope bounds must remain ordered before continuation rewrite",
        );
    }

    match direction {
        Direction::Asc => (Bound::Excluded(anchor.clone()), upper.clone()),
        Direction::Desc => (lower.clone(), Bound::Excluded(anchor.clone())),
    }
}

///
/// anchor_within_envelope
///
/// Validate that a continuation anchor remains inside the original envelope.
///
#[must_use]
pub(in crate::db) fn anchor_within_envelope<K: Ord + Clone>(
    direction: Direction,
    anchor: &K,
    lower: &Bound<K>,
    upper: &Bound<K>,
) -> bool {
    KeyEnvelope::new(direction, lower.clone(), upper.clone()).contains(anchor)
}

///
/// key_within_envelope
///
/// Validate that one key is contained by one canonical bound envelope.
/// This centralizes inclusive/exclusive bound semantics under index authority.
///
#[must_use]
pub(in crate::db) fn key_within_envelope<K: Ord + Clone>(
    key: &K,
    lower: &Bound<K>,
    upper: &Bound<K>,
) -> bool {
    // Envelope containment is direction-agnostic; use one canonical direction.
    KeyEnvelope::new(Direction::Asc, lower.clone(), upper.clone()).contains(key)
}

///
/// KeyEnvelope
///
/// Canonical raw-key envelope with direction-aware continuation semantics.
/// Centralizes anchor rewrite, containment checks, monotonic advancement, and
/// empty-envelope detection for cursor continuation paths.
///

pub(in crate::db) struct KeyEnvelope<K> {
    comparator: DirectionComparator,
    lower: Bound<K>,
    upper: Bound<K>,
}

impl<K> KeyEnvelope<K>
where
    K: Ord,
{
    pub(in crate::db) const fn new(direction: Direction, lower: Bound<K>, upper: Bound<K>) -> Self {
        Self {
            comparator: DirectionComparator::new(direction),
            lower,
            upper,
        }
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

    pub(in crate::db) fn continuation_advanced(&self, candidate: &K, anchor: &K) -> bool {
        self.comparator.is_strictly_after(candidate, anchor)
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            direction::Direction,
            identity::{EntityName, IndexName},
            index::{EncodedValue, IndexId, IndexKey, IndexKeyKind, RawIndexKey},
        },
        traits::Storable,
        value::Value,
    };
    use proptest::prelude::*;
    use std::{borrow::Cow, cmp::Ordering, ops::Bound};

    use super::{anchor_within_envelope, continuation_advanced};

    fn raw_key(byte: u8) -> RawIndexKey {
        <RawIndexKey as Storable>::from_bytes(Cow::Owned(vec![byte]))
    }

    #[test]
    fn anchor_within_envelope_is_bidirectionally_contained_for_current_model() {
        let lower = Bound::Included(raw_key(0x10));
        let upper = Bound::Excluded(raw_key(0x20));
        let inside = raw_key(0x18);
        let below = raw_key(0x0F);
        let at_excluded_upper = raw_key(0x20);

        assert_eq!(
            anchor_within_envelope(Direction::Asc, &inside, &lower, &upper),
            anchor_within_envelope(Direction::Desc, &inside, &lower, &upper),
            "ASC and DESC envelope containment must match for equivalent bounds",
        );
        assert_eq!(
            anchor_within_envelope(Direction::Asc, &below, &lower, &upper),
            anchor_within_envelope(Direction::Desc, &below, &lower, &upper),
            "ASC and DESC envelope containment must match for below-lower anchors",
        );
        assert_eq!(
            anchor_within_envelope(Direction::Asc, &at_excluded_upper, &lower, &upper),
            anchor_within_envelope(Direction::Desc, &at_excluded_upper, &lower, &upper),
            "ASC and DESC envelope containment must match for upper-boundary anchors",
        );
    }

    #[test]
    fn continuation_advanced_is_directional() {
        let anchor = raw_key(0x10);
        let asc_candidate = raw_key(0x11);
        let desc_candidate = raw_key(0x0F);

        assert!(continuation_advanced(
            Direction::Asc,
            &asc_candidate,
            &anchor
        ));
        assert!(!continuation_advanced(
            Direction::Asc,
            &desc_candidate,
            &anchor
        ));

        assert!(continuation_advanced(
            Direction::Desc,
            &desc_candidate,
            &anchor
        ));
        assert!(!continuation_advanced(
            Direction::Desc,
            &asc_candidate,
            &anchor
        ));
    }

    fn property_index_id() -> IndexId {
        let entity = EntityName::try_from_str("continuation_property")
            .expect("property test entity name should parse");
        IndexId(
            IndexName::try_from_parts(&entity, &["f0", "f1", "f2"])
                .expect("property test index name should parse"),
        )
    }

    // Build one canonical raw index key from semantic composite components.
    fn canonical_raw_key(values: &[Value]) -> RawIndexKey {
        let encoded = EncodedValue::try_encode_all(values)
            .expect("property-domain values must remain canonically index-encodable");
        let (key, _) = IndexKey::bounds_for_prefix_with_kind(
            &property_index_id(),
            IndexKeyKind::User,
            values.len(),
            encoded.as_slice(),
        );

        key.to_raw()
    }

    fn int_component_strategy() -> impl Strategy<Value = Value> {
        prop_oneof![
            Just(Value::Int(i64::MIN)),
            Just(Value::Int(-1_i64)),
            Just(Value::Int(0_i64)),
            Just(Value::Int(1_i64)),
            Just(Value::Int(i64::MAX)),
        ]
    }

    fn text_component_strategy() -> impl Strategy<Value = Value> {
        prop_oneof![
            Just(Value::Text(String::new())),
            Just(Value::Text("a".to_string())),
            Just(Value::Text("mm".to_string())),
            Just(Value::Text("zz".to_string())),
        ]
    }

    fn uint_component_strategy() -> impl Strategy<Value = Value> {
        prop_oneof![
            Just(Value::Uint(0_u64)),
            Just(Value::Uint(1_u64)),
            Just(Value::Uint(1024_u64)),
            Just(Value::Uint(u64::MAX)),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn cross_layer_canonical_ordering_is_consistent(
            left_int in int_component_strategy(),
            left_text in text_component_strategy(),
            left_uint in uint_component_strategy(),
            right_int in int_component_strategy(),
            right_text in text_component_strategy(),
            right_uint in uint_component_strategy(),
        ) {
            // Phase 1: canonicalize through encode -> decode -> re-encode for both keys.
            let left_raw = canonical_raw_key(&[left_int, left_text, left_uint]);
            let left_decoded = IndexKey::try_from_raw(&left_raw)
                .expect("left canonical raw key should decode");
            let left_recanonical = left_decoded.to_raw();
            let left_decoded_again = IndexKey::try_from_raw(&left_recanonical)
                .expect("left recanonical raw key should decode");
            let left_recanonical_again = left_decoded_again.to_raw();
            prop_assert_eq!(
                &left_recanonical,
                &left_recanonical_again,
                "left key canonicalization must remain idempotent across repeated decode/re-encode",
            );

            let right_raw = canonical_raw_key(&[right_int, right_text, right_uint]);
            let right_decoded = IndexKey::try_from_raw(&right_raw)
                .expect("right canonical raw key should decode");
            let right_recanonical = right_decoded.to_raw();
            let right_decoded_again = IndexKey::try_from_raw(&right_recanonical)
                .expect("right recanonical raw key should decode");
            let right_recanonical_again = right_decoded_again.to_raw();
            prop_assert_eq!(
                &right_recanonical,
                &right_recanonical_again,
                "right key canonicalization must remain idempotent across repeated decode/re-encode",
            );

            // Phase 2: treat left as anchor and right as candidate under all comparator gates.
            let ordering = left_recanonical.cmp(&right_recanonical);

            let lower_included_asc = anchor_within_envelope(
                Direction::Asc,
                &right_recanonical,
                &Bound::Included(left_recanonical.clone()),
                &Bound::Unbounded,
            );
            let lower_excluded_asc = anchor_within_envelope(
                Direction::Asc,
                &right_recanonical,
                &Bound::Excluded(left_recanonical.clone()),
                &Bound::Unbounded,
            );
            let upper_included_asc = anchor_within_envelope(
                Direction::Asc,
                &right_recanonical,
                &Bound::Unbounded,
                &Bound::Included(left_recanonical.clone()),
            );
            let upper_excluded_asc = anchor_within_envelope(
                Direction::Asc,
                &right_recanonical,
                &Bound::Unbounded,
                &Bound::Excluded(left_recanonical.clone()),
            );
            let lower_included_desc = anchor_within_envelope(
                Direction::Desc,
                &right_recanonical,
                &Bound::Included(left_recanonical.clone()),
                &Bound::Unbounded,
            );
            let lower_excluded_desc = anchor_within_envelope(
                Direction::Desc,
                &right_recanonical,
                &Bound::Excluded(left_recanonical.clone()),
                &Bound::Unbounded,
            );
            let upper_included_desc = anchor_within_envelope(
                Direction::Desc,
                &right_recanonical,
                &Bound::Unbounded,
                &Bound::Included(left_recanonical.clone()),
            );
            let upper_excluded_desc = anchor_within_envelope(
                Direction::Desc,
                &right_recanonical,
                &Bound::Unbounded,
                &Bound::Excluded(left_recanonical.clone()),
            );
            let asc_advanced =
                continuation_advanced(Direction::Asc, &right_recanonical, &left_recanonical);
            let desc_advanced =
                continuation_advanced(Direction::Desc, &right_recanonical, &left_recanonical);

            // Phase 3: assert all cross-layer checks agree on the same ordering domain.
            prop_assert_eq!(
                lower_included_asc,
                ordering != Ordering::Greater,
                "included lower-bound containment must match raw-key comparator",
            );
            prop_assert_eq!(
                lower_excluded_asc,
                ordering == Ordering::Less,
                "excluded lower-bound containment must match strict raw-key comparator",
            );
            prop_assert_eq!(
                upper_included_asc,
                ordering != Ordering::Less,
                "included upper-bound containment must match raw-key comparator",
            );
            prop_assert_eq!(
                upper_excluded_asc,
                ordering == Ordering::Greater,
                "excluded upper-bound containment must match strict raw-key comparator",
            );
            prop_assert_eq!(
                lower_included_desc,
                lower_included_asc,
                "envelope containment must be direction-symmetric for included lower bounds",
            );
            prop_assert_eq!(
                lower_excluded_desc,
                lower_excluded_asc,
                "envelope containment must be direction-symmetric for excluded lower bounds",
            );
            prop_assert_eq!(
                upper_included_desc,
                upper_included_asc,
                "envelope containment must be direction-symmetric for included upper bounds",
            );
            prop_assert_eq!(
                upper_excluded_desc,
                upper_excluded_asc,
                "envelope containment must be direction-symmetric for excluded upper bounds",
            );
            prop_assert_eq!(
                asc_advanced,
                ordering == Ordering::Less,
                "ASC continuation advancement must match strict raw-key ordering",
            );
            prop_assert_eq!(
                desc_advanced,
                ordering == Ordering::Greater,
                "DESC continuation advancement must match strict raw-key ordering",
            );
        }
    }

    #[test]
    fn canonical_ordering_property_domain_rejects_null_components() {
        let err = EncodedValue::try_encode_all(&[Value::Null, Value::Text("a".to_string())])
            .expect_err("null values must remain non-indexable for canonical key domains");
        let message = err.to_string();
        assert!(
            message.contains("null") || message.contains("index"),
            "null component rejection should remain explicit: {message}",
        );
    }
}
