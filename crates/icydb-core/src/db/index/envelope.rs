//! Module: index::envelope
//! Responsibility: canonical bound-envelope and continuation-envelope helpers for index-domain operations.
//! Does not own: planner continuation policy or token wire formats.
//! Boundary: index-owned key-envelope semantics consumed by cursor/runtime/index layers.

use crate::{db::direction::Direction, error::InternalError};
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

/// Shared directional strict-advancement comparator for continuation checks.
/// `candidate` advances only when it is strictly after `anchor` under direction.
#[must_use]
pub(in crate::db) fn continuation_advanced<K: Ord>(
    direction: Direction,
    candidate: &K,
    anchor: &K,
) -> bool {
    match direction {
        Direction::Asc => candidate > anchor,
        Direction::Desc => candidate < anchor,
    }
}

/// Rewrite continuation bounds while cloning only retained bound edges.
#[must_use]
pub(in crate::db) fn resume_bounds_from_refs<K: Clone + Ord>(
    direction: Direction,
    lower: &Bound<K>,
    upper: &Bound<K>,
    anchor: &K,
) -> (Bound<K>, Bound<K>) {
    #[cfg(debug_assertions)]
    {
        debug_assert!(
            key_within_envelope(anchor, lower, upper),
            "cursor anchor escaped envelope",
        );

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

/// Validate continuation anchor containment against the original index-scan envelope.
pub(in crate::db) fn validate_index_scan_continuation_envelope<K: Ord + Clone>(
    anchor: Option<&K>,
    lower: &Bound<K>,
    upper: &Bound<K>,
) -> Result<(), InternalError> {
    if let Some(anchor) = anchor
        && !key_within_envelope(anchor, lower, upper)
    {
        return Err(InternalError::index_invariant(
            "index-range continuation anchor is outside the requested range envelope",
        ));
    }

    Ok(())
}

/// Validate strict directional continuation advancement for one scan candidate.
pub(in crate::db) fn validate_index_scan_continuation_advancement<K: Ord>(
    direction: Direction,
    anchor: Option<&K>,
    candidate: &K,
) -> Result<(), InternalError> {
    if let Some(anchor) = anchor
        && !continuation_advanced(direction, candidate, anchor)
    {
        return Err(InternalError::index_invariant(
            "index-range continuation scan did not advance beyond the anchor",
        ));
    }

    Ok(())
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
    use super::{
        KeyEnvelope, continuation_advanced, key_within_envelope, resume_bounds_from_refs,
        validate_index_scan_continuation_advancement, validate_index_scan_continuation_envelope,
    };
    use crate::{
        db::{
            direction::Direction,
            index::{
                EncodedValue, RawIndexKey, envelope_is_empty,
                key::{IndexId, IndexKeyKind},
                raw_keys_for_encoded_prefix_with_kind,
            },
        },
        error::{ErrorClass, ErrorOrigin},
        traits::Storable,
        value::Value,
    };
    use proptest::prelude::*;
    use std::{borrow::Cow, cmp::Ordering, ops::Bound};

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

    #[test]
    fn continuation_advanced_is_directional() {
        let anchor = 0x10_u8;
        let asc_candidate = 0x11_u8;
        let desc_candidate = 0x0F_u8;

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

    #[test]
    fn continuation_advancement_guard_rejects_non_advanced_candidate_asc() {
        let anchor = 0x10_u8;
        let candidate = 0x10_u8;

        let err =
            validate_index_scan_continuation_advancement(Direction::Asc, Some(&anchor), &candidate)
                .expect_err("ASC continuation candidate equal to anchor must be rejected");

        assert_eq!(err.class, ErrorClass::InvariantViolation);
        assert_eq!(err.origin, ErrorOrigin::Index);
    }

    #[test]
    fn continuation_advancement_guard_rejects_non_advanced_candidate_desc() {
        let anchor = 0x10_u8;
        let candidate = 0x11_u8;

        let err = validate_index_scan_continuation_advancement(
            Direction::Desc,
            Some(&anchor),
            &candidate,
        )
        .expect_err("DESC continuation candidate not strictly after anchor must be rejected");

        assert_eq!(err.class, ErrorClass::InvariantViolation);
        assert_eq!(err.origin, ErrorOrigin::Index);
    }

    #[test]
    fn anchor_containment_guard_rejects_out_of_envelope_anchor() {
        let lower = Bound::Included(0x10_u8);
        let upper = Bound::Excluded(0x20_u8);
        let anchor = 0x20_u8;

        let err = validate_index_scan_continuation_envelope(Some(&anchor), &lower, &upper)
            .expect_err("out-of-envelope continuation anchor must be rejected");

        assert_eq!(err.class, ErrorClass::InvariantViolation);
        assert_eq!(err.origin, ErrorOrigin::Index);
    }

    #[test]
    fn anchor_equal_to_upper_resumes_to_empty_envelope() {
        let lower = Bound::Included(raw_key(0x10));
        let upper = Bound::Included(raw_key(0x20));
        let anchor = raw_key(0x20);

        let (resumed_lower, resumed_upper) =
            resume_bounds_from_refs(Direction::Asc, &lower, &upper, &anchor);
        assert!(
            envelope_is_empty(&resumed_lower, &resumed_upper),
            "anchor==upper must resume to an empty envelope so scan can short-circuit",
        );
    }

    fn raw_key(byte: u8) -> RawIndexKey {
        <RawIndexKey as Storable>::from_bytes(Cow::Owned(vec![byte]))
    }

    fn property_index_id() -> IndexId {
        IndexId::new(crate::types::EntityTag::new(0xC01A_71C0_0000_0001), 0)
    }

    // Build one canonical raw index key from semantic composite components.
    fn canonical_raw_key(values: &[Value]) -> RawIndexKey {
        let encoded = EncodedValue::try_encode_all(values)
            .expect("property-domain values must remain canonically index-encodable");
        let (key, _) = raw_keys_for_encoded_prefix_with_kind(
            &property_index_id(),
            IndexKeyKind::User,
            values.len(),
            encoded.as_slice(),
        );

        key
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
            let left_recanonical = canonical_raw_key(&[left_int, left_text, left_uint]);
            let right_recanonical = canonical_raw_key(&[right_int, right_text, right_uint]);

            // Phase 2: treat left as anchor and right as candidate under all comparator gates.
            let ordering = left_recanonical.cmp(&right_recanonical);
            let lower_included_asc = key_within_envelope(
                &right_recanonical,
                &Bound::Included(left_recanonical.clone()),
                &Bound::Unbounded,
            );
            let lower_excluded_asc = key_within_envelope(
                &right_recanonical,
                &Bound::Excluded(left_recanonical.clone()),
                &Bound::Unbounded,
            );
            let upper_included_asc = key_within_envelope(
                &right_recanonical,
                &Bound::Unbounded,
                &Bound::Included(left_recanonical.clone()),
            );
            let upper_excluded_asc = key_within_envelope(
                &right_recanonical,
                &Bound::Unbounded,
                &Bound::Excluded(left_recanonical.clone()),
            );
            let lower_included_desc = key_within_envelope(
                &right_recanonical,
                &Bound::Included(left_recanonical.clone()),
                &Bound::Unbounded,
            );
            let lower_excluded_desc = key_within_envelope(
                &right_recanonical,
                &Bound::Excluded(left_recanonical.clone()),
                &Bound::Unbounded,
            );
            let upper_included_desc = key_within_envelope(
                &right_recanonical,
                &Bound::Unbounded,
                &Bound::Included(left_recanonical.clone()),
            );
            let upper_excluded_desc = key_within_envelope(
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
