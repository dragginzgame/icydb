//! Module: cursor::envelope
//! Responsibility: cursor-owned continuation advancement and resume-bound helpers.
//! Does not own: index storage traversal mechanics.
//! Boundary: centralizes strict continuation and bound rewrite semantics for cursor consumers.

use crate::db::direction::Direction;
use crate::error::InternalError;
use std::ops::Bound;

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
            anchor_within_envelope(anchor, lower, upper),
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

/// Validate that a continuation anchor remains inside the original envelope.
#[must_use]
pub(in crate::db) fn anchor_within_envelope<K: Ord + Clone>(
    anchor: &K,
    lower: &Bound<K>,
    upper: &Bound<K>,
) -> bool {
    let lower_ok = match lower {
        Bound::Unbounded => true,
        Bound::Included(boundary) => anchor >= boundary,
        Bound::Excluded(boundary) => anchor > boundary,
    };
    let upper_ok = match upper {
        Bound::Unbounded => true,
        Bound::Included(boundary) => anchor <= boundary,
        Bound::Excluded(boundary) => anchor < boundary,
    };

    lower_ok && upper_ok
}

/// Validate continuation anchor containment against the original index-scan envelope.
pub(in crate::db) fn validate_index_scan_continuation_envelope<K: Ord + Clone>(
    anchor: Option<&K>,
    lower: &Bound<K>,
    upper: &Bound<K>,
) -> Result<(), InternalError> {
    if let Some(anchor) = anchor
        && !anchor_within_envelope(anchor, lower, upper)
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
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        anchor_within_envelope, continuation_advanced, resume_bounds_from_refs,
        validate_index_scan_continuation_advancement, validate_index_scan_continuation_envelope,
    };
    use crate::{
        db::{
            direction::Direction,
            identity::{EntityName, IndexName},
            index::{
                EncodedValue, IndexId, IndexKey, IndexKeyKind, RawIndexKey, envelope_is_empty,
                raw_keys_for_encoded_prefix_with_kind,
            },
        },
        error::{ErrorClass, ErrorOrigin},
        traits::Storable,
        value::Value,
    };
    use proptest::prelude::*;
    use std::{borrow::Cow, cmp::Ordering, ops::Bound};

    fn raw_key(byte: u8) -> RawIndexKey {
        <RawIndexKey as Storable>::from_bytes(Cow::Owned(vec![byte]))
    }

    #[test]
    fn anchor_within_envelope_enforces_bounds_for_current_model() {
        let lower = Bound::Included(raw_key(0x10));
        let upper = Bound::Excluded(raw_key(0x20));
        let inside = raw_key(0x18);
        let below = raw_key(0x0F);
        let at_excluded_upper = raw_key(0x20);

        assert!(anchor_within_envelope(&inside, &lower, &upper));
        assert!(!anchor_within_envelope(&below, &lower, &upper));
        assert!(!anchor_within_envelope(&at_excluded_upper, &lower, &upper));
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

    #[test]
    fn continuation_advancement_guard_rejects_non_advanced_candidate_asc() {
        let anchor = raw_key(0x10);
        let candidate = raw_key(0x10);

        let err =
            validate_index_scan_continuation_advancement(Direction::Asc, Some(&anchor), &candidate)
                .expect_err("ASC continuation candidate equal to anchor must be rejected");

        assert_eq!(err.class, ErrorClass::InvariantViolation);
        assert_eq!(err.origin, ErrorOrigin::Index);
    }

    #[test]
    fn continuation_advancement_guard_rejects_non_advanced_candidate_desc() {
        let anchor = raw_key(0x10);
        let candidate = raw_key(0x11);

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
        let lower = Bound::Included(raw_key(0x10));
        let upper = Bound::Excluded(raw_key(0x20));
        let anchor = raw_key(0x20);

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
                &right_recanonical,
                &Bound::Included(left_recanonical.clone()),
                &Bound::Unbounded,
            );
            let lower_excluded_asc = anchor_within_envelope(
                &right_recanonical,
                &Bound::Excluded(left_recanonical.clone()),
                &Bound::Unbounded,
            );
            let upper_included_asc = anchor_within_envelope(
                &right_recanonical,
                &Bound::Unbounded,
                &Bound::Included(left_recanonical.clone()),
            );
            let upper_excluded_asc = anchor_within_envelope(
                &right_recanonical,
                &Bound::Unbounded,
                &Bound::Excluded(left_recanonical.clone()),
            );
            let lower_included_desc = anchor_within_envelope(
                &right_recanonical,
                &Bound::Included(left_recanonical.clone()),
                &Bound::Unbounded,
            );
            let lower_excluded_desc = anchor_within_envelope(
                &right_recanonical,
                &Bound::Excluded(left_recanonical.clone()),
                &Bound::Unbounded,
            );
            let upper_included_desc = anchor_within_envelope(
                &right_recanonical,
                &Bound::Unbounded,
                &Bound::Included(left_recanonical.clone()),
            );
            let upper_excluded_desc = anchor_within_envelope(
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
