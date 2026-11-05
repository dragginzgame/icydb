use super::*;
use crate::error::{ErrorClass, ErrorOrigin};

fn owner() -> IdentityStateOwner {
    IdentityStateOwner::try_new(
        DatabaseIncarnationId::for_tests(0x21),
        EntityTag::new(7),
        FieldId::new(3),
    )
    .expect("identity owner should admit")
}

fn advance() -> IdentityAdvanceId {
    IdentityAdvanceId::try_new([0x31; 16], [0x41; 16], 19, 5)
        .expect("nonzero advance identity should admit")
}

fn assert_identity_corruption(bytes: &[u8]) {
    let error = decode_identity_state(bytes).expect_err("malformed state must reject");
    assert_eq!(error.class(), ErrorClass::Corruption);
    assert_eq!(error.origin(), ErrorOrigin::Identity);
}

#[test]
fn identity_state_codec_is_fixed_width_checksummed_and_canonical() {
    let state = IdentityState::try_new(
        owner(),
        AcceptedFieldKind::Nat128,
        IdentityStateLifecycle::Retired,
        u128::MAX,
        Some(advance()),
    )
    .expect("max-width retired state should admit");
    let encoded = encode_identity_state(&state).expect("identity state should encode");

    assert_eq!(encoded.len(), IDENTITY_STATE_RECORD_BYTES);
    assert_eq!(
        decode_identity_state(&encoded).expect("identity state should decode"),
        state,
    );
    assert_eq!(
        encoded
            .get(39..55)
            .expect("high-water bytes should occupy the fixed field"),
        &u128::MAX.to_be_bytes(),
    );
}

#[test]
fn identity_state_codec_rejects_bad_envelope_and_noncanonical_absent_advance() {
    let state = IdentityState::new_active(owner(), AcceptedFieldKind::Nat64)
        .expect("zero state should admit");
    let encoded = encode_identity_state(&state).expect("identity state should encode");

    assert_identity_corruption(&encoded[..encoded.len().saturating_sub(1)]);

    for offset in [0, 8, 37, 38, 100] {
        let mut malformed = encoded.clone();
        malformed[offset] ^= 0xFF;
        assert_identity_corruption(&malformed);
    }

    let mut absent_with_payload = encoded;
    absent_with_payload[56] = 1;
    let checksum = crc32c(&absent_with_payload[..IDENTITY_STATE_BODY_BYTES]);
    absent_with_payload[IDENTITY_STATE_BODY_BYTES..].copy_from_slice(&checksum.to_be_bytes());
    assert_identity_corruption(&absent_with_payload);

    let mut present_without_identity =
        encode_identity_state(&state).expect("identity state should encode");
    present_without_identity[55] = IDENTITY_STATE_ADVANCE_PRESENT;
    let checksum = crc32c(&present_without_identity[..IDENTITY_STATE_BODY_BYTES]);
    present_without_identity[IDENTITY_STATE_BODY_BYTES..].copy_from_slice(&checksum.to_be_bytes());
    assert_identity_corruption(&present_without_identity);
}

#[test]
fn identity_state_enforces_exact_unsigned_width_bounds() {
    for (kind, maximum) in [
        (AcceptedFieldKind::Nat8, u128::from(u8::MAX)),
        (AcceptedFieldKind::Nat16, u128::from(u16::MAX)),
        (AcceptedFieldKind::Nat32, u128::from(u32::MAX)),
        (AcceptedFieldKind::Nat64, u128::from(u64::MAX)),
        (AcceptedFieldKind::Nat128, u128::MAX),
    ] {
        let state = IdentityState::try_new(
            owner(),
            kind.clone(),
            IdentityStateLifecycle::Active,
            maximum,
            Some(advance()),
        )
        .expect("exact kind maximum should admit");
        assert_eq!(
            decode_identity_state(
                &encode_identity_state(&state).expect("bounded state should encode")
            )
            .expect("bounded state should decode"),
            state,
        );

        if let Some(overflow) = maximum.checked_add(1) {
            assert!(
                IdentityState::try_new(
                    owner(),
                    kind,
                    IdentityStateLifecycle::Active,
                    overflow,
                    None,
                )
                .is_err()
            );
        }
    }
}

#[test]
fn identity_state_rejects_non_identity_kind_and_zero_owner_components() {
    assert!(
        IdentityState::new_active(owner(), AcceptedFieldKind::Int64).is_err(),
        "signed state must reject",
    );
    assert!(
        IdentityStateOwner::try_new(
            DatabaseIncarnationId::for_tests(0x21),
            EntityTag::new(0),
            FieldId::new(3),
        )
        .is_err(),
    );
    assert!(
        IdentityStateOwner::try_new(
            DatabaseIncarnationId::for_tests(0x21),
            EntityTag::new(7),
            FieldId::new(0),
        )
        .is_err(),
    );
    assert!(
        IdentityState::try_new(
            owner(),
            AcceptedFieldKind::Nat64,
            IdentityStateLifecycle::Active,
            1,
            None,
        )
        .is_err(),
        "nonzero materialized state requires exact applied-advance evidence",
    );
    assert!(
        IdentityState::try_new(
            owner(),
            AcceptedFieldKind::Nat64,
            IdentityStateLifecycle::Active,
            0,
            Some(advance()),
        )
        .is_err(),
        "zero materialized state cannot claim an applied advance",
    );
}

#[test]
fn statement_cursor_allocates_exact_runtime_shapes_in_input_order() {
    for (kind, first, second) in [
        (AcceptedFieldKind::Nat8, Value::Nat64(1), Value::Nat64(2)),
        (AcceptedFieldKind::Nat16, Value::Nat64(1), Value::Nat64(2)),
        (AcceptedFieldKind::Nat32, Value::Nat64(1), Value::Nat64(2)),
        (AcceptedFieldKind::Nat64, Value::Nat64(1), Value::Nat64(2)),
        (
            AcceptedFieldKind::Nat128,
            Value::Nat128(1),
            Value::Nat128(2),
        ),
    ] {
        let state =
            IdentityState::new_active(owner(), kind).expect("zero active state should admit");
        let mut cursor = IdentityStatementCursor::from_active_state(&state)
            .expect("active state should open a cursor");

        let allocation_0 = cursor
            .allocate(4, 0)
            .expect("first allocation should succeed");
        let allocation_1 = cursor
            .allocate(4, 1)
            .expect("second allocation should succeed");

        assert_eq!(allocation_0.owner(), owner());
        assert_eq!(allocation_0.field_slot(), 4);
        assert_eq!(allocation_0.input_ordinal(), 0);
        assert_eq!(allocation_0.value(), &first);
        assert_eq!(allocation_1.input_ordinal(), 1);
        assert_eq!(allocation_1.value(), &second);
        assert_eq!(cursor.owner(), owner());
        assert_eq!(cursor.expected_high_water(), 0);
        assert_eq!(cursor.tentative_high_water(), 2);
        assert_eq!(cursor.allocation_count(), 2);
        assert!(cursor.has_allocations());
    }
}

#[test]
fn statement_cursor_rejects_exhaustion_and_order_without_advancing() {
    for (kind, maximum) in [
        (AcceptedFieldKind::Nat8, u128::from(u8::MAX)),
        (AcceptedFieldKind::Nat16, u128::from(u16::MAX)),
        (AcceptedFieldKind::Nat32, u128::from(u32::MAX)),
        (AcceptedFieldKind::Nat64, u128::from(u64::MAX)),
        (AcceptedFieldKind::Nat128, u128::MAX),
    ] {
        let state = IdentityState::try_new(
            owner(),
            kind,
            IdentityStateLifecycle::Active,
            maximum,
            Some(advance()),
        )
        .expect("exact maximum should admit");
        let mut exhausted = IdentityStatementCursor::from_active_state(&state)
            .expect("active state should open a cursor");
        let error = exhausted
            .allocate(0, 0)
            .expect_err("allocation beyond the exact width must reject");
        assert_eq!(error.class(), ErrorClass::Unsupported);
        assert_eq!(error.origin(), ErrorOrigin::Identity);
        assert_eq!(exhausted.expected_high_water(), maximum);
        assert_eq!(exhausted.tentative_high_water(), maximum);
        assert_eq!(exhausted.allocation_count(), 0);
        assert!(!exhausted.has_allocations());
    }

    let state = IdentityState::new_active(owner(), AcceptedFieldKind::Nat64)
        .expect("zero active state should admit");
    let mut out_of_order = IdentityStatementCursor::from_active_state(&state)
        .expect("active state should open a cursor");
    let error = out_of_order
        .allocate(0, 1)
        .expect_err("the first candidate must retain ordinal zero");
    assert_eq!(error.class(), ErrorClass::InvariantViolation);
    assert_eq!(error.origin(), ErrorOrigin::Store);
    assert_eq!(out_of_order.tentative_high_water(), 0);
    assert_eq!(out_of_order.allocation_count(), 0);
}

#[test]
fn marker_range_distinguishes_committed_from_materialized_state() {
    let state = IdentityState::new_active(owner(), AcceptedFieldKind::Nat64)
        .expect("zero active state should admit");
    let range = IdentityRangeAdvance::try_new(owner(), 0, 2, 2).expect("range should admit");
    let advance = advance();

    let pending = state
        .range_commit_state(range, advance)
        .expect("the marker should commit the pending range");
    assert_eq!(pending.materialized_high_water(), 0);
    assert_eq!(pending.committed_high_water(), 2);
    assert!(!pending.is_materialized());

    let materialized = state
        .apply_range_advance(range, advance)
        .expect("the committed range should materialize");
    let complete = materialized
        .range_commit_state(range, advance)
        .expect("exact replay should remain materialized");
    assert_eq!(complete.materialized_high_water(), 2);
    assert_eq!(complete.committed_high_water(), 2);
    assert!(complete.is_materialized());

    let stale_advance = IdentityAdvanceId::try_new([0x32; 16], [0x42; 16], 20, 6)
        .expect("second advance identity should admit");
    assert!(
        materialized
            .range_commit_state(range, stale_advance)
            .is_err(),
        "numeric equality without the exact advance identity is corruption",
    );
}

#[test]
fn tentative_range_preflight_classifies_conflict_and_corruption() {
    let identity_state = IdentityState::new_active(owner(), AcceptedFieldKind::Nat8)
        .expect("active state should admit");
    let current =
        IdentityRangeAdvance::try_new(owner(), 0, 2, 2).expect("current range should admit");
    identity_state
        .preflight_range_advance(current)
        .expect("current range should preflight");

    let stale = IdentityRangeAdvance::try_new(owner(), 1, 2, 1)
        .expect("stale range should remain structurally valid");
    let error = identity_state
        .preflight_range_advance(stale)
        .expect_err("stale high-water must conflict");
    assert_eq!(error.class(), ErrorClass::Conflict);
    assert_eq!(error.origin(), ErrorOrigin::Identity);

    let other_owner = IdentityStateOwner::try_new(
        owner().database_incarnation_id(),
        EntityTag::new(8),
        owner().field_id(),
    )
    .expect("other owner should admit");
    let wrong_owner = IdentityRangeAdvance::try_new(other_owner, 0, 1, 1)
        .expect("other-owner range should admit");
    let exhausted = IdentityRangeAdvance::try_new(owner(), 0, 256, 256)
        .expect("width-exhausting range should remain structurally valid");
    let retired = identity_state.retire().expect("active state should retire");

    for result in [
        identity_state.preflight_range_advance(wrong_owner),
        identity_state.preflight_range_advance(exhausted),
        retired.preflight_range_advance(current),
    ] {
        let error = result.expect_err("invalid state/range pairing must be corruption");
        assert_eq!(error.class(), ErrorClass::Corruption);
        assert_eq!(error.origin(), ErrorOrigin::Identity);
    }
}
