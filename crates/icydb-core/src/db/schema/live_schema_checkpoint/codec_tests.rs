//! Canonical checkpoint admission without serializing a second full record.

use super::{
    IDENTITY_STATE_RECORD_BYTES, LiveSchemaCheckpoint, LiveSchemaCheckpointKey, crc32c,
    decode_checkpoint, encode_checkpoint,
};
use crate::{
    db::{
        integrity::DatabaseIncarnationId,
        schema::{
            AcceptedFieldKind, AcceptedSchemaRevision, FieldId,
            empty_accepted_schema_candidate_for_tests,
            identity_state::{
                IdentityAdvanceId, IdentityState, IdentityStateLifecycle, IdentityStateOwner,
            },
        },
    },
    error::{ErrorClass, ErrorOrigin},
    types::EntityTag,
};

const STORE_PATH: &str = "test::CanonicalCheckpoint";

fn checkpoint(high_water: bool, lifecycle: IdentityStateLifecycle) -> LiveSchemaCheckpoint {
    let candidate =
        empty_accepted_schema_candidate_for_tests(STORE_PATH, AcceptedSchemaRevision::new(1));
    let advance = IdentityAdvanceId::try_new([1; 16], [2; 16], u64::MAX, u32::MAX).unwrap();
    let identity_states = [
        (AcceptedFieldKind::Nat8, u128::from(u8::MAX)),
        (AcceptedFieldKind::Nat16, u128::from(u16::MAX)),
        (AcceptedFieldKind::Nat32, u128::from(u32::MAX)),
        (AcceptedFieldKind::Nat64, u128::from(u64::MAX)),
        (AcceptedFieldKind::Nat128, u128::MAX),
    ]
    .into_iter()
    .enumerate()
    .map(|(ordinal, (kind, maximum))| {
        let field = FieldId::new(u32::try_from(ordinal + 1).unwrap());
        let owner = IdentityStateOwner::try_new(
            DatabaseIncarnationId::for_tests(0x71),
            EntityTag::new(u64::from(field.get())),
            field,
        )
        .unwrap();
        let state = IdentityState::try_new(
            owner,
            kind,
            lifecycle,
            if high_water { maximum } else { 0 },
            high_water.then_some(advance),
        )
        .unwrap();
        ((owner.entity_tag(), owner.field_id()), state)
    })
    .collect();
    LiveSchemaCheckpoint {
        candidate,
        identity_states,
    }
}

fn rewrite_checksum(bytes: &mut [u8]) {
    let body_end = bytes.len() - size_of::<u32>();
    let checksum = crc32c(&bytes[..body_end]).to_be_bytes();
    bytes[body_end..].copy_from_slice(&checksum);
}

fn assert_corruption(bytes: &[u8], key: LiveSchemaCheckpointKey, origin: ErrorOrigin) {
    let error = decode_checkpoint(bytes, key)
        .err()
        .expect("malformed checkpoint must reject");
    assert_eq!(error.class(), ErrorClass::Corruption);
    assert_eq!(error.origin(), origin);
}

#[test]
fn accepted_checkpoint_bytes_remain_exact_across_identity_states_and_mutations() {
    let key = LiveSchemaCheckpointKey::for_store(STORE_PATH).unwrap();
    for high_water in [false, true] {
        for lifecycle in [
            IdentityStateLifecycle::Active,
            IdentityStateLifecycle::Retired,
        ] {
            let expected = checkpoint(high_water, lifecycle);
            let encoded = encode_checkpoint(STORE_PATH, &expected).unwrap();
            let decoded = decode_checkpoint(&encoded, key).unwrap();
            assert_eq!(decoded.identity_states(), expected.identity_states());
            assert_eq!(encode_checkpoint(STORE_PATH, &decoded).unwrap(), encoded);

            // This is the former decoder's byte-equality condition, retained
            // as an acceptance oracle rather than a production allocation.
            // Repair the envelope CRC so mutations reach the structural checks.
            for offset in 0..encoded.len() - size_of::<u32>() {
                for mask in [1, 0x80, 0xff] {
                    let mut mutated = encoded.clone();
                    mutated[offset] ^= mask;
                    rewrite_checksum(&mut mutated);
                    if let Ok(decoded) = decode_checkpoint(&mutated, key) {
                        assert_eq!(encode_checkpoint(STORE_PATH, &decoded).unwrap(), mutated);
                    }
                }
            }
        }
    }
}

#[test]
fn checkpoint_rejects_truncation_extra_input_wrong_key_and_noncanonical_inventory() {
    let key = LiveSchemaCheckpointKey::for_store(STORE_PATH).unwrap();
    let checkpoint = checkpoint(false, IdentityStateLifecycle::Active);
    let encoded = encode_checkpoint(STORE_PATH, &checkpoint).unwrap();
    for length in 0..encoded.len() {
        assert!(decode_checkpoint(&encoded[..length], key).is_err());
    }
    let mut trailing = encoded.clone();
    trailing.insert(trailing.len() - size_of::<u32>(), 0);
    rewrite_checksum(&mut trailing);
    assert_corruption(&trailing, key, ErrorOrigin::Store);
    let other_key = LiveSchemaCheckpointKey::for_store("test::Other").unwrap();
    assert_corruption(&encoded, other_key, ErrorOrigin::Store);

    let states_start = encoded.len()
        - size_of::<u32>()
        - checkpoint.identity_states().len() * IDENTITY_STATE_RECORD_BYTES;
    let first = states_start..states_start + IDENTITY_STATE_RECORD_BYTES;
    let second = first.end..first.end + IDENTITY_STATE_RECORD_BYTES;
    let mut duplicate = encoded.clone();
    duplicate.copy_within(first.clone(), second.start);
    rewrite_checksum(&mut duplicate);
    assert_corruption(&duplicate, key, ErrorOrigin::Identity);
    let mut reversed = encoded.clone();
    reversed[first.clone()].copy_from_slice(&encoded[second.clone()]);
    reversed[second].copy_from_slice(&encoded[first.clone()]);
    rewrite_checksum(&mut reversed);
    assert_corruption(&reversed, key, ErrorOrigin::Identity);

    // A valid outer CRC must not hide nonzero padding for an absent advance.
    let mut noncanonical = encoded;
    noncanonical[first.start + 56] = 1;
    rewrite_checksum(&mut noncanonical[first]);
    rewrite_checksum(&mut noncanonical);
    assert_corruption(&noncanonical, key, ErrorOrigin::Identity);
}

fn identity_ranges(
    checkpoint: &LiveSchemaCheckpoint,
) -> Vec<(super::IdentityRangeAdvance, IdentityAdvanceId)> {
    checkpoint
        .identity_states()
        .values()
        .take(2)
        .enumerate()
        .map(|(ordinal, state)| {
            (
                super::IdentityRangeAdvance::try_new(state.owner(), 0, 1, 1).unwrap(),
                IdentityAdvanceId::try_new([3; 16], [4; 16], 1, u32::try_from(ordinal).unwrap())
                    .unwrap(),
            )
        })
        .collect()
}

#[test]
fn prepared_identity_checkpoint_combines_owners_and_matches_replay_bytes() {
    use crate::{db::query::preparation::with_preparation_work, testing::test_memory};
    use std::rc::Rc;
    let memory = Rc::new(test_memory(243));
    let mut store = super::LiveSchemaCheckpointStore::open(memory.clone()).unwrap();
    let initial = checkpoint(false, IdentityStateLifecycle::Active);
    let ranges = identity_ranges(&initial);
    let key = LiveSchemaCheckpointKey::for_store(STORE_PATH).unwrap();
    let before = encode_checkpoint(STORE_PATH, &initial).unwrap();
    store
        .map
        .insert(key, super::SchemaControlBytes(before.clone()))
        .expect("fixture storage operation should succeed");
    let prepared =
        with_preparation_work(|work| store.prepare_identity_ranges(STORE_PATH, &ranges, work))
            .unwrap();
    assert_eq!(store.map.get(&key).unwrap().unwrap().0, before);
    // Ordinary recovery's independent per-range application is the byte oracle.
    for (range, advance) in &ranges {
        store
            .apply_identity_range(STORE_PATH, *range, *advance)
            .unwrap();
    }
    let replayed = store.map.get(&key).unwrap().unwrap().0;
    assert_eq!(prepared.bytes, replayed);
    // Exact replay remains idempotent; a different marker cannot claim the
    // same materialized range. Both leave the accepted checkpoint untouched.
    for (range, advance) in &ranges {
        store
            .apply_identity_range(STORE_PATH, *range, *advance)
            .unwrap();
        let other = IdentityAdvanceId::try_new([9; 16], [4; 16], 1, 0).unwrap();
        assert!(
            store
                .apply_identity_range(STORE_PATH, *range, other)
                .is_err()
        );
        assert_eq!(store.map.get(&key).unwrap().unwrap().0, replayed);
    }
    store
        .map
        .insert(key, super::SchemaControlBytes(before))
        .expect("fixture storage operation should succeed");
    store
        .apply_prepared_identity_checkpoint(prepared)
        .expect("fixture storage operation should succeed");
    drop(store);
    let reopened = super::LiveSchemaCheckpointStore::open(memory).unwrap();
    assert_eq!(reopened.map.get(&key).unwrap().unwrap().0, replayed);
    let after = reopened.load(STORE_PATH).unwrap().unwrap();
    assert_eq!(
        after.candidate().encoded_bundle(),
        initial.candidate().encoded_bundle()
    );
    for ((owner, old), (_, state)) in initial
        .identity_states()
        .iter()
        .zip(after.identity_states())
    {
        if let Some((_, advance)) = ranges
            .iter()
            .find(|(range, _)| range.owner() == old.owner())
        {
            assert_eq!(state.materialized_high_water(), 1);
            assert_eq!(state.last_applied_advance(), Some(*advance));
        } else {
            assert_eq!(after.identity_states().get(owner), Some(old));
        }
    }
}

#[test]
fn prepared_identity_checkpoint_stale_and_budget_refusals_leave_bytes_unchanged() {
    use crate::db::{query::preparation::PreparationWork, tests::support::request_with_limit};
    use icydb_diagnostic_code::{
        DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    };
    let mut store =
        super::LiveSchemaCheckpointStore::open(std::rc::Rc::new(crate::testing::test_memory(243)))
            .unwrap();
    let initial = checkpoint(false, IdentityStateLifecycle::Active);
    let ranges = identity_ranges(&initial);
    let key = LiveSchemaCheckpointKey::for_store(STORE_PATH).unwrap();
    let before = encode_checkpoint(STORE_PATH, &initial).unwrap();
    store
        .map
        .insert(key, super::SchemaControlBytes(before.clone()))
        .expect("fixture storage operation should succeed");
    let root = request_with_limit(Resource::TemporaryBytes, before.len() as u64 - 1);
    let error = PreparationWork::run_internal(&root.scope(), Lane::Mutation, |work| {
        store.prepare_identity_ranges(STORE_PATH, &ranges, work)
    })
    .unwrap_err();
    assert!(error.diagnostic_facts().contains(&(
        icydb_diagnostic_code::DiagnosticFactTag::BudgetResource,
        Resource::TemporaryBytes.raw()
    )));
    assert_eq!(store.map.get(&key).unwrap().unwrap().0, before);
    let root = request_with_limit(Resource::TemporaryBytes, before.len() as u64);
    let prepared = PreparationWork::run_internal(&root.scope(), Lane::Mutation, |work| {
        store.prepare_identity_ranges(STORE_PATH, &ranges, work)
    })
    .unwrap();
    assert_eq!(root.observed(Resource::TemporaryBytes), before.len() as u64);
    assert!(
        PreparationWork::run_internal(&root.scope(), Lane::Mutation, |work| {
            store.prepare_identity_ranges(STORE_PATH, &ranges, work)
        })
        .is_err()
    );
    assert_eq!(store.map.get(&key).unwrap().unwrap().0, before);
    // A later stale owner must not publish the already-valid earlier owner.
    store
        .apply_identity_range(STORE_PATH, ranges[1].0, ranges[1].1)
        .unwrap();
    let competing = store.map.get(&key).unwrap().unwrap().0;
    let error = crate::db::query::preparation::with_preparation_work(|work| {
        store.prepare_identity_ranges(STORE_PATH, &ranges, work)
    })
    .unwrap_err();
    assert_eq!(error.class(), ErrorClass::Conflict);
    assert_eq!(error.origin(), ErrorOrigin::Identity);
    assert_eq!(store.map.get(&key).unwrap().unwrap().0, competing);
    drop(prepared); // stale preparation is never applied outside its synchronous window.
}
