//! Current control encoding, borrowed inspection and registry rejection contracts.

use super::*;
use crate::error::ErrorClass;

fn registry(count: usize) -> Vec<PersistedStoreAllocation> {
    (0..count)
        .map(|entry| PersistedStoreAllocation {
            state: if entry % 2 == 0 {
                PersistedStoreAllocationState::Active
            } else {
                PersistedStoreAllocationState::Retired
            },
            roles: std::array::from_fn(|role| PersistedStoreAllocationIdentity {
                memory_id: u8::try_from(entry * STORE_ALLOCATION_ROLES + role).unwrap(),
                stable_key: format!("store.s{entry}.r{role}.v1"),
            }),
        })
        .collect()
}

// Independent current-wire fixture also permits malformed registry inputs.
fn wire_registry(registry: &[PersistedStoreAllocation], marker: &[u8]) -> Vec<u8> {
    let mut bytes = COMMIT_CONTROL_MAGIC.to_vec();
    bytes.push(COMMIT_CONTROL_STATE_VERSION_CURRENT);
    bytes.extend_from_slice(&[0x31; DATABASE_INCARNATION_BYTES]);
    bytes.extend_from_slice(&[0x42; CURSOR_AUTHENTICATION_KEY_BYTES]);
    bytes.extend_from_slice(&7_u64.to_le_bytes());
    bytes.push(u8::try_from(registry.len()).unwrap());
    for entry in registry {
        bytes.push(match entry.state {
            PersistedStoreAllocationState::Active => 1,
            PersistedStoreAllocationState::Retired => 2,
        });
        for role in &entry.roles {
            bytes.push(role.memory_id);
            bytes.push(u8::try_from(role.stable_key.len()).unwrap());
            bytes.extend_from_slice(role.stable_key.as_bytes());
        }
    }
    bytes.extend_from_slice(&u32::try_from(marker.len()).unwrap().to_le_bytes());
    bytes.extend_from_slice(marker);
    bytes
}

fn assert_corrupt_control(bytes: &[u8]) {
    for result in [
        inspect_commit_control_slot(bytes).map(|_| ()),
        inspect_commit_control_header(bytes).map(|_| ()),
        commit_control_slot_encoded_len(bytes).map(|_| ()),
    ] {
        assert_eq!(result.unwrap_err().class, ErrorClass::Corruption);
    }
}

#[test]
fn borrowed_control_preserves_empty_and_maximal_registry_bytes_and_owned_output() {
    for count in [0, MAX_PERSISTED_STORE_ALLOCATIONS] {
        let expected = registry(count);
        let bytes = wire_registry(&expected, &[]);
        let slot = inspect_commit_control_slot(&bytes).unwrap();
        assert_eq!(slot.to_owned_registry(), expected);
        assert_eq!(slot.database_incarnation_id.to_bytes(), [0x31; 16]);
        assert_eq!(slot.cursor_authentication_key, [0x42; 32]);
        assert_eq!(slot.database_commit_sequence, 7);
        let reencoded = encode_empty_commit_control_slot(
            slot.database_incarnation_id,
            slot.cursor_authentication_key,
            slot.database_commit_sequence,
            &slot.to_owned_registry(),
        )
        .unwrap();
        assert_eq!(reencoded, bytes);
        // The inspection view's names must borrow the supplied bytes, while the
        // explicitly materialized output can survive dropping that payload.
        for entry in &slot.registry {
            for (_, name) in entry.roles {
                assert!(bytes.as_ptr_range().contains(&name.as_ptr()));
            }
        }
        let owned = slot.to_owned_registry();
        drop(slot);
        drop(bytes);
        assert_eq!(owned, expected);
    }
}

#[test]
fn control_encoder_and_decoder_share_registry_rejections() {
    let valid = registry(2);
    let mut bad_id = valid.clone();
    bad_id[1].roles[3].memory_id = bad_id[0].roles[0].memory_id;
    let mut bad_key = valid.clone();
    bad_key[1].roles[3].stable_key = bad_key[0].roles[0].stable_key.clone();
    let mut bad_order = valid.clone();
    bad_order.swap(0, 1);
    let mut bad_grammar = valid.clone();
    bad_grammar[1].roles[3].stable_key = "INVALID.v1".to_string();
    let mut long_key = valid;
    long_key[1].roles[3].stable_key = format!("{}.v1", "a".repeat(MAX_STABLE_KEY_BYTES));
    for invalid in [bad_id, bad_key, bad_order, bad_grammar, long_key] {
        assert_corrupt_control(&wire_registry(&invalid, &[]));
        let error = encode_empty_commit_control_slot(
            DatabaseIncarnationId::try_from_bytes([0x31; 16]).unwrap(),
            [0x42; 32],
            7,
            &invalid,
        )
        .unwrap_err();
        assert_eq!(error.class, ErrorClass::Corruption);
    }
}

#[test]
fn borrowed_control_rejects_malformed_registry_before_returning_metadata() {
    let bytes = wire_registry(&registry(1), &[]);
    for end in 0..bytes.len() {
        assert!(inspect_commit_control_slot(&bytes[..end]).is_err());
        assert!(inspect_commit_control_header(&bytes[..end]).is_err());
        assert!(commit_control_slot_encoded_len(&bytes[..end]).is_err());
    }
    let mut bad_count = bytes.clone();
    bad_count[CURRENT_CONTROL_PREFIX_BYTES - 1] = 17;
    assert_corrupt_control(&bad_count);
    let mut bad_state = bytes.clone();
    bad_state[CURRENT_CONTROL_PREFIX_BYTES] = 0;
    assert_corrupt_control(&bad_state);
    let mut bad_utf8 = bytes;
    bad_utf8[CURRENT_CONTROL_PREFIX_BYTES + 3] = 0xff;
    assert_corrupt_control(&bad_utf8);
}

#[test]
fn control_header_accepts_bounded_prefix_but_full_inspection_requires_exact_payload() {
    let bytes = wire_registry(&registry(2), &[1, 2, 3, 4]);
    let prefix = &bytes[..bytes.len() - 4];
    let header = inspect_commit_control_header(prefix).unwrap();
    assert_eq!(header.header_len, prefix.len());
    assert_eq!(header.marker_len, 4);
    assert_eq!(
        commit_control_slot_encoded_len(prefix).unwrap(),
        bytes.len()
    );
    assert!(inspect_commit_control_slot(prefix).is_err());
    assert_eq!(
        inspect_commit_control_slot(&bytes).unwrap().marker_bytes,
        &[1, 2, 3, 4]
    );
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(inspect_commit_control_slot(&trailing).is_err());
    let mut excessive = prefix.to_vec();
    let offset = excessive.len() - COMMIT_MARKER_LENGTH_BYTES;
    excessive[offset..].copy_from_slice(&(MAX_COMMIT_BYTES + 1).to_le_bytes());
    assert!(inspect_commit_control_header(&excessive).is_err());
    assert!(inspect_commit_control_slot(&excessive).is_err());
}
