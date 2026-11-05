//! Test-only constructors and adapters for the parent module.

use super::*;

impl CommitStore {
    /// Encode one raw commit-control slot payload for recovery tests.
    pub(in crate::db::commit) fn encode_raw_control_slot_for_tests(
        marker_bytes: Vec<u8>,
    ) -> Result<Vec<u8>, InternalError> {
        encode_commit_control_slot(
            DatabaseIncarnationId::for_tests(0x31),
            [0x42; 32],
            &marker_bytes,
        )
    }

    /// Initialize one current-format database control store for direct tests.
    pub(super) fn init(memory: std::rc::Rc<RuntimeMemory<DefaultMemoryImpl>>) -> Self {
        initialize_current_database_control_for_tests(&memory);
        Self::open(memory).expect("test database control store should initialize")
    }

    /// Persist one commit marker while proving the current slot has no marker.
    pub(in crate::db::commit) fn set_if_empty(
        &self,
        marker: &CommitMarker,
    ) -> Result<EncodedCommitControlSlot, InternalError> {
        let prepared = self.prepare_set_if_empty(marker)?;
        self.publish_prepared_marker(prepared)
    }

    /// Clear the marker slot directly for tests that intentionally persist corruption.
    pub(in crate::db::commit) fn clear_raw_for_tests(&self) {
        let bytes = self
            .read_control_slot()
            .expect("test control should decode");
        let slot = inspect_commit_control_slot(&bytes).expect("test control should inspect");
        let encoded = encode_empty_commit_control_slot(
            slot.database_incarnation_id,
            slot.cursor_authentication_key,
            slot.database_commit_sequence,
            &slot.to_owned_registry(),
        )
        .expect("test empty control should encode");
        self.write_control_slot(&encoded)
            .expect("test database control slot should clear");
        mark_commit_marker_verified_absent();
    }

    /// Overwrite the raw marker bytes directly for recovery tests.
    pub(in crate::db::commit) fn set_raw_marker_bytes_for_tests(&self, bytes: Vec<u8>) {
        if bytes.is_empty() {
            mark_commit_marker_verified_absent();
        } else {
            mark_commit_marker_may_be_present();
        }

        let encoded = if bytes.is_empty() {
            let control = self
                .read_control_slot()
                .expect("test control should decode");
            let slot = inspect_commit_control_slot(&control).expect("test control should inspect");
            encode_empty_commit_control_slot(
                slot.database_incarnation_id,
                slot.cursor_authentication_key,
                slot.database_commit_sequence,
                &slot.to_owned_registry(),
            )
            .expect("test empty control should encode")
        } else {
            bytes
        };
        self.write_control_slot(&encoded)
            .expect("test raw commit marker bytes should fit control memory");
    }

    pub(super) fn raw_control_slot_bytes_for_tests(&self) -> Vec<u8> {
        self.read_framed_control_slot()
            .expect("test database control frame should decode")
    }
}

pub(in crate::db) fn validate_commit_marker_envelope_for_tests(
    bytes: &[u8],
) -> Result<(), InternalError> {
    RawCommitMarker(bytes.to_vec()).try_decode().map(drop)
}

/// Persist one valid database-control slot whose marker payload is supplied
/// directly by a recovery test.
pub(in crate::db) fn persist_raw_commit_marker_for_tests(
    marker_bytes: Vec<u8>,
) -> Result<(), InternalError> {
    let control_slot = CommitStore::encode_raw_control_slot_for_tests(marker_bytes)?;
    with_commit_store(|store| {
        store.set_raw_marker_bytes_for_tests(control_slot);
        Ok(())
    })
}

/// Measure one retained marker and its embedded journal batches without
/// changing stable state.
pub(in crate::db) fn retained_commit_marker_measurement_for_tests()
-> Result<Option<(usize, Vec<usize>)>, InternalError> {
    with_commit_store(|store| {
        let control_slot = store.read_control_slot()?;
        let marker_bytes = decode_commit_control_slot(&control_slot)?;
        let marker_bytes_len = marker_bytes.len();
        let Some(marker) = RawCommitMarker(marker_bytes).try_decode()? else {
            return Ok(None);
        };
        let journal_batch_bytes = marker
            .journal_batches()
            .iter()
            .map(journal_batch_encoded_len)
            .collect();
        Ok(Some((marker_bytes_len, journal_batch_bytes)))
    })
}

pub(in crate::db) fn initialize_current_commit_control_for_tests(
    memory: std::rc::Rc<RuntimeMemory<DefaultMemoryImpl>>,
) -> Result<(), InternalError> {
    let store = CommitStore {
        memory: memory.clone(),
    };
    if !store.control_slot_is_uninitialized() {
        store.read_control_slot()?;
        return Ok(());
    }
    let replacement = prepare_commit_control_replacement(
        memory,
        DatabaseIncarnationId::for_tests(0x31),
        [0x42; 32],
        0,
        &[],
    )?;
    apply_prepared_commit_control_replacement(replacement);
    Ok(())
}
