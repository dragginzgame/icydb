//! Test-only constructors and adapters for the parent module.

use super::*;

impl JournalInspectionLimits {
    #[must_use]
    pub(in crate::db) const fn for_tests(decoded_batches: usize, decoded_bytes: usize) -> Self {
        Self {
            decoded_batches,
            decoded_bytes,
        }
    }
}

impl PreparedEntityMutationRevision {
    /// Return the target entity bound into this preparation.
    #[must_use]
    pub(in crate::db) const fn entity_tag(self) -> crate::types::EntityTag {
        self.entity_tag
    }

    /// Return the exact journal sequence reserved for the prepared row batch.
    #[must_use]
    pub(in crate::db) const fn journal_sequence(self) -> JournalSequence {
        self.journal_sequence
    }
}

impl JournalTailStore {
    /// Initialize the exact current-format tail control on an empty tail.
    pub(in crate::db) fn initialize_current_tail_control(&mut self) -> Result<(), InternalError> {
        if !self.preflight_current_tail_control_initialization()? {
            return Ok(());
        }
        self.apply_current_tail_control_initialization();
        Ok(())
    }

    /// Return whether current-format exact tail authority is present.
    pub(in crate::db) fn has_current_tail_control(&self) -> bool {
        self.map
            .contains_key(&JournalTailKey::tail_convergence_control())
    }

    /// Persist a new durable fold watermark.
    ///
    /// Watermarks may advance or be rewritten idempotently, but they never
    /// move backward. The journal tail itself is the replay-boundary authority;
    /// no extra stable memory ID is required.
    pub(in crate::db) fn persist_fold_watermark(
        &mut self,
        watermark: FoldWatermark,
    ) -> Result<(), InternalError> {
        let current = self.fold_watermark()?;
        if watermark.highest_folded_journal_sequence() < current.highest_folded_journal_sequence()
            || (watermark.highest_folded_journal_sequence()
                == current.highest_folded_journal_sequence()
                && watermark.fold_epoch() < current.fold_epoch())
        {
            return Err(journal_tail_corruption());
        }

        self.map.insert(
            JournalTailKey::fold_watermark(),
            RawJournalChunk::from_bytes(encode_fold_watermark(watermark)),
        );

        Ok(())
    }

    /// Remove folded journal batches through the provided sequence.
    ///
    /// The persisted fold watermark remains authoritative if cleanup is
    /// interrupted after the watermark is advanced.
    pub(in crate::db) fn clear_batches_through(&mut self, watermark: JournalSequence) {
        if watermark == FOLD_WATERMARK_CONTROL_SEQUENCE {
            return;
        }

        let keys = self
            .map
            .range((
                Included(JournalTailKey::new(JournalSequence::new(1), 0)),
                Included(JournalTailKey::new(watermark, u32::MAX)),
            ))
            .map(|entry| *entry.key())
            .collect::<Vec<_>>();
        for key in keys {
            let _ = self.map.remove(&key);
        }
    }

    /// Visit complete batches after the durable fold watermark in replay order.
    ///
    /// This read boundary validates the first journal-tail invariants needed by
    /// recovery: encoded sequence must match physical key, sequences above the
    /// watermark are contiguous, and batch IDs do not repeat across sequences.
    pub(in crate::db) fn visit_batches_after(
        &self,
        watermark: JournalSequence,
        mut visitor: impl FnMut(&JournalBatch) -> Result<(), InternalError>,
    ) -> Result<(), InternalError> {
        let mut expected = watermark.next();
        let mut seen_batch_ids = BTreeSet::new();

        loop {
            let expected_sequence = expected.ok_or_else(journal_tail_corruption)?;
            let Some(entry) = self
                .map
                .range((
                    Included(JournalTailKey::new(expected_sequence, 0)),
                    Unbounded,
                ))
                .next()
            else {
                break;
            };
            if entry.key().sequence != expected_sequence {
                return Err(journal_tail_corruption());
            }

            let batch = RawJournalBatch::from_control_bytes(
                self.raw_batch_bytes_for_sequence(expected_sequence)?
                    .ok_or_else(journal_tail_corruption)?,
            )
            .decode()?;
            if batch.journal_sequence() != expected_sequence {
                return Err(journal_tail_corruption());
            }
            if !seen_batch_ids.insert(batch.batch_id()) {
                return Err(journal_tail_corruption());
            }

            visitor(&batch)?;
            expected = expected_sequence.next();
        }

        Ok(())
    }

    /// Return the number of complete journal-tail batches.
    #[must_use]
    pub(in crate::db) fn len(&self) -> u64 {
        self.map
            .iter()
            .filter_map(|entry| {
                let sequence = entry.key().sequence;
                (sequence != FOLD_WATERMARK_CONTROL_SEQUENCE).then_some(sequence)
            })
            .collect::<BTreeSet<_>>()
            .len() as u64
    }

    /// Return whether the journal tail is currently empty.
    #[must_use]
    pub(in crate::db) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Remove one retained entity entry while leaving its completion marker.
    pub(in crate::db) fn remove_entity_mutation_revision_for_tests(
        &mut self,
        entity_tag: EntityTag,
    ) -> Result<(), InternalError> {
        let authority = self.entity_mutation_revision_authority()?;
        let ordinal = authority
            .entries
            .binary_search_by_key(&entity_tag, |entry| entry.entity_tag)
            .map_err(|_| journal_tail_corruption())?;
        let chunk_ordinal = ordinal / MAX_ENTITY_MUTATION_REVISIONS_PER_CHUNK;
        let chunk_offset = ordinal % MAX_ENTITY_MUTATION_REVISIONS_PER_CHUNK;
        let chunk_ordinal = u32::try_from(chunk_ordinal).map_err(|_| journal_tail_corruption())?;
        let key = JournalTailKey::entity_mutation_revision_data_chunk(chunk_ordinal)?;
        let mut entries = self
            .map
            .get(&key)
            .ok_or_else(journal_tail_corruption)
            .and_then(|raw| decode_entity_mutation_revision_chunk(raw.as_bytes()))?;
        entries.remove(chunk_offset);
        self.map.insert(
            key,
            RawJournalChunk::from_bytes(encode_entity_mutation_revision_chunk(entries.as_slice())?),
        );
        Ok(())
    }

    /// Insert raw journal-tail bytes for persisted-corruption tests.
    pub(in crate::db) fn insert_raw_batch_for_tests(
        &mut self,
        sequence: JournalSequence,
        bytes: &[u8],
    ) -> Result<(), InternalError> {
        self.append_raw_batch(sequence, bytes)
    }

    pub(super) fn append_raw_batch(
        &mut self,
        sequence: JournalSequence,
        bytes: &[u8],
    ) -> Result<(), InternalError> {
        if sequence == FOLD_WATERMARK_CONTROL_SEQUENCE {
            return Err(journal_tail_corruption());
        }
        if bytes.is_empty() || bytes.len() > MAX_JOURNAL_BATCH_BYTES as usize {
            return Err(journal_tail_corruption());
        }

        if let Some(existing) = self.raw_batch_prefix_bytes_for_sequence(sequence)? {
            if existing == bytes {
                return Ok(());
            }
            if existing.len() > bytes.len() || !bytes.starts_with(existing.as_slice()) {
                return Err(journal_tail_corruption());
            }
        }

        for (chunk_index, chunk) in bytes.chunks(JOURNAL_TAIL_CHUNK_BYTES as usize).enumerate() {
            let chunk_index = u32::try_from(chunk_index).map_err(|_| journal_tail_corruption())?;
            let key = JournalTailKey::new(sequence, chunk_index);
            if self.map.contains_key(&key) {
                continue;
            }
            self.map
                .insert(key, RawJournalChunk::from_bytes(chunk.to_vec()));
        }

        Ok(())
    }
}

pub(super) fn current_entity_tags_for_test_batch(
    batch: &JournalBatch,
) -> Result<Vec<EntityTag>, InternalError> {
    if let Some(tags) = batch.published_entity_tags() {
        return Ok(tags.to_vec());
    }
    Ok(batch_row_mutation_entity_tags(batch)?.into_iter().collect())
}
