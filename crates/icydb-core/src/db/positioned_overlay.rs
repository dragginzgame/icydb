//! Module: db::positioned_overlay
//! Responsibility: provenance and retirement rules for journal live overlays.
//! Does not own: journal scheduling, canonical fold, admission, or persisted controls.
//! Boundary: data/index/schema stores own values while this module owns exact batch positions.

use crate::{
    db::{
        index::IndexEntryValue,
        journal::{JournalRecord, JournalSequence},
        registry::StoreAllocationIdentity,
    },
    error::InternalError,
};
use std::collections::{BTreeMap, BTreeSet};

/// One complete-batch position that owns a live overlay effect.
///
/// The logical target is the key in the family-local position map. Accepted
/// journal batches prohibit repeated logical targets where record order would
/// otherwise require a general record ordinal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct JournalOverlayPosition {
    store_allocation: StoreAllocationIdentity,
    journal_sequence: JournalSequence,
}

impl JournalOverlayPosition {
    /// Bind one live effect to its physical store and tail-local batch.
    #[must_use]
    pub(in crate::db) const fn new(
        store_allocation: StoreAllocationIdentity,
        journal_sequence: JournalSequence,
    ) -> Self {
        Self {
            store_allocation,
            journal_sequence,
        }
    }
}

/// Result of preflighting exact retirement for one logical target.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum PositionedOverlayRetirement {
    /// The selected batch still owns the target and its overlay may be removed.
    Exact,
    /// A later complete batch owns the target, so its overlay must remain.
    Superseded,
}

/// Family-local positions for values and tombstones owned by an existing store.
///
/// Values remain in the store's existing live map and tombstones remain in its
/// existing tombstone set. Keeping provenance separate avoids a second read
/// authority and lets every current-state consumer retain the established
/// live-before-canonical traversal.
pub(in crate::db) struct PositionedOverlayMetadata<K> {
    positions: BTreeMap<K, JournalOverlayPosition>,
}

impl<K> PositionedOverlayMetadata<K> {
    /// Build empty provenance for one overlay family.
    #[must_use]
    pub(in crate::db) const fn new() -> Self {
        Self {
            positions: BTreeMap::new(),
        }
    }

    /// Return the number of positioned logical targets for boundedness tests.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn len(&self) -> usize {
        self.positions.len()
    }

    /// Drop all volatile positions during startup projection reconstruction.
    pub(in crate::db) fn clear(&mut self) {
        self.positions.clear();
    }
}

impl<K: Ord> PositionedOverlayMetadata<K> {
    /// Prove that publishing this position preserves newest-batch authority.
    pub(in crate::db) fn preflight_publish(
        &self,
        key: &K,
        position: JournalOverlayPosition,
    ) -> Result<(), InternalError> {
        let Some(current) = self.positions.get(key).copied() else {
            return Ok(());
        };
        if current.store_allocation != position.store_allocation
            || current.journal_sequence > position.journal_sequence
        {
            return Err(InternalError::store_invariant());
        }
        Ok(())
    }

    /// Preflight sorted, distinct publication targets without publishing any position.
    /// Stores retain their own typed publication receipts and value application.
    pub(in crate::db) fn preflight_publication_keys(
        &self,
        keys: impl IntoIterator<Item = K>,
        position: JournalOverlayPosition,
    ) -> Result<Vec<K>, InternalError> {
        let keys = keys.into_iter().collect::<BTreeSet<_>>();
        for key in &keys {
            self.preflight_publish(key, position)?;
        }
        Ok(keys.into_iter().collect())
    }

    /// Publish one position after `preflight_publish` has succeeded.
    pub(in crate::db) fn publish_preflighted(&mut self, key: K, position: JournalOverlayPosition) {
        self.positions.insert(key, position);
    }

    /// Prove whether the selected batch may remove this exact overlay target.
    pub(in crate::db) fn preflight_retirement(
        &self,
        key: &K,
        position: JournalOverlayPosition,
    ) -> Result<PositionedOverlayRetirement, InternalError> {
        let current = self
            .positions
            .get(key)
            .copied()
            .ok_or_else(InternalError::store_invariant)?;
        if current.store_allocation != position.store_allocation
            || current.journal_sequence < position.journal_sequence
        {
            return Err(InternalError::store_invariant());
        }
        if current == position {
            Ok(PositionedOverlayRetirement::Exact)
        } else {
            Ok(PositionedOverlayRetirement::Superseded)
        }
    }

    /// Preflight sorted, distinct exact/superseded targets before canonical mutation.
    /// A missing or invalid target refuses the complete preparation without effects.
    pub(in crate::db) fn preflight_retirements(
        &self,
        keys: impl IntoIterator<Item = K>,
        position: JournalOverlayPosition,
    ) -> Result<Vec<(K, PositionedOverlayRetirement)>, InternalError> {
        keys.into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|key| {
                self.preflight_retirement(&key, position)
                    .map(|retirement| (key, retirement))
            })
            .collect()
    }

    /// Return whether one logical target has live overlay provenance.
    #[must_use]
    pub(in crate::db) fn is_positioned(&self, key: &K) -> bool {
        self.positions.contains_key(key)
    }

    /// Remove exact live values, tombstones and provenance after retirement preflight.
    /// Superseded entries retain all newer overlay authority.
    pub(in crate::db) fn retire_overlays_preflighted<V>(
        &mut self,
        entries: impl IntoIterator<Item = (K, PositionedOverlayRetirement)>,
        live: &mut BTreeMap<K, V>,
        tombstones: &mut BTreeSet<K>,
    ) {
        for (key, retirement) in entries {
            if retirement == PositionedOverlayRetirement::Exact {
                live.remove(&key);
                tombstones.remove(&key);
                self.retire_preflighted(&key, retirement);
            }
        }
    }

    /// Remove an exact position after retirement preflight has succeeded.
    pub(in crate::db) fn retire_preflighted(
        &mut self,
        key: &K,
        retirement: PositionedOverlayRetirement,
    ) {
        if retirement == PositionedOverlayRetirement::Exact {
            self.positions.remove(key);
        }
    }
}

/// Exhaustive online decision for one maintained effect family.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum OnlineOverlayDecision {
    DataPositive,
    DataTombstone,
    IndexPositive,
    IndexTombstone,
    SchemaPositive,
    SchemaTombstone,
}

/// Classify every maintained journal record without a fallback decision.
#[must_use]
pub(in crate::db) const fn classify_journal_overlay(
    record: &JournalRecord,
) -> OnlineOverlayDecision {
    match record {
        JournalRecord::RowPut { .. } => OnlineOverlayDecision::DataPositive,
        JournalRecord::RowDelete { .. } => OnlineOverlayDecision::DataTombstone,
        JournalRecord::AcceptedSchemaPublish { .. } => OnlineOverlayDecision::SchemaPositive,
        JournalRecord::AcceptedSchemaIndexDelete { .. } => OnlineOverlayDecision::IndexTombstone,
        JournalRecord::AcceptedSchemaIndexPut { .. } => OnlineOverlayDecision::IndexPositive,
        JournalRecord::ConstraintValidationJobPut { .. } => OnlineOverlayDecision::SchemaPositive,
        JournalRecord::ConstraintValidationJobDelete { .. } => {
            OnlineOverlayDecision::SchemaTombstone
        }
        JournalRecord::ConstraintValidationIndexPut { .. } => OnlineOverlayDecision::IndexPositive,
        JournalRecord::IdentityRangeAdvance { .. } => OnlineOverlayDecision::SchemaPositive,
        #[cfg(any(test, feature = "migration"))]
        JournalRecord::SchemaMigrationRowPut { .. } => OnlineOverlayDecision::DataPositive,
        #[cfg(any(test, feature = "migration"))]
        JournalRecord::SchemaMigrationIndexPut { .. } => OnlineOverlayDecision::IndexPositive,
    }
}

/// Classify the maintained derived-index effect representation exhaustively.
#[must_use]
pub(in crate::db) const fn classify_derived_index_overlay(
    value: Option<&IndexEntryValue>,
) -> OnlineOverlayDecision {
    match value {
        Some(_) => OnlineOverlayDecision::IndexPositive,
        None => OnlineOverlayDecision::IndexTombstone,
    }
}

#[cfg(test)]
mod tests;
