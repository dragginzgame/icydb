//! Module: db::schema::cardinality_build
//! Responsibility: bounded canonical construction and lifecycle of one cardinality generation.
//! Does not own: incremental maintenance, planner consumption, or timer scheduling.
//! Boundary: exact source authority -> isolated pages -> one complete Ready publication.

use crate::{
    MAX_INDEX_FIELDS,
    db::{
        data::{DataStore, DecodedDataStoreKey},
        index::{IndexEntryExistenceWitness, IndexId, IndexKey, IndexKeyKind, IndexStore},
        integrity::DatabaseIncarnationId,
        journal::FoldWatermark,
        registry::StoreAllocationIdentities,
        schema::{
            SchemaStore,
            cardinality_generation::{
                CardinalityAcceptedRootIdentity, CardinalityBuildCheckpoint,
                CardinalityBuildCursor, CardinalityBuildPhase, CardinalityBuildTotals,
                CardinalityCountDigest, CardinalityCountSlot, CardinalityGenerationHeader,
                CardinalityGenerationId, CardinalityGenerationState, CardinalityLogicalCountKey,
                CardinalitySourceIdentity, CardinalitySourceMismatch,
            },
        },
    },
    error::InternalError,
    types::EntityTag,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
};

pub(in crate::db) const MAX_CARDINALITY_BUILD_SOURCE_ENTRIES_PER_PAGE: u64 = 4_096;
pub(in crate::db) const MAX_CARDINALITY_BUILD_SOURCE_BYTES_PER_PAGE: u64 = 16_777_216;
pub(in crate::db) const MAX_CARDINALITY_BUILD_PREFIX_UPDATES_PER_PAGE: u64 = 16_384;
const MAX_CARDINALITY_SLOT_CLEAR_ENTRIES_PER_PAGE: usize = 4_096;

/// Immutable accepted domain and exact canonical source identity for one build pass.
#[derive(Clone)]
pub(in crate::db) struct CardinalityBuildAuthority {
    source: CardinalitySourceIdentity,
    accepted_entities: BTreeSet<EntityTag>,
    accepted_indexes: BTreeMap<IndexId, usize>,
}

impl CardinalityBuildAuthority {
    /// Derive the build domain exclusively from canonical accepted schema authority.
    pub(in crate::db) fn derive(
        schema: &SchemaStore,
        database_incarnation: DatabaseIncarnationId,
        allocations: StoreAllocationIdentities,
        fold_watermark: FoldWatermark,
    ) -> Result<Self, InternalError> {
        let authority = schema.current_canonical_accepted_schema_authority()?;
        let (accepted_root, accepted_entities, accepted_indexes) = match authority {
            None => (None, BTreeSet::new(), BTreeMap::new()),
            Some((selection, bundle)) => {
                let root = selection.root();
                let accepted_root = Some(CardinalityAcceptedRootIdentity::new(
                    root.revision(),
                    root.fingerprint(),
                )?);
                let mut accepted_entities = BTreeSet::new();
                let mut accepted_indexes = BTreeMap::new();
                for (entity, snapshot) in bundle.entity_snapshots() {
                    if !accepted_entities.insert(*entity) {
                        return Err(InternalError::store_corruption());
                    }
                    for index in snapshot.indexes() {
                        let component_count = index.key().component_count();
                        if component_count == 0 || component_count > MAX_INDEX_FIELDS {
                            return Err(InternalError::store_corruption());
                        }
                        let index_id = IndexId::new_with_generation(
                            *entity,
                            index.ordinal(),
                            index.physical_generation(),
                        );
                        if accepted_indexes.insert(index_id, component_count).is_some() {
                            return Err(InternalError::store_corruption());
                        }
                    }
                }
                (accepted_root, accepted_entities, accepted_indexes)
            }
        };
        let source = CardinalitySourceIdentity::derive(
            database_incarnation,
            allocations,
            accepted_root,
            accepted_indexes.keys().copied(),
            fold_watermark,
        )?;
        Ok(Self {
            source,
            accepted_entities,
            accepted_indexes,
        })
    }

    #[must_use]
    pub(in crate::db) const fn source(&self) -> CardinalitySourceIdentity {
        self.source
    }

    #[must_use]
    pub(in crate::db) fn accepts_entity(&self, entity: EntityTag) -> bool {
        self.accepted_entities.contains(&entity)
    }

    #[must_use]
    pub(in crate::db) fn accepted_index_component_count(&self, index: IndexId) -> Option<usize> {
        self.accepted_indexes.get(&index).copied()
    }

    #[cfg(test)]
    const fn from_parts(
        source: CardinalitySourceIdentity,
        accepted_entities: BTreeSet<EntityTag>,
        accepted_indexes: BTreeMap<IndexId, usize>,
    ) -> Self {
        Self {
            source,
            accepted_entities,
            accepted_indexes,
        }
    }
}

/// Unforgeable result of observing complete exhaustion of both canonical domains.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct CardinalityReadyCandidate {
    header: CardinalityGenerationHeader,
    cursor: CardinalityBuildCursor,
}

impl CardinalityReadyCandidate {
    #[must_use]
    pub(in crate::db) const fn header(&self) -> CardinalityGenerationHeader {
        self.header
    }

    #[must_use]
    pub(in crate::db) const fn cursor(&self) -> &CardinalityBuildCursor {
        &self.cursor
    }
}

/// Unforgeable proof that both canonical source domains were observed empty.
pub(in crate::db) struct EmptyCardinalityReadyCandidate {
    source: CardinalitySourceIdentity,
}

impl EmptyCardinalityReadyCandidate {
    #[must_use]
    pub(in crate::db) const fn source(&self) -> CardinalitySourceIdentity {
        self.source
    }
}

/// Result of one bounded builder invocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum CardinalityBuildPageOutcome {
    Clearing {
        generation: CardinalityGenerationId,
        slot: CardinalityCountSlot,
        has_more: bool,
    },
    Advanced {
        generation: CardinalityGenerationId,
        slot: CardinalityCountSlot,
        phase: CardinalityBuildPhase,
        totals: CardinalityBuildTotals,
    },
    CandidateComplete {
        generation: CardinalityGenerationId,
        slot: CardinalityCountSlot,
        totals: CardinalityBuildTotals,
        candidate: Box<CardinalityReadyCandidate>,
    },
    SourceChanged(CardinalitySourceMismatch),
    AlreadyReady,
}

/// One bounded lifecycle decision owned by the existing replicated driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum CardinalityGenerationPageOutcome {
    /// This store already has exact current Ready evidence.
    Quiescent,
    /// One clear, scan, or source-restart transition completed.
    WorkRemaining,
    /// One complete generation became Ready atomically.
    PublishedReady,
}

/// Advance one store-local generation through restart, construction, or publication.
///
/// The authority callback is evaluated both before and after every mutating
/// build page. A changed source switches immediately to the other isolated
/// slot and never merges counts accumulated under the stale source.
pub(in crate::db) fn drive_cardinality_generation_page(
    data: &DataStore,
    index: &IndexStore,
    schema: &mut SchemaStore,
    mut derive_authority: impl FnMut(&SchemaStore) -> Result<CardinalityBuildAuthority, InternalError>,
) -> Result<CardinalityGenerationPageOutcome, InternalError> {
    let authority = derive_authority(schema)?;
    if let Some(header) = schema.cardinality_generation_header()? {
        if header.validate_source(authority.source()).is_err() {
            schema.restart_cardinality_generation(header, authority.source())?;
            return Ok(CardinalityGenerationPageOutcome::WorkRemaining);
        }
        if header.state() == CardinalityGenerationState::Ready {
            if schema.cardinality_build_cursor()?.is_some() {
                return Err(InternalError::store_corruption());
            }
            return Ok(CardinalityGenerationPageOutcome::Quiescent);
        }
    } else if data.canonical_is_empty()? && index.canonical_is_empty()? {
        let current = derive_authority(schema)?;
        let empty = EmptyCardinalityReadyCandidate {
            source: current.source(),
        };
        schema.publish_empty_cardinality_generation(&empty)?;
        return Ok(CardinalityGenerationPageOutcome::PublishedReady);
    }

    let outcome = advance_cardinality_build_page(data, index, schema, &authority)?;
    let current = derive_authority(schema)?;
    if current.source() != authority.source() {
        let header = schema
            .cardinality_generation_header()?
            .ok_or_else(InternalError::store_corruption)?;
        schema.restart_cardinality_generation(header, current.source())?;
        return Ok(CardinalityGenerationPageOutcome::WorkRemaining);
    }

    match outcome {
        CardinalityBuildPageOutcome::CandidateComplete { candidate, .. } => {
            schema.publish_ready_cardinality_generation(&candidate, current.source())?;
            Ok(CardinalityGenerationPageOutcome::PublishedReady)
        }
        CardinalityBuildPageOutcome::Clearing { .. }
        | CardinalityBuildPageOutcome::Advanced { .. } => {
            Ok(CardinalityGenerationPageOutcome::WorkRemaining)
        }
        CardinalityBuildPageOutcome::SourceChanged(_) => {
            let header = schema
                .cardinality_generation_header()?
                .ok_or_else(InternalError::store_corruption)?;
            schema.restart_cardinality_generation(header, current.source())?;
            Ok(CardinalityGenerationPageOutcome::WorkRemaining)
        }
        CardinalityBuildPageOutcome::AlreadyReady => {
            Ok(CardinalityGenerationPageOutcome::Quiescent)
        }
    }
}

/// Advance at most one bounded clearing or canonical scan page.
pub(in crate::db) fn advance_cardinality_build_page(
    data: &DataStore,
    index: &IndexStore,
    schema: &mut SchemaStore,
    authority: &CardinalityBuildAuthority,
) -> Result<CardinalityBuildPageOutcome, InternalError> {
    let header = if let Some(header) = schema.cardinality_generation_header()? {
        header
    } else {
        if !schema.cardinality_storage_is_pristine()? {
            return Err(InternalError::store_corruption());
        }
        let header = CardinalityGenerationHeader::new(
            CardinalityGenerationId::INITIAL,
            CardinalityGenerationState::Building,
            CardinalityCountSlot::A,
            authority.source,
        );
        schema.write_cardinality_generation_header(header)?;
        header
    };
    if let Err(mismatch) = header.validate_source(authority.source) {
        return Ok(CardinalityBuildPageOutcome::SourceChanged(mismatch));
    }
    if header.state() == CardinalityGenerationState::Ready {
        return Ok(CardinalityBuildPageOutcome::AlreadyReady);
    }

    let Some(cursor) = schema.cardinality_build_cursor()? else {
        let initial_cursor = CardinalityBuildCursor::new(
            header.generation(),
            header.slot(),
            header.source(),
            CardinalityBuildPhase::Rows,
            None,
            CardinalityBuildTotals::default(),
        )?;
        let has_more = schema.clear_cardinality_count_slot_page(
            header,
            &initial_cursor,
            MAX_CARDINALITY_SLOT_CLEAR_ENTRIES_PER_PAGE,
        )?;
        return Ok(CardinalityBuildPageOutcome::Clearing {
            generation: header.generation(),
            slot: header.slot(),
            has_more,
        });
    };
    cursor.validate_header(header)?;

    let page = match cursor.phase() {
        CardinalityBuildPhase::Rows => collect_row_page(data, &cursor, authority)?,
        CardinalityBuildPhase::Indexes => collect_index_page(index, &cursor, authority)?,
    };
    let increments = coalesce_count_increments(page.count_digests)?;
    let prepared_counts = schema.prepare_cardinality_count_increments(
        header.slot(),
        header.generation(),
        &increments,
    )?;
    let totals = cursor.totals().checked_add_page(
        page.source_entries,
        page.source_bytes,
        page.prefix_updates,
        prepared_counts.new_count_keys(),
    )?;
    let next_cursor = CardinalityBuildCursor::new(
        header.generation(),
        header.slot(),
        header.source(),
        page.next_phase,
        page.next_checkpoint,
        totals,
    )?;
    let prepared =
        schema.prepare_cardinality_build_page(header, &cursor, prepared_counts, &next_cursor)?;
    let ready_candidate = page.candidate_complete.then(|| {
        Box::new(CardinalityReadyCandidate {
            header,
            cursor: next_cursor.clone(),
        })
    });
    schema.apply_prepared_cardinality_build_page(prepared)?;

    if let Some(candidate) = ready_candidate {
        Ok(CardinalityBuildPageOutcome::CandidateComplete {
            generation: header.generation(),
            slot: header.slot(),
            totals,
            candidate,
        })
    } else {
        Ok(CardinalityBuildPageOutcome::Advanced {
            generation: header.generation(),
            slot: header.slot(),
            phase: next_cursor.phase(),
            totals,
        })
    }
}

struct CollectedBuildPage {
    source_entries: u64,
    source_bytes: u64,
    prefix_updates: u64,
    count_digests: Vec<CardinalityCountDigest>,
    next_phase: CardinalityBuildPhase,
    next_checkpoint: Option<CardinalityBuildCheckpoint>,
    candidate_complete: bool,
}

#[derive(Default)]
struct BuildPageBudget {
    source_entries: u64,
    source_bytes: u64,
    prefix_updates: u64,
}

impl BuildPageBudget {
    fn admit(&mut self, source_bytes: u64, prefix_updates: u64) -> Result<bool, InternalError> {
        let next_entries = self
            .source_entries
            .checked_add(1)
            .ok_or_else(InternalError::store_unsupported)?;
        let next_bytes = self
            .source_bytes
            .checked_add(source_bytes)
            .ok_or_else(InternalError::store_unsupported)?;
        let next_prefix_updates = self
            .prefix_updates
            .checked_add(prefix_updates)
            .ok_or_else(InternalError::store_unsupported)?;
        if next_entries > MAX_CARDINALITY_BUILD_SOURCE_ENTRIES_PER_PAGE
            || next_bytes > MAX_CARDINALITY_BUILD_SOURCE_BYTES_PER_PAGE
            || next_prefix_updates > MAX_CARDINALITY_BUILD_PREFIX_UPDATES_PER_PAGE
        {
            return Ok(false);
        }
        self.source_entries = next_entries;
        self.source_bytes = next_bytes;
        self.prefix_updates = next_prefix_updates;
        Ok(true)
    }
}

fn collect_row_page(
    data: &DataStore,
    cursor: &CardinalityBuildCursor,
    authority: &CardinalityBuildAuthority,
) -> Result<CollectedBuildPage, InternalError> {
    let checkpoint = match cursor.checkpoint() {
        None => None,
        Some(CardinalityBuildCheckpoint::Row(key)) => {
            DecodedDataStoreKey::try_from_raw(key)
                .map_err(|_| InternalError::store_corruption())?;
            Some(key)
        }
        Some(CardinalityBuildCheckpoint::Index(_)) => {
            return Err(InternalError::store_corruption());
        }
    };
    let mut budget = BuildPageBudget::default();
    let mut count_digests = Vec::new();
    let maximum_entries = usize::try_from(MAX_CARDINALITY_BUILD_SOURCE_ENTRIES_PER_PAGE)
        .map_err(|_| InternalError::store_unsupported())?;
    count_digests
        .try_reserve_exact(maximum_entries)
        .map_err(|_| InternalError::store_unsupported())?;
    let mut last_key = None;
    let mut has_more = false;
    data.visit_canonical_entries_after(checkpoint, |key, row| {
        let decoded = DecodedDataStoreKey::try_from_raw(key)
            .map_err(|_| InternalError::store_corruption())?;
        if !authority.accepted_entities.contains(&decoded.entity_tag()) {
            return Err(InternalError::store_corruption());
        }
        let source_bytes = key
            .as_bytes()
            .len()
            .checked_add(row.len())
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(InternalError::store_unsupported)?;
        if !budget.admit(source_bytes, 0)? {
            has_more = true;
            return Ok(true);
        }
        count_digests.push(CardinalityLogicalCountKey::Entity(decoded.entity_tag()).digest()?);
        last_key = Some(key.clone());
        Ok(false)
    })?;
    if has_more && last_key.is_none() {
        return Err(InternalError::store_unsupported());
    }
    Ok(CollectedBuildPage {
        source_entries: budget.source_entries,
        source_bytes: budget.source_bytes,
        prefix_updates: 0,
        count_digests,
        next_phase: if has_more {
            CardinalityBuildPhase::Rows
        } else {
            CardinalityBuildPhase::Indexes
        },
        next_checkpoint: if has_more {
            last_key.map(CardinalityBuildCheckpoint::Row)
        } else {
            None
        },
        candidate_complete: false,
    })
}

fn collect_index_page(
    index: &IndexStore,
    cursor: &CardinalityBuildCursor,
    authority: &CardinalityBuildAuthority,
) -> Result<CollectedBuildPage, InternalError> {
    let checkpoint = match cursor.checkpoint() {
        None => None,
        Some(CardinalityBuildCheckpoint::Index(key)) => {
            IndexKey::try_from_raw(key).map_err(|_| InternalError::store_corruption())?;
            Some(key)
        }
        Some(CardinalityBuildCheckpoint::Row(_)) => {
            return Err(InternalError::store_corruption());
        }
    };
    let lower = checkpoint
        .cloned()
        .map_or(Bound::Unbounded, Bound::Excluded);
    let upper = Bound::Unbounded;
    let mut budget = BuildPageBudget::default();
    let mut count_digests = Vec::new();
    let maximum_prefix_updates = usize::try_from(MAX_CARDINALITY_BUILD_PREFIX_UPDATES_PER_PAGE)
        .map_err(|_| InternalError::store_unsupported())?;
    count_digests
        .try_reserve_exact(maximum_prefix_updates)
        .map_err(|_| InternalError::store_unsupported())?;
    let mut last_key = None;
    let mut has_more = false;
    index.visit_canonical_raw_entries_in_range((&lower, &upper), |raw_key, entry| {
        let key = IndexKey::try_from_raw(raw_key).map_err(|_| InternalError::store_corruption())?;
        let witness = entry
            .decode_row_witness_from_index_key(&key)
            .map_err(|_| InternalError::store_corruption())?;
        let accepted_component_count = if key.key_kind() == IndexKeyKind::User {
            authority.accepted_indexes.get(key.index_id()).copied()
        } else {
            None
        };
        if let Some(expected) = accepted_component_count
            && key.component_count() != expected
        {
            return Err(InternalError::store_corruption());
        }
        let prefix_updates = if accepted_component_count.is_some()
            && witness.existence_witness() == IndexEntryExistenceWitness::Present
        {
            u64::try_from(key.component_count()).map_err(|_| InternalError::store_unsupported())?
        } else {
            0
        };
        let source_bytes = raw_key
            .as_bytes()
            .len()
            .checked_add(entry.as_bytes().len())
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(InternalError::store_unsupported)?;
        if !budget.admit(source_bytes, prefix_updates)? {
            has_more = true;
            return Ok(true);
        }
        if prefix_updates != 0 {
            let mut components = Vec::new();
            components
                .try_reserve_exact(key.component_count())
                .map_err(|_| InternalError::store_unsupported())?;
            for component_index in 0..key.component_count() {
                let component = key
                    .component(component_index)
                    .ok_or_else(InternalError::store_corruption)?;
                components.push(component.to_vec());
                count_digests.push(CardinalityCountDigest::for_user_index_prefix(
                    *key.index_id(),
                    components.as_slice(),
                )?);
            }
        }
        last_key = Some(raw_key.clone());
        Ok(false)
    })?;
    if has_more && last_key.is_none() {
        return Err(InternalError::store_unsupported());
    }
    Ok(CollectedBuildPage {
        source_entries: budget.source_entries,
        source_bytes: budget.source_bytes,
        prefix_updates: budget.prefix_updates,
        count_digests,
        next_phase: CardinalityBuildPhase::Indexes,
        next_checkpoint: last_key
            .or_else(|| checkpoint.cloned())
            .map(CardinalityBuildCheckpoint::Index),
        candidate_complete: !has_more,
    })
}

fn coalesce_count_increments(
    mut digests: Vec<CardinalityCountDigest>,
) -> Result<Vec<(CardinalityCountDigest, u64)>, InternalError> {
    digests.sort_unstable();
    let mut increments: Vec<(CardinalityCountDigest, u64)> = Vec::new();
    increments
        .try_reserve_exact(digests.len())
        .map_err(|_| InternalError::store_unsupported())?;
    for digest in digests {
        if let Some((previous, count)) = increments.last_mut()
            && *previous == digest
        {
            *count = count
                .checked_add(1)
                .ok_or_else(InternalError::store_unsupported)?;
        } else {
            increments.push((digest, 1_u64));
        }
    }
    Ok(increments)
}

#[cfg(test)]
mod tests;
