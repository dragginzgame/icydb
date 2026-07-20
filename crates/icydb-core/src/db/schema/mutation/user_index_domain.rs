//! Module: db::schema::mutation::user_index_domain
//! Responsibility: bounded, zero-write user-index-domain replacement staging.
//! Does not own: accepted-schema marker persistence or physical index apply.
//! Boundary: accepted schema + authoritative rows + current index view -> staged raw replacement.

use crate::{
    db::{
        commit::CommitSchemaFingerprint,
        data::{CanonicalSlotReader, StructuralRowContract},
        index::{
            IndexEntryValue, IndexId, IndexKey, IndexKeyKind, IndexState, IndexStore,
            IndexStoreVisit, RawIndexStoreKey,
        },
        key_taxonomy::PrimaryKeyValue,
        predicate::{PredicateProgram, normalize, parse_sql_predicate},
        schema::{
            AcceptedCatalogIdentity, PersistedSchemaSnapshot, SchemaExpressionIndexRebuildTarget,
            SchemaFieldPathIndexRebuildTarget, SchemaVersion,
            accepted_schema_cache_fingerprint_for_persisted_snapshot,
            mutation::SchemaMutationRequest,
        },
    },
    error::InternalError,
    types::EntityTag,
};
use std::{collections::BTreeSet, mem::size_of};

const MAX_SOURCE_ROWS: usize = 65_536;
const MAX_SOURCE_ROW_BYTES: usize = 256 * 1024 * 1024;
const MAX_PROJECTION_ENTRIES: usize = 131_072;
const MAX_DELETION_KEYS: usize = 65_536;
const MAX_STAGED_RAW_BYTES: usize = 256 * 1024 * 1024;
const MAX_PROJECTION_WORK_UNITS: usize = 262_144;

///
/// SchemaUserIndexDomainRow
///
/// One decoded authoritative row supplied to complete-domain staging.
/// Owned by schema mutation and valid only for the current non-awaiting stage.
///

#[derive(Clone, Copy)]
pub(in crate::db) struct SchemaUserIndexDomainRow<'a> {
    primary_key_value: PrimaryKeyValue,
    slots: &'a dyn CanonicalSlotReader,
    encoded_row_bytes: usize,
}

impl<'a> SchemaUserIndexDomainRow<'a> {
    /// Bind one validated row identity, canonical slot reader, and source size.
    #[must_use]
    pub(in crate::db) fn new(
        primary_key_value: impl Into<PrimaryKeyValue>,
        slots: &'a dyn CanonicalSlotReader,
        encoded_row_bytes: usize,
    ) -> Self {
        Self {
            primary_key_value: primary_key_value.into(),
            slots,
            encoded_row_bytes,
        }
    }
}

///
/// StagedUserIndexDomainEntry
///
/// One exact raw key/value pair in the accepted-after user-index projection.
/// Owned by schema mutation until marker-first physical apply consumes it.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct StagedUserIndexDomainEntry {
    key: RawIndexStoreKey,
    value: IndexEntryValue,
}

impl StagedUserIndexDomainEntry {
    /// Borrow the prevalidated raw store key.
    #[must_use]
    pub(in crate::db) const fn key(&self) -> &RawIndexStoreKey {
        &self.key
    }

    /// Borrow the prevalidated raw store value.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn value(&self) -> &IndexEntryValue {
        &self.value
    }

    /// Consume this staged entry into its allocation-complete raw parts.
    pub(in crate::db) fn into_parts(self) -> (RawIndexStoreKey, IndexEntryValue) {
        (self.key, self.value)
    }
}

///
/// StagedUserIndexDomainUsage
///
/// Deterministic resource usage retained with one complete staged replacement.
/// Schema mutation owns these counters and does not expose their limits publicly.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct StagedUserIndexDomainUsage {
    source_rows: usize,
    source_row_bytes: usize,
    projection_entries: usize,
    deletion_keys: usize,
    staged_raw_bytes: usize,
    projection_work_units: usize,
}

impl StagedUserIndexDomainUsage {
    /// Return the number of authoritative rows scanned once by the builder.
    #[cfg(any(test, feature = "sql"))]
    #[must_use]
    pub(in crate::db) const fn source_rows(self) -> usize {
        self.source_rows
    }

    /// Return the peak raw payload and deterministic-sort workspace charge.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn staged_raw_bytes(self) -> usize {
        self.staged_raw_bytes
    }
}

///
/// StagedUserIndexDomainReplacement
///
/// Complete, bounded raw replacement for one entity's user-index domain.
///
/// The deletion keys are proven to equal the current accepted-before physical
/// domain. The final entries are proven to be the complete accepted-after
/// projection. Constructing this value performs no `IndexStore` mutation.
/// Schema mutation owns this transient contract; commit publication consumes it.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct StagedUserIndexDomainReplacement {
    store_path: &'static str,
    entity_tag: EntityTag,
    accepted_before_identity: AcceptedCatalogIdentity,
    accepted_after_version: SchemaVersion,
    accepted_after_fingerprint: CommitSchemaFingerprint,
    deletion_keys: Vec<RawIndexStoreKey>,
    final_entries: Vec<StagedUserIndexDomainEntry>,
    usage: StagedUserIndexDomainUsage,
}

impl StagedUserIndexDomainReplacement {
    /// Derive and validate a complete user-index-domain replacement without
    /// changing physical index state.
    pub(in crate::db) fn stage<'a>(
        accepted_before_identity: AcceptedCatalogIdentity,
        accepted_before: &PersistedSchemaSnapshot,
        accepted_after: &PersistedSchemaSnapshot,
        predicate_row_contract: Option<&StructuralRowContract>,
        rows: impl IntoIterator<Item = SchemaUserIndexDomainRow<'a>>,
        index_store: &IndexStore,
    ) -> Result<Self, StagedUserIndexDomainError> {
        validate_stage_authority(
            accepted_before_identity,
            accepted_before,
            accepted_after,
            predicate_row_contract,
            index_store,
        )?;

        let entity_tag = accepted_before_identity.entity_tag();
        let before_projection = PreparedUserIndexProjection::from_snapshot(
            entity_tag,
            accepted_before,
            predicate_row_contract,
        )?;
        let after_projection = PreparedUserIndexProjection::from_snapshot(
            entity_tag,
            accepted_after,
            predicate_row_contract,
        )?;
        let mut budget = StagedUserIndexDomainBudget::standard();
        let mut expected_before = Vec::new();
        let mut final_entries = Vec::new();

        for row in rows {
            budget.consume_source_row(row.encoded_row_bytes)?;
            before_projection.derive_row(entity_tag, &row, &mut expected_before, &mut budget)?;
            after_projection.derive_row(entity_tag, &row, &mut final_entries, &mut budget)?;
        }

        validate_projection(&mut expected_before, &before_projection.unique_index_ids)?;
        validate_projection(&mut final_entries, &after_projection.unique_index_ids)?;
        let observed_before =
            observe_current_user_index_domain(index_store, entity_tag, &mut budget)?;
        if observed_before != expected_before {
            return Err(StagedUserIndexDomainError::CurrentDomainMismatch);
        }

        let deletion_keys = observed_before
            .into_iter()
            .map(|entry| entry.key)
            .collect::<Vec<_>>();
        validate_insertion_collisions(index_store, &deletion_keys, &final_entries)?;
        budget.finish_sort_workspace(
            expected_before.len(),
            final_entries.len(),
            deletion_keys.len(),
        )?;

        Ok(Self {
            store_path: accepted_before_identity.store_path(),
            entity_tag,
            accepted_before_identity,
            accepted_after_version: accepted_after.version(),
            accepted_after_fingerprint: accepted_schema_cache_fingerprint_for_persisted_snapshot(
                accepted_after,
            )
            .map_err(StagedUserIndexDomainError::Fingerprint)?,
            deletion_keys,
            final_entries,
            usage: budget.usage(),
        })
    }

    /// Borrow the backing store path captured from accepted-before authority.
    #[must_use]
    pub(in crate::db) const fn store_path(&self) -> &'static str {
        self.store_path
    }

    /// Return the affected entity identity.
    #[must_use]
    pub(in crate::db) const fn entity_tag(&self) -> EntityTag {
        self.entity_tag
    }

    /// Return the exact accepted-before catalog identity used for derivation.
    #[must_use]
    pub(in crate::db) const fn accepted_before_identity(&self) -> AcceptedCatalogIdentity {
        self.accepted_before_identity
    }

    /// Return the accepted-after declared schema version used for derivation.
    #[must_use]
    pub(in crate::db) const fn accepted_after_version(&self) -> SchemaVersion {
        self.accepted_after_version
    }

    /// Return the accepted-after candidate fingerprint used for derivation.
    #[must_use]
    pub(in crate::db) const fn accepted_after_fingerprint(&self) -> CommitSchemaFingerprint {
        self.accepted_after_fingerprint
    }

    /// Borrow the exact raw deletion set for mechanical apply.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn deletion_keys(&self) -> &[RawIndexStoreKey] {
        self.deletion_keys.as_slice()
    }

    /// Borrow the complete sorted accepted-after raw projection.
    #[cfg(any(test, feature = "sql"))]
    #[must_use]
    pub(in crate::db) const fn final_entries(&self) -> &[StagedUserIndexDomainEntry] {
        self.final_entries.as_slice()
    }

    /// Return deterministic staging resource usage.
    #[cfg(any(test, feature = "sql"))]
    #[must_use]
    pub(in crate::db) const fn usage(&self) -> StagedUserIndexDomainUsage {
        self.usage
    }

    /// Consume the stage into the exact raw payload required by mechanical apply.
    pub(in crate::db) fn into_apply_parts(
        self,
    ) -> (Vec<RawIndexStoreKey>, Vec<StagedUserIndexDomainEntry>) {
        (self.deletion_keys, self.final_entries)
    }
}

///
/// StagedUserIndexDomainError
///
/// Typed, precommit-only rejection from complete-domain staging.
/// Schema mutation owns these failures; no variant permits physical mutation.
///

pub(in crate::db) enum StagedUserIndexDomainError {
    /// The accepted-after entity identity differs from accepted-before.
    AcceptedAfterEntityMismatch,

    /// The supplied accepted-before catalog identity does not match the snapshot.
    AcceptedBeforeIdentityMismatch,

    /// An accepted index points outside the authoritative backing store.
    AcceptedIndexStoreMismatch,

    /// The current physical user-index domain differs from row-derived truth.
    CurrentDomainMismatch,

    /// The current physical domain exceeds its private deletion-key bound.
    DeletionKeyLimitExceeded,

    /// Two rows produced an identical complete raw key.
    DuplicateRawKey,

    /// A unique index produced equal component tuples for distinct rows.
    DuplicateUniqueKey,

    /// Accepted-schema fingerprint construction failed.
    Fingerprint(InternalError),

    /// A final key collides with state outside the proven deletion domain.
    InsertionCollision,

    /// The backing index store is not query-ready before staging.
    IndexStoreNotReady,

    /// Accepted row values could not produce a semantic index key.
    KeyDerivation(InternalError),

    /// A semantic index key could not encode into its bounded raw form.
    KeyEncode,

    /// A filtered index lacks the accepted row contract required to compile it.
    MissingPredicateRowContract,

    /// One current physical key could not be decoded and classified safely.
    PhysicalKeyDecode,

    /// An accepted index predicate failed while evaluating one row.
    PredicateEvaluation(InternalError),

    /// An accepted index predicate could not be parsed.
    PredicateParse,

    /// The combined before/after projection exceeds its private entry bound.
    ProjectionEntryLimitExceeded,

    /// Derivation and physical-classification work exceeds its private bound.
    ProjectionWorkLimitExceeded,

    /// The predicate row contract belongs to a different entity.
    RowContractEntityMismatch,

    /// Aggregate authoritative-row bytes exceed the private staging bound.
    SourceRowBytesLimitExceeded,

    /// The authoritative row count exceeds the private staging bound.
    SourceRowLimitExceeded,

    /// Retained raw payload plus deterministic-sort workspace exceeds its bound.
    StagedRawBytesLimitExceeded,

    /// Accepted index metadata cannot lower to a maintained key shape.
    UnsupportedAcceptedIndex,
}

impl StagedUserIndexDomainError {
    /// Preserve stable underlying causes and classify stage-owned rejection at
    /// the schema-publication boundary.
    pub(in crate::db) fn into_internal_error(self) -> InternalError {
        match self {
            Self::Fingerprint(error)
            | Self::KeyDerivation(error)
            | Self::PredicateEvaluation(error) => error,
            Self::CurrentDomainMismatch
            | Self::DuplicateRawKey
            | Self::DuplicateUniqueKey
            | Self::InsertionCollision
            | Self::PhysicalKeyDecode => InternalError::store_corruption(),
            Self::AcceptedAfterEntityMismatch
            | Self::AcceptedBeforeIdentityMismatch
            | Self::AcceptedIndexStoreMismatch
            | Self::RowContractEntityMismatch => InternalError::store_invariant(),
            Self::DeletionKeyLimitExceeded
            | Self::IndexStoreNotReady
            | Self::KeyEncode
            | Self::MissingPredicateRowContract
            | Self::PredicateParse
            | Self::ProjectionEntryLimitExceeded
            | Self::ProjectionWorkLimitExceeded
            | Self::SourceRowBytesLimitExceeded
            | Self::SourceRowLimitExceeded
            | Self::StagedRawBytesLimitExceeded
            | Self::UnsupportedAcceptedIndex => InternalError::store_unsupported(),
        }
    }
}

// Concrete key-shape distinction used only while deriving one projection.
enum PreparedUserIndexTarget {
    FieldPath(SchemaFieldPathIndexRebuildTarget),
    Expression(SchemaExpressionIndexRebuildTarget),
}

// One accepted index with its precompiled optional membership predicate.
struct PreparedUserIndex {
    target: PreparedUserIndexTarget,
    predicate: Option<PredicateProgram>,
}

impl PreparedUserIndex {
    fn derive_key(
        &self,
        entity_tag: EntityTag,
        row: &SchemaUserIndexDomainRow<'_>,
    ) -> Result<Option<IndexKey>, StagedUserIndexDomainError> {
        if let Some(predicate) = self.predicate.as_ref()
            && !predicate
                .eval_with_structural_slot_reader(row.slots)
                .map_err(StagedUserIndexDomainError::PredicateEvaluation)?
        {
            return Ok(None);
        }

        match &self.target {
            PreparedUserIndexTarget::FieldPath(target) => {
                IndexKey::new_from_slots_with_field_path_rebuild_target(
                    entity_tag,
                    row.primary_key_value,
                    target,
                    row.slots,
                )
            }
            PreparedUserIndexTarget::Expression(target) => {
                IndexKey::new_from_slots_with_expression_rebuild_target(
                    entity_tag,
                    row.primary_key_value,
                    target,
                    row.slots,
                )
            }
        }
        .map_err(StagedUserIndexDomainError::KeyDerivation)
    }
}

// Complete accepted index set prepared for one row traversal.
struct PreparedUserIndexProjection {
    indexes: Vec<PreparedUserIndex>,
    unique_index_ids: BTreeSet<IndexId>,
}

impl PreparedUserIndexProjection {
    fn from_snapshot(
        entity_tag: EntityTag,
        snapshot: &PersistedSchemaSnapshot,
        predicate_row_contract: Option<&StructuralRowContract>,
    ) -> Result<Self, StagedUserIndexDomainError> {
        let mut indexes = Vec::with_capacity(snapshot.indexes().len());
        let mut unique_index_ids = BTreeSet::new();

        for index in snapshot.indexes() {
            let request = if index.key().is_field_path_only() {
                SchemaMutationRequest::from_accepted_field_path_index(index)
            } else {
                SchemaMutationRequest::from_accepted_expression_index(index)
            }
            .map_err(|_| StagedUserIndexDomainError::UnsupportedAcceptedIndex)?;
            let target = match request {
                SchemaMutationRequest::AddFieldPathIndex { target } => {
                    PreparedUserIndexTarget::FieldPath(target)
                }
                SchemaMutationRequest::AddExpressionIndex { target } => {
                    PreparedUserIndexTarget::Expression(target)
                }
                SchemaMutationRequest::ExactMatch | SchemaMutationRequest::AppendOnlyFields(_) => {
                    return Err(StagedUserIndexDomainError::UnsupportedAcceptedIndex);
                }
            };
            let predicate = index
                .predicate_sql()
                .map(|sql| {
                    let row_contract = predicate_row_contract
                        .ok_or(StagedUserIndexDomainError::MissingPredicateRowContract)?;
                    parse_sql_predicate(sql)
                        .map(|predicate| {
                            PredicateProgram::compile_with_row_contract(
                                row_contract,
                                &normalize(&predicate),
                            )
                        })
                        .map_err(|_| StagedUserIndexDomainError::PredicateParse)
                })
                .transpose()?;
            if index.unique() {
                unique_index_ids.insert(IndexId::new(entity_tag, index.ordinal()));
            }
            indexes.push(PreparedUserIndex { target, predicate });
        }

        Ok(Self {
            indexes,
            unique_index_ids,
        })
    }

    fn derive_row(
        &self,
        entity_tag: EntityTag,
        row: &SchemaUserIndexDomainRow<'_>,
        entries: &mut Vec<StagedUserIndexDomainEntry>,
        budget: &mut StagedUserIndexDomainBudget,
    ) -> Result<(), StagedUserIndexDomainError> {
        for index in &self.indexes {
            budget.consume_projection_work()?;
            let Some(key) = index.derive_key(entity_tag, row)? else {
                continue;
            };
            let key = key
                .to_raw()
                .map_err(|_| StagedUserIndexDomainError::KeyEncode)?;
            let value = IndexEntryValue::presence();
            budget.consume_projection_entry(key.as_bytes().len())?;
            entries.push(StagedUserIndexDomainEntry { key, value });
        }

        Ok(())
    }
}

fn validate_stage_authority(
    accepted_before_identity: AcceptedCatalogIdentity,
    accepted_before: &PersistedSchemaSnapshot,
    accepted_after: &PersistedSchemaSnapshot,
    predicate_row_contract: Option<&StructuralRowContract>,
    index_store: &IndexStore,
) -> Result<(), StagedUserIndexDomainError> {
    let accepted_before_fingerprint =
        accepted_schema_cache_fingerprint_for_persisted_snapshot(accepted_before)
            .map_err(StagedUserIndexDomainError::Fingerprint)?;
    let entity_path_matches =
        accepted_before_identity.entity_path() == accepted_before.entity_path();
    let schema_version_matches =
        accepted_before_identity.accepted_schema_version() == accepted_before.version();
    let schema_fingerprint_matches =
        accepted_before_identity.accepted_schema_fingerprint() == accepted_before_fingerprint;
    if !(entity_path_matches && schema_version_matches && schema_fingerprint_matches) {
        return Err(StagedUserIndexDomainError::AcceptedBeforeIdentityMismatch);
    }
    if accepted_after.entity_path() != accepted_before.entity_path() {
        return Err(StagedUserIndexDomainError::AcceptedAfterEntityMismatch);
    }
    let store_path = accepted_before_identity.store_path();
    if accepted_before
        .indexes()
        .iter()
        .chain(accepted_after.indexes())
        .any(|index| index.store() != store_path)
    {
        return Err(StagedUserIndexDomainError::AcceptedIndexStoreMismatch);
    }
    if predicate_row_contract.is_some_and(|row_contract| {
        row_contract.entity_path() != accepted_before_identity.entity_path()
    }) {
        return Err(StagedUserIndexDomainError::RowContractEntityMismatch);
    }
    if index_store.state() != IndexState::Ready {
        return Err(StagedUserIndexDomainError::IndexStoreNotReady);
    }

    Ok(())
}

fn validate_projection(
    entries: &mut [StagedUserIndexDomainEntry],
    unique_index_ids: &BTreeSet<IndexId>,
) -> Result<(), StagedUserIndexDomainError> {
    entries.sort_unstable_by(|left, right| left.key.cmp(&right.key));
    for pair in entries.windows(2) {
        if pair[0].key == pair[1].key {
            return Err(StagedUserIndexDomainError::DuplicateRawKey);
        }
        let left = IndexKey::try_from_raw(&pair[0].key)
            .map_err(|_| StagedUserIndexDomainError::PhysicalKeyDecode)?;
        let right = IndexKey::try_from_raw(&pair[1].key)
            .map_err(|_| StagedUserIndexDomainError::PhysicalKeyDecode)?;
        if left.index_id() == right.index_id()
            && unique_index_ids.contains(left.index_id())
            && left.has_same_components(&right)
        {
            return Err(StagedUserIndexDomainError::DuplicateUniqueKey);
        }
    }

    Ok(())
}

fn observe_current_user_index_domain(
    index_store: &IndexStore,
    entity_tag: EntityTag,
    budget: &mut StagedUserIndexDomainBudget,
) -> Result<Vec<StagedUserIndexDomainEntry>, StagedUserIndexDomainError> {
    let mut entries = Vec::new();
    let result = index_store.visit_entries(|raw_key, value| {
        budget.consume_projection_work()?;
        let key = IndexKey::try_from_raw(raw_key)
            .map_err(|_| StagedUserIndexDomainError::PhysicalKeyDecode)?;
        if key.key_kind() == IndexKeyKind::User && key.index_id().entity_tag() == entity_tag {
            budget.consume_deletion_key(raw_key.as_bytes().len())?;
            entries.push(StagedUserIndexDomainEntry {
                key: raw_key.clone(),
                value: value.clone(),
            });
        }
        Ok(IndexStoreVisit::Continue)
    });
    result?;

    Ok(entries)
}

fn validate_insertion_collisions(
    index_store: &IndexStore,
    deletion_keys: &[RawIndexStoreKey],
    final_entries: &[StagedUserIndexDomainEntry],
) -> Result<(), StagedUserIndexDomainError> {
    for entry in final_entries {
        if index_store.get(entry.key()).is_some()
            && deletion_keys.binary_search(entry.key()).is_err()
        {
            return Err(StagedUserIndexDomainError::InsertionCollision);
        }
    }

    Ok(())
}

// Incremental private budget shared by both projections and physical observation.
struct StagedUserIndexDomainBudget {
    usage: StagedUserIndexDomainUsage,
}

impl StagedUserIndexDomainBudget {
    const fn standard() -> Self {
        Self {
            usage: StagedUserIndexDomainUsage {
                source_rows: 0,
                source_row_bytes: 0,
                projection_entries: 0,
                deletion_keys: 0,
                staged_raw_bytes: 0,
                projection_work_units: 0,
            },
        }
    }

    fn consume_source_row(
        &mut self,
        encoded_row_bytes: usize,
    ) -> Result<(), StagedUserIndexDomainError> {
        self.usage.source_rows = self
            .usage
            .source_rows
            .checked_add(1)
            .ok_or(StagedUserIndexDomainError::SourceRowLimitExceeded)?;
        if self.usage.source_rows > MAX_SOURCE_ROWS {
            return Err(StagedUserIndexDomainError::SourceRowLimitExceeded);
        }
        self.usage.source_row_bytes = self
            .usage
            .source_row_bytes
            .checked_add(encoded_row_bytes)
            .ok_or(StagedUserIndexDomainError::SourceRowBytesLimitExceeded)?;
        if self.usage.source_row_bytes > MAX_SOURCE_ROW_BYTES {
            return Err(StagedUserIndexDomainError::SourceRowBytesLimitExceeded);
        }

        Ok(())
    }

    fn consume_projection_work(&mut self) -> Result<(), StagedUserIndexDomainError> {
        self.usage.projection_work_units = self
            .usage
            .projection_work_units
            .checked_add(1)
            .ok_or(StagedUserIndexDomainError::ProjectionWorkLimitExceeded)?;
        if self.usage.projection_work_units > MAX_PROJECTION_WORK_UNITS {
            return Err(StagedUserIndexDomainError::ProjectionWorkLimitExceeded);
        }

        Ok(())
    }

    fn consume_projection_entry(
        &mut self,
        key_bytes: usize,
    ) -> Result<(), StagedUserIndexDomainError> {
        self.usage.projection_entries = self
            .usage
            .projection_entries
            .checked_add(1)
            .ok_or(StagedUserIndexDomainError::ProjectionEntryLimitExceeded)?;
        if self.usage.projection_entries > MAX_PROJECTION_ENTRIES {
            return Err(StagedUserIndexDomainError::ProjectionEntryLimitExceeded);
        }
        self.consume_staged_bytes(
            key_bytes
                .checked_add(1)
                .and_then(|bytes| bytes.checked_add(size_of::<StagedUserIndexDomainEntry>()))
                .ok_or(StagedUserIndexDomainError::StagedRawBytesLimitExceeded)?,
        )
    }

    fn consume_deletion_key(&mut self, key_bytes: usize) -> Result<(), StagedUserIndexDomainError> {
        self.usage.deletion_keys = self
            .usage
            .deletion_keys
            .checked_add(1)
            .ok_or(StagedUserIndexDomainError::DeletionKeyLimitExceeded)?;
        if self.usage.deletion_keys > MAX_DELETION_KEYS {
            return Err(StagedUserIndexDomainError::DeletionKeyLimitExceeded);
        }
        self.consume_staged_bytes(
            key_bytes
                .checked_add(1)
                .and_then(|bytes| bytes.checked_add(size_of::<StagedUserIndexDomainEntry>()))
                .ok_or(StagedUserIndexDomainError::StagedRawBytesLimitExceeded)?,
        )
    }

    fn finish_sort_workspace(
        &mut self,
        before_entries: usize,
        after_entries: usize,
        deletion_keys: usize,
    ) -> Result<(), StagedUserIndexDomainError> {
        let entry_workspace = before_entries
            .checked_add(after_entries)
            .and_then(|count| count.checked_mul(size_of::<StagedUserIndexDomainEntry>()))
            .ok_or(StagedUserIndexDomainError::StagedRawBytesLimitExceeded)?;
        let deletion_workspace = deletion_keys
            .checked_mul(size_of::<RawIndexStoreKey>())
            .ok_or(StagedUserIndexDomainError::StagedRawBytesLimitExceeded)?;
        self.consume_staged_bytes(
            entry_workspace
                .checked_add(deletion_workspace)
                .ok_or(StagedUserIndexDomainError::StagedRawBytesLimitExceeded)?,
        )
    }

    fn consume_staged_bytes(&mut self, bytes: usize) -> Result<(), StagedUserIndexDomainError> {
        self.usage.staged_raw_bytes = self
            .usage
            .staged_raw_bytes
            .checked_add(bytes)
            .ok_or(StagedUserIndexDomainError::StagedRawBytesLimitExceeded)?;
        if self.usage.staged_raw_bytes > MAX_STAGED_RAW_BYTES {
            return Err(StagedUserIndexDomainError::StagedRawBytesLimitExceeded);
        }

        Ok(())
    }

    const fn usage(&self) -> StagedUserIndexDomainUsage {
        self.usage
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_budget_fixes_and_admits_the_design_measurement_point() {
        let design_rows = std::hint::black_box(2_048usize);
        let design_indexes = std::hint::black_box(8usize);
        let design_domain_entries = design_rows * design_indexes;
        let design_projection_entries = design_domain_entries * 2;
        let design_work_units = design_domain_entries * 3;

        assert_eq!(MAX_SOURCE_ROWS, 65_536);
        assert_eq!(MAX_SOURCE_ROW_BYTES, 256 * 1024 * 1024);
        assert_eq!(MAX_PROJECTION_ENTRIES, 131_072);
        assert_eq!(MAX_DELETION_KEYS, 65_536);
        assert_eq!(MAX_STAGED_RAW_BYTES, 256 * 1024 * 1024);
        assert_eq!(MAX_PROJECTION_WORK_UNITS, 262_144);
        assert!(MAX_SOURCE_ROWS >= design_rows);
        assert!(MAX_PROJECTION_ENTRIES >= design_projection_entries);
        assert!(MAX_DELETION_KEYS >= design_domain_entries);
        assert!(MAX_PROJECTION_WORK_UNITS >= design_work_units);
    }

    #[test]
    fn standard_budget_rejects_each_aggregate_dimension_at_its_boundary() {
        let mut row_count = StagedUserIndexDomainBudget::standard();
        for _ in 0..MAX_SOURCE_ROWS {
            assert!(row_count.consume_source_row(0).is_ok());
        }
        assert!(matches!(
            row_count.consume_source_row(0),
            Err(StagedUserIndexDomainError::SourceRowLimitExceeded),
        ));

        let mut row_bytes = StagedUserIndexDomainBudget::standard();
        assert!(matches!(
            row_bytes.consume_source_row(MAX_SOURCE_ROW_BYTES + 1),
            Err(StagedUserIndexDomainError::SourceRowBytesLimitExceeded),
        ));

        let mut entry_count = StagedUserIndexDomainBudget::standard();
        for _ in 0..MAX_PROJECTION_ENTRIES {
            assert!(entry_count.consume_projection_entry(0).is_ok());
        }
        assert!(matches!(
            entry_count.consume_projection_entry(0),
            Err(StagedUserIndexDomainError::ProjectionEntryLimitExceeded),
        ));

        let mut deletion_count = StagedUserIndexDomainBudget::standard();
        for _ in 0..MAX_DELETION_KEYS {
            assert!(deletion_count.consume_deletion_key(0).is_ok());
        }
        assert!(matches!(
            deletion_count.consume_deletion_key(0),
            Err(StagedUserIndexDomainError::DeletionKeyLimitExceeded),
        ));

        let mut work = StagedUserIndexDomainBudget::standard();
        for _ in 0..MAX_PROJECTION_WORK_UNITS {
            assert!(work.consume_projection_work().is_ok());
        }
        assert!(matches!(
            work.consume_projection_work(),
            Err(StagedUserIndexDomainError::ProjectionWorkLimitExceeded),
        ));

        let mut staged_bytes = StagedUserIndexDomainBudget::standard();
        assert!(matches!(
            staged_bytes.consume_staged_bytes(MAX_STAGED_RAW_BYTES + 1),
            Err(StagedUserIndexDomainError::StagedRawBytesLimitExceeded),
        ));
    }
}
