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
            AcceptedCatalogIdentity, MAX_SCHEMA_PROJECTION_ENTRIES,
            MAX_SCHEMA_PROJECTION_WORK_UNITS, MAX_SCHEMA_STAGED_RAW_BYTES, PersistedSchemaSnapshot,
            SchemaExpressionIndexRebuildTarget, SchemaFieldPathIndexRebuildTarget,
            SchemaTransitionSourceBudget, SchemaVersion,
            accepted_schema_cache_fingerprint_for_persisted_snapshot,
            mutation::SchemaMutationRequest,
        },
    },
    error::{InternalError, SchemaTransitionBudgetResource},
    types::EntityTag,
};
use std::{collections::BTreeSet, mem::size_of};

const MAX_DELETION_KEYS: usize = 65_536;

///
/// SchemaUserIndexDomainRow
///
/// One decoded authoritative row supplied to complete-domain staging.
/// Owned by schema mutation and valid only for the current non-awaiting stage.
///

#[derive(Clone, Copy)]
pub(in crate::db) struct SchemaUserIndexDomainRow<'a> {
    primary_key_value: PrimaryKeyValue,
    accepted_before_slots: &'a dyn CanonicalSlotReader,
    accepted_after_slots: &'a dyn CanonicalSlotReader,
    encoded_row_bytes: usize,
}

impl<'a> SchemaUserIndexDomainRow<'a> {
    /// Bind one validated row identity, canonical slot reader, and source size.
    #[must_use]
    pub(in crate::db) fn new(
        primary_key_value: impl Into<PrimaryKeyValue>,
        accepted_before_slots: &'a dyn CanonicalSlotReader,
        accepted_after_slots: &'a dyn CanonicalSlotReader,
        encoded_row_bytes: usize,
    ) -> Self {
        Self {
            primary_key_value: primary_key_value.into(),
            accepted_before_slots,
            accepted_after_slots,
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
    accepted_before_entries: usize,
    accepted_after_entries: usize,
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

    /// Return the number of entries in the row-derived accepted-before domain.
    #[cfg(any(test, feature = "sql"))]
    #[must_use]
    pub(in crate::db) const fn accepted_before_entries(self) -> usize {
        self.accepted_before_entries
    }

    /// Return the number of entries in the row-derived accepted-after domain.
    #[cfg(any(test, feature = "sql"))]
    #[must_use]
    pub(in crate::db) const fn accepted_after_entries(self) -> usize {
        self.accepted_after_entries
    }

    /// Return the peak raw payload and deterministic-sort workspace charge.
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
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn final_entries(&self) -> &[StagedUserIndexDomainEntry] {
        self.final_entries.as_slice()
    }

    /// Return deterministic staging resource usage.
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
/// StagedUserIndexDomainReplacementBuilder
///
/// Incremental, zero-write builder for one complete user-index-domain stage.
/// Schema reconciliation feeds authoritative rows directly into this owner so
/// aggregate bounds are enforced before projection state is retained.
///

pub(in crate::db) struct StagedUserIndexDomainReplacementBuilder {
    store_path: &'static str,
    entity_tag: EntityTag,
    accepted_before_identity: AcceptedCatalogIdentity,
    accepted_after_version: SchemaVersion,
    accepted_after_fingerprint: CommitSchemaFingerprint,
    before_projection: PreparedUserIndexProjection,
    after_projection: PreparedUserIndexProjection,
    expected_before: Vec<StagedUserIndexDomainEntry>,
    final_entries: Vec<StagedUserIndexDomainEntry>,
    budget: StagedUserIndexDomainBudget,
}

impl StagedUserIndexDomainReplacementBuilder {
    /// Begin one stage from accepted schema authority and a Ready physical view.
    pub(in crate::db) fn new(
        accepted_before_identity: AcceptedCatalogIdentity,
        accepted_before: &PersistedSchemaSnapshot,
        accepted_after: &PersistedSchemaSnapshot,
        accepted_before_row_contract: Option<&StructuralRowContract>,
        accepted_after_row_contract: Option<&StructuralRowContract>,
        index_store: &IndexStore,
    ) -> Result<Self, StagedUserIndexDomainError> {
        validate_stage_authority(
            accepted_before_identity,
            accepted_before,
            accepted_after,
            accepted_before_row_contract,
            accepted_after_row_contract,
            index_store,
        )?;

        let entity_tag = accepted_before_identity.entity_tag();
        let before_projection = PreparedUserIndexProjection::from_snapshot(
            entity_tag,
            accepted_before,
            accepted_before_row_contract,
        )?;
        let after_projection = PreparedUserIndexProjection::from_snapshot(
            entity_tag,
            accepted_after,
            accepted_after_row_contract,
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
            before_projection,
            after_projection,
            expected_before: Vec::new(),
            final_entries: Vec::new(),
            budget: StagedUserIndexDomainBudget::standard(),
        })
    }

    /// Consume one authoritative row, enforcing aggregate bounds before
    /// retaining its accepted-before or accepted-after entries.
    pub(in crate::db) fn observe_row(
        &mut self,
        row: &SchemaUserIndexDomainRow<'_>,
    ) -> Result<(), StagedUserIndexDomainError> {
        self.budget.consume_source_row(row.encoded_row_bytes)?;
        self.before_projection.derive_row(
            self.entity_tag,
            row,
            row.accepted_before_slots,
            &mut self.expected_before,
            &mut self.budget,
        )?;
        self.after_projection.derive_row(
            self.entity_tag,
            row,
            row.accepted_after_slots,
            &mut self.final_entries,
            &mut self.budget,
        )
    }

    /// Finish exact physical validation and produce the allocation-complete
    /// replacement without changing `IndexStore` state.
    pub(in crate::db) fn finish(
        mut self,
        index_store: &IndexStore,
    ) -> Result<StagedUserIndexDomainReplacement, StagedUserIndexDomainError> {
        if index_store.state() != IndexState::Ready {
            return Err(StagedUserIndexDomainError::IndexStoreNotReady);
        }
        validate_projection(
            &mut self.expected_before,
            &self.before_projection.unique_index_ids,
            ProjectionAuthority::AcceptedBefore,
            self.accepted_before_identity.entity_path(),
        )?;
        validate_projection(
            &mut self.final_entries,
            &self.after_projection.unique_index_ids,
            ProjectionAuthority::CandidateAfter,
            self.accepted_before_identity.entity_path(),
        )?;
        let observed_before =
            observe_current_user_index_domain(index_store, self.entity_tag, &mut self.budget)?;
        if observed_before != self.expected_before {
            return Err(StagedUserIndexDomainError::CurrentDomainMismatch);
        }

        let deletion_keys = observed_before
            .into_iter()
            .map(|entry| entry.key)
            .collect::<Vec<_>>();
        validate_insertion_collisions(index_store, &deletion_keys, &self.final_entries)?;
        self.budget.finish_sort_workspace(
            self.expected_before.len(),
            self.final_entries.len(),
            deletion_keys.len(),
        )?;
        self.budget
            .record_projection_counts(self.expected_before.len(), self.final_entries.len());

        Ok(StagedUserIndexDomainReplacement {
            store_path: self.store_path,
            entity_tag: self.entity_tag,
            accepted_before_identity: self.accepted_before_identity,
            accepted_after_version: self.accepted_after_version,
            accepted_after_fingerprint: self.accepted_after_fingerprint,
            deletion_keys,
            final_entries: self.final_entries,
            usage: self.budget.usage(),
        })
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

    /// Complete staging exceeded one canonical schema-transition resource.
    BudgetExceeded(SchemaTransitionBudgetResource),

    /// An accepted index points outside the authoritative backing store.
    AcceptedIndexStoreMismatch,

    /// The current physical user-index domain differs from row-derived truth.
    CurrentDomainMismatch,

    /// Two rows produced an identical complete raw key.
    DuplicateRawKey,

    /// A unique index produced equal component tuples for distinct rows.
    DuplicateUniqueKey,

    /// The accepted-after candidate would violate one unique index.
    CandidateUniqueConflict { entity_path: &'static str },

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

    /// The predicate row contract belongs to a different entity.
    RowContractEntityMismatch,

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
            Self::CandidateUniqueConflict { entity_path } => {
                InternalError::index_violation(entity_path, &[])
            }
            Self::CurrentDomainMismatch
            | Self::DuplicateRawKey
            | Self::DuplicateUniqueKey
            | Self::InsertionCollision
            | Self::PhysicalKeyDecode => InternalError::store_corruption(),
            Self::AcceptedAfterEntityMismatch
            | Self::AcceptedBeforeIdentityMismatch
            | Self::AcceptedIndexStoreMismatch
            | Self::RowContractEntityMismatch => InternalError::store_invariant(),
            Self::BudgetExceeded(resource) => {
                InternalError::schema_transition_budget_exceeded(resource)
            }
            Self::IndexStoreNotReady
            | Self::KeyEncode
            | Self::MissingPredicateRowContract
            | Self::PredicateParse
            | Self::UnsupportedAcceptedIndex => InternalError::store_unsupported(),
        }
    }
}

///
/// PreparedUserIndexTarget
///
/// Concrete accepted key shape used only while deriving one staged projection.
///

enum PreparedUserIndexTarget {
    /// Accepted field-path key derivation.
    FieldPath(SchemaFieldPathIndexRebuildTarget),
    /// Accepted expression or mixed-key derivation.
    Expression(SchemaExpressionIndexRebuildTarget),
}

///
/// PreparedUserIndex
///
/// One accepted index plus its precompiled optional membership predicate.
///

struct PreparedUserIndex {
    target: PreparedUserIndexTarget,
    predicate: Option<PredicateProgram>,
}

impl PreparedUserIndex {
    fn from_accepted_index(
        index: &crate::db::schema::PersistedIndexSnapshot,
        predicate_row_contract: Option<&StructuralRowContract>,
    ) -> Result<Self, StagedUserIndexDomainError> {
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

        Ok(Self { target, predicate })
    }

    fn derive_key(
        &self,
        entity_tag: EntityTag,
        row: &SchemaUserIndexDomainRow<'_>,
        slots: &dyn CanonicalSlotReader,
    ) -> Result<Option<IndexKey>, StagedUserIndexDomainError> {
        if let Some(predicate) = self.predicate.as_ref()
            && !predicate
                .eval_with_structural_slot_reader(slots)
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
                    slots,
                )
            }
            PreparedUserIndexTarget::Expression(target) => {
                IndexKey::new_from_slots_with_expression_rebuild_target(
                    entity_tag,
                    row.primary_key_value,
                    target,
                    slots,
                )
            }
        }
        .map_err(StagedUserIndexDomainError::KeyDerivation)
    }
}

/// One accepted-schema unique-index projection bound to an exact physical generation.
///
/// The projection derives keys for either an accepted generation or a
/// planner-invisible activation candidate. Callers own traversal, physical
/// comparison, duplicate classification, and publication policy.
pub(in crate::db) struct UniqueConstraintProjection {
    entity_tag: EntityTag,
    prepared: PreparedUserIndex,
}

impl UniqueConstraintProjection {
    /// Compile one exact unique-index owner against the accepted row contract.
    pub(in crate::db) fn new(
        entity_tag: EntityTag,
        index: &crate::db::schema::PersistedIndexSnapshot,
        row_contract: &StructuralRowContract,
    ) -> Result<Self, InternalError> {
        if !index.unique() || index.physical_generation() == 0 {
            return Err(InternalError::store_invariant());
        }
        let prepared = PreparedUserIndex::from_accepted_index(index, Some(row_contract))
            .map_err(StagedUserIndexDomainError::into_internal_error)?;
        Ok(Self {
            entity_tag,
            prepared,
        })
    }

    /// Derive the optional candidate key for one validated canonical row.
    pub(in crate::db) fn derive_key(
        &self,
        primary_key: &crate::db::key_taxonomy::PrimaryKeyValue,
        slots: &dyn CanonicalSlotReader,
    ) -> Result<Option<RawIndexStoreKey>, InternalError> {
        let row = SchemaUserIndexDomainRow::new(*primary_key, slots, slots, 0);
        self.prepared
            .derive_key(self.entity_tag, &row, slots)
            .map_err(StagedUserIndexDomainError::into_internal_error)?
            .map(|key| key.to_raw().map_err(|_| InternalError::index_invariant()))
            .transpose()
    }
}

///
/// ProjectionAuthority
///
/// Trust role used to preserve the typed cause of uniqueness rejection.
///

#[derive(Clone, Copy)]
enum ProjectionAuthority {
    /// Already accepted physical truth; violations indicate corruption.
    AcceptedBefore,
    /// Proposed accepted-after truth; uniqueness violations are conflicts.
    CandidateAfter,
}

///
/// PreparedUserIndexProjection
///
/// Complete accepted index set prepared for one authoritative row traversal.
///

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
            let prepared = PreparedUserIndex::from_accepted_index(index, predicate_row_contract)?;
            if index.unique() {
                unique_index_ids.insert(IndexId::new_with_generation(
                    entity_tag,
                    index.ordinal(),
                    index.physical_generation(),
                ));
            }
            indexes.push(prepared);
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
        slots: &dyn CanonicalSlotReader,
        entries: &mut Vec<StagedUserIndexDomainEntry>,
        budget: &mut StagedUserIndexDomainBudget,
    ) -> Result<(), StagedUserIndexDomainError> {
        for index in &self.indexes {
            budget.consume_projection_work()?;
            let Some(key) = index.derive_key(entity_tag, row, slots)? else {
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
    accepted_before_row_contract: Option<&StructuralRowContract>,
    accepted_after_row_contract: Option<&StructuralRowContract>,
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
    if [accepted_before_row_contract, accepted_after_row_contract]
        .into_iter()
        .flatten()
        .any(|row_contract| row_contract.entity_path() != accepted_before_identity.entity_path())
    {
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
    authority: ProjectionAuthority,
    entity_path: &'static str,
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
            return Err(match authority {
                ProjectionAuthority::AcceptedBefore => {
                    StagedUserIndexDomainError::DuplicateUniqueKey
                }
                ProjectionAuthority::CandidateAfter => {
                    StagedUserIndexDomainError::CandidateUniqueConflict { entity_path }
                }
            });
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

///
/// StagedUserIndexDomainBudget
///
/// Incremental private budget shared by both projections and physical observation.
///

struct StagedUserIndexDomainBudget {
    source: SchemaTransitionSourceBudget,
    usage: StagedUserIndexDomainUsage,
}

impl StagedUserIndexDomainBudget {
    const fn standard() -> Self {
        Self {
            source: SchemaTransitionSourceBudget::standard(),
            usage: StagedUserIndexDomainUsage {
                source_rows: 0,
                source_row_bytes: 0,
                accepted_before_entries: 0,
                accepted_after_entries: 0,
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
        self.source
            .consume_source_row(encoded_row_bytes)
            .map_err(StagedUserIndexDomainError::BudgetExceeded)
    }

    fn consume_projection_work(&mut self) -> Result<(), StagedUserIndexDomainError> {
        self.usage.projection_work_units = self.usage.projection_work_units.checked_add(1).ok_or(
            StagedUserIndexDomainError::BudgetExceeded(
                SchemaTransitionBudgetResource::ProjectionWorkUnits,
            ),
        )?;
        if self.usage.projection_work_units > MAX_SCHEMA_PROJECTION_WORK_UNITS {
            return Err(StagedUserIndexDomainError::BudgetExceeded(
                SchemaTransitionBudgetResource::ProjectionWorkUnits,
            ));
        }

        Ok(())
    }

    fn consume_projection_entry(
        &mut self,
        key_bytes: usize,
    ) -> Result<(), StagedUserIndexDomainError> {
        self.usage.projection_entries = self.usage.projection_entries.checked_add(1).ok_or(
            StagedUserIndexDomainError::BudgetExceeded(
                SchemaTransitionBudgetResource::ProjectionEntries,
            ),
        )?;
        if self.usage.projection_entries > MAX_SCHEMA_PROJECTION_ENTRIES {
            return Err(StagedUserIndexDomainError::BudgetExceeded(
                SchemaTransitionBudgetResource::ProjectionEntries,
            ));
        }
        self.consume_staged_bytes(
            key_bytes
                .checked_add(1)
                .and_then(|bytes| bytes.checked_add(size_of::<StagedUserIndexDomainEntry>()))
                .ok_or(StagedUserIndexDomainError::BudgetExceeded(
                    SchemaTransitionBudgetResource::StagedRawBytes,
                ))?,
        )
    }

    fn consume_deletion_key(&mut self, key_bytes: usize) -> Result<(), StagedUserIndexDomainError> {
        self.usage.deletion_keys = self.usage.deletion_keys.checked_add(1).ok_or(
            StagedUserIndexDomainError::BudgetExceeded(
                SchemaTransitionBudgetResource::DeletionKeys,
            ),
        )?;
        if self.usage.deletion_keys > MAX_DELETION_KEYS {
            return Err(StagedUserIndexDomainError::BudgetExceeded(
                SchemaTransitionBudgetResource::DeletionKeys,
            ));
        }
        self.consume_staged_bytes(
            key_bytes
                .checked_add(1)
                .and_then(|bytes| bytes.checked_add(size_of::<StagedUserIndexDomainEntry>()))
                .ok_or(StagedUserIndexDomainError::BudgetExceeded(
                    SchemaTransitionBudgetResource::StagedRawBytes,
                ))?,
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
            .ok_or(StagedUserIndexDomainError::BudgetExceeded(
                SchemaTransitionBudgetResource::StagedRawBytes,
            ))?;
        let deletion_workspace = deletion_keys
            .checked_mul(size_of::<RawIndexStoreKey>())
            .ok_or(StagedUserIndexDomainError::BudgetExceeded(
                SchemaTransitionBudgetResource::StagedRawBytes,
            ))?;
        self.consume_staged_bytes(entry_workspace.checked_add(deletion_workspace).ok_or(
            StagedUserIndexDomainError::BudgetExceeded(
                SchemaTransitionBudgetResource::StagedRawBytes,
            ),
        )?)
    }

    fn consume_staged_bytes(&mut self, bytes: usize) -> Result<(), StagedUserIndexDomainError> {
        self.usage.staged_raw_bytes = self.usage.staged_raw_bytes.checked_add(bytes).ok_or(
            StagedUserIndexDomainError::BudgetExceeded(
                SchemaTransitionBudgetResource::StagedRawBytes,
            ),
        )?;
        if self.usage.staged_raw_bytes > MAX_SCHEMA_STAGED_RAW_BYTES {
            return Err(StagedUserIndexDomainError::BudgetExceeded(
                SchemaTransitionBudgetResource::StagedRawBytes,
            ));
        }

        Ok(())
    }

    const fn record_projection_counts(
        &mut self,
        accepted_before_entries: usize,
        accepted_after_entries: usize,
    ) {
        self.usage.accepted_before_entries = accepted_before_entries;
        self.usage.accepted_after_entries = accepted_after_entries;
    }

    const fn usage(&self) -> StagedUserIndexDomainUsage {
        StagedUserIndexDomainUsage {
            source_rows: self.source.source_rows(),
            source_row_bytes: self.source.source_row_bytes(),
            ..self.usage
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stage_budget_rejections_preserve_the_exact_resource() {
        for resource in [
            SchemaTransitionBudgetResource::SourceRows,
            SchemaTransitionBudgetResource::SourceRowBytes,
            SchemaTransitionBudgetResource::ProjectionEntries,
            SchemaTransitionBudgetResource::DeletionKeys,
            SchemaTransitionBudgetResource::ProjectionWorkUnits,
            SchemaTransitionBudgetResource::StagedRawBytes,
        ] {
            let internal =
                StagedUserIndexDomainError::BudgetExceeded(resource).into_internal_error();
            assert!(matches!(
                internal.detail(),
                Some(crate::error::ErrorDetail::Store(
                    crate::error::StoreError::SchemaTransitionBudgetExceeded {
                        resource: actual,
                    }
                )) if *actual == resource
            ));
        }
    }

    #[test]
    fn standard_budget_fixes_and_admits_the_design_measurement_point() {
        let design_rows = std::hint::black_box(2_048usize);
        let design_indexes = std::hint::black_box(8usize);
        let design_domain_entries = design_rows * design_indexes;
        let design_projection_entries = design_domain_entries * 2;
        let design_work_units = design_domain_entries * 3;

        assert_eq!(MAX_SCHEMA_PROJECTION_ENTRIES, 131_072);
        assert_eq!(MAX_DELETION_KEYS, 65_536);
        assert_eq!(MAX_SCHEMA_STAGED_RAW_BYTES, 256 * 1024 * 1024);
        assert_eq!(MAX_SCHEMA_PROJECTION_WORK_UNITS, 262_144);
        let mut source = SchemaTransitionSourceBudget::standard();
        for _ in 0..design_rows {
            source
                .consume_source_row(0)
                .expect("the design row count should fit the shared source budget");
        }
        assert!(MAX_SCHEMA_PROJECTION_ENTRIES >= design_projection_entries);
        assert!(MAX_DELETION_KEYS >= design_domain_entries);
        assert!(MAX_SCHEMA_PROJECTION_WORK_UNITS >= design_work_units);
    }

    #[test]
    fn standard_budget_rejects_each_aggregate_dimension_at_its_boundary() {
        let mut entry_count = StagedUserIndexDomainBudget::standard();
        for _ in 0..MAX_SCHEMA_PROJECTION_ENTRIES {
            assert!(entry_count.consume_projection_entry(0).is_ok());
        }
        assert!(matches!(
            entry_count.consume_projection_entry(0),
            Err(StagedUserIndexDomainError::BudgetExceeded(
                SchemaTransitionBudgetResource::ProjectionEntries,
            )),
        ));

        let mut deletion_count = StagedUserIndexDomainBudget::standard();
        for _ in 0..MAX_DELETION_KEYS {
            assert!(deletion_count.consume_deletion_key(0).is_ok());
        }
        assert!(matches!(
            deletion_count.consume_deletion_key(0),
            Err(StagedUserIndexDomainError::BudgetExceeded(
                SchemaTransitionBudgetResource::DeletionKeys,
            )),
        ));

        let mut work = StagedUserIndexDomainBudget::standard();
        for _ in 0..MAX_SCHEMA_PROJECTION_WORK_UNITS {
            assert!(work.consume_projection_work().is_ok());
        }
        assert!(matches!(
            work.consume_projection_work(),
            Err(StagedUserIndexDomainError::BudgetExceeded(
                SchemaTransitionBudgetResource::ProjectionWorkUnits,
            )),
        ));

        let mut staged_bytes = StagedUserIndexDomainBudget::standard();
        assert!(matches!(
            staged_bytes.consume_staged_bytes(MAX_SCHEMA_STAGED_RAW_BYTES + 1),
            Err(StagedUserIndexDomainError::BudgetExceeded(
                SchemaTransitionBudgetResource::StagedRawBytes,
            )),
        ));
    }
}
