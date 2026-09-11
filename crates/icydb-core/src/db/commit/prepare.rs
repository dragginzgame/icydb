//! Module: db::commit::prepare
//! Responsibility: decode commit-marker row ops into mechanical store mutations.
//! Does not own: marker persistence, commit-window lifecycle, or recovery orchestration.
//! Boundary: commit::marker -> commit::prepare -> commit::apply (one-way).

use crate::{
    db::{
        Db,
        commit::{
            CommitRowOp, CommitSchemaFingerprint, PreparedIndexMutation, PreparedRowCommitOp,
        },
        data::{
            AcceptedStructuralRowAuthority, CanonicalRow, CanonicalSlotReader, DataStore,
            DecodedDataStoreKey, RawDataStoreKey, RawRow, StructuralRowContract,
            StructuralSlotReader, canonical_row_from_structural_slot_reader_with_accepted_contract,
        },
        index::{
            IndexDelta, IndexDeltaGroup, IndexEntryValue, IndexMembershipDelta, IndexMutationPlan,
            IndexPlanReadView, IndexReadContract, IndexRowIdentity, IndexStore, RawIndexStoreKey,
            StructuralIndexEntryReader, StructuralPrimaryRowReader,
            plan_index_mutation_for_slot_reader_structural,
        },
        key_taxonomy::PrimaryKeyValue,
        relation::{
            RelationCommitBudget, RelationConstraintProjection, RelationProjectionBudget,
            ReverseRelationSourceInfo,
        },
        schema::{
            AcceptedCatalogSnapshotSelection, ConstraintActivationKind, ConstraintId, SchemaInfo,
            UniqueConstraintProjection,
        },
    },
    error::{AcceptedConstraintFactContext, ErrorClass, InternalError},
    traits::CanisterKind,
    types::EntityTag,
};
use std::{cell::RefCell, ops::Bound, rc::Rc, thread::LocalKey};

///
/// CommitPrepareAuthority
///
/// Resolved authority needed by nongeneric commit-preparation stages.
///

#[derive(Clone)]
struct CommitPrepareAuthority {
    entity_path: Rc<str>,
    entity_tag: EntityTag,
    schema_fingerprint: crate::db::commit::CommitSchemaFingerprint,
    data_store_path: &'static str,
    relation_source: ReverseRelationSourceInfo,
}

/// Accepted storage-backed constraint schedule shared by one commit batch.

struct AcceptedStorageConstraintSchedule {
    row_contract: StructuralRowContract,
    schema_info: Option<SchemaInfo>,
    candidate_unique: Option<CandidateUniqueCommitContract>,
    relations: Vec<RelationConstraintProjection>,
}

/// One pending unique owner whose staged generation must track safe deletes.

struct CandidateUniqueCommitContract {
    constraint_id: ConstraintId,
    projection: UniqueConstraintProjection,
    index_store: &'static LocalKey<RefCell<IndexStore>>,
}

/// Immutable accepted-schema authority shared by every row in one commit batch.
pub(in crate::db) struct CommitPrepareContext {
    authority: CommitPrepareAuthority,
    constraint_schedule: AcceptedStorageConstraintSchedule,
    mode: CommitPrepareMode,
}

impl CommitPrepareContext {
    /// Accepted entity identity retained with this preparation authority.
    pub(in crate::db) const fn entity_tag(&self) -> EntityTag {
        self.authority.entity_tag
    }
}

/// Reuse immutable accepted setup within one synchronous preparation operation.
/// The caller chooses current/canonical authority; this owner only shares its
/// successful contexts. Never retain it across callbacks or catalog mutations.
pub(in crate::db) struct CommitPrepareContextCache {
    mode: CommitPrepareMode,
    contexts: Vec<CommitPrepareContext>,
}

impl CommitPrepareContextCache {
    pub(in crate::db) const fn new(mode: CommitPrepareMode) -> Self {
        Self {
            mode,
            contexts: Vec::new(),
        }
    }

    /// Resolve once per entity/schema pair, without caching failed preparation.
    pub(in crate::db) fn get_or_prepare(
        &mut self,
        entity_path: &str,
        fingerprint: CommitSchemaFingerprint,
        prepare: impl FnOnce(CommitPrepareMode) -> Result<CommitPrepareContext, InternalError>,
    ) -> Result<&CommitPrepareContext, InternalError> {
        let index = if let Some(index) = self.contexts.iter().position(|context| {
            context.authority.entity_path.as_ref() == entity_path
                && context.authority.schema_fingerprint == fingerprint
        }) {
            index
        } else {
            let context = prepare(self.mode)?;
            let index = self.contexts.len();
            self.contexts.push(context);
            index
        };
        self.contexts
            .get(index)
            .ok_or_else(InternalError::query_executor_invariant)
    }
}

#[derive(Clone, Copy)]
pub(in crate::db) enum CommitPrepareMode {
    NormalWrite,
    RecoveryReplay,
    DerivedRebuild,
}

impl CommitPrepareMode {
    const fn include_candidate_relation_effects(self) -> bool {
        matches!(self, Self::NormalWrite | Self::RecoveryReplay)
    }

    const fn validate_relation_targets(self) -> bool {
        matches!(self, Self::NormalWrite)
    }
}

impl CommitPrepareAuthority {
    fn from_catalog_selection(
        selection: &AcceptedCatalogSnapshotSelection,
        schema_fingerprint: CommitSchemaFingerprint,
    ) -> Self {
        let identity = selection.identity();
        let entity_path = identity.entity_path_handle();
        let entity_tag = identity.entity_tag();
        Self {
            relation_source: ReverseRelationSourceInfo::new(entity_path.clone(), entity_tag),
            entity_path,
            entity_tag,
            schema_fingerprint,
            data_store_path: identity.store_path(),
        }
    }
}

///
/// CommitInputs
///
/// Structural commit inputs decoded before forward-index planning runs.
///

struct CommitInputs {
    raw_key: RawDataStoreKey,
    data_key: DecodedDataStoreKey,
    rows: CommitRowImages<RawRow>,
}

impl CommitInputs {
    /// Build the canonical schema-fingerprint mismatch mapping for structural commit inputs.
    fn schema_fingerprint_mismatch(
        _entity_path: &str,
        _marker: crate::db::commit::CommitSchemaFingerprint,
        _runtime: crate::db::commit::CommitSchemaFingerprint,
    ) -> InternalError {
        InternalError::store_unsupported()
    }
}

///
/// CommitRowImages
///
/// Byte-identical before/after images share construction, not semantics: both
/// sides remain present to every index and relation consumer. The same shape
/// carries raw rows and their validated readers without another ownership layer.
///

enum CommitRowImages<T> {
    Shared(T),
    Distinct { before: Option<T>, after: Option<T> },
}

impl<T> CommitRowImages<T> {
    const fn as_refs(&self) -> (Option<&T>, Option<&T>) {
        match self {
            Self::Shared(row) => (Some(row), Some(row)),
            Self::Distinct { before, after } => (before.as_ref(), after.as_ref()),
        }
    }

    // Preserve sharing through fallible construction; neither a partly mapped
    // pair nor a successful first side escapes if the other side rejects.
    fn try_map_ref<'a, U>(
        &'a self,
        mut map: impl FnMut(&'a T) -> Result<U, InternalError>,
    ) -> Result<CommitRowImages<U>, InternalError> {
        match self {
            Self::Shared(row) => Ok(CommitRowImages::Shared(map(row)?)),
            Self::Distinct { before, after } => Ok(CommitRowImages::Distinct {
                before: before.as_ref().map(&mut map).transpose()?,
                after: after.as_ref().map(map).transpose()?,
            }),
        }
    }
}

impl CommitRowImages<RawRow> {
    fn from_bytes(before: Option<&[u8]>, after: Option<&[u8]>) -> Result<Self, InternalError> {
        // Reject oversized input before the equality walk or destination copy.
        for bytes in [before, after].into_iter().flatten() {
            RawRow::ensure_size(bytes)?;
        }
        // Exact persisted bytes under one accepted contract can share all row
        // validation. Semantically equivalent but differently encoded rows cannot.
        if let (Some(before), Some(after)) = (before, after)
            && before == after
        {
            return Ok(Self::Shared(RawRow::from_untrusted_bytes(after.to_vec())?));
        }
        Ok(Self::Distinct {
            before: before
                .map(|bytes| RawRow::from_untrusted_bytes(bytes.to_vec()))
                .transpose()?,
            after: after
                .map(|bytes| RawRow::from_untrusted_bytes(bytes.to_vec()))
                .transpose()?,
        })
    }
}

///
/// CommitIndexPlanReadView
///
/// Commit-owned adapter that resolves schema index stores to concrete stores
/// before delegating reads to the active preflight reader view. Keeping this
/// adapter here prevents index planning from depending on registry or executor
/// state.
///

struct CommitIndexPlanReadView<'a, C: CanisterKind> {
    db: &'a Db<C>,
    row_reader: &'a dyn StructuralPrimaryRowReader,
    index_reader: &'a dyn StructuralIndexEntryReader,
}

impl<C> CommitIndexPlanReadView<'_, C>
where
    C: CanisterKind,
{
    /// Resolve the store handle for one schema-owned index store path.
    fn index_store(
        &self,
        index_store: &str,
    ) -> Result<&'static LocalKey<RefCell<crate::db::index::IndexStore>>, InternalError> {
        self.db
            .with_store_registry(|registry| registry.try_get_store(index_store))
            .map(|store| store.index_store())
    }
}

impl<C> IndexPlanReadView for CommitIndexPlanReadView<'_, C>
where
    C: CanisterKind,
{
    fn read_primary_row(&self, key: &DecodedDataStoreKey) -> Result<Option<RawRow>, InternalError> {
        self.row_reader.read_primary_row(key)
    }

    fn has_primary_row_override(&self, key: &DecodedDataStoreKey) -> Result<bool, InternalError> {
        self.row_reader.has_primary_row_override(key)
    }

    fn read_index_entry(
        &self,
        index: IndexReadContract<'_>,
        key: &RawIndexStoreKey,
    ) -> Result<Option<IndexEntryValue>, InternalError> {
        let index_store = self.index_store(index.store_path())?;

        self.index_reader.read_index_entry(index_store, key)
    }

    fn read_index_keys_in_raw_range(
        &self,
        index: IndexReadContract<'_>,
        bounds: (&Bound<RawIndexStoreKey>, &Bound<RawIndexStoreKey>),
        limit: usize,
    ) -> Result<Vec<PrimaryKeyValue>, InternalError> {
        let index_store = self.index_store(index.store_path())?;

        self.index_reader
            .read_index_keys_in_raw_range(index_store, bounds, limit)
    }
}

/// Build operation-local commit authority from the caller's accepted selection.
/// The caller owns current/canonical selection; mechanical preparation mode
/// must not silently select a different schema. The recorded row fingerprint
/// stays separate because validated historical replay can use a later schema.
pub(in crate::db) fn prepare_commit_context_from_catalog_selection<C: CanisterKind>(
    db: &Db<C>,
    selection: &AcceptedCatalogSnapshotSelection,
    schema_fingerprint: CommitSchemaFingerprint,
    mode: CommitPrepareMode,
) -> Result<CommitPrepareContext, InternalError> {
    let authority = CommitPrepareAuthority::from_catalog_selection(selection, schema_fingerprint);
    let constraint_schedule =
        accepted_storage_constraint_schedule(db, &authority, selection, mode)?;

    Ok(CommitPrepareContext {
        authority,
        constraint_schedule,
        mode,
    })
}

/// Prepare one row while borrowing batch-resolved accepted-schema authority.
pub(in crate::db) fn prepare_row_commit_with_context<C: CanisterKind>(
    db: &Db<C>,
    op: &CommitRowOp,
    context: &CommitPrepareContext,
    row_reader: &dyn StructuralPrimaryRowReader,
    index_reader: &dyn StructuralIndexEntryReader,
    relation_budget: &mut RelationCommitBudget,
) -> Result<PreparedRowCommitOp, InternalError> {
    prepare_row_commit_for_entity_impl(db, op, context, row_reader, index_reader, relation_budget)
}

// Decode both optional commit-marker row images through the structural row
// boundary once so malformed fields fail closed before index planning.
fn decode_commit_marker_rows_for_preflight<'a>(
    data_key: &DecodedDataStoreKey,
    rows: &'a CommitRowImages<RawRow>,
    row_contract: &'a StructuralRowContract,
) -> Result<CommitRowImages<StructuralSlotReader<'a>>, InternalError> {
    rows.try_map_ref(|row| decode_commit_marker_structural_slots(data_key, row, row_contract))
}

// Keep the full commit-preparation body out of the thin wrapper entrypoints so
// codegen does not clone the same logic into both prepare surfaces per entity.
#[inline(never)]
fn prepare_row_commit_for_entity_impl<C>(
    db: &Db<C>,
    op: &CommitRowOp,
    context: &CommitPrepareContext,
    row_reader: &dyn StructuralPrimaryRowReader,
    index_reader: &dyn StructuralIndexEntryReader,
    relation_budget: &mut RelationCommitBudget,
) -> Result<PreparedRowCommitOp, InternalError>
where
    C: crate::traits::CanisterKind,
{
    // Phase 1: resolve accepted marker authority before structural row decode
    // so path/schema mismatches fail before constraint or maintenance work.
    let authority = &context.authority;
    let constraint_schedule = &context.constraint_schedule;
    let structural = prepare_row_commit_structural_inputs(op, authority, context.mode)?;

    // Phase 2: decode the persisted row images once through the structural
    // slot-reader boundary before any forward-index planning runs.
    let (decoded, forward_index_ops) = {
        let decoded = decode_commit_marker_rows_for_preflight(
            &structural.data_key,
            &structural.rows,
            &constraint_schedule.row_contract,
        )?;

        // Phase 3: derive forward index work from the already validated
        // structural rows when the entity owns secondary indexes.
        let index_plan = if constraint_schedule.schema_info.is_some() {
            prepare_forward_index_commit_leaf(
                db,
                authority,
                row_reader,
                index_reader,
                constraint_schedule,
                op.mutation_diagnostic_context,
                &structural.data_key,
                &decoded,
            )?
        } else {
            empty_forward_index_plan()
        };
        let mut forward_index_ops = materialize_forward_index_commit_ops(db, index_plan)?;
        let (old_slots, new_slots) = decoded.as_refs();
        forward_index_ops.extend(prepare_candidate_unique_index_commit_ops(
            constraint_schedule.candidate_unique.as_ref(),
            authority,
            op.mutation_diagnostic_context,
            &structural.data_key,
            old_slots,
            new_slots,
        )?);

        (decoded, forward_index_ops)
    };

    let source_primary_key = structural.data_key.primary_key_value();
    let mut reverse_index_ops = Vec::new();
    let mut old_relation_budget = RelationProjectionBudget::default();
    let mut new_relation_budget = RelationProjectionBudget::default();
    let (old_slots, new_slots) = decoded.as_refs();
    for relation in &constraint_schedule.relations {
        reverse_index_ops.extend(relation.prepare_source_transition(
            row_reader,
            context.mode.validate_relation_targets(),
            authority.schema_fingerprint,
            op.mutation_diagnostic_context,
            &source_primary_key,
            old_slots,
            new_slots,
            &mut old_relation_budget,
            &mut new_relation_budget,
            relation_budget,
        )?);
    }
    let data_value = new_slots
        .map(canonical_row_from_structural_slot_reader_with_accepted_contract)
        .transpose()?;

    finalize_row_commit_structural(
        db,
        authority.clone(),
        structural.raw_key,
        forward_index_ops,
        reverse_index_ops,
        data_value,
    )
}

// Return one empty forward-index plan when the entity has no secondary indexes.
const fn empty_forward_index_plan() -> IndexMutationPlan {
    IndexMutationPlan::new(Vec::new())
}

// Decode only the structural row views required for forward-index planning and
// produce structural-ready forward-index outputs.
#[expect(
    clippy::too_many_arguments,
    reason = "the commit leaf receives one existing structural plan plus the per-operation diagnostic identity"
)]
fn prepare_forward_index_commit_leaf<C>(
    db: &Db<C>,
    authority: &CommitPrepareAuthority,
    row_reader: &dyn StructuralPrimaryRowReader,
    index_reader: &dyn StructuralIndexEntryReader,
    constraint_schedule: &AcceptedStorageConstraintSchedule,
    mutation: Option<crate::error::MutationDiagnosticContext>,
    data_key: &DecodedDataStoreKey,
    decoded: &CommitRowImages<StructuralSlotReader<'_>>,
) -> Result<IndexMutationPlan, InternalError>
where
    C: crate::traits::CanisterKind,
{
    let Some(schema_info) = constraint_schedule.schema_info.as_ref() else {
        return Ok(empty_forward_index_plan());
    };
    let primary_key = data_key.primary_key_value();

    let read_view = CommitIndexPlanReadView {
        db,
        row_reader,
        index_reader,
    };
    let (old_slots, new_slots) = decoded.as_refs();

    match plan_index_mutation_for_slot_reader_structural(
        authority.entity_tag,
        authority.schema_fingerprint,
        mutation,
        schema_info,
        &read_view,
        &constraint_schedule.row_contract,
        old_slots.map(|_| &primary_key),
        old_slots.map(|slots| slots as &dyn CanonicalSlotReader),
        new_slots.map(|_| &primary_key),
        new_slots.map(|slots| slots as &dyn CanonicalSlotReader),
    ) {
        Ok(index_plan) => Ok(index_plan),
        Err(err) => Err(err.into_internal_error()),
    }
}

// Decode one commit-marker row into one validated slot reader so both
// hardening and forward-index planning share the same structural row boundary.
fn decode_commit_marker_structural_slots<'a>(
    data_key: &DecodedDataStoreKey,
    row: &'a RawRow,
    row_contract: &'a StructuralRowContract,
) -> Result<StructuralSlotReader<'a>, InternalError> {
    let slots =
        StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(row, row_contract)
            .map_err(|err| {
                if err.class() == ErrorClass::IncompatiblePersistedFormat {
                    InternalError::serialize_incompatible_persisted_format()
                } else {
                    InternalError::serialize_corruption()
                }
            })?;
    slots
        .validate_primary_key(data_key)
        .map_err(|_| InternalError::store_corruption())?;

    Ok(slots)
}

// Build the accepted-schema contracts used by commit preflight.
//
// Commit preparation may need to inspect committed rows while rebuilding
// unique-index proofs and reverse-index mutations. Those storage reads must use
// the same accepted row and index contracts as mutation staging so rows from
// an earlier accepted append-only layout revision remain valid when nullable
// fields have since been appended.
fn accepted_storage_constraint_schedule<C>(
    db: &Db<C>,
    authority: &CommitPrepareAuthority,
    selection: &AcceptedCatalogSnapshotSelection,
    mode: CommitPrepareMode,
) -> Result<AcceptedStorageConstraintSchedule, InternalError>
where
    C: CanisterKind,
{
    let accepted_authority = AcceptedStructuralRowAuthority::from_catalog_selection(
        authority.entity_path.as_ref(),
        selection,
    )?;
    let value_catalog = selection.value_catalog_handle().clone();
    let (accepted, row_contract) = accepted_authority.into_parts();
    let candidate_unique = candidate_unique_commit_contract(
        db,
        authority.entity_tag,
        accepted.persisted_snapshot(),
        &row_contract,
    )?;
    let relations = mutation_relation_constraint_schedule(
        db,
        authority.relation_source.clone(),
        accepted.persisted_snapshot(),
        &row_contract,
        mode.include_candidate_relation_effects(),
    )?;
    Ok(AcceptedStorageConstraintSchedule {
        row_contract,
        schema_info: (!accepted.persisted_snapshot().indexes().is_empty()).then(|| {
            SchemaInfo::from_accepted_snapshot_and_catalog(&accepted, value_catalog, true)
        }),
        candidate_unique,
        relations,
    })
}

fn mutation_relation_constraint_schedule<C: CanisterKind>(
    db: &Db<C>,
    source: ReverseRelationSourceInfo,
    snapshot: &crate::db::schema::PersistedSchemaSnapshot,
    row_contract: &StructuralRowContract,
    include_candidate_relation: bool,
) -> Result<Vec<RelationConstraintProjection>, InternalError> {
    let mut projections = Vec::with_capacity(
        snapshot
            .relations()
            .len()
            .saturating_add(usize::from(include_candidate_relation)),
    );
    for relation in snapshot.relations() {
        projections.push(RelationConstraintProjection::new_active(
            db,
            source.clone(),
            snapshot,
            row_contract,
            relation,
        )?);
    }
    if !include_candidate_relation {
        return Ok(projections);
    }

    let [candidate] = snapshot.candidate_relations() else {
        if snapshot.candidate_relations().is_empty() {
            return Ok(projections);
        }
        return Err(InternalError::store_corruption());
    };
    let activation = snapshot
        .constraint_activations()
        .iter()
        .find(|activation| {
            matches!(
                activation.kind(),
                ConstraintActivationKind::Relation { relation_id }
                    if *relation_id == candidate.id()
            )
        })
        .ok_or_else(InternalError::store_corruption)?;
    if candidate.physical_generation() != activation.activation_epoch() {
        return Err(InternalError::store_corruption());
    }
    projections.push(RelationConstraintProjection::new(
        db,
        source,
        snapshot,
        row_contract,
        candidate,
    )?);
    Ok(projections)
}

fn candidate_unique_commit_contract<C: CanisterKind>(
    db: &Db<C>,
    entity_tag: EntityTag,
    snapshot: &crate::db::schema::PersistedSchemaSnapshot,
    row_contract: &StructuralRowContract,
) -> Result<Option<CandidateUniqueCommitContract>, InternalError> {
    let [candidate] = snapshot.candidate_indexes() else {
        if snapshot.candidate_indexes().is_empty() {
            return Ok(None);
        }
        return Err(InternalError::store_corruption());
    };
    let activation = snapshot
        .constraint_activations()
        .iter()
        .find(|activation| {
            matches!(
                activation.kind(),
                ConstraintActivationKind::Unique { index_id }
                    if *index_id == candidate.schema_id()
            )
        })
        .ok_or_else(InternalError::store_corruption)?;
    let projection = UniqueConstraintProjection::new(entity_tag, candidate, row_contract)?;
    let index_store = db
        .with_store_registry(|registry| registry.try_get_store(candidate.store()))?
        .index_store();
    Ok(Some(CandidateUniqueCommitContract {
        constraint_id: activation.id(),
        projection,
        index_store,
    }))
}

fn prepare_candidate_unique_index_commit_ops(
    candidate: Option<&CandidateUniqueCommitContract>,
    authority: &CommitPrepareAuthority,
    mutation: Option<crate::error::MutationDiagnosticContext>,
    data_key: &DecodedDataStoreKey,
    old_slots: Option<&StructuralSlotReader<'_>>,
    new_slots: Option<&StructuralSlotReader<'_>>,
) -> Result<Vec<PreparedIndexMutation>, InternalError> {
    let Some(candidate) = candidate else {
        return Ok(Vec::new());
    };
    let primary_key = data_key.primary_key_value();
    let old_key = old_slots
        .map(|slots| candidate.projection.derive_key(&primary_key, slots))
        .transpose()?
        .flatten();
    let new_key = new_slots
        .map(|slots| candidate.projection.derive_key(&primary_key, slots))
        .transpose()?
        .flatten();
    match (old_key, new_key) {
        (Some(old_key), None) => Ok(vec![PreparedIndexMutation::new(
            candidate.index_store,
            old_key,
            None,
        )]),
        (old_key, new_key) if old_slots.is_some() && new_slots.is_some() => {
            if old_key != new_key {
                return Err(InternalError::mutation_constraint_activation_write_blocked(
                    AcceptedConstraintFactContext::write_admission(
                        crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
                        authority.schema_fingerprint,
                        authority.entity_tag.value(),
                        candidate.constraint_id.get(),
                        icydb_diagnostic_code::DiagnosticConstraintKind::Unique,
                        mutation,
                        None,
                    ),
                ));
            }
            Ok(Vec::new())
        }
        (None, None | Some(_)) => Ok(Vec::new()),
        (Some(_), Some(_)) => Err(InternalError::store_invariant()),
    }
}

// Decode structural commit inputs before the typed forward-index leaf runs.
fn prepare_row_commit_structural_inputs(
    op: &CommitRowOp,
    authority: &CommitPrepareAuthority,
    mode: CommitPrepareMode,
) -> Result<CommitInputs, InternalError> {
    if op.entity_path.as_ref() != authority.entity_path.as_ref() {
        return Err(InternalError::store_corruption());
    }
    if op.schema_fingerprint != authority.schema_fingerprint {
        return Err(CommitInputs::schema_fingerprint_mismatch(
            authority.entity_path.as_ref(),
            op.schema_fingerprint,
            authority.schema_fingerprint,
        ));
    }

    let raw_key = op.key.clone();
    let data_key = DecodedDataStoreKey::try_from_raw(&raw_key)
        .map_err(|_| InternalError::store_corruption())?;
    let rows = CommitRowImages::from_bytes(op.before.as_deref(), op.after.as_deref())?;

    // A marker-owned deletion may already be reflected in direct storage.
    // Replay still validates key/schema authority, but absence is its correct
    // terminal state. Ordinary writes cannot prepare an empty transition.
    if op.before.is_none()
        && op.after.is_none()
        && !matches!(mode, CommitPrepareMode::RecoveryReplay)
    {
        return Err(InternalError::store_corruption());
    }

    Ok(CommitInputs {
        raw_key,
        data_key,
        rows,
    })
}

// Resume structural orchestration after the typed forward-index leaf has
// produced structural-ready outputs.
fn finalize_row_commit_structural<C>(
    db: &Db<C>,
    authority: CommitPrepareAuthority,
    data_key: RawDataStoreKey,
    forward_index_ops: Vec<PreparedIndexMutation>,
    reverse_index_ops: Vec<PreparedIndexMutation>,
    data_value: Option<CanonicalRow>,
) -> Result<PreparedRowCommitOp, InternalError>
where
    C: crate::traits::CanisterKind,
{
    let data_store = db.with_store_registry(|reg| reg.try_get_store(authority.data_store_path))?;

    Ok(materialize_prepared_row_commit(
        forward_index_ops,
        reverse_index_ops,
        data_store.data_store(),
        data_store.index_store(),
        data_key,
        data_value,
    ))
}

// Materialize one prepared row commit entirely from structural planning outputs.
fn materialize_prepared_row_commit(
    forward_index_ops: Vec<PreparedIndexMutation>,
    reverse_index_ops: Vec<PreparedIndexMutation>,
    data_store: &'static LocalKey<RefCell<DataStore>>,
    data_index_store: &'static LocalKey<RefCell<IndexStore>>,
    data_key: RawDataStoreKey,
    data_value: Option<CanonicalRow>,
) -> PreparedRowCommitOp {
    let mut index_ops = forward_index_ops;
    index_ops.reserve(reverse_index_ops.len());
    index_ops.extend(reverse_index_ops);

    PreparedRowCommitOp {
        index_ops,
        data_store,
        data_index_store,
        data_key,
        data_value,
    }
}

// Convert index-domain deltas into commit-owned raw index operations. This is
// the first layer that knows both the active preflight reader view and the
// commit op shape.
fn materialize_forward_index_commit_ops<C>(
    db: &Db<C>,
    index_plan: IndexMutationPlan,
) -> Result<Vec<PreparedIndexMutation>, InternalError>
where
    C: crate::traits::CanisterKind,
{
    let mut commit_ops = Vec::with_capacity(index_plan.groups.len().saturating_mul(2));

    for group in index_plan.groups {
        build_commit_ops_for_index_group(&mut commit_ops, db, group)?;
    }

    Ok(commit_ops)
}

// Materialize one per-index delta group. Same-key membership is normally
// removed by index planning; retaining the no-op check here keeps manually
// constructed internal plans from producing redundant stable-index writes.
fn build_commit_ops_for_index_group<C>(
    commit_ops: &mut Vec<PreparedIndexMutation>,
    db: &Db<C>,
    group: IndexDeltaGroup,
) -> Result<(), InternalError>
where
    C: crate::traits::CanisterKind,
{
    let mut remove_delta = None;
    let mut insert_delta = None;
    let index_store = db
        .with_store_registry(|registry| registry.try_get_store(group.index_store.as_str()))
        .map(|store| store.index_store())?;

    for delta in group.deltas {
        match delta {
            IndexDelta::Remove(delta) => remove_delta = Some(delta),
            IndexDelta::Insert(delta) => insert_delta = Some(delta),
        }
    }

    build_commit_ops_for_index_delta_pair(commit_ops, index_store, remove_delta, insert_delta)?;

    Ok(())
}

// Compute commit-time index operations for one old/new membership pair.
fn build_commit_ops_for_index_delta_pair(
    commit_ops: &mut Vec<PreparedIndexMutation>,
    store: &'static LocalKey<RefCell<crate::db::index::IndexStore>>,
    remove_delta: Option<IndexMembershipDelta>,
    insert_delta: Option<IndexMembershipDelta>,
) -> Result<(), InternalError> {
    // Phase 1: same-key transitions preserve membership and need no write.
    if remove_delta
        .as_ref()
        .zip(insert_delta.as_ref())
        .is_some_and(|(old_delta, new_delta)| old_delta.key == new_delta.key)
    {
        return Ok(());
    }

    // Phase 2: different-key transitions can touch at most two keys. Preserve
    // deterministic key order without the general BTreeMap machinery.
    let mut first: Option<(
        RawIndexStoreKey,
        Option<IndexRowIdentity>,
        PreparedIndexMutationBuilder,
    )> = None;
    let mut second: Option<(
        RawIndexStoreKey,
        Option<IndexRowIdentity>,
        PreparedIndexMutationBuilder,
    )> = None;

    if let Some(remove_delta) = remove_delta {
        insert_commit_candidate(
            &mut first,
            &mut second,
            remove_delta.key.to_raw()?,
            None,
            PreparedIndexMutation::new,
        );
    }

    if let Some(insert_delta) = insert_delta {
        insert_commit_candidate(
            &mut first,
            &mut second,
            insert_delta.key.to_raw()?,
            Some(IndexRowIdentity::new(&insert_delta.primary_key)),
            PreparedIndexMutation::new,
        );
    }

    if let Some((raw_key, entry, build_commit_op)) = first {
        push_commit_op_for_index_entry(commit_ops, store, raw_key, entry, build_commit_op);
    }
    if let Some((raw_key, entry, build_commit_op)) = second {
        push_commit_op_for_index_entry(commit_ops, store, raw_key, entry, build_commit_op);
    }

    Ok(())
}

/// Insert one touched key into the small fixed-size ordered candidate set.
fn insert_commit_candidate(
    first: &mut Option<(
        RawIndexStoreKey,
        Option<IndexRowIdentity>,
        PreparedIndexMutationBuilder,
    )>,
    second: &mut Option<(
        RawIndexStoreKey,
        Option<IndexRowIdentity>,
        PreparedIndexMutationBuilder,
    )>,
    raw_key: RawIndexStoreKey,
    entry: Option<IndexRowIdentity>,
    build_commit_op: PreparedIndexMutationBuilder,
) {
    match first {
        None => *first = Some((raw_key, entry, build_commit_op)),
        Some((first_key, _, _)) if raw_key < *first_key => {
            *second = first.take();
            *first = Some((raw_key, entry, build_commit_op));
        }
        _ => *second = Some((raw_key, entry, build_commit_op)),
    }
}

type PreparedIndexMutationBuilder = fn(
    &'static LocalKey<RefCell<crate::db::index::IndexStore>>,
    RawIndexStoreKey,
    Option<IndexEntryValue>,
) -> PreparedIndexMutation;

// Encode one touched index entry into one deterministic commit operation.
fn push_commit_op_for_index_entry(
    commit_ops: &mut Vec<PreparedIndexMutation>,
    store: &'static LocalKey<RefCell<crate::db::index::IndexStore>>,
    raw_key: RawIndexStoreKey,
    entry: Option<IndexRowIdentity>,
    build_commit_op: PreparedIndexMutationBuilder,
) {
    let value = entry.map(|_| IndexEntryValue::presence());

    commit_ops.push(build_commit_op(store, raw_key, value));
}

#[cfg(test)]
mod tests {
    mod images;

    use super::CommitPrepareMode;

    #[test]
    fn commit_prepare_modes_separate_normal_admission_from_replay_and_rebuild() {
        assert!(CommitPrepareMode::NormalWrite.validate_relation_targets());
        assert!(CommitPrepareMode::NormalWrite.include_candidate_relation_effects());

        assert!(!CommitPrepareMode::RecoveryReplay.validate_relation_targets());
        assert!(CommitPrepareMode::RecoveryReplay.include_candidate_relation_effects());

        assert!(!CommitPrepareMode::DerivedRebuild.validate_relation_targets());
        assert!(!CommitPrepareMode::DerivedRebuild.include_candidate_relation_effects());
    }
}
