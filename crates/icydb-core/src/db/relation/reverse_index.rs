//! Module: relation::reverse_index
//! Responsibility: maintain reverse-index relation targets for relation consistency.
//! Does not own: planner query semantics or execution routing policies.
//! Boundary: applies relation reverse-index mutations during commit pathways.

mod target_keys;

use crate::{
    db::schema::{FieldStorageDecode, LeafCodec},
    db::{
        Db,
        commit::PreparedIndexMutation,
        data::{
            CanonicalSlotReader, DecodedDataStoreKey, RawDataStoreKey, RawRow, ScalarSlotValueRef,
            SlotReader, StructuralRowContract, StructuralSlotReader,
            decode_accepted_relation_target_primary_key_components_bytes,
            decode_runtime_value_from_accepted_field_contract,
        },
        direction::Direction,
        index::{
            BorrowedIndexKeyFrame, IndexEntryValue, IndexId, IndexKey, IndexKeyEncodeError,
            IndexKeyKind, IndexRowIdentity, IndexState, IndexStore, RawIndexStoreKey,
            StructuralPrimaryRowReader, raw_keys_for_component_prefix_with_kind,
        },
        key_taxonomy::{
            EncodedPrimaryKey, IndexStoreKeyKind, PrimaryKeyComponent, PrimaryKeyValue,
            encode_index_store_key,
        },
        registry::{StoreHandle, StoreRelationSourceCapability, StoreRelationTargetCapability},
        relation::{
            AcceptedRelationCardinality, AcceptedRelationTargetAuthority,
            AcceptedRelationTargetContract, AcceptedRelationTupleEdgeLocalComponent,
            accepted_relation_target_contract, accepted_relation_target_metadata_from_kind,
            accepted_relation_tuple_edge_descriptor, accepted_scalar_relation_cardinality,
            validate_relation_primary_key_component_kind,
        },
        schema::AcceptedFieldKind,
        schema::{
            AcceptedConstraintIdentity, AcceptedFieldDecodeContract, AcceptedRelationValueContract,
            AcceptedValueCatalogHandle, OwnedAcceptedRelationEdgeContract,
            OwnedAcceptedRelationSourceContract, PersistedRelationEdgeSnapshot,
            PersistedRelationPathStepSnapshot, PersistedRelationSourceSnapshot,
            PersistedSchemaSnapshot, RelationId, accepted_relation_path_terminal,
        },
    },
    error::{AcceptedConstraintFactContext, InternalError, MutationDiagnosticContext},
    traits::CanisterKind,
    types::EntityTag,
};
use std::{
    cell::RefCell, collections::BTreeSet, mem::size_of, ops::Bound, rc::Rc, thread::LocalKey,
};

use target_keys::RelationTargetKeys;

// All reverse relations share one reserved system-index ordinal. Exact
// RelationId bytes inside each key own semantic reverse-domain identity.
const RELATION_SYSTEM_INDEX_ORDINAL: u16 = u16::MAX;

const MAX_NESTED_RELATION_IMAGE_TRAVERSAL_WORK: u64 = 349_440;
const MAX_NESTED_RELATION_IMAGE_RAW_REFERENCES: u64 = 5_460;
const MAX_RELATION_BATCH_TRAVERSAL_WORK: u64 = 349_440;
const MAX_RELATION_BATCH_RAW_REFERENCES: u64 = 5_460;
const MAX_RELATION_BATCH_UNIQUE_TARGET_LOOKUPS: u64 = 3_276;
const MAX_RELATION_BATCH_REVERSE_DELTAS: u64 = 5_460;

/// Per-row-image counters shared by every nested relation projected in scope.
#[derive(Default)]
pub(in crate::db) struct RelationProjectionBudget {
    traversal_work: u64,
    raw_references: u64,
}

/// Cumulative relation counters shared by one complete atomic batch.
#[derive(Default)]
pub(in crate::db) struct RelationCommitBudget {
    traversal_work: u64,
    raw_references: u64,
    // One immutable final-row view per batch: cache misses as misses, while
    // counting every distinct lookup rather than only successful validation.
    target_lookup_results: Vec<(RawDataStoreKey, bool)>,
    // Successful whole-delete-set checks share the batch's immutable final view.
    validated_delete_sources: BTreeSet<(RelationId, RawDataStoreKey)>,
    reverse_deltas: u64,
}

impl RelationProjectionBudget {
    fn charge_traversal(
        &mut self,
        batch: &mut RelationCommitBudget,
        amount: usize,
    ) -> Result<(), InternalError> {
        let amount = u64::try_from(amount).map_err(|_| {
            relation_budget_error(
                icydb_diagnostic_code::DiagnosticExecutionBudgetResource::NestedValueSteps,
                MAX_NESTED_RELATION_IMAGE_TRAVERSAL_WORK,
                u64::MAX,
            )
        })?;
        self.traversal_work = charge_relation_counter(
            self.traversal_work,
            amount,
            MAX_NESTED_RELATION_IMAGE_TRAVERSAL_WORK,
            icydb_diagnostic_code::DiagnosticExecutionBudgetResource::NestedValueSteps,
        )?;
        batch.traversal_work = charge_relation_counter(
            batch.traversal_work,
            amount,
            MAX_RELATION_BATCH_TRAVERSAL_WORK,
            icydb_diagnostic_code::DiagnosticExecutionBudgetResource::NestedValueSteps,
        )?;
        Ok(())
    }

    fn charge_nested_references(
        &mut self,
        batch: &mut RelationCommitBudget,
        amount: usize,
    ) -> Result<(), InternalError> {
        let amount = u64::try_from(amount).map_err(|_| {
            relation_budget_error(
                icydb_diagnostic_code::DiagnosticExecutionBudgetResource::ResultRows,
                MAX_NESTED_RELATION_IMAGE_RAW_REFERENCES,
                u64::MAX,
            )
        })?;
        self.raw_references = charge_relation_counter(
            self.raw_references,
            amount,
            MAX_NESTED_RELATION_IMAGE_RAW_REFERENCES,
            icydb_diagnostic_code::DiagnosticExecutionBudgetResource::ResultRows,
        )?;
        batch.charge_raw_references_u64(amount)
    }
}

impl RelationCommitBudget {
    /// Validate one complete source/relation only once against the final delete set.
    pub(in crate::db::relation) fn validate_delete_source_once(
        &mut self,
        relation_id: RelationId,
        source_key: RawDataStoreKey,
        validate: impl FnOnce(&mut Self) -> Result<(), InternalError>,
    ) -> Result<(), InternalError> {
        let identity = (relation_id, source_key);
        if self.validated_delete_sources.contains(&identity) {
            return Ok(());
        }
        // Each valid affected source/relation must remove at least one reverse
        // edge. Its fact set cannot exceed the existing batch effect ceiling.
        // Bound stale physical witnesses too, without another configurable cap.
        charge_relation_counter(
            u64::try_from(self.validated_delete_sources.len()).unwrap_or(u64::MAX),
            1,
            MAX_RELATION_BATCH_REVERSE_DELTAS,
            icydb_diagnostic_code::DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
        )?;
        validate(self)?;
        self.validated_delete_sources.insert(identity);

        Ok(())
    }

    fn charge_raw_references(&mut self, amount: usize) -> Result<(), InternalError> {
        let amount = u64::try_from(amount).map_err(|_| {
            relation_budget_error(
                icydb_diagnostic_code::DiagnosticExecutionBudgetResource::ResultRows,
                MAX_RELATION_BATCH_RAW_REFERENCES,
                u64::MAX,
            )
        })?;
        self.charge_raw_references_u64(amount)
    }

    fn charge_raw_references_u64(&mut self, amount: u64) -> Result<(), InternalError> {
        self.raw_references = charge_relation_counter(
            self.raw_references,
            amount,
            MAX_RELATION_BATCH_RAW_REFERENCES,
            icydb_diagnostic_code::DiagnosticExecutionBudgetResource::ResultRows,
        )?;
        Ok(())
    }

    fn validate_target_once(
        &mut self,
        key: RawDataStoreKey,
        validate: impl FnOnce(&RawDataStoreKey) -> Result<bool, InternalError>,
    ) -> Result<Option<RawDataStoreKey>, InternalError> {
        let insertion_index = match self
            .target_lookup_results
            .binary_search_by(|(target, _)| target.cmp(&key))
        {
            Ok(index) => return Ok((!self.target_lookup_results[index].1).then_some(key)),
            Err(index) => index,
        };
        let observed = u64::try_from(self.target_lookup_results.len())
            .map_or(u64::MAX, |count| count.saturating_add(1));
        if observed > MAX_RELATION_BATCH_UNIQUE_TARGET_LOOKUPS {
            return Err(relation_budget_error(
                icydb_diagnostic_code::DiagnosticExecutionBudgetResource::RowsVisited,
                MAX_RELATION_BATCH_UNIQUE_TARGET_LOOKUPS,
                MAX_RELATION_BATCH_UNIQUE_TARGET_LOOKUPS.saturating_add(1),
            ));
        }
        let exists = validate(&key)?;
        let missing = (!exists).then(|| key.clone());
        self.target_lookup_results
            .insert(insertion_index, (key, exists));
        Ok(missing)
    }

    fn charge_reverse_delta(&mut self) -> Result<(), InternalError> {
        self.reverse_deltas = charge_relation_counter(
            self.reverse_deltas,
            1,
            MAX_RELATION_BATCH_REVERSE_DELTAS,
            icydb_diagnostic_code::DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
        )?;
        Ok(())
    }
}

fn charge_relation_counter(
    current: u64,
    amount: u64,
    limit: u64,
    resource: icydb_diagnostic_code::DiagnosticExecutionBudgetResource,
) -> Result<u64, InternalError> {
    let observed = current.saturating_add(amount);
    if observed > limit {
        return Err(relation_budget_error(resource, limit, observed));
    }
    Ok(observed)
}

fn relation_budget_error(
    resource: icydb_diagnostic_code::DiagnosticExecutionBudgetResource,
    limit: u64,
    observed: u64,
) -> InternalError {
    InternalError::relation_budget_exceeded(resource, limit, observed)
}

///
/// ReverseRelationSourceInfo
///
/// Resolved authority used while preparing reverse-index mutations.
/// Carries only the source entity path and tag required for diagnostics and
/// reverse-index identity, so the heavy mutation loop does not need `S`.
///

#[derive(Clone, Debug)]
pub(crate) struct ReverseRelationSourceInfo {
    path: Rc<str>,
    entity_tag: EntityTag,
}

impl ReverseRelationSourceInfo {
    /// Build structural source authority from an accepted runtime entity identity.
    pub(in crate::db) fn new(path: impl Into<Rc<str>>, entity_tag: EntityTag) -> Self {
        Self {
            path: path.into(),
            entity_tag,
        }
    }

    /// Borrow the accepted source path used for diagnostics.
    #[must_use]
    pub(in crate::db::relation) fn path(&self) -> &str {
        &self.path
    }

    /// Return the structural source entity tag used for reverse-index identity.
    #[must_use]
    pub(in crate::db::relation) const fn entity_tag(&self) -> EntityTag {
        self.entity_tag
    }
}

#[derive(Clone, Debug)]
pub(in crate::db::relation) struct AcceptedRelationInfo {
    constraint: AcceptedConstraintIdentity,
    reverse_identity: AcceptedRelationReverseIdentity,
    source_field_index: usize,
    source: AcceptedRelationSource,
    target: AcceptedRelationTargetIdentity,
    cardinality: AcceptedRelationCardinality,
}

#[derive(Clone, Debug)]
enum AcceptedRelationSource {
    Direct(AcceptedRelationLocalComponents),
    Nested(AcceptedNestedRelationSource),
}

enum AcceptedRelationBindingSource<'a> {
    Direct(&'a [usize]),
    Nested {
        root_slot: usize,
        steps: &'a [PersistedRelationPathStepSnapshot],
    },
}

struct AcceptedRelationBinding<'a> {
    constraint: AcceptedConstraintIdentity,
    reverse_identity: AcceptedRelationReverseIdentity,
    target_path: &'a str,
    source: AcceptedRelationBindingSource<'a>,
}

#[derive(Clone, Debug)]
struct AcceptedNestedRelationSource {
    root_slot: usize,
    steps: Vec<PersistedRelationPathStepSnapshot>,
    value_catalog: AcceptedValueCatalogHandle,
}

#[derive(Clone, Copy, Debug)]
struct AcceptedRelationReverseIdentity {
    relation_id: RelationId,
    physical_generation: u64,
}

impl AcceptedRelationReverseIdentity {
    const fn new(relation_id: RelationId, physical_generation: u64) -> Self {
        Self {
            relation_id,
            physical_generation,
        }
    }
}

/// Accepted-schema relation projection bound to one exact reverse generation.
///
/// The projection covers either active accepted state or an isolated
/// activation candidate. Callers own visibility, traversal, and publication.
#[derive(Clone, Debug)]
pub(in crate::db) struct RelationConstraintProjection {
    source: ReverseRelationSourceInfo,
    relation: AcceptedRelationInfo,
    target_store_path: &'static str,
    target_store: StoreHandle,
}

/// One candidate reverse entry and its registry-owned target store.
#[derive(Clone)]
pub(in crate::db) struct RelationConstraintIndexEntry {
    target_store_path: &'static str,
    target_store: StoreHandle,
    key: RawIndexStoreKey,
}

/// Candidate projection of one source row, including unresolved targets.
pub(in crate::db) struct RelationConstraintRowProjection {
    entries: Vec<RelationConstraintIndexEntry>,
    missing_targets: Vec<RawDataStoreKey>,
}

impl AcceptedRelationInfo {
    fn new(
        constraint: AcceptedConstraintIdentity,
        reverse_identity: AcceptedRelationReverseIdentity,
        source_field_index: usize,
        local_components: AcceptedRelationLocalComponents,
        target_contract: AcceptedRelationTargetContract,
        cardinality: AcceptedRelationCardinality,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            constraint,
            reverse_identity,
            source_field_index,
            source: AcceptedRelationSource::Direct(local_components),
            target: AcceptedRelationTargetIdentity::from_target_contract(target_contract)?,
            cardinality,
        })
    }

    fn new_nested(
        constraint: AcceptedConstraintIdentity,
        reverse_identity: AcceptedRelationReverseIdentity,
        nested: AcceptedNestedRelationSource,
        target_contract: AcceptedRelationTargetContract,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            constraint,
            reverse_identity,
            source_field_index: nested.root_slot,
            source: AcceptedRelationSource::Nested(nested),
            target: AcceptedRelationTargetIdentity::from_target_contract(target_contract)?,
            cardinality: AcceptedRelationCardinality::Single,
        })
    }

    #[must_use]
    pub(in crate::db::relation) const fn field_index(&self) -> usize {
        self.source_field_index
    }

    #[must_use]
    pub(in crate::db::relation) const fn relation_id(&self) -> RelationId {
        self.reverse_identity.relation_id
    }

    #[must_use]
    pub(in crate::db::relation) const fn physical_generation(&self) -> u64 {
        self.reverse_identity.physical_generation
    }

    #[must_use]
    fn scalar_relation_field_kind(&self) -> Option<&AcceptedFieldKind> {
        self.scalar_local_component()
            .map(AcceptedRelationLocalComponent::field_kind)
    }

    #[must_use]
    const fn local_components(&self) -> Option<&AcceptedRelationLocalComponents> {
        match &self.source {
            AcceptedRelationSource::Direct(components) => Some(components),
            AcceptedRelationSource::Nested(_) => None,
        }
    }

    #[must_use]
    pub(in crate::db::relation) const fn target(&self) -> &AcceptedRelationTargetIdentity {
        &self.target
    }

    const fn cardinality(&self) -> AcceptedRelationCardinality {
        self.cardinality
    }

    const fn scalar_local_component(&self) -> Option<&AcceptedRelationLocalComponent> {
        match &self.source {
            AcceptedRelationSource::Direct(components) => components.scalar_component(),
            AcceptedRelationSource::Nested(_) => None,
        }
    }

    const fn nested_source(&self) -> Option<&AcceptedNestedRelationSource> {
        match &self.source {
            AcceptedRelationSource::Nested(nested) => Some(nested),
            AcceptedRelationSource::Direct(_) => None,
        }
    }

    pub(in crate::db::relation) fn write_violation(
        &self,
        accepted_schema_fingerprint: crate::db::commit::CommitSchemaFingerprint,
        entity_tag: EntityTag,
        mutation: Option<MutationDiagnosticContext>,
    ) -> InternalError {
        InternalError::mutation_constraint_violation(
            AcceptedConstraintFactContext::write_admission(
                crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
                accepted_schema_fingerprint,
                entity_tag.value(),
                self.constraint.id().get(),
                icydb_diagnostic_code::DiagnosticConstraintKind::Relation,
                mutation,
                None,
            ),
        )
    }
}

impl RelationConstraintProjection {
    /// Return the exact planner-invisible reverse-index generation.
    #[cfg(feature = "migration")]
    pub(in crate::db) const fn index_id(&self) -> IndexId {
        reverse_index_id_for_relation(&self.source, &self.relation)
    }
    /// Bind one isolated activation candidate to row and target-store authority.
    pub(in crate::db) fn new<C: CanisterKind>(
        db: &Db<C>,
        source: ReverseRelationSourceInfo,
        snapshot: &crate::db::schema::PersistedSchemaSnapshot,
        row_contract: &StructuralRowContract,
        edge: &crate::db::schema::PersistedRelationEdgeSnapshot,
        target_contract: AcceptedRelationTargetContract,
    ) -> Result<Self, InternalError> {
        if edge.physical_generation() == 0 {
            return Err(InternalError::store_corruption());
        }
        Self::bind(db, source, snapshot, row_contract, edge, target_contract)
    }

    /// Bind one active accepted relation to row and target-store authority.
    ///
    /// Initial accepted relations legitimately use generation zero. Activated
    /// candidates use [`Self::new`] so their isolated generation remains
    /// nonzero.
    pub(in crate::db) fn new_active<C: CanisterKind>(
        db: &Db<C>,
        source: ReverseRelationSourceInfo,
        snapshot: &crate::db::schema::PersistedSchemaSnapshot,
        row_contract: &StructuralRowContract,
        edge: &crate::db::schema::PersistedRelationEdgeSnapshot,
        target_contract: AcceptedRelationTargetContract,
    ) -> Result<Self, InternalError> {
        Self::bind(db, source, snapshot, row_contract, edge, target_contract)
    }

    fn bind<C: CanisterKind>(
        db: &Db<C>,
        source: ReverseRelationSourceInfo,
        snapshot: &crate::db::schema::PersistedSchemaSnapshot,
        row_contract: &StructuralRowContract,
        edge: &crate::db::schema::PersistedRelationEdgeSnapshot,
        target_contract: AcceptedRelationTargetContract,
    ) -> Result<Self, InternalError> {
        let relation = relation_info_from_snapshot_edge(
            db,
            source.path(),
            snapshot,
            row_contract,
            edge,
            target_contract,
        )?;
        let (target_store_path, target_store) =
            relation_target_store_binding(db, &source, &relation)?;
        Ok(Self {
            source,
            relation,
            target_store_path,
            target_store,
        })
    }

    /// Return the stable accepted logical relation identity.
    #[must_use]
    pub(in crate::db) const fn relation_id(&self) -> RelationId {
        self.relation.relation_id()
    }

    /// Return the exact generation carried by every projected reverse key.
    #[must_use]
    pub(in crate::db) const fn physical_generation(&self) -> u64 {
        self.relation.physical_generation()
    }

    /// Borrow the target store path participating in projection verification.
    #[must_use]
    pub(in crate::db) const fn target_store_path(&self) -> &'static str {
        self.target_store_path
    }

    /// Return the target store participating in projection verification.
    #[must_use]
    pub(in crate::db) const fn target_store(&self) -> StoreHandle {
        self.target_store
    }

    /// Build canonical inclusive bounds for this active reverse generation.
    pub(in crate::db) fn raw_bounds(
        &self,
    ) -> Result<(Bound<RawIndexStoreKey>, Bound<RawIndexStoreKey>), InternalError> {
        let index_id = reverse_index_id_for_relation(&self.source, &self.relation);
        let relation_id = relation_id_component(&self.relation);
        let (lower, upper) = raw_keys_for_component_prefix_with_kind::<Vec<u8>>(
            &index_id,
            IndexKeyKind::System,
            2,
            &[relation_id.to_vec()],
        )
        .map_err(|_| InternalError::store_corruption())?;

        Ok((Bound::Included(lower), Bound::Included(upper)))
    }

    /// Prove that one decoded key names this exact active reverse generation.
    #[must_use]
    pub(in crate::db) fn contains_decoded_key(&self, key: &BorrowedIndexKeyFrame<'_>) -> bool {
        let expected = reverse_index_id_for_relation(&self.source, &self.relation);
        key.key_kind() == IndexKeyKind::System
            && key.index_id() == expected
            && key.components().len() == 2
            && key.components().first().copied()
                == Some(relation_id_component(&self.relation).as_slice())
    }

    /// Project one source row and classify target existence deterministically.
    pub(in crate::db) fn project_row(
        &self,
        source_primary_key: &PrimaryKeyValue,
        row: &StructuralSlotReader<'_>,
        validate_targets: bool,
    ) -> Result<RelationConstraintRowProjection, InternalError> {
        let mut projection_budget = RelationProjectionBudget::default();
        let mut commit_budget = RelationCommitBudget::default();
        self.project_row_with_budgets(
            source_primary_key,
            row,
            validate_targets,
            &mut projection_budget,
            &mut commit_budget,
        )
    }

    /// Project one relation while sharing row-image and lifecycle counters.
    pub(in crate::db) fn project_row_with_budgets(
        &self,
        source_primary_key: &PrimaryKeyValue,
        row: &StructuralSlotReader<'_>,
        validate_targets: bool,
        projection_budget: &mut RelationProjectionBudget,
        commit_budget: &mut RelationCommitBudget,
    ) -> Result<RelationConstraintRowProjection, InternalError> {
        self.project_row_with_target_lookup(
            source_primary_key,
            row,
            validate_targets,
            projection_budget,
            commit_budget,
            |target, raw_target| {
                Ok(self
                    .target_store
                    .with_data(|data_store| data_store.contains(raw_target))
                    && target.entity_tag() == self.relation.target().entity_tag())
            },
        )
    }

    fn project_row_with_target_reader(
        &self,
        source_primary_key: &PrimaryKeyValue,
        row: &StructuralSlotReader<'_>,
        validate_targets: bool,
        projection_budget: &mut RelationProjectionBudget,
        commit_budget: &mut RelationCommitBudget,
        target_reader: &dyn StructuralPrimaryRowReader,
    ) -> Result<RelationConstraintRowProjection, InternalError> {
        self.project_row_with_target_lookup(
            source_primary_key,
            row,
            validate_targets,
            projection_budget,
            commit_budget,
            |target, _| target_reader.contains_primary_row(target),
        )
    }

    fn project_row_with_target_lookup(
        &self,
        source_primary_key: &PrimaryKeyValue,
        row: &StructuralSlotReader<'_>,
        validate_targets: bool,
        projection_budget: &mut RelationProjectionBudget,
        commit_budget: &mut RelationCommitBudget,
        mut target_exists: impl FnMut(
            &DecodedDataStoreKey,
            &RawDataStoreKey,
        ) -> Result<bool, InternalError>,
    ) -> Result<RelationConstraintRowProjection, InternalError> {
        let target_keys = relation_target_identities_for_source_slots(
            row,
            &self.relation,
            projection_budget,
            commit_budget,
        )?;
        let mut entries = Vec::with_capacity(target_keys.len());
        let mut missing_targets = Vec::new();
        for (target_key, target_value) in target_keys {
            if validate_targets
                && let Some(missing_target) =
                    commit_budget.validate_target_once(target_key, |raw_target| {
                        let target = DecodedDataStoreKey::new(
                            self.relation.target().entity_tag(),
                            &target_value,
                        );
                        target_exists(&target, raw_target)
                    })?
            {
                missing_targets.push(missing_target);
                continue;
            }
            let Some(key) = reverse_index_key_for_target_and_source_primary_key_value(
                &self.source,
                &self.relation,
                &target_value,
                source_primary_key,
            )?
            else {
                continue;
            };
            entries.push(RelationConstraintIndexEntry {
                target_store_path: self.target_store_path,
                target_store: self.target_store,
                key,
            });
        }
        Ok(RelationConstraintRowProjection {
            entries,
            missing_targets,
        })
    }

    /// Prepare reverse deltas for one live source-row transition against the
    /// mutation scheduler's authoritative target-row view.
    #[expect(
        clippy::too_many_arguments,
        reason = "the accepted relation transition requires both row views plus exact per-operation diagnostic authority"
    )]
    pub(in crate::db) fn prepare_source_transition(
        &self,
        target_reader: &dyn StructuralPrimaryRowReader,
        validate_targets: bool,
        accepted_schema_fingerprint: crate::db::commit::CommitSchemaFingerprint,
        mutation: Option<MutationDiagnosticContext>,
        source_primary_key: &PrimaryKeyValue,
        old_row: Option<&StructuralSlotReader<'_>>,
        new_row: Option<&StructuralSlotReader<'_>>,
        old_budget: &mut RelationProjectionBudget,
        new_budget: &mut RelationProjectionBudget,
        commit_budget: &mut RelationCommitBudget,
    ) -> Result<Vec<PreparedIndexMutation>, InternalError> {
        let old_entries = old_row
            .map(|row| {
                self.project_row_with_target_reader(
                    source_primary_key,
                    row,
                    false,
                    old_budget,
                    commit_budget,
                    target_reader,
                )
            })
            .transpose()?
            .map(RelationConstraintRowProjection::into_entries)
            .unwrap_or_default();
        let new_projection = new_row
            .map(|row| {
                self.project_row_with_target_reader(
                    source_primary_key,
                    row,
                    validate_targets,
                    new_budget,
                    commit_budget,
                    target_reader,
                )
            })
            .transpose()?;
        if new_projection
            .as_ref()
            .is_some_and(|projection| !projection.missing_targets().is_empty())
        {
            // Missing identities were freshly encoded from accepted target keys.
            return Err(self.relation.write_violation(
                accepted_schema_fingerprint,
                self.source.entity_tag(),
                mutation,
            ));
        }
        let new_entries = new_projection
            .map(RelationConstraintRowProjection::into_entries)
            .unwrap_or_default();

        merge_relation_entries(old_entries, new_entries, commit_budget)
    }
}

impl RelationConstraintIndexEntry {
    /// Borrow the deterministic target-store ordering identity.
    #[must_use]
    pub(in crate::db) const fn target_store_path(&self) -> &'static str {
        self.target_store_path
    }

    /// Return the target store that owns this candidate reverse entry.
    #[must_use]
    pub(in crate::db) const fn target_store(&self) -> StoreHandle {
        self.target_store
    }

    /// Borrow the fully encoded isolated reverse key.
    #[must_use]
    pub(in crate::db) const fn key(&self) -> &RawIndexStoreKey {
        &self.key
    }
}

fn merge_relation_entries(
    old_entries: Vec<RelationConstraintIndexEntry>,
    new_entries: Vec<RelationConstraintIndexEntry>,
    commit_budget: &mut RelationCommitBudget,
) -> Result<Vec<PreparedIndexMutation>, InternalError> {
    let mut effects = Vec::new();
    let mut old_index = 0usize;
    let mut new_index = 0usize;
    while old_index < old_entries.len() || new_index < new_entries.len() {
        let (entry, old_contains, new_contains) =
            match (old_entries.get(old_index), new_entries.get(new_index)) {
                (Some(old), Some(new)) => {
                    match relation_entry_identity(old).cmp(&relation_entry_identity(new)) {
                        std::cmp::Ordering::Less => {
                            old_index = old_index.saturating_add(1);
                            (old, true, false)
                        }
                        std::cmp::Ordering::Greater => {
                            new_index = new_index.saturating_add(1);
                            (new, false, true)
                        }
                        std::cmp::Ordering::Equal => {
                            old_index = old_index.saturating_add(1);
                            new_index = new_index.saturating_add(1);
                            (old, true, true)
                        }
                    }
                }
                (Some(old), None) => {
                    old_index = old_index.saturating_add(1);
                    (old, true, false)
                }
                (None, Some(new)) => {
                    new_index = new_index.saturating_add(1);
                    (new, false, true)
                }
                (None, None) => break,
            };
        if old_contains == new_contains {
            continue;
        }
        // One prepared row transition is already old/new coalesced, and the
        // batch owns one final transition per source key. Its remaining
        // physical reverse operations are therefore distinct here.
        commit_budget.charge_reverse_delta()?;
        effects.push(PreparedIndexMutation::new(
            entry.target_store.index_store(),
            entry.key.clone(),
            new_contains.then(IndexEntryValue::presence),
        ));
    }
    Ok(effects)
}

const fn relation_entry_identity(
    entry: &RelationConstraintIndexEntry,
) -> (&'static str, &RawIndexStoreKey) {
    (entry.target_store_path, &entry.key)
}

impl RelationConstraintRowProjection {
    /// Borrow canonical reverse entries for this source row.
    #[must_use]
    pub(in crate::db) const fn entries(&self) -> &[RelationConstraintIndexEntry] {
        self.entries.as_slice()
    }

    /// Borrow target keys absent from authoritative target data.
    #[must_use]
    pub(in crate::db) const fn missing_targets(&self) -> &[RawDataStoreKey] {
        self.missing_targets.as_slice()
    }

    /// Consume the projection into candidate reverse entries.
    pub(in crate::db) fn into_entries(self) -> Vec<RelationConstraintIndexEntry> {
        self.entries
    }
}

#[derive(Clone, Debug)]
struct AcceptedRelationLocalComponents {
    components: Vec<AcceptedRelationLocalComponent>,
}

impl AcceptedRelationLocalComponents {
    fn scalar(
        field_index: usize,
        field: AcceptedFieldDecodeContract<'_>,
    ) -> Result<Self, InternalError> {
        Self::try_from_component_specs(&[AcceptedRelationLocalComponentSpec {
            index: field_index,
            field,
        }])
    }

    fn try_from_component_specs(
        components: &[AcceptedRelationLocalComponentSpec<'_>],
    ) -> Result<Self, InternalError> {
        if components.is_empty() {
            return Err(InternalError::persisted_row_decode_corruption());
        }

        Ok(Self {
            components: components
                .iter()
                .map(|component| AcceptedRelationLocalComponent {
                    index: component.index,
                    name: component.field.field_name().to_string(),
                    kind: component.field.kind().clone(),
                    nullable: component.field.nullable(),
                    storage_decode: component.field.storage_decode(),
                    leaf_codec: component.field.leaf_codec(),
                })
                .collect(),
        })
    }

    #[must_use]
    const fn component_count(&self) -> usize {
        self.components.len()
    }

    #[must_use]
    const fn components(&self) -> &[AcceptedRelationLocalComponent] {
        self.components.as_slice()
    }

    #[must_use]
    const fn scalar_component(&self) -> Option<&AcceptedRelationLocalComponent> {
        let [component] = self.components.as_slice() else {
            return None;
        };

        Some(component)
    }
}

#[derive(Clone, Copy, Debug)]
struct AcceptedRelationLocalComponentSpec<'a> {
    index: usize,
    field: AcceptedFieldDecodeContract<'a>,
}

#[derive(Clone, Debug)]
struct AcceptedRelationLocalComponent {
    index: usize,
    name: String,
    kind: AcceptedFieldKind,
    nullable: bool,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
}

impl AcceptedRelationLocalComponent {
    #[must_use]
    const fn field_index(&self) -> usize {
        self.index
    }

    #[must_use]
    const fn field_kind(&self) -> &AcceptedFieldKind {
        &self.kind
    }

    #[must_use]
    const fn decode_contract(&self) -> AcceptedFieldDecodeContract<'_> {
        AcceptedFieldDecodeContract::new(
            self.name.as_str(),
            &self.kind,
            self.nullable,
            self.storage_decode,
            self.leaf_codec,
        )
    }
}

#[derive(Clone, Debug)]
pub(in crate::db::relation) struct AcceptedRelationTargetIdentity {
    authority: AcceptedRelationTargetAuthority,
    primary_key: AcceptedRelationTargetPrimaryKey,
}

impl AcceptedRelationTargetIdentity {
    #[cfg(test)]
    fn try_new(
        target_path: &str,
        target_entity_name: &str,
        target_entity_tag: EntityTag,
        target_store_path: &str,
        key_kinds: &[AcceptedFieldKind],
    ) -> Result<Self, InternalError> {
        Ok(Self {
            authority: AcceptedRelationTargetAuthority::try_new(
                target_path,
                target_entity_name,
                target_entity_tag,
                target_store_path,
            )?,
            primary_key: AcceptedRelationTargetPrimaryKey::try_from_component_kinds(key_kinds)?,
        })
    }

    fn from_target_contract(
        contract: AcceptedRelationTargetContract,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            primary_key: AcceptedRelationTargetPrimaryKey::try_from_component_kinds(
                contract.primary_key_kinds(),
            )?,
            authority: contract.into_target(),
        })
    }

    #[must_use]
    pub(in crate::db::relation) const fn path(&self) -> &str {
        self.authority.path()
    }

    #[must_use]
    const fn entity_tag(&self) -> EntityTag {
        self.authority.entity_tag()
    }

    #[must_use]
    const fn store_path(&self) -> &str {
        self.authority.store_path()
    }

    #[must_use]
    const fn primary_key(&self) -> &AcceptedRelationTargetPrimaryKey {
        &self.primary_key
    }
}

#[derive(Clone, Debug)]
struct AcceptedRelationTargetPrimaryKey {
    component_kinds: Vec<AcceptedFieldKind>,
}

impl AcceptedRelationTargetPrimaryKey {
    fn try_from_component_kinds(
        component_kinds: &[AcceptedFieldKind],
    ) -> Result<Self, InternalError> {
        if component_kinds.is_empty() {
            return Err(InternalError::persisted_row_decode_corruption());
        }

        Ok(Self {
            component_kinds: component_kinds.to_vec(),
        })
    }

    #[must_use]
    const fn component_kinds(&self) -> &[AcceptedFieldKind] {
        self.component_kinds.as_slice()
    }

    #[must_use]
    const fn single_component_kind(&self) -> Option<&AcceptedFieldKind> {
        let [key_kind] = self.component_kinds.as_slice() else {
            return None;
        };

        Some(key_kind)
    }
}

pub(in crate::db::relation) fn accepted_relations_for_row_contract<C>(
    db: &Db<C>,
    source_path: &str,
    source_row_contract: &StructuralRowContract,
    target_path_filter: Option<&str>,
) -> Result<Vec<AcceptedRelationInfo>, InternalError>
where
    C: CanisterKind,
{
    accepted_relations_from_edges(db, source_path, source_row_contract, target_path_filter)
}

fn accepted_relations_from_edges<C>(
    db: &Db<C>,
    source_path: &str,
    source_row_contract: &StructuralRowContract,
    target_path_filter: Option<&str>,
) -> Result<Vec<AcceptedRelationInfo>, InternalError>
where
    C: CanisterKind,
{
    let mut relations = Vec::new();

    for edge in source_row_contract.accepted_relation_edges() {
        let relation = accepted_relation_from_edge(db, source_path, source_row_contract, edge)?;

        if target_path_filter.is_some_and(|filter| filter != relation.target().path()) {
            continue;
        }

        relations.push(relation);
    }

    Ok(relations)
}

fn accepted_relation_from_edge<C>(
    db: &Db<C>,
    source_path: &str,
    source_row_contract: &StructuralRowContract,
    edge: &OwnedAcceptedRelationEdgeContract,
) -> Result<AcceptedRelationInfo, InternalError>
where
    C: CanisterKind,
{
    let source = match edge.source() {
        OwnedAcceptedRelationSourceContract::Direct { field_slots } => {
            AcceptedRelationBindingSource::Direct(field_slots)
        }
        OwnedAcceptedRelationSourceContract::Nested { root_slot, steps } => {
            AcceptedRelationBindingSource::Nested {
                root_slot: *root_slot,
                steps,
            }
        }
    };
    accepted_relation_from_binding(
        db,
        source_path,
        source_row_contract,
        accepted_relation_target_contract(db, edge.target_path())?,
        AcceptedRelationBinding {
            constraint: edge.constraint().clone(),
            reverse_identity: AcceptedRelationReverseIdentity::new(
                edge.relation_id(),
                edge.physical_generation(),
            ),
            target_path: edge.target_path(),
            source,
        },
    )
}

fn accepted_relation_from_binding<C>(
    db: &Db<C>,
    source_path: &str,
    source_row_contract: &StructuralRowContract,
    target_contract: AcceptedRelationTargetContract,
    binding: AcceptedRelationBinding<'_>,
) -> Result<AcceptedRelationInfo, InternalError>
where
    C: CanisterKind,
{
    let relation_id = binding.reverse_identity.relation_id;
    match compile_accepted_relation_binding(source_row_contract, target_contract, binding) {
        Ok(relation) => Ok(relation),
        Err(error) => {
            // Resolve diagnostic identity only on failure; successful planning
            // needs no additional catalog lookup or allocated context.
            let Ok(source) = db.accepted_runtime_entity_for_path(source_path) else {
                // An absent source has no accepted identity to report. Keep
                // the original cause instead of replacing it during enrichment.
                return Err(error);
            };
            Err(error.with_relation_identity(source.entity_tag().value(), relation_id.get()))
        }
    }
}

fn compile_accepted_relation_binding(
    source_row_contract: &StructuralRowContract,
    target_contract: AcceptedRelationTargetContract,
    binding: AcceptedRelationBinding<'_>,
) -> Result<AcceptedRelationInfo, InternalError> {
    let AcceptedRelationBinding {
        constraint,
        reverse_identity,
        target_path,
        source,
    } = binding;
    if target_contract.target.path() != target_path {
        return Err(InternalError::store_invariant());
    }
    if let Some((nested, terminal)) = accepted_relation_traversal(&source, source_row_contract)? {
        let local_component = AcceptedRelationTupleEdgeLocalComponent::new(terminal.kind());
        let descriptor = accepted_relation_tuple_edge_descriptor(
            target_contract,
            std::slice::from_ref(&local_component),
        )?;
        return AcceptedRelationInfo::new_nested(
            constraint,
            reverse_identity,
            nested,
            descriptor.into_target_contract(),
        );
    }
    let AcceptedRelationBindingSource::Direct(slots) = source else {
        return Err(InternalError::store_invariant());
    };
    let local_fields = slots
        .iter()
        .map(|slot| {
            source_row_contract
                .required_accepted_field_decode_contract(*slot)
                .map(|field| (*slot, field))
        })
        .collect::<Result<Vec<_>, _>>()?;

    if let [(slot, field)] = local_fields.as_slice()
        && let Some(cardinality) =
            accepted_scalar_relation_cardinality(field.kind(), &target_contract)?
    {
        return AcceptedRelationInfo::new(
            constraint,
            reverse_identity,
            *slot,
            AcceptedRelationLocalComponents::scalar(*slot, *field)?,
            target_contract,
            cardinality,
        );
    }

    let local_component_facts = local_fields
        .iter()
        .map(|(_, field)| AcceptedRelationTupleEdgeLocalComponent::new(field.kind()))
        .collect::<Vec<_>>();
    let tuple_descriptor =
        accepted_relation_tuple_edge_descriptor(target_contract, local_component_facts.as_slice())?;
    let component_specs = local_fields
        .iter()
        .map(|(slot, field)| AcceptedRelationLocalComponentSpec {
            index: *slot,
            field: *field,
        })
        .collect::<Vec<_>>();
    let source_field_index = slots
        .first()
        .copied()
        .ok_or_else(InternalError::store_corruption)?;
    AcceptedRelationInfo::new(
        constraint,
        reverse_identity,
        source_field_index,
        AcceptedRelationLocalComponents::try_from_component_specs(component_specs.as_slice())?,
        tuple_descriptor.into_target_contract(),
        AcceptedRelationCardinality::Single,
    )
}

fn relation_info_from_snapshot_edge<C>(
    db: &Db<C>,
    source_path: &str,
    snapshot: &crate::db::schema::PersistedSchemaSnapshot,
    row_contract: &StructuralRowContract,
    edge: &crate::db::schema::PersistedRelationEdgeSnapshot,
    target_contract: AcceptedRelationTargetContract,
) -> Result<AcceptedRelationInfo, InternalError>
where
    C: CanisterKind,
{
    let constraint = snapshot
        .relation_enforcement_identity(edge.id())
        .ok_or_else(InternalError::store_corruption)?;
    let reverse_identity =
        AcceptedRelationReverseIdentity::new(edge.id(), edge.physical_generation());
    match edge.source() {
        PersistedRelationSourceSnapshot::Direct { field_ids } => {
            let slots = field_ids
                .iter()
                .map(|field_id| {
                    snapshot
                        .fields()
                        .iter()
                        .find(|field| field.id() == *field_id)
                        .map(|field| usize::from(field.slot().get()))
                        .ok_or_else(InternalError::store_corruption)
                })
                .collect::<Result<Vec<_>, _>>()?;
            accepted_relation_from_binding(
                db,
                source_path,
                row_contract,
                target_contract,
                AcceptedRelationBinding {
                    constraint,
                    reverse_identity,
                    target_path: edge.target_path(),
                    source: AcceptedRelationBindingSource::Direct(&slots),
                },
            )
        }
        PersistedRelationSourceSnapshot::Nested {
            root_field_id,
            steps,
        } => {
            let root_slot = snapshot
                .fields()
                .iter()
                .find(|field| field.id() == *root_field_id)
                .map(|field| usize::from(field.slot().get()))
                .ok_or_else(InternalError::store_corruption)?;
            accepted_relation_from_binding(
                db,
                source_path,
                row_contract,
                target_contract,
                AcceptedRelationBinding {
                    constraint,
                    reverse_identity,
                    target_path: edge.target_path(),
                    source: AcceptedRelationBindingSource::Nested { root_slot, steps },
                },
            )
        }
    }
}

// Direct collection edges carry target identity in the accepted edge, not in
// their element kind. Compile every collection through one bounded traversal;
// scalar/tuple sources retain their direct component projection.
fn accepted_relation_traversal(
    source: &AcceptedRelationBindingSource<'_>,
    row_contract: &StructuralRowContract,
) -> Result<Option<(AcceptedNestedRelationSource, AcceptedRelationValueContract)>, InternalError> {
    match source {
        AcceptedRelationBindingSource::Nested { root_slot, steps } => {
            accepted_nested_relation_source(*root_slot, steps, row_contract).map(Some)
        }
        AcceptedRelationBindingSource::Direct([slot]) => {
            let field = row_contract.required_accepted_field_decode_contract(*slot)?;
            let step = match field.kind() {
                AcceptedFieldKind::List(_) => PersistedRelationPathStepSnapshot::ListItems,
                AcceptedFieldKind::Set(_) => PersistedRelationPathStepSnapshot::SetItems,
                _ => return Ok(None),
            };
            let steps = [PersistedRelationPathStepSnapshot::OptionalSome, step];
            accepted_nested_relation_source(
                *slot,
                &steps[usize::from(!field.nullable())..],
                row_contract,
            )
            .map(Some)
        }
        AcceptedRelationBindingSource::Direct(_) => Ok(None),
    }
}

fn accepted_nested_relation_source(
    root_slot: usize,
    steps: &[PersistedRelationPathStepSnapshot],
    row_contract: &StructuralRowContract,
) -> Result<(AcceptedNestedRelationSource, AcceptedRelationValueContract), InternalError> {
    let root = row_contract.required_accepted_field_decode_contract(root_slot)?;
    let value_catalog = row_contract.accepted_value_catalog_handle();
    let terminal = accepted_relation_path_terminal(
        AcceptedRelationValueContract::new(root.kind().clone(), root.nullable()),
        steps,
        value_catalog.enum_catalog(),
        value_catalog.composite_catalog(),
    )
    .ok_or_else(InternalError::store_corruption)?;
    Ok((
        AcceptedNestedRelationSource {
            root_slot,
            steps: steps.to_vec(),
            value_catalog: value_catalog.clone(),
        },
        terminal,
    ))
}

/// Build the shared relation-system index identity for one physical generation.
const fn reverse_index_id_for_relation(
    source: &ReverseRelationSourceInfo,
    relation: &AcceptedRelationInfo,
) -> IndexId {
    IndexId::new_with_generation(
        source.entity_tag,
        RELATION_SYSTEM_INDEX_ORDINAL,
        relation.physical_generation(),
    )
}

const fn relation_id_component(relation: &AcceptedRelationInfo) -> [u8; size_of::<u32>()] {
    relation.relation_id().get().to_be_bytes()
}

/// Prove that one removed relation owns no surviving reverse physical entry.
///
/// The observer derives the exact physical generation from accepted source
/// authority. Bounded prefix probes use the runtime's canonical merged view,
/// including live entries and tombstones, without observing unrelated domains.
pub(in crate::db) fn prove_empty_reverse_relation_domain(
    index_store: &IndexStore,
    source_entity: EntityTag,
    source_snapshot: &PersistedSchemaSnapshot,
    relation: &PersistedRelationEdgeSnapshot,
) -> Result<(), InternalError> {
    if index_store.state() != IndexState::Ready {
        return Err(InternalError::store_unsupported());
    }
    if !source_snapshot
        .relations()
        .iter()
        .any(|accepted| accepted == relation)
    {
        return Err(InternalError::store_corruption());
    }
    let expected = IndexId::new_with_generation(
        source_entity,
        RELATION_SYSTEM_INDEX_ORDINAL,
        relation.physical_generation(),
    );
    let expected_relation_id = relation.id().get().to_be_bytes();
    // Component count precedes components in store ordering. Probe every
    // codec-admitted count so a stray one/three/four-component entry cannot
    // evade removal's existing refusal merely because reverse keys use two.
    // Each probe rejects its first visible witness; the shared reader retains
    // canonical/live precedence and skips tombstoned physical entries.
    for component_count in 1..=crate::MAX_INDEX_FIELDS {
        let (lower, upper) = raw_keys_for_component_prefix_with_kind(
            &expected,
            IndexKeyKind::System,
            component_count,
            &[expected_relation_id.as_slice()],
        )
        .map_err(|_| InternalError::store_corruption())?;
        index_store.visit_raw_entries_in_range(
            (
                Bound::Included(lower).as_ref(),
                Bound::Included(upper).as_ref(),
            ),
            Direction::Asc,
            |raw_key, _| {
                IndexKey::try_from_raw(raw_key).map_err(|_| InternalError::store_corruption())?;
                Err(InternalError::store_unsupported())
            },
        )?;
    }

    Ok(())
}

/// Build reverse-index prefix bounds for one complete target primary key.
pub(super) fn reverse_index_key_bounds_for_target_primary_key_value(
    source: &ReverseRelationSourceInfo,
    relation: &AcceptedRelationInfo,
    target_key_value: &PrimaryKeyValue,
) -> Result<Option<(RawIndexStoreKey, RawIndexStoreKey)>, InternalError> {
    let encoded_value = encode_reverse_relation_target_identity_component(target_key_value)?;
    let relation_id = relation_id_component(relation);

    let index_id = reverse_index_id_for_relation(source, relation);
    let (start, end) = raw_keys_for_component_prefix_with_kind(
        &index_id,
        IndexKeyKind::System,
        2,
        &[relation_id.as_slice(), encoded_value.as_slice()],
    )
    .map_err(|_| InternalError::query_executor_invariant())?;

    Ok(Some((start, end)))
}

/// Build the concrete reverse-index key for one target/source relation edge.
fn reverse_index_key_for_target_and_source_primary_key_value(
    source: &ReverseRelationSourceInfo,
    relation: &AcceptedRelationInfo,
    target_key_value: &PrimaryKeyValue,
    source_key_value: &PrimaryKeyValue,
) -> Result<Option<RawIndexStoreKey>, InternalError> {
    let encoded_value = encode_reverse_relation_target_identity_component(target_key_value)?;
    let relation_id = relation_id_component(relation);

    let index_id = reverse_index_id_for_relation(source, relation);
    let source_key = IndexKey::compact_primary_key_value_bytes(source_key_value)?;
    // The two components are a fixed relation ID and an admitted complete PK;
    // the suffix is also canonically encoded. Frame those borrowed segments
    // directly, retaining the taxonomy owner's complete-key budget and errors.
    let key = encode_index_store_key(
        IndexStoreKeyKind::System,
        index_id,
        [relation_id.as_slice(), encoded_value.as_slice()].into_iter(),
        &source_key,
    )
    .map_err(IndexKeyEncodeError::from)?;

    Ok(Some(key))
}

// Encode full relation target row identity as the reverse-index target
// component. This keeps scalar and composite targets on one key-owned path and
// prevents first-component projection from entering reverse-index storage.
fn encode_reverse_relation_target_identity_component(
    target_key_value: &PrimaryKeyValue,
) -> Result<Vec<u8>, InternalError> {
    EncodedPrimaryKey::encode(*target_key_value)
        .map(Vec::from)
        .map_err(|_| InternalError::persisted_row_decode_corruption())
}

// Preserve known target identities beside their canonical raw keys. Sorting and
// deduplication remain byte-owned; projection never decodes keys it just encoded.
fn relation_target_identities_for_source_slots(
    row_fields: &StructuralSlotReader<'_>,
    relation: &AcceptedRelationInfo,
    projection_budget: &mut RelationProjectionBudget,
    commit_budget: &mut RelationCommitBudget,
) -> Result<Vec<(RawDataStoreKey, PrimaryKeyValue)>, InternalError> {
    let keys = relation_target_keys_for_source_slots(
        row_fields,
        relation,
        projection_budget,
        commit_budget,
    )?;

    relation_target_identities_from_relation_target_keys(relation, keys)
}

/// Check one final source row against every target deleted by the same batch.
pub(in crate::db::relation) fn source_row_references_any_deleted_relation_target(
    raw_row: &RawRow,
    source_row_contract: StructuralRowContract,
    relation: &AcceptedRelationInfo,
    deleted_target_keys: &BTreeSet<RawDataStoreKey>,
    commit_budget: &mut RelationCommitBudget,
) -> Result<bool, InternalError> {
    let row_fields =
        StructuralSlotReader::from_raw_row_with_validated_contract(raw_row, source_row_contract)?;

    let mut projection_budget = RelationProjectionBudget::default();
    let keys = relation_target_identities_for_source_slots(
        &row_fields,
        relation,
        &mut projection_budget,
        commit_budget,
    )?;

    Ok(keys
        .iter()
        .any(|(key, _)| deleted_target_keys.contains(key)))
}

/// Decode a reverse-index entry into source-key membership for validation.
pub(super) fn decode_reverse_entry(
    index_key: &RawIndexStoreKey,
    raw_entry: &IndexEntryValue,
) -> Result<IndexRowIdentity, InternalError> {
    raw_entry
        .decode_row_identity(index_key)
        .map_err(|_| InternalError::index_corruption())
}

/// Resolve target store handle for one relation edge.
pub(super) fn relation_target_store<C>(
    db: &Db<C>,
    source: &ReverseRelationSourceInfo,
    relation: &AcceptedRelationInfo,
) -> Result<&'static LocalKey<RefCell<IndexStore>>, InternalError>
where
    C: CanisterKind,
{
    relation_target_store_binding(db, source, relation).map(|(_, store)| store.index_store())
}

// Resolve the registry-owned static path together with its store handle so a
// staged relation projection has deterministic cross-store ordering identity.
fn relation_target_store_binding<C>(
    db: &Db<C>,
    source: &ReverseRelationSourceInfo,
    relation: &AcceptedRelationInfo,
) -> Result<(&'static str, StoreHandle), InternalError>
where
    C: CanisterKind,
{
    let target = relation.target();
    let (target_store_path, target_store) = db.with_store_registry(|registry| {
        registry
            .iter()
            .find(|(path, _)| *path == target.store_path())
            .ok_or_else(InternalError::executor_internal)
    })?;
    let source_runtime = db.accepted_runtime_entity_for_tag(source.entity_tag())?;
    let source_store = db.store_handle(source_runtime.store_path())?;
    if matches!(
        (
            source_store.storage_capabilities().relation_source(),
            target_store.storage_capabilities().relation_target(),
        ),
        (
            StoreRelationSourceCapability::DurableSource,
            StoreRelationTargetCapability::VolatileTarget,
        )
    ) {
        return Err(InternalError::executor_unsupported());
    }

    Ok((target_store_path, target_store))
}

/// Decode one stored delete key, skipping other entities in the shared delete set.
pub(in crate::db::relation) fn decode_relation_target_data_key(
    relation: &AcceptedRelationInfo,
    target_raw_key: &RawDataStoreKey,
) -> Result<Option<DecodedDataStoreKey>, InternalError> {
    let target_data_key = DecodedDataStoreKey::try_from_raw(target_raw_key)
        .map_err(|_| InternalError::identity_corruption())?;

    if target_data_key.entity_tag() != relation.target().entity_tag() {
        return Ok(None);
    }

    Ok(Some(target_data_key))
}

// Keep each primary-key value with its encoded key in canonical byte order.
fn relation_target_identities_from_relation_target_keys(
    relation: &AcceptedRelationInfo,
    keys: RelationTargetKeys,
) -> Result<Vec<(RawDataStoreKey, PrimaryKeyValue)>, InternalError> {
    let values = keys.into_values();
    let mut keys = Vec::with_capacity(values.len());
    for value in values {
        let raw = raw_relation_target_key_from_primary_key_value(relation, &value)?;
        keys.push((raw, value));
    }
    keys.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
    keys.dedup_by(|(left, _), (right, _)| left == right);

    Ok(keys)
}

// Decode one relation field into structural target keys through the shared
// scalar-fast-path or field-bytes path used by delete validation and
// reverse-index mutation preparation.
fn relation_target_keys_for_source_slots(
    row_fields: &StructuralSlotReader<'_>,
    relation: &AcceptedRelationInfo,
    projection_budget: &mut RelationProjectionBudget,
    commit_budget: &mut RelationCommitBudget,
) -> Result<RelationTargetKeys, InternalError> {
    if let Some(nested) = relation.nested_source() {
        return relation_target_keys_from_nested_source(
            row_fields,
            nested,
            projection_budget,
            commit_budget,
        );
    }
    let keys = if relation
        .scalar_relation_field_kind()
        .and_then(accepted_relation_target_metadata_from_kind)
        .is_none()
    {
        relation_target_keys_from_component_slots(row_fields, relation)?
    } else if let Some(keys) = relation_target_keys_from_scalar_slot(row_fields, relation)? {
        // Keep single relation slots on the scalar fast path when the persisted
        // field already uses a primary-key-compatible leaf codec.
        keys
    } else {
        // Decode the declared relation field payload directly into target keys
        // without rebuilding a runtime `Value` container.
        relation_target_keys_from_field_bytes(row_fields, relation)?
    };
    commit_budget.charge_raw_references(keys.len())?;
    Ok(keys)
}

fn relation_target_keys_from_nested_source(
    row_fields: &StructuralSlotReader<'_>,
    nested: &AcceptedNestedRelationSource,
    projection_budget: &mut RelationProjectionBudget,
    commit_budget: &mut RelationCommitBudget,
) -> Result<RelationTargetKeys, InternalError> {
    // The reader owns the decoded root; traversal borrows it and emits owned keys.
    let root = row_fields.required_value_by_contract_cow(nested.root_slot)?;
    let mut values = vec![root.as_ref()];
    for step in &nested.steps {
        projection_budget.charge_traversal(commit_budget, values.len())?;
        match step {
            PersistedRelationPathStepSnapshot::OptionalSome => {
                let mut present = Vec::with_capacity(values.len());
                for value in values {
                    if !matches!(value, crate::value::Value::Null) {
                        present.push(value);
                    }
                }
                values = present;
            }
            PersistedRelationPathStepSnapshot::EnterNamed => {}
            PersistedRelationPathStepSnapshot::RecordMember {
                composite_type_id,
                member_id,
                ..
            } => {
                let member_name = nested
                    .value_catalog
                    .record_member_name(*composite_type_id, *member_id)
                    .ok_or_else(InternalError::store_corruption)?;
                let mut members = Vec::with_capacity(values.len());
                for value in values {
                    let crate::value::Value::Map(entries) = value else {
                        return Err(InternalError::persisted_row_decode_corruption());
                    };
                    members.push(
                        entries
                            .iter()
                            .find_map(|(key, value)| {
                                matches!(key, crate::value::Value::Text(name) if name == member_name)
                                    .then_some(value)
                            })
                            .ok_or_else(InternalError::persisted_row_decode_corruption)?,
                    );
                }
                values = members;
            }
            PersistedRelationPathStepSnapshot::EnumVariantPayload {
                enum_type_id,
                variant_id,
                ..
            } => {
                let mut payloads = Vec::with_capacity(values.len());
                for value in values {
                    let crate::value::Value::Enum(enum_value) = value else {
                        return Err(InternalError::persisted_row_decode_corruption());
                    };
                    if enum_value.type_id() != *enum_type_id {
                        return Err(InternalError::persisted_row_decode_corruption());
                    }
                    if enum_value.variant_id() != *variant_id {
                        continue;
                    }
                    let crate::value::CanonicalEnumBody::Payload(payload) = enum_value.body()
                    else {
                        return Err(InternalError::persisted_row_decode_corruption());
                    };
                    payloads.push(payload.as_ref());
                }
                values = payloads;
            }
            PersistedRelationPathStepSnapshot::ListItems
            | PersistedRelationPathStepSnapshot::SetItems => {
                let mut items = Vec::new();
                for value in values {
                    let crate::value::Value::List(nested_items) = value else {
                        return Err(InternalError::persisted_row_decode_corruption());
                    };
                    items.extend(nested_items);
                }
                values = items;
            }
            PersistedRelationPathStepSnapshot::MapValues => {
                let mut map_values = Vec::new();
                for value in values {
                    let crate::value::Value::Map(entries) = value else {
                        return Err(InternalError::persisted_row_decode_corruption());
                    };
                    for (_, value) in entries {
                        map_values.push(value);
                    }
                }
                values = map_values;
            }
        }
    }
    projection_budget.charge_nested_references(commit_budget, values.len())?;
    let mut components = Vec::with_capacity(values.len());
    for value in values {
        components.push(
            PrimaryKeyComponent::from_runtime_value(value)
                .ok_or_else(InternalError::persisted_row_decode_corruption)?,
        );
    }
    Ok(RelationTargetKeys::from_scalar_components(components))
}

fn relation_target_keys_from_component_slots(
    row_fields: &StructuralSlotReader<'_>,
    relation: &AcceptedRelationInfo,
) -> Result<RelationTargetKeys, InternalError> {
    let local_components = relation
        .local_components()
        .ok_or_else(InternalError::store_invariant)?;
    let mut components = Vec::with_capacity(local_components.component_count());
    let mut null_count = 0usize;

    for local_component in local_components.components() {
        let bytes = row_fields.required_field_bytes(local_component.field_index())?;
        let value = decode_runtime_value_from_accepted_field_contract(
            local_component.decode_contract(),
            bytes,
        )
        .map_err(|_| InternalError::persisted_row_decode_corruption())?;
        if matches!(value, crate::value::Value::Null) {
            null_count = null_count.saturating_add(1);
            continue;
        }
        let Some(component) = PrimaryKeyComponent::from_runtime_value(&value) else {
            return Err(InternalError::persisted_row_decode_corruption());
        };
        components.push(component);
    }

    if null_count == local_components.component_count() {
        return Ok(RelationTargetKeys::none());
    }
    if null_count != 0 {
        return Err(InternalError::persisted_row_decode_corruption());
    }

    let key = relation_target_primary_key_value_from_components(components.as_slice())?;

    Ok(RelationTargetKeys::one(&key))
}

fn relation_target_primary_key_value_from_components(
    components: &[PrimaryKeyComponent],
) -> Result<PrimaryKeyValue, InternalError> {
    match components {
        [component] => Ok(PrimaryKeyValue::Scalar(*component)),
        _ => Ok(PrimaryKeyValue::Composite(
            crate::db::key_taxonomy::CompositePrimaryKeyValue::try_from_components(components)
                .map_err(|_| InternalError::persisted_row_decode_corruption())?,
        )),
    }
}

// Decode the one relation field payload needed by structural delete
// validation directly into relation target keys from the encoded field bytes.
fn relation_target_keys_from_field_bytes(
    row_fields: &StructuralSlotReader<'_>,
    relation: &AcceptedRelationInfo,
) -> Result<RelationTargetKeys, InternalError> {
    validate_relation_field_kind(relation)?;

    let component = relation
        .scalar_local_component()
        .ok_or_else(InternalError::persisted_row_decode_corruption)?;
    let bytes = row_fields.required_field_bytes(component.field_index())?;
    let keys =
        decode_accepted_relation_target_primary_key_components_bytes(bytes, component.field_kind())
            .map_err(|_| InternalError::persisted_row_decode_corruption())?;

    Ok(RelationTargetKeys::from_scalar_components(keys))
}

// Decode one singular relation directly from the scalar slot codec when
// the relation key kind is already primary-key-compatible on the persisted row.
fn relation_target_keys_from_scalar_slot(
    row_fields: &StructuralSlotReader<'_>,
    relation: &AcceptedRelationInfo,
) -> Result<Option<RelationTargetKeys>, InternalError> {
    let Some(field_kind) = relation.scalar_relation_field_kind() else {
        return Ok(None);
    };
    if !matches!(field_kind, AcceptedFieldKind::Relation { .. }) {
        return Ok(None);
    }
    if !relation_scalar_slot_fast_path_key_kind_supported(field_kind) {
        return Ok(None);
    }
    if !matches!(
        row_fields.field_leaf_codec(relation.field_index())?,
        LeafCodec::Scalar(_)
    ) {
        return Ok(None);
    }

    // A candidate-logical row may supply a frozen historical scalar without
    // carrying physical bytes for the newly introduced slot. Consume that
    // semantic value through the same accepted row contract instead of
    // misclassifying legitimate historical absence as scalar corruption.
    if row_fields.get_bytes(relation.field_index()).is_none() {
        return match row_fields.required_value_by_contract(relation.field_index())? {
            crate::value::Value::Null => Ok(Some(RelationTargetKeys::none())),
            value => {
                let component = PrimaryKeyComponent::from_runtime_value(&value)
                    .ok_or_else(InternalError::persisted_row_decode_corruption)?;
                let key = PrimaryKeyValue::Scalar(component);

                Ok(Some(RelationTargetKeys::one(&key)))
            }
        };
    }

    match row_fields.required_scalar(relation.field_index())? {
        ScalarSlotValueRef::Null => Ok(Some(RelationTargetKeys::none())),
        ScalarSlotValueRef::Value(value) => {
            let primary_key_value = value
                .into_primary_key_component()
                .ok_or_else(InternalError::persisted_row_decode_corruption)?;

            let key = PrimaryKeyValue::Scalar(primary_key_value);

            Ok(Some(RelationTargetKeys::one(&key)))
        }
    }
}

fn relation_scalar_slot_fast_path_key_kind_supported(kind: &AcceptedFieldKind) -> bool {
    let AcceptedFieldKind::Relation { key_kind, .. } = kind else {
        return false;
    };

    matches!(
        key_kind.as_ref(),
        AcceptedFieldKind::Int8
            | AcceptedFieldKind::Int16
            | AcceptedFieldKind::Int32
            | AcceptedFieldKind::Int64
            | AcceptedFieldKind::Principal
            | AcceptedFieldKind::Subaccount
            | AcceptedFieldKind::Timestamp
            | AcceptedFieldKind::Nat8
            | AcceptedFieldKind::Nat16
            | AcceptedFieldKind::Nat32
            | AcceptedFieldKind::Nat64
            | AcceptedFieldKind::Ulid
            | AcceptedFieldKind::Unit
            | AcceptedFieldKind::U256
    )
}

// Encode one decoded relation primary-key value directly into the target raw-key
// shape without materializing an intermediate runtime `Value`.
fn raw_relation_target_key_from_primary_key_value(
    relation: &AcceptedRelationInfo,
    value: &PrimaryKeyValue,
) -> Result<RawDataStoreKey, InternalError> {
    DecodedDataStoreKey::new(relation.target().entity_tag(), value)
        .to_raw_compact_key_error()
        .map_err(|_| InternalError::persisted_row_decode_corruption())
}

// Enforce the narrow relation-field shapes that relation structural
// decode is allowed to accept on this path.
fn validate_relation_field_kind(relation: &AcceptedRelationInfo) -> Result<(), InternalError> {
    match relation.cardinality() {
        AcceptedRelationCardinality::Single
        | AcceptedRelationCardinality::List
        | AcceptedRelationCardinality::Set => {
            validate_scalar_relation_target_primary_key_kind(relation)
        }
    }
}

// Scalar collection relation fields still use this single-component gate;
// tuple relation edges use accepted relation-edge metadata instead.
fn validate_scalar_relation_target_primary_key_kind(
    relation: &AcceptedRelationInfo,
) -> Result<(), InternalError> {
    let local_components = relation
        .local_components()
        .ok_or_else(InternalError::store_invariant)?;
    if local_components.component_count() != relation.target().primary_key().component_kinds().len()
    {
        return Err(InternalError::persisted_row_decode_corruption());
    }

    let Some(key_kind) = relation.target().primary_key().single_component_kind() else {
        return Err(InternalError::persisted_row_decode_corruption());
    };

    validate_relation_primary_key_component_kind(key_kind)
}

#[cfg(test)]
mod tests;
