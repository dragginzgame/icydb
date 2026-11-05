//! Catalog-native source-migration rename and lineage planning.
//!
//! This module derives planner-local accepted candidates only. It never writes
//! accepted schema, lineage, application receipts, or durable migration state.

use std::collections::{BTreeMap, BTreeSet};

use icydb_schema::{
    ConstraintFragmentKind, ConstraintSourceKey, EntityFragment, EntityMigration,
    EntitySourceDigest, EntitySourceKey, FieldSourceKey, RuleSourceKey, SchemaMigrationPlan,
    SchemaMigrationRename, SchemaMigrationTransform, SchemaProposal, TargetStoreIdentity,
    TypeSourceKey,
};

use crate::{
    db::schema::{
        AcceptedCompositeCatalog, AcceptedConstraintCatalog, AcceptedConstraintKind,
        AcceptedConstraintSnapshot, AcceptedEnumCatalog, AcceptedNamedTypeIdentity,
        AcceptedSchemaRevisionBundle, AcceptedSourceBindingCatalog, CandidateSchemaRevision,
        ConstraintId, ConstraintOrigin, ExistingProposalStore, PersistedFieldSnapshot,
        PersistedIndexSnapshot, PersistedRelationEdgeSnapshot, PersistedSchemaSnapshot,
        SchemaFieldSlot, SchemaIndexId, SchemaRowLayout, SchemaVersion, bind_source_check_expr,
        lower_existing_schema_proposal, lower_migration_index, lower_migration_nested_leaves,
        lower_new_migration_index, lower_proposed_field, lower_relation_source,
        migration_lineage::{
            AcceptedEntitySourceLineageCatalog, AcceptedEntitySourceLineageState,
            AcceptedEntitySourceVersion,
        },
        migration_transform::{
            CompiledMigrationEntityProgram, compile_migration_programs,
            transition_requires_row_rewrite,
        },
        relation_edge_from_source,
    },
    error::{ErrorOrigin, InternalError},
    types::EntityTag,
};

/// Typed planning failures before public endpoint error projection.
#[derive(Debug)]
pub(in crate::db::schema) enum SchemaMigrationPlanningError {
    /// Preserve operational preparation failures instead of reporting invalid schema.
    Preparation(InternalError),
    Unadopted,
    MissingMigration,
    VersionGap,
    Downgrade,
    EmptyEntityVersionBump,
    UnknownFromObject,
    UnknownToObject,
    KindMismatch,
    IdentityConflict,
    UnexplainedSchemaDifference,
    UnsupportedTransform,
    RekeyedCatalogInvalid,
    CandidateMismatch,
    CorruptLineage,
}

impl SchemaMigrationPlanningError {
    // Candidate construction is fallible operational work. Keep the existing
    // semantic classification only for schema-owned rejections.
    fn candidate_failure(error: InternalError, semantic: Self) -> Self {
        if error.origin() == ErrorOrigin::Executor {
            Self::Preparation(error)
        } else {
            semantic
        }
    }
}

/// One lineage value to publish with a future accepted candidate head.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct PlannedEntitySourceLineage {
    store: TargetStoreIdentity,
    entity: EntityTag,
    version: AcceptedEntitySourceVersion,
    digest: EntitySourceDigest,
}

impl PlannedEntitySourceLineage {
    // All planning routes derive the version and digest from the same proposed entity.
    fn from_proposal(
        proposal: &SchemaProposal,
        entity: &EntityFragment,
        store: TargetStoreIdentity,
        entity_tag: EntityTag,
    ) -> Result<Self, SchemaMigrationPlanningError> {
        Ok(Self {
            store,
            entity: entity_tag,
            version: AcceptedEntitySourceVersion::try_new(entity.version().get())
                .map_err(|_| SchemaMigrationPlanningError::CorruptLineage)?,
            digest: proposal
                .entity_source_digest(entity.source_key())
                .map_err(|_| SchemaMigrationPlanningError::CandidateMismatch)?,
        })
    }

    #[must_use]
    pub(in crate::db::schema) const fn store(self) -> TargetStoreIdentity {
        self.store
    }

    #[must_use]
    pub(in crate::db::schema) const fn entity(self) -> EntityTag {
        self.entity
    }

    #[must_use]
    pub(in crate::db::schema) const fn version(self) -> AcceptedEntitySourceVersion {
        self.version
    }

    #[must_use]
    pub(in crate::db::schema) const fn digest(self) -> EntitySourceDigest {
        self.digest
    }
}

/// Exact planner-local candidates and their deferred lineage publication.
pub(in crate::db::schema) struct PlannedSchemaMigration {
    candidates: Vec<CandidateSchemaRevision>,
    lineage: Vec<PlannedEntitySourceLineage>,
    physical: bool,
    programs: Vec<CompiledMigrationEntityProgram>,
}

impl PlannedSchemaMigration {
    #[must_use]
    pub(in crate::db::schema) fn candidates(&self) -> &[CandidateSchemaRevision] {
        &self.candidates
    }

    #[must_use]
    pub(in crate::db::schema) fn lineage(&self) -> &[PlannedEntitySourceLineage] {
        &self.lineage
    }

    #[must_use]
    pub(in crate::db::schema) const fn requires_physical_validation(&self) -> bool {
        self.physical
    }

    #[must_use]
    pub(in crate::db::schema) const fn programs(&self) -> &[CompiledMigrationEntityProgram] {
        self.programs.as_slice()
    }
}

struct ResolvedTransition<'a> {
    store_path: &'static str,
    store_identity: TargetStoreIdentity,
    entity_tag: EntityTag,
    entity: &'a EntityFragment,
    transition: &'a EntityMigration,
}

struct WorkingStore<'a> {
    path: &'static str,
    identity: TargetStoreIdentity,
    before: &'a AcceptedSchemaRevisionBundle,
    enums: AcceptedEnumCatalog,
    composites: AcceptedCompositeCatalog,
    bindings: AcceptedSourceBindingCatalog,
    snapshots: BTreeMap<EntityTag, PersistedSchemaSnapshot>,
    touched: BTreeSet<EntityTag>,
}

impl<'a> WorkingStore<'a> {
    fn new(store: &ExistingProposalStore<'a>) -> Self {
        Self {
            path: store.path,
            identity: store.identity,
            before: store.bundle,
            enums: store.bundle.enum_catalog().clone(),
            composites: store.bundle.composite_catalog().clone(),
            bindings: store.bundle.source_bindings().clone(),
            snapshots: store.bundle.entity_snapshots().clone(),
            touched: BTreeSet::new(),
        }
    }

    fn exact_bundle(&self) -> Result<AcceptedSchemaRevisionBundle, SchemaMigrationPlanningError> {
        AcceptedSchemaRevisionBundle::new_with_source_bindings(
            self.before.revision(),
            self.path,
            self.enums.clone(),
            self.composites.clone(),
            self.bindings.clone(),
            self.snapshots.clone(),
        )
        .map_err(|_| SchemaMigrationPlanningError::RekeyedCatalogInvalid)
    }

    fn candidate(&self) -> Result<Option<CandidateSchemaRevision>, SchemaMigrationPlanningError> {
        if self.touched.is_empty() {
            return Ok(None);
        }
        let revision = self
            .before
            .revision()
            .checked_next()
            .ok_or(SchemaMigrationPlanningError::VersionGap)?;
        let bundle = AcceptedSchemaRevisionBundle::new_with_source_bindings(
            revision,
            self.path,
            self.enums.clone(),
            self.composites.clone(),
            self.bindings.clone(),
            self.snapshots.clone(),
        )
        .map_err(|_| SchemaMigrationPlanningError::CandidateMismatch)?;
        CandidateSchemaRevision::new(bundle)
            .map(Some)
            .map_err(|error| {
                SchemaMigrationPlanningError::candidate_failure(
                    error,
                    SchemaMigrationPlanningError::CandidateMismatch,
                )
            })
    }
}

/// Derive one explicit adoption without publishing it.
#[cfg(any(feature = "migration", test))]
pub(in crate::db::schema) fn plan_entity_source_adoption(
    proposal: &SchemaProposal,
    stores: &[ExistingProposalStore<'_>],
    current_lineage: &AcceptedEntitySourceLineageCatalog,
) -> Result<Vec<PlannedEntitySourceLineage>, SchemaMigrationPlanningError> {
    if proposal.migration().is_some() || !proposal.removals().is_empty() {
        return Err(SchemaMigrationPlanningError::UnexplainedSchemaDifference);
    }
    let entities = proposal_entities(proposal);
    if entities.values().any(|entity| entity.version().get() != 1) {
        return Err(SchemaMigrationPlanningError::VersionGap);
    }
    if !lower_existing_schema_proposal(proposal, stores)
        .map_err(|error| {
            SchemaMigrationPlanningError::candidate_failure(
                error,
                SchemaMigrationPlanningError::UnexplainedSchemaDifference,
            )
        })?
        .is_empty()
    {
        return Err(SchemaMigrationPlanningError::UnexplainedSchemaDifference);
    }
    let mut planned = Vec::with_capacity(entities.len());
    for (source, entity) in entities {
        let (store, entity_tag) = resolve_entity(stores, source)
            .ok_or(SchemaMigrationPlanningError::UnknownFromObject)?;
        if let Some(lineage) = current_lineage.get(store.identity, entity_tag)
            && !matches!(lineage.state(), AcceptedEntitySourceLineageState::Unadopted)
        {
            return Err(SchemaMigrationPlanningError::IdentityConflict);
        }
        planned.push(PlannedEntitySourceLineage::from_proposal(
            proposal,
            entity,
            store.identity,
            entity_tag,
        )?);
    }
    sort_source_lineage(&mut planned);
    Ok(planned)
}

/// Derive source lineage only for entities absent from current accepted catalogs.
/// Installation and ordinary creation share this owner; old lineage is untouched.
pub(in crate::db::schema) fn plan_new_entity_source_lineage(
    proposal: &SchemaProposal,
    current_bundles: &[Option<AcceptedSchemaRevisionBundle>],
    candidates: &[CandidateSchemaRevision],
) -> Result<Vec<PlannedEntitySourceLineage>, SchemaMigrationPlanningError> {
    let entities = proposal_entities(proposal);
    let assignments = proposal_assignments(proposal);
    let mut planned = Vec::with_capacity(entities.len());
    for (source, entity) in entities {
        if current_bundles
            .iter()
            .flatten()
            .any(|bundle| bundle.source_bindings().entity(source).is_some())
        {
            continue;
        }
        let store_identity = assignments
            .get(source)
            .copied()
            .ok_or(SchemaMigrationPlanningError::UnknownToObject)?;
        let candidate = candidates
            .iter()
            .find(|candidate| {
                candidate
                    .bundle()
                    .source_bindings()
                    .entity(source)
                    .is_some()
            })
            .ok_or(SchemaMigrationPlanningError::UnknownToObject)?;
        let entity_tag = candidate
            .bundle()
            .source_bindings()
            .entity(source)
            .ok_or(SchemaMigrationPlanningError::UnknownToObject)?;
        planned.push(PlannedEntitySourceLineage::from_proposal(
            proposal,
            entity,
            store_identity,
            entity_tag,
        )?);
    }
    sort_source_lineage(&mut planned);
    Ok(planned)
}

/// Bind one coordinated current plan to accepted IDs and derive exact
/// metadata candidates. The command boundary has already checked the live head;
/// lineage publication heads may predate SQL-owned catalog edits.
/// No durable state changes in this function.
pub(in crate::db::schema) fn plan_schema_migration(
    proposal: &SchemaProposal,
    stores: &[ExistingProposalStore<'_>],
    current_lineage: &AcceptedEntitySourceLineageCatalog,
) -> Result<PlannedSchemaMigration, SchemaMigrationPlanningError> {
    let plan = proposal
        .migration()
        .ok_or(SchemaMigrationPlanningError::MissingMigration)?;
    let physical = plan
        .transitions()
        .iter()
        .any(transition_requires_row_rewrite);
    let entities = proposal_entities(proposal);
    let assignments = proposal_assignments(proposal);
    let resolved = resolve_transitions(
        proposal,
        plan,
        stores,
        current_lineage,
        &entities,
        &assignments,
    )?;
    validate_unchanged_lineage(proposal, stores, current_lineage, &entities, &resolved)?;

    let mut working = BTreeMap::new();
    for store in stores {
        working.insert(store.path, WorkingStore::new(store));
    }
    apply_rename_bindings(&resolved, &mut working)?;
    rebuild_transitioned_snapshots(&resolved, &mut working)?;
    // The ordinary lowerer is the remaining-change authority. Its returned
    // candidate may deliberately contain staged activation state, which is
    // already the exact accepted plan and must not be lowered a second time.
    let (reconciled, mut lineage) = reconcile_rekeyed_view(proposal, &working)?;
    let mut reconciled_by_store = BTreeMap::new();
    for candidate in reconciled {
        reconciled_by_store.insert(candidate.store_path().to_string(), candidate);
    }
    let mut candidates = Vec::new();
    for store in working.values() {
        if let Some(candidate) = reconciled_by_store.remove(store.path) {
            candidates.push(candidate);
        } else if let Some(candidate) = store.candidate()? {
            candidates.push(candidate);
        }
    }
    if !reconciled_by_store.is_empty() {
        return Err(SchemaMigrationPlanningError::CandidateMismatch);
    }
    let programs = compile_migration_programs(proposal, stores, candidates.as_slice())
        .map_err(|_| SchemaMigrationPlanningError::UnsupportedTransform)?;
    if physical == programs.is_empty() {
        return Err(SchemaMigrationPlanningError::CandidateMismatch);
    }
    let transitioned = resolved
        .iter()
        .map(|binding| {
            PlannedEntitySourceLineage::from_proposal(
                proposal,
                binding.entity,
                binding.store_identity,
                binding.entity_tag,
            )
        })
        .collect::<Result<Vec<_>, SchemaMigrationPlanningError>>()?;
    lineage.extend(transitioned);
    sort_source_lineage(&mut lineage);
    Ok(PlannedSchemaMigration {
        candidates,
        lineage,
        physical,
        programs,
    })
}

// Publication order is canonical across adoption, installation and migration.
fn sort_source_lineage(lineage: &mut [PlannedEntitySourceLineage]) {
    icydb_schema::compact_sort_unstable_by(lineage, |left, right| {
        (left.store, left.entity).cmp(&(right.store, right.entity))
    });
}

fn proposal_entities(proposal: &SchemaProposal) -> BTreeMap<&EntitySourceKey, &EntityFragment> {
    // Canonical proposal validation already rejects duplicate source keys;
    // direct insertion avoids BTreeMap's stable bulk collector in Wasm.
    let mut entities = BTreeMap::new();
    for fragment in proposal.fragments() {
        for entity in fragment.entities() {
            entities.insert(entity.source_key(), entity);
        }
    }
    entities
}

fn proposal_assignments(
    proposal: &SchemaProposal,
) -> BTreeMap<&EntitySourceKey, TargetStoreIdentity> {
    let mut assignments = BTreeMap::new();
    for assignment in proposal.assignments() {
        assignments.insert(assignment.entity(), assignment.store());
    }
    assignments
}

fn resolve_transitions<'a>(
    proposal: &SchemaProposal,
    plan: &'a SchemaMigrationPlan,
    stores: &[ExistingProposalStore<'_>],
    current_lineage: &AcceptedEntitySourceLineageCatalog,
    entities: &BTreeMap<&EntitySourceKey, &'a EntityFragment>,
    assignments: &BTreeMap<&EntitySourceKey, TargetStoreIdentity>,
) -> Result<Vec<ResolvedTransition<'a>>, SchemaMigrationPlanningError> {
    let mut resolved = Vec::with_capacity(plan.transitions().len());
    for transition in plan.transitions() {
        let entity = entities
            .get(transition.entity())
            .copied()
            .ok_or(SchemaMigrationPlanningError::UnknownToObject)?;
        let predecessor = transition
            .from_name()
            .unwrap_or_else(|| transition.entity());
        let (store, entity_tag) = resolve_entity(stores, predecessor)
            .ok_or(SchemaMigrationPlanningError::UnknownFromObject)?;
        if assignments.get(transition.entity()).copied() != Some(store.identity) {
            return Err(SchemaMigrationPlanningError::KindMismatch);
        }
        let lineage = current_lineage
            .get(store.identity, entity_tag)
            .ok_or(SchemaMigrationPlanningError::Unadopted)?;
        let AcceptedEntitySourceLineageState::Adopted {
            version,
            source_digest,
        } = lineage.state()
        else {
            return Err(SchemaMigrationPlanningError::Unadopted);
        };
        match version.get().cmp(&transition.from().get()) {
            std::cmp::Ordering::Less => return Err(SchemaMigrationPlanningError::VersionGap),
            std::cmp::Ordering::Greater => return Err(SchemaMigrationPlanningError::Downgrade),
            std::cmp::Ordering::Equal => {}
        }
        let current_digest = proposal
            .entity_source_digest(transition.entity())
            .map_err(|_| SchemaMigrationPlanningError::CandidateMismatch)?;
        if *source_digest == current_digest && transition.transforms().is_empty() {
            return Err(SchemaMigrationPlanningError::EmptyEntityVersionBump);
        }
        // A companion may change only relation target names explained by this
        // plan. Comparing complete source meaning also protects named types and
        // referenced target fields that a snapshot-only comparison would miss.
        if transition.from_name().is_none()
            && transition.renames().is_empty()
            && transition.transforms().is_empty()
            && proposal
                .entity_source_digest_before_entity_renames(transition.entity())
                .map_err(|_| SchemaMigrationPlanningError::CandidateMismatch)?
                != *source_digest
        {
            return Err(SchemaMigrationPlanningError::UnexplainedSchemaDifference);
        }
        resolved.push(ResolvedTransition {
            store_path: store.path,
            store_identity: store.identity,
            entity_tag,
            entity,
            transition,
        });
    }
    Ok(resolved)
}

fn validate_unchanged_lineage(
    proposal: &SchemaProposal,
    stores: &[ExistingProposalStore<'_>],
    current_lineage: &AcceptedEntitySourceLineageCatalog,
    entities: &BTreeMap<&EntitySourceKey, &EntityFragment>,
    resolved: &[ResolvedTransition<'_>],
) -> Result<(), SchemaMigrationPlanningError> {
    for (source, entity) in entities {
        if resolved
            .iter()
            .any(|binding| binding.entity.source_key() == *source)
        {
            continue;
        }
        // A renamed predecessor source may be reused by a fresh declaration.
        // Its old tag belongs to the resolved transition, not the addition.
        let Some((store, entity_tag)) = resolve_entity(stores, source).filter(|(store, tag)| {
            !resolved.iter().any(|binding| {
                binding.store_identity == store.identity && binding.entity_tag == *tag
            })
        }) else {
            if entity.version().get() != 1 {
                return Err(SchemaMigrationPlanningError::VersionGap);
            }
            continue;
        };
        let lineage = current_lineage
            .get(store.identity, entity_tag)
            .ok_or(SchemaMigrationPlanningError::Unadopted)?;
        let AcceptedEntitySourceLineageState::Adopted {
            version,
            source_digest,
        } = lineage.state()
        else {
            return Err(SchemaMigrationPlanningError::Unadopted);
        };
        match version.get().cmp(&entity.version().get()) {
            std::cmp::Ordering::Less => return Err(SchemaMigrationPlanningError::MissingMigration),
            std::cmp::Ordering::Greater => return Err(SchemaMigrationPlanningError::Downgrade),
            std::cmp::Ordering::Equal => {}
        }
        if *source_digest
            != proposal
                .entity_source_digest(source)
                .map_err(|_| SchemaMigrationPlanningError::CandidateMismatch)?
        {
            return Err(SchemaMigrationPlanningError::MissingMigration);
        }
    }
    Ok(())
}

fn resolve_entity<'a>(
    stores: &'a [ExistingProposalStore<'_>],
    source: &EntitySourceKey,
) -> Option<(&'a ExistingProposalStore<'a>, EntityTag)> {
    let mut resolved = stores.iter().filter_map(|store| {
        store
            .bundle
            .source_bindings()
            .entity(source)
            .map(|entity| (store, entity))
    });
    let first = resolved.next()?;
    resolved.next().is_none().then_some(first)
}

fn apply_rename_bindings(
    transitions: &[ResolvedTransition<'_>],
    stores: &mut BTreeMap<&'static str, WorkingStore<'_>>,
) -> Result<(), SchemaMigrationPlanningError> {
    let mut shared = BTreeSet::new();
    for binding in transitions {
        let store = stores
            .get_mut(binding.store_path)
            .ok_or(SchemaMigrationPlanningError::CandidateMismatch)?;
        if let Some(from) = binding.transition.from_name() {
            store
                .bindings
                .rekey_entity(
                    from,
                    binding.entity.source_key().clone(),
                    binding.entity_tag,
                )
                .map_err(|_| SchemaMigrationPlanningError::IdentityConflict)?;
        }
        for rename in binding.transition.renames() {
            apply_rename_binding(binding, rename, store, &mut shared)?;
        }
        store.touched.insert(binding.entity_tag);
    }
    Ok(())
}

#[expect(
    clippy::too_many_lines,
    reason = "the closed rename matrix is kept exhaustive in one kind-dispatch boundary"
)]
fn apply_rename_binding(
    binding: &ResolvedTransition<'_>,
    rename: &SchemaMigrationRename,
    store: &mut WorkingStore<'_>,
    shared: &mut BTreeSet<(&'static str, u8, String, String, String)>,
) -> Result<(), SchemaMigrationPlanningError> {
    match rename {
        SchemaMigrationRename::Field { from, to } => {
            let id = store
                .bindings
                .field(binding.entity_tag, from)
                .ok_or(SchemaMigrationPlanningError::UnknownFromObject)?;
            store
                .bindings
                .rekey_field(binding.entity_tag, from, to.clone(), id)
                .map_err(|_| SchemaMigrationPlanningError::IdentityConflict)
        }
        SchemaMigrationRename::NamedType { from, to } => {
            let key = (
                store.path,
                1,
                String::new(),
                from.to_string(),
                to.to_string(),
            );
            if !shared.insert(key) {
                return Ok(());
            }
            let identity = store
                .bindings
                .named_type(from)
                .ok_or(SchemaMigrationPlanningError::UnknownFromObject)?;
            store
                .bindings
                .rekey_named_type(from, to.clone(), identity)
                .map_err(|_| SchemaMigrationPlanningError::IdentityConflict)?;
            match identity {
                AcceptedNamedTypeIdentity::Enum(id) => {
                    store.enums = store
                        .enums
                        .clone()
                        .with_renamed_type(id, to.to_string())
                        .map_err(|_| SchemaMigrationPlanningError::KindMismatch)?;
                }
                AcceptedNamedTypeIdentity::Composite(id) => {
                    store.composites = store
                        .composites
                        .clone()
                        .with_renamed_type(id, to.to_string())
                        .map_err(|_| SchemaMigrationPlanningError::KindMismatch)?;
                }
            }
            Ok(())
        }
        SchemaMigrationRename::EnumVariant {
            named_type,
            from,
            to,
        } => {
            let key = (
                store.path,
                2,
                named_type.to_string(),
                from.to_string(),
                to.to_string(),
            );
            if !shared.insert(key) {
                return Ok(());
            }
            let owner = migration_target_type(binding.transition, named_type);
            let AcceptedNamedTypeIdentity::Enum(type_id) = store
                .bindings
                .named_type(&owner)
                .ok_or(SchemaMigrationPlanningError::UnknownFromObject)?
            else {
                return Err(SchemaMigrationPlanningError::KindMismatch);
            };
            let variant = store
                .bindings
                .enum_variant(type_id, from)
                .ok_or(SchemaMigrationPlanningError::UnknownFromObject)?;
            store
                .bindings
                .rekey_enum_variant(type_id, from, to.clone(), variant)
                .map_err(|_| SchemaMigrationPlanningError::IdentityConflict)?;
            store.enums = store
                .enums
                .clone()
                .with_renamed_variant(type_id, variant, to.to_string())
                .map_err(|_| SchemaMigrationPlanningError::KindMismatch)?;
            Ok(())
        }
        SchemaMigrationRename::RecordField {
            named_type,
            from,
            to,
        } => {
            let key = (
                store.path,
                3,
                named_type.to_string(),
                from.to_string(),
                to.to_string(),
            );
            if !shared.insert(key) {
                return Ok(());
            }
            let owner = migration_target_type(binding.transition, named_type);
            let AcceptedNamedTypeIdentity::Composite(type_id) =
                store
                    .bindings
                    .named_type(&owner)
                    .ok_or(SchemaMigrationPlanningError::UnknownFromObject)?
            else {
                return Err(SchemaMigrationPlanningError::KindMismatch);
            };
            let field = store
                .bindings
                .composite_field(type_id, from)
                .ok_or(SchemaMigrationPlanningError::UnknownFromObject)?;
            store
                .bindings
                .rekey_composite_field(type_id, from, to.clone(), field)
                .map_err(|_| SchemaMigrationPlanningError::IdentityConflict)?;
            store.composites = store
                .composites
                .clone()
                .with_renamed_record_field(type_id, field, to.to_string(), &store.enums)
                .map_err(|_| SchemaMigrationPlanningError::KindMismatch)?;
            Ok(())
        }
        SchemaMigrationRename::Relation { from, to } => {
            let id = store
                .bindings
                .relation(binding.entity_tag, from)
                .ok_or(SchemaMigrationPlanningError::UnknownFromObject)?;
            store
                .bindings
                .rekey_relation(binding.entity_tag, from, to.clone(), id)
                .map_err(|_| SchemaMigrationPlanningError::IdentityConflict)
        }
        SchemaMigrationRename::Constraint { from, to } => {
            let id = store
                .bindings
                .constraint(binding.entity_tag, from)
                .ok_or(SchemaMigrationPlanningError::UnknownFromObject)?;
            store
                .bindings
                .rekey_constraint(binding.entity_tag, from, to.clone(), id)
                .map_err(|_| SchemaMigrationPlanningError::IdentityConflict)
        }
        SchemaMigrationRename::Rule {
            named_type,
            from,
            to,
        } => rekey_rule_constraint(binding, store, named_type, from, to),
    }
}

fn migration_target_type(transition: &EntityMigration, accepted: &TypeSourceKey) -> TypeSourceKey {
    transition
        .renames()
        .iter()
        .find_map(|rename| match rename {
            SchemaMigrationRename::NamedType { from, to } if from == accepted => Some(to.clone()),
            _ => None,
        })
        .unwrap_or_else(|| accepted.clone())
}

fn rekey_rule_constraint(
    binding: &ResolvedTransition<'_>,
    store: &mut WorkingStore<'_>,
    accepted_type: &TypeSourceKey,
    accepted_rule: &RuleSourceKey,
    current_rule: &RuleSourceKey,
) -> Result<(), SchemaMigrationPlanningError> {
    let current_type = migration_target_type(binding.transition, accepted_type);
    let current = binding
        .entity
        .constraints()
        .iter()
        .find_map(|constraint| match constraint.kind() {
            ConstraintFragmentKind::TargetedRule(rule)
                if rule.target_type() == &current_type && rule.rule() == current_rule =>
            {
                Some((constraint, rule))
            }
            ConstraintFragmentKind::Check(_) | ConstraintFragmentKind::TargetedRule(_) => None,
        })
        .ok_or(SchemaMigrationPlanningError::UnknownToObject)?;
    let accepted_root = inverse_field_source(binding.transition, current.1.root());
    let accepted_source =
        ConstraintSourceKey::for_targeted_field_rule(&accepted_root, accepted_type, accepted_rule);
    let id = store
        .bindings
        .constraint(binding.entity_tag, &accepted_source)
        .ok_or(SchemaMigrationPlanningError::UnknownFromObject)?;
    store
        .bindings
        .rekey_constraint(
            binding.entity_tag,
            &accepted_source,
            current.0.source_key().clone(),
            id,
        )
        .map_err(|_| SchemaMigrationPlanningError::IdentityConflict)
}

/// Resolve a current source key in the predecessor namespace using declared renames.
pub(in crate::db::schema) fn inverse_field_source(
    transition: &EntityMigration,
    current: &FieldSourceKey,
) -> FieldSourceKey {
    transition
        .renames()
        .iter()
        .find_map(|rename| match rename {
            SchemaMigrationRename::Field { from, to } if to == current => Some(from.clone()),
            _ => None,
        })
        .unwrap_or_else(|| current.clone())
}

fn rebuild_transitioned_snapshots(
    transitions: &[ResolvedTransition<'_>],
    stores: &mut BTreeMap<&'static str, WorkingStore<'_>>,
) -> Result<(), SchemaMigrationPlanningError> {
    // First establish every current entity path, field label, and index
    // contract. Cross-entity relation targets can then resolve simultaneously
    // against the complete rekeyed view.
    for binding in transitions {
        let store = stores
            .get_mut(binding.store_path)
            .ok_or(SchemaMigrationPlanningError::CandidateMismatch)?;
        let before = store
            .snapshots
            .get(&binding.entity_tag)
            .cloned()
            .ok_or(SchemaMigrationPlanningError::UnknownFromObject)?;
        if !before.constraint_activations().is_empty()
            || !before.candidate_indexes().is_empty()
            || !before.candidate_relations().is_empty()
        {
            return Err(SchemaMigrationPlanningError::UnexplainedSchemaDifference);
        }
        let target_version = next_schema_version(&before)?;
        let rebuilt = rebuild_fields(binding, store, &before)?;
        let constraint_catalog = rebuild_physical_field_constraints(
            before.constraint_catalog(),
            before.fields(),
            rebuilt.fields.as_slice(),
        )?;
        let provisional = rebuild_snapshot_shell(
            binding,
            &before,
            target_version,
            rebuilt.layout,
            rebuilt.fields,
            before.indexes().to_vec(),
            before.relations().to_vec(),
            constraint_catalog,
        );
        let indexes = rebuild_indexes(binding, store, &before, &provisional)?;
        let constraint_catalog = rebuild_physical_index_constraints(
            provisional.constraint_catalog(),
            before.indexes(),
            indexes.as_slice(),
        )?;
        let with_indexes = rebuild_snapshot_shell(
            binding,
            &before,
            target_version,
            provisional.row_layout().clone(),
            provisional.fields().to_vec(),
            indexes,
            before.relations().to_vec(),
            constraint_catalog,
        );
        install_reserved_migration_relation_ids(binding, store, with_indexes)?;
    }

    for binding in transitions {
        let relations = rebuild_relations(binding, stores)?;
        let store = stores
            .get_mut(binding.store_path)
            .ok_or(SchemaMigrationPlanningError::CandidateMismatch)?;
        let current = store
            .snapshots
            .get(&binding.entity_tag)
            .cloned()
            .ok_or(SchemaMigrationPlanningError::UnknownFromObject)?;
        let with_relations = rebuild_snapshot_shell(
            binding,
            &current,
            current.version(),
            current.row_layout().clone(),
            current.fields().to_vec(),
            current.indexes().to_vec(),
            relations.clone(),
            rebuild_physical_relation_constraints(
                current.constraint_catalog(),
                current.relations(),
                relations.as_slice(),
            )?,
        );
        store.snapshots.insert(binding.entity_tag, with_relations);
    }

    for binding in transitions {
        let store = stores
            .get_mut(binding.store_path)
            .ok_or(SchemaMigrationPlanningError::CandidateMismatch)?;
        let current = store
            .snapshots
            .get(&binding.entity_tag)
            .cloned()
            .ok_or(SchemaMigrationPlanningError::UnknownFromObject)?;
        let constraints = rebuild_constraints(binding, store, &current)?;
        let target = rebuild_snapshot_shell(
            binding,
            &current,
            current.version(),
            current.row_layout().clone(),
            current.fields().to_vec(),
            current.indexes().to_vec(),
            current.relations().to_vec(),
            constraints,
        );
        store.snapshots.insert(binding.entity_tag, target);
    }
    Ok(())
}

fn install_reserved_migration_relation_ids(
    binding: &ResolvedTransition<'_>,
    store: &mut WorkingStore<'_>,
    snapshot: PersistedSchemaSnapshot,
) -> Result<(), SchemaMigrationPlanningError> {
    let allocator = reserve_new_migration_relations(binding, store, &snapshot)?;
    store.snapshots.insert(
        binding.entity_tag,
        snapshot.with_relation_id_allocator(allocator),
    );
    Ok(())
}

struct RebuiltMigrationFields {
    fields: Vec<PersistedFieldSnapshot>,
    layout: SchemaRowLayout,
}

fn rebuild_fields(
    binding: &ResolvedTransition<'_>,
    store: &mut WorkingStore<'_>,
    before: &PersistedSchemaSnapshot,
) -> Result<RebuiltMigrationFields, SchemaMigrationPlanningError> {
    let transform_targets = binding
        .transition
        .transforms()
        .iter()
        .map(SchemaMigrationTransform::target)
        .cloned()
        .collect::<BTreeSet<_>>();
    let physical = transition_requires_row_rewrite(binding.transition);
    let layout_version = if physical {
        before
            .row_layout()
            .current_version()
            .checked_next()
            .ok_or(SchemaMigrationPlanningError::VersionGap)?
    } else {
        before.row_layout().current_version()
    };
    let mut fields = before.fields().to_vec();
    let mut layout = before.row_layout().field_to_slot().to_vec();
    let mut next_field_id = fields
        .iter()
        .map(|field| field.id().get())
        .max()
        .unwrap_or(0);
    for proposed in binding.entity.fields() {
        let target = transform_targets.contains(proposed.source_key());
        match store
            .bindings
            .field(binding.entity_tag, proposed.source_key())
        {
            Some(id) => {
                let position = fields
                    .iter()
                    .position(|field| field.id() == id)
                    .ok_or(SchemaMigrationPlanningError::IdentityConflict)?;
                if !fields[position].generated() {
                    return Err(SchemaMigrationPlanningError::IdentityConflict);
                }
                fields[position] = if target {
                    let introduced_in_layout = fields[position].introduced_in_layout();
                    lower_proposed_field(
                        proposed,
                        id,
                        fields[position].slot(),
                        introduced_in_layout,
                        &store.bindings,
                        &store.enums,
                        &store.composites,
                    )
                    .map_err(|_| SchemaMigrationPlanningError::UnsupportedTransform)?
                } else {
                    let nested_leaves =
                        lower_migration_nested_leaves(fields[position].kind(), &store.composites)
                            .map_err(|_| SchemaMigrationPlanningError::RekeyedCatalogInvalid)?;
                    fields[position].clone_with_migration_metadata(
                        proposed.name().as_str().to_string(),
                        nested_leaves,
                    )
                };
            }
            None if target => {
                next_field_id = next_field_id
                    .checked_add(1)
                    .ok_or(SchemaMigrationPlanningError::UnsupportedTransform)?;
                let id = crate::db::schema::FieldId::new(next_field_id);
                let slot = SchemaFieldSlot::from_generated_index(layout.len())
                    .ok_or(SchemaMigrationPlanningError::UnsupportedTransform)?;
                store
                    .bindings
                    .insert_migration_field(binding.entity_tag, proposed.source_key().clone(), id)
                    .map_err(|_| SchemaMigrationPlanningError::IdentityConflict)?;
                fields.push(
                    lower_proposed_field(
                        proposed,
                        id,
                        slot,
                        layout_version,
                        &store.bindings,
                        &store.enums,
                        &store.composites,
                    )
                    .map_err(|_| SchemaMigrationPlanningError::UnsupportedTransform)?,
                );
                layout.push((id, slot));
            }
            None => {
                // A genuinely new non-transform field remains absent from the
                // temporary view so ordinary catalog-native lowering owns it.
            }
        }
    }
    let layout = if physical {
        // Every retained field is materialized in the new layout, including
        // SQL-owned fields. Historical absence fills no longer apply there.
        for field in &mut fields {
            *field = field.clone_for_full_layout_rewrite(field.id(), field.slot());
        }
        SchemaRowLayout::single_version(layout_version, layout)
    } else {
        before.row_layout().clone()
    };
    Ok(RebuiltMigrationFields { fields, layout })
}

#[expect(
    clippy::too_many_arguments,
    reason = "the shell boundary makes every candidate catalog component explicit"
)]
fn rebuild_snapshot_shell(
    binding: &ResolvedTransition<'_>,
    before: &PersistedSchemaSnapshot,
    version: SchemaVersion,
    row_layout: SchemaRowLayout,
    fields: Vec<PersistedFieldSnapshot>,
    indexes: Vec<PersistedIndexSnapshot>,
    relations: Vec<PersistedRelationEdgeSnapshot>,
    constraints: AcceptedConstraintCatalog,
) -> PersistedSchemaSnapshot {
    PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        version,
        binding.entity.source_key().to_string(),
        binding.entity.name().as_str().to_string(),
        before.primary_key_field_ids().to_vec(),
        row_layout,
        fields,
        indexes,
    )
    .with_constraint_catalog(constraints)
    .with_relation_id_allocator(before.relation_id_allocator())
    .with_relations(relations)
}

fn rebuild_physical_field_constraints(
    before: &AcceptedConstraintCatalog,
    before_fields: &[PersistedFieldSnapshot],
    target_fields: &[PersistedFieldSnapshot],
) -> Result<AcceptedConstraintCatalog, SchemaMigrationPlanningError> {
    let mut catalog = before.clone();
    for target in target_fields {
        let previous = before_fields.iter().find(|field| field.id() == target.id());
        match previous.map(PersistedFieldSnapshot::nullable) {
            Some(false) if target.nullable() => {
                catalog = catalog
                    .with_removed_not_null(target.id())
                    .map_err(|_| SchemaMigrationPlanningError::UnsupportedTransform)?;
            }
            None | Some(true) if !target.nullable() => {
                catalog = catalog
                    .with_added_not_null(target)
                    .map_err(|_| SchemaMigrationPlanningError::UnsupportedTransform)?;
            }
            None | Some(false | true) => {}
        }
    }
    Ok(catalog)
}

fn rebuild_physical_index_constraints(
    before: &AcceptedConstraintCatalog,
    before_indexes: &[PersistedIndexSnapshot],
    target_indexes: &[PersistedIndexSnapshot],
) -> Result<AcceptedConstraintCatalog, SchemaMigrationPlanningError> {
    let mut catalog = before.clone();
    for target in target_indexes {
        let previous = before_indexes
            .iter()
            .find(|index| index.schema_id() == target.schema_id());
        match previous.map(PersistedIndexSnapshot::unique) {
            Some(true) if !target.unique() => {
                catalog = catalog
                    .with_removed_unique(target.schema_id())
                    .map_err(|_| SchemaMigrationPlanningError::UnsupportedTransform)?;
            }
            None | Some(false) if target.unique() => {
                catalog = catalog
                    .with_added_unique(target)
                    .map_err(|_| SchemaMigrationPlanningError::UnsupportedTransform)?;
            }
            None | Some(false | true) => {}
        }
    }
    Ok(catalog)
}

fn next_schema_version(
    before: &PersistedSchemaSnapshot,
) -> Result<SchemaVersion, SchemaMigrationPlanningError> {
    before
        .version()
        .get()
        .checked_add(1)
        .map(SchemaVersion::new)
        .ok_or(SchemaMigrationPlanningError::VersionGap)
}

#[expect(
    clippy::too_many_lines,
    reason = "one pass owns existing, renamed, and newly reserved physical index identity"
)]
fn rebuild_indexes(
    binding: &ResolvedTransition<'_>,
    store: &mut WorkingStore<'_>,
    before: &PersistedSchemaSnapshot,
    provisional: &PersistedSchemaSnapshot,
) -> Result<Vec<PersistedIndexSnapshot>, SchemaMigrationPlanningError> {
    let mut indexes = before.indexes().to_vec();
    let mut claimed = BTreeSet::new();
    let physical = transition_requires_row_rewrite(binding.transition);
    let generation = store
        .before
        .revision()
        .checked_next()
        .ok_or(SchemaMigrationPlanningError::VersionGap)?
        .get();
    let mut next_schema_id = indexes
        .iter()
        .map(|index| index.schema_id().get())
        .max()
        .unwrap_or(0);
    let mut next_ordinal = indexes
        .iter()
        .map(PersistedIndexSnapshot::ordinal)
        .max()
        .unwrap_or(0);
    for proposed in binding.entity.indexes() {
        let id = if let Some(id) = store
            .bindings
            .index(binding.entity_tag, proposed.source_key())
        {
            id
        } else {
            match infer_and_rekey_index(binding, store, before, provisional, proposed)? {
                Some(id) => id,
                None if physical => {
                    next_schema_id = next_schema_id
                        .checked_add(1)
                        .ok_or(SchemaMigrationPlanningError::UnsupportedTransform)?;
                    next_ordinal = next_ordinal
                        .checked_add(1)
                        .ok_or(SchemaMigrationPlanningError::UnsupportedTransform)?;
                    let id = SchemaIndexId::new(next_schema_id)
                        .ok_or(SchemaMigrationPlanningError::UnsupportedTransform)?;
                    store
                        .bindings
                        .insert_migration_index(
                            binding.entity_tag,
                            proposed.source_key().clone(),
                            id,
                        )
                        .map_err(|_| SchemaMigrationPlanningError::IdentityConflict)?;
                    indexes.push(
                        lower_new_migration_index(
                            proposed,
                            id,
                            next_ordinal,
                            generation,
                            store.path,
                            &store.enums,
                            &store.composites,
                            binding.entity_tag,
                            provisional,
                            &store.bindings,
                        )
                        .map_err(|_| SchemaMigrationPlanningError::UnexplainedSchemaDifference)?,
                    );
                    claimed.insert(id);
                    continue;
                }
                None => continue,
            }
        };
        if !claimed.insert(id) {
            return Err(SchemaMigrationPlanningError::IdentityConflict);
        }
        let accepted = before
            .indexes()
            .iter()
            .find(|index| index.schema_id() == id)
            .ok_or(SchemaMigrationPlanningError::IdentityConflict)?;
        let target = lower_migration_index(
            proposed,
            accepted,
            &store.enums,
            &store.composites,
            binding.entity_tag,
            provisional,
            &store.bindings,
        )
        .map_err(|_| SchemaMigrationPlanningError::UnexplainedSchemaDifference)?;
        let expected = relabel_index_for_fields(accepted, before, provisional);
        let semantics_changed = !index_contract_matches_ignoring_name(&expected, &target);
        if semantics_changed && !physical {
            return Err(SchemaMigrationPlanningError::UnexplainedSchemaDifference);
        }
        let position = indexes
            .iter()
            .position(|index| index.schema_id() == id)
            .ok_or(SchemaMigrationPlanningError::IdentityConflict)?;
        indexes[position] = if physical {
            target.clone_with_schema_identity(id, target.ordinal(), generation)
        } else {
            target
        };
    }
    icydb_schema::compact_sort_unstable_by(&mut indexes, |left, right| {
        left.ordinal().cmp(&right.ordinal())
    });
    Ok(indexes)
}

fn infer_and_rekey_index(
    binding: &ResolvedTransition<'_>,
    store: &mut WorkingStore<'_>,
    before: &PersistedSchemaSnapshot,
    provisional: &PersistedSchemaSnapshot,
    proposed: &icydb_schema::IndexFragment,
) -> Result<Option<crate::db::schema::SchemaIndexId>, SchemaMigrationPlanningError> {
    let mut matches = Vec::new();
    for (old_source, id) in store.bindings.index_bindings(binding.entity_tag) {
        let accepted = before
            .indexes()
            .iter()
            .find(|index| index.schema_id() == id)
            .ok_or(SchemaMigrationPlanningError::IdentityConflict)?;
        if !accepted.generated() {
            continue;
        }
        let target = lower_migration_index(
            proposed,
            accepted,
            &store.enums,
            &store.composites,
            binding.entity_tag,
            provisional,
            &store.bindings,
        )
        .map_err(|_| SchemaMigrationPlanningError::UnexplainedSchemaDifference)?;
        let expected = relabel_index_for_fields(accepted, before, provisional);
        if index_contract_matches_ignoring_name(&expected, &target) {
            matches.push((old_source.clone(), id));
        }
    }
    let [(old_source, id)] = matches.as_slice() else {
        return if matches.is_empty() {
            Ok(None)
        } else {
            Err(SchemaMigrationPlanningError::IdentityConflict)
        };
    };
    store
        .bindings
        .rekey_index(
            binding.entity_tag,
            old_source,
            proposed.source_key().clone(),
            *id,
        )
        .map_err(|_| SchemaMigrationPlanningError::IdentityConflict)?;
    Ok(Some(*id))
}

fn relabel_index_for_fields(
    index: &PersistedIndexSnapshot,
    before: &PersistedSchemaSnapshot,
    after: &PersistedSchemaSnapshot,
) -> PersistedIndexSnapshot {
    let mut relabeled = index.clone();
    for old_field in before.fields() {
        let Some(new_field) = after
            .fields()
            .iter()
            .find(|field| field.id() == old_field.id())
        else {
            continue;
        };
        if old_field.name() != new_field.name() {
            relabeled =
                relabeled.clone_with_renamed_field_path_root(old_field.id(), new_field.name());
        }
    }
    relabeled
}

fn index_contract_matches_ignoring_name(
    accepted: &PersistedIndexSnapshot,
    target: &PersistedIndexSnapshot,
) -> bool {
    accepted.schema_id() == target.schema_id()
        && accepted.ordinal() == target.ordinal()
        && accepted.physical_generation() == target.physical_generation()
        && accepted.store() == target.store()
        && accepted.unique() == target.unique()
        && accepted.origin() == target.origin()
        && accepted.key() == target.key()
        && accepted.predicate() == target.predicate()
}

fn rebuild_relations(
    binding: &ResolvedTransition<'_>,
    stores: &BTreeMap<&'static str, WorkingStore<'_>>,
) -> Result<Vec<PersistedRelationEdgeSnapshot>, SchemaMigrationPlanningError> {
    let store = stores
        .get(binding.store_path)
        .ok_or(SchemaMigrationPlanningError::CandidateMismatch)?;
    let before = store
        .snapshots
        .get(&binding.entity_tag)
        .ok_or(SchemaMigrationPlanningError::UnknownFromObject)?;
    let mut relations = before.relations().to_vec();
    let physical = transition_requires_row_rewrite(binding.transition);
    let generation = store
        .before
        .revision()
        .checked_next()
        .ok_or(SchemaMigrationPlanningError::VersionGap)?
        .get();
    for proposed in binding.entity.relations() {
        let Some(id) = store
            .bindings
            .relation(binding.entity_tag, proposed.source_key())
        else {
            continue;
        };
        let replacement = {
            let source =
                lower_relation_source(proposed.source(), binding.entity_tag, &store.bindings)
                    .map_err(|_| SchemaMigrationPlanningError::KindMismatch)?;
            let (target_store, target_tag, target) =
                resolve_working_entity(stores, proposed.target_entity())
                    .ok_or(SchemaMigrationPlanningError::UnknownToObject)?;
            let target_fields = proposed
                .target_fields()
                .iter()
                .map(|source| {
                    target_store
                        .bindings
                        .field(target_tag, source)
                        .ok_or(SchemaMigrationPlanningError::UnknownToObject)
                })
                .collect::<Result<Vec<_>, _>>()?;
            if target_fields != target.primary_key_field_ids() {
                return Err(SchemaMigrationPlanningError::KindMismatch);
            }
            match before
                .relations()
                .iter()
                .find(|relation| relation.id() == id)
            {
                Some(accepted) => {
                    if &source != accepted.source() {
                        return Err(SchemaMigrationPlanningError::KindMismatch);
                    }
                    let replacement = accepted.clone_with_metadata(
                        proposed.name().as_str().to_string(),
                        target.entity_path().to_string(),
                    );
                    // Source identity can stay fixed while a transform replaces
                    // its values. Isolate those reverse edges in the candidate
                    // generation, just as for a newly declared relation.
                    let source_transformed =
                        binding.transition.transforms().iter().any(|transform| {
                            store
                                .bindings
                                .field(binding.entity_tag, transform.target())
                                .is_some_and(|field| source.uses_root_field(field))
                        });
                    if source_transformed {
                        replacement.clone_with_physical_generation(generation)
                    } else {
                        replacement
                    }
                }
                None if physical => relation_edge_from_source(
                    id,
                    proposed.name().as_str().to_string(),
                    target.entity_path().to_string(),
                    source,
                )
                .clone_with_physical_generation(generation),
                None => return Err(SchemaMigrationPlanningError::IdentityConflict),
            }
        };
        if let Some(position) = relations.iter().position(|relation| relation.id() == id) {
            relations[position] = replacement;
        } else {
            relations.push(replacement);
        }
    }
    Ok(relations)
}

fn reserve_new_migration_relations(
    binding: &ResolvedTransition<'_>,
    store: &mut WorkingStore<'_>,
    snapshot: &PersistedSchemaSnapshot,
) -> Result<crate::db::schema::RelationIdAllocator, SchemaMigrationPlanningError> {
    let mut allocator = snapshot.relation_id_allocator();
    if !transition_requires_row_rewrite(binding.transition) {
        return Ok(allocator);
    }
    for proposed in binding.entity.relations() {
        if store
            .bindings
            .relation(binding.entity_tag, proposed.source_key())
            .is_some()
        {
            continue;
        }
        let (reserved, id) = allocator
            .checked_reserve()
            .ok_or(SchemaMigrationPlanningError::UnsupportedTransform)?;
        allocator = reserved;
        store
            .bindings
            .insert_migration_relation(binding.entity_tag, proposed.source_key().clone(), id)
            .map_err(|_| SchemaMigrationPlanningError::IdentityConflict)?;
    }
    Ok(allocator)
}

fn rebuild_physical_relation_constraints(
    before: &AcceptedConstraintCatalog,
    before_relations: &[PersistedRelationEdgeSnapshot],
    target_relations: &[PersistedRelationEdgeSnapshot],
) -> Result<AcceptedConstraintCatalog, SchemaMigrationPlanningError> {
    let mut catalog = before.clone();
    for target in target_relations {
        if before_relations
            .iter()
            .all(|relation| relation.id() != target.id())
        {
            catalog = catalog
                .with_added_relation(target)
                .map_err(|_| SchemaMigrationPlanningError::UnsupportedTransform)?;
        }
    }
    Ok(catalog)
}

fn resolve_working_entity<'a>(
    stores: &'a BTreeMap<&'static str, WorkingStore<'_>>,
    source: &EntitySourceKey,
) -> Option<(&'a WorkingStore<'a>, EntityTag, &'a PersistedSchemaSnapshot)> {
    let mut resolved = stores.values().filter_map(|store| {
        let entity = store.bindings.entity(source)?;
        Some((store, entity, store.snapshots.get(&entity)?))
    });
    let first = resolved.next()?;
    resolved.next().is_none().then_some(first)
}

fn rebuild_constraints(
    binding: &ResolvedTransition<'_>,
    store: &mut WorkingStore<'_>,
    target: &PersistedSchemaSnapshot,
) -> Result<AcceptedConstraintCatalog, SchemaMigrationPlanningError> {
    if !transition_requires_row_rewrite(binding.transition) {
        return rebuild_metadata_constraints(binding, store, target);
    }
    let mut proposed_by_id = BTreeMap::new();
    for constraint in binding.entity.constraints() {
        if let Some(id) = store
            .bindings
            .constraint(binding.entity_tag, constraint.source_key())
        {
            proposed_by_id.insert(id, constraint);
        }
    }
    let mut observed = BTreeSet::new();
    let constraints = target
        .constraints()
        .iter()
        .filter_map(|constraint| {
            if constraint.origin() == ConstraintOrigin::Generated
                && matches!(
                    constraint.kind(),
                    AcceptedConstraintKind::Check { .. }
                        | AcceptedConstraintKind::TargetedRule { .. }
                )
            {
                let proposed = proposed_by_id.get(&constraint.id()).copied()?;
                observed.insert(constraint.id());
                let kind = compile_migration_constraint_kind(binding, store, target, proposed);
                return Some(kind.map(|kind| {
                    AcceptedConstraintSnapshot::new(
                        constraint.id(),
                        proposed.name().as_str().to_string(),
                        ConstraintOrigin::Generated,
                        kind,
                    )
                }));
            }
            let name = resolve_constraint_name(constraint, target);
            Some(name.map(|name| constraint.clone_with_name(name)))
        })
        .collect::<Result<Vec<AcceptedConstraintSnapshot>, SchemaMigrationPlanningError>>()?;
    if proposed_by_id.keys().any(|id| !observed.contains(id)) {
        return Err(SchemaMigrationPlanningError::IdentityConflict);
    }
    let mut catalog = AcceptedConstraintCatalog::from_persisted_parts(
        target.constraint_catalog().allocator(),
        constraints,
        Vec::new(),
    );
    for proposed in binding.entity.constraints() {
        if store
            .bindings
            .constraint(binding.entity_tag, proposed.source_key())
            .is_some()
        {
            continue;
        }
        let kind = compile_migration_constraint_kind(binding, store, target, proposed)?;
        catalog = match kind {
            AcceptedConstraintKind::Check { expression } => catalog
                .with_added_check(
                    proposed.name().as_str().to_string(),
                    ConstraintOrigin::Generated,
                    *expression,
                )
                .map_err(|_| SchemaMigrationPlanningError::UnsupportedTransform)?,
            AcceptedConstraintKind::TargetedRule { target, operation } => catalog
                .with_added_targeted_rule(
                    proposed.name().as_str().to_string(),
                    ConstraintOrigin::Generated,
                    target,
                    *operation,
                )
                .map_err(|_| SchemaMigrationPlanningError::UnsupportedTransform)?,
            AcceptedConstraintKind::PrimaryKey
            | AcceptedConstraintKind::NotNull { .. }
            | AcceptedConstraintKind::Unique { .. }
            | AcceptedConstraintKind::Relation { .. } => {
                return Err(SchemaMigrationPlanningError::UnsupportedTransform);
            }
        };
        let id = ConstraintId::new(catalog.allocator().high_water())
            .ok_or(SchemaMigrationPlanningError::UnsupportedTransform)?;
        store
            .bindings
            .insert_constraint(binding.entity_tag, proposed.source_key().clone(), id)
            .map_err(|_| SchemaMigrationPlanningError::IdentityConflict)?;
    }
    Ok(catalog)
}

// Catalog identities own unique/relation display names in both migration paths.
// Generated row-constraint proposal names remain the caller's responsibility.
fn resolve_constraint_name(
    constraint: &AcceptedConstraintSnapshot,
    target: &PersistedSchemaSnapshot,
) -> Result<String, SchemaMigrationPlanningError> {
    let name = match constraint.kind() {
        AcceptedConstraintKind::Unique { index_id } => target
            .indexes()
            .iter()
            .find(|index| index.schema_id() == *index_id)
            .map(PersistedIndexSnapshot::name),
        AcceptedConstraintKind::Relation { relation_id } => target
            .relations()
            .iter()
            .find(|relation| relation.id() == *relation_id)
            .map(PersistedRelationEdgeSnapshot::name),
        AcceptedConstraintKind::PrimaryKey
        | AcceptedConstraintKind::NotNull { .. }
        | AcceptedConstraintKind::Check { .. }
        | AcceptedConstraintKind::TargetedRule { .. } => Some(constraint.name()),
    };
    name.map(str::to_string)
        .ok_or(SchemaMigrationPlanningError::IdentityConflict)
}

fn rebuild_metadata_constraints(
    binding: &ResolvedTransition<'_>,
    store: &WorkingStore<'_>,
    target: &PersistedSchemaSnapshot,
) -> Result<AcceptedConstraintCatalog, SchemaMigrationPlanningError> {
    let mut current_row_names = BTreeMap::new();
    for constraint in binding.entity.constraints() {
        let id = store
            .bindings
            .constraint(binding.entity_tag, constraint.source_key())
            .ok_or(SchemaMigrationPlanningError::UnknownToObject)?;
        current_row_names.insert(id, constraint.name().as_str().to_string());
    }
    let constraints = target
        .constraints()
        .iter()
        .map(|constraint| {
            let name = match constraint.kind() {
                AcceptedConstraintKind::Check { .. }
                | AcceptedConstraintKind::TargetedRule { .. }
                    if constraint.origin() == ConstraintOrigin::Generated =>
                {
                    current_row_names
                        .get(&constraint.id())
                        .cloned()
                        .unwrap_or_else(|| constraint.name().to_string())
                }
                _ => resolve_constraint_name(constraint, target)?,
            };
            Ok(constraint.clone_with_name(name))
        })
        .collect::<Result<Vec<AcceptedConstraintSnapshot>, SchemaMigrationPlanningError>>()?;
    Ok(AcceptedConstraintCatalog::from_persisted_parts(
        target.constraint_catalog().allocator(),
        constraints,
        Vec::new(),
    ))
}

fn compile_migration_constraint_kind(
    binding: &ResolvedTransition<'_>,
    store: &WorkingStore<'_>,
    target: &PersistedSchemaSnapshot,
    proposed: &icydb_schema::ConstraintFragment,
) -> Result<AcceptedConstraintKind, SchemaMigrationPlanningError> {
    match proposed.kind() {
        ConstraintFragmentKind::Check(expression) => bind_source_check_expr(
            expression,
            binding.entity_tag,
            &store.bindings,
            target,
            &store.enums,
            &store.composites,
        )
        .map(|expression| AcceptedConstraintKind::Check {
            expression: Box::new(expression),
        })
        .map_err(|_| SchemaMigrationPlanningError::UnsupportedTransform),
        ConstraintFragmentKind::TargetedRule(rule) => {
            super::application_lowering::bind_targeted_rule(
                rule,
                binding.entity_tag,
                &store.bindings,
                target,
                &store.enums,
                &store.composites,
            )
            .map(|(target, operation)| AcceptedConstraintKind::TargetedRule {
                target,
                operation: Box::new(operation),
            })
            .map_err(|_| SchemaMigrationPlanningError::UnsupportedTransform)
        }
    }
}

fn reconcile_rekeyed_view(
    proposal: &SchemaProposal,
    stores: &BTreeMap<&'static str, WorkingStore<'_>>,
) -> Result<
    (
        Vec<CandidateSchemaRevision>,
        Vec<PlannedEntitySourceLineage>,
    ),
    SchemaMigrationPlanningError,
> {
    let bundles = stores
        .values()
        .map(WorkingStore::exact_bundle)
        .collect::<Result<Vec<_>, _>>()?;
    let exact_stores = stores
        .values()
        .zip(&bundles)
        .map(|(store, bundle)| ExistingProposalStore {
            path: store.path,
            identity: store.identity,
            bundle,
        })
        .collect::<Vec<_>>();
    let candidates = lower_existing_schema_proposal(proposal, &exact_stores).map_err(|error| {
        SchemaMigrationPlanningError::candidate_failure(
            error,
            SchemaMigrationPlanningError::UnexplainedSchemaDifference,
        )
    })?;
    // Use the rekeyed accepted view so renamed entities retain their tag and
    // lineage, even when an addition reuses the predecessor's source name.
    let current = bundles.into_iter().map(Some).collect::<Vec<_>>();
    let lineage = plan_new_entity_source_lineage(proposal, &current, &candidates)?;
    Ok((candidates, lineage))
}

#[cfg(test)]
mod tests;
