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
        PersistedIndexSnapshot, PersistedRelationEdgeSnapshot, PersistedSchemaSnapshot, RelationId,
        SchemaFieldSlot, SchemaIndexId, SchemaRowLayout, SchemaVersion, bind_source_check_expr,
        lower_existing_schema_proposal, lower_migration_field, lower_migration_index,
        lower_migration_nested_leaves, lower_new_migration_index,
        migration_lineage::{
            AcceptedEntitySourceLineageCatalog, AcceptedEntitySourceLineageState,
            AcceptedEntitySourceVersion,
        },
        migration_transform::{CompiledMigrationEntityProgram, compile_migration_programs},
    },
    types::EntityTag,
};

/// Typed planning failures before public endpoint error projection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMigrationPlanningError {
    Unadopted,
    MissingMigration,
    VersionGap,
    Downgrade,
    EmptyEntityVersionBump,
    StaleAcceptedHead,
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

/// One lineage value to publish with a future accepted candidate head.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct PlannedEntitySourceLineage {
    store: TargetStoreIdentity,
    entity: EntityTag,
    version: AcceptedEntitySourceVersion,
    digest: EntitySourceDigest,
}

impl PlannedEntitySourceLineage {
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
            .map_err(|_| SchemaMigrationPlanningError::CandidateMismatch)
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
        .map_err(|_| SchemaMigrationPlanningError::UnexplainedSchemaDifference)?
        .is_empty()
    {
        return Err(SchemaMigrationPlanningError::UnexplainedSchemaDifference);
    }
    let mut planned = Vec::with_capacity(entities.len());
    for (source, entity) in entities {
        let (store, entity_tag) = resolve_entity(stores, source)
            .ok_or(SchemaMigrationPlanningError::UnknownFromObject)?;
        if let Some(lineage) = current_lineage.get(store.identity, entity_tag) {
            if lineage.accepted_head() != proposal.expected_head() {
                return Err(SchemaMigrationPlanningError::StaleAcceptedHead);
            }
            if !matches!(lineage.state(), AcceptedEntitySourceLineageState::Unadopted) {
                return Err(SchemaMigrationPlanningError::IdentityConflict);
            }
        }
        planned.push(PlannedEntitySourceLineage {
            store: store.identity,
            entity: entity_tag,
            version: AcceptedEntitySourceVersion::try_new(entity.version().get())
                .map_err(|_| SchemaMigrationPlanningError::CorruptLineage)?,
            digest: proposal
                .entity_source_digest(source)
                .map_err(|_| SchemaMigrationPlanningError::CandidateMismatch)?,
        });
    }
    planned.sort_unstable_by_key(|entry| (entry.store, entry.entity));
    Ok(planned)
}

/// Derive current-version lineage for a fresh initial candidate without
/// writing the database-control record.
pub(in crate::db::schema) fn plan_initial_entity_source_lineage(
    proposal: &SchemaProposal,
    candidates: &[CandidateSchemaRevision],
) -> Result<Vec<PlannedEntitySourceLineage>, SchemaMigrationPlanningError> {
    let entities = proposal_entities(proposal);
    let assignments = proposal_assignments(proposal);
    let mut planned = Vec::with_capacity(entities.len());
    for (source, entity) in entities {
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
        planned.push(PlannedEntitySourceLineage {
            store: store_identity,
            entity: entity_tag,
            version: AcceptedEntitySourceVersion::try_new(entity.version().get())
                .map_err(|_| SchemaMigrationPlanningError::CorruptLineage)?,
            digest: proposal
                .entity_source_digest(source)
                .map_err(|_| SchemaMigrationPlanningError::CandidateMismatch)?,
        });
    }
    planned.sort_unstable_by_key(|entry| (entry.store, entry.entity));
    Ok(planned)
}

/// Bind one coordinated current plan to accepted IDs and derive exact
/// metadata candidates. No durable state changes in this function.
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
        .any(|transition| !transition.transforms().is_empty());
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
    rebuild_transitioned_snapshots(proposal, &resolved, &mut working)?;
    // The ordinary lowerer is the remaining-change authority. Its returned
    // candidate may deliberately contain staged activation state, which is
    // already the exact accepted plan and must not be lowered a second time.
    let reconciled = reconcile_rekeyed_view(proposal, &working)?;
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
    let mut lineage = resolved
        .iter()
        .map(|binding| {
            Ok(PlannedEntitySourceLineage {
                store: binding.store_identity,
                entity: binding.entity_tag,
                version: AcceptedEntitySourceVersion::try_new(binding.entity.version().get())
                    .map_err(|_| SchemaMigrationPlanningError::CorruptLineage)?,
                digest: proposal
                    .entity_source_digest(binding.entity.source_key())
                    .map_err(|_| SchemaMigrationPlanningError::CandidateMismatch)?,
            })
        })
        .collect::<Result<Vec<_>, SchemaMigrationPlanningError>>()?;
    lineage.sort_unstable_by_key(|entry| (entry.store, entry.entity));
    Ok(PlannedSchemaMigration {
        candidates,
        lineage,
        physical,
        programs,
    })
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
        if lineage.accepted_head() != proposal.expected_head() {
            return Err(SchemaMigrationPlanningError::StaleAcceptedHead);
        }
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
        let (store, entity_tag) =
            resolve_entity(stores, source).ok_or(SchemaMigrationPlanningError::MissingMigration)?;
        let lineage = current_lineage
            .get(store.identity, entity_tag)
            .ok_or(SchemaMigrationPlanningError::Unadopted)?;
        if lineage.accepted_head() != proposal.expected_head() {
            return Err(SchemaMigrationPlanningError::StaleAcceptedHead);
        }
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

fn inverse_field_source(transition: &EntityMigration, current: &FieldSourceKey) -> FieldSourceKey {
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
    _proposal: &SchemaProposal,
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
        reserve_new_migration_relations(binding, store, &with_indexes)?;
        store.snapshots.insert(binding.entity_tag, with_indexes);
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
    let physical = !transform_targets.is_empty();
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
                    lower_migration_field(
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
                    let renamed = fields[position].clone_with_migration_metadata(
                        proposed.name().as_str().to_string(),
                        nested_leaves,
                    );
                    if physical {
                        renamed.clone_for_full_layout_rewrite(id, renamed.slot())
                    } else {
                        renamed
                    }
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
                    lower_migration_field(
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
    let physical = !binding.transition.transforms().is_empty();
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
                            store.before.revision(),
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
            store.before.revision(),
            &store.enums,
            &store.composites,
            binding.entity_tag,
            provisional,
            &store.bindings,
        )
        .map_err(|_| SchemaMigrationPlanningError::UnexplainedSchemaDifference)?;
        let expected = relabel_index_for_fields(accepted, before, provisional)?;
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
    indexes.sort_unstable_by_key(PersistedIndexSnapshot::ordinal);
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
            store.before.revision(),
            &store.enums,
            &store.composites,
            binding.entity_tag,
            provisional,
            &store.bindings,
        )
        .map_err(|_| SchemaMigrationPlanningError::UnexplainedSchemaDifference)?;
        let expected = relabel_index_for_fields(accepted, before, provisional)?;
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
) -> Result<PersistedIndexSnapshot, SchemaMigrationPlanningError> {
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
            relabeled = relabeled
                .clone_with_renamed_field_path_root(
                    old_field.id(),
                    old_field.name(),
                    new_field.name(),
                )
                .ok_or(SchemaMigrationPlanningError::UnexplainedSchemaDifference)?;
        }
    }
    Ok(relabeled)
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
        && accepted.predicate_sql() == target.predicate_sql()
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
    let physical = !binding.transition.transforms().is_empty();
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
            let local_fields = proposed
                .local_fields()
                .iter()
                .map(|source| {
                    store
                        .bindings
                        .field(binding.entity_tag, source)
                        .ok_or(SchemaMigrationPlanningError::UnknownToObject)
                })
                .collect::<Result<Vec<_>, _>>()?;
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
                    if local_fields != accepted.local_field_ids() {
                        return Err(SchemaMigrationPlanningError::KindMismatch);
                    }
                    accepted.clone_with_metadata(
                        proposed.name().as_str().to_string(),
                        target.entity_path().to_string(),
                    )
                }
                None if physical => PersistedRelationEdgeSnapshot::new(
                    id,
                    proposed.name().as_str().to_string(),
                    target.entity_path().to_string(),
                    local_fields,
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
    relations.sort_unstable_by_key(PersistedRelationEdgeSnapshot::id);
    Ok(relations)
}

fn reserve_new_migration_relations(
    binding: &ResolvedTransition<'_>,
    store: &mut WorkingStore<'_>,
    snapshot: &PersistedSchemaSnapshot,
) -> Result<(), SchemaMigrationPlanningError> {
    if binding.transition.transforms().is_empty() {
        return Ok(());
    }
    let mut next_relation_id = snapshot
        .relations()
        .iter()
        .map(|relation| relation.id().get())
        .max()
        .unwrap_or(0);
    for proposed in binding.entity.relations() {
        if store
            .bindings
            .relation(binding.entity_tag, proposed.source_key())
            .is_some()
        {
            continue;
        }
        next_relation_id = next_relation_id
            .checked_add(1)
            .ok_or(SchemaMigrationPlanningError::UnsupportedTransform)?;
        let id = RelationId::new(next_relation_id)
            .ok_or(SchemaMigrationPlanningError::UnsupportedTransform)?;
        store
            .bindings
            .insert_migration_relation(binding.entity_tag, proposed.source_key().clone(), id)
            .map_err(|_| SchemaMigrationPlanningError::IdentityConflict)?;
    }
    Ok(())
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

#[expect(
    clippy::too_many_lines,
    reason = "one pass keeps physical constraint replacement and reservation deterministic"
)]
fn rebuild_constraints(
    binding: &ResolvedTransition<'_>,
    store: &mut WorkingStore<'_>,
    target: &PersistedSchemaSnapshot,
) -> Result<AcceptedConstraintCatalog, SchemaMigrationPlanningError> {
    if binding.transition.transforms().is_empty() {
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
            let name = match constraint.kind() {
                AcceptedConstraintKind::Unique { index_id } => target
                    .indexes()
                    .iter()
                    .find(|index| index.schema_id() == *index_id)
                    .map(|index| index.name().to_string()),
                AcceptedConstraintKind::Relation { relation_id } => target
                    .relations()
                    .iter()
                    .find(|relation| relation.id() == *relation_id)
                    .map(|relation| relation.name().to_string()),
                AcceptedConstraintKind::PrimaryKey
                | AcceptedConstraintKind::NotNull { .. }
                | AcceptedConstraintKind::Check { .. }
                | AcceptedConstraintKind::TargetedRule { .. } => {
                    Some(constraint.name().to_string())
                }
            }
            .ok_or(SchemaMigrationPlanningError::IdentityConflict);
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
                AcceptedConstraintKind::Unique { index_id } => target
                    .indexes()
                    .iter()
                    .find(|index| index.schema_id() == *index_id)
                    .map(|index| index.name().to_string()),
                AcceptedConstraintKind::Relation { relation_id } => target
                    .relations()
                    .iter()
                    .find(|relation| relation.id() == *relation_id)
                    .map(|relation| relation.name().to_string()),
                AcceptedConstraintKind::Check { .. }
                | AcceptedConstraintKind::TargetedRule { .. }
                    if constraint.origin() == ConstraintOrigin::Generated =>
                {
                    Some(
                        current_row_names
                            .get(&constraint.id())
                            .cloned()
                            .unwrap_or_else(|| constraint.name().to_string()),
                    )
                }
                AcceptedConstraintKind::PrimaryKey
                | AcceptedConstraintKind::NotNull { .. }
                | AcceptedConstraintKind::Check { .. }
                | AcceptedConstraintKind::TargetedRule { .. } => {
                    Some(constraint.name().to_string())
                }
            }
            .ok_or(SchemaMigrationPlanningError::IdentityConflict)?;
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
) -> Result<Vec<CandidateSchemaRevision>, SchemaMigrationPlanningError> {
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
    lower_existing_schema_proposal(proposal, &exact_stores)
        .map_err(|_| SchemaMigrationPlanningError::UnexplainedSchemaDifference)
}

#[cfg(test)]
mod tests {
    use super::*;
    use icydb_schema::{
        ConstraintFragment, DeclaredEntityVersion, EntityStoreAssignment, EnumTypeFragment,
        EnumVariantFragment, ExpectedAcceptedHead, ExpectedSchemaFingerprint, FieldFragment,
        FieldInsertPolicy, FieldManagementPolicy, FieldType, IndexFragment, IndexKeyFragment,
        IndexSourceKey, NamedTypeFragment, RecordFieldFragment, RecordTypeFragment,
        RelationDeleteAction, RelationFragment, RelationSourceKey, RuleSourceKey, ScalarLiteral,
        ScalarType, SchemaCapability, SchemaFragment, SchemaMigrationRename,
        SchemaMigrationTransform, SchemaName, SchemaSubmissionKey, SourceCheckExpr,
        SourceCheckInstruction, SourceRuleOperation, TargetDatabaseIdentity, TargetedRuleFragment,
    };

    use crate::db::schema::{
        lower_initial_schema_proposal, migration_lineage::AcceptedEntitySourceLineage,
    };

    fn field(value: &str) -> FieldSourceKey {
        FieldSourceKey::try_new(value).expect("field should admit")
    }

    fn r#type(value: &str) -> TypeSourceKey {
        TypeSourceKey::try_new(value).expect("type should admit")
    }

    fn name(value: &str) -> SchemaName {
        SchemaName::try_new(value).expect("name should admit")
    }

    fn proposal(
        version: u32,
        entity_name: &str,
        field_name: &str,
        expected_head: ExpectedAcceptedHead,
        migration: Option<SchemaMigrationPlan>,
    ) -> SchemaProposal {
        let entity_source = EntitySourceKey::try_new(entity_name).expect("entity should admit");
        let entity = EntityFragment::try_new(
            SchemaName::try_new(entity_name).expect("entity name should admit"),
            DeclaredEntityVersion::try_new(version).expect("version should admit"),
            vec![FieldFragment::new(
                SchemaName::try_new(field_name).expect("field name should admit"),
                FieldType::Scalar(icydb_schema::ScalarType::Nat64),
                false,
                FieldInsertPolicy::Required,
                None,
            )],
            vec![field(field_name)],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .expect("entity should admit");
        SchemaProposal::try_compose(
            migration
                .is_some()
                .then_some(SchemaCapability::VERSIONED_MIGRATIONS)
                .into_iter()
                .collect(),
            TargetDatabaseIdentity::from_bytes([1; 32]),
            SchemaSubmissionKey::try_new(format!("{entity_name}-{version}"))
                .expect("submission should admit"),
            expected_head,
            vec![SchemaFragment::try_new(vec![entity], Vec::new()).expect("fragment should admit")],
            vec![EntityStoreAssignment::new(
                entity_source,
                TargetStoreIdentity::from_bytes([2; 32]),
            )],
            Vec::new(),
            migration,
        )
        .expect("proposal should admit")
    }

    fn head() -> ExpectedAcceptedHead {
        ExpectedAcceptedHead::Exact {
            revision: 1,
            fingerprint: ExpectedSchemaFingerprint::from_bytes([3; 32]),
        }
    }

    fn accepted_fixture() -> (SchemaProposal, CandidateSchemaRevision) {
        let initial = proposal(1, "User", "email", ExpectedAcceptedHead::Empty, None);
        let candidate = lower_initial_schema_proposal(
            &initial,
            &[crate::db::schema::ProposalStoreTarget {
                path: "test::Store",
                identity: TargetStoreIdentity::from_bytes([2; 32]),
            }],
        )
        .expect("initial proposal should lower")
        .pop()
        .expect("initial candidate should exist");
        (initial, candidate)
    }

    fn checked_cast_proposal(
        current: bool,
        expected_head: ExpectedAcceptedHead,
        migration: Option<SchemaMigrationPlan>,
    ) -> SchemaProposal {
        let entity_source = EntitySourceKey::try_new("User").expect("entity should admit");
        let value_name = if current { "value" } else { "old_value" };
        let value_kind = if current {
            ScalarType::Nat8
        } else {
            ScalarType::Int64
        };
        let entity = EntityFragment::try_new(
            name("User"),
            DeclaredEntityVersion::try_new(if current { 2 } else { 1 })
                .expect("version should admit"),
            vec![
                FieldFragment::new(
                    name("id"),
                    FieldType::Scalar(ScalarType::Nat64),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
                FieldFragment::new(
                    name(value_name),
                    FieldType::Scalar(value_kind),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
            ],
            vec![field("id")],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .expect("entity should admit");
        SchemaProposal::try_compose(
            migration
                .is_some()
                .then_some(SchemaCapability::VERSIONED_MIGRATIONS)
                .into_iter()
                .collect(),
            TargetDatabaseIdentity::from_bytes([1; 32]),
            SchemaSubmissionKey::try_new(if current { "cast-v2" } else { "cast-v1" })
                .expect("submission should admit"),
            expected_head,
            vec![SchemaFragment::try_new(vec![entity], Vec::new()).expect("fragment should admit")],
            vec![EntityStoreAssignment::new(
                entity_source.clone(),
                TargetStoreIdentity::from_bytes([2; 32]),
            )],
            current
                .then(|| icydb_schema::SchemaRemoval::Field {
                    entity: entity_source,
                    field: field("old_value"),
                })
                .into_iter()
                .collect(),
            migration,
        )
        .expect("proposal should admit")
    }

    fn maintained_user_proposal(
        current: bool,
        expected_head: ExpectedAcceptedHead,
        migration: Option<SchemaMigrationPlan>,
    ) -> SchemaProposal {
        let entity_source = EntitySourceKey::try_new("SqlTestUser").expect("entity should admit");
        let control_source = EntitySourceKey::try_new("Control").expect("entity should admit");
        SchemaProposal::try_compose(
            migration
                .is_some()
                .then_some(SchemaCapability::VERSIONED_MIGRATIONS)
                .into_iter()
                .collect(),
            TargetDatabaseIdentity::from_bytes([1; 32]),
            SchemaSubmissionKey::try_new(if current { "user-v2" } else { "user-v1" })
                .expect("submission should admit"),
            expected_head,
            vec![
                SchemaFragment::try_new(
                    vec![maintained_user_entity(current), maintained_control_entity()],
                    Vec::new(),
                )
                .expect("fragment should admit"),
            ],
            vec![
                EntityStoreAssignment::new(entity_source, TargetStoreIdentity::from_bytes([2; 32])),
                EntityStoreAssignment::new(
                    control_source,
                    TargetStoreIdentity::from_bytes([2; 32]),
                ),
            ],
            Vec::new(),
            migration,
        )
        .expect("proposal should admit")
    }

    fn maintained_user_entity(current: bool) -> EntityFragment {
        let scalar = |field_name, kind, insert_policy, management| {
            FieldFragment::new(
                name(field_name),
                FieldType::Scalar(kind),
                false,
                insert_policy,
                management,
            )
        };
        EntityFragment::try_new(
            name("SqlTestUser"),
            DeclaredEntityVersion::try_new(if current { 2 } else { 1 })
                .expect("version should admit"),
            vec![
                scalar("id", ScalarType::Ulid, FieldInsertPolicy::Generated, None),
                scalar(
                    "name",
                    ScalarType::Text { max_len: None },
                    FieldInsertPolicy::Required,
                    None,
                ),
                scalar(
                    "age",
                    if current {
                        ScalarType::Nat16
                    } else {
                        ScalarType::Int32
                    },
                    FieldInsertPolicy::Required,
                    None,
                ),
                scalar(
                    if current { "score" } else { "rank" },
                    ScalarType::Int32,
                    FieldInsertPolicy::Required,
                    None,
                ),
                scalar(
                    "created_at",
                    ScalarType::Timestamp,
                    FieldInsertPolicy::Required,
                    Some(FieldManagementPolicy::CreatedAt),
                ),
                scalar(
                    "updated_at",
                    ScalarType::Timestamp,
                    FieldInsertPolicy::Required,
                    Some(FieldManagementPolicy::UpdatedAt),
                ),
            ],
            vec![field("id")],
            vec![
                IndexFragment::try_new(
                    name("idx_sql_test_user__name"),
                    vec![IndexKeyFragment::Field(field("name"))],
                    false,
                    None,
                )
                .expect("index should admit"),
            ],
            Vec::new(),
            Vec::new(),
        )
        .expect("entity should admit")
    }

    fn maintained_control_entity() -> EntityFragment {
        EntityFragment::try_new(
            name("Control"),
            DeclaredEntityVersion::try_new(1).expect("version should admit"),
            vec![FieldFragment::new(
                name("id"),
                FieldType::Scalar(ScalarType::Nat64),
                false,
                FieldInsertPolicy::Generated,
                None,
            )],
            vec![field("id")],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .expect("control should admit")
    }

    fn closed_transform_proposal(
        current: bool,
        expected_head: ExpectedAcceptedHead,
        migration: Option<SchemaMigrationPlan>,
    ) -> SchemaProposal {
        let field_fragment = |name_value: &str, kind, nullable| {
            FieldFragment::new(
                name(name_value),
                FieldType::Scalar(kind),
                nullable,
                FieldInsertPolicy::Required,
                None,
            )
        };
        let mut fields = vec![
            field_fragment("id", ScalarType::Nat64, false),
            field_fragment("signed", ScalarType::Int64, false),
            field_fragment("optional", ScalarType::Int64, true),
        ];
        if current {
            fields.extend([
                field_fragment("filled", ScalarType::Nat64, false),
                field_fragment("copied", ScalarType::Int64, false),
                field_fragment("cast", ScalarType::Nat8, false),
                field_fragment("coalesced", ScalarType::Int64, false),
            ]);
        }
        let entity_source = EntitySourceKey::try_new("User").expect("entity should admit");
        let entity = EntityFragment::try_new(
            name("User"),
            DeclaredEntityVersion::try_new(if current { 2 } else { 1 })
                .expect("version should admit"),
            fields,
            vec![field("id")],
            current
                .then(|| {
                    IndexFragment::try_new(
                        name("cast_unique"),
                        vec![IndexKeyFragment::Field(field("cast"))],
                        true,
                        None,
                    )
                    .expect("index should admit")
                })
                .into_iter()
                .collect(),
            Vec::new(),
            Vec::new(),
        )
        .expect("entity should admit");
        SchemaProposal::try_compose(
            migration
                .is_some()
                .then_some(SchemaCapability::VERSIONED_MIGRATIONS)
                .into_iter()
                .collect(),
            TargetDatabaseIdentity::from_bytes([1; 32]),
            SchemaSubmissionKey::try_new(if current { "closed-v2" } else { "closed-v1" })
                .expect("submission should admit"),
            expected_head,
            vec![SchemaFragment::try_new(vec![entity], Vec::new()).expect("fragment should admit")],
            vec![EntityStoreAssignment::new(
                entity_source,
                TargetStoreIdentity::from_bytes([2; 32]),
            )],
            Vec::new(),
            migration,
        )
        .expect("proposal should admit")
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the complete rename fixture keeps every accepted object kind visible"
    )]
    fn complete_rename_proposal(
        current: bool,
        expected_head: ExpectedAcceptedHead,
        migration: Option<SchemaMigrationPlan>,
    ) -> SchemaProposal {
        let entity_name = if current { "Entry" } else { "Item" };
        let parent_field = if current { "owner_id" } else { "parent_id" };
        let status_type = if current { "Status" } else { "OldStatus" };
        let status_variant = if current { "Queued" } else { "Pending" };
        let profile_type = if current { "Profile" } else { "OldProfile" };
        let profile_member = if current { "label" } else { "nickname" };
        let score_type = if current { "Score" } else { "OldScore" };
        let relation_name = if current {
            "owner_relation"
        } else {
            "parent_relation"
        };
        let check_name = if current {
            "nonnegative_owner"
        } else {
            "nonnegative_parent"
        };
        let rule_name = if current { "bounded" } else { "range" };
        let check = SourceCheckExpr::try_new(vec![
            SourceCheckInstruction::Field(field(parent_field)),
            SourceCheckInstruction::Literal(ScalarLiteral::Nat(0)),
            SourceCheckInstruction::GreaterThanOrEqual,
        ])
        .expect("check should admit");
        let entity = EntityFragment::try_new(
            name(entity_name),
            DeclaredEntityVersion::try_new(if current { 2 } else { 1 })
                .expect("version should admit"),
            vec![
                FieldFragment::new(
                    name("id"),
                    FieldType::Scalar(ScalarType::Nat64),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
                FieldFragment::new(
                    name(parent_field),
                    FieldType::Scalar(ScalarType::Nat64),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
                FieldFragment::new(
                    name("status"),
                    FieldType::Named(r#type(status_type)),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
                FieldFragment::new(
                    name("profile"),
                    FieldType::Named(r#type(profile_type)),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
                FieldFragment::new(
                    name("score"),
                    FieldType::Named(r#type(score_type)),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
            ],
            vec![field("id")],
            vec![
                IndexFragment::try_new(
                    name("parent_lookup"),
                    vec![IndexKeyFragment::Field(field(parent_field))],
                    false,
                    None,
                )
                .expect("index should admit"),
            ],
            vec![
                RelationFragment::try_new(
                    name(relation_name),
                    vec![field(parent_field)],
                    EntitySourceKey::try_new(entity_name).expect("entity should admit"),
                    vec![field("id")],
                    RelationDeleteAction::Restrict,
                )
                .expect("relation should admit"),
            ],
            vec![
                ConstraintFragment::check(name(check_name), check),
                ConstraintFragment::targeted_rule(TargetedRuleFragment::new(
                    field("score"),
                    r#type(score_type),
                    name(rule_name),
                    SourceRuleOperation::NumericRangeInclusive {
                        min: ScalarLiteral::Nat(0),
                        max: ScalarLiteral::Nat(if current { 8 } else { 10 }),
                    },
                )),
            ],
        )
        .expect("entity should admit");
        let fragment = SchemaFragment::try_new(
            vec![entity],
            vec![
                NamedTypeFragment::Enum(
                    EnumTypeFragment::try_new(
                        name(status_type),
                        vec![EnumVariantFragment::new(name(status_variant))],
                    )
                    .expect("enum should admit"),
                ),
                NamedTypeFragment::Record(
                    RecordTypeFragment::try_new(
                        name(profile_type),
                        vec![RecordFieldFragment::new(
                            name(profile_member),
                            FieldType::Scalar(ScalarType::Text { max_len: Some(64) }),
                            false,
                        )],
                    )
                    .expect("record should admit"),
                ),
                NamedTypeFragment::newtype(name(score_type), FieldType::Scalar(ScalarType::Nat8)),
            ],
        )
        .expect("fragment should admit");
        let mut capabilities = vec![
            SchemaCapability::ACCEPTED_CHECKS,
            SchemaCapability::EXACT_COMPOSITE_TYPES,
            SchemaCapability::RESTRICTIVE_RELATIONS,
            SchemaCapability::SECONDARY_INDEXES,
        ];
        if migration.is_some() {
            capabilities.push(SchemaCapability::VERSIONED_MIGRATIONS);
        }
        SchemaProposal::try_compose(
            capabilities,
            TargetDatabaseIdentity::from_bytes([1; 32]),
            SchemaSubmissionKey::try_new(if current {
                "complete-v2"
            } else {
                "complete-v1"
            })
            .expect("submission should admit"),
            expected_head,
            vec![fragment],
            vec![EntityStoreAssignment::new(
                EntitySourceKey::try_new(entity_name).expect("entity should admit"),
                TargetStoreIdentity::from_bytes([2; 32]),
            )],
            Vec::new(),
            migration,
        )
        .expect("proposal should admit")
    }

    fn complete_rename_plan() -> SchemaMigrationPlan {
        SchemaMigrationPlan::try_new(vec![
            EntityMigration::try_new(
                EntitySourceKey::try_new("Entry").expect("entity should admit"),
                DeclaredEntityVersion::try_new(1).expect("version should admit"),
                Some(EntitySourceKey::try_new("Item").expect("entity should admit")),
                vec![
                    SchemaMigrationRename::Field {
                        from: field("parent_id"),
                        to: field("owner_id"),
                    },
                    SchemaMigrationRename::NamedType {
                        from: r#type("OldStatus"),
                        to: r#type("Status"),
                    },
                    SchemaMigrationRename::NamedType {
                        from: r#type("OldProfile"),
                        to: r#type("Profile"),
                    },
                    SchemaMigrationRename::NamedType {
                        from: r#type("OldScore"),
                        to: r#type("Score"),
                    },
                    SchemaMigrationRename::EnumVariant {
                        named_type: r#type("OldStatus"),
                        from: r#type("Pending"),
                        to: r#type("Queued"),
                    },
                    SchemaMigrationRename::RecordField {
                        named_type: r#type("OldProfile"),
                        from: field("nickname"),
                        to: field("label"),
                    },
                    SchemaMigrationRename::Relation {
                        from: RelationSourceKey::try_new("parent_relation")
                            .expect("relation should admit"),
                        to: RelationSourceKey::try_new("owner_relation")
                            .expect("relation should admit"),
                    },
                    SchemaMigrationRename::Constraint {
                        from: ConstraintSourceKey::try_new("nonnegative_parent")
                            .expect("constraint should admit"),
                        to: ConstraintSourceKey::try_new("nonnegative_owner")
                            .expect("constraint should admit"),
                    },
                    SchemaMigrationRename::Rule {
                        named_type: r#type("OldScore"),
                        from: RuleSourceKey::try_new("range").expect("rule should admit"),
                        to: RuleSourceKey::try_new("bounded").expect("rule should admit"),
                    },
                ],
                Vec::new(),
            )
            .expect("transition should admit"),
        ])
        .expect("plan should admit")
    }

    fn shared_type_proposal(
        current: bool,
        expected_head: ExpectedAcceptedHead,
        migration: Option<SchemaMigrationPlan>,
    ) -> SchemaProposal {
        let type_name = if current { "Value" } else { "OldValue" };
        let version = DeclaredEntityVersion::try_new(if current { 2 } else { 1 })
            .expect("version should admit");
        let entities = ["Alpha", "Beta"]
            .into_iter()
            .map(|entity_name| {
                EntityFragment::try_new(
                    name(entity_name),
                    version,
                    vec![
                        FieldFragment::new(
                            name("id"),
                            FieldType::Scalar(ScalarType::Nat64),
                            false,
                            FieldInsertPolicy::Required,
                            None,
                        ),
                        FieldFragment::new(
                            name("value"),
                            FieldType::Named(r#type(type_name)),
                            false,
                            FieldInsertPolicy::Required,
                            None,
                        ),
                    ],
                    vec![field("id")],
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                )
                .expect("entity should admit")
            })
            .collect::<Vec<_>>();
        let fragment = SchemaFragment::try_new(
            entities,
            vec![NamedTypeFragment::newtype(
                name(type_name),
                FieldType::Scalar(ScalarType::Nat16),
            )],
        )
        .expect("fragment should admit");
        let mut capabilities = vec![SchemaCapability::EXACT_COMPOSITE_TYPES];
        if migration.is_some() {
            capabilities.push(SchemaCapability::VERSIONED_MIGRATIONS);
        }
        SchemaProposal::try_compose(
            capabilities,
            TargetDatabaseIdentity::from_bytes([1; 32]),
            SchemaSubmissionKey::try_new(if current { "shared-v2" } else { "shared-v1" })
                .expect("submission should admit"),
            expected_head,
            vec![fragment],
            vec![
                EntityStoreAssignment::new(
                    EntitySourceKey::try_new("Alpha").expect("entity should admit"),
                    TargetStoreIdentity::from_bytes([2; 32]),
                ),
                EntityStoreAssignment::new(
                    EntitySourceKey::try_new("Beta").expect("entity should admit"),
                    TargetStoreIdentity::from_bytes([5; 32]),
                ),
            ],
            Vec::new(),
            migration,
        )
        .expect("proposal should admit")
    }

    fn shared_type_plan() -> SchemaMigrationPlan {
        SchemaMigrationPlan::try_new(
            ["Alpha", "Beta"]
                .into_iter()
                .map(|entity_name| {
                    EntityMigration::try_new(
                        EntitySourceKey::try_new(entity_name).expect("entity should admit"),
                        DeclaredEntityVersion::try_new(1).expect("version should admit"),
                        None,
                        vec![SchemaMigrationRename::NamedType {
                            from: r#type("OldValue"),
                            to: r#type("Value"),
                        }],
                        Vec::new(),
                    )
                    .expect("transition should admit")
                })
                .collect(),
        )
        .expect("plan should admit")
    }

    #[test]
    fn adoption_requires_v1_and_exact_noop_reconciliation() {
        let (initial, candidate) = accepted_fixture();
        let existing = ExistingProposalStore {
            path: "test::Store",
            identity: TargetStoreIdentity::from_bytes([2; 32]),
            bundle: candidate.bundle(),
        };
        let adopted = plan_entity_source_adoption(
            &proposal(1, "User", "email", head(), None),
            std::slice::from_ref(&existing),
            &AcceptedEntitySourceLineageCatalog::default(),
        )
        .expect("exact v1 adoption should plan");
        assert_eq!(adopted.len(), 1);
        assert_eq!(adopted[0].version().get(), 1);
        assert_eq!(
            adopted[0].digest(),
            initial
                .entity_source_digest(&EntitySourceKey::try_new("User").expect("key should admit"))
                .expect("digest should derive"),
        );

        assert_eq!(
            plan_entity_source_adoption(
                &proposal(2, "User", "email", head(), None),
                std::slice::from_ref(&existing),
                &AcceptedEntitySourceLineageCatalog::default(),
            )
            .err(),
            Some(SchemaMigrationPlanningError::VersionGap),
        );
        assert_eq!(
            plan_entity_source_adoption(
                &proposal(1, "User", "primary_email", head(), None),
                std::slice::from_ref(&existing),
                &AcceptedEntitySourceLineageCatalog::default(),
            )
            .err(),
            Some(SchemaMigrationPlanningError::UnexplainedSchemaDifference),
        );

        let entity = candidate
            .bundle()
            .source_bindings()
            .entity(&EntitySourceKey::try_new("User").expect("key should admit"))
            .expect("entity should bind");
        let mut stale = AcceptedEntitySourceLineageCatalog::default();
        stale
            .insert(
                TargetStoreIdentity::from_bytes([2; 32]),
                entity,
                AcceptedEntitySourceLineage::unadopted(ExpectedAcceptedHead::Exact {
                    revision: 2,
                    fingerprint: ExpectedSchemaFingerprint::from_bytes([4; 32]),
                })
                .expect("lineage should admit"),
            )
            .expect("lineage should insert");
        assert_eq!(
            plan_entity_source_adoption(
                &proposal(1, "User", "email", head(), None),
                std::slice::from_ref(&existing),
                &stale,
            )
            .err(),
            Some(SchemaMigrationPlanningError::StaleAcceptedHead),
        );

        let mut already_adopted = AcceptedEntitySourceLineageCatalog::default();
        already_adopted
            .insert(
                TargetStoreIdentity::from_bytes([2; 32]),
                entity,
                AcceptedEntitySourceLineage::adopted(
                    head(),
                    AcceptedEntitySourceVersion::try_new(1).expect("version should admit"),
                    initial
                        .entity_source_digest(
                            &EntitySourceKey::try_new("User").expect("entity should admit"),
                        )
                        .expect("digest should derive"),
                )
                .expect("lineage should admit"),
            )
            .expect("lineage should insert");
        assert_eq!(
            plan_entity_source_adoption(
                &proposal(1, "User", "email", head(), None),
                std::slice::from_ref(&existing),
                &already_adopted,
            )
            .err(),
            Some(SchemaMigrationPlanningError::IdentityConflict),
        );
    }

    #[test]
    fn explicit_entity_and_field_rename_preserve_accepted_ids_in_candidate() {
        let (initial, candidate) = accepted_fixture();
        let accepted_entity = candidate
            .bundle()
            .source_bindings()
            .entity(&EntitySourceKey::try_new("User").expect("key should admit"))
            .expect("entity should bind");
        let accepted_field = candidate
            .bundle()
            .source_bindings()
            .field(accepted_entity, &field("email"))
            .expect("field should bind");
        let transition = EntityMigration::try_new(
            EntitySourceKey::try_new("Account").expect("entity should admit"),
            DeclaredEntityVersion::try_new(1).expect("version should admit"),
            Some(EntitySourceKey::try_new("User").expect("entity should admit")),
            vec![SchemaMigrationRename::Field {
                from: field("email"),
                to: field("primary_email"),
            }],
            Vec::new(),
        )
        .expect("transition should admit");
        let plan = SchemaMigrationPlan::try_new(vec![transition]).expect("plan should admit");
        let migration = proposal(2, "Account", "primary_email", head(), Some(plan));
        let mut lineage = AcceptedEntitySourceLineageCatalog::default();
        lineage
            .insert(
                TargetStoreIdentity::from_bytes([2; 32]),
                accepted_entity,
                AcceptedEntitySourceLineage::adopted(
                    head(),
                    AcceptedEntitySourceVersion::try_new(1).expect("version should admit"),
                    initial
                        .entity_source_digest(
                            &EntitySourceKey::try_new("User").expect("key should admit"),
                        )
                        .expect("digest should derive"),
                )
                .expect("lineage should admit"),
            )
            .expect("lineage should insert");
        let planned = plan_schema_migration(
            &migration,
            &[ExistingProposalStore {
                path: "test::Store",
                identity: TargetStoreIdentity::from_bytes([2; 32]),
                bundle: candidate.bundle(),
            }],
            &lineage,
        )
        .expect("rename should plan");
        let target = planned.candidates()[0].bundle();
        assert_eq!(
            target
                .source_bindings()
                .entity(&EntitySourceKey::try_new("Account").expect("key should admit")),
            Some(accepted_entity),
        );
        assert_eq!(
            target
                .source_bindings()
                .field(accepted_entity, &field("primary_email")),
            Some(accepted_field),
        );
        assert!(
            target
                .source_bindings()
                .entity(&EntitySourceKey::try_new("User").expect("key should admit"))
                .is_none(),
        );
        assert_eq!(planned.lineage()[0].version().get(), 2);
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one matrix test proves every renamed source key and accepted ID together"
    )]
    fn complete_metadata_rename_set_preserves_every_accepted_identity_without_aliases() {
        let initial = complete_rename_proposal(false, ExpectedAcceptedHead::Empty, None);
        let candidate = lower_initial_schema_proposal(
            &initial,
            &[crate::db::schema::ProposalStoreTarget {
                path: "test::Store",
                identity: TargetStoreIdentity::from_bytes([2; 32]),
            }],
        )
        .expect("initial proposal should lower")
        .pop()
        .expect("initial candidate should exist");
        let accepted = candidate.bundle().source_bindings();
        let old_entity = EntitySourceKey::try_new("Item").expect("entity should admit");
        let entity_id = accepted.entity(&old_entity).expect("entity should bind");
        let parent_id = accepted
            .field(entity_id, &field("parent_id"))
            .expect("field should bind");
        let status_id = accepted
            .named_type(&r#type("OldStatus"))
            .expect("enum should bind");
        let profile_id = accepted
            .named_type(&r#type("OldProfile"))
            .expect("record should bind");
        let score_id = accepted
            .named_type(&r#type("OldScore"))
            .expect("newtype should bind");
        let AcceptedNamedTypeIdentity::Enum(status_type_id) = status_id else {
            panic!("status should retain enum identity");
        };
        let AcceptedNamedTypeIdentity::Composite(profile_type_id) = profile_id else {
            panic!("profile should retain composite identity");
        };
        let pending_id = accepted
            .enum_variant(status_type_id, &r#type("Pending"))
            .expect("variant should bind");
        let nickname_id = accepted
            .composite_field(profile_type_id, &field("nickname"))
            .expect("record field should bind");
        let relation_id = accepted
            .relation(
                entity_id,
                &RelationSourceKey::try_new("parent_relation").expect("relation should admit"),
            )
            .expect("relation should bind");
        let check_id = accepted
            .constraint(
                entity_id,
                &ConstraintSourceKey::try_new("nonnegative_parent")
                    .expect("constraint should admit"),
            )
            .expect("constraint should bind");
        let old_rule = ConstraintSourceKey::for_targeted_field_rule(
            &field("score"),
            &r#type("OldScore"),
            &RuleSourceKey::try_new("range").expect("rule should admit"),
        );
        let rule_id = accepted
            .constraint(entity_id, &old_rule)
            .expect("rule should bind");
        let index_id = accepted
            .index(
                entity_id,
                &IndexSourceKey::try_new("parent_lookup").expect("index should admit"),
            )
            .expect("index should bind");
        let old_index = candidate.bundle().entity_snapshots()[&entity_id]
            .indexes()
            .iter()
            .find(|index| index.schema_id() == index_id)
            .expect("index should exist");

        let mut lineage = AcceptedEntitySourceLineageCatalog::default();
        lineage
            .insert(
                TargetStoreIdentity::from_bytes([2; 32]),
                entity_id,
                AcceptedEntitySourceLineage::adopted(
                    head(),
                    AcceptedEntitySourceVersion::try_new(1).expect("version should admit"),
                    initial
                        .entity_source_digest(&old_entity)
                        .expect("digest should derive"),
                )
                .expect("lineage should admit"),
            )
            .expect("lineage should insert");
        let migration = complete_rename_proposal(true, head(), Some(complete_rename_plan()));
        let planned = plan_schema_migration(
            &migration,
            &[ExistingProposalStore {
                path: "test::Store",
                identity: TargetStoreIdentity::from_bytes([2; 32]),
                bundle: candidate.bundle(),
            }],
            &lineage,
        )
        .expect("complete metadata rename should plan");
        let target = planned.candidates()[0].bundle();
        let bindings = target.source_bindings();
        let new_entity = EntitySourceKey::try_new("Entry").expect("entity should admit");
        assert_eq!(bindings.entity(&new_entity), Some(entity_id));
        assert_eq!(
            bindings.field(entity_id, &field("owner_id")),
            Some(parent_id)
        );
        assert_eq!(bindings.named_type(&r#type("Status")), Some(status_id));
        assert_eq!(bindings.named_type(&r#type("Profile")), Some(profile_id));
        assert_eq!(bindings.named_type(&r#type("Score")), Some(score_id));
        assert_eq!(
            bindings.enum_variant(status_type_id, &r#type("Queued")),
            Some(pending_id),
        );
        assert_eq!(
            bindings.composite_field(profile_type_id, &field("label")),
            Some(nickname_id),
        );
        assert_eq!(
            bindings.relation(
                entity_id,
                &RelationSourceKey::try_new("owner_relation").expect("relation should admit"),
            ),
            Some(relation_id),
        );
        assert_eq!(
            bindings.constraint(
                entity_id,
                &ConstraintSourceKey::try_new("nonnegative_owner")
                    .expect("constraint should admit"),
            ),
            Some(check_id),
        );
        let new_rule = ConstraintSourceKey::for_targeted_field_rule(
            &field("score"),
            &r#type("Score"),
            &RuleSourceKey::try_new("bounded").expect("rule should admit"),
        );
        assert_eq!(bindings.constraint(entity_id, &new_rule), Some(rule_id));
        assert!(bindings.entity(&old_entity).is_none());
        assert!(bindings.field(entity_id, &field("parent_id")).is_none());
        assert!(bindings.named_type(&r#type("OldStatus")).is_none());
        assert!(
            bindings
                .enum_variant(status_type_id, &r#type("Pending"))
                .is_none(),
        );
        assert!(
            bindings
                .composite_field(profile_type_id, &field("nickname"))
                .is_none(),
        );
        assert!(bindings.constraint(entity_id, &old_rule).is_none());
        let target_snapshot = &target.entity_snapshots()[&entity_id];
        let target_index = target_snapshot
            .indexes()
            .iter()
            .find(|index| index.schema_id() == index_id)
            .expect("index should remain");
        assert_eq!(
            target_index.physical_generation(),
            old_index.physical_generation()
        );
        assert_eq!(target_snapshot.relations()[0].id(), relation_id);
        assert_eq!(target_snapshot.relations()[0].name(), "owner_relation");
        assert_eq!(target_snapshot.relations()[0].target_path(), "Entry");
        assert!(
            target_snapshot
                .constraint_catalog()
                .activation(rule_id)
                .is_some(),
            "the ordinary accepted-rule mutation must remain classified beside the renames",
        );
    }

    #[test]
    fn shared_named_type_rename_updates_every_store_local_copy_simultaneously() {
        let initial = shared_type_proposal(false, ExpectedAcceptedHead::Empty, None);
        let mut candidates = lower_initial_schema_proposal(
            &initial,
            &[
                crate::db::schema::ProposalStoreTarget {
                    path: "test::AlphaStore",
                    identity: TargetStoreIdentity::from_bytes([2; 32]),
                },
                crate::db::schema::ProposalStoreTarget {
                    path: "test::BetaStore",
                    identity: TargetStoreIdentity::from_bytes([5; 32]),
                },
            ],
        )
        .expect("initial proposal should lower");
        candidates.sort_by(|left, right| left.store_path().cmp(right.store_path()));
        let mut lineage = AcceptedEntitySourceLineageCatalog::default();
        let mut accepted = Vec::new();
        for (candidate, entity_name, store) in [
            (
                &candidates[0],
                "Alpha",
                TargetStoreIdentity::from_bytes([2; 32]),
            ),
            (
                &candidates[1],
                "Beta",
                TargetStoreIdentity::from_bytes([5; 32]),
            ),
        ] {
            let entity_source = EntitySourceKey::try_new(entity_name).expect("entity should admit");
            let entity_id = candidate
                .bundle()
                .source_bindings()
                .entity(&entity_source)
                .expect("entity should bind");
            let type_id = candidate
                .bundle()
                .source_bindings()
                .named_type(&r#type("OldValue"))
                .expect("type should bind");
            lineage
                .insert(
                    store,
                    entity_id,
                    AcceptedEntitySourceLineage::adopted(
                        head(),
                        AcceptedEntitySourceVersion::try_new(1).expect("version should admit"),
                        initial
                            .entity_source_digest(&entity_source)
                            .expect("digest should derive"),
                    )
                    .expect("lineage should admit"),
                )
                .expect("lineage should insert");
            accepted.push((candidate.store_path(), type_id));
        }
        let migration = shared_type_proposal(true, head(), Some(shared_type_plan()));
        let stores = vec![
            ExistingProposalStore {
                path: "test::AlphaStore",
                identity: TargetStoreIdentity::from_bytes([2; 32]),
                bundle: candidates[0].bundle(),
            },
            ExistingProposalStore {
                path: "test::BetaStore",
                identity: TargetStoreIdentity::from_bytes([5; 32]),
                bundle: candidates[1].bundle(),
            },
        ];
        let planned = plan_schema_migration(&migration, &stores, &lineage)
            .expect("shared rename should plan");
        assert_eq!(planned.candidates().len(), 2);
        for candidate in planned.candidates() {
            let expected = accepted
                .iter()
                .find(|(path, _)| *path == candidate.store_path())
                .map(|(_, identity)| *identity)
                .expect("accepted type should exist");
            assert_eq!(
                candidate
                    .bundle()
                    .source_bindings()
                    .named_type(&r#type("Value")),
                Some(expected),
            );
            assert!(
                candidate
                    .bundle()
                    .source_bindings()
                    .named_type(&r#type("OldValue"))
                    .is_none(),
            );
        }
    }

    #[test]
    fn unadopted_lineage_fails_before_candidate_derivation() {
        let (_, candidate) = accepted_fixture();
        let transition = EntityMigration::try_new(
            EntitySourceKey::try_new("User").expect("entity should admit"),
            DeclaredEntityVersion::try_new(1).expect("version should admit"),
            None,
            vec![SchemaMigrationRename::Field {
                from: field("email"),
                to: field("primary_email"),
            }],
            Vec::new(),
        )
        .expect("transition should admit");
        let migration = proposal(
            2,
            "User",
            "primary_email",
            head(),
            Some(SchemaMigrationPlan::try_new(vec![transition]).expect("plan should admit")),
        );
        assert_eq!(
            plan_schema_migration(
                &migration,
                &[ExistingProposalStore {
                    path: "test::Store",
                    identity: TargetStoreIdentity::from_bytes([2; 32]),
                    bundle: candidate.bundle(),
                }],
                &AcceptedEntitySourceLineageCatalog::default(),
            )
            .err(),
            Some(SchemaMigrationPlanningError::Unadopted),
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one rejection matrix compares the four version and head classifications"
    )]
    fn stale_head_version_gap_downgrade_and_empty_bump_are_distinct() {
        let (initial, candidate) = accepted_fixture();
        let entity_source = EntitySourceKey::try_new("User").expect("entity should admit");
        let entity_id = candidate
            .bundle()
            .source_bindings()
            .entity(&entity_source)
            .expect("entity should bind");
        let accepted_digest = initial
            .entity_source_digest(&entity_source)
            .expect("digest should derive");
        let store = TargetStoreIdentity::from_bytes([2; 32]);
        let existing = || ExistingProposalStore {
            path: "test::Store",
            identity: store,
            bundle: candidate.bundle(),
        };
        let rename = |from_version| {
            SchemaMigrationPlan::try_new(vec![
                EntityMigration::try_new(
                    entity_source.clone(),
                    DeclaredEntityVersion::try_new(from_version).expect("version should admit"),
                    None,
                    vec![SchemaMigrationRename::Field {
                        from: field("email"),
                        to: field("primary_email"),
                    }],
                    Vec::new(),
                )
                .expect("transition should admit"),
            ])
            .expect("plan should admit")
        };

        let mut stale = AcceptedEntitySourceLineageCatalog::default();
        stale
            .insert(
                store,
                entity_id,
                AcceptedEntitySourceLineage::adopted(
                    ExpectedAcceptedHead::Exact {
                        revision: 2,
                        fingerprint: ExpectedSchemaFingerprint::from_bytes([4; 32]),
                    },
                    AcceptedEntitySourceVersion::try_new(1).expect("version should admit"),
                    accepted_digest,
                )
                .expect("lineage should admit"),
            )
            .expect("lineage should insert");
        assert_eq!(
            plan_schema_migration(
                &proposal(2, "User", "primary_email", head(), Some(rename(1))),
                &[existing()],
                &stale,
            )
            .err(),
            Some(SchemaMigrationPlanningError::StaleAcceptedHead),
        );

        for (accepted_version, target_version, from_version, expected) in [
            (1, 3, 2, SchemaMigrationPlanningError::VersionGap),
            (2, 2, 1, SchemaMigrationPlanningError::Downgrade),
        ] {
            let mut lineage = AcceptedEntitySourceLineageCatalog::default();
            lineage
                .insert(
                    store,
                    entity_id,
                    AcceptedEntitySourceLineage::adopted(
                        head(),
                        AcceptedEntitySourceVersion::try_new(accepted_version)
                            .expect("version should admit"),
                        accepted_digest,
                    )
                    .expect("lineage should admit"),
                )
                .expect("lineage should insert");
            assert_eq!(
                plan_schema_migration(
                    &proposal(
                        target_version,
                        "User",
                        "primary_email",
                        head(),
                        Some(rename(from_version)),
                    ),
                    &[existing()],
                    &lineage,
                )
                .err(),
                Some(expected),
            );
        }

        let empty_plan = SchemaMigrationPlan::try_new(vec![
            EntityMigration::try_new(
                entity_source.clone(),
                DeclaredEntityVersion::try_new(1).expect("version should admit"),
                None,
                vec![SchemaMigrationRename::Field {
                    from: field("obsolete_email"),
                    to: field("email"),
                }],
                Vec::new(),
            )
            .expect("transition should admit"),
        ])
        .expect("plan should admit");
        let mut lineage = AcceptedEntitySourceLineageCatalog::default();
        lineage
            .insert(
                store,
                entity_id,
                AcceptedEntitySourceLineage::adopted(
                    head(),
                    AcceptedEntitySourceVersion::try_new(1).expect("version should admit"),
                    accepted_digest,
                )
                .expect("lineage should admit"),
            )
            .expect("lineage should insert");
        assert_eq!(
            plan_schema_migration(
                &proposal(2, "User", "email", head(), Some(empty_plan)),
                &[existing()],
                &lineage,
            )
            .err(),
            Some(SchemaMigrationPlanningError::EmptyEntityVersionBump),
        );
    }

    #[test]
    fn physical_transform_cannot_target_the_primary_key() {
        let (initial, candidate) = accepted_fixture();
        let entity_source = EntitySourceKey::try_new("User").expect("entity should admit");
        let entity_id = candidate
            .bundle()
            .source_bindings()
            .entity(&entity_source)
            .expect("entity should bind");
        let transition = EntityMigration::try_new(
            entity_source.clone(),
            DeclaredEntityVersion::try_new(1).expect("version should admit"),
            None,
            Vec::new(),
            vec![SchemaMigrationTransform::Fill {
                to: field("email"),
                literal: ScalarLiteral::Nat(7),
            }],
        )
        .expect("transition should admit");
        let migration = proposal(
            2,
            "User",
            "email",
            head(),
            Some(SchemaMigrationPlan::try_new(vec![transition]).expect("plan should admit")),
        );
        let mut lineage = AcceptedEntitySourceLineageCatalog::default();
        lineage
            .insert(
                TargetStoreIdentity::from_bytes([2; 32]),
                entity_id,
                AcceptedEntitySourceLineage::adopted(
                    head(),
                    AcceptedEntitySourceVersion::try_new(1).expect("version should admit"),
                    initial
                        .entity_source_digest(&entity_source)
                        .expect("digest should derive"),
                )
                .expect("lineage should admit"),
            )
            .expect("lineage should insert");
        assert_eq!(
            plan_schema_migration(
                &migration,
                &[ExistingProposalStore {
                    path: "test::Store",
                    identity: TargetStoreIdentity::from_bytes([2; 32]),
                    bundle: candidate.bundle(),
                }],
                &lineage,
            )
            .err(),
            Some(SchemaMigrationPlanningError::UnsupportedTransform),
        );
    }

    #[test]
    fn checked_cast_transform_compiles_to_accepted_ids_and_full_rewrite_layout() {
        let initial = checked_cast_proposal(false, ExpectedAcceptedHead::Empty, None);
        let candidate = lower_initial_schema_proposal(
            &initial,
            &[crate::db::schema::ProposalStoreTarget {
                path: "test::Store",
                identity: TargetStoreIdentity::from_bytes([2; 32]),
            }],
        )
        .expect("initial proposal should lower")
        .pop()
        .expect("initial candidate should exist");
        let entity_source = EntitySourceKey::try_new("User").expect("entity should admit");
        let entity_id = candidate
            .bundle()
            .source_bindings()
            .entity(&entity_source)
            .expect("entity should bind");
        let old_value = candidate
            .bundle()
            .source_bindings()
            .field(entity_id, &field("old_value"))
            .expect("old field should bind");
        let plan = SchemaMigrationPlan::try_new(vec![
            EntityMigration::try_new(
                entity_source.clone(),
                DeclaredEntityVersion::try_new(1).expect("version should admit"),
                None,
                Vec::new(),
                vec![SchemaMigrationTransform::CheckedCast {
                    from: field("old_value"),
                    to: field("value"),
                    target: ScalarType::Nat8,
                }],
            )
            .expect("transition should admit"),
        ])
        .expect("plan should admit");
        let migration = checked_cast_proposal(true, head(), Some(plan));
        let mut lineage = AcceptedEntitySourceLineageCatalog::default();
        lineage
            .insert(
                TargetStoreIdentity::from_bytes([2; 32]),
                entity_id,
                AcceptedEntitySourceLineage::adopted(
                    head(),
                    AcceptedEntitySourceVersion::try_new(1).expect("version should admit"),
                    initial
                        .entity_source_digest(&entity_source)
                        .expect("digest should derive"),
                )
                .expect("lineage should admit"),
            )
            .expect("lineage should insert");
        let planned = plan_schema_migration(
            &migration,
            &[ExistingProposalStore {
                path: "test::Store",
                identity: TargetStoreIdentity::from_bytes([2; 32]),
                bundle: candidate.bundle(),
            }],
            &lineage,
        )
        .expect("physical migration should plan");
        assert!(planned.requires_physical_validation());
        assert_eq!(planned.programs().len(), 1);
        let next = planned.candidates()[0]
            .bundle()
            .entity_snapshots()
            .get(&entity_id)
            .expect("candidate entity should exist");
        let value_id = planned.candidates()[0]
            .bundle()
            .source_bindings()
            .field(entity_id, &field("value"))
            .expect("candidate field should bind");
        // Dense field removal may reuse the predecessor ordinal in the
        // candidate catalog. The compiled program keeps the before and after
        // catalog contexts distinct, so numeric equality is not identity
        // aliasing across accepted revisions.
        assert_eq!(old_value, value_id);
        assert!(
            planned.candidates()[0]
                .bundle()
                .source_bindings()
                .field(entity_id, &field("old_value"))
                .is_none()
        );
        assert_eq!(
            next.row_layout().current_version(),
            next.row_layout().history_floor(),
        );
        assert!(
            next.row_layout().current_version()
                > candidate.bundle().entity_snapshots()[&entity_id]
                    .row_layout()
                    .current_version()
        );
    }

    #[test]
    fn managed_generated_indexed_entity_plans_rename_and_same_field_checked_cast() {
        let initial = maintained_user_proposal(false, ExpectedAcceptedHead::Empty, None);
        let candidate = lower_initial_schema_proposal(
            &initial,
            &[crate::db::schema::ProposalStoreTarget {
                path: "test::Store",
                identity: TargetStoreIdentity::from_bytes([2; 32]),
            }],
        )
        .expect("initial proposal should lower")
        .pop()
        .expect("initial candidate should exist");
        let entity_source = EntitySourceKey::try_new("SqlTestUser").expect("entity should admit");
        let entity_id = candidate
            .bundle()
            .source_bindings()
            .entity(&entity_source)
            .expect("entity should bind");
        let plan = SchemaMigrationPlan::try_new(vec![
            EntityMigration::try_new(
                entity_source.clone(),
                DeclaredEntityVersion::try_new(1).expect("version should admit"),
                None,
                vec![SchemaMigrationRename::Field {
                    from: field("rank"),
                    to: field("score"),
                }],
                vec![SchemaMigrationTransform::CheckedCast {
                    from: field("age"),
                    to: field("age"),
                    target: ScalarType::Nat16,
                }],
            )
            .expect("transition should admit"),
        ])
        .expect("plan should admit");
        let migration = maintained_user_proposal(true, head(), Some(plan));
        let mut lineage = AcceptedEntitySourceLineageCatalog::default();
        lineage
            .insert(
                TargetStoreIdentity::from_bytes([2; 32]),
                entity_id,
                AcceptedEntitySourceLineage::adopted(
                    head(),
                    AcceptedEntitySourceVersion::try_new(1).expect("version should admit"),
                    initial
                        .entity_source_digest(&entity_source)
                        .expect("digest should derive"),
                )
                .expect("lineage should admit"),
            )
            .expect("lineage should insert");

        let control_source = EntitySourceKey::try_new("Control").expect("entity should admit");
        let control_id = candidate
            .bundle()
            .source_bindings()
            .entity(&control_source)
            .expect("control should bind");
        lineage
            .insert(
                TargetStoreIdentity::from_bytes([2; 32]),
                control_id,
                AcceptedEntitySourceLineage::adopted(
                    head(),
                    AcceptedEntitySourceVersion::try_new(1).expect("version should admit"),
                    initial
                        .entity_source_digest(&control_source)
                        .expect("digest should derive"),
                )
                .expect("lineage should admit"),
            )
            .expect("control lineage should insert");

        let planned = plan_schema_migration(
            &migration,
            &[ExistingProposalStore {
                path: "test::Store",
                identity: TargetStoreIdentity::from_bytes([2; 32]),
                bundle: candidate.bundle(),
            }],
            &lineage,
        )
        .expect("maintained entity migration should plan");

        assert!(planned.requires_physical_validation());
        assert_eq!(planned.programs().len(), 1);
    }

    #[test]
    fn every_closed_transform_form_compiles_into_one_accepted_id_program() {
        let initial = closed_transform_proposal(false, ExpectedAcceptedHead::Empty, None);
        let candidate = lower_initial_schema_proposal(
            &initial,
            &[crate::db::schema::ProposalStoreTarget {
                path: "test::Store",
                identity: TargetStoreIdentity::from_bytes([2; 32]),
            }],
        )
        .expect("initial proposal should lower")
        .pop()
        .expect("initial candidate should exist");
        let entity_source = EntitySourceKey::try_new("User").expect("entity should admit");
        let entity_id = candidate
            .bundle()
            .source_bindings()
            .entity(&entity_source)
            .expect("entity should bind");
        let transition = EntityMigration::try_new(
            entity_source.clone(),
            DeclaredEntityVersion::try_new(1).expect("version should admit"),
            None,
            Vec::new(),
            vec![
                SchemaMigrationTransform::Fill {
                    to: field("filled"),
                    literal: ScalarLiteral::Nat(9),
                },
                SchemaMigrationTransform::Copy {
                    from: field("signed"),
                    to: field("copied"),
                },
                SchemaMigrationTransform::CheckedCast {
                    from: field("signed"),
                    to: field("cast"),
                    target: ScalarType::Nat8,
                },
                SchemaMigrationTransform::Coalesce {
                    from: field("optional"),
                    to: field("coalesced"),
                    literal: ScalarLiteral::Int(3),
                },
            ],
        )
        .expect("transition should admit");
        let migration = closed_transform_proposal(
            true,
            head(),
            Some(SchemaMigrationPlan::try_new(vec![transition]).expect("plan should admit")),
        );
        let mut lineage = AcceptedEntitySourceLineageCatalog::default();
        lineage
            .insert(
                TargetStoreIdentity::from_bytes([2; 32]),
                entity_id,
                AcceptedEntitySourceLineage::adopted(
                    head(),
                    AcceptedEntitySourceVersion::try_new(1).expect("version should admit"),
                    initial
                        .entity_source_digest(&entity_source)
                        .expect("digest should derive"),
                )
                .expect("lineage should admit"),
            )
            .expect("lineage should insert");
        let planned = plan_schema_migration(
            &migration,
            &[ExistingProposalStore {
                path: "test::Store",
                identity: TargetStoreIdentity::from_bytes([2; 32]),
                bundle: candidate.bundle(),
            }],
            &lineage,
        )
        .expect("closed transform plan should compile");
        assert_eq!(planned.programs().len(), 1);
        assert_eq!(planned.programs()[0].transform_count(), 4);
        let candidate_entity = &planned.candidates()[0].bundle().entity_snapshots()[&entity_id];
        assert_eq!(candidate_entity.indexes().len(), 1);
        assert!(candidate_entity.indexes()[0].unique());
        assert_ne!(candidate_entity.indexes()[0].physical_generation(), 0);
    }
}
