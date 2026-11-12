//! Module: db::schema::application_lowering
//! Responsibility: lower source-keyed initial proposals into accepted catalog candidates.
//! Does not own: optimistic admission, durable receipts, publication, or activation progress.
//! Boundary: validated public proposal plus target-store routing -> catalog-native candidates.

use std::collections::{BTreeMap, BTreeSet};

use icydb_schema::{
    ConstraintSourceKey, EntityFragment, EntitySourceKey, FieldInsertPolicy, FieldManagementPolicy,
    FieldSourceKey, FieldType, IndexKeyFragment, IndexSourceKey, NamedTypeFragment,
    RelationPathStepFragment, RelationSourceFragment, RelationSourceKey, ScalarLiteral, ScalarType,
    SchemaProposal, SchemaRemoval, SourceRuleOperation, TargetStoreIdentity, TargetedRuleFragment,
    TypeSourceKey,
};

use crate::{
    MAX_INDEX_FIELDS,
    db::{
        data::encode_input_value_for_candidate_field_contract,
        identity::EntityName,
        index::{index_expression_output_kind, index_expression_text},
        schema::{
            AcceptedConstraintCatalog, AcceptedConstraintKind, AcceptedEnumCatalog,
            AcceptedFieldDecodeContract, AcceptedFieldKind, AcceptedNamedTypeIdentity,
            AcceptedRuleOperation, AcceptedRuleTarget, AcceptedSchemaRevision,
            AcceptedSchemaRevisionBundle, AcceptedSourceBindingCatalog, CandidateSchemaRevision,
            ConstraintActivationKind, ConstraintId, ConstraintOrigin, FieldId,
            FieldInsertGeneration, FieldStorageDecode, FieldWriteManagement, LeafCodec,
            MAX_ACCEPTED_RECURSIVE_DEPTH, PersistedFieldOrigin, PersistedFieldSnapshot,
            PersistedIndexExpressionOp, PersistedIndexExpressionSnapshot,
            PersistedIndexFieldPathSnapshot, PersistedIndexKeyItemSnapshot,
            PersistedIndexKeySnapshot, PersistedIndexSnapshot, PersistedNestedLeafSnapshot,
            PersistedRelationEdgeSnapshot, PersistedRelationPathStepSnapshot,
            PersistedRelationSourceSnapshot, PersistedSchemaSnapshot, RelationId, RowLayoutVersion,
            SchemaFieldSlot, SchemaFieldWritePolicy, SchemaHistoricalFill, SchemaIndexId,
            SchemaInsertDefault, SchemaRowLayout, SchemaVersion, ValueAdmissionBudget,
            accepted_rule_exact_numeric_kind_is_supported, accepted_rule_length_kind_is_supported,
            accepted_rule_numeric_kind_is_supported, accepted_rule_target_is_reachable,
            bind_source_check_expr, bind_source_rule_literal,
            composite_catalog::{
                AcceptedCompositeCatalog, AcceptedCompositeElement, AcceptedCompositeField,
                AcceptedCompositeShape, CompositeFieldId, CompositeTypeId,
            },
            derive_dense_field_removal_candidate, derive_dense_index_removal_candidate,
            derive_relation_removal_candidate,
            enum_catalog::InitialEnumDefinitions,
            source_literal_input,
        },
    },
    error::InternalError,
    types::EntityTag,
    value::{EnumTypeId, EnumVariantId},
};

/// One registered store routing fact admitted by the application boundary.
#[derive(Clone, Copy)]
pub(in crate::db::schema) struct ProposalStoreTarget {
    pub(in crate::db::schema) path: &'static str,
    pub(in crate::db::schema) identity: TargetStoreIdentity,
}

/// One registered store and its exact current accepted authority used while
/// lowering an existing-head proposal.
pub(in crate::db::schema) struct ExistingProposalStore<'a> {
    pub(in crate::db::schema) path: &'static str,
    pub(in crate::db::schema) identity: TargetStoreIdentity,
    pub(in crate::db::schema) bundle: &'a AcceptedSchemaRevisionBundle,
}

/// Immutable accepted-value and cross-entity facts shared while lowering one
/// store-local initial candidate.
struct InitialStoreContext<'a> {
    store_path: &'static str,
    assignments: &'a BTreeMap<EntitySourceKey, &'static str>,
    all_entities: &'a BTreeMap<EntitySourceKey, &'a EntityFragment>,
    accepted_entities: &'a BTreeMap<EntitySourceKey, EntityTag>,
    enum_catalog: &'a AcceptedEnumCatalog,
    composite_catalog: &'a AcceptedCompositeCatalog,
    named_type_bindings: AcceptedSourceBindingCatalog,
}

struct InitialNamedTypes {
    enum_catalog: AcceptedEnumCatalog,
    composite_catalog: AcceptedCompositeCatalog,
    bindings: AcceptedSourceBindingCatalog,
}

/// Exact named-type identity authority used by recursive field-type lowering.
///
/// Initial proposal lowering owns freshly allocated identities before the
/// source-binding catalog exists. Existing-head and runtime binding checks use
/// the accepted source-binding catalog. Keeping both cases in this closed
/// owner avoids generating one recursive lowering body per caller closure.
#[derive(Clone, Copy)]
enum NamedTypeIdentityLookup<'a> {
    Initial(&'a BTreeMap<TypeSourceKey, AcceptedNamedTypeIdentity>),
    Accepted(&'a AcceptedSourceBindingCatalog),
}

impl NamedTypeIdentityLookup<'_> {
    fn resolve(self, source: &TypeSourceKey) -> Option<AcceptedNamedTypeIdentity> {
        match self {
            Self::Initial(bindings) => bindings.get(source).copied(),
            Self::Accepted(bindings) => bindings.named_type(source),
        }
    }
}

/// Accepted identities and value catalogs used to lower proposed field facts.
#[derive(Clone, Copy)]
pub(in crate::db::schema) struct AcceptedFieldLowering<'a> {
    pub(in crate::db::schema) bindings: &'a AcceptedSourceBindingCatalog,
    pub(in crate::db::schema) enum_catalog: &'a AcceptedEnumCatalog,
    pub(in crate::db::schema) composite_catalog: &'a AcceptedCompositeCatalog,
}

impl AcceptedFieldLowering<'_> {
    fn lower_default(
        self,
        policy: &FieldInsertPolicy,
        field_name: &str,
        kind: &AcceptedFieldKind,
        nullable: bool,
        storage_decode: FieldStorageDecode,
        leaf_codec: LeafCodec,
    ) -> Result<SchemaInsertDefault, InternalError> {
        let FieldInsertPolicy::Default(literal) = policy else {
            return Ok(SchemaInsertDefault::None);
        };
        let input = source_literal_input(literal, kind, self.bindings, self.enum_catalog)
            .map_err(|_| InternalError::store_unsupported())?;
        let field = AcceptedFieldDecodeContract::new(
            field_name,
            kind,
            nullable,
            storage_decode,
            leaf_codec,
        );
        let mut budget = ValueAdmissionBudget::standard();
        let payload = encode_input_value_for_candidate_field_contract(
            self.enum_catalog,
            self.composite_catalog,
            field,
            input,
            &mut budget,
        )?;
        Ok(SchemaInsertDefault::SlotPayload(payload))
    }
}

impl AcceptedFieldLowering<'_> {
    /// Lower generated field facts once while retaining the caller's explicit
    /// identity, slot, introduced layout and historical-absence contract.
    ///
    /// Reconciliation preserves accepted historical fill. Initial construction has
    /// no predecessor rows, and full-layout migration rewrites every row; those
    /// callers explicitly reject absent historical slots.
    pub(in crate::db::schema) fn lower_field(
        self,
        proposed: &icydb_schema::FieldFragment,
        id: FieldId,
        slot: SchemaFieldSlot,
        introduced_in_layout: RowLayoutVersion,
        historical_fill: SchemaHistoricalFill,
    ) -> Result<PersistedFieldSnapshot, InternalError> {
        let kind = lower_field_type(proposed.field_type(), self.bindings)?;
        let storage_decode = field_storage_decode(proposed.field_type());
        let leaf_codec = field_leaf_codec(proposed.field_type(), &kind);
        let nested_leaves = lower_migration_nested_leaves(&kind, self.composite_catalog)?;
        let write_policy =
            lower_write_policy(proposed.insert_policy(), proposed.management(), &kind)?;
        let insert_default = self.lower_default(
            proposed.insert_policy(),
            proposed.name().as_str(),
            &kind,
            proposed.nullable(),
            storage_decode,
            leaf_codec,
        )?;

        Ok(PersistedFieldSnapshot::new_with_write_policy_and_origin(
            id,
            proposed.name().as_str().to_string(),
            slot,
            kind,
            nested_leaves,
            proposed.nullable(),
            introduced_in_layout,
            insert_default,
            historical_fill,
            write_policy,
            PersistedFieldOrigin::Generated,
            storage_decode,
            leaf_codec,
        ))
    }
}

type InitialEnumVariantBindings = BTreeMap<(EnumTypeId, TypeSourceKey), EnumVariantId>;
type InitialCompositeFieldBindings = BTreeMap<(CompositeTypeId, FieldSourceKey), CompositeFieldId>;
type InitialCompositeDefinitions = BTreeMap<CompositeTypeId, (String, AcceptedCompositeShape)>;

/// Composite catalog and member bindings allocated from one source closure.
struct InitialCompositeTypes {
    catalog: AcceptedCompositeCatalog,
    field_bindings: InitialCompositeFieldBindings,
}

/// Mutable accepted identities allocated while completing one store-local
/// initial candidate.
#[derive(Default)]
struct InitialObjectBindings {
    indexes: BTreeMap<(EntityTag, icydb_schema::IndexSourceKey), SchemaIndexId>,
    relations: BTreeMap<(EntityTag, icydb_schema::RelationSourceKey), RelationId>,
    constraints: BTreeMap<(EntityTag, icydb_schema::ConstraintSourceKey), ConstraintId>,
}

type ExistingEntitiesByStore<'store, 'bundle, 'proposal> = BTreeMap<
    &'static str,
    (
        &'store ExistingProposalStore<'bundle>,
        Vec<&'proposal EntityFragment>,
    ),
>;

fn lower_initial_named_types(
    entities: &[&EntityFragment],
    types: &BTreeMap<TypeSourceKey, &NamedTypeFragment>,
) -> Result<InitialNamedTypes, InternalError> {
    let reachable = collect_reachable_named_types(entities, types)?;
    let mut type_bindings = BTreeMap::new();
    let mut next_enum_id = 1_u32;
    let mut next_composite_id = 1_u32;
    for source in &reachable {
        let definition = types
            .get(source)
            .copied()
            .ok_or_else(InternalError::store_unsupported)?;
        let identity = if matches!(definition, NamedTypeFragment::Enum(_)) {
            let id = EnumTypeId::new(next_enum_id).ok_or_else(InternalError::store_unsupported)?;
            next_enum_id = next_enum_id
                .checked_add(1)
                .ok_or_else(InternalError::store_unsupported)?;
            AcceptedNamedTypeIdentity::Enum(id)
        } else {
            let id = CompositeTypeId::new(next_composite_id)
                .ok_or_else(InternalError::store_unsupported)?;
            next_composite_id = next_composite_id
                .checked_add(1)
                .ok_or_else(InternalError::store_unsupported)?;
            AcceptedNamedTypeIdentity::Composite(id)
        };
        type_bindings.insert(source.clone(), identity);
    }

    let (enum_catalog, enum_variant_bindings) = lower_initial_enum_catalog(types, &type_bindings)?;
    let composite_types = lower_initial_composite_catalog(types, &type_bindings, &enum_catalog)?;
    let bindings = AcceptedSourceBindingCatalog::default().with_initial_named_types(
        type_bindings,
        enum_variant_bindings,
        composite_types.field_bindings,
    );
    Ok(InitialNamedTypes {
        enum_catalog,
        composite_catalog: composite_types.catalog,
        bindings,
    })
}

fn collect_reachable_named_types(
    entities: &[&EntityFragment],
    types: &BTreeMap<TypeSourceKey, &NamedTypeFragment>,
) -> Result<BTreeSet<TypeSourceKey>, InternalError> {
    let mut pending = entities.iter().flat_map(|entity| entity.fields()).fold(
        Vec::new(),
        |mut pending, field| {
            field
                .field_type()
                .append_named_type_dependencies(&mut pending);
            pending
        },
    );
    let mut reachable = BTreeSet::new();
    while let Some(source) = pending.pop() {
        if !reachable.insert(source.clone()) {
            continue;
        }
        let definition = types
            .get(&source)
            .copied()
            .ok_or_else(InternalError::store_unsupported)?;
        definition.append_named_type_dependencies(&mut pending);
    }
    Ok(reachable)
}

fn lower_initial_enum_catalog(
    types: &BTreeMap<TypeSourceKey, &NamedTypeFragment>,
    bindings: &BTreeMap<TypeSourceKey, AcceptedNamedTypeIdentity>,
) -> Result<(AcceptedEnumCatalog, InitialEnumVariantBindings), InternalError> {
    let (definitions, variant_bindings) = lower_new_enum_definitions(types, bindings, bindings)?;
    let catalog = AcceptedEnumCatalog::from_initial_definitions(definitions)
        .map_err(|_| InternalError::store_unsupported())?;
    Ok((catalog, variant_bindings))
}

/// Construct fresh definitions using the full accepted/proposed identity closure.
fn lower_new_enum_definitions(
    types: &BTreeMap<TypeSourceKey, &NamedTypeFragment>,
    allocated: &BTreeMap<TypeSourceKey, AcceptedNamedTypeIdentity>,
    bindings: &BTreeMap<TypeSourceKey, AcceptedNamedTypeIdentity>,
) -> Result<(InitialEnumDefinitions, InitialEnumVariantBindings), InternalError> {
    let mut definitions = BTreeMap::new();
    let mut variant_bindings = BTreeMap::new();
    for (source, identity) in allocated {
        let AcceptedNamedTypeIdentity::Enum(type_id) = identity else {
            continue;
        };
        let Some(NamedTypeFragment::Enum(definition)) = types.get(source).copied() else {
            return Err(InternalError::store_invariant());
        };
        let mut variants = BTreeMap::new();
        for (offset, variant) in definition.variants().iter().enumerate() {
            let raw = u32::try_from(offset)
                .ok()
                .and_then(|value| value.checked_add(1))
                .ok_or_else(InternalError::store_unsupported)?;
            let variant_id =
                EnumVariantId::new(raw).ok_or_else(InternalError::store_unsupported)?;
            let payload = variant
                .payload()
                .map(|payload| {
                    Ok::<_, InternalError>((
                        lower_initial_field_type(payload, bindings)?,
                        field_storage_decode(payload),
                    ))
                })
                .transpose()?;
            variants.insert(variant_id, (variant.name().as_str().to_string(), payload));
            variant_bindings.insert((*type_id, variant.source_key().clone()), variant_id);
        }
        definitions.insert(*type_id, (definition.name().as_str().to_string(), variants));
    }
    Ok((definitions, variant_bindings))
}

fn lower_initial_composite_catalog(
    types: &BTreeMap<TypeSourceKey, &NamedTypeFragment>,
    bindings: &BTreeMap<TypeSourceKey, AcceptedNamedTypeIdentity>,
    enum_catalog: &AcceptedEnumCatalog,
) -> Result<InitialCompositeTypes, InternalError> {
    let (definitions, field_bindings) = lower_new_composite_definitions(types, bindings, bindings)?;
    let catalog = AcceptedCompositeCatalog::from_initial_definitions(definitions, enum_catalog)
        .map_err(|_| InternalError::store_unsupported())?;
    Ok(InitialCompositeTypes {
        catalog,
        field_bindings,
    })
}

/// Allocate record members once; all cross-type references use pinned identities.
fn lower_new_composite_definitions(
    types: &BTreeMap<TypeSourceKey, &NamedTypeFragment>,
    allocated: &BTreeMap<TypeSourceKey, AcceptedNamedTypeIdentity>,
    bindings: &BTreeMap<TypeSourceKey, AcceptedNamedTypeIdentity>,
) -> Result<(InitialCompositeDefinitions, InitialCompositeFieldBindings), InternalError> {
    let mut definitions = BTreeMap::new();
    let mut field_bindings = BTreeMap::new();
    for (source, identity) in allocated {
        let AcceptedNamedTypeIdentity::Composite(type_id) = identity else {
            continue;
        };
        let definition = types
            .get(source)
            .copied()
            .ok_or_else(InternalError::store_invariant)?;
        let shape =
            lower_initial_composite_shape(*type_id, definition, bindings, &mut field_bindings)?;
        definitions.insert(*type_id, (definition.name().as_str().to_string(), shape));
    }
    Ok((definitions, field_bindings))
}

fn lower_initial_composite_shape(
    type_id: CompositeTypeId,
    definition: &NamedTypeFragment,
    bindings: &BTreeMap<TypeSourceKey, AcceptedNamedTypeIdentity>,
    field_bindings: &mut InitialCompositeFieldBindings,
) -> Result<AcceptedCompositeShape, InternalError> {
    let shape = match definition {
        NamedTypeFragment::Record(record) => {
            let mut fields = record
                .fields()
                .iter()
                .enumerate()
                .map(|(offset, field)| {
                    let raw = u32::try_from(offset)
                        .ok()
                        .and_then(|value| value.checked_add(1))
                        .ok_or_else(InternalError::store_unsupported)?;
                    let field_id =
                        CompositeFieldId::new(raw).ok_or_else(InternalError::store_unsupported)?;
                    if field_bindings
                        .insert((type_id, field.source_key().clone()), field_id)
                        .is_some()
                    {
                        return Err(InternalError::store_invariant());
                    }
                    Ok(AcceptedCompositeField::new(
                        field_id,
                        field.name().as_str().to_string(),
                        AcceptedCompositeElement::new(
                            lower_initial_field_type(field.field_type(), bindings)?,
                            field.nullable(),
                        ),
                    ))
                })
                .collect::<Result<Vec<_>, InternalError>>()?;
            icydb_schema::compact_sort_unstable_by(&mut fields, |left, right| {
                left.name().cmp(right.name())
            });
            AcceptedCompositeShape::Record(fields)
        }
        NamedTypeFragment::Enum(_) => return Err(InternalError::store_invariant()),
        _ => lower_non_record_composite_shape(
            definition,
            NamedTypeIdentityLookup::Initial(bindings),
        )?,
    };
    Ok(shape)
}

fn proposal_definitions(
    proposal: &SchemaProposal,
) -> (
    BTreeMap<EntitySourceKey, &EntityFragment>,
    BTreeMap<TypeSourceKey, &NamedTypeFragment>,
) {
    // Proposal validation owns duplicate rejection. Direct insertion avoids
    // BTreeMap's stable bulk-collection sorter at this Wasm boundary.
    let mut entities = BTreeMap::new();
    let mut types = BTreeMap::new();
    for fragment in proposal.fragments() {
        for entity in fragment.entities() {
            entities.insert(entity.source_key().clone(), entity);
        }
        for r#type in fragment.types() {
            types.insert(r#type.source_key().clone(), r#type);
        }
    }
    (entities, types)
}

/// Lower a source-keyed proposal against an empty accepted database.
///
/// This is the sole proposal-to-accepted candidate path. Mutations of an
/// existing accepted head remain rejected until their identity-preserving
/// catalog transitions are connected here; callers never substitute generated
/// model authority or partially publish a proposal.
pub(in crate::db::schema) fn lower_initial_schema_proposal(
    proposal: &SchemaProposal,
    stores: &[ProposalStoreTarget],
) -> Result<Vec<CandidateSchemaRevision>, InternalError> {
    if !proposal.removals().is_empty() {
        return Err(InternalError::store_unsupported());
    }

    let mut store_paths = BTreeMap::new();
    for store in stores {
        store_paths.insert(store.identity, store.path);
    }
    let mut assignments = BTreeMap::new();
    for assignment in proposal.assignments() {
        let path = store_paths
            .get(&assignment.store())
            .copied()
            .ok_or_else(InternalError::store_unsupported)?;
        assignments.insert(assignment.entity().clone(), path);
    }
    let (entities, types) = proposal_definitions(proposal);
    if entities.len() != proposal.assignments().len()
        || assignments
            .keys()
            .any(|source| !entities.contains_key(source))
    {
        return Err(InternalError::store_unsupported());
    }

    let mut entities_by_store = BTreeMap::<&'static str, Vec<&EntityFragment>>::new();
    // Each store retains the source-key order of this BTreeMap subsequence.
    for (source, entity) in &entities {
        let path = assignments
            .get(source)
            .copied()
            .ok_or_else(InternalError::store_unsupported)?;
        entities_by_store.entry(path).or_default().push(*entity);
    }

    let accepted_entities = allocate_entity_identities(&entities_by_store)?;
    let mut candidates = Vec::with_capacity(entities_by_store.len());
    for (store_path, store_entities) in entities_by_store {
        candidates.push(lower_initial_store(
            store_path,
            store_entities.as_slice(),
            &assignments,
            &entities,
            &accepted_entities,
            &types,
        )?);
    }
    validate_entity_namespace(&candidates, &[])?;
    Ok(candidates)
}

/// Lower an exact proposal against a non-empty accepted head.
///
/// This existing-head lane owns future insert-default and source-keyed display
/// metadata reconciliation, creation of empty entities, explicit removal of
/// accepted generated entities, checks, fields, indexes and unreferenced named
/// types, and generated checks whose complete historical domain is proven empty at the
/// application boundary. Existing structural facts must resolve through immutable
/// source bindings and match accepted authority exactly. New entity fields use
/// scalar, newly declared or already accepted named types. Other additions,
/// activation work, and physical changes therefore fail before candidate
/// construction instead of falling back to generated-model reconciliation.
pub(in crate::db::schema) fn lower_existing_schema_proposal(
    proposal: &SchemaProposal,
    stores: &[ExistingProposalStore<'_>],
) -> Result<Vec<CandidateSchemaRevision>, InternalError> {
    let removes_entity = proposal
        .removals()
        .iter()
        .any(|removal| matches!(removal, SchemaRemoval::Entity(_)));
    if removes_entity
        && (proposal.removals().len() != 1
            || !proposal.fragments().is_empty()
            || !proposal.assignments().is_empty())
    {
        return Err(InternalError::store_unsupported());
    }
    let (mut entities_by_store, types) = existing_proposal_entities_by_store(proposal, stores)?;
    let mut removals_by_store = BTreeMap::<&'static str, ExistingStoreRemovals>::new();
    for removal in proposal.removals() {
        if let SchemaRemoval::Type(source) = removal {
            attach_existing_type_removal(
                stores,
                source,
                &mut entities_by_store,
                &mut removals_by_store,
            )?;
            continue;
        }
        let (store, resolved) = resolve_existing_removal(stores, removal)?;
        entities_by_store
            .entry(store.path)
            .or_insert_with(|| (store, Vec::new()));
        removals_by_store
            .entry(store.path)
            .or_default()
            .push(resolved);
    }

    lower_existing_store_candidates(proposal, stores, entities_by_store, &types, |path| {
        Some(removals_by_store.remove(path).unwrap_or_default())
    })
}

/// Lower one canonical generated proposal whose sealed ingress proves that it
/// cannot request explicit removals.
pub(in crate::db::schema) fn lower_generated_existing_schema_proposal(
    proposal: &SchemaProposal,
    stores: &[ExistingProposalStore<'_>],
) -> Result<Vec<CandidateSchemaRevision>, InternalError> {
    if !proposal.removals().is_empty() {
        return Err(InternalError::store_invariant());
    }
    let (entities_by_store, types) = existing_proposal_entities_by_store(proposal, stores)?;
    lower_existing_store_candidates(proposal, stores, entities_by_store, &types, |_| None)
}

// Both ingress lanes share candidate construction and final namespace/type admission.
// A present removal batch, even empty, retains existing-lane catalog validation;
// generated ingress has already rejected removals and skips that transition.
fn lower_existing_store_candidates(
    proposal: &SchemaProposal,
    stores: &[ExistingProposalStore<'_>],
    entities_by_store: ExistingEntitiesByStore<'_, '_, '_>,
    types: &BTreeMap<TypeSourceKey, &NamedTypeFragment>,
    mut removals_for_store: impl FnMut(&'static str) -> Option<ExistingStoreRemovals>,
) -> Result<Vec<CandidateSchemaRevision>, InternalError> {
    let mut used_types = BTreeSet::new();
    let mut candidates = Vec::new();
    for (_, (store, mut entities)) in entities_by_store {
        icydb_schema::compact_sort_unstable_by(&mut entities, |left, right| {
            left.source_key().cmp(right.source_key())
        });
        let mut state = ExistingStoreCandidateState::new(store, &entities, types, &mut used_types)?;
        let version_advanced = removals_for_store(store.path)
            .map(|removals| state.apply_removals(removals))
            .transpose()?;
        if let Some(candidate) =
            state.finish(store, stores, proposal, entities, version_advanced.as_ref())?
        {
            candidates.push(candidate);
        }
    }
    if used_types.len() != types.len() {
        return Err(InternalError::store_unsupported());
    }
    validate_entity_namespace(&candidates, stores)?;
    Ok(candidates)
}

// Validate the projected database, including stores omitted from the proposal.
// A candidate replaces its whole store bundle; including that store's old names
// would incorrectly reject unchanged entities and case-only renames.
fn validate_entity_namespace(
    candidates: &[CandidateSchemaRevision],
    stores: &[ExistingProposalStore<'_>],
) -> Result<(), InternalError> {
    let snapshots = candidates
        .iter()
        .flat_map(|candidate| candidate.bundle().entity_snapshots().values())
        .chain(
            stores
                .iter()
                .filter(|store| {
                    !candidates
                        .iter()
                        .any(|candidate| candidate.store_path() == store.path)
                })
                .flat_map(|store| store.bundle.entity_snapshots().values()),
        );
    let mut names = BTreeSet::new();
    for snapshot in snapshots {
        let name = EntityName::try_from_str(snapshot.entity_name())
            .map_err(|_| InternalError::store_unsupported())?
            .ascii_case_fold();
        if !names.insert(name) {
            return Err(InternalError::store_unsupported());
        }
    }
    Ok(())
}

fn existing_proposal_entities_by_store<'store, 'bundle, 'proposal>(
    proposal: &'proposal SchemaProposal,
    stores: &'store [ExistingProposalStore<'bundle>],
) -> Result<
    (
        ExistingEntitiesByStore<'store, 'bundle, 'proposal>,
        BTreeMap<TypeSourceKey, &'proposal NamedTypeFragment>,
    ),
    InternalError,
> {
    let mut store_by_identity = BTreeMap::new();
    for store in stores {
        store_by_identity.insert(store.identity, store);
    }
    let (entities, types) = proposal_definitions(proposal);
    if entities.len() != proposal.assignments().len() {
        return Err(InternalError::store_unsupported());
    }

    let mut entities_by_store = ExistingEntitiesByStore::new();
    for assignment in proposal.assignments() {
        let store = store_by_identity
            .get(&assignment.store())
            .copied()
            .ok_or_else(InternalError::store_unsupported)?;
        let entity = entities
            .get(assignment.entity())
            .copied()
            .ok_or_else(InternalError::store_unsupported)?;
        verify_entity_binding_owner(stores, assignment.entity(), store)?;
        entities_by_store
            .entry(store.path)
            .or_insert_with(|| (store, Vec::new()))
            .1
            .push(entity);
    }
    Ok((entities_by_store, types))
}

/// One source-resolved generated row-local constraint selected for exact removal.
#[derive(Eq, Ord, PartialEq, PartialOrd)]
struct ExistingConstraintRemoval {
    entity_tag: EntityTag,
    source: ConstraintSourceKey,
    id: ConstraintId,
}

/// One source-resolved generated entity selected for exact removal.
#[derive(Eq, Ord, PartialEq, PartialOrd)]
struct ExistingEntityRemoval {
    source: EntitySourceKey,
    tag: EntityTag,
}

/// One source-resolved generated field selected for exact physical removal.
#[derive(Eq, Ord, PartialEq, PartialOrd)]
struct ExistingFieldRemoval {
    entity_tag: EntityTag,
    source: FieldSourceKey,
    id: FieldId,
}

/// One source-resolved generated secondary index selected for exact removal.
#[derive(Eq, Ord, PartialEq, PartialOrd)]
struct ExistingIndexRemoval {
    entity_tag: EntityTag,
    source: IndexSourceKey,
    id: SchemaIndexId,
}

/// One source-resolved generated relation selected for exact removal.
#[derive(Eq, Ord, PartialEq, PartialOrd)]
struct ExistingRelationRemoval {
    entity_tag: EntityTag,
    source: RelationSourceKey,
    id: RelationId,
}

/// One source-resolved generated named type selected for exact removal.
#[derive(Eq, Ord, PartialEq, PartialOrd)]
struct ExistingTypeRemoval {
    source: TypeSourceKey,
    identity: AcceptedNamedTypeIdentity,
}

/// Source-resolved removals grouped for one store-local candidate.
#[derive(Default)]
struct ExistingStoreRemovals {
    constraints: Vec<ExistingConstraintRemoval>,
    entities: Vec<ExistingEntityRemoval>,
    fields: Vec<ExistingFieldRemoval>,
    indexes: Vec<ExistingIndexRemoval>,
    relations: Vec<ExistingRelationRemoval>,
    types: Vec<ExistingTypeRemoval>,
}

impl ExistingStoreRemovals {
    const fn is_empty(&self) -> bool {
        self.constraints.is_empty()
            && self.entities.is_empty()
            && self.fields.is_empty()
            && self.indexes.is_empty()
            && self.relations.is_empty()
            && self.types.is_empty()
    }

    fn push(&mut self, removal: ExistingRemoval) {
        match removal {
            ExistingRemoval::Constraint(removal) => self.constraints.push(removal),
            ExistingRemoval::Entity(removal) => self.entities.push(removal),
            ExistingRemoval::Field(removal) => self.fields.push(removal),
            ExistingRemoval::Index(removal) => self.indexes.push(removal),
            ExistingRemoval::Relation(removal) => self.relations.push(removal),
        }
    }
}

/// One source-resolved removal whose store owner is carried separately.
enum ExistingRemoval {
    Constraint(ExistingConstraintRemoval),
    Entity(ExistingEntityRemoval),
    Field(ExistingFieldRemoval),
    Index(ExistingIndexRemoval),
    Relation(ExistingRelationRemoval),
}

/// Shared existing-store candidate state after accepted catalogs are pinned.
/// Explicit removal mutation remains a separate optional preparation step so
/// generated no-removal actors do not retain it.
struct ExistingStoreCandidateState {
    catalogs: ExistingCatalogCandidate,
    snapshots: BTreeMap<EntityTag, PersistedSchemaSnapshot>,
    source_bindings: AcceptedSourceBindingCatalog,
    changed: bool,
}

impl ExistingStoreCandidateState {
    fn new(
        store: &ExistingProposalStore<'_>,
        entities: &[&EntityFragment],
        types: &BTreeMap<TypeSourceKey, &NamedTypeFragment>,
        used_types: &mut BTreeSet<TypeSourceKey>,
    ) -> Result<Self, InternalError> {
        let additions = collect_added_named_types(store.bundle, entities, types)?;
        let mut catalogs =
            lower_existing_named_catalogs(store.bundle, entities, types, &additions, used_types)?;
        let mut source_bindings = store.bundle.source_bindings().clone();
        append_added_named_types(&mut catalogs, &mut source_bindings, types, &additions)?;
        Ok(Self {
            catalogs,
            snapshots: store.bundle.entity_snapshots().clone(),
            source_bindings,
            changed: false,
        })
    }

    fn apply_removals(
        &mut self,
        mut removals: ExistingStoreRemovals,
    ) -> Result<BTreeSet<EntityTag>, InternalError> {
        icydb_schema::compact_sort_unstable_by(&mut removals.constraints, Ord::cmp);
        icydb_schema::compact_sort_unstable_by(&mut removals.entities, Ord::cmp);
        icydb_schema::compact_sort_unstable_by(&mut removals.fields, Ord::cmp);
        icydb_schema::compact_sort_unstable_by(&mut removals.indexes, Ord::cmp);
        icydb_schema::compact_sort_unstable_by(&mut removals.relations, Ord::cmp);
        icydb_schema::compact_sort_unstable_by(&mut removals.types, Ord::cmp);
        self.changed = !removals.is_empty();

        let mut changed_entities = apply_existing_constraint_removals(
            &mut self.snapshots,
            &mut self.source_bindings,
            removals.constraints.as_slice(),
        )?;
        changed_entities.extend(apply_existing_index_removals(
            &mut self.snapshots,
            &mut self.source_bindings,
            removals.indexes.as_slice(),
        )?);
        changed_entities.extend(apply_existing_relation_removals(
            &mut self.snapshots,
            &mut self.source_bindings,
            removals.relations.as_slice(),
        )?);
        changed_entities.extend(apply_existing_field_removals(
            &mut self.snapshots,
            &mut self.source_bindings,
            removals.fields.as_slice(),
        )?);
        apply_existing_type_removals(
            &mut self.catalogs,
            &self.snapshots,
            &mut self.source_bindings,
            removals.types.as_slice(),
        )?;
        apply_existing_entity_removals(
            &mut self.snapshots,
            &mut self.source_bindings,
            removals.entities.as_slice(),
        )?;
        advance_removed_entity_schema_versions(&mut self.snapshots, &changed_entities)?;
        Ok(changed_entities)
    }

    fn finish(
        mut self,
        store: &ExistingProposalStore<'_>,
        stores: &[ExistingProposalStore<'_>],
        proposal: &SchemaProposal,
        entities: Vec<&EntityFragment>,
        version_advanced: Option<&BTreeSet<EntityTag>>,
    ) -> Result<Option<CandidateSchemaRevision>, InternalError> {
        self.add_new_entities(store, stores, proposal, &entities)?;
        for entity in entities {
            let Some(entity_tag) = store.bundle.source_bindings().entity(entity.source_key())
            else {
                continue;
            };
            let current = self
                .snapshots
                .get(&entity_tag)
                .ok_or_else(InternalError::store_invariant)?;
            if let Some(candidate) = lower_existing_entity(
                store.bundle,
                &self.catalogs,
                stores,
                entity,
                current,
                version_advanced.is_some_and(|tags| tags.contains(&entity_tag)),
                &mut self.source_bindings,
            )? {
                self.snapshots.insert(entity_tag, candidate);
                self.changed = true;
            }
        }
        if !self.changed && !self.catalogs.changed {
            return Ok(None);
        }
        let revision = store
            .bundle
            .revision()
            .checked_next()
            .ok_or_else(InternalError::store_unsupported)?;
        let bundle = AcceptedSchemaRevisionBundle::new_with_source_bindings(
            revision,
            store.path,
            self.catalogs.enum_catalog,
            self.catalogs.composite_catalog,
            self.source_bindings,
            self.snapshots,
        )?;
        CandidateSchemaRevision::new(bundle).map(Some)
    }

    /// Construct new empty entity scopes using the same field/index/constraint
    /// lowering as installation, while retaining current accepted value IDs.
    fn add_new_entities(
        &mut self,
        store: &ExistingProposalStore<'_>,
        stores: &[ExistingProposalStore<'_>],
        proposal: &SchemaProposal,
        entities: &[&EntityFragment],
    ) -> Result<(), InternalError> {
        let added = entities
            .iter()
            .copied()
            .filter(|entity| {
                store
                    .bundle
                    .source_bindings()
                    .entity(entity.source_key())
                    .is_none()
            })
            .collect::<Vec<_>>();
        if added.is_empty() {
            return Ok(());
        }
        let (all_entities, _) = proposal_definitions(proposal);
        let accepted_entities = allocate_added_entity_identities(proposal, stores)?;
        let mut assignments = BTreeMap::new();
        for assignment in proposal.assignments() {
            let owner = stores
                .iter()
                .find(|owner| owner.identity == assignment.store())
                .ok_or_else(InternalError::store_unsupported)?;
            assignments.insert(assignment.entity().clone(), owner.path);
        }
        let context = InitialStoreContext {
            store_path: store.path,
            assignments: &assignments,
            all_entities: &all_entities,
            accepted_entities: &accepted_entities,
            enum_catalog: &self.catalogs.enum_catalog,
            composite_catalog: &self.catalogs.composite_catalog,
            named_type_bindings: self.source_bindings.clone(),
        };
        self.snapshots.extend(lower_new_entity_snapshots(
            &context,
            &added,
            &mut self.source_bindings,
        )?);
        self.changed = true;
        Ok(())
    }
}

/// Resolve one explicit removal to its unique accepted store and structural
/// identity owner.
fn resolve_existing_removal<'store, 'bundle>(
    stores: &'store [ExistingProposalStore<'bundle>],
    removal: &SchemaRemoval,
) -> Result<(&'store ExistingProposalStore<'bundle>, ExistingRemoval), InternalError> {
    match removal {
        SchemaRemoval::Entity(entity) => {
            let (store, removal) = resolve_existing_generated_entity_removal(stores, entity)?;
            Ok((store, ExistingRemoval::Entity(removal)))
        }
        SchemaRemoval::Constraint { entity, constraint } => {
            let (store, removal) =
                resolve_existing_generated_constraint_removal(stores, entity, constraint)?;
            Ok((store, ExistingRemoval::Constraint(removal)))
        }
        SchemaRemoval::Field { entity, field } => {
            let (store, removal) = resolve_existing_generated_field_removal(stores, entity, field)?;
            Ok((store, ExistingRemoval::Field(removal)))
        }
        SchemaRemoval::Index { entity, index } => {
            let (store, removal) = resolve_existing_generated_index_removal(stores, entity, index)?;
            Ok((store, ExistingRemoval::Index(removal)))
        }
        SchemaRemoval::Relation { entity, relation } => {
            let (store, removal) =
                resolve_existing_generated_relation_removal(stores, entity, relation)?;
            Ok((store, ExistingRemoval::Relation(removal)))
        }
        SchemaRemoval::Type(_) => Err(InternalError::store_unsupported()),
    }
}

// All generated removals require one unambiguous accepted entity with no pending
// constraint, index, or relation publication. Kind-specific ownership stays local.
fn resolve_existing_removal_entity<'store, 'bundle>(
    stores: &'store [ExistingProposalStore<'bundle>],
    entity_source: &EntitySourceKey,
) -> Result<
    (
        &'store ExistingProposalStore<'bundle>,
        EntityTag,
        &'bundle PersistedSchemaSnapshot,
    ),
    InternalError,
> {
    let (store, entity_tag, snapshot) = resolve_existing_entity(stores, entity_source)?;
    if !snapshot.constraint_activations().is_empty()
        || !snapshot.candidate_indexes().is_empty()
        || !snapshot.candidate_relations().is_empty()
    {
        return Err(InternalError::store_unsupported());
    }
    Ok((store, entity_tag, snapshot))
}

/// Resolve one generated entity removal solely through immutable accepted
/// source identity.
fn resolve_existing_generated_entity_removal<'store, 'bundle>(
    stores: &'store [ExistingProposalStore<'bundle>],
    entity_source: &EntitySourceKey,
) -> Result<
    (
        &'store ExistingProposalStore<'bundle>,
        ExistingEntityRemoval,
    ),
    InternalError,
> {
    let (store, entity_tag, _) = resolve_existing_removal_entity(stores, entity_source)?;
    Ok((
        store,
        ExistingEntityRemoval {
            source: entity_source.clone(),
            tag: entity_tag,
        },
    ))
}

/// Attach one source-keyed named-type removal to every store-local accepted
/// copy of that same generated definition.
fn attach_existing_type_removal<'store, 'bundle>(
    stores: &'store [ExistingProposalStore<'bundle>],
    source: &TypeSourceKey,
    entities_by_store: &mut ExistingEntitiesByStore<'store, 'bundle, '_>,
    removals_by_store: &mut BTreeMap<&'static str, ExistingStoreRemovals>,
) -> Result<(), InternalError> {
    let mut found = false;
    for store in stores {
        let Some(identity) = store.bundle.source_bindings().named_type(source) else {
            continue;
        };
        found = true;
        entities_by_store
            .entry(store.path)
            .or_insert_with(|| (store, Vec::new()));
        removals_by_store
            .entry(store.path)
            .or_default()
            .types
            .push(ExistingTypeRemoval {
                source: source.clone(),
                identity,
            });
    }
    if !found {
        return Err(InternalError::store_unsupported());
    }
    Ok(())
}

fn apply_existing_type_removals(
    catalogs: &mut ExistingCatalogCandidate,
    snapshots: &BTreeMap<EntityTag, PersistedSchemaSnapshot>,
    source_bindings: &mut AcceptedSourceBindingCatalog,
    removals: &[ExistingTypeRemoval],
) -> Result<(), InternalError> {
    let enum_type_ids = removals
        .iter()
        .filter_map(|removal| match removal.identity {
            AcceptedNamedTypeIdentity::Enum(type_id) => Some(type_id),
            AcceptedNamedTypeIdentity::Composite(_) => None,
        })
        .collect::<BTreeSet<_>>();
    let composite_type_ids = removals
        .iter()
        .filter_map(|removal| match removal.identity {
            AcceptedNamedTypeIdentity::Composite(type_id) => Some(type_id),
            AcceptedNamedTypeIdentity::Enum(_) => None,
        })
        .collect::<BTreeSet<_>>();
    if enum_type_ids.len().saturating_add(composite_type_ids.len()) != removals.len() {
        return Err(InternalError::store_unsupported());
    }
    catalogs.enum_catalog = catalogs
        .enum_catalog
        .clone()
        .with_removed_types(&enum_type_ids)
        .map_err(|_| InternalError::store_unsupported())?;
    catalogs.composite_catalog = catalogs
        .composite_catalog
        .clone()
        .with_removed_types(&composite_type_ids, &catalogs.enum_catalog)
        .map_err(|_| InternalError::store_unsupported())?;
    for removal in removals {
        source_bindings.remove_named_type(&removal.source, removal.identity)?;
    }
    catalogs.changed |= !removals.is_empty();
    if snapshots
        .values()
        .flat_map(PersistedSchemaSnapshot::fields)
        .any(|field| {
            !catalogs
                .composite_catalog
                .matches_kind(&catalogs.enum_catalog, field.kind())
                || field.nested_leaves().iter().any(|leaf| {
                    !catalogs
                        .composite_catalog
                        .matches_kind(&catalogs.enum_catalog, leaf.kind())
                })
        })
    {
        return Err(InternalError::store_unsupported());
    }
    Ok(())
}

/// Remove one exact entity snapshot and its complete source-binding subtree.
fn apply_existing_entity_removals(
    snapshots: &mut BTreeMap<EntityTag, PersistedSchemaSnapshot>,
    source_bindings: &mut AcceptedSourceBindingCatalog,
    removals: &[ExistingEntityRemoval],
) -> Result<(), InternalError> {
    if removals.len() > 1 {
        return Err(InternalError::store_unsupported());
    }
    for removal in removals {
        snapshots
            .remove(&removal.tag)
            .ok_or_else(InternalError::store_invariant)?;
        source_bindings.remove_entity(&removal.source, removal.tag)?;
    }
    Ok(())
}

// Resolve the shared immutable identity before constructing a kind-specific
// removal. The member callback distinguishes absence from disallowed ownership:
// None is a catalog invariant, while Some(false) is an unsupported removal.
fn resolve_existing_generated_removal_id<'store, 'bundle, Id: Copy>(
    stores: &'store [ExistingProposalStore<'bundle>],
    entity_source: &EntitySourceKey,
    binding: impl FnOnce(&AcceptedSourceBindingCatalog, EntityTag) -> Option<Id>,
    member_admitted: impl FnOnce(&PersistedSchemaSnapshot, Id) -> Option<bool>,
) -> Result<(&'store ExistingProposalStore<'bundle>, EntityTag, Id), InternalError> {
    let (store, entity_tag, snapshot) = resolve_existing_removal_entity(stores, entity_source)?;
    let id = binding(store.bundle.source_bindings(), entity_tag)
        .ok_or_else(InternalError::store_unsupported)?;
    if !member_admitted(snapshot, id).ok_or_else(InternalError::store_invariant)? {
        return Err(InternalError::store_unsupported());
    }
    Ok((store, entity_tag, id))
}

/// Resolve one removal solely through immutable accepted source identity.
///
/// A live activation or independently owned check remains outside this
/// metadata-only transition and must fail before a candidate exists.
fn resolve_existing_generated_constraint_removal<'store, 'bundle>(
    stores: &'store [ExistingProposalStore<'bundle>],
    entity_source: &EntitySourceKey,
    constraint_source: &ConstraintSourceKey,
) -> Result<
    (
        &'store ExistingProposalStore<'bundle>,
        ExistingConstraintRemoval,
    ),
    InternalError,
> {
    let (store, entity_tag, constraint_id) = resolve_existing_generated_removal_id(
        stores,
        entity_source,
        |bindings, tag| bindings.constraint(tag, constraint_source),
        |snapshot, id| {
            snapshot
                .constraints()
                .iter()
                .find(|constraint| constraint.id() == id)
                .map(|constraint| {
                    constraint.origin() == ConstraintOrigin::Generated
                        && matches!(
                            constraint.kind(),
                            AcceptedConstraintKind::Check { .. }
                                | AcceptedConstraintKind::TargetedRule { .. }
                        )
                })
        },
    )?;
    Ok((
        store,
        ExistingConstraintRemoval {
            entity_tag,
            source: constraint_source.clone(),
            id: constraint_id,
        },
    ))
}

/// Resolve one generated field removal solely through immutable accepted
/// source identity.
fn resolve_existing_generated_field_removal<'store, 'bundle>(
    stores: &'store [ExistingProposalStore<'bundle>],
    entity_source: &EntitySourceKey,
    field_source: &FieldSourceKey,
) -> Result<(&'store ExistingProposalStore<'bundle>, ExistingFieldRemoval), InternalError> {
    let (store, entity_tag, field_id) = resolve_existing_generated_removal_id(
        stores,
        entity_source,
        |bindings, tag| bindings.field(tag, field_source),
        |snapshot, id| {
            snapshot
                .fields()
                .iter()
                .find(|field| field.id() == id)
                .map(PersistedFieldSnapshot::generated)
        },
    )?;
    Ok((
        store,
        ExistingFieldRemoval {
            entity_tag,
            source: field_source.clone(),
            id: field_id,
        },
    ))
}

/// Resolve one generated secondary-index removal solely through immutable
/// accepted source identity.
fn resolve_existing_generated_index_removal<'store, 'bundle>(
    stores: &'store [ExistingProposalStore<'bundle>],
    entity_source: &EntitySourceKey,
    index_source: &IndexSourceKey,
) -> Result<(&'store ExistingProposalStore<'bundle>, ExistingIndexRemoval), InternalError> {
    let (store, entity_tag, index_id) = resolve_existing_generated_removal_id(
        stores,
        entity_source,
        |bindings, tag| bindings.index(tag, index_source),
        |snapshot, id| {
            snapshot
                .indexes()
                .iter()
                .find(|index| index.schema_id() == id)
                .map(PersistedIndexSnapshot::generated)
        },
    )?;
    Ok((
        store,
        ExistingIndexRemoval {
            entity_tag,
            source: index_source.clone(),
            id: index_id,
        },
    ))
}

/// Resolve one generated relation removal solely through immutable accepted
/// source identity.
fn resolve_existing_generated_relation_removal<'store, 'bundle>(
    stores: &'store [ExistingProposalStore<'bundle>],
    entity_source: &EntitySourceKey,
    relation_source: &RelationSourceKey,
) -> Result<
    (
        &'store ExistingProposalStore<'bundle>,
        ExistingRelationRemoval,
    ),
    InternalError,
> {
    let (store, entity_tag, relation_id) = resolve_existing_generated_removal_id(
        stores,
        entity_source,
        |bindings, tag| bindings.relation(tag, relation_source),
        |snapshot, id| {
            snapshot
                .relations()
                .iter()
                .any(|relation| relation.id() == id)
                .then_some(true)
        },
    )?;
    Ok((
        store,
        ExistingRelationRemoval {
            entity_tag,
            source: relation_source.clone(),
            id: relation_id,
        },
    ))
}

/// Remove generated row-local constraints and source bindings atomically.
fn apply_existing_constraint_removals(
    snapshots: &mut BTreeMap<EntityTag, PersistedSchemaSnapshot>,
    source_bindings: &mut AcceptedSourceBindingCatalog,
    removals: &[ExistingConstraintRemoval],
) -> Result<BTreeSet<EntityTag>, InternalError> {
    let mut removals_by_entity = BTreeMap::<EntityTag, Vec<&ExistingConstraintRemoval>>::new();
    for removal in removals {
        removals_by_entity
            .entry(removal.entity_tag)
            .or_default()
            .push(removal);
    }

    let mut changed_entities = BTreeSet::new();
    for (entity_tag, entity_removals) in removals_by_entity {
        let current = snapshots
            .get(&entity_tag)
            .cloned()
            .ok_or_else(InternalError::store_invariant)?;
        let mut catalog = current.constraint_catalog().clone();
        for removal in entity_removals {
            catalog = catalog
                .with_removed_generated_constraint(removal.id)
                .map_err(|_| InternalError::store_invariant())?;
            source_bindings.remove_constraint(entity_tag, &removal.source, removal.id)?;
        }
        snapshots.insert(entity_tag, current.with_constraint_catalog(catalog));
        changed_entities.insert(entity_tag);
    }
    Ok(changed_entities)
}

/// Remove generated indexes and source bindings through one dense physical
/// ordinal derivation per entity.
fn apply_existing_index_removals(
    snapshots: &mut BTreeMap<EntityTag, PersistedSchemaSnapshot>,
    source_bindings: &mut AcceptedSourceBindingCatalog,
    removals: &[ExistingIndexRemoval],
) -> Result<BTreeSet<EntityTag>, InternalError> {
    apply_single_existing_removals(
        snapshots,
        removals,
        |removal| removal.entity_tag,
        |current, removal| {
            let candidate = derive_dense_index_removal_candidate(current, removal.id)
                .map_err(|_| InternalError::store_unsupported())?;
            source_bindings.remove_index(removal.entity_tag, &removal.source, removal.id)?;
            Ok(candidate)
        },
    )
}

/// Remove one generated relation and its paired constraint per entity.
fn apply_existing_relation_removals(
    snapshots: &mut BTreeMap<EntityTag, PersistedSchemaSnapshot>,
    source_bindings: &mut AcceptedSourceBindingCatalog,
    removals: &[ExistingRelationRemoval],
) -> Result<BTreeSet<EntityTag>, InternalError> {
    apply_single_existing_removals(
        snapshots,
        removals,
        |removal| removal.entity_tag,
        |current, removal| {
            let candidate = derive_relation_removal_candidate(current, removal.id)?;
            source_bindings.remove_relation(removal.entity_tag, &removal.source, removal.id)?;
            Ok(candidate)
        },
    )
}

/// Derive one dense generated-field removal per entity and carry every
/// retained source binding through the candidate-owned field-ID lineage.
fn apply_existing_field_removals(
    snapshots: &mut BTreeMap<EntityTag, PersistedSchemaSnapshot>,
    source_bindings: &mut AcceptedSourceBindingCatalog,
    removals: &[ExistingFieldRemoval],
) -> Result<BTreeSet<EntityTag>, InternalError> {
    apply_single_existing_removals(
        snapshots,
        removals,
        |removal| removal.entity_tag,
        |current, removal| {
            let candidate = derive_dense_field_removal_candidate(current, removal.id)
                .map_err(|_| InternalError::store_unsupported())?;
            source_bindings.remove_field_and_remap(
                removal.entity_tag,
                &removal.source,
                removal.id,
                |field_id| candidate.retained_field_id(field_id),
            )?;
            Ok(candidate.into_snapshot())
        },
    )
}

// Preserve entity ordering and reject multiple removals before reading that
// entity's snapshot. Kind-specific derivation and binding updates stay together.
fn apply_single_existing_removals<R>(
    snapshots: &mut BTreeMap<EntityTag, PersistedSchemaSnapshot>,
    removals: &[R],
    entity_tag: impl Fn(&R) -> EntityTag,
    mut derive: impl FnMut(
        &PersistedSchemaSnapshot,
        &R,
    ) -> Result<PersistedSchemaSnapshot, InternalError>,
) -> Result<BTreeSet<EntityTag>, InternalError> {
    let mut removals_by_entity = BTreeMap::<EntityTag, Vec<&R>>::new();
    for removal in removals {
        removals_by_entity
            .entry(entity_tag(removal))
            .or_default()
            .push(removal);
    }

    let mut changed_entities = BTreeSet::new();
    for (entity_tag, entity_removals) in removals_by_entity {
        let [removal] = entity_removals.as_slice() else {
            return Err(InternalError::store_unsupported());
        };
        let current = snapshots
            .get(&entity_tag)
            .cloned()
            .ok_or_else(InternalError::store_invariant)?;
        let candidate = derive(&current, removal)?;
        snapshots.insert(entity_tag, candidate);
        changed_entities.insert(entity_tag);
    }
    Ok(changed_entities)
}

/// Advance each entity touched by explicit removals exactly once, including
/// proposals that combine check and field removal.
fn advance_removed_entity_schema_versions(
    snapshots: &mut BTreeMap<EntityTag, PersistedSchemaSnapshot>,
    changed_entities: &BTreeSet<EntityTag>,
) -> Result<(), InternalError> {
    for entity_tag in changed_entities {
        let current = snapshots
            .get(entity_tag)
            .cloned()
            .ok_or_else(InternalError::store_invariant)?;
        let version = current
            .version()
            .get()
            .checked_add(1)
            .map(SchemaVersion::new)
            .ok_or_else(InternalError::store_unsupported)?;
        snapshots.insert(*entity_tag, current.with_schema_version(version));
    }
    Ok(())
}

/// Candidate value catalogs for one exact existing accepted head.
struct ExistingCatalogCandidate {
    enum_catalog: AcceptedEnumCatalog,
    composite_catalog: AcceptedCompositeCatalog,
    changed: bool,
}

fn verify_entity_binding_owner(
    stores: &[ExistingProposalStore<'_>],
    source: &EntitySourceKey,
    expected: &ExistingProposalStore<'_>,
) -> Result<(), InternalError> {
    let owners = stores
        .iter()
        .filter(|store| store.bundle.source_bindings().entity(source).is_some())
        .collect::<Vec<_>>();
    match owners.as_slice() {
        [] => Ok(()),
        [owner] if owner.path == expected.path => Ok(()),
        _ => Err(InternalError::store_unsupported()),
    }
}

/// Allocate all proposed additions together, so canonical store/source ordering
/// is independent of which store-local candidate is currently being lowered.
fn allocate_added_entity_identities(
    proposal: &SchemaProposal,
    stores: &[ExistingProposalStore<'_>],
) -> Result<BTreeMap<EntitySourceKey, EntityTag>, InternalError> {
    let mut next = stores
        .iter()
        .flat_map(|store| store.bundle.entity_snapshots().keys())
        .map(|tag| tag.value())
        .max()
        .unwrap_or(0);
    let (entities, _) = proposal_definitions(proposal);
    let mut ordered = BTreeMap::new();
    for assignment in proposal.assignments() {
        let store = stores
            .iter()
            .find(|store| store.identity == assignment.store())
            .ok_or_else(InternalError::store_unsupported)?;
        ordered.insert((store.path, assignment.entity()), store);
    }
    let mut accepted = BTreeMap::new();
    for ((_, source), store) in ordered {
        let tag = if let Some(tag) = store.bundle.source_bindings().entity(source) {
            tag
        } else {
            let entity = entities
                .get(source)
                .ok_or_else(InternalError::store_invariant)?;
            if entity.version().get() != 1 {
                return Err(InternalError::store_unsupported());
            }
            next = next
                .checked_add(1)
                .ok_or_else(InternalError::store_unsupported)?;
            EntityTag::new(next)
        };
        accepted.insert(source.clone(), tag);
    }
    Ok(accepted)
}

/// Discover only absent definitions reachable from newly declared entities.
/// An omitted existing definition is already closed by accepted catalogs.
fn collect_added_named_types(
    bundle: &AcceptedSchemaRevisionBundle,
    entities: &[&EntityFragment],
    types: &BTreeMap<TypeSourceKey, &NamedTypeFragment>,
) -> Result<BTreeSet<TypeSourceKey>, InternalError> {
    let mut pending = Vec::new();
    for entity in entities {
        if bundle
            .source_bindings()
            .entity(entity.source_key())
            .is_none()
        {
            for field in entity.fields() {
                field
                    .field_type()
                    .append_named_type_dependencies(&mut pending);
            }
        }
    }
    let mut visited = BTreeSet::new();
    let mut added = BTreeSet::new();
    while let Some(source) = pending.pop() {
        if !visited.insert(source.clone()) {
            continue;
        }
        let existing = bundle.source_bindings().named_type(&source).is_some();
        if !existing {
            added.insert(source.clone());
        }
        if let Some(definition) = types.get(&source).copied() {
            definition.append_named_type_dependencies(&mut pending);
        } else if !existing {
            return Err(InternalError::store_unsupported());
        }
    }
    Ok(added)
}

/// Extend one candidate's accepted catalogs and identity bindings together.
/// Old definitions are copied from accepted authority, never regenerated from
/// declarations. Fresh enums and composites are merged before closure checking.
fn append_added_named_types(
    catalogs: &mut ExistingCatalogCandidate,
    source_bindings: &mut AcceptedSourceBindingCatalog,
    types: &BTreeMap<TypeSourceKey, &NamedTypeFragment>,
    additions: &BTreeSet<TypeSourceKey>,
) -> Result<(), InternalError> {
    if additions.is_empty() {
        return Ok(());
    }
    let mut enum_id = catalogs
        .enum_catalog
        .type_ids()
        .map(EnumTypeId::get)
        .max()
        .unwrap_or(0);
    let mut composite_id = catalogs
        .composite_catalog
        .id_by_path()
        .values()
        .map(|id| id.get())
        .max()
        .unwrap_or(0);
    let mut allocated = BTreeMap::new();
    for source in additions {
        let definition = types
            .get(source)
            .copied()
            .ok_or_else(InternalError::store_unsupported)?;
        if catalogs
            .enum_catalog
            .type_id(definition.name().as_str())
            .is_some()
            || catalogs
                .composite_catalog
                .id_by_path()
                .contains_key(definition.name().as_str())
        {
            return Err(InternalError::store_unsupported());
        }
        let identity = if matches!(definition, NamedTypeFragment::Enum(_)) {
            enum_id = enum_id
                .checked_add(1)
                .ok_or_else(InternalError::store_unsupported)?;
            AcceptedNamedTypeIdentity::Enum(
                EnumTypeId::new(enum_id).ok_or_else(InternalError::store_unsupported)?,
            )
        } else {
            composite_id = composite_id
                .checked_add(1)
                .ok_or_else(InternalError::store_unsupported)?;
            AcceptedNamedTypeIdentity::Composite(
                CompositeTypeId::new(composite_id).ok_or_else(InternalError::store_unsupported)?,
            )
        };
        allocated.insert(source.clone(), identity);
    }
    let mut all = source_bindings.named_types().clone();
    all.extend(allocated.clone());
    let (enums, variants) = lower_new_enum_definitions(types, &allocated, &all)?;
    let (composites, fields) = lower_new_composite_definitions(types, &allocated, &all)?;
    let enum_catalog = catalogs
        .enum_catalog
        .clone()
        .with_added_definitions(enums)
        .map_err(|_| InternalError::store_unsupported())?;
    let composite_catalog = catalogs
        .composite_catalog
        .clone()
        .with_added_definitions(composites, &enum_catalog)
        .map_err(|_| InternalError::store_unsupported())?;
    source_bindings.append_new_named_types(allocated, variants, fields)?;
    catalogs.enum_catalog = enum_catalog;
    catalogs.composite_catalog = composite_catalog;
    catalogs.changed = true;
    Ok(())
}

fn lower_existing_named_catalogs(
    bundle: &AcceptedSchemaRevisionBundle,
    entities: &[&EntityFragment],
    types: &BTreeMap<TypeSourceKey, &NamedTypeFragment>,
    additions: &BTreeSet<TypeSourceKey>,
    used_types: &mut BTreeSet<TypeSourceKey>,
) -> Result<ExistingCatalogCandidate, InternalError> {
    let mut pending = entities
        .iter()
        .flat_map(|entity| entity.fields())
        .filter_map(|field| named_field_type_source(field.field_type()))
        .collect::<Vec<_>>();
    let mut visited = BTreeSet::new();
    while let Some(source) = pending.pop() {
        if !visited.insert(source.clone()) {
            continue;
        }
        let identity = bundle.source_bindings().named_type(&source);
        if identity.is_none() {
            if !additions.contains(&source) {
                return Err(InternalError::store_unsupported());
            }
            let definition = types
                .get(&source)
                .copied()
                .ok_or_else(InternalError::store_unsupported)?;
            definition.append_named_type_dependencies(&mut pending);
            used_types.insert(source);
            continue;
        }
        if let Some(definition) = types.get(&source).copied() {
            if matches!(
                (identity, definition),
                (
                    Some(AcceptedNamedTypeIdentity::Enum(_)),
                    NamedTypeFragment::Enum(_)
                ) | (
                    Some(AcceptedNamedTypeIdentity::Composite(_)),
                    NamedTypeFragment::Record(_)
                        | NamedTypeFragment::Newtype { .. }
                        | NamedTypeFragment::List { .. }
                        | NamedTypeFragment::Set { .. }
                        | NamedTypeFragment::Map { .. }
                        | NamedTypeFragment::Tuple { .. }
                )
            ) {
                definition.append_named_type_dependencies(&mut pending);
                used_types.insert(source);
            } else {
                return Err(InternalError::store_unsupported());
            }
        }
    }

    visited.retain(|source| !additions.contains(source));
    for source in &visited {
        let Some(NamedTypeFragment::Enum(proposed)) = types.get(source).copied() else {
            continue;
        };
        let AcceptedNamedTypeIdentity::Enum(type_id) = bundle
            .source_bindings()
            .named_type(source)
            .ok_or_else(InternalError::store_unsupported)?
        else {
            return Err(InternalError::store_unsupported());
        };
        let mut variants = BTreeMap::new();
        for variant in proposed.variants() {
            let variant_id = bundle
                .source_bindings()
                .enum_variant(type_id, variant.source_key())
                .ok_or_else(InternalError::store_unsupported)?;
            let payload = variant
                .payload()
                .map(|payload| {
                    Ok::<_, InternalError>((
                        lower_field_type(payload, bundle.source_bindings())?,
                        field_storage_decode(payload),
                    ))
                })
                .transpose()?;
            variants.insert(variant_id, (variant.name().as_str().to_string(), payload));
        }
        let accepted = bundle
            .enum_catalog()
            .enum_type(type_id)
            .ok_or_else(InternalError::store_invariant)?;
        if !accepted.matches_exact_definition(proposed.name().as_str(), &variants) {
            return Err(InternalError::store_unsupported());
        }
    }

    let composite_catalog = lower_existing_composite_catalog(bundle, &visited, types)?;
    Ok(ExistingCatalogCandidate {
        enum_catalog: bundle.enum_catalog().clone(),
        composite_catalog,
        changed: false,
    })
}

fn lower_existing_composite_catalog(
    bundle: &AcceptedSchemaRevisionBundle,
    visited: &BTreeSet<TypeSourceKey>,
    types: &BTreeMap<TypeSourceKey, &NamedTypeFragment>,
) -> Result<AcceptedCompositeCatalog, InternalError> {
    for source in visited {
        let Some(definition) = types.get(source).copied() else {
            continue;
        };
        let AcceptedNamedTypeIdentity::Composite(type_id) = bundle
            .source_bindings()
            .named_type(source)
            .ok_or_else(InternalError::store_unsupported)?
        else {
            continue;
        };
        let shape = lower_existing_composite_shape(bundle.source_bindings(), type_id, definition)?;
        let accepted = bundle
            .composite_catalog()
            .composite_type(type_id)
            .ok_or_else(InternalError::store_invariant)?;
        if accepted.path() != definition.name().as_str() || accepted.shape() != &shape {
            return Err(InternalError::store_unsupported());
        }
    }
    Ok(bundle.composite_catalog().clone())
}

fn named_field_type_source(field_type: &FieldType) -> Option<TypeSourceKey> {
    match field_type {
        FieldType::List(item) => named_field_type_source(item),
        FieldType::Named(source) => Some(source.clone()),
        FieldType::Scalar(_) => None,
    }
}

fn lower_existing_composite_shape(
    bindings: &AcceptedSourceBindingCatalog,
    type_id: CompositeTypeId,
    definition: &NamedTypeFragment,
) -> Result<AcceptedCompositeShape, InternalError> {
    let shape = match definition {
        NamedTypeFragment::Record(record) => {
            let mut fields = record
                .fields()
                .iter()
                .map(|field| {
                    let field_id = bindings
                        .composite_field(type_id, field.source_key())
                        .ok_or_else(InternalError::store_unsupported)?;
                    Ok(AcceptedCompositeField::new(
                        field_id,
                        field.name().as_str().to_string(),
                        AcceptedCompositeElement::new(
                            lower_field_type(field.field_type(), bindings)?,
                            field.nullable(),
                        ),
                    ))
                })
                .collect::<Result<Vec<_>, InternalError>>()?;
            icydb_schema::compact_sort_unstable_by(&mut fields, |left, right| {
                left.name().cmp(right.name())
            });
            AcceptedCompositeShape::Record(fields)
        }
        NamedTypeFragment::Enum(_) => return Err(InternalError::store_unsupported()),
        _ => lower_non_record_composite_shape(
            definition,
            NamedTypeIdentityLookup::Accepted(bindings),
        )?,
    };
    Ok(shape)
}

fn lower_non_record_composite_shape(
    definition: &NamedTypeFragment,
    named_types: NamedTypeIdentityLookup<'_>,
) -> Result<AcceptedCompositeShape, InternalError> {
    let shape = match definition {
        NamedTypeFragment::Newtype { inner, .. } => AcceptedCompositeShape::Newtype(
            AcceptedCompositeElement::new(lower_field_type_with_lookup(inner, named_types)?, false),
        ),
        NamedTypeFragment::List { item, .. } => {
            AcceptedCompositeShape::Newtype(AcceptedCompositeElement::new(
                AcceptedFieldKind::List(Box::new(lower_field_type_with_lookup(item, named_types)?)),
                false,
            ))
        }
        NamedTypeFragment::Set { item, .. } => {
            AcceptedCompositeShape::Newtype(AcceptedCompositeElement::new(
                AcceptedFieldKind::Set(Box::new(lower_field_type_with_lookup(item, named_types)?)),
                false,
            ))
        }
        NamedTypeFragment::Map { key, value, .. } => {
            AcceptedCompositeShape::Newtype(AcceptedCompositeElement::new(
                AcceptedFieldKind::Map {
                    key: Box::new(lower_field_type_with_lookup(key, named_types)?),
                    value: Box::new(lower_field_type_with_lookup(value, named_types)?),
                },
                false,
            ))
        }
        NamedTypeFragment::Tuple { members, .. } => AcceptedCompositeShape::Tuple(
            members
                .iter()
                .map(|member| {
                    Ok(AcceptedCompositeElement::new(
                        lower_field_type_with_lookup(member.field_type(), named_types)?,
                        member.nullable(),
                    ))
                })
                .collect::<Result<Vec<_>, InternalError>>()?,
        ),
        NamedTypeFragment::Record(_) | NamedTypeFragment::Enum(_) => {
            return Err(InternalError::store_invariant());
        }
    };
    Ok(shape)
}

fn lower_existing_entity(
    bundle: &AcceptedSchemaRevisionBundle,
    catalogs: &ExistingCatalogCandidate,
    stores: &[ExistingProposalStore<'_>],
    entity: &EntityFragment,
    current: &PersistedSchemaSnapshot,
    schema_version_already_advanced: bool,
    source_bindings: &mut AcceptedSourceBindingCatalog,
) -> Result<Option<PersistedSchemaSnapshot>, InternalError> {
    let entity_tag = source_bindings
        .entity(entity.source_key())
        .ok_or_else(InternalError::store_invariant)?;
    let has_live_state = !current.constraint_activations().is_empty()
        || !current.candidate_indexes().is_empty()
        || !current.candidate_relations().is_empty();
    // SQL-owned activations can outlive a deployment. Compare generated facts
    // before rejecting them; generated-owned work still has its exact job owner.
    if has_live_state
        && (current.constraint_activations().is_empty()
            || current
                .constraint_activations()
                .iter()
                .any(|activation| activation.origin() != ConstraintOrigin::SqlDdl))
    {
        return Err(InternalError::store_unsupported());
    }

    let field_candidate =
        lower_existing_fields(catalogs, entity, entity_tag, current, source_bindings)?;
    let bindings = &*source_bindings;
    let primary_key = entity
        .primary_key()
        .iter()
        .map(|source| {
            bindings
                .field(entity_tag, source)
                .ok_or_else(InternalError::store_unsupported)
        })
        .collect::<Result<Vec<_>, _>>()?;
    if primary_key != current.primary_key_field_ids() {
        return Err(InternalError::store_unsupported());
    }
    if field_candidate.field_names_changed
        && current.indexes().iter().any(|index| !index.generated())
    {
        return Err(InternalError::store_unsupported());
    }
    let fields_changed = field_candidate.changed;

    let provisional = rebuild_existing_entity_snapshot(
        current,
        current.version(),
        entity.name().as_str(),
        field_candidate.fields,
        current.indexes().to_vec(),
        current.constraint_catalog().clone(),
    );
    let indexes = lower_existing_indexes(
        bundle,
        catalogs,
        entity,
        entity_tag,
        current,
        &provisional,
        bindings,
    )?;
    verify_existing_relations(stores, entity, entity_tag, current, bindings)?;
    let constraint_catalog = lower_existing_constraints(
        bundle,
        catalogs,
        entity,
        entity_tag,
        &provisional,
        current,
        source_bindings,
    )?;
    let constraints_changed = constraint_catalog != *current.constraint_catalog();

    if !fields_changed
        && entity.name().as_str() == current.entity_name()
        && indexes == current.indexes()
        && !constraints_changed
    {
        return Ok(None);
    }
    if has_live_state {
        return Err(InternalError::store_unsupported());
    }
    let version = if schema_version_already_advanced {
        current.version()
    } else {
        current
            .version()
            .get()
            .checked_add(1)
            .map(SchemaVersion::new)
            .ok_or_else(InternalError::store_unsupported)?
    };
    Ok(Some(rebuild_existing_entity_snapshot(
        current,
        version,
        entity.name().as_str(),
        provisional.fields().to_vec(),
        indexes,
        constraint_catalog,
    )))
}

fn rebuild_existing_entity_snapshot(
    current: &PersistedSchemaSnapshot,
    version: SchemaVersion,
    entity_name: &str,
    fields: Vec<PersistedFieldSnapshot>,
    indexes: Vec<PersistedIndexSnapshot>,
    constraint_catalog: AcceptedConstraintCatalog,
) -> PersistedSchemaSnapshot {
    PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        version,
        current.entity_path().to_string(),
        entity_name.to_string(),
        current.primary_key_field_ids().to_vec(),
        current.row_layout().clone(),
        fields,
        indexes,
    )
    .with_constraint_catalog(constraint_catalog)
    .with_relation_id_allocator(current.relation_id_allocator())
    .with_relations(current.relations().to_vec())
    .with_constraint_candidates(
        current.candidate_indexes().to_vec(),
        current.candidate_relations().to_vec(),
    )
}

/// Re-declared generated fields plus the metadata changes relevant to indexes.
struct ExistingFieldCandidate {
    fields: Vec<PersistedFieldSnapshot>,
    changed: bool,
    field_names_changed: bool,
}

fn lower_existing_fields(
    catalogs: &ExistingCatalogCandidate,
    entity: &EntityFragment,
    entity_tag: EntityTag,
    current: &PersistedSchemaSnapshot,
    bindings: &AcceptedSourceBindingCatalog,
) -> Result<ExistingFieldCandidate, InternalError> {
    let generated_field_count = current
        .fields()
        .iter()
        .filter(|field| field.generated())
        .count();
    if generated_field_count != entity.fields().len() {
        return Err(InternalError::store_unsupported());
    }
    let mut fields = current.fields().to_vec();
    let mut changed = false;
    let mut field_names_changed = false;
    for proposed in entity.fields() {
        let field_id = bindings
            .field(entity_tag, proposed.source_key())
            .ok_or_else(InternalError::store_unsupported)?;
        let position = fields
            .iter()
            .position(|field| field.id() == field_id)
            .ok_or_else(InternalError::store_invariant)?;
        let accepted = &fields[position];
        if !accepted.generated() {
            return Err(InternalError::store_unsupported());
        }
        let candidate = AcceptedFieldLowering {
            bindings,
            enum_catalog: &catalogs.enum_catalog,
            composite_catalog: &catalogs.composite_catalog,
        }
        .lower_field(
            proposed,
            accepted.id(),
            accepted.slot(),
            accepted.introduced_in_layout(),
            accepted.historical_fill().clone(),
        )?;
        if candidate.kind() != accepted.kind()
            || candidate.nullable() != accepted.nullable()
            || candidate.write_policy() != accepted.write_policy()
            || candidate.storage_decode() != accepted.storage_decode()
            || candidate.leaf_codec() != accepted.leaf_codec()
        {
            return Err(InternalError::store_unsupported());
        }
        if candidate != *accepted {
            if candidate.name() != accepted.name() {
                field_names_changed = true;
            }
            fields[position] = candidate;
            changed = true;
        }
    }
    Ok(ExistingFieldCandidate {
        fields,
        changed,
        field_names_changed,
    })
}

fn lower_existing_indexes(
    bundle: &AcceptedSchemaRevisionBundle,
    catalogs: &ExistingCatalogCandidate,
    entity: &EntityFragment,
    entity_tag: EntityTag,
    current: &PersistedSchemaSnapshot,
    candidate: &PersistedSchemaSnapshot,
    bindings: &AcceptedSourceBindingCatalog,
) -> Result<Vec<PersistedIndexSnapshot>, InternalError> {
    if current
        .indexes()
        .iter()
        .filter(|index| index.generated())
        .count()
        != entity.indexes().len()
    {
        return Err(InternalError::store_unsupported());
    }
    let mut indexes = current.indexes().to_vec();
    for proposed in entity.indexes() {
        let index_id = bindings
            .index(entity_tag, proposed.source_key())
            .ok_or_else(InternalError::store_unsupported)?;
        let accepted = current
            .indexes()
            .iter()
            .find(|index| index.schema_id() == index_id)
            .ok_or_else(InternalError::store_invariant)?;
        if !accepted.generated() {
            return Err(InternalError::store_unsupported());
        }
        let before = IndexLowering {
            enum_catalog: bundle.enum_catalog(),
            composite_catalog: bundle.composite_catalog(),
            entity_tag,
            snapshot: current,
            bindings,
        }
        .lower(proposed, accepted)?;
        if before != *accepted {
            return Err(InternalError::store_unsupported());
        }
        let after = IndexLowering {
            enum_catalog: &catalogs.enum_catalog,
            composite_catalog: &catalogs.composite_catalog,
            entity_tag,
            snapshot: candidate,
            bindings,
        }
        .lower(proposed, accepted)?;
        let position = indexes
            .iter()
            .position(|index| index.schema_id() == accepted.schema_id())
            .ok_or_else(InternalError::store_invariant)?;
        indexes[position] = after;
    }
    Ok(indexes)
}

/// Catalog and entity facts shared by initial, reconciled and migration indexes.
struct IndexLowering<'a> {
    pub(in crate::db::schema) enum_catalog: &'a AcceptedEnumCatalog,
    pub(in crate::db::schema) composite_catalog: &'a AcceptedCompositeCatalog,
    entity_tag: EntityTag,
    snapshot: &'a PersistedSchemaSnapshot,
    bindings: &'a AcceptedSourceBindingCatalog,
}

/// Lower one migration-bound current index through the ordinary accepted
/// index constructor while retaining the supplied accepted identity.
#[cfg(feature = "migration")]
pub(in crate::db::schema) fn lower_migration_index(
    proposed: &icydb_schema::IndexFragment,
    accepted: &PersistedIndexSnapshot,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
    entity_tag: EntityTag,
    snapshot: &PersistedSchemaSnapshot,
    bindings: &AcceptedSourceBindingCatalog,
) -> Result<PersistedIndexSnapshot, InternalError> {
    IndexLowering {
        enum_catalog,
        composite_catalog,
        entity_tag,
        snapshot,
        bindings,
    }
    .lower(proposed, accepted)
}

/// Lower one new generated index directly into an unpublished migration
/// candidate under already-reserved accepted identities.
#[cfg(feature = "migration")]
#[expect(
    clippy::too_many_arguments,
    reason = "new migration indexes bind every catalog and physical identity explicitly"
)]
pub(in crate::db::schema) fn lower_new_migration_index(
    proposed: &icydb_schema::IndexFragment,
    schema_id: SchemaIndexId,
    ordinal: u16,
    physical_generation: u64,
    store_path: &'static str,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
    entity_tag: EntityTag,
    snapshot: &PersistedSchemaSnapshot,
    bindings: &AcceptedSourceBindingCatalog,
) -> Result<PersistedIndexSnapshot, InternalError> {
    Ok(IndexLowering {
        enum_catalog,
        composite_catalog,
        entity_tag,
        snapshot,
        bindings,
    }
    .lower_new(proposed, schema_id, ordinal, store_path)?
    .clone_with_schema_identity(schema_id, ordinal, physical_generation))
}

impl IndexLowering<'_> {
    fn lower(
        &self,
        proposed: &icydb_schema::IndexFragment,
        accepted: &PersistedIndexSnapshot,
    ) -> Result<PersistedIndexSnapshot, InternalError> {
        Ok(self
            .lower_new(
                proposed,
                accepted.schema_id(),
                accepted.ordinal(),
                accepted.store(),
            )?
            .clone_with_schema_identity(
                accepted.schema_id(),
                accepted.ordinal(),
                accepted.physical_generation(),
            ))
    }

    // Bind key/predicate semantics once; callers retain identity allocation and
    // existing or reserved physical-generation selection.
    fn lower_new(
        &self,
        proposed: &icydb_schema::IndexFragment,
        schema_id: SchemaIndexId,
        ordinal: u16,
        store_path: &str,
    ) -> Result<PersistedIndexSnapshot, InternalError> {
        let key = lower_index_key(
            proposed.key(),
            self.entity_tag,
            self.snapshot,
            self.bindings,
        )?;
        let predicate = proposed
            .predicate()
            .map(|predicate| {
                let accepted_expression = bind_source_check_expr(
                    predicate,
                    self.entity_tag,
                    self.bindings,
                    self.snapshot,
                    self.enum_catalog,
                    self.composite_catalog,
                )
                .map_err(|_| InternalError::store_unsupported())?;
                crate::db::schema::AcceptedIndexPredicate::from_check(
                    &accepted_expression,
                    self.snapshot.fields(),
                    self.composite_catalog,
                )
            })
            .transpose()?;
        Ok(PersistedIndexSnapshot::new(
            schema_id,
            ordinal,
            proposed.name().as_str().to_string(),
            store_path.to_string(),
            proposed.unique(),
            key,
            predicate,
        ))
    }
}

fn verify_existing_relations(
    stores: &[ExistingProposalStore<'_>],
    entity: &EntityFragment,
    entity_tag: EntityTag,
    current: &PersistedSchemaSnapshot,
    bindings: &AcceptedSourceBindingCatalog,
) -> Result<(), InternalError> {
    if current.relations().len() != entity.relations().len() {
        return Err(InternalError::store_unsupported());
    }
    for proposed in entity.relations() {
        let relation_id = bindings
            .relation(entity_tag, proposed.source_key())
            .ok_or_else(InternalError::store_unsupported)?;
        let accepted = current
            .relations()
            .iter()
            .find(|relation| relation.id() == relation_id)
            .ok_or_else(InternalError::store_invariant)?;
        let (target_store, target_tag, target) =
            resolve_existing_entity(stores, proposed.target_entity())?;
        let target_fields = proposed
            .target_fields()
            .iter()
            .map(|source| {
                target_store
                    .bundle
                    .source_bindings()
                    .field(target_tag, source)
                    .ok_or_else(InternalError::store_unsupported)
            })
            .collect::<Result<Vec<_>, _>>()?;
        if target_fields != target.primary_key_field_ids() {
            return Err(InternalError::store_unsupported());
        }
        let source = lower_relation_source(proposed.source(), entity_tag, bindings)?;
        let candidate = relation_edge_from_source(
            accepted.id(),
            proposed.name().as_str().to_string(),
            target.entity_path().to_string(),
            source,
        )
        .clone_with_physical_generation(accepted.physical_generation());
        if candidate != *accepted {
            return Err(InternalError::store_unsupported());
        }
    }
    Ok(())
}

fn resolve_existing_entity<'store, 'bundle>(
    stores: &'store [ExistingProposalStore<'bundle>],
    source: &EntitySourceKey,
) -> Result<
    (
        &'store ExistingProposalStore<'bundle>,
        EntityTag,
        &'bundle PersistedSchemaSnapshot,
    ),
    InternalError,
> {
    let mut resolved = None;
    for store in stores {
        let Some(entity_tag) = store.bundle.source_bindings().entity(source) else {
            continue;
        };
        let snapshot = store
            .bundle
            .entity_snapshots()
            .get(&entity_tag)
            .ok_or_else(InternalError::store_invariant)?;
        if resolved.replace((store, entity_tag, snapshot)).is_some() {
            return Err(InternalError::store_unsupported());
        }
    }
    resolved.ok_or_else(InternalError::store_unsupported)
}

pub(in crate::db::schema) fn bind_targeted_rule(
    proposed: &TargetedRuleFragment,
    entity_tag: EntityTag,
    bindings: &AcceptedSourceBindingCatalog,
    candidate: &PersistedSchemaSnapshot,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
) -> Result<(AcceptedRuleTarget, AcceptedRuleOperation), InternalError> {
    let root_field_id = bindings
        .field(entity_tag, proposed.root())
        .ok_or_else(InternalError::store_unsupported)?;
    let root = candidate
        .fields()
        .iter()
        .find(|field| field.id() == root_field_id)
        .ok_or_else(InternalError::store_invariant)?;
    let target_type = bindings
        .named_type(proposed.target_type())
        .ok_or_else(InternalError::store_unsupported)?;
    if !accepted_rule_target_is_reachable(root.kind(), target_type, enum_catalog, composite_catalog)
    {
        return Err(InternalError::store_unsupported());
    }

    let target_kind = match target_type {
        AcceptedNamedTypeIdentity::Enum(type_id) => AcceptedFieldKind::Enum { type_id },
        AcceptedNamedTypeIdentity::Composite(type_id) => AcceptedFieldKind::Composite { type_id },
    };
    let resolved_kind = composite_catalog
        .resolve_newtype_value_kind(&target_kind)
        .ok_or_else(InternalError::store_unsupported)?;
    let bind_literal = |literal: &ScalarLiteral, kind: &AcceptedFieldKind| {
        bind_source_rule_literal(
            literal,
            kind.clone(),
            bindings,
            enum_catalog,
            composite_catalog,
        )
        .map_err(|_| InternalError::store_unsupported())
    };
    let operation = match proposed.operation() {
        SourceRuleOperation::LengthRangeInclusive { min, max }
            if accepted_rule_length_kind_is_supported(resolved_kind) =>
        {
            AcceptedRuleOperation::LengthRangeInclusive {
                min: *min,
                max: *max,
            }
        }
        SourceRuleOperation::NumericMinimumInclusive { value }
            if accepted_rule_numeric_kind_is_supported(resolved_kind)
                && source_rule_literal_is_exact_for_accepted_kind(value, resolved_kind) =>
        {
            AcceptedRuleOperation::NumericMinimumInclusive {
                value: bind_literal(value, resolved_kind)?,
            }
        }
        SourceRuleOperation::NumericMaximumInclusive { value }
            if accepted_rule_numeric_kind_is_supported(resolved_kind)
                && source_rule_literal_is_exact_for_accepted_kind(value, resolved_kind) =>
        {
            AcceptedRuleOperation::NumericMaximumInclusive {
                value: bind_literal(value, resolved_kind)?,
            }
        }
        SourceRuleOperation::NumericRangeInclusive { min, max }
            if accepted_rule_numeric_kind_is_supported(resolved_kind)
                && source_rule_literal_is_exact_for_accepted_kind(min, resolved_kind)
                && source_rule_literal_is_exact_for_accepted_kind(max, resolved_kind) =>
        {
            AcceptedRuleOperation::NumericRangeInclusive {
                min: bind_literal(min, resolved_kind)?,
                max: bind_literal(max, resolved_kind)?,
            }
        }
        SourceRuleOperation::MultipleOf { divisor }
            if accepted_rule_exact_numeric_kind_is_supported(resolved_kind)
                && source_rule_literal_is_exact_for_accepted_kind(divisor, resolved_kind) =>
        {
            AcceptedRuleOperation::MultipleOf {
                divisor: bind_literal(divisor, resolved_kind)?,
            }
        }
        SourceRuleOperation::LengthRangeInclusive { .. }
        | SourceRuleOperation::MultipleOf { .. }
        | SourceRuleOperation::NumericMaximumInclusive { .. }
        | SourceRuleOperation::NumericMinimumInclusive { .. }
        | SourceRuleOperation::NumericRangeInclusive { .. } => {
            return Err(InternalError::store_unsupported());
        }
    };
    Ok((
        AcceptedRuleTarget::new(root_field_id, target_type),
        operation,
    ))
}

fn source_rule_literal_is_exact_for_accepted_kind(
    literal: &ScalarLiteral,
    kind: &AcceptedFieldKind,
) -> bool {
    if let (ScalarLiteral::Decimal(value), AcceptedFieldKind::Decimal { scale }) = (literal, kind) {
        let value = value.normalize();
        return value.scale() <= *scale && value.scale_to_integer(*scale).is_some();
    }
    true
}

fn lower_existing_constraints(
    bundle: &AcceptedSchemaRevisionBundle,
    catalogs: &ExistingCatalogCandidate,
    entity: &EntityFragment,
    entity_tag: EntityTag,
    candidate: &PersistedSchemaSnapshot,
    accepted_snapshot: &PersistedSchemaSnapshot,
    bindings: &mut AcceptedSourceBindingCatalog,
) -> Result<AcceptedConstraintCatalog, InternalError> {
    ExistingConstraintLowering {
        bundle,
        catalogs,
        entity_tag,
        candidate,
        accepted_snapshot,
        bindings,
    }
    .lower(entity)
}

struct ExistingConstraintLowering<'a> {
    bundle: &'a AcceptedSchemaRevisionBundle,
    catalogs: &'a ExistingCatalogCandidate,
    entity_tag: EntityTag,
    candidate: &'a PersistedSchemaSnapshot,
    accepted_snapshot: &'a PersistedSchemaSnapshot,
    bindings: &'a mut AcceptedSourceBindingCatalog,
}

impl ExistingConstraintLowering<'_> {
    fn lower(
        &mut self,
        entity: &EntityFragment,
    ) -> Result<AcceptedConstraintCatalog, InternalError> {
        let mut catalog = self.accepted_snapshot.constraint_catalog().clone();
        let mut declared = BTreeSet::new();
        for proposed in entity.constraints() {
            let (next_catalog, constraint_id) = match proposed.kind() {
                icydb_schema::ConstraintFragmentKind::Check(expression) => {
                    self.lower_check(catalog, proposed, expression)?
                }
                icydb_schema::ConstraintFragmentKind::TargetedRule(rule) => {
                    self.lower_targeted_rule(catalog, proposed, rule)?
                }
            };
            catalog = next_catalog;
            declared.insert(constraint_id);
        }
        if !all_existing_generated_row_constraints_are_declared(self.accepted_snapshot, &declared) {
            return Err(InternalError::store_unsupported());
        }
        Ok(catalog)
    }

    fn lower_check(
        &mut self,
        catalog: AcceptedConstraintCatalog,
        proposed: &icydb_schema::ConstraintFragment,
        proposed_expression: &icydb_schema::SourceCheckExpr,
    ) -> Result<(AcceptedConstraintCatalog, ConstraintId), InternalError> {
        let expression = bind_source_check_expr(
            proposed_expression,
            self.entity_tag,
            self.bindings,
            self.candidate,
            &self.catalogs.enum_catalog,
            &self.catalogs.composite_catalog,
        )
        .map_err(|_| InternalError::store_unsupported())?;
        if let Some(constraint_id) = self
            .bindings
            .constraint(self.entity_tag, proposed.source_key())
        {
            let accepted = self
                .accepted_snapshot
                .constraints()
                .iter()
                .find(|constraint| constraint.id() == constraint_id)
                .ok_or_else(InternalError::store_invariant)?;
            if accepted.origin() != ConstraintOrigin::Generated
                || accepted.name() != proposed.name().as_str()
                || !matches!(
                    accepted.kind(),
                    AcceptedConstraintKind::Check {
                        expression: accepted_expression
                    } if accepted_expression.as_ref() == &expression
                )
            {
                return Err(InternalError::store_unsupported());
            }
            return Ok((catalog, constraint_id));
        }

        let catalog = catalog
            .with_added_check_activation(
                proposed.name().as_str().to_string(),
                ConstraintOrigin::Generated,
                expression,
                self.bundle.semantic_fingerprint()?,
                self.activation_epoch()?,
            )
            .map_err(|_| InternalError::store_unsupported())?;
        let constraint_id = self.bind_new_constraint(&catalog, proposed)?;
        Ok((catalog, constraint_id))
    }

    fn lower_targeted_rule(
        &mut self,
        catalog: AcceptedConstraintCatalog,
        proposed: &icydb_schema::ConstraintFragment,
        proposed_rule: &TargetedRuleFragment,
    ) -> Result<(AcceptedConstraintCatalog, ConstraintId), InternalError> {
        let (target, operation) = bind_targeted_rule(
            proposed_rule,
            self.entity_tag,
            self.bindings,
            self.candidate,
            &self.catalogs.enum_catalog,
            &self.catalogs.composite_catalog,
        )?;
        if let Some(constraint_id) = self
            .bindings
            .constraint(self.entity_tag, proposed.source_key())
        {
            let accepted = self
                .accepted_snapshot
                .constraints()
                .iter()
                .find(|constraint| constraint.id() == constraint_id);
            let activation_matches = self
                .accepted_snapshot
                .constraint_activations()
                .iter()
                .find(|activation| activation.id() == constraint_id)
                .is_some_and(|activation| {
                    activation.origin() == ConstraintOrigin::Generated
                        && activation.name() == proposed.name().as_str()
                        && matches!(
                            activation.kind(),
                            ConstraintActivationKind::TargetedRule {
                                target: accepted_target,
                                operation: accepted_operation,
                            } if *accepted_target == target
                                && accepted_operation.as_ref() == &operation
                        )
                });
            if activation_matches {
                return Ok((catalog, constraint_id));
            }
            let Some(accepted) = accepted else {
                return Err(InternalError::store_unsupported());
            };
            let AcceptedConstraintKind::TargetedRule {
                target: accepted_target,
                operation: accepted_operation,
            } = accepted.kind()
            else {
                return Err(InternalError::store_unsupported());
            };
            if accepted.origin() != ConstraintOrigin::Generated
                || accepted.name() != proposed.name().as_str()
                || *accepted_target != target
            {
                return Err(InternalError::store_unsupported());
            }
            if accepted_operation.as_ref() == &operation {
                return Ok((catalog, constraint_id));
            }
            let catalog = catalog
                .with_replaced_targeted_rule_activation(
                    constraint_id,
                    target,
                    operation,
                    self.bundle.semantic_fingerprint()?,
                    self.activation_epoch()?,
                )
                .map_err(|_| InternalError::store_unsupported())?;
            return Ok((catalog, constraint_id));
        }

        let catalog = catalog
            .with_added_targeted_rule_activation(
                proposed.name().as_str().to_string(),
                ConstraintOrigin::Generated,
                target,
                operation,
                self.bundle.semantic_fingerprint()?,
                self.activation_epoch()?,
            )
            .map_err(|_| InternalError::store_unsupported())?;
        let constraint_id = self.bind_new_constraint(&catalog, proposed)?;
        Ok((catalog, constraint_id))
    }

    fn bind_new_constraint(
        &mut self,
        catalog: &AcceptedConstraintCatalog,
        proposed: &icydb_schema::ConstraintFragment,
    ) -> Result<ConstraintId, InternalError> {
        let constraint_id = ConstraintId::new(catalog.allocator().high_water())
            .ok_or_else(InternalError::store_invariant)?;
        self.bindings.insert_constraint(
            self.entity_tag,
            proposed.source_key().clone(),
            constraint_id,
        )?;
        Ok(constraint_id)
    }

    fn activation_epoch(&self) -> Result<u64, InternalError> {
        Ok(self
            .bundle
            .revision()
            .checked_next()
            .ok_or_else(InternalError::store_unsupported)?
            .get())
    }
}

fn all_existing_generated_row_constraints_are_declared(
    snapshot: &PersistedSchemaSnapshot,
    declared: &BTreeSet<ConstraintId>,
) -> bool {
    snapshot
        .constraints()
        .iter()
        .filter(|constraint| {
            constraint.origin() == ConstraintOrigin::Generated
                && matches!(
                    constraint.kind(),
                    AcceptedConstraintKind::Check { .. }
                        | AcceptedConstraintKind::TargetedRule { .. }
                )
        })
        .all(|constraint| declared.contains(&constraint.id()))
}

fn allocate_entity_identities(
    entities_by_store: &BTreeMap<&'static str, Vec<&EntityFragment>>,
) -> Result<BTreeMap<EntitySourceKey, EntityTag>, InternalError> {
    let mut accepted = BTreeMap::new();
    let mut next = 1u64;
    for entities in entities_by_store.values() {
        for entity in entities {
            let entity_tag = EntityTag::new(next);
            next = next
                .checked_add(1)
                .ok_or_else(InternalError::store_unsupported)?;
            if accepted
                .insert(entity.source_key().clone(), entity_tag)
                .is_some()
            {
                return Err(InternalError::store_unsupported());
            }
        }
    }
    Ok(accepted)
}

fn lower_initial_store(
    store_path: &'static str,
    entities: &[&EntityFragment],
    assignments: &BTreeMap<EntitySourceKey, &'static str>,
    all_entities: &BTreeMap<EntitySourceKey, &EntityFragment>,
    accepted_entities: &BTreeMap<EntitySourceKey, EntityTag>,
    types: &BTreeMap<TypeSourceKey, &NamedTypeFragment>,
) -> Result<CandidateSchemaRevision, InternalError> {
    let named_types = lower_initial_named_types(entities, types)?;
    let context = InitialStoreContext {
        store_path,
        assignments,
        all_entities,
        accepted_entities,
        enum_catalog: &named_types.enum_catalog,
        composite_catalog: &named_types.composite_catalog,
        named_type_bindings: named_types.bindings,
    };
    let mut bindings = context.named_type_bindings.clone();
    let snapshots = lower_new_entity_snapshots(&context, entities, &mut bindings)?;
    let bundle = AcceptedSchemaRevisionBundle::new_with_source_bindings(
        AcceptedSchemaRevision::INITIAL,
        store_path,
        named_types.enum_catalog,
        named_types.composite_catalog,
        bindings,
        snapshots,
    )?;
    CandidateSchemaRevision::new(bundle)
}

/// Installation and additive reconciliation share one empty-entity constructor.
/// Allocate all fields before lowering relations, so same-proposal entities can
/// reference one another without depending on declaration order.
fn lower_new_entity_snapshots(
    context: &InitialStoreContext<'_>,
    entities: &[&EntityFragment],
    bindings: &mut AcceptedSourceBindingCatalog,
) -> Result<BTreeMap<EntityTag, PersistedSchemaSnapshot>, InternalError> {
    let mut entity_bindings = BTreeMap::new();
    let mut field_bindings = BTreeMap::new();
    let mut provisional = BTreeMap::new();
    for entity in entities {
        let tag = context
            .accepted_entities
            .get(entity.source_key())
            .copied()
            .ok_or_else(InternalError::store_invariant)?;
        entity_bindings.insert(entity.source_key().clone(), tag);
        provisional.insert(
            tag,
            lower_initial_entity_fields(context, entity, tag, &mut field_bindings)?,
        );
    }
    let mut partial_bindings = bindings.clone();
    partial_bindings.append_new_entities(AcceptedSourceBindingCatalog::initial(
        entity_bindings.clone(),
        field_bindings.clone(),
        BTreeMap::new(),
        BTreeMap::new(),
        BTreeMap::new(),
    ))?;
    let mut objects = InitialObjectBindings::default();
    let mut snapshots = BTreeMap::new();
    for entity in entities {
        let tag = partial_bindings
            .entity(entity.source_key())
            .ok_or_else(InternalError::store_invariant)?;
        let initial = provisional
            .get(&tag)
            .ok_or_else(InternalError::store_invariant)?;
        snapshots.insert(
            tag,
            lower_initial_complete_snapshot(
                context,
                entity,
                tag,
                initial,
                &partial_bindings,
                &mut objects,
            )?,
        );
    }
    bindings.append_new_entities(AcceptedSourceBindingCatalog::initial(
        entity_bindings,
        field_bindings,
        objects.constraints,
        objects.indexes,
        objects.relations,
    ))?;
    Ok(snapshots)
}

fn lower_initial_complete_snapshot(
    context: &InitialStoreContext<'_>,
    entity: &EntityFragment,
    entity_tag: EntityTag,
    initial: &PersistedSchemaSnapshot,
    bindings: &AcceptedSourceBindingCatalog,
    object_bindings: &mut InitialObjectBindings,
) -> Result<PersistedSchemaSnapshot, InternalError> {
    let indexes = lower_initial_indexes(
        context,
        entity,
        entity_tag,
        initial,
        bindings,
        &mut object_bindings.indexes,
    )?;
    let relations = lower_initial_relations(
        context,
        entity,
        entity_tag,
        bindings,
        &mut object_bindings.relations,
    )?;
    let snapshot = PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        initial.version(),
        initial.entity_path().to_string(),
        initial.entity_name().to_string(),
        initial.primary_key_field_ids().to_vec(),
        initial.row_layout().clone(),
        initial.fields().to_vec(),
        indexes,
    )
    .with_relations(relations);
    lower_initial_constraints(
        context,
        entity,
        entity_tag,
        bindings,
        snapshot,
        &mut object_bindings.constraints,
    )
}

fn lower_initial_constraints(
    context: &InitialStoreContext<'_>,
    entity: &EntityFragment,
    entity_tag: EntityTag,
    bindings: &AcceptedSourceBindingCatalog,
    snapshot: PersistedSchemaSnapshot,
    accepted_bindings: &mut BTreeMap<(EntityTag, icydb_schema::ConstraintSourceKey), ConstraintId>,
) -> Result<PersistedSchemaSnapshot, InternalError> {
    let mut catalog = AcceptedConstraintCatalog::initial(
        snapshot.fields(),
        snapshot.indexes(),
        snapshot.relations(),
    )
    .map_err(|_| InternalError::store_unsupported())?;
    for constraint in entity.constraints() {
        match constraint.kind() {
            icydb_schema::ConstraintFragmentKind::Check(source_expression) => {
                let expression = bind_source_check_expr(
                    source_expression,
                    entity_tag,
                    bindings,
                    &snapshot,
                    context.enum_catalog,
                    context.composite_catalog,
                )
                .map_err(|_| InternalError::store_unsupported())?;
                catalog = catalog
                    .with_added_check(
                        constraint.name().as_str().to_string(),
                        ConstraintOrigin::Generated,
                        expression,
                    )
                    .map_err(|_| InternalError::store_unsupported())?;
            }
            icydb_schema::ConstraintFragmentKind::TargetedRule(source_rule) => {
                let (target, operation) = bind_targeted_rule(
                    source_rule,
                    entity_tag,
                    bindings,
                    &snapshot,
                    context.enum_catalog,
                    context.composite_catalog,
                )?;
                catalog = catalog
                    .with_added_targeted_rule(
                        constraint.name().as_str().to_string(),
                        ConstraintOrigin::Generated,
                        target,
                        operation,
                    )
                    .map_err(|_| InternalError::store_unsupported())?;
            }
        }
        let id = ConstraintId::new(catalog.allocator().high_water())
            .ok_or_else(InternalError::store_invariant)?;
        accepted_bindings.insert((entity_tag, constraint.source_key().clone()), id);
    }
    Ok(snapshot.with_constraint_catalog(catalog))
}

fn lower_initial_entity_fields(
    context: &InitialStoreContext<'_>,
    entity: &EntityFragment,
    entity_tag: EntityTag,
    bindings: &mut BTreeMap<(EntityTag, FieldSourceKey), FieldId>,
) -> Result<PersistedSchemaSnapshot, InternalError> {
    let mut fields = Vec::with_capacity(entity.fields().len());
    let mut layout = Vec::with_capacity(entity.fields().len());
    for (offset, field) in entity.fields().iter().enumerate() {
        let raw_id = u32::try_from(offset)
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or_else(InternalError::store_unsupported)?;
        let raw_slot = u16::try_from(offset).map_err(|_| InternalError::store_unsupported())?;
        let id = FieldId::new(raw_id);
        let slot = SchemaFieldSlot::new(raw_slot);
        fields.push(
            AcceptedFieldLowering {
                bindings: &context.named_type_bindings,
                enum_catalog: context.enum_catalog,
                composite_catalog: context.composite_catalog,
            }
            .lower_field(
                field,
                id,
                slot,
                RowLayoutVersion::INITIAL,
                SchemaHistoricalFill::Reject,
            )?,
        );
        layout.push((id, slot));
        bindings.insert((entity_tag, field.source_key().clone()), id);
    }
    let primary_key = entity
        .primary_key()
        .iter()
        .map(|source| {
            bindings
                .get(&(entity_tag, source.clone()))
                .copied()
                .ok_or_else(InternalError::store_invariant)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(
        PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
            SchemaVersion::initial(),
            entity.source_key().as_str().to_string(),
            entity.name().as_str().to_string(),
            primary_key,
            SchemaRowLayout::single_version(RowLayoutVersion::INITIAL, layout),
            fields,
            Vec::new(),
        ),
    )
}

pub(in crate::db::schema) fn lower_migration_nested_leaves(
    kind: &AcceptedFieldKind,
    catalog: &AcceptedCompositeCatalog,
) -> Result<Vec<PersistedNestedLeafSnapshot>, InternalError> {
    let AcceptedFieldKind::Composite { type_id } = kind else {
        return Ok(Vec::new());
    };
    let Some(definition) = catalog.composite_type(*type_id) else {
        return Err(InternalError::store_invariant());
    };
    let AcceptedCompositeShape::Record(fields) = definition.shape() else {
        return Ok(Vec::new());
    };
    let mut leaves = Vec::new();
    let mut active = BTreeSet::from([*type_id]);
    for field in fields {
        push_nested_leaves(
            field.name(),
            field.contract(),
            catalog,
            &mut Vec::new(),
            &mut leaves,
            &mut active,
            0,
        )?;
    }
    icydb_schema::compact_sort_unstable_by(&mut leaves, |left, right| {
        left.path().cmp(right.path())
    });
    Ok(leaves)
}

fn push_nested_leaves(
    name: &str,
    contract: &AcceptedCompositeElement,
    catalog: &AcceptedCompositeCatalog,
    path: &mut Vec<String>,
    leaves: &mut Vec<PersistedNestedLeafSnapshot>,
    active: &mut BTreeSet<CompositeTypeId>,
    depth: usize,
) -> Result<(), InternalError> {
    // The snapshot codec caps nested leaves per field. Enforce that same cap
    // before cloning paths/kinds, including across reused sibling records.
    if depth >= MAX_ACCEPTED_RECURSIVE_DEPTH || leaves.len() >= icydb_schema::MAX_FRAGMENT_FIELDS {
        return Err(InternalError::store_unsupported());
    }
    path.push(name.to_string());
    leaves.push(PersistedNestedLeafSnapshot::new(
        path.clone(),
        contract.kind().clone(),
        contract.nullable(),
    ));
    if let AcceptedFieldKind::Composite { type_id } = contract.kind() {
        let definition = catalog
            .composite_type(*type_id)
            .ok_or_else(InternalError::store_invariant)?;
        if let AcceptedCompositeShape::Record(fields) = definition.shape()
            && active.insert(*type_id)
        {
            for field in fields {
                push_nested_leaves(
                    field.name(),
                    field.contract(),
                    catalog,
                    path,
                    leaves,
                    active,
                    depth.saturating_add(1),
                )?;
            }
            active.remove(type_id);
        }
    }
    path.pop();
    Ok(())
}

fn lower_initial_indexes(
    context: &InitialStoreContext<'_>,
    entity: &EntityFragment,
    entity_tag: EntityTag,
    snapshot: &PersistedSchemaSnapshot,
    bindings: &AcceptedSourceBindingCatalog,
    accepted_bindings: &mut BTreeMap<(EntityTag, icydb_schema::IndexSourceKey), SchemaIndexId>,
) -> Result<Vec<PersistedIndexSnapshot>, InternalError> {
    entity
        .indexes()
        .iter()
        .enumerate()
        .map(|(offset, index)| {
            let raw_id = u32::try_from(offset)
                .ok()
                .and_then(|value| value.checked_add(1))
                .ok_or_else(InternalError::store_unsupported)?;
            let ordinal = u16::try_from(offset)
                .ok()
                .and_then(|value| value.checked_add(1))
                .ok_or_else(InternalError::store_unsupported)?;
            let id = SchemaIndexId::new(raw_id).ok_or_else(InternalError::store_unsupported)?;
            let accepted = IndexLowering {
                enum_catalog: context.enum_catalog,
                composite_catalog: context.composite_catalog,
                entity_tag,
                snapshot,
                bindings,
            }
            .lower_new(index, id, ordinal, context.store_path)?;
            accepted_bindings.insert((entity_tag, index.source_key().clone()), id);
            Ok(accepted)
        })
        .collect()
}

fn lower_index_key(
    key: &[IndexKeyFragment],
    entity_tag: EntityTag,
    snapshot: &PersistedSchemaSnapshot,
    bindings: &AcceptedSourceBindingCatalog,
) -> Result<PersistedIndexKeySnapshot, InternalError> {
    // Reject unsupported proposal width before resolving or copying components.
    // Initial proposals and migrations share this storage-domain boundary.
    if !(1..=MAX_INDEX_FIELDS).contains(&key.len()) {
        return Err(InternalError::store_unsupported());
    }
    let items = key
        .iter()
        .map(|component| {
            let field_id = bindings
                .field(entity_tag, component.field())
                .ok_or_else(InternalError::store_invariant)?;
            let field = snapshot
                .fields()
                .iter()
                .find(|field| field.id() == field_id)
                .ok_or_else(InternalError::store_invariant)?;
            let source = PersistedIndexFieldPathSnapshot::new(
                field.id(),
                field.slot(),
                vec![field.name().to_string()],
                field.kind().clone(),
                field.nullable(),
            );
            if let IndexKeyFragment::Field(_) = component {
                Ok::<_, InternalError>(PersistedIndexKeyItemSnapshot::FieldPath(source))
            } else {
                let op =
                    index_expression_op(component).ok_or_else(InternalError::store_invariant)?;
                let output_kind = index_expression_output_kind(op, field.kind())
                    .ok_or_else(InternalError::store_unsupported)?;
                Ok(PersistedIndexKeyItemSnapshot::Expression(Box::new(
                    PersistedIndexExpressionSnapshot::new(
                        op,
                        source,
                        field.kind().clone(),
                        output_kind,
                        index_expression_text(op, field.name()),
                    ),
                )))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(PersistedIndexKeySnapshot::new(items))
}

fn lower_initial_relations(
    context: &InitialStoreContext<'_>,
    entity: &EntityFragment,
    entity_tag: EntityTag,
    bindings: &AcceptedSourceBindingCatalog,
    accepted_bindings: &mut BTreeMap<(EntityTag, icydb_schema::RelationSourceKey), RelationId>,
) -> Result<Vec<PersistedRelationEdgeSnapshot>, InternalError> {
    entity
        .relations()
        .iter()
        .enumerate()
        .map(|(offset, relation)| {
            let raw_id = u32::try_from(offset)
                .ok()
                .and_then(|value| value.checked_add(1))
                .ok_or_else(InternalError::store_unsupported)?;
            let id = RelationId::new(raw_id).ok_or_else(InternalError::store_unsupported)?;
            let target = context
                .all_entities
                .get(relation.target_entity())
                .copied()
                .ok_or_else(InternalError::store_unsupported)?;
            if target.primary_key() != relation.target_fields() {
                return Err(InternalError::store_unsupported());
            }
            if !context
                .accepted_entities
                .contains_key(relation.target_entity())
                || !context.assignments.contains_key(relation.target_entity())
            {
                return Err(InternalError::store_invariant());
            }
            let source = lower_relation_source(relation.source(), entity_tag, bindings)?;
            accepted_bindings.insert((entity_tag, relation.source_key().clone()), id);
            Ok(relation_edge_from_source(
                id,
                relation.name().as_str().to_string(),
                target.source_key().as_str().to_string(),
                source,
            ))
        })
        .collect()
}

pub(in crate::db::schema) fn relation_edge_from_source(
    id: RelationId,
    name: String,
    target_path: String,
    source: PersistedRelationSourceSnapshot,
) -> PersistedRelationEdgeSnapshot {
    match source {
        PersistedRelationSourceSnapshot::Direct { field_ids } => {
            PersistedRelationEdgeSnapshot::new_direct(id, name, target_path, field_ids)
        }
        PersistedRelationSourceSnapshot::Nested {
            root_field_id,
            steps,
        } => PersistedRelationEdgeSnapshot::new_nested(id, name, target_path, root_field_id, steps),
    }
}

pub(in crate::db::schema) fn lower_relation_source(
    source: &RelationSourceFragment,
    entity_tag: EntityTag,
    bindings: &AcceptedSourceBindingCatalog,
) -> Result<PersistedRelationSourceSnapshot, InternalError> {
    let RelationSourceFragment::Nested { root, steps } = source else {
        let RelationSourceFragment::Direct { fields } = source else {
            return Err(InternalError::store_invariant());
        };
        return fields
            .iter()
            .map(|field| {
                bindings
                    .field(entity_tag, field)
                    .ok_or_else(InternalError::store_unsupported)
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|field_ids| PersistedRelationSourceSnapshot::Direct { field_ids });
    };

    let root_field_id = bindings
        .field(entity_tag, root)
        .ok_or_else(InternalError::store_unsupported)?;
    let mut accepted_steps = Vec::with_capacity(steps.len());
    for step in steps {
        accepted_steps.push(match step {
            RelationPathStepFragment::OptionalSome => {
                PersistedRelationPathStepSnapshot::OptionalSome
            }
            RelationPathStepFragment::EnterNamed { .. } => {
                PersistedRelationPathStepSnapshot::EnterNamed
            }
            RelationPathStepFragment::RecordMember { record, field } => {
                let AcceptedNamedTypeIdentity::Composite(type_id) = bindings
                    .named_type(record)
                    .ok_or_else(InternalError::store_unsupported)?
                else {
                    return Err(InternalError::store_unsupported());
                };
                let member_id = bindings
                    .composite_field(type_id, field)
                    .ok_or_else(InternalError::store_unsupported)?;
                PersistedRelationPathStepSnapshot::RecordMember {
                    composite_type_id: type_id,
                    member_id,
                }
            }
            RelationPathStepFragment::EnumVariantPayload { r#enum, variant } => {
                let AcceptedNamedTypeIdentity::Enum(type_id) = bindings
                    .named_type(r#enum)
                    .ok_or_else(InternalError::store_unsupported)?
                else {
                    return Err(InternalError::store_unsupported());
                };
                let variant_id = bindings
                    .enum_variant(type_id, variant)
                    .ok_or_else(InternalError::store_unsupported)?;
                PersistedRelationPathStepSnapshot::EnumVariantPayload {
                    enum_type_id: type_id,
                    variant_id,
                }
            }
            RelationPathStepFragment::ListItems => PersistedRelationPathStepSnapshot::ListItems,
            RelationPathStepFragment::SetItems => PersistedRelationPathStepSnapshot::SetItems,
            RelationPathStepFragment::MapValues => PersistedRelationPathStepSnapshot::MapValues,
        });
    }
    Ok(PersistedRelationSourceSnapshot::Nested {
        root_field_id,
        steps: accepted_steps,
    })
}

fn lower_write_policy(
    insert: &FieldInsertPolicy,
    management: Option<FieldManagementPolicy>,
    kind: &AcceptedFieldKind,
) -> Result<SchemaFieldWritePolicy, InternalError> {
    let insert_generation = match insert {
        FieldInsertPolicy::Generated
            if matches!(
                kind,
                AcceptedFieldKind::Nat8
                    | AcceptedFieldKind::Nat16
                    | AcceptedFieldKind::Nat32
                    | AcceptedFieldKind::Nat64
                    | AcceptedFieldKind::Nat128
            ) =>
        {
            Some(FieldInsertGeneration::Identity)
        }
        FieldInsertPolicy::Generated if matches!(kind, AcceptedFieldKind::Ulid) => {
            Some(FieldInsertGeneration::Ulid)
        }
        FieldInsertPolicy::Generated if matches!(kind, AcceptedFieldKind::Timestamp) => {
            Some(FieldInsertGeneration::Timestamp)
        }
        FieldInsertPolicy::Generated => return Err(InternalError::store_unsupported()),
        FieldInsertPolicy::Required
        | FieldInsertPolicy::Nullable
        | FieldInsertPolicy::Default(_) => None,
    };
    let write_management = match management {
        Some(FieldManagementPolicy::CreatedAt) => Some(FieldWriteManagement::CreatedAt),
        Some(FieldManagementPolicy::UpdatedAt) => Some(FieldWriteManagement::UpdatedAt),
        None => None,
    };
    Ok(SchemaFieldWritePolicy::from_model_policies(
        insert_generation,
        write_management,
    ))
}

fn lower_initial_field_type(
    field_type: &FieldType,
    bindings: &BTreeMap<TypeSourceKey, AcceptedNamedTypeIdentity>,
) -> Result<AcceptedFieldKind, InternalError> {
    lower_field_type_with_lookup(field_type, NamedTypeIdentityLookup::Initial(bindings))
}

pub(in crate::db) fn lower_field_type(
    field_type: &FieldType,
    bindings: &AcceptedSourceBindingCatalog,
) -> Result<AcceptedFieldKind, InternalError> {
    lower_field_type_with_lookup(field_type, NamedTypeIdentityLookup::Accepted(bindings))
}

fn lower_field_type_with_lookup(
    field_type: &FieldType,
    named_types: NamedTypeIdentityLookup<'_>,
) -> Result<AcceptedFieldKind, InternalError> {
    let scalar = match field_type {
        FieldType::Scalar(scalar) => scalar,
        FieldType::List(item) => {
            return Ok(AcceptedFieldKind::List(Box::new(
                lower_field_type_with_lookup(item, named_types)?,
            )));
        }
        FieldType::Named(source) => {
            return named_types
                .resolve(source)
                .map(|identity| match identity {
                    AcceptedNamedTypeIdentity::Enum(type_id) => AcceptedFieldKind::Enum { type_id },
                    AcceptedNamedTypeIdentity::Composite(type_id) => {
                        AcceptedFieldKind::Composite { type_id }
                    }
                })
                .ok_or_else(InternalError::store_unsupported);
        }
    };
    Ok(lower_scalar_type(scalar))
}

pub(in crate::db) const fn lower_scalar_type(scalar: &ScalarType) -> AcceptedFieldKind {
    match scalar {
        ScalarType::Account => AcceptedFieldKind::Account,
        ScalarType::Blob { max_len } => AcceptedFieldKind::Blob { max_len: *max_len },
        ScalarType::Bool => AcceptedFieldKind::Bool,
        ScalarType::Date => AcceptedFieldKind::Date,
        ScalarType::Decimal { scale } => AcceptedFieldKind::Decimal { scale: *scale },
        ScalarType::Duration => AcceptedFieldKind::Duration,
        ScalarType::Float32 => AcceptedFieldKind::Float32,
        ScalarType::Float64 => AcceptedFieldKind::Float64,
        ScalarType::Int8 => AcceptedFieldKind::Int8,
        ScalarType::Int16 => AcceptedFieldKind::Int16,
        ScalarType::Int32 => AcceptedFieldKind::Int32,
        ScalarType::Int64 => AcceptedFieldKind::Int64,
        ScalarType::Int128 => AcceptedFieldKind::Int128,
        ScalarType::Principal => AcceptedFieldKind::Principal,
        ScalarType::Subaccount => AcceptedFieldKind::Subaccount,
        ScalarType::Text { max_len } => AcceptedFieldKind::Text { max_len: *max_len },
        ScalarType::Timestamp => AcceptedFieldKind::Timestamp,
        ScalarType::Nat8 => AcceptedFieldKind::Nat8,
        ScalarType::Nat16 => AcceptedFieldKind::Nat16,
        ScalarType::Nat32 => AcceptedFieldKind::Nat32,
        ScalarType::Nat64 => AcceptedFieldKind::Nat64,
        ScalarType::Nat128 => AcceptedFieldKind::Nat128,
        ScalarType::Ulid => AcceptedFieldKind::Ulid,
        ScalarType::Unit => AcceptedFieldKind::Unit,
        ScalarType::U256 => AcceptedFieldKind::U256,
    }
}

const fn field_storage_decode(field_type: &FieldType) -> FieldStorageDecode {
    match field_type {
        FieldType::List(item) => field_storage_decode(item),
        FieldType::Named(_) => FieldStorageDecode::CatalogValue,
        FieldType::Scalar(_) => FieldStorageDecode::ByKind,
    }
}

const fn field_leaf_codec(field_type: &FieldType, kind: &AcceptedFieldKind) -> LeafCodec {
    kind.leaf_codec_for_storage(field_storage_decode(field_type))
}

const fn index_expression_op(component: &IndexKeyFragment) -> Option<PersistedIndexExpressionOp> {
    match component {
        IndexKeyFragment::Lower(_) => Some(PersistedIndexExpressionOp::Lower),
        IndexKeyFragment::Upper(_) => Some(PersistedIndexExpressionOp::Upper),
        IndexKeyFragment::Trim(_) => Some(PersistedIndexExpressionOp::Trim),
        IndexKeyFragment::LowerTrim(_) => Some(PersistedIndexExpressionOp::LowerTrim),
        IndexKeyFragment::Date(_) => Some(PersistedIndexExpressionOp::Date),
        IndexKeyFragment::Year(_) => Some(PersistedIndexExpressionOp::Year),
        IndexKeyFragment::Month(_) => Some(PersistedIndexExpressionOp::Month),
        IndexKeyFragment::Day(_) => Some(PersistedIndexExpressionOp::Day),
        IndexKeyFragment::Field(_) => None,
    }
}

#[cfg(test)]
mod tests;
