//! Module: db::schema::application_lowering
//! Responsibility: lower source-keyed initial proposals into accepted catalog candidates.
//! Does not own: optimistic admission, durable receipts, publication, or activation progress.
//! Boundary: validated public proposal plus target-store routing -> catalog-native candidates.

use std::collections::{BTreeMap, BTreeSet};

use icydb_schema::{
    ConstraintSourceKey, EntityFragment, EntitySourceKey, FieldInsertPolicy, FieldManagementPolicy,
    FieldSourceKey, FieldType, IndexKeyFragment, IndexSourceKey, NamedTypeFragment,
    RelationSourceKey, ScalarLiteral, ScalarType, SchemaProposal, SchemaRemoval,
    SourceRuleOperation, TargetStoreIdentity, TargetedRuleFragment, TypeSourceKey,
};

use crate::{
    db::{
        data::encode_input_value_for_candidate_field_contract,
        schema::{
            AcceptedConstraintCatalog, AcceptedConstraintKind, AcceptedEnumCatalog,
            AcceptedFieldDecodeContract, AcceptedFieldKind, AcceptedNamedTypeIdentity,
            AcceptedRuleOperation, AcceptedRuleTarget, AcceptedSchemaFingerprint,
            AcceptedSchemaRevision, AcceptedSchemaRevisionBundle, AcceptedSourceBindingCatalog,
            AcceptedStoreCatalogScope, AcceptedValueCatalogHandle, CandidateSchemaRevision,
            ConstraintActivationKind, ConstraintId, ConstraintOrigin, FieldId,
            FieldInsertGeneration, FieldStorageDecode, FieldWriteManagement, LeafCodec,
            MAX_ACCEPTED_RECURSIVE_DEPTH, PersistedFieldOrigin, PersistedFieldSnapshot,
            PersistedIndexExpressionOp, PersistedIndexExpressionSnapshot,
            PersistedIndexFieldPathSnapshot, PersistedIndexKeyItemSnapshot,
            PersistedIndexKeySnapshot, PersistedIndexSnapshot, PersistedNestedLeafSnapshot,
            PersistedRelationEdgeSnapshot, PersistedSchemaSnapshot, RelationId, RowLayoutVersion,
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
            derive_relation_removal_candidate, render_accepted_check_expr_sql,
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
    enum_catalog: AcceptedEnumCatalog,
    composite_catalog: AcceptedCompositeCatalog,
    named_type_bindings: AcceptedSourceBindingCatalog,
    value_catalog: AcceptedValueCatalogHandle,
}

impl<'a> InitialStoreContext<'a> {
    fn new(
        store_path: &'static str,
        assignments: &'a BTreeMap<EntitySourceKey, &'static str>,
        all_entities: &'a BTreeMap<EntitySourceKey, &'a EntityFragment>,
        accepted_entities: &'a BTreeMap<EntitySourceKey, EntityTag>,
        entities: &[&EntityFragment],
        types: &BTreeMap<TypeSourceKey, &'a NamedTypeFragment>,
    ) -> Result<Self, InternalError> {
        let named_types = lower_initial_named_types(entities, types)?;
        // Rendering index predicates consumes only the value catalogs. The
        // unpublished authority identity cannot enter the candidate and is
        // replaced by the bundle's computed fingerprint.
        let value_catalog = AcceptedValueCatalogHandle::new(
            named_types.enum_catalog.clone(),
            named_types.composite_catalog.clone(),
            AcceptedStoreCatalogScope::new(),
            AcceptedSchemaRevision::INITIAL,
            AcceptedSchemaFingerprint::new([1; 32]),
        );
        Ok(Self {
            store_path,
            assignments,
            all_entities,
            accepted_entities,
            enum_catalog: named_types.enum_catalog,
            composite_catalog: named_types.composite_catalog,
            named_type_bindings: named_types.bindings,
            value_catalog,
        })
    }
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

/// Accepted catalogs required to lower one future insert-default policy.
#[derive(Clone, Copy)]
struct AcceptedDefaultLowering<'a> {
    bindings: &'a AcceptedSourceBindingCatalog,
    enum_catalog: &'a AcceptedEnumCatalog,
    composite_catalog: &'a AcceptedCompositeCatalog,
}

impl AcceptedDefaultLowering<'_> {
    fn lower(
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

type InitialEnumVariantBindings = BTreeMap<(EnumTypeId, TypeSourceKey), EnumVariantId>;
type InitialCompositeFieldBindings = BTreeMap<(CompositeTypeId, FieldSourceKey), CompositeFieldId>;

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
            collect_field_type_dependency(field.field_type(), &mut pending);
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
        collect_named_type_dependencies(definition, &mut pending);
    }
    Ok(reachable)
}

fn collect_named_type_dependencies(
    definition: &NamedTypeFragment,
    pending: &mut Vec<TypeSourceKey>,
) {
    match definition {
        NamedTypeFragment::Record(record) => {
            for field in record.fields() {
                collect_field_type_dependency(field.field_type(), pending);
            }
        }
        NamedTypeFragment::Enum(r#enum) => {
            for variant in r#enum.variants() {
                if let Some(payload) = variant.payload() {
                    collect_field_type_dependency(payload, pending);
                }
            }
        }
        NamedTypeFragment::Newtype { inner, .. }
        | NamedTypeFragment::List { item: inner, .. }
        | NamedTypeFragment::Set { item: inner, .. } => {
            collect_field_type_dependency(inner, pending);
        }
        NamedTypeFragment::Map { key, value, .. } => {
            collect_field_type_dependency(key, pending);
            collect_field_type_dependency(value, pending);
        }
        NamedTypeFragment::Tuple { members, .. } => {
            for member in members {
                collect_field_type_dependency(member.field_type(), pending);
            }
        }
    }
}

fn collect_field_type_dependency(field_type: &FieldType, pending: &mut Vec<TypeSourceKey>) {
    match field_type {
        FieldType::List(item) => collect_field_type_dependency(item, pending),
        FieldType::Named(source) => pending.push(source.clone()),
        FieldType::Scalar(_) => {}
    }
}

fn lower_initial_enum_catalog(
    types: &BTreeMap<TypeSourceKey, &NamedTypeFragment>,
    bindings: &BTreeMap<TypeSourceKey, AcceptedNamedTypeIdentity>,
) -> Result<(AcceptedEnumCatalog, InitialEnumVariantBindings), InternalError> {
    let mut definitions = BTreeMap::new();
    let mut variant_bindings = BTreeMap::new();
    for (source, identity) in bindings {
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
    let catalog = AcceptedEnumCatalog::from_initial_definitions(definitions)
        .map_err(|_| InternalError::store_unsupported())?;
    Ok((catalog, variant_bindings))
}

fn lower_initial_composite_catalog(
    types: &BTreeMap<TypeSourceKey, &NamedTypeFragment>,
    bindings: &BTreeMap<TypeSourceKey, AcceptedNamedTypeIdentity>,
    enum_catalog: &AcceptedEnumCatalog,
) -> Result<InitialCompositeTypes, InternalError> {
    let mut definitions = BTreeMap::new();
    let mut field_bindings = BTreeMap::new();
    for (source, identity) in bindings {
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
    let catalog = AcceptedCompositeCatalog::from_initial_definitions(definitions, enum_catalog)
        .map_err(|_| InternalError::store_unsupported())?;
    Ok(InitialCompositeTypes {
        catalog,
        field_bindings,
    })
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
    for (source, entity) in &entities {
        let path = assignments
            .get(source)
            .copied()
            .ok_or_else(InternalError::store_unsupported)?;
        entities_by_store.entry(path).or_default().push(*entity);
    }
    for store_entities in entities_by_store.values_mut() {
        icydb_schema::compact_sort_unstable_by(store_entities, |left, right| {
            left.source_key().cmp(right.source_key())
        });
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
    Ok(candidates)
}

/// Lower an exact proposal against a non-empty accepted head.
///
/// This existing-head lane owns future insert-default and source-keyed display
/// metadata reconciliation, plus explicit removal of accepted generated
/// entities, checks, fields, indexes, and unreferenced named types and addition
/// of generated checks whose complete historical domain is proven empty at the
/// application boundary. Every structural fact must resolve through immutable
/// source bindings and match accepted authority exactly. Other additions,
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

    let mut used_types = BTreeSet::new();
    let mut candidates = Vec::new();
    for (_, (store, store_entities)) in entities_by_store {
        let removals = removals_by_store.remove(store.path).unwrap_or_default();
        if let Some(candidate) = lower_existing_store_candidate(
            store,
            stores,
            store_entities,
            removals,
            &types,
            &mut used_types,
        )? {
            candidates.push(candidate);
        }
    }
    if used_types.len() != types.len() {
        return Err(InternalError::store_unsupported());
    }
    Ok(candidates)
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
    let mut used_types = BTreeSet::new();
    let mut candidates = Vec::new();
    for (_, (store, store_entities)) in entities_by_store {
        if let Some(candidate) = lower_generated_existing_store_candidate(
            store,
            stores,
            store_entities,
            &types,
            &mut used_types,
        )? {
            candidates.push(candidate);
        }
    }
    if used_types.len() != types.len() {
        return Err(InternalError::store_unsupported());
    }
    Ok(candidates)
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
        verify_unique_entity_binding(stores, assignment.entity(), store)?;
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
        Ok(Self {
            catalogs: lower_existing_named_catalogs(store.bundle, entities, types, used_types)?,
            snapshots: store.bundle.entity_snapshots().clone(),
            source_bindings: store.bundle.source_bindings().clone(),
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
        entities: Vec<&EntityFragment>,
        version_advanced: Option<&BTreeSet<EntityTag>>,
    ) -> Result<Option<CandidateSchemaRevision>, InternalError> {
        for entity in entities {
            let entity_tag = store
                .bundle
                .source_bindings()
                .entity(entity.source_key())
                .ok_or_else(InternalError::store_unsupported)?;
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
}

fn lower_existing_store_candidate(
    store: &ExistingProposalStore<'_>,
    stores: &[ExistingProposalStore<'_>],
    mut entities: Vec<&EntityFragment>,
    removals: ExistingStoreRemovals,
    types: &BTreeMap<TypeSourceKey, &NamedTypeFragment>,
    used_types: &mut BTreeSet<TypeSourceKey>,
) -> Result<Option<CandidateSchemaRevision>, InternalError> {
    icydb_schema::compact_sort_unstable_by(&mut entities, |left, right| {
        left.source_key().cmp(right.source_key())
    });
    let mut state = ExistingStoreCandidateState::new(store, &entities, types, used_types)?;
    let version_advanced = state.apply_removals(removals)?;
    state.finish(store, stores, entities, Some(&version_advanced))
}

fn lower_generated_existing_store_candidate(
    store: &ExistingProposalStore<'_>,
    stores: &[ExistingProposalStore<'_>],
    mut entities: Vec<&EntityFragment>,
    types: &BTreeMap<TypeSourceKey, &NamedTypeFragment>,
    used_types: &mut BTreeSet<TypeSourceKey>,
) -> Result<Option<CandidateSchemaRevision>, InternalError> {
    icydb_schema::compact_sort_unstable_by(&mut entities, |left, right| {
        left.source_key().cmp(right.source_key())
    });
    ExistingStoreCandidateState::new(store, &entities, types, used_types)?
        .finish(store, stores, entities, None)
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
    let mut resolved = None;
    for store in stores {
        let Some(entity_tag) = store.bundle.source_bindings().entity(entity_source) else {
            continue;
        };
        if resolved.is_some() {
            return Err(InternalError::store_unsupported());
        }
        let snapshot = store
            .bundle
            .entity_snapshots()
            .get(&entity_tag)
            .ok_or_else(InternalError::store_invariant)?;
        if !snapshot.constraint_activations().is_empty()
            || !snapshot.candidate_indexes().is_empty()
            || !snapshot.candidate_relations().is_empty()
        {
            return Err(InternalError::store_unsupported());
        }
        resolved = Some((
            store,
            ExistingEntityRemoval {
                source: entity_source.clone(),
                tag: entity_tag,
            },
        ));
    }
    resolved.ok_or_else(InternalError::store_unsupported)
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
    let mut resolved = None;
    for store in stores {
        let Some(entity_tag) = store.bundle.source_bindings().entity(entity_source) else {
            continue;
        };
        if resolved.is_some() {
            return Err(InternalError::store_unsupported());
        }
        let snapshot = store
            .bundle
            .entity_snapshots()
            .get(&entity_tag)
            .ok_or_else(InternalError::store_invariant)?;
        if !snapshot.constraint_activations().is_empty()
            || !snapshot.candidate_indexes().is_empty()
            || !snapshot.candidate_relations().is_empty()
        {
            return Err(InternalError::store_unsupported());
        }
        let constraint_id = store
            .bundle
            .source_bindings()
            .constraint(entity_tag, constraint_source)
            .ok_or_else(InternalError::store_unsupported)?;
        let Some(constraint) = snapshot
            .constraints()
            .iter()
            .find(|constraint| constraint.id() == constraint_id)
        else {
            return if snapshot
                .constraint_activations()
                .iter()
                .any(|activation| activation.id() == constraint_id)
            {
                Err(InternalError::store_unsupported())
            } else {
                Err(InternalError::store_invariant())
            };
        };
        if constraint.origin() != ConstraintOrigin::Generated
            || !matches!(
                constraint.kind(),
                AcceptedConstraintKind::Check { .. } | AcceptedConstraintKind::TargetedRule { .. }
            )
        {
            return Err(InternalError::store_unsupported());
        }
        resolved = Some((
            store,
            ExistingConstraintRemoval {
                entity_tag,
                source: constraint_source.clone(),
                id: constraint_id,
            },
        ));
    }
    resolved.ok_or_else(InternalError::store_unsupported)
}

/// Resolve one generated field removal solely through immutable accepted
/// source identity.
fn resolve_existing_generated_field_removal<'store, 'bundle>(
    stores: &'store [ExistingProposalStore<'bundle>],
    entity_source: &EntitySourceKey,
    field_source: &FieldSourceKey,
) -> Result<(&'store ExistingProposalStore<'bundle>, ExistingFieldRemoval), InternalError> {
    let mut resolved = None;
    for store in stores {
        let Some(entity_tag) = store.bundle.source_bindings().entity(entity_source) else {
            continue;
        };
        if resolved.is_some() {
            return Err(InternalError::store_unsupported());
        }
        let snapshot = store
            .bundle
            .entity_snapshots()
            .get(&entity_tag)
            .ok_or_else(InternalError::store_invariant)?;
        if !snapshot.constraint_activations().is_empty()
            || !snapshot.candidate_indexes().is_empty()
            || !snapshot.candidate_relations().is_empty()
        {
            return Err(InternalError::store_unsupported());
        }
        let field_id = store
            .bundle
            .source_bindings()
            .field(entity_tag, field_source)
            .ok_or_else(InternalError::store_unsupported)?;
        let field = snapshot
            .fields()
            .iter()
            .find(|field| field.id() == field_id)
            .ok_or_else(InternalError::store_invariant)?;
        if !field.generated() {
            return Err(InternalError::store_unsupported());
        }
        resolved = Some((
            store,
            ExistingFieldRemoval {
                entity_tag,
                source: field_source.clone(),
                id: field_id,
            },
        ));
    }
    resolved.ok_or_else(InternalError::store_unsupported)
}

/// Resolve one generated secondary-index removal solely through immutable
/// accepted source identity.
fn resolve_existing_generated_index_removal<'store, 'bundle>(
    stores: &'store [ExistingProposalStore<'bundle>],
    entity_source: &EntitySourceKey,
    index_source: &IndexSourceKey,
) -> Result<(&'store ExistingProposalStore<'bundle>, ExistingIndexRemoval), InternalError> {
    let mut resolved = None;
    for store in stores {
        let Some(entity_tag) = store.bundle.source_bindings().entity(entity_source) else {
            continue;
        };
        if resolved.is_some() {
            return Err(InternalError::store_unsupported());
        }
        let snapshot = store
            .bundle
            .entity_snapshots()
            .get(&entity_tag)
            .ok_or_else(InternalError::store_invariant)?;
        if !snapshot.constraint_activations().is_empty()
            || !snapshot.candidate_indexes().is_empty()
            || !snapshot.candidate_relations().is_empty()
        {
            return Err(InternalError::store_unsupported());
        }
        let index_id = store
            .bundle
            .source_bindings()
            .index(entity_tag, index_source)
            .ok_or_else(InternalError::store_unsupported)?;
        let index = snapshot
            .indexes()
            .iter()
            .find(|index| index.schema_id() == index_id)
            .ok_or_else(InternalError::store_invariant)?;
        if !index.generated() {
            return Err(InternalError::store_unsupported());
        }
        resolved = Some((
            store,
            ExistingIndexRemoval {
                entity_tag,
                source: index_source.clone(),
                id: index_id,
            },
        ));
    }
    resolved.ok_or_else(InternalError::store_unsupported)
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
    let mut resolved = None;
    for store in stores {
        let Some(entity_tag) = store.bundle.source_bindings().entity(entity_source) else {
            continue;
        };
        if resolved.is_some() {
            return Err(InternalError::store_unsupported());
        }
        let snapshot = store
            .bundle
            .entity_snapshots()
            .get(&entity_tag)
            .ok_or_else(InternalError::store_invariant)?;
        if !snapshot.constraint_activations().is_empty()
            || !snapshot.candidate_indexes().is_empty()
            || !snapshot.candidate_relations().is_empty()
        {
            return Err(InternalError::store_unsupported());
        }
        let relation_id = store
            .bundle
            .source_bindings()
            .relation(entity_tag, relation_source)
            .ok_or_else(InternalError::store_unsupported)?;
        if !snapshot
            .relations()
            .iter()
            .any(|relation| relation.id() == relation_id)
        {
            return Err(InternalError::store_invariant());
        }
        resolved = Some((
            store,
            ExistingRelationRemoval {
                entity_tag,
                source: relation_source.clone(),
                id: relation_id,
            },
        ));
    }
    resolved.ok_or_else(InternalError::store_unsupported)
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
    let mut removals_by_entity = BTreeMap::<EntityTag, Vec<&ExistingIndexRemoval>>::new();
    for removal in removals {
        removals_by_entity
            .entry(removal.entity_tag)
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
        let candidate = derive_dense_index_removal_candidate(&current, removal.id)
            .map_err(|_| InternalError::store_unsupported())?;
        source_bindings.remove_index(entity_tag, &removal.source, removal.id)?;
        snapshots.insert(entity_tag, candidate);
        changed_entities.insert(entity_tag);
    }
    Ok(changed_entities)
}

/// Remove one generated relation and its paired constraint per entity.
fn apply_existing_relation_removals(
    snapshots: &mut BTreeMap<EntityTag, PersistedSchemaSnapshot>,
    source_bindings: &mut AcceptedSourceBindingCatalog,
    removals: &[ExistingRelationRemoval],
) -> Result<BTreeSet<EntityTag>, InternalError> {
    let mut removals_by_entity = BTreeMap::<EntityTag, Vec<&ExistingRelationRemoval>>::new();
    for removal in removals {
        removals_by_entity
            .entry(removal.entity_tag)
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
        let candidate = derive_relation_removal_candidate(&current, removal.id)?;
        source_bindings.remove_relation(entity_tag, &removal.source, removal.id)?;
        snapshots.insert(entity_tag, candidate);
        changed_entities.insert(entity_tag);
    }
    Ok(changed_entities)
}

/// Derive one dense generated-field removal per entity and carry every
/// retained source binding through the candidate-owned field-ID lineage.
fn apply_existing_field_removals(
    snapshots: &mut BTreeMap<EntityTag, PersistedSchemaSnapshot>,
    source_bindings: &mut AcceptedSourceBindingCatalog,
    removals: &[ExistingFieldRemoval],
) -> Result<BTreeSet<EntityTag>, InternalError> {
    let mut removals_by_entity = BTreeMap::<EntityTag, Vec<&ExistingFieldRemoval>>::new();
    for removal in removals {
        removals_by_entity
            .entry(removal.entity_tag)
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
        let candidate = derive_dense_field_removal_candidate(&current, removal.id)
            .map_err(|_| InternalError::store_unsupported())?;
        source_bindings.remove_field_and_remap(
            entity_tag,
            &removal.source,
            removal.id,
            |field_id| candidate.retained_field_id(field_id),
        )?;
        snapshots.insert(entity_tag, candidate.into_snapshot());
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

fn verify_unique_entity_binding(
    stores: &[ExistingProposalStore<'_>],
    source: &EntitySourceKey,
    expected: &ExistingProposalStore<'_>,
) -> Result<(), InternalError> {
    let owners = stores
        .iter()
        .filter(|store| store.bundle.source_bindings().entity(source).is_some())
        .collect::<Vec<_>>();
    if !matches!(owners.as_slice(), [owner] if owner.path == expected.path) {
        return Err(InternalError::store_unsupported());
    }
    Ok(())
}

fn lower_existing_named_catalogs(
    bundle: &AcceptedSchemaRevisionBundle,
    entities: &[&EntityFragment],
    types: &BTreeMap<TypeSourceKey, &NamedTypeFragment>,
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
        let identity = bundle
            .source_bindings()
            .named_type(&source)
            .ok_or_else(InternalError::store_unsupported)?;
        if let Some(definition) = types.get(&source).copied() {
            if matches!(
                (identity, definition),
                (
                    AcceptedNamedTypeIdentity::Enum(_),
                    NamedTypeFragment::Enum(_)
                ) | (
                    AcceptedNamedTypeIdentity::Composite(_),
                    NamedTypeFragment::Record(_)
                        | NamedTypeFragment::Newtype { .. }
                        | NamedTypeFragment::List { .. }
                        | NamedTypeFragment::Set { .. }
                        | NamedTypeFragment::Map { .. }
                        | NamedTypeFragment::Tuple { .. }
                )
            ) {
                collect_named_type_dependencies(definition, &mut pending);
                used_types.insert(source);
            } else {
                return Err(InternalError::store_unsupported());
            }
        }
    }

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
    if !current.constraint_activations().is_empty()
        || !current.candidate_indexes().is_empty()
        || !current.candidate_relations().is_empty()
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
        let kind = lower_field_type(proposed.field_type(), bindings)?;
        let storage_decode = field_storage_decode(proposed.field_type());
        let leaf_codec = field_leaf_codec(proposed.field_type(), &kind);
        let nested_leaves = lower_migration_nested_leaves(&kind, &catalogs.composite_catalog)?;
        let write_policy =
            lower_write_policy(proposed.insert_policy(), proposed.management(), &kind)?;
        let insert_default = AcceptedDefaultLowering {
            bindings,
            enum_catalog: &catalogs.enum_catalog,
            composite_catalog: &catalogs.composite_catalog,
        }
        .lower(
            proposed.insert_policy(),
            proposed.name().as_str(),
            &kind,
            proposed.nullable(),
            storage_decode,
            leaf_codec,
        )?;
        let candidate = PersistedFieldSnapshot::new_with_write_policy_and_origin(
            accepted.id(),
            proposed.name().as_str().to_string(),
            accepted.slot(),
            kind,
            nested_leaves,
            proposed.nullable(),
            accepted.introduced_in_layout(),
            insert_default,
            accepted.historical_fill().clone(),
            write_policy,
            PersistedFieldOrigin::Generated,
            storage_decode,
            leaf_codec,
        );
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

/// Lower one generated migration target through the ordinary accepted-field
/// constructors while retaining the migration planner's explicit identity
/// and full-rewrite slot assignment.
///
/// This seam deliberately does not accept historical fill. A physical
/// migration validates and later rewrites every predecessor row into the new
/// layout, so the target layout admits no absent legacy slot.
#[cfg(any(test, feature = "migration"))]
pub(in crate::db::schema) fn lower_migration_field(
    proposed: &icydb_schema::FieldFragment,
    id: FieldId,
    slot: SchemaFieldSlot,
    introduced_in_layout: RowLayoutVersion,
    bindings: &AcceptedSourceBindingCatalog,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
) -> Result<PersistedFieldSnapshot, InternalError> {
    let kind = lower_field_type(proposed.field_type(), bindings)?;
    let storage_decode = field_storage_decode(proposed.field_type());
    let leaf_codec = field_leaf_codec(proposed.field_type(), &kind);
    let nested_leaves = lower_migration_nested_leaves(&kind, composite_catalog)?;
    let write_policy = lower_write_policy(proposed.insert_policy(), proposed.management(), &kind)?;
    let insert_default = AcceptedDefaultLowering {
        bindings,
        enum_catalog,
        composite_catalog,
    }
    .lower(
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
        SchemaHistoricalFill::Reject,
        write_policy,
        PersistedFieldOrigin::Generated,
        storage_decode,
        leaf_codec,
    ))
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
        let before = ExistingIndexLowering {
            revision: bundle.revision(),
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
        let after = ExistingIndexLowering {
            revision: bundle.revision(),
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

/// Catalog and entity facts used to prove or rebuild one generated index.
struct ExistingIndexLowering<'a> {
    revision: AcceptedSchemaRevision,
    enum_catalog: &'a AcceptedEnumCatalog,
    composite_catalog: &'a AcceptedCompositeCatalog,
    entity_tag: EntityTag,
    snapshot: &'a PersistedSchemaSnapshot,
    bindings: &'a AcceptedSourceBindingCatalog,
}

/// Lower one migration-bound current index through the ordinary accepted
/// index constructor while retaining the supplied accepted identity.
#[cfg(any(test, feature = "migration"))]
#[expect(
    clippy::too_many_arguments,
    reason = "migration index proof keeps every accepted catalog and identity input explicit"
)]
pub(in crate::db::schema) fn lower_migration_index(
    proposed: &icydb_schema::IndexFragment,
    accepted: &PersistedIndexSnapshot,
    revision: AcceptedSchemaRevision,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
    entity_tag: EntityTag,
    snapshot: &PersistedSchemaSnapshot,
    bindings: &AcceptedSourceBindingCatalog,
) -> Result<PersistedIndexSnapshot, InternalError> {
    ExistingIndexLowering {
        revision,
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
#[cfg(any(test, feature = "migration"))]
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
    revision: AcceptedSchemaRevision,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
    entity_tag: EntityTag,
    snapshot: &PersistedSchemaSnapshot,
    bindings: &AcceptedSourceBindingCatalog,
) -> Result<PersistedIndexSnapshot, InternalError> {
    let value_catalog = AcceptedValueCatalogHandle::new(
        enum_catalog.clone(),
        composite_catalog.clone(),
        AcceptedStoreCatalogScope::new(),
        revision,
        AcceptedSchemaFingerprint::new([1; 32]),
    );
    let key = lower_index_key(proposed.key(), entity_tag, snapshot, bindings)?;
    let predicate_sql = proposed
        .predicate()
        .map(|predicate| {
            let accepted = bind_source_check_expr(
                predicate,
                entity_tag,
                bindings,
                snapshot,
                enum_catalog,
                composite_catalog,
            )
            .map_err(|_| InternalError::store_unsupported())?;
            render_accepted_check_expr_sql(&accepted, snapshot, &value_catalog)
        })
        .transpose()?;
    Ok(PersistedIndexSnapshot::new(
        schema_id,
        ordinal,
        proposed.name().as_str().to_string(),
        store_path.to_string(),
        proposed.unique(),
        key,
        predicate_sql,
    )
    .clone_with_schema_identity(schema_id, ordinal, physical_generation))
}

impl ExistingIndexLowering<'_> {
    fn lower(
        &self,
        proposed: &icydb_schema::IndexFragment,
        accepted: &PersistedIndexSnapshot,
    ) -> Result<PersistedIndexSnapshot, InternalError> {
        let value_catalog = AcceptedValueCatalogHandle::new(
            self.enum_catalog.clone(),
            self.composite_catalog.clone(),
            AcceptedStoreCatalogScope::new(),
            self.revision,
            AcceptedSchemaFingerprint::new([1; 32]),
        );
        let key = lower_index_key(
            proposed.key(),
            self.entity_tag,
            self.snapshot,
            self.bindings,
        )?;
        let predicate_sql = proposed
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
                render_accepted_check_expr_sql(&accepted_expression, self.snapshot, &value_catalog)
            })
            .transpose()?;
        Ok(PersistedIndexSnapshot::new(
            accepted.schema_id(),
            accepted.ordinal(),
            proposed.name().as_str().to_string(),
            accepted.store().to_string(),
            proposed.unique(),
            key,
            predicate_sql,
        )
        .clone_with_schema_identity(
            accepted.schema_id(),
            accepted.ordinal(),
            accepted.physical_generation(),
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
        let local_fields = proposed
            .local_fields()
            .iter()
            .map(|source| {
                bindings
                    .field(entity_tag, source)
                    .ok_or_else(InternalError::store_unsupported)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let (target_bundle, target_tag, target) =
            resolve_existing_entity(stores, proposed.target_entity())?;
        let target_fields = proposed
            .target_fields()
            .iter()
            .map(|source| {
                target_bundle
                    .source_bindings()
                    .field(target_tag, source)
                    .ok_or_else(InternalError::store_unsupported)
            })
            .collect::<Result<Vec<_>, _>>()?;
        if target_fields != target.primary_key_field_ids() {
            return Err(InternalError::store_unsupported());
        }
        let candidate = PersistedRelationEdgeSnapshot::new_direct(
            accepted.id(),
            proposed.name().as_str().to_string(),
            target.entity_path().to_string(),
            local_fields,
        )
        .clone_with_physical_generation(accepted.physical_generation());
        if candidate != *accepted {
            return Err(InternalError::store_unsupported());
        }
    }
    Ok(())
}

fn resolve_existing_entity<'bundle>(
    stores: &[ExistingProposalStore<'bundle>],
    source: &EntitySourceKey,
) -> Result<
    (
        &'bundle AcceptedSchemaRevisionBundle,
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
        if resolved
            .replace((store.bundle, entity_tag, snapshot))
            .is_some()
        {
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
    let bind_literal = |literal: &ScalarLiteral, kind: AcceptedFieldKind| {
        bind_source_rule_literal(literal, kind, bindings, enum_catalog, composite_catalog)
            .map_err(|_| InternalError::store_unsupported())
    };
    let operation = match proposed.operation() {
        SourceRuleOperation::LengthRangeInclusive { min, max }
            if accepted_rule_length_kind_is_supported(&resolved_kind) =>
        {
            AcceptedRuleOperation::LengthRangeInclusive {
                min: *min,
                max: *max,
            }
        }
        SourceRuleOperation::NumericMinimumInclusive { value }
            if accepted_rule_numeric_kind_is_supported(&resolved_kind)
                && source_rule_literal_is_exact_for_accepted_kind(value, &resolved_kind) =>
        {
            AcceptedRuleOperation::NumericMinimumInclusive {
                value: bind_literal(value, resolved_kind)?,
            }
        }
        SourceRuleOperation::NumericMaximumInclusive { value }
            if accepted_rule_numeric_kind_is_supported(&resolved_kind)
                && source_rule_literal_is_exact_for_accepted_kind(value, &resolved_kind) =>
        {
            AcceptedRuleOperation::NumericMaximumInclusive {
                value: bind_literal(value, resolved_kind)?,
            }
        }
        SourceRuleOperation::NumericRangeInclusive { min, max }
            if accepted_rule_numeric_kind_is_supported(&resolved_kind)
                && source_rule_literal_is_exact_for_accepted_kind(min, &resolved_kind)
                && source_rule_literal_is_exact_for_accepted_kind(max, &resolved_kind) =>
        {
            AcceptedRuleOperation::NumericRangeInclusive {
                min: bind_literal(min, resolved_kind.clone())?,
                max: bind_literal(max, resolved_kind)?,
            }
        }
        SourceRuleOperation::MultipleOf { divisor }
            if accepted_rule_exact_numeric_kind_is_supported(&resolved_kind)
                && source_rule_literal_is_exact_for_accepted_kind(divisor, &resolved_kind) =>
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
    let context = InitialStoreContext::new(
        store_path,
        assignments,
        all_entities,
        accepted_entities,
        entities,
        types,
    )?;
    let mut entity_bindings = BTreeMap::new();
    let mut field_bindings = BTreeMap::new();
    let mut provisional = BTreeMap::new();

    for entity in entities {
        let entity_tag = accepted_entities
            .get(entity.source_key())
            .copied()
            .ok_or_else(InternalError::store_invariant)?;
        entity_bindings.insert(entity.source_key().clone(), entity_tag);
        let snapshot =
            lower_initial_entity_fields(&context, entity, entity_tag, &mut field_bindings)?;
        provisional.insert(entity_tag, snapshot);
    }

    let partial_bindings = AcceptedSourceBindingCatalog::initial(
        entity_bindings.clone(),
        field_bindings.clone(),
        BTreeMap::new(),
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .with_initial_named_types_from(&context.named_type_bindings);
    let mut object_bindings = InitialObjectBindings::default();
    let mut snapshots = BTreeMap::new();

    for entity in entities {
        let entity_tag = partial_bindings
            .entity(entity.source_key())
            .ok_or_else(InternalError::store_invariant)?;
        let initial = provisional
            .get(&entity_tag)
            .ok_or_else(InternalError::store_invariant)?;
        let snapshot = lower_initial_complete_snapshot(
            &context,
            entity,
            entity_tag,
            initial,
            &partial_bindings,
            &mut object_bindings,
        )?;
        snapshots.insert(entity_tag, snapshot);
    }

    let bindings = AcceptedSourceBindingCatalog::initial(
        entity_bindings,
        field_bindings,
        object_bindings.constraints,
        object_bindings.indexes,
        object_bindings.relations,
    )
    .with_initial_named_types_from(&context.named_type_bindings);
    let bundle = AcceptedSchemaRevisionBundle::new_with_source_bindings(
        AcceptedSchemaRevision::INITIAL,
        store_path,
        context.enum_catalog,
        context.composite_catalog,
        bindings,
        snapshots,
    )?;
    CandidateSchemaRevision::new(bundle)
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
                    &context.enum_catalog,
                    &context.composite_catalog,
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
                    &context.enum_catalog,
                    &context.composite_catalog,
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
        let kind = lower_field_type(field.field_type(), &context.named_type_bindings)?;
        let storage_decode = field_storage_decode(field.field_type());
        let leaf_codec = field_leaf_codec(field.field_type(), &kind);
        let nested_leaves = lower_migration_nested_leaves(&kind, &context.composite_catalog)?;
        let write_policy = lower_write_policy(field.insert_policy(), field.management(), &kind)?;
        let insert_default = AcceptedDefaultLowering {
            bindings: &context.named_type_bindings,
            enum_catalog: &context.enum_catalog,
            composite_catalog: &context.composite_catalog,
        }
        .lower(
            field.insert_policy(),
            field.name().as_str(),
            &kind,
            field.nullable(),
            storage_decode,
            leaf_codec,
        )?;
        fields.push(PersistedFieldSnapshot::new_with_write_policy_and_origin(
            id,
            field.name().as_str().to_string(),
            slot,
            kind,
            nested_leaves,
            field.nullable(),
            RowLayoutVersion::INITIAL,
            insert_default,
            SchemaHistoricalFill::Reject,
            write_policy,
            PersistedFieldOrigin::Generated,
            storage_decode,
            leaf_codec,
        ));
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
    if depth >= MAX_ACCEPTED_RECURSIVE_DEPTH {
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
            let key = lower_index_key(index.key(), entity_tag, snapshot, bindings)?;
            let predicate_sql = index
                .predicate()
                .map(|predicate| {
                    let accepted = bind_source_check_expr(
                        predicate,
                        entity_tag,
                        bindings,
                        snapshot,
                        &context.enum_catalog,
                        &context.composite_catalog,
                    )
                    .map_err(|_| InternalError::store_unsupported())?;
                    render_accepted_check_expr_sql(&accepted, snapshot, &context.value_catalog)
                })
                .transpose()?;
            accepted_bindings.insert((entity_tag, index.source_key().clone()), id);
            Ok(PersistedIndexSnapshot::new(
                id,
                ordinal,
                index.name().as_str().to_string(),
                context.store_path.to_string(),
                index.unique(),
                key,
                predicate_sql,
            ))
        })
        .collect()
}

fn lower_index_key(
    key: &[IndexKeyFragment],
    entity_tag: EntityTag,
    snapshot: &PersistedSchemaSnapshot,
    bindings: &AcceptedSourceBindingCatalog,
) -> Result<PersistedIndexKeySnapshot, InternalError> {
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
    if items
        .iter()
        .all(|item| matches!(item, PersistedIndexKeyItemSnapshot::FieldPath(_)))
    {
        Ok(PersistedIndexKeySnapshot::FieldPath(
            items
                .into_iter()
                .filter_map(|item| match item {
                    PersistedIndexKeyItemSnapshot::FieldPath(path) => Some(path),
                    PersistedIndexKeyItemSnapshot::Expression(_) => None,
                })
                .collect(),
        ))
    } else {
        Ok(PersistedIndexKeySnapshot::Items(items))
    }
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
            let local_fields = relation
                .local_fields()
                .iter()
                .map(|source| {
                    bindings
                        .field(entity_tag, source)
                        .ok_or_else(InternalError::store_invariant)
                })
                .collect::<Result<Vec<_>, _>>()?;
            for (local, target_source) in
                relation.local_fields().iter().zip(relation.target_fields())
            {
                let local = entity
                    .fields()
                    .iter()
                    .find(|field| field.source_key() == local)
                    .ok_or_else(InternalError::store_invariant)?;
                let target_field = target
                    .fields()
                    .iter()
                    .find(|field| field.source_key() == target_source)
                    .ok_or_else(InternalError::store_invariant)?;
                let local_type = match local.field_type() {
                    FieldType::List(item) => item.as_ref(),
                    field_type => field_type,
                };
                if local_type != target_field.field_type() {
                    return Err(InternalError::store_unsupported());
                }
            }
            accepted_bindings.insert((entity_tag, relation.source_key().clone()), id);
            Ok(PersistedRelationEdgeSnapshot::new_direct(
                id,
                relation.name().as_str().to_string(),
                target.source_key().as_str().to_string(),
                local_fields,
            ))
        })
        .collect()
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
        ScalarType::IntBig { max_bytes } => AcceptedFieldKind::IntBig {
            max_bytes: *max_bytes,
        },
        ScalarType::Principal => AcceptedFieldKind::Principal,
        ScalarType::Subaccount => AcceptedFieldKind::Subaccount,
        ScalarType::Text { max_len } => AcceptedFieldKind::Text { max_len: *max_len },
        ScalarType::Timestamp => AcceptedFieldKind::Timestamp,
        ScalarType::Nat8 => AcceptedFieldKind::Nat8,
        ScalarType::Nat16 => AcceptedFieldKind::Nat16,
        ScalarType::Nat32 => AcceptedFieldKind::Nat32,
        ScalarType::Nat64 => AcceptedFieldKind::Nat64,
        ScalarType::Nat128 => AcceptedFieldKind::Nat128,
        ScalarType::NatBig { max_bytes } => AcceptedFieldKind::NatBig {
            max_bytes: *max_bytes,
        },
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

fn index_expression_output_kind(
    op: PersistedIndexExpressionOp,
    source: &AcceptedFieldKind,
) -> Option<AcceptedFieldKind> {
    match op {
        PersistedIndexExpressionOp::Lower
        | PersistedIndexExpressionOp::Upper
        | PersistedIndexExpressionOp::Trim
        | PersistedIndexExpressionOp::LowerTrim
            if matches!(source, AcceptedFieldKind::Text { .. }) =>
        {
            Some(source.clone())
        }
        PersistedIndexExpressionOp::Date
            if matches!(
                source,
                AcceptedFieldKind::Date | AcceptedFieldKind::Timestamp
            ) =>
        {
            Some(AcceptedFieldKind::Date)
        }
        PersistedIndexExpressionOp::Year
        | PersistedIndexExpressionOp::Month
        | PersistedIndexExpressionOp::Day
            if matches!(
                source,
                AcceptedFieldKind::Date | AcceptedFieldKind::Timestamp
            ) =>
        {
            Some(AcceptedFieldKind::Int64)
        }
        _ => None,
    }
}

fn index_expression_text(op: PersistedIndexExpressionOp, field: &str) -> String {
    match op {
        PersistedIndexExpressionOp::Lower => format!("expr:v1:LOWER({field})"),
        PersistedIndexExpressionOp::Upper => format!("expr:v1:UPPER({field})"),
        PersistedIndexExpressionOp::Trim => format!("expr:v1:TRIM({field})"),
        PersistedIndexExpressionOp::LowerTrim => format!("expr:v1:LOWER(TRIM({field}))"),
        PersistedIndexExpressionOp::Date => format!("expr:v1:DATE({field})"),
        PersistedIndexExpressionOp::Year => format!("expr:v1:YEAR({field})"),
        PersistedIndexExpressionOp::Month => format!("expr:v1:MONTH({field})"),
        PersistedIndexExpressionOp::Day => format!("expr:v1:DAY({field})"),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        ExistingProposalStore, ProposalStoreTarget, lower_existing_schema_proposal,
        lower_field_type, lower_generated_existing_schema_proposal, lower_initial_field_type,
        lower_initial_schema_proposal,
    };
    use crate::db::{
        data::{
            decode_canonical_value_storage_bytes, encode_input_value_for_candidate_field_contract,
            validate_default_payload_for_accepted_field_contract,
        },
        schema::{
            AcceptedConstraintCatalog, AcceptedConstraintKind, AcceptedConstraintSnapshot,
            AcceptedFieldDecodeContract, AcceptedFieldKind, AcceptedNamedTypeIdentity,
            AcceptedRuleOperation, AcceptedRuleTarget, AcceptedSchemaRevision,
            AcceptedSchemaRevisionBundle, AcceptedSchemaSnapshot, AcceptedSourceBindingCatalog,
            AcceptedStoreCatalogScope, AcceptedValueCatalogHandle, AcceptedValueContract,
            CompiledAcceptedRowConstraints, ConstraintActivationKind, ConstraintOrigin,
            FieldInsertGeneration, PersistedFieldSnapshot, SchemaInsertDefault,
            ValueAdmissionBudget,
            composite_catalog::AcceptedCompositeShape,
            enum_catalog::{
                AcceptedEnumVariantBody, ValueAdmissionError, normalize_candidate_value,
            },
        },
    };
    use crate::value::{EnumTypeId, InputValue, PublicEnumValue, PublicValue, Value};
    use icydb_schema::{
        ConstraintFragment, ConstraintSourceKey, DeclaredEntityVersion, EntityFragment,
        EntitySourceKey, EntityStoreAssignment, EnumTypeFragment, EnumVariantFragment,
        ExpectedAcceptedHead, ExpectedSchemaFingerprint, FieldFragment, FieldInsertPolicy,
        FieldSourceKey, FieldType, IndexFragment, IndexKeyFragment, IndexSourceKey,
        NamedTypeFragment, RecordFieldFragment, RecordTypeFragment, ScalarLiteral, ScalarType,
        SchemaCapability, SchemaFragment, SchemaName, SchemaProposal, SchemaRemoval,
        SchemaSubmissionKey, SourceCheckExpr, SourceCheckInstruction, SourceRuleOperation,
        TargetDatabaseIdentity, TargetStoreIdentity, TargetedRuleFragment, TupleElementFragment,
        TypeSourceKey,
    };
    fn name(value: &str) -> SchemaName {
        SchemaName::try_new(value).expect("test schema name should admit")
    }

    fn version_one() -> DeclaredEntityVersion {
        DeclaredEntityVersion::try_new(1).expect("fixture version should admit")
    }

    #[test]
    fn field_type_lowering_uses_the_same_closed_named_identity_authority() {
        let source = TypeSourceKey::try_new("Status").expect("type source should admit");
        let type_id = EnumTypeId::new(7).expect("enum type id should admit");
        let identities =
            BTreeMap::from([(source.clone(), AcceptedNamedTypeIdentity::Enum(type_id))]);
        let accepted = AcceptedSourceBindingCatalog::default().with_initial_named_types(
            identities.clone(),
            BTreeMap::new(),
            BTreeMap::new(),
        );
        let field_type = FieldType::List(Box::new(FieldType::Named(source)));
        let expected = AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Enum { type_id }));

        assert_eq!(
            lower_initial_field_type(&field_type, &identities)
                .expect("initial named identity should resolve"),
            expected,
        );
        assert_eq!(
            lower_field_type(&field_type, &accepted)
                .expect("accepted named identity should resolve"),
            expected,
        );
    }

    struct TargetedRuleProposalFixture {
        proposal: SchemaProposal,
        entity_source: EntitySourceKey,
        value_source: FieldSourceKey,
        value_type: TypeSourceKey,
        other_type: TypeSourceKey,
        constraint_source: ConstraintSourceKey,
        store: TargetStoreIdentity,
    }

    fn targeted_rule_proposal_fixture(
        expected_head: ExpectedAcceptedHead,
        submission_key: &str,
        value_type_name: &str,
        other_type_name: &str,
        include_rule: bool,
        removals: Vec<SchemaRemoval>,
    ) -> TargetedRuleProposalFixture {
        targeted_rule_proposal_fixture_with_operation(
            expected_head,
            submission_key,
            value_type_name,
            other_type_name,
            "range",
            include_rule.then_some(SourceRuleOperation::NumericRangeInclusive {
                min: ScalarLiteral::Nat(0),
                max: ScalarLiteral::Nat(10),
            }),
            removals,
        )
    }

    fn targeted_rule_proposal_fixture_with_operation(
        expected_head: ExpectedAcceptedHead,
        submission_key: &str,
        value_type_name: &str,
        other_type_name: &str,
        rule_name: &str,
        operation: Option<SourceRuleOperation>,
        removals: Vec<SchemaRemoval>,
    ) -> TargetedRuleProposalFixture {
        let entity_source =
            EntitySourceKey::try_new("Targeted").expect("entity source should admit");
        let id_source = FieldSourceKey::try_new("id").expect("field source should admit");
        let value_source = FieldSourceKey::try_new("value").expect("field source should admit");
        let value_type = TypeSourceKey::try_new(value_type_name).expect("type source should admit");
        let other_type = TypeSourceKey::try_new(other_type_name).expect("type source should admit");
        let rule_source =
            icydb_schema::RuleSourceKey::try_new(rule_name).expect("rule source should admit");
        let constraint_source =
            ConstraintSourceKey::for_targeted_field_rule(&value_source, &value_type, &rule_source);
        let constraints = operation
            .map(|operation| {
                ConstraintFragment::targeted_rule(TargetedRuleFragment::new(
                    value_source.clone(),
                    value_type.clone(),
                    name(rule_name),
                    operation,
                ))
            })
            .into_iter()
            .collect();
        let entity = EntityFragment::try_new(
            name("Targeted"),
            version_one(),
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
                    FieldType::Named(value_type.clone()),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
                FieldFragment::new(
                    name("other"),
                    FieldType::Named(other_type.clone()),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
            ],
            vec![id_source],
            Vec::new(),
            Vec::new(),
            constraints,
        )
        .expect("targeted entity should admit");
        let fragment = SchemaFragment::try_new(
            vec![entity],
            vec![
                NamedTypeFragment::newtype(
                    name(value_type_name),
                    FieldType::Scalar(ScalarType::Nat8),
                ),
                NamedTypeFragment::newtype(
                    name(other_type_name),
                    FieldType::Scalar(ScalarType::Nat16),
                ),
            ],
        )
        .expect("targeted fragment should admit");
        let store = TargetStoreIdentity::from_bytes([0x64; 32]);
        let proposal = SchemaProposal::try_compose(
            vec![SchemaCapability::ACCEPTED_CHECKS],
            TargetDatabaseIdentity::from_bytes([0x63; 32]),
            SchemaSubmissionKey::try_new(submission_key).expect("submission should admit"),
            expected_head,
            vec![fragment],
            vec![EntityStoreAssignment::new(entity_source.clone(), store)],
            removals,
            None,
        )
        .expect("targeted proposal should compose");
        TargetedRuleProposalFixture {
            proposal,
            entity_source,
            value_source,
            value_type,
            other_type,
            constraint_source,
            store,
        }
    }

    fn scalar_proposal_fixture(
        expected_head: ExpectedAcceptedHead,
        submission_key: &str,
        score_default: i128,
    ) -> (SchemaProposal, EntitySourceKey, TargetStoreIdentity) {
        scalar_proposal_fixture_with_names(
            expected_head,
            submission_key,
            score_default,
            "Item",
            "score",
            true,
            Vec::new(),
        )
    }

    fn nullable_unique_proposal_fixture(
        predicate: Option<SourceCheckExpr>,
    ) -> (SchemaProposal, TargetStoreIdentity) {
        let entity_source =
            EntitySourceKey::try_new("Account").expect("test entity source should admit");
        let id_source = FieldSourceKey::try_new("id").expect("test id source should admit");
        let email_source =
            FieldSourceKey::try_new("email").expect("test email source should admit");
        let entity = EntityFragment::try_new(
            name("Account"),
            version_one(),
            vec![
                FieldFragment::new(
                    name("id"),
                    FieldType::Scalar(ScalarType::Nat64),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
                FieldFragment::new(
                    name("email"),
                    FieldType::Scalar(ScalarType::Text { max_len: None }),
                    true,
                    FieldInsertPolicy::Required,
                    None,
                ),
            ],
            vec![id_source],
            vec![
                IndexFragment::try_new(
                    name("account_email"),
                    vec![IndexKeyFragment::Field(email_source)],
                    true,
                    predicate,
                )
                .expect("test index should admit"),
            ],
            Vec::new(),
            Vec::new(),
        )
        .expect("test entity should admit");
        let fragment =
            SchemaFragment::try_new(vec![entity], Vec::new()).expect("test fragment should admit");
        let store = TargetStoreIdentity::from_bytes([0x32; 32]);
        let proposal = SchemaProposal::try_compose(
            vec![SchemaCapability::SECONDARY_INDEXES],
            TargetDatabaseIdentity::from_bytes([0x31; 32]),
            SchemaSubmissionKey::try_new("nullable-unique")
                .expect("test submission key should admit"),
            ExpectedAcceptedHead::Empty,
            vec![fragment],
            vec![EntityStoreAssignment::new(entity_source, store)],
            Vec::new(),
            None,
        )
        .expect("test proposal should compose");
        (proposal, store)
    }

    fn scalar_proposal_fixture_with_names(
        expected_head: ExpectedAcceptedHead,
        submission_key: &str,
        score_default: i128,
        entity_name: &str,
        score_name: &str,
        include_check: bool,
        removals: Vec<SchemaRemoval>,
    ) -> (SchemaProposal, EntitySourceKey, TargetStoreIdentity) {
        let entity_source =
            EntitySourceKey::try_new(entity_name).expect("test entity source should admit");
        let id_source = FieldSourceKey::try_new("id").expect("test field source should admit");
        let score_source =
            FieldSourceKey::try_new(score_name).expect("test field source should admit");
        let check = SourceCheckExpr::try_new(vec![
            SourceCheckInstruction::Field(score_source.clone()),
            SourceCheckInstruction::Literal(ScalarLiteral::Int(0)),
            SourceCheckInstruction::GreaterThanOrEqual,
        ])
        .expect("test check should admit");
        let entity = EntityFragment::try_new(
            name(entity_name),
            version_one(),
            vec![
                FieldFragment::new(
                    name("id"),
                    FieldType::Scalar(ScalarType::Nat64),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
                FieldFragment::new(
                    name(score_name),
                    FieldType::Scalar(ScalarType::Int64),
                    false,
                    FieldInsertPolicy::Default(ScalarLiteral::Int(score_default)),
                    None,
                ),
            ],
            vec![id_source],
            vec![
                IndexFragment::try_new(
                    name("score_idx"),
                    vec![IndexKeyFragment::Field(score_source)],
                    false,
                    None,
                )
                .expect("test index should admit"),
            ],
            Vec::new(),
            include_check
                .then(|| ConstraintFragment::check(name("score_non_negative"), check))
                .into_iter()
                .collect(),
        )
        .expect("test entity should admit");
        let fragment =
            SchemaFragment::try_new(vec![entity], Vec::new()).expect("test fragment should admit");
        let store = TargetStoreIdentity::from_bytes([0x22; 32]);
        let proposal = SchemaProposal::try_compose(
            vec![
                SchemaCapability::ACCEPTED_CHECKS,
                SchemaCapability::INSERT_DEFAULTS,
                SchemaCapability::SECONDARY_INDEXES,
            ],
            TargetDatabaseIdentity::from_bytes([0x11; 32]),
            SchemaSubmissionKey::try_new(submission_key).expect("test submission key should admit"),
            expected_head,
            vec![fragment],
            vec![EntityStoreAssignment::new(entity_source.clone(), store)],
            removals,
            None,
        )
        .expect("test proposal should compose");
        (proposal, entity_source, store)
    }

    #[test]
    fn initial_scalar_proposal_lowers_source_identity_defaults_indexes_and_checks() {
        let (proposal, entity_source, store) =
            scalar_proposal_fixture(ExpectedAcceptedHead::Empty, "initial-scalar", 5);

        let candidates = lower_initial_schema_proposal(
            &proposal,
            &[ProposalStoreTarget {
                path: "test::Store",
                identity: store,
            }],
        )
        .expect("initial scalar proposal should lower");

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].revision(), AcceptedSchemaRevision::INITIAL);
        let bundle = candidates[0].bundle();
        let entity_tag = bundle
            .source_bindings_for_tests()
            .entity(&entity_source)
            .expect("entity source should bind");
        let snapshot = bundle
            .entity_snapshots()
            .get(&entity_tag)
            .expect("entity snapshot should exist");
        assert_eq!(snapshot.indexes().len(), 1);
        assert!(matches!(
            snapshot
                .fields()
                .iter()
                .find(|field| field.name() == "score")
                .expect("score should exist")
                .insert_default(),
            SchemaInsertDefault::SlotPayload(payload) if !payload.is_empty()
        ));
        assert!(
            snapshot
                .constraint_catalog()
                .constraints()
                .iter()
                .any(|constraint| matches!(
                    constraint.kind(),
                    AcceptedConstraintKind::Check { .. }
                ) && constraint.name() == "score_non_negative")
        );
        assert_eq!(
            bundle
                .source_bindings_for_tests()
                .field_binding_count_for_tests(entity_tag),
            2,
        );
        assert_eq!(
            bundle
                .source_bindings_for_tests()
                .index_binding_count_for_tests(entity_tag),
            1,
        );
        assert_eq!(
            bundle
                .source_bindings_for_tests()
                .constraint_binding_count_for_tests(entity_tag),
            1,
        );
    }

    #[test]
    fn initial_generated_nullable_unique_index_uses_canonical_contract() {
        let (implicit, store) = nullable_unique_proposal_fixture(None);
        let error = lower_initial_schema_proposal(
            &implicit,
            &[ProposalStoreTarget {
                path: "test::Store",
                identity: store,
            }],
        )
        .expect_err("generated implicit nullable uniqueness must reject");
        assert_eq!(error.class(), crate::error::ErrorClass::Unsupported);
        assert_eq!(error.origin(), crate::error::ErrorOrigin::Store);

        let email_source =
            FieldSourceKey::try_new("email").expect("test email source should admit");
        let guard = SourceCheckExpr::try_new(vec![
            SourceCheckInstruction::Field(email_source),
            SourceCheckInstruction::IsNotNull,
        ])
        .expect("test guard should admit");
        let (explicit, store) = nullable_unique_proposal_fixture(Some(guard));
        let candidates = lower_initial_schema_proposal(
            &explicit,
            &[ProposalStoreTarget {
                path: "test::Store",
                identity: store,
            }],
        )
        .expect("generated explicit nullable uniqueness should lower");
        let index = candidates[0]
            .bundle()
            .entity_snapshots()
            .values()
            .next()
            .expect("generated entity should exist")
            .indexes()
            .first()
            .expect("generated index should exist");
        assert_eq!(index.predicate_sql(), Some("email IS NOT NULL"));
    }

    #[test]
    fn initial_identity_policy_lowers_all_exact_unsigned_widths_to_accepted_ids() {
        for (index, scalar) in [
            ScalarType::Nat8,
            ScalarType::Nat16,
            ScalarType::Nat32,
            ScalarType::Nat64,
            ScalarType::Nat128,
        ]
        .into_iter()
        .enumerate()
        {
            let entity_name = format!("Identity{index}");
            let entity_source =
                EntitySourceKey::try_new(&entity_name).expect("entity source should admit");
            let id_source = FieldSourceKey::try_new("id").expect("field source should admit");
            let entity = EntityFragment::try_new(
                name(&entity_name),
                version_one(),
                vec![FieldFragment::new(
                    name("id"),
                    FieldType::Scalar(scalar),
                    false,
                    FieldInsertPolicy::Generated,
                    None,
                )],
                vec![id_source.clone()],
                Vec::new(),
                Vec::new(),
                Vec::new(),
            )
            .expect("exact unsigned identity proposal should admit");
            let fragment = SchemaFragment::try_new(vec![entity], Vec::new())
                .expect("identity fragment should admit");
            let discriminator =
                u8::try_from(index).expect("identity width fixture index should fit one byte");
            let store = TargetStoreIdentity::from_bytes(
                [0x70_u8
                    .checked_add(discriminator)
                    .expect("identity width fixture discriminator should fit"); 32],
            );
            let proposal = SchemaProposal::try_compose(
                vec![SchemaCapability::GENERATED_VALUES],
                TargetDatabaseIdentity::from_bytes([0x61; 32]),
                SchemaSubmissionKey::try_new(format!("identity-{index}"))
                    .expect("submission key should admit"),
                ExpectedAcceptedHead::Empty,
                vec![fragment],
                vec![EntityStoreAssignment::new(entity_source.clone(), store)],
                Vec::new(),
                None,
            )
            .expect("identity proposal should compose");

            let candidates = lower_initial_schema_proposal(
                &proposal,
                &[ProposalStoreTarget {
                    path: "test::IdentityStore",
                    identity: store,
                }],
            )
            .expect("identity proposal should lower through accepted authority");
            let candidate = &candidates[0];
            let entity_tag = candidate
                .bundle()
                .source_bindings_for_tests()
                .entity(&entity_source)
                .expect("entity source should bind to accepted identity");
            let field_id = candidate
                .bundle()
                .source_bindings_for_tests()
                .field(entity_tag, &id_source)
                .expect("field source should bind to accepted identity");
            let field = candidate.bundle().entity_snapshots()[&entity_tag]
                .fields()
                .iter()
                .find(|field| field.id() == field_id)
                .expect("accepted identity field should exist");

            assert_eq!(candidate.store_path(), "test::IdentityStore");
            assert_eq!(
                field.write_policy().insert_generation(),
                Some(FieldInsertGeneration::Identity),
            );
        }
    }

    #[test]
    fn initial_newtype_rules_bind_through_the_accepted_composite_catalog() {
        let entity_source =
            EntitySourceKey::try_new("Compass").expect("test entity source should admit");
        let id_source = FieldSourceKey::try_new("id").expect("test id source should admit");
        let degrees_source =
            FieldSourceKey::try_new("degrees").expect("test degrees source should admit");
        let degrees_type =
            TypeSourceKey::try_new("Degrees").expect("test type source should admit");
        let expression = SourceCheckExpr::try_new(vec![
            SourceCheckInstruction::Field(degrees_source.clone()),
            SourceCheckInstruction::Literal(ScalarLiteral::Nat(360)),
            SourceCheckInstruction::LessThanOrEqual,
        ])
        .expect("test newtype rule should admit");
        let entity = EntityFragment::try_new(
            name("Compass"),
            version_one(),
            vec![
                FieldFragment::new(
                    name("id"),
                    FieldType::Scalar(ScalarType::Nat64),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
                FieldFragment::new(
                    name("degrees"),
                    FieldType::Named(degrees_type),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
            ],
            vec![id_source],
            Vec::new(),
            Vec::new(),
            vec![ConstraintFragment::check(name("degrees_range"), expression)],
        )
        .expect("test entity should admit");
        let fragment = SchemaFragment::try_new(
            vec![entity],
            vec![NamedTypeFragment::newtype(
                name("Degrees"),
                FieldType::Scalar(ScalarType::Nat16),
            )],
        )
        .expect("test newtype fragment should admit");
        let store = TargetStoreIdentity::from_bytes([0x24; 32]);
        let proposal = SchemaProposal::try_compose(
            vec![
                SchemaCapability::ACCEPTED_CHECKS,
                SchemaCapability::EXACT_COMPOSITE_TYPES,
            ],
            TargetDatabaseIdentity::from_bytes([0x14; 32]),
            SchemaSubmissionKey::try_new("initial-newtype-rule")
                .expect("test submission key should admit"),
            ExpectedAcceptedHead::Empty,
            vec![fragment],
            vec![EntityStoreAssignment::new(entity_source.clone(), store)],
            Vec::new(),
            None,
        )
        .expect("test proposal should compose");

        let candidates = lower_initial_schema_proposal(
            &proposal,
            &[ProposalStoreTarget {
                path: "test::Store",
                identity: store,
            }],
        )
        .expect("newtype rule should lower through accepted catalog authority");
        let bundle = candidates[0].bundle();
        let entity_tag = bundle
            .source_bindings_for_tests()
            .entity(&entity_source)
            .expect("entity source should bind");
        let snapshot = bundle
            .entity_snapshots()
            .get(&entity_tag)
            .expect("entity snapshot should exist");

        assert!(snapshot.constraints().iter().any(|constraint| {
            constraint.name() == "degrees_range"
                && matches!(constraint.kind(), AcceptedConstraintKind::Check { .. })
        }));
        assert!(matches!(
            snapshot
                .fields()
                .iter()
                .find(|field| field.id()
                    == bundle
                        .source_bindings_for_tests()
                        .field(entity_tag, &degrees_source)
                        .expect("degrees field source should bind"))
                .map(PersistedFieldSnapshot::kind),
            Some(crate::db::schema::AcceptedFieldKind::Composite { .. })
        ));
    }

    #[test]
    fn initial_targeted_source_rule_binds_and_persists_accepted_identities() {
        let fixture = targeted_rule_proposal_fixture(
            ExpectedAcceptedHead::Empty,
            "targeted-binding",
            "Value",
            "Other",
            true,
            Vec::new(),
        );

        let candidates = lower_initial_schema_proposal(
            &fixture.proposal,
            &[ProposalStoreTarget {
                path: "test::Store",
                identity: fixture.store,
            }],
        )
        .expect("targeted rule should bind into the accepted candidate");
        let bundle = candidates[0].bundle();
        let entity_tag = bundle
            .source_bindings_for_tests()
            .entity(&fixture.entity_source)
            .expect("entity source should bind");
        let root_field_id = bundle
            .source_bindings_for_tests()
            .field(entity_tag, &fixture.value_source)
            .expect("root field source should bind");
        let target_type = bundle
            .source_bindings_for_tests()
            .named_type(&fixture.value_type)
            .expect("target type source should bind");
        let constraint_id = bundle
            .source_bindings_for_tests()
            .constraint(entity_tag, &fixture.constraint_source)
            .expect("constraint source should bind");
        let snapshot = &bundle.entity_snapshots()[&entity_tag];
        let accepted = snapshot
            .constraints()
            .iter()
            .find(|constraint| constraint.id() == constraint_id)
            .expect("accepted targeted rule should persist");
        let AcceptedConstraintKind::TargetedRule { target, operation } = accepted.kind() else {
            panic!("source targeted rule must not lower through a general check");
        };
        assert_eq!(target.root_field_id(), root_field_id);
        assert_eq!(target.target_type(), target_type);
        let AcceptedRuleOperation::NumericRangeInclusive { min, max } = operation.as_ref() else {
            panic!("numeric range operation should remain closed and exact");
        };
        assert_eq!(min.kind(), &crate::db::schema::AcceptedFieldKind::Nat8);
        assert_eq!(max.kind(), &crate::db::schema::AcceptedFieldKind::Nat8);
        assert!(!min.payload().is_empty());
        assert!(!max.payload().is_empty());
        let accepted_schema =
            AcceptedSchemaSnapshot::try_new(snapshot.clone()).expect("snapshot should be accepted");
        let value_catalog = AcceptedValueCatalogHandle::new(
            bundle.enum_catalog().clone(),
            bundle.composite_catalog().clone(),
            AcceptedStoreCatalogScope::new(),
            bundle.revision(),
            bundle
                .semantic_fingerprint()
                .expect("bundle fingerprint should derive"),
        );
        let program =
            CompiledAcceptedRowConstraints::compile(&accepted_schema, &value_catalog, [0xA3; 16])
                .expect("N4 should compile the accepted targeted rule");
        program
            .evaluate(
                [0xA3; 16],
                &[
                    Some(Value::Ulid(crate::types::Ulid::from_u128(1))),
                    Some(Value::Nat64(5)),
                    Some(Value::Nat64(0)),
                ],
            )
            .expect("N5 should admit a compliant targeted value");
        assert!(
            bundle.semantic_fingerprint().is_ok(),
            "accepted target and operation must participate in the bundle fingerprint",
        );
        let repeated = lower_initial_schema_proposal(
            &fixture.proposal,
            &[ProposalStoreTarget {
                path: "test::Store",
                identity: fixture.store,
            }],
        )
        .expect("repeated targeted proposal should lower");
        assert_eq!(
            candidates[0].encoded_bundle(),
            repeated[0].encoded_bundle(),
            "accepted target encoding must be canonical",
        );
    }

    #[test]
    fn accepted_target_binding_rejects_retargeting_across_root_fields() {
        let fixture = targeted_rule_proposal_fixture(
            ExpectedAcceptedHead::Empty,
            "targeted-retarget",
            "Value",
            "Other",
            true,
            Vec::new(),
        );
        let candidates = lower_initial_schema_proposal(
            &fixture.proposal,
            &[ProposalStoreTarget {
                path: "test::Store",
                identity: fixture.store,
            }],
        )
        .expect("valid targeted proposal should lower");
        let bundle = candidates[0].bundle();
        let entity_tag = bundle
            .source_bindings_for_tests()
            .entity(&fixture.entity_source)
            .expect("entity source should bind");
        let other_type = bundle
            .source_bindings_for_tests()
            .named_type(&fixture.other_type)
            .expect("other type source should bind");
        let mut snapshots = bundle.entity_snapshots().clone();
        let snapshot = snapshots
            .get(&entity_tag)
            .cloned()
            .expect("entity snapshot should exist");
        let constraints = snapshot
            .constraints()
            .iter()
            .map(|constraint| {
                let kind = match constraint.kind() {
                    AcceptedConstraintKind::TargetedRule { target, operation } => {
                        AcceptedConstraintKind::TargetedRule {
                            target: AcceptedRuleTarget::new(target.root_field_id(), other_type),
                            operation: operation.clone(),
                        }
                    }
                    _ => constraint.kind().clone(),
                };
                AcceptedConstraintSnapshot::new(
                    constraint.id(),
                    constraint.name().to_string(),
                    constraint.origin(),
                    kind,
                )
            })
            .collect();
        let malformed_catalog = AcceptedConstraintCatalog::from_persisted_parts(
            snapshot.constraint_id_allocator(),
            constraints,
            snapshot.constraint_activations().to_vec(),
        );
        snapshots.insert(
            entity_tag,
            snapshot.with_constraint_catalog(malformed_catalog),
        );

        assert!(
            AcceptedSchemaRevisionBundle::new_with_source_bindings(
                bundle.revision(),
                bundle.store_path(),
                bundle.enum_catalog().clone(),
                bundle.composite_catalog().clone(),
                bundle.source_bindings_for_tests().clone(),
                snapshots,
            )
            .is_err(),
            "accepted target must remain reachable below its bound root field",
        );
    }

    #[test]
    fn existing_targeted_rule_addition_reserves_source_bound_activation() {
        let initial = targeted_rule_proposal_fixture(
            ExpectedAcceptedHead::Empty,
            "targeted-addition-initial",
            "Value",
            "Other",
            false,
            Vec::new(),
        );
        let initial_candidates = lower_initial_schema_proposal(
            &initial.proposal,
            &[ProposalStoreTarget {
                path: "test::Store",
                identity: initial.store,
            }],
        )
        .expect("initial proposal without a targeted rule should lower");
        let addition = targeted_rule_proposal_fixture(
            ExpectedAcceptedHead::Exact {
                revision: 1,
                fingerprint: ExpectedSchemaFingerprint::from_bytes([0x33; 32]),
            },
            "targeted-addition-existing",
            "Value",
            "Other",
            true,
            Vec::new(),
        );

        let candidate = lower_existing_schema_proposal(
            &addition.proposal,
            &[ExistingProposalStore {
                path: "test::Store",
                identity: initial.store,
                bundle: initial_candidates[0].bundle(),
            }],
        )
        .expect("N6 should lower the targeted rule through accepted activation")
        .pop()
        .expect("targeted addition should produce one candidate");
        let entity_tag = candidate
            .bundle()
            .source_bindings_for_tests()
            .entity(&addition.entity_source)
            .expect("entity source should remain bound");
        let constraint_id = candidate
            .bundle()
            .source_bindings_for_tests()
            .constraint(entity_tag, &addition.constraint_source)
            .expect("targeted source should bind to its reserved activation");
        assert!(
            matches!(
                candidate.bundle().entity_snapshots()[&entity_tag]
                    .constraint_catalog()
                    .activation(constraint_id)
                    .map(crate::db::schema::ConstraintActivationSnapshot::kind),
                Some(ConstraintActivationKind::TargetedRule { .. })
            ),
            "targeted addition must remain pending until historical proof",
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "the stable identity, write gate, and direct promotion assertions form one lifecycle"
    )]
    fn targeted_rule_semantic_edit_stages_and_promotes_under_stable_accepted_identity() {
        let initial = targeted_rule_proposal_fixture(
            ExpectedAcceptedHead::Empty,
            "targeted-edit-initial",
            "Value",
            "Other",
            true,
            Vec::new(),
        );
        let initial_candidate = lower_initial_schema_proposal(
            &initial.proposal,
            &[ProposalStoreTarget {
                path: "test::Store",
                identity: initial.store,
            }],
        )
        .expect("initial targeted rule should lower")
        .pop()
        .expect("initial targeted rule should produce one candidate");
        let initial_bundle = initial_candidate.bundle();
        let entity_tag = initial_bundle
            .source_bindings_for_tests()
            .entity(&initial.entity_source)
            .expect("entity source should bind");
        let constraint_id = initial_bundle
            .source_bindings_for_tests()
            .constraint(entity_tag, &initial.constraint_source)
            .expect("targeted source should bind");
        let initial_snapshot = &initial_bundle.entity_snapshots()[&entity_tag];
        let initial_high_water = initial_snapshot.constraint_id_allocator().high_water();

        let edited = targeted_rule_proposal_fixture_with_operation(
            ExpectedAcceptedHead::Exact {
                revision: initial_bundle.revision().get(),
                fingerprint: ExpectedSchemaFingerprint::from_bytes([0x34; 32]),
            },
            "targeted-edit-candidate",
            "Value",
            "Other",
            "range",
            Some(SourceRuleOperation::NumericMaximumInclusive {
                value: ScalarLiteral::Nat(8),
            }),
            Vec::new(),
        );
        let candidate = lower_existing_schema_proposal(
            &edited.proposal,
            &[ExistingProposalStore {
                path: "test::Store",
                identity: initial.store,
                bundle: initial_bundle,
            }],
        )
        .expect("same-identity semantic edit should stage through accepted activation")
        .pop()
        .expect("semantic edit should produce one candidate");
        let staged = &candidate.bundle().entity_snapshots()[&entity_tag];

        assert_eq!(
            candidate
                .bundle()
                .source_bindings_for_tests()
                .constraint(entity_tag, &edited.constraint_source),
            Some(constraint_id),
        );
        assert_eq!(
            staged.constraint_id_allocator().high_water(),
            initial_high_water,
            "semantic edits must not allocate a replacement identity",
        );
        assert!(staged.constraints().iter().any(|constraint| {
            constraint.id() == constraint_id
                && matches!(
                    constraint.kind(),
                    AcceptedConstraintKind::TargetedRule { operation, .. }
                        if matches!(
                            operation.as_ref(),
                            AcceptedRuleOperation::NumericRangeInclusive { .. }
                        )
                )
        }));
        assert!(matches!(
            staged
                .constraint_catalog()
                .activation(constraint_id)
                .map(crate::db::schema::ConstraintActivationSnapshot::kind),
            Some(ConstraintActivationKind::TargetedRule { operation, .. })
                if matches!(
                    operation.as_ref(),
                    AcceptedRuleOperation::NumericMaximumInclusive { .. }
                )
        ));

        let accepted_schema = AcceptedSchemaSnapshot::try_new(staged.clone())
            .expect("staged replacement should remain accepted row authority");
        let value_catalog = AcceptedValueCatalogHandle::new(
            candidate.bundle().enum_catalog().clone(),
            candidate.bundle().composite_catalog().clone(),
            AcceptedStoreCatalogScope::new(),
            candidate.revision(),
            candidate.root().fingerprint(),
        );
        let program =
            CompiledAcceptedRowConstraints::compile(&accepted_schema, &value_catalog, [0xA4; 16])
                .expect("old accepted rule and candidate write gate should compile together");
        let target_field = match staged
            .constraint_catalog()
            .activation(constraint_id)
            .expect("replacement activation should remain staged")
            .kind()
        {
            ConstraintActivationKind::TargetedRule { target, .. } => target.root_field_id(),
            _ => panic!("replacement activation should remain targeted"),
        };
        let target_slot = staged
            .row_layout()
            .slot_for_field(target_field)
            .expect("target root should retain a row slot");
        let mut values = vec![Some(Value::Nat64(0)); staged.row_layout().allocated_slot_count()];
        values[usize::from(target_slot.get())] = Some(Value::Nat64(9));
        assert!(
            program.evaluate([0xA4; 16], &values).is_err(),
            "a row admitted by the old range must still be rejected by the staged maximum",
        );

        let promoted_catalog = staged
            .constraint_catalog()
            .clone()
            .with_directly_validated_activation(constraint_id)
            .expect("bounded proof should promote the replacement");
        assert!(promoted_catalog.activation(constraint_id).is_none());
        assert!(promoted_catalog.constraints().iter().any(|constraint| {
            constraint.id() == constraint_id
                && matches!(
                    constraint.kind(),
                    AcceptedConstraintKind::TargetedRule { operation, .. }
                        if matches!(
                            operation.as_ref(),
                            AcceptedRuleOperation::NumericMaximumInclusive { .. }
                        )
                )
        }));
    }

    #[test]
    fn targeted_rule_local_name_replacement_is_explicit_remove_and_fresh_add() {
        let initial = targeted_rule_proposal_fixture(
            ExpectedAcceptedHead::Empty,
            "targeted-rename-initial",
            "Value",
            "Other",
            true,
            Vec::new(),
        );
        let initial_candidate = lower_initial_schema_proposal(
            &initial.proposal,
            &[ProposalStoreTarget {
                path: "test::Store",
                identity: initial.store,
            }],
        )
        .expect("initial targeted rule should lower")
        .pop()
        .expect("initial targeted rule should produce one candidate");
        let initial_bundle = initial_candidate.bundle();
        let entity_tag = initial_bundle
            .source_bindings_for_tests()
            .entity(&initial.entity_source)
            .expect("entity source should bind");
        let old_id = initial_bundle
            .source_bindings_for_tests()
            .constraint(entity_tag, &initial.constraint_source)
            .expect("old targeted source should bind");
        let replacement = targeted_rule_proposal_fixture_with_operation(
            ExpectedAcceptedHead::Exact {
                revision: initial_bundle.revision().get(),
                fingerprint: ExpectedSchemaFingerprint::from_bytes([0x35; 32]),
            },
            "targeted-rename-replacement",
            "Value",
            "Other",
            "cap",
            Some(SourceRuleOperation::NumericMaximumInclusive {
                value: ScalarLiteral::Nat(8),
            }),
            vec![SchemaRemoval::Constraint {
                entity: initial.entity_source.clone(),
                constraint: initial.constraint_source.clone(),
            }],
        );
        let candidate = lower_existing_schema_proposal(
            &replacement.proposal,
            &[ExistingProposalStore {
                path: "test::Store",
                identity: initial.store,
                bundle: initial_bundle,
            }],
        )
        .expect("explicit remove/add should lower")
        .pop()
        .expect("explicit remove/add should produce one candidate");
        let new_id = candidate
            .bundle()
            .source_bindings_for_tests()
            .constraint(entity_tag, &replacement.constraint_source)
            .expect("replacement source should bind");

        assert!(new_id > old_id);
        assert_eq!(
            candidate
                .bundle()
                .source_bindings_for_tests()
                .constraint(entity_tag, &initial.constraint_source),
            None,
        );
        assert!(
            candidate.bundle().entity_snapshots()[&entity_tag]
                .constraint_catalog()
                .activation(new_id)
                .is_some(),
        );
        assert!(
            !candidate.bundle().entity_snapshots()[&entity_tag]
                .constraints()
                .iter()
                .any(|constraint| constraint.id() == old_id)
        );
    }

    #[test]
    fn existing_scalar_default_change_preserves_structural_owner_identity() {
        let (initial, entity_source, store) =
            scalar_proposal_fixture(ExpectedAcceptedHead::Empty, "existing-initial", 5);
        let initial_candidates = lower_initial_schema_proposal(
            &initial,
            &[ProposalStoreTarget {
                path: "test::Store",
                identity: store,
            }],
        )
        .expect("initial scalar proposal should lower");
        let initial_bundle = initial_candidates[0].bundle();
        let (changed, _, _) = scalar_proposal_fixture(
            ExpectedAcceptedHead::Exact {
                revision: 1,
                fingerprint: ExpectedSchemaFingerprint::from_bytes([0x44; 32]),
            },
            "existing-default-change",
            7,
        );

        let candidates = lower_existing_schema_proposal(
            &changed,
            &[ExistingProposalStore {
                path: "test::Store",
                identity: store,
                bundle: initial_bundle,
            }],
        )
        .expect("existing future-default proposal should lower");

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].revision().get(), 2);
        let changed_bundle = candidates[0].bundle();
        assert_eq!(
            changed_bundle.source_bindings_for_tests(),
            initial_bundle.source_bindings_for_tests(),
        );
        let entity_tag = changed_bundle
            .source_bindings_for_tests()
            .entity(&entity_source)
            .expect("entity source should remain bound");
        let before = initial_bundle
            .entity_snapshots()
            .get(&entity_tag)
            .expect("initial entity should exist");
        let after = changed_bundle
            .entity_snapshots()
            .get(&entity_tag)
            .expect("changed entity should exist");
        assert_eq!(after.row_layout(), before.row_layout());
        assert_eq!(after.indexes(), before.indexes());
        assert_eq!(after.constraint_catalog(), before.constraint_catalog());
        assert_ne!(after.fields(), before.fields());
    }

    #[test]
    fn existing_generated_check_addition_reserves_one_source_bound_activation() {
        let (initial, entity_source, store) = scalar_proposal_fixture_with_names(
            ExpectedAcceptedHead::Empty,
            "check-addition-initial",
            5,
            "Item",
            "score",
            false,
            Vec::new(),
        );
        let initial_candidates = lower_initial_schema_proposal(
            &initial,
            &[ProposalStoreTarget {
                path: "test::Store",
                identity: store,
            }],
        )
        .expect("initial proposal without a generated check should lower");
        let initial_bundle = initial_candidates[0].bundle();
        let (addition, _, _) = scalar_proposal_fixture(
            ExpectedAcceptedHead::Exact {
                revision: 1,
                fingerprint: ExpectedSchemaFingerprint::from_bytes([0x44; 32]),
            },
            "add-generated-check",
            5,
        );

        let stores = [ExistingProposalStore {
            path: "test::Store",
            identity: store,
            bundle: initial_bundle,
        }];
        let candidates = lower_existing_schema_proposal(&addition, &stores)
            .expect("generated check addition should lower");
        let generated = lower_generated_existing_schema_proposal(&addition, &stores)
            .expect("sealed generated check addition should lower");
        assert_eq!(generated[0].bundle(), candidates[0].bundle());
        let added_bundle = candidates[0].bundle();
        let entity_tag = added_bundle
            .source_bindings_for_tests()
            .entity(&entity_source)
            .expect("entity source should remain bound");
        let before = initial_bundle
            .entity_snapshots()
            .get(&entity_tag)
            .expect("initial entity should exist");
        let after = added_bundle
            .entity_snapshots()
            .get(&entity_tag)
            .expect("entity should survive check addition");
        let source = ConstraintSourceKey::try_new("score_non_negative")
            .expect("test constraint source should admit");
        let added_id = added_bundle
            .source_bindings_for_tests()
            .constraint(entity_tag, &source)
            .expect("new check source should bind");

        assert_eq!(after.version().get(), before.version().get() + 1);
        assert_eq!(
            after.constraint_id_allocator().high_water(),
            before.constraint_id_allocator().high_water() + 1,
        );
        assert!(after.constraint_activations().iter().any(|constraint| {
            constraint.id() == added_id
                && constraint.origin() == ConstraintOrigin::Generated
                && matches!(constraint.kind(), ConstraintActivationKind::Check { .. })
        }));
        assert_eq!(after.fields(), before.fields());
        assert_eq!(after.row_layout(), before.row_layout());
        assert_eq!(after.indexes(), before.indexes());
        assert_eq!(after.relations(), before.relations());
    }

    #[test]
    fn existing_generated_check_removal_retires_only_its_source_bound_owner() {
        let (initial, entity_source, store) =
            scalar_proposal_fixture(ExpectedAcceptedHead::Empty, "removal-initial", 5);
        let initial_candidates = lower_initial_schema_proposal(
            &initial,
            &[ProposalStoreTarget {
                path: "test::Store",
                identity: store,
            }],
        )
        .expect("initial scalar proposal should lower");
        let initial_bundle = initial_candidates[0].bundle();
        let removal = SchemaProposal::try_compose(
            vec![SchemaCapability::ACCEPTED_CHECKS],
            TargetDatabaseIdentity::from_bytes([0x11; 32]),
            SchemaSubmissionKey::try_new("remove-generated-check")
                .expect("test submission key should admit"),
            ExpectedAcceptedHead::Exact {
                revision: 1,
                fingerprint: ExpectedSchemaFingerprint::from_bytes([0x44; 32]),
            },
            Vec::new(),
            Vec::new(),
            vec![SchemaRemoval::Constraint {
                entity: entity_source.clone(),
                constraint: ConstraintSourceKey::try_new("score_non_negative")
                    .expect("test constraint source should admit"),
            }],
            None,
        )
        .expect("exact removal proposal should compose");

        let stores = [ExistingProposalStore {
            path: "test::Store",
            identity: store,
            bundle: initial_bundle,
        }];
        assert!(
            lower_generated_existing_schema_proposal(&removal, &stores).is_err(),
            "sealed generated ingress must reject explicit removals",
        );
        let candidates = lower_existing_schema_proposal(&removal, &stores)
            .expect("explicit generated check removal should lower");

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].revision().get(), 2);
        let removed_bundle = candidates[0].bundle();
        let entity_tag = initial_bundle
            .source_bindings_for_tests()
            .entity(&entity_source)
            .expect("entity source should bind");
        let before = initial_bundle
            .entity_snapshots()
            .get(&entity_tag)
            .expect("initial entity should exist");
        let after = removed_bundle
            .entity_snapshots()
            .get(&entity_tag)
            .expect("entity should survive check removal");
        let removed_id = initial_bundle
            .source_bindings_for_tests()
            .constraint(
                entity_tag,
                &ConstraintSourceKey::try_new("score_non_negative")
                    .expect("test constraint source should admit"),
            )
            .expect("initial check source should bind");

        assert_eq!(after.version().get(), before.version().get() + 1);
        assert_eq!(
            after.constraint_id_allocator(),
            before.constraint_id_allocator()
        );
        assert!(
            after
                .constraints()
                .iter()
                .all(|constraint| constraint.id() != removed_id)
        );
        assert_eq!(after.fields(), before.fields());
        assert_eq!(after.row_layout(), before.row_layout());
        assert_eq!(after.indexes(), before.indexes());
        assert_eq!(after.relations(), before.relations());
        assert_eq!(
            removed_bundle
                .source_bindings_for_tests()
                .constraint_binding_count_for_tests(entity_tag),
            0,
        );
        assert_eq!(
            removed_bundle
                .source_bindings_for_tests()
                .field_binding_count_for_tests(entity_tag),
            2,
        );
        assert_eq!(
            removed_bundle
                .source_bindings_for_tests()
                .index_binding_count_for_tests(entity_tag),
            1,
        );
    }

    #[test]
    fn existing_generated_field_removal_rejects_an_index_dependency() {
        let (initial, entity_source, store) =
            scalar_proposal_fixture(ExpectedAcceptedHead::Empty, "physical-removal-initial", 5);
        let initial_candidates = lower_initial_schema_proposal(
            &initial,
            &[ProposalStoreTarget {
                path: "test::Store",
                identity: store,
            }],
        )
        .expect("initial scalar proposal should lower");
        let removal = SchemaProposal::try_compose(
            Vec::new(),
            TargetDatabaseIdentity::from_bytes([0x11; 32]),
            SchemaSubmissionKey::try_new("remove-generated-field")
                .expect("test submission key should admit"),
            ExpectedAcceptedHead::Exact {
                revision: 1,
                fingerprint: ExpectedSchemaFingerprint::from_bytes([0x44; 32]),
            },
            Vec::new(),
            Vec::new(),
            vec![SchemaRemoval::Field {
                entity: entity_source,
                field: FieldSourceKey::try_new("score").expect("test field source should admit"),
            }],
            None,
        )
        .expect("exact physical removal proposal should compose");

        assert!(
            lower_existing_schema_proposal(
                &removal,
                &[ExistingProposalStore {
                    path: "test::Store",
                    identity: store,
                    bundle: initial_candidates[0].bundle(),
                }],
            )
            .is_err(),
            "field removal must not discard a dependent accepted index",
        );
    }

    #[test]
    fn existing_generated_index_removal_uses_stable_source_identity() {
        let (initial, entity_source, store) =
            scalar_proposal_fixture(ExpectedAcceptedHead::Empty, "index-removal-initial", 5);
        let initial_candidates = lower_initial_schema_proposal(
            &initial,
            &[ProposalStoreTarget {
                path: "test::Store",
                identity: store,
            }],
        )
        .expect("initial scalar proposal should lower");
        let initial_bundle = initial_candidates[0].bundle();
        let entity_tag = initial_bundle
            .source_bindings_for_tests()
            .entity(&entity_source)
            .expect("entity source should bind");
        let removed_id = initial_bundle
            .source_bindings_for_tests()
            .index(
                entity_tag,
                &IndexSourceKey::try_new("score_idx").expect("test index source should admit"),
            )
            .expect("index source should bind");
        let removal = SchemaProposal::try_compose(
            Vec::new(),
            TargetDatabaseIdentity::from_bytes([0x11; 32]),
            SchemaSubmissionKey::try_new("remove-generated-index")
                .expect("test submission key should admit"),
            ExpectedAcceptedHead::Exact {
                revision: 1,
                fingerprint: ExpectedSchemaFingerprint::from_bytes([0x44; 32]),
            },
            Vec::new(),
            Vec::new(),
            vec![SchemaRemoval::Index {
                entity: entity_source,
                index: IndexSourceKey::try_new("score_idx")
                    .expect("test index source should admit"),
            }],
            None,
        )
        .expect("exact index removal proposal should compose");

        let candidates = lower_existing_schema_proposal(
            &removal,
            &[ExistingProposalStore {
                path: "test::Store",
                identity: store,
                bundle: initial_bundle,
            }],
        )
        .expect("generated index removal should lower");
        let removed_bundle = candidates[0].bundle();
        let after = removed_bundle
            .entity_snapshots()
            .get(&entity_tag)
            .expect("entity should survive index removal");

        assert_eq!(after.version().get(), 2);
        assert!(
            after
                .indexes()
                .iter()
                .all(|index| index.schema_id() != removed_id)
        );
        assert_eq!(
            removed_bundle
                .source_bindings_for_tests()
                .index_binding_count_for_tests(entity_tag),
            0,
        );
    }

    #[test]
    fn existing_check_removal_and_default_change_advance_schema_version_once() {
        let (initial, _, store) =
            scalar_proposal_fixture(ExpectedAcceptedHead::Empty, "combined-change-initial", 5);
        let initial_candidates = lower_initial_schema_proposal(
            &initial,
            &[ProposalStoreTarget {
                path: "test::Store",
                identity: store,
            }],
        )
        .expect("initial scalar proposal should lower");
        let (changed, entity_source, _) = scalar_proposal_fixture_with_names(
            ExpectedAcceptedHead::Exact {
                revision: 1,
                fingerprint: ExpectedSchemaFingerprint::from_bytes([0x44; 32]),
            },
            "combined-check-removal-default-change",
            7,
            "Item",
            "score",
            false,
            vec![SchemaRemoval::Constraint {
                entity: EntitySourceKey::try_new("Item").expect("test entity source should admit"),
                constraint: ConstraintSourceKey::try_new("score_non_negative")
                    .expect("test constraint source should admit"),
            }],
        );

        let candidates = lower_existing_schema_proposal(
            &changed,
            &[ExistingProposalStore {
                path: "test::Store",
                identity: store,
                bundle: initial_candidates[0].bundle(),
            }],
        )
        .expect("combined metadata change should lower");
        let bundle = candidates[0].bundle();
        let entity_tag = bundle
            .source_bindings_for_tests()
            .entity(&entity_source)
            .expect("entity source should remain bound");
        let snapshot = bundle
            .entity_snapshots()
            .get(&entity_tag)
            .expect("entity should survive combined change");
        let before = initial_candidates[0]
            .bundle()
            .entity_snapshots()
            .get(&entity_tag)
            .expect("initial entity should exist");

        assert_eq!(snapshot.version().get(), 2);
        assert_eq!(
            bundle
                .source_bindings_for_tests()
                .constraint_binding_count_for_tests(entity_tag),
            0,
        );
        assert_ne!(snapshot.fields(), before.fields());
        assert!(matches!(
            snapshot
                .fields()
                .iter()
                .find(|field| field.name() == "score")
                .expect("score should exist")
                .insert_default(),
            SchemaInsertDefault::SlotPayload(payload) if !payload.is_empty()
        ));
    }

    struct NamedTypeKeys {
        status: TypeSourceKey,
        profile: TypeSourceKey,
        score: TypeSourceKey,
        tags: TypeSourceKey,
        roles: TypeSourceKey,
        counters: TypeSourceKey,
        pair: TypeSourceKey,
    }

    fn named_type_fragments() -> (NamedTypeKeys, TypeSourceKey, Vec<NamedTypeFragment>) {
        let keys = NamedTypeKeys {
            status: TypeSourceKey::try_new("Status").expect("type key should admit"),
            profile: TypeSourceKey::try_new("Profile").expect("type key should admit"),
            score: TypeSourceKey::try_new("Score").expect("type key should admit"),
            tags: TypeSourceKey::try_new("Tags").expect("type key should admit"),
            roles: TypeSourceKey::try_new("Roles").expect("type key should admit"),
            counters: TypeSourceKey::try_new("Counters").expect("type key should admit"),
            pair: TypeSourceKey::try_new("Pair").expect("type key should admit"),
        };
        let active = TypeSourceKey::try_new("Active").expect("variant key should admit");
        let variants = vec![
            EnumVariantFragment::new(name("Active")),
            EnumVariantFragment::with_payload(
                name("Disabled"),
                FieldType::List(Box::new(FieldType::Scalar(ScalarType::Nat16))),
            ),
        ];
        let record_fields = vec![
            RecordFieldFragment::new(
                name("label"),
                FieldType::Scalar(ScalarType::Text { max_len: Some(64) }),
                false,
            ),
            RecordFieldFragment::new(name("status"), FieldType::Named(keys.status.clone()), false),
        ];
        let types = vec![
            NamedTypeFragment::Enum(
                EnumTypeFragment::try_new(name("Status"), variants).expect("enum should admit"),
            ),
            NamedTypeFragment::Record(
                RecordTypeFragment::try_new(name("Profile"), record_fields)
                    .expect("record should admit"),
            ),
            NamedTypeFragment::newtype(name("Score"), FieldType::Scalar(ScalarType::Int64)),
            NamedTypeFragment::list(
                name("Tags"),
                FieldType::Scalar(ScalarType::Text { max_len: Some(32) }),
            ),
            NamedTypeFragment::set(
                name("Roles"),
                FieldType::Scalar(ScalarType::Text { max_len: Some(32) }),
            ),
            NamedTypeFragment::map(
                name("Counters"),
                FieldType::Scalar(ScalarType::Text { max_len: Some(32) }),
                FieldType::Scalar(ScalarType::Nat64),
            ),
            NamedTypeFragment::tuple(
                name("Pair"),
                vec![
                    TupleElementFragment::new(
                        FieldType::Scalar(ScalarType::Text { max_len: Some(32) }),
                        false,
                    ),
                    TupleElementFragment::new(FieldType::Scalar(ScalarType::Nat64), true),
                ],
            ),
        ];
        (keys, active, types)
    }

    fn named_holder_entity(
        keys: &NamedTypeKeys,
        active: TypeSourceKey,
    ) -> (EntitySourceKey, EntityFragment) {
        let entity_source = EntitySourceKey::try_new("Holder").expect("entity key should admit");
        let id_source = FieldSourceKey::try_new("id").expect("field key should admit");
        let mut fields = vec![FieldFragment::new(
            name("id"),
            FieldType::Scalar(ScalarType::Nat64),
            false,
            FieldInsertPolicy::Required,
            None,
        )];
        for (suffix, field_type) in [
            ("profile", keys.profile.clone()),
            ("score", keys.score.clone()),
            ("tags", keys.tags.clone()),
            ("roles", keys.roles.clone()),
            ("counters", keys.counters.clone()),
            ("pair", keys.pair.clone()),
        ] {
            fields.push(FieldFragment::new(
                name(suffix),
                FieldType::Named(field_type),
                false,
                FieldInsertPolicy::Required,
                None,
            ));
        }
        fields.push(FieldFragment::new(
            name("status"),
            FieldType::Named(keys.status.clone()),
            false,
            FieldInsertPolicy::Default(ScalarLiteral::EnumUnit {
                enum_type: keys.status.clone(),
                variant: active,
            }),
            None,
        ));
        let entity = EntityFragment::try_new(
            name("Holder"),
            version_one(),
            fields,
            vec![id_source],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .expect("entity should admit");
        (entity_source, entity)
    }

    fn named_other_entity(status: &TypeSourceKey) -> (EntitySourceKey, EntityFragment) {
        let source = EntitySourceKey::try_new("Other").expect("entity key should admit");
        let id = FieldSourceKey::try_new("id").expect("field key should admit");
        let entity = EntityFragment::try_new(
            name("Other"),
            version_one(),
            vec![
                FieldFragment::new(
                    name("id"),
                    FieldType::Scalar(ScalarType::Nat64),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
                FieldFragment::new(
                    name("status"),
                    FieldType::Named(status.clone()),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
            ],
            vec![id],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .expect("other entity should admit");
        (source, entity)
    }

    fn assert_primary_named_bundle(
        bundle: &super::AcceptedSchemaRevisionBundle,
        entity_source: &EntitySourceKey,
        status: &TypeSourceKey,
    ) {
        let bindings = bundle.source_bindings_for_tests();
        assert_eq!(bindings.named_type_binding_count_for_tests(), 7);
        assert_eq!(bindings.enum_variant_binding_count_for_tests(), 2);
        let status_id = match bindings
            .named_type(status)
            .expect("enum source should bind")
        {
            super::AcceptedNamedTypeIdentity::Enum(type_id) => type_id,
            super::AcceptedNamedTypeIdentity::Composite(_) => {
                panic!("status should bind to the enum catalog")
            }
        };
        assert_eq!(
            bundle
                .enum_catalog()
                .enum_type(status_id)
                .expect("status enum should exist")
                .variant_count(),
            2,
        );
        assert_payload_enum_and_nullable_tuple(bundle, status_id);
        let profile_id = bundle
            .composite_catalog()
            .type_id("Profile")
            .expect("profile composite should exist");
        let super::AcceptedCompositeShape::Record(profile_fields) = bundle
            .composite_catalog()
            .composite_type(profile_id)
            .expect("profile definition should exist")
            .shape()
        else {
            panic!("profile should remain a record")
        };
        let label_source = FieldSourceKey::try_new("label").expect("field source should admit");
        let status_source = FieldSourceKey::try_new("status").expect("field source should admit");
        let label_id = bindings
            .composite_field(profile_id, &label_source)
            .expect("label source should bind");
        let status_field_id = bindings
            .composite_field(profile_id, &status_source)
            .expect("status source should bind");
        assert_eq!(
            bindings.composite_field_binding_count_for_tests(profile_id),
            profile_fields.len(),
        );
        assert_eq!(
            profile_fields
                .iter()
                .find(|field| field.name() == "label")
                .map(super::AcceptedCompositeField::id),
            Some(label_id),
        );
        assert_eq!(
            profile_fields
                .iter()
                .find(|field| field.name() == "status")
                .map(super::AcceptedCompositeField::id),
            Some(status_field_id),
        );
        assert!(matches!(
            profile_fields[1].contract().kind(),
            super::AcceptedFieldKind::Enum { type_id } if *type_id == status_id
        ));
        for type_name in ["Score", "Tags", "Roles", "Counters", "Pair"] {
            assert!(bundle.composite_catalog().type_id(type_name).is_some());
        }
        let entity_tag = bindings
            .entity(entity_source)
            .expect("entity source should bind");
        let snapshot = bundle
            .entity_snapshots()
            .get(&entity_tag)
            .expect("entity snapshot should exist");
        assert!(
            snapshot
                .fields()
                .iter()
                .filter(|field| field.name() != "id")
                .all(|field| {
                    field.storage_decode() == super::FieldStorageDecode::CatalogValue
                        && field.leaf_codec() == super::LeafCodec::Structural
                })
        );
        assert!(matches!(
            snapshot
                .fields()
                .iter()
                .find(|field| field.name() == "status")
                .expect("status should exist")
                .insert_default(),
            SchemaInsertDefault::SlotPayload(payload) if !payload.is_empty()
        ));
    }

    fn assert_payload_enum_and_nullable_tuple(
        bundle: &super::AcceptedSchemaRevisionBundle,
        status_id: super::EnumTypeId,
    ) {
        let disabled_source =
            TypeSourceKey::try_new("Disabled").expect("variant source should admit");
        let disabled_id = bundle
            .source_bindings_for_tests()
            .enum_variant(status_id, &disabled_source)
            .expect("payload variant source should bind");
        let disabled = bundle
            .enum_catalog()
            .enum_type(status_id)
            .and_then(|definition| definition.variant(disabled_id))
            .expect("payload variant should exist");
        assert!(matches!(
            disabled.body(),
            AcceptedEnumVariantBody::Payload { contract }
                if matches!(
                    contract.kind(),
                    super::AcceptedFieldKind::List(item)
                        if matches!(item.as_ref(), super::AcceptedFieldKind::Nat16)
                )
                    && contract.storage_decode() == super::FieldStorageDecode::ByKind
        ));

        let pair_id = bundle
            .composite_catalog()
            .type_id("Pair")
            .expect("pair composite should exist");
        let super::AcceptedCompositeShape::Tuple(pair) = bundle
            .composite_catalog()
            .composite_type(pair_id)
            .expect("pair definition should exist")
            .shape()
        else {
            panic!("pair should remain a tuple")
        };
        assert!(!pair[0].nullable());
        assert!(pair[1].nullable());
    }

    #[test]
    // Keep the complete downstream schema shape visible in one regression.
    #[allow(clippy::too_many_lines)]
    fn initial_lowering_publishes_and_round_trips_collection_named_type_cycles() {
        let field_key = TypeSourceKey::try_new("FieldKey").expect("type source should admit");
        let values = TypeSourceKey::try_new("Values").expect("type source should admit");
        let field_value = TypeSourceKey::try_new("FieldValue").expect("type source should admit");
        let value = TypeSourceKey::try_new("Value").expect("type source should admit");
        let tokens = TypeSourceKey::try_new("Tokens").expect("type source should admit");
        let token_amount = TypeSourceKey::try_new("TokenAmount").expect("type source should admit");
        let tier = TypeSourceKey::try_new("Tier").expect("type source should admit");
        let claim_cost = TypeSourceKey::try_new("ClaimCost").expect("type source should admit");
        let claim_cost_tiers =
            TypeSourceKey::try_new("ClaimCostTiers").expect("type source should admit");
        let policy = TypeSourceKey::try_new("CollectionPolicy").expect("type source should admit");
        let named_types = vec![
            NamedTypeFragment::newtype(
                name("FieldKey"),
                FieldType::Scalar(ScalarType::Text { max_len: Some(64) }),
            ),
            NamedTypeFragment::map(
                name("Values"),
                FieldType::Named(field_key.clone()),
                FieldType::Named(field_value.clone()),
            ),
            NamedTypeFragment::Enum(
                EnumTypeFragment::try_new(
                    name("FieldValue"),
                    vec![
                        EnumVariantFragment::with_payload(
                            name("One"),
                            FieldType::Named(value.clone()),
                        ),
                        EnumVariantFragment::with_payload(
                            name("Many"),
                            FieldType::List(Box::new(FieldType::Named(value.clone()))),
                        ),
                    ],
                )
                .expect("field-value enum should admit"),
            ),
            NamedTypeFragment::Enum(
                EnumTypeFragment::try_new(
                    name("Value"),
                    vec![
                        EnumVariantFragment::with_payload(
                            name("Text"),
                            FieldType::Scalar(ScalarType::Text { max_len: Some(128) }),
                        ),
                        EnumVariantFragment::with_payload(
                            name("Record"),
                            FieldType::Named(values.clone()),
                        ),
                    ],
                )
                .expect("value enum should admit"),
            ),
            NamedTypeFragment::newtype(name("Tokens"), FieldType::Scalar(ScalarType::Nat64)),
            NamedTypeFragment::newtype(name("TokenAmount"), FieldType::Scalar(ScalarType::Nat64)),
            NamedTypeFragment::newtype(
                name("Tier"),
                FieldType::Scalar(ScalarType::Text { max_len: Some(32) }),
            ),
            NamedTypeFragment::Enum(
                EnumTypeFragment::try_new(
                    name("ClaimCost"),
                    vec![
                        EnumVariantFragment::new(name("Free")),
                        EnumVariantFragment::with_payload(name("Icp"), FieldType::Named(tokens)),
                        EnumVariantFragment::with_payload(
                            name("Icrc1"),
                            FieldType::Named(token_amount),
                        ),
                    ],
                )
                .expect("claim-cost enum should admit"),
            ),
            NamedTypeFragment::map(
                name("ClaimCostTiers"),
                FieldType::Named(tier),
                FieldType::Named(claim_cost.clone()),
            ),
            NamedTypeFragment::Record(
                RecordTypeFragment::try_new(
                    name("CollectionPolicy"),
                    vec![
                        RecordFieldFragment::new(
                            name("claim_cost_tiers"),
                            FieldType::Named(claim_cost_tiers),
                            false,
                        ),
                        RecordFieldFragment::new(
                            name("fallback"),
                            FieldType::Named(value.clone()),
                            true,
                        ),
                        RecordFieldFragment::new(
                            name("values"),
                            FieldType::Named(values.clone()),
                            false,
                        ),
                    ],
                )
                .expect("collection policy should admit"),
            ),
        ];
        let entity_source =
            EntitySourceKey::try_new("Collection").expect("entity source should admit");
        let id_source = FieldSourceKey::try_new("id").expect("field source should admit");
        let policy_source = FieldSourceKey::try_new("policy").expect("field source should admit");
        let rule_source = icydb_schema::RuleSourceKey::try_new("field_key_length")
            .expect("rule source should admit");
        let constraint_source =
            ConstraintSourceKey::for_targeted_field_rule(&policy_source, &field_key, &rule_source);
        let entity = EntityFragment::try_new(
            name("Collection"),
            version_one(),
            vec![
                FieldFragment::new(
                    name("id"),
                    FieldType::Scalar(ScalarType::Nat64),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
                FieldFragment::new(
                    name("policy"),
                    FieldType::Named(policy),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
            ],
            vec![id_source],
            Vec::new(),
            Vec::new(),
            vec![ConstraintFragment::targeted_rule(
                TargetedRuleFragment::new(
                    policy_source,
                    field_key.clone(),
                    name("field_key_length"),
                    SourceRuleOperation::LengthRangeInclusive { min: 1, max: 64 },
                ),
            )],
        )
        .expect("collection entity should admit");
        let fragment =
            SchemaFragment::try_new(vec![entity], named_types).expect("cyclic graph should admit");
        let store = TargetStoreIdentity::from_bytes([0x72; 32]);
        let proposal = SchemaProposal::try_compose(
            vec![
                SchemaCapability::ACCEPTED_CHECKS,
                SchemaCapability::EXACT_COMPOSITE_TYPES,
            ],
            TargetDatabaseIdentity::from_bytes([0x71; 32]),
            SchemaSubmissionKey::try_new("collection-cyclic-named-types")
                .expect("submission key should admit"),
            ExpectedAcceptedHead::Empty,
            vec![fragment],
            vec![EntityStoreAssignment::new(entity_source.clone(), store)],
            Vec::new(),
            None,
        )
        .expect("cyclic proposal should compose");

        let candidates = lower_initial_schema_proposal(
            &proposal,
            &[ProposalStoreTarget {
                path: "fixture::Store",
                identity: store,
            }],
        )
        .expect("cyclic named types should lower");
        let candidate = &candidates[0];
        let recovered = super::CandidateSchemaRevision::from_encoded(
            candidate.encoded_bundle().to_vec(),
            candidate.encoded_root().to_vec(),
        )
        .expect("persisted cyclic candidate should recover");
        assert_eq!(recovered.root(), candidate.root());
        assert_eq!(recovered.bundle(), candidate.bundle());
        let bundle = candidate.bundle();
        let bindings = bundle.source_bindings_for_tests();
        let entity_tag = bindings
            .entity(&entity_source)
            .expect("Collection should bind");
        let field_key_identity = bindings
            .named_type(&field_key)
            .expect("FieldKey should bind");
        let targeted_id = bindings
            .constraint(entity_tag, &constraint_source)
            .expect("cyclic targeted rule should bind");
        assert!(matches!(
            bundle.entity_snapshots()[&entity_tag]
                .constraints()
                .iter()
                .find(|constraint| constraint.id() == targeted_id)
                .map(AcceptedConstraintSnapshot::kind),
            Some(AcceptedConstraintKind::TargetedRule { target, .. })
                if target.target_type() == field_key_identity
        ));
        let values_composite_id = match bindings.named_type(&values).expect("Values should bind") {
            AcceptedNamedTypeIdentity::Composite(type_id) => type_id,
            AcceptedNamedTypeIdentity::Enum(_) => panic!("Values should be composite"),
        };
        let field_value_id = match bindings
            .named_type(&field_value)
            .expect("FieldValue should bind")
        {
            AcceptedNamedTypeIdentity::Enum(type_id) => type_id,
            AcceptedNamedTypeIdentity::Composite(_) => panic!("FieldValue should be enum"),
        };
        let value_enum_id = match bindings.named_type(&value).expect("Value should bind") {
            AcceptedNamedTypeIdentity::Enum(type_id) => type_id,
            AcceptedNamedTypeIdentity::Composite(_) => panic!("Value should be enum"),
        };
        let AcceptedCompositeShape::Newtype(values_contract) = bundle
            .composite_catalog()
            .composite_type(values_composite_id)
            .expect("Values should exist")
            .shape()
        else {
            panic!("Values should lower as one map newtype")
        };
        assert!(matches!(
            values_contract.kind(),
            super::AcceptedFieldKind::Map { value, .. }
                if matches!(
                    value.as_ref(),
                    super::AcceptedFieldKind::Enum { type_id } if *type_id == field_value_id
                )
        ));
        let field_value_definition = bundle
            .enum_catalog()
            .enum_type(field_value_id)
            .expect("FieldValue should exist");
        for variant_name in ["One", "Many"] {
            let variant = field_value_definition
                .variant(
                    field_value_definition
                        .variant_id(variant_name)
                        .expect("variant should exist"),
                )
                .expect("variant definition should exist");
            assert!(matches!(
                variant.body(),
                AcceptedEnumVariantBody::Payload { contract }
                    if match contract.kind() {
                        super::AcceptedFieldKind::Enum { type_id } => {
                            variant_name == "One" && *type_id == value_enum_id
                        }
                        super::AcceptedFieldKind::List(item) => {
                            variant_name == "Many"
                                && matches!(
                                    item.as_ref(),
                                    super::AcceptedFieldKind::Enum { type_id }
                                        if *type_id == value_enum_id
                                )
                        }
                        _ => false,
                    }
            ));
        }
        let record_variant = bundle
            .enum_catalog()
            .enum_type(value_enum_id)
            .and_then(|definition| definition.variant_id("Record").map(|id| (definition, id)))
            .and_then(|(definition, id)| definition.variant(id))
            .expect("Value::Record should exist");
        assert!(matches!(
            record_variant.body(),
            AcceptedEnumVariantBody::Payload { contract }
                if matches!(
                    contract.kind(),
                    super::AcceptedFieldKind::Composite { type_id }
                        if *type_id == values_composite_id
                )
        ));

        let claim_cost_id = match bindings
            .named_type(&claim_cost)
            .expect("ClaimCost should bind")
        {
            AcceptedNamedTypeIdentity::Enum(type_id) => type_id,
            AcceptedNamedTypeIdentity::Composite(_) => panic!("ClaimCost should be enum"),
        };
        let claim_cost_definition = bundle
            .enum_catalog()
            .enum_type(claim_cost_id)
            .expect("ClaimCost should exist");
        assert!(matches!(
            claim_cost_definition
                .variant(
                    claim_cost_definition
                        .variant_id("Free")
                        .expect("Free should exist")
                )
                .expect("Free definition should exist")
                .body(),
            AcceptedEnumVariantBody::Unit
        ));
        for variant_name in ["Icp", "Icrc1"] {
            assert!(matches!(
                claim_cost_definition
                    .variant(
                        claim_cost_definition
                            .variant_id(variant_name)
                            .expect("payload variant should exist")
                    )
                    .expect("payload variant definition should exist")
                    .body(),
                AcceptedEnumVariantBody::Payload { contract }
                    if matches!(
                        contract.kind(),
                        super::AcceptedFieldKind::Composite { .. }
                    )
            ));
        }

        let entity_tag = bindings
            .entity(&entity_source)
            .expect("collection entity should bind");
        let policy_field = bundle
            .entity_snapshots()
            .get(&entity_tag)
            .and_then(|snapshot| {
                snapshot
                    .fields()
                    .iter()
                    .find(|field| field.name() == "policy")
            })
            .expect("policy field should exist");
        let field_contract = AcceptedFieldDecodeContract::new(
            policy_field.name(),
            policy_field.kind(),
            policy_field.nullable(),
            policy_field.storage_decode(),
            policy_field.leaf_codec(),
        );
        let payload_enum = |variant: &str, payload: InputValue| {
            InputValue::loose_enum(variant)
                .with_enum_payload(payload)
                .expect("an enum input should accept one recursive payload")
        };
        let finite_value = InputValue::map(vec![
            (
                InputValue::from("claim_cost_tiers"),
                InputValue::map(vec![
                    (InputValue::from("free"), InputValue::loose_enum("Free")),
                    (
                        InputValue::from("gold"),
                        payload_enum("Icp", InputValue::nat64(10)),
                    ),
                    (
                        InputValue::from("silver"),
                        payload_enum("Icrc1", InputValue::nat64(20)),
                    ),
                ]),
            ),
            (InputValue::from("fallback"), InputValue::null()),
            (
                InputValue::from("values"),
                InputValue::map(vec![(
                    InputValue::from("root"),
                    payload_enum(
                        "One",
                        payload_enum(
                            "Record",
                            InputValue::map(vec![(
                                InputValue::from("nested"),
                                payload_enum(
                                    "Many",
                                    InputValue::list(vec![payload_enum(
                                        "Text",
                                        InputValue::from("leaf"),
                                    )]),
                                ),
                            )]),
                        ),
                    ),
                )]),
            ),
        ]);
        let mut budget = ValueAdmissionBudget::standard();
        let encoded = encode_input_value_for_candidate_field_contract(
            bundle.enum_catalog(),
            bundle.composite_catalog(),
            field_contract,
            finite_value,
            &mut budget,
        )
        .expect("finite cyclic-contract value should encode");
        validate_default_payload_for_accepted_field_contract(
            bundle.enum_catalog(),
            bundle.composite_catalog(),
            field_contract,
            &encoded,
        )
        .expect("encoded finite value should validate against accepted catalogs");
        assert!(
            decode_canonical_value_storage_bytes(&encoded).is_ok(),
            "finite cyclic-contract value should use the bounded canonical wire",
        );

        let values_kind = super::AcceptedFieldKind::Composite {
            type_id: values_composite_id,
        };
        let values_contract = AcceptedValueContract::from_candidate_catalogs(
            bundle.enum_catalog(),
            bundle.composite_catalog(),
            &values_kind,
            super::FieldStorageDecode::CatalogValue,
        )
        .expect("Values contract should resolve");
        let mut nested = PublicValue::Enum(
            PublicEnumValue::loose("Text").with_payload(PublicValue::Text("leaf".to_string())),
        );
        for level in 0..=super::MAX_ACCEPTED_RECURSIVE_DEPTH {
            nested = PublicValue::Enum(PublicEnumValue::loose("Record").with_payload(
                PublicValue::Map(vec![(
                    PublicValue::Text(format!("level-{level}")),
                    PublicValue::Enum(PublicEnumValue::loose("One").with_payload(nested)),
                )]),
            ));
        }
        let mut budget = ValueAdmissionBudget::standard();
        let error = normalize_candidate_value(
            bundle.enum_catalog(),
            bundle.composite_catalog(),
            &values_contract,
            InputValue::from_public(PublicValue::Map(vec![(
                PublicValue::Text("root".to_string()),
                PublicValue::Enum(PublicEnumValue::loose("One").with_payload(nested)),
            )])),
            &mut budget,
        )
        .expect_err("an excessive runtime value depth should reject");
        assert_eq!(error, ValueAdmissionError::DepthExceeded);
    }

    #[test]
    // Keep both named definitions and the resulting leaf projection together.
    #[allow(clippy::too_many_lines)]
    fn initial_lowering_cuts_mutual_record_leaf_expansion_at_the_resolved_back_edge() {
        let left = TypeSourceKey::try_new("Left").expect("type source should admit");
        let right = TypeSourceKey::try_new("Right").expect("type source should admit");
        let named_types = vec![
            NamedTypeFragment::Record(
                RecordTypeFragment::try_new(
                    name("Left"),
                    vec![RecordFieldFragment::new(
                        name("right"),
                        FieldType::Named(right),
                        false,
                    )],
                )
                .expect("left record should admit"),
            ),
            NamedTypeFragment::Record(
                RecordTypeFragment::try_new(
                    name("Right"),
                    vec![RecordFieldFragment::new(
                        name("left"),
                        FieldType::Named(left.clone()),
                        false,
                    )],
                )
                .expect("right record should admit"),
            ),
        ];
        let entity_source =
            EntitySourceKey::try_new("CycleHolder").expect("entity source should admit");
        let id_source = FieldSourceKey::try_new("id").expect("field source should admit");
        let entity = EntityFragment::try_new(
            name("CycleHolder"),
            version_one(),
            vec![
                FieldFragment::new(
                    name("id"),
                    FieldType::Scalar(ScalarType::Nat64),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
                FieldFragment::new(
                    name("left"),
                    FieldType::Named(left),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
            ],
            vec![id_source],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .expect("cycle holder should admit");
        let store = TargetStoreIdentity::from_bytes([0x82; 32]);
        let proposal = SchemaProposal::try_compose(
            vec![SchemaCapability::EXACT_COMPOSITE_TYPES],
            TargetDatabaseIdentity::from_bytes([0x81; 32]),
            SchemaSubmissionKey::try_new("mutual-record-cycle")
                .expect("submission key should admit"),
            ExpectedAcceptedHead::Empty,
            vec![
                SchemaFragment::try_new(vec![entity], named_types)
                    .expect("resolved record cycle should compose"),
            ],
            vec![EntityStoreAssignment::new(entity_source.clone(), store)],
            Vec::new(),
            None,
        )
        .expect("record-cycle proposal should compose");

        let candidates = lower_initial_schema_proposal(
            &proposal,
            &[ProposalStoreTarget {
                path: "cycle::Store",
                identity: store,
            }],
        )
        .expect("record cycle should lower and survive bundle round-trip");
        let bundle = candidates[0].bundle();
        let entity_tag = bundle
            .source_bindings_for_tests()
            .entity(&entity_source)
            .expect("cycle holder should bind");
        let left_field = bundle
            .entity_snapshots()
            .get(&entity_tag)
            .and_then(|snapshot| {
                snapshot
                    .fields()
                    .iter()
                    .find(|field| field.name() == "left")
            })
            .expect("left field should exist");
        assert_eq!(
            left_field
                .nested_leaves()
                .iter()
                .map(super::PersistedNestedLeafSnapshot::path)
                .collect::<Vec<_>>(),
            [
                ["right".to_string()].as_slice(),
                ["right".to_string(), "left".to_string()].as_slice(),
            ],
        );
    }

    #[test]
    fn initial_lowering_freezes_reachable_named_type_shapes_and_enum_defaults() {
        let (keys, active, named_types) = named_type_fragments();
        let (entity_source, entity) = named_holder_entity(&keys, active);
        let (other_entity_source, other_entity) = named_other_entity(&keys.status);
        let fragment = SchemaFragment::try_new(vec![entity, other_entity], named_types)
            .expect("fragment should admit");
        let store = TargetStoreIdentity::from_bytes([0x22; 32]);
        let other_store = TargetStoreIdentity::from_bytes([0x33; 32]);
        let proposal = SchemaProposal::try_compose(
            vec![
                SchemaCapability::EXACT_COMPOSITE_TYPES,
                SchemaCapability::INSERT_DEFAULTS,
            ],
            TargetDatabaseIdentity::from_bytes([0x11; 32]),
            SchemaSubmissionKey::try_new("named-type-lowering")
                .expect("test submission key should admit"),
            ExpectedAcceptedHead::Empty,
            vec![fragment],
            vec![
                EntityStoreAssignment::new(entity_source.clone(), store),
                EntityStoreAssignment::new(other_entity_source.clone(), other_store),
            ],
            Vec::new(),
            None,
        )
        .expect("test proposal should compose");
        let targets = [
            ProposalStoreTarget {
                path: "test::Store",
                identity: store,
            },
            ProposalStoreTarget {
                path: "test::OtherStore",
                identity: other_store,
            },
        ];
        let candidates =
            lower_initial_schema_proposal(&proposal, &targets).expect("named types should lower");
        let bundle = candidates
            .iter()
            .find(|candidate| candidate.store_path() == "test::Store")
            .expect("primary store candidate should exist")
            .bundle();
        assert_primary_named_bundle(bundle, &entity_source, &keys.status);

        let other_bundle = candidates
            .iter()
            .find(|candidate| candidate.store_path() == "test::OtherStore")
            .expect("other store candidate should exist")
            .bundle();
        assert_eq!(
            other_bundle
                .source_bindings_for_tests()
                .named_type_binding_count_for_tests(),
            1,
            "each store should freeze only its complete reachable type closure"
        );
        assert!(
            other_bundle
                .source_bindings_for_tests()
                .entity(&other_entity_source)
                .is_some()
        );
        assert!(
            other_bundle
                .composite_catalog()
                .type_id("Profile")
                .is_none()
        );
    }

    #[test]
    fn existing_named_type_redeclaration_accepts_exact_current_definitions() {
        let (keys, active, named_types) = named_type_fragments();
        let (entity_source, entity) = named_holder_entity(&keys, active);
        let store = TargetStoreIdentity::from_bytes([0x35; 32]);
        let initial = SchemaProposal::try_compose(
            vec![
                SchemaCapability::EXACT_COMPOSITE_TYPES,
                SchemaCapability::INSERT_DEFAULTS,
            ],
            TargetDatabaseIdentity::from_bytes([0x25; 32]),
            SchemaSubmissionKey::try_new("exact-named-type-initial")
                .expect("initial submission key should admit"),
            ExpectedAcceptedHead::Empty,
            vec![
                SchemaFragment::try_new(vec![entity], named_types)
                    .expect("initial named-type fragment should admit"),
            ],
            vec![EntityStoreAssignment::new(entity_source.clone(), store)],
            Vec::new(),
            None,
        )
        .expect("initial named-type proposal should compose");
        let initial_candidates = lower_initial_schema_proposal(
            &initial,
            &[ProposalStoreTarget {
                path: "test::Store",
                identity: store,
            }],
        )
        .expect("initial named-type proposal should lower");

        let (keys, active, named_types) = named_type_fragments();
        let (_, entity) = named_holder_entity(&keys, active);
        let exact = SchemaProposal::try_compose(
            vec![
                SchemaCapability::EXACT_COMPOSITE_TYPES,
                SchemaCapability::INSERT_DEFAULTS,
            ],
            TargetDatabaseIdentity::from_bytes([0x25; 32]),
            SchemaSubmissionKey::try_new("exact-named-type-redeclaration")
                .expect("existing submission key should admit"),
            ExpectedAcceptedHead::Exact {
                revision: 1,
                fingerprint: ExpectedSchemaFingerprint::from_bytes([0x45; 32]),
            },
            vec![
                SchemaFragment::try_new(vec![entity], named_types)
                    .expect("exact named-type fragment should admit"),
            ],
            vec![EntityStoreAssignment::new(entity_source, store)],
            Vec::new(),
            None,
        )
        .expect("exact named-type proposal should compose");

        let candidates = lower_existing_schema_proposal(
            &exact,
            &[ExistingProposalStore {
                path: "test::Store",
                identity: store,
                bundle: initial_candidates[0].bundle(),
            }],
        )
        .expect("exact current named-type definitions should remain valid");

        assert!(
            candidates.is_empty(),
            "an exact named-type redeclaration must remain a no-op"
        );
    }

    fn recursive_type_removal_proposal(
        submission_key: &str,
        removals: Vec<SchemaRemoval>,
    ) -> SchemaProposal {
        SchemaProposal::try_compose(
            vec![SchemaCapability::EXACT_COMPOSITE_TYPES],
            TargetDatabaseIdentity::from_bytes([0x91; 32]),
            SchemaSubmissionKey::try_new(submission_key)
                .expect("recursive removal submission key should admit"),
            ExpectedAcceptedHead::Exact {
                revision: 1,
                fingerprint: ExpectedSchemaFingerprint::from_bytes([0x92; 32]),
            },
            Vec::new(),
            Vec::new(),
            removals,
            None,
        )
        .expect("recursive removal proposal should compose")
    }

    fn recursive_holder_entity(
        name_value: &str,
        root_type: &TypeSourceKey,
    ) -> (EntitySourceKey, EntityFragment) {
        let entity_source =
            EntitySourceKey::try_new(name_value).expect("entity source should admit");
        let id_source = FieldSourceKey::try_new("id").expect("id field source should admit");
        let entity = EntityFragment::try_new(
            name(name_value),
            version_one(),
            vec![
                FieldFragment::new(
                    name("id"),
                    FieldType::Scalar(ScalarType::Nat64),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
                FieldFragment::new(
                    name("root"),
                    FieldType::Named(root_type.clone()),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
            ],
            vec![id_source],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .expect("recursive holder should admit");
        (entity_source, entity)
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "the regression proves external-reference rejection, partial-cycle rejection, and multi-store component removal together"
    )]
    fn existing_recursive_named_type_removal_is_closed_and_store_local() {
        let node_type = TypeSourceKey::try_new("Node").expect("node type source should admit");
        let record_type =
            TypeSourceKey::try_new("Record").expect("record type source should admit");
        let named_types = vec![
            NamedTypeFragment::Enum(
                EnumTypeFragment::try_new(
                    name("Node"),
                    vec![
                        EnumVariantFragment::new(name("End")),
                        EnumVariantFragment::with_payload(
                            name("Record"),
                            FieldType::Named(record_type.clone()),
                        ),
                    ],
                )
                .expect("recursive enum should admit"),
            ),
            NamedTypeFragment::Record(
                RecordTypeFragment::try_new(
                    name("Record"),
                    vec![RecordFieldFragment::new(
                        name("next"),
                        FieldType::Named(node_type.clone()),
                        false,
                    )],
                )
                .expect("recursive record should admit"),
            ),
        ];
        let (left_source, left) = recursive_holder_entity("LeftHolder", &record_type);
        let (right_source, right) = recursive_holder_entity("RightHolder", &record_type);
        let left_store = TargetStoreIdentity::from_bytes([0x93; 32]);
        let right_store = TargetStoreIdentity::from_bytes([0x94; 32]);
        let initial = SchemaProposal::try_compose(
            vec![SchemaCapability::EXACT_COMPOSITE_TYPES],
            TargetDatabaseIdentity::from_bytes([0x91; 32]),
            SchemaSubmissionKey::try_new("recursive-component-initial")
                .expect("initial submission key should admit"),
            ExpectedAcceptedHead::Empty,
            vec![
                SchemaFragment::try_new(vec![left, right], named_types)
                    .expect("recursive component fragment should admit"),
            ],
            vec![
                EntityStoreAssignment::new(left_source.clone(), left_store),
                EntityStoreAssignment::new(right_source.clone(), right_store),
            ],
            Vec::new(),
            None,
        )
        .expect("recursive component proposal should compose");
        let targets = [
            ProposalStoreTarget {
                path: "cycle::LeftStore",
                identity: left_store,
            },
            ProposalStoreTarget {
                path: "cycle::RightStore",
                identity: right_store,
            },
        ];
        let initial_candidates =
            lower_initial_schema_proposal(&initial, &targets).expect("cycle should lower");
        let initial_left = initial_candidates
            .iter()
            .find(|candidate| candidate.store_path() == "cycle::LeftStore")
            .expect("left candidate should exist");
        let initial_right = initial_candidates
            .iter()
            .find(|candidate| candidate.store_path() == "cycle::RightStore")
            .expect("right candidate should exist");
        let remove_types = recursive_type_removal_proposal(
            "remove-referenced-cycle",
            vec![
                SchemaRemoval::Type(node_type.clone()),
                SchemaRemoval::Type(record_type.clone()),
            ],
        );
        assert!(
            lower_existing_schema_proposal(
                &remove_types,
                &[
                    ExistingProposalStore {
                        path: "cycle::LeftStore",
                        identity: left_store,
                        bundle: initial_left.bundle(),
                    },
                    ExistingProposalStore {
                        path: "cycle::RightStore",
                        identity: right_store,
                        bundle: initial_right.bundle(),
                    },
                ],
            )
            .is_err(),
            "entity fields must keep the recursive component live",
        );

        let remove_left = recursive_type_removal_proposal(
            "remove-left-holder",
            vec![SchemaRemoval::Entity(left_source)],
        );
        let left_without_entity = lower_existing_schema_proposal(
            &remove_left,
            &[
                ExistingProposalStore {
                    path: "cycle::LeftStore",
                    identity: left_store,
                    bundle: initial_left.bundle(),
                },
                ExistingProposalStore {
                    path: "cycle::RightStore",
                    identity: right_store,
                    bundle: initial_right.bundle(),
                },
            ],
        )
        .expect("left entity removal should lower")
        .pop()
        .expect("left entity removal should produce one candidate");
        let remove_right = recursive_type_removal_proposal(
            "remove-right-holder",
            vec![SchemaRemoval::Entity(right_source)],
        );
        let right_without_entity = lower_existing_schema_proposal(
            &remove_right,
            &[
                ExistingProposalStore {
                    path: "cycle::LeftStore",
                    identity: left_store,
                    bundle: left_without_entity.bundle(),
                },
                ExistingProposalStore {
                    path: "cycle::RightStore",
                    identity: right_store,
                    bundle: initial_right.bundle(),
                },
            ],
        )
        .expect("right entity removal should lower")
        .pop()
        .expect("right entity removal should produce one candidate");
        let stores_without_entities = [
            ExistingProposalStore {
                path: "cycle::LeftStore",
                identity: left_store,
                bundle: left_without_entity.bundle(),
            },
            ExistingProposalStore {
                path: "cycle::RightStore",
                identity: right_store,
                bundle: right_without_entity.bundle(),
            },
        ];
        for (submission_key, source) in [
            ("remove-node-only", node_type),
            ("remove-record-only", record_type),
        ] {
            let partial =
                recursive_type_removal_proposal(submission_key, vec![SchemaRemoval::Type(source)]);
            assert!(
                lower_existing_schema_proposal(&partial, &stores_without_entities).is_err(),
                "one retained cycle member must prevent partial removal",
            );
        }

        let removed = lower_existing_schema_proposal(&remove_types, &stores_without_entities)
            .expect("the complete unreferenced cycle should be removed");
        assert_eq!(removed.len(), 2);
        for candidate in removed {
            let bundle = candidate.bundle();
            assert_eq!(
                bundle
                    .source_bindings_for_tests()
                    .named_type_binding_count_for_tests(),
                0,
            );
            assert!(bundle.enum_catalog().type_id("Node").is_none());
            assert!(bundle.composite_catalog().type_id("Record").is_none());
        }
    }
}
