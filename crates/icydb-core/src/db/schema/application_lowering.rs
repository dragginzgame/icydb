//! Module: db::schema::application_lowering
//! Responsibility: lower source-keyed initial proposals into accepted catalog candidates.
//! Does not own: optimistic admission, durable receipts, publication, or activation progress.
//! Boundary: validated public proposal plus target-store routing -> catalog-native candidates.

use std::collections::{BTreeMap, BTreeSet};

use icydb_schema::{
    ConstraintSourceKey, EntityFragment, EntitySourceKey, FieldInsertPolicy, FieldManagementPolicy,
    FieldSourceKey, FieldType, IndexKeyFragment, NamedTypeFragment, ScalarType, SchemaProposal,
    SchemaRemoval, TargetStoreIdentity, TypeSourceKey,
};

use crate::{
    db::{
        data::encode_input_value_for_candidate_field_contract,
        schema::{
            AcceptedConstraintCatalog, AcceptedConstraintKind, AcceptedEnumCatalog,
            AcceptedFieldDecodeContract, AcceptedFieldKind, AcceptedNamedTypeIdentity,
            AcceptedSchemaFingerprint, AcceptedSchemaRevision, AcceptedSchemaRevisionBundle,
            AcceptedSourceBindingCatalog, AcceptedStoreCatalogScope, AcceptedValueCatalogHandle,
            CandidateSchemaRevision, ConstraintId, ConstraintOrigin, FieldId,
            MAX_ACCEPTED_RECURSIVE_DEPTH, PersistedFieldOrigin, PersistedFieldSnapshot,
            PersistedIndexExpressionOp, PersistedIndexExpressionSnapshot,
            PersistedIndexFieldPathSnapshot, PersistedIndexKeyItemSnapshot,
            PersistedIndexKeySnapshot, PersistedIndexSnapshot, PersistedNestedLeafSnapshot,
            PersistedRelationEdgeSnapshot, PersistedSchemaSnapshot, RelationId, RowLayoutVersion,
            SchemaFieldSlot, SchemaFieldWritePolicy, SchemaHistoricalFill, SchemaIndexId,
            SchemaInsertDefault, SchemaRowLayout, SchemaVersion, ValueAdmissionBudget,
            bind_source_check_expr,
            composite_catalog::{
                AcceptedCompositeCatalog, AcceptedCompositeElement, AcceptedCompositeField,
                AcceptedCompositeShape, CompositeFieldId, CompositeTypeId,
            },
            render_accepted_check_expr_sql, source_literal_input,
        },
    },
    error::InternalError,
    model::field::{
        FieldInsertGeneration, FieldStorageDecode, FieldWriteManagement, LeafCodec, ScalarCodec,
    },
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
    let mut pending = entities
        .iter()
        .flat_map(|entity| entity.fields())
        .filter_map(|field| match field.field_type() {
            FieldType::Named(source) => Some(source.clone()),
            FieldType::Scalar(_) => None,
        })
        .collect::<Vec<_>>();
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
        NamedTypeFragment::Enum(_) => {}
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
                collect_field_type_dependency(member, pending);
            }
        }
    }
}

fn collect_field_type_dependency(field_type: &FieldType, pending: &mut Vec<TypeSourceKey>) {
    if let FieldType::Named(source) = field_type {
        pending.push(source.clone());
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
            variants.insert(variant_id, variant.name().as_str().to_string());
            variant_bindings.insert((*type_id, variant.source_key().clone()), variant_id);
        }
        definitions.insert(*type_id, (definition.name().as_str().to_string(), variants));
    }
    let catalog = AcceptedEnumCatalog::from_initial_unit_definitions(definitions)
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
                            lower_field_type(field.field_type(), |source| {
                                bindings.get(source).copied()
                            })?,
                            field.nullable(),
                        ),
                    ))
                })
                .collect::<Result<Vec<_>, InternalError>>()?;
            fields.sort_by(|left, right| left.name().cmp(right.name()));
            AcceptedCompositeShape::Record(fields)
        }
        NamedTypeFragment::Enum(_) => return Err(InternalError::store_invariant()),
        NamedTypeFragment::Newtype { inner, .. } => {
            AcceptedCompositeShape::Newtype(AcceptedCompositeElement::new(
                lower_field_type(inner, |source| bindings.get(source).copied())?,
                false,
            ))
        }
        NamedTypeFragment::List { item, .. } => {
            AcceptedCompositeShape::Newtype(AcceptedCompositeElement::new(
                AcceptedFieldKind::List(Box::new(lower_field_type(item, |source| {
                    bindings.get(source).copied()
                })?)),
                false,
            ))
        }
        NamedTypeFragment::Set { item, .. } => {
            AcceptedCompositeShape::Newtype(AcceptedCompositeElement::new(
                AcceptedFieldKind::Set(Box::new(lower_field_type(item, |source| {
                    bindings.get(source).copied()
                })?)),
                false,
            ))
        }
        NamedTypeFragment::Map { key, value, .. } => {
            AcceptedCompositeShape::Newtype(AcceptedCompositeElement::new(
                AcceptedFieldKind::Map {
                    key: Box::new(lower_field_type(key, |source| {
                        bindings.get(source).copied()
                    })?),
                    value: Box::new(lower_field_type(value, |source| {
                        bindings.get(source).copied()
                    })?),
                },
                false,
            ))
        }
        NamedTypeFragment::Tuple { members, .. } => AcceptedCompositeShape::Tuple(
            members
                .iter()
                .map(|member| {
                    Ok(AcceptedCompositeElement::new(
                        lower_field_type(member, |source| bindings.get(source).copied())?,
                        false,
                    ))
                })
                .collect::<Result<Vec<_>, InternalError>>()?,
        ),
    };
    Ok(shape)
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

    let store_paths = stores
        .iter()
        .map(|store| (store.identity, store.path))
        .collect::<BTreeMap<_, _>>();
    let assignments = proposal
        .assignments()
        .iter()
        .map(|assignment| {
            store_paths
                .get(&assignment.store())
                .copied()
                .map(|path| (assignment.entity().clone(), path))
                .ok_or_else(InternalError::store_unsupported)
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let entities = proposal
        .fragments()
        .iter()
        .flat_map(icydb_schema::SchemaFragment::entities)
        .map(|entity| (entity.source_key().clone(), entity))
        .collect::<BTreeMap<_, _>>();
    let types = proposal
        .fragments()
        .iter()
        .flat_map(icydb_schema::SchemaFragment::types)
        .map(|r#type| (r#type.source_key().clone(), r#type))
        .collect::<BTreeMap<_, _>>();
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
        store_entities.sort_by(|left, right| left.source_key().cmp(right.source_key()));
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
    candidates.sort_by(|left, right| left.store_path().cmp(right.store_path()));
    Ok(candidates)
}

/// Lower an exact proposal against a non-empty accepted head.
///
/// This existing-head lane owns future insert-default and source-keyed display
/// metadata reconciliation, plus explicit removal of accepted generated
/// checks and addition of generated checks whose complete historical domain is
/// proven empty at the application boundary. Every structural fact must
/// resolve through immutable source bindings and match accepted authority
/// exactly. Other additions, activation work, and physical changes therefore
/// fail before candidate construction instead of falling back to
/// generated-model reconciliation.
pub(in crate::db::schema) fn lower_existing_schema_proposal(
    proposal: &SchemaProposal,
    stores: &[ExistingProposalStore<'_>],
) -> Result<Vec<CandidateSchemaRevision>, InternalError> {
    let store_by_identity = stores
        .iter()
        .map(|store| (store.identity, store))
        .collect::<BTreeMap<_, _>>();
    let entities = proposal
        .fragments()
        .iter()
        .flat_map(icydb_schema::SchemaFragment::entities)
        .map(|entity| (entity.source_key().clone(), entity))
        .collect::<BTreeMap<_, _>>();
    let types = proposal
        .fragments()
        .iter()
        .flat_map(icydb_schema::SchemaFragment::types)
        .map(|r#type| (r#type.source_key().clone(), r#type))
        .collect::<BTreeMap<_, _>>();
    if entities.len() != proposal.assignments().len() {
        return Err(InternalError::store_unsupported());
    }

    let mut entities_by_store =
        BTreeMap::<&'static str, (&ExistingProposalStore<'_>, Vec<&EntityFragment>)>::new();
    let mut check_removals_by_store = BTreeMap::<&'static str, Vec<ExistingCheckRemoval>>::new();
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
    for removal in proposal.removals() {
        let SchemaRemoval::Constraint { entity, constraint } = removal else {
            return Err(InternalError::store_unsupported());
        };
        let (store, resolved) =
            resolve_existing_generated_check_removal(stores, entity, constraint)?;
        entities_by_store
            .entry(store.path)
            .or_insert_with(|| (store, Vec::new()));
        check_removals_by_store
            .entry(store.path)
            .or_default()
            .push(resolved);
    }

    let mut used_types = BTreeSet::new();
    let mut candidates = Vec::new();
    for (_, (store, store_entities)) in entities_by_store {
        let removals = check_removals_by_store
            .remove(store.path)
            .unwrap_or_default();
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
    candidates.sort_by(|left, right| left.store_path().cmp(right.store_path()));
    Ok(candidates)
}

/// One source-resolved generated check selected for exact removal.
#[derive(Eq, Ord, PartialEq, PartialOrd)]
struct ExistingCheckRemoval {
    entity_tag: EntityTag,
    source: ConstraintSourceKey,
    id: ConstraintId,
}

fn lower_existing_store_candidate(
    store: &ExistingProposalStore<'_>,
    stores: &[ExistingProposalStore<'_>],
    mut entities: Vec<&EntityFragment>,
    mut check_removals: Vec<ExistingCheckRemoval>,
    types: &BTreeMap<TypeSourceKey, &NamedTypeFragment>,
    used_types: &mut BTreeSet<TypeSourceKey>,
) -> Result<Option<CandidateSchemaRevision>, InternalError> {
    entities.sort_by(|left, right| left.source_key().cmp(right.source_key()));
    check_removals.sort();
    let catalogs =
        lower_existing_named_catalogs(store.bundle, entities.as_slice(), types, used_types)?;
    let mut snapshots = store.bundle.entity_snapshots().clone();
    let mut source_bindings = store.bundle.source_bindings().clone();
    let removal_entity_tags = apply_existing_check_removals(
        &mut snapshots,
        &mut source_bindings,
        check_removals.as_slice(),
    )?;
    let proposed_entity_tags = entities
        .iter()
        .map(|entity| {
            store
                .bundle
                .source_bindings()
                .entity(entity.source_key())
                .ok_or_else(InternalError::store_unsupported)
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    verify_unrebuildable_enum_predicates(store.bundle, &catalogs, &proposed_entity_tags)?;
    let mut changed = !check_removals.is_empty();
    for entity in entities {
        let entity_tag = store
            .bundle
            .source_bindings()
            .entity(entity.source_key())
            .ok_or_else(InternalError::store_unsupported)?;
        let current = snapshots
            .get(&entity_tag)
            .ok_or_else(InternalError::store_invariant)?;
        if let Some(candidate) = lower_existing_entity(
            store.bundle,
            &catalogs,
            stores,
            entity,
            current,
            removal_entity_tags.contains(&entity_tag),
            &mut source_bindings,
        )? {
            snapshots.insert(entity_tag, candidate);
            changed = true;
        }
    }
    if !changed && !catalogs.changed {
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
        catalogs.enum_catalog,
        catalogs.composite_catalog,
        source_bindings,
        snapshots,
    )?;
    CandidateSchemaRevision::new(bundle).map(Some)
}

/// Resolve one removal solely through immutable accepted source identity.
///
/// A live activation or independently owned check remains outside this
/// metadata-only transition and must fail before a candidate exists.
fn resolve_existing_generated_check_removal<'store, 'bundle>(
    stores: &'store [ExistingProposalStore<'bundle>],
    entity_source: &EntitySourceKey,
    constraint_source: &ConstraintSourceKey,
) -> Result<(&'store ExistingProposalStore<'bundle>, ExistingCheckRemoval), InternalError> {
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
            || !matches!(constraint.kind(), AcceptedConstraintKind::Check { .. })
        {
            return Err(InternalError::store_unsupported());
        }
        resolved = Some((
            store,
            ExistingCheckRemoval {
                entity_tag,
                source: constraint_source.clone(),
                id: constraint_id,
            },
        ));
    }
    resolved.ok_or_else(InternalError::store_unsupported)
}

/// Remove checks and source bindings as one candidate-local identity update.
///
/// Each affected entity advances its schema version exactly once regardless
/// of how many accepted generated checks the proposal removes.
fn apply_existing_check_removals(
    snapshots: &mut BTreeMap<EntityTag, PersistedSchemaSnapshot>,
    source_bindings: &mut AcceptedSourceBindingCatalog,
    removals: &[ExistingCheckRemoval],
) -> Result<BTreeSet<EntityTag>, InternalError> {
    let mut removals_by_entity = BTreeMap::<EntityTag, Vec<&ExistingCheckRemoval>>::new();
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
                .with_removed_generated_check(removal.id)
                .map_err(|_| InternalError::store_invariant())?;
            source_bindings.remove_constraint(entity_tag, &removal.source, removal.id)?;
        }
        let version = current
            .version()
            .get()
            .checked_add(1)
            .map(SchemaVersion::new)
            .ok_or_else(InternalError::store_unsupported)?;
        snapshots.insert(
            entity_tag,
            current
                .with_constraint_catalog(catalog)
                .with_schema_version(version),
        );
        changed_entities.insert(entity_tag);
    }
    Ok(changed_entities)
}

/// Candidate value catalogs for one exact existing accepted head.
struct ExistingCatalogCandidate {
    enum_catalog: AcceptedEnumCatalog,
    composite_catalog: AcceptedCompositeCatalog,
    enum_changed: bool,
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

    let mut enum_catalog = bundle.enum_catalog().clone();
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
        let variants = proposed
            .variants()
            .iter()
            .map(|variant| {
                let variant_id = bundle
                    .source_bindings()
                    .enum_variant(type_id, variant.source_key())
                    .ok_or_else(InternalError::store_unsupported)?;
                Ok((variant_id, variant.name().as_str().to_string()))
            })
            .collect::<Result<BTreeMap<_, _>, InternalError>>()?;
        enum_catalog = enum_catalog
            .with_redeclared_unit_metadata(type_id, proposed.name().as_str().to_string(), variants)
            .map_err(|_| InternalError::store_unsupported())?;
    }
    let enum_changed = enum_catalog != *bundle.enum_catalog();

    let mut composite_catalog = bundle.composite_catalog().clone();
    for source in &visited {
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
        let accepted = composite_catalog
            .composite_type(type_id)
            .ok_or_else(InternalError::store_invariant)?;
        if accepted.shape() != &shape {
            return Err(InternalError::store_unsupported());
        }
        composite_catalog = composite_catalog
            .with_redeclared_path(
                type_id,
                definition.name().as_str().to_string(),
                &enum_catalog,
            )
            .map_err(|_| InternalError::store_unsupported())?;
    }
    let changed = enum_changed || composite_catalog != *bundle.composite_catalog();
    Ok(ExistingCatalogCandidate {
        enum_catalog,
        composite_catalog,
        enum_changed,
        changed,
    })
}

fn named_field_type_source(field_type: &FieldType) -> Option<TypeSourceKey> {
    match field_type {
        FieldType::Named(source) => Some(source.clone()),
        FieldType::Scalar(_) => None,
    }
}

fn verify_unrebuildable_enum_predicates(
    bundle: &AcceptedSchemaRevisionBundle,
    catalogs: &ExistingCatalogCandidate,
    proposed_entity_tags: &BTreeSet<EntityTag>,
) -> Result<(), InternalError> {
    if !catalogs.enum_changed {
        return Ok(());
    }
    let has_unrebuildable_predicate =
        bundle
            .entity_snapshots()
            .iter()
            .any(|(entity_tag, snapshot)| {
                snapshot
                    .indexes()
                    .iter()
                    .chain(snapshot.candidate_indexes())
                    .any(|index| {
                        index.predicate_sql().is_some()
                            && (!proposed_entity_tags.contains(entity_tag) || !index.generated())
                    })
            });
    if has_unrebuildable_predicate {
        return Err(InternalError::store_unsupported());
    }
    Ok(())
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
                            lower_field_type(field.field_type(), |source| {
                                bindings.named_type(source)
                            })?,
                            field.nullable(),
                        ),
                    ))
                })
                .collect::<Result<Vec<_>, InternalError>>()?;
            fields.sort_by(|left, right| left.name().cmp(right.name()));
            AcceptedCompositeShape::Record(fields)
        }
        NamedTypeFragment::Newtype { inner, .. } => {
            AcceptedCompositeShape::Newtype(AcceptedCompositeElement::new(
                lower_field_type(inner, |source| bindings.named_type(source))?,
                false,
            ))
        }
        NamedTypeFragment::List { item, .. } => {
            AcceptedCompositeShape::Newtype(AcceptedCompositeElement::new(
                AcceptedFieldKind::List(Box::new(lower_field_type(item, |source| {
                    bindings.named_type(source)
                })?)),
                false,
            ))
        }
        NamedTypeFragment::Set { item, .. } => {
            AcceptedCompositeShape::Newtype(AcceptedCompositeElement::new(
                AcceptedFieldKind::Set(Box::new(lower_field_type(item, |source| {
                    bindings.named_type(source)
                })?)),
                false,
            ))
        }
        NamedTypeFragment::Map { key, value, .. } => {
            AcceptedCompositeShape::Newtype(AcceptedCompositeElement::new(
                AcceptedFieldKind::Map {
                    key: Box::new(lower_field_type(key, |source| bindings.named_type(source))?),
                    value: Box::new(lower_field_type(value, |source| {
                        bindings.named_type(source)
                    })?),
                },
                false,
            ))
        }
        NamedTypeFragment::Tuple { members, .. } => AcceptedCompositeShape::Tuple(
            members
                .iter()
                .map(|member| {
                    Ok(AcceptedCompositeElement::new(
                        lower_field_type(member, |source| bindings.named_type(source))?,
                        false,
                    ))
                })
                .collect::<Result<Vec<_>, InternalError>>()?,
        ),
        NamedTypeFragment::Enum(_) => return Err(InternalError::store_unsupported()),
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

    let field_candidate = lower_existing_fields(bundle, catalogs, entity, entity_tag, current)?;
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

    let provisional = PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        current.version(),
        current.entity_path().to_string(),
        entity.name().as_str().to_string(),
        current.primary_key_field_ids().to_vec(),
        current.row_layout().clone(),
        field_candidate.fields,
        current.indexes().to_vec(),
    )
    .with_constraint_catalog(current.constraint_catalog().clone())
    .with_relations(current.relations().to_vec())
    .with_constraint_candidates(
        current.candidate_indexes().to_vec(),
        current.candidate_relations().to_vec(),
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
    let constraint_catalog = lower_existing_checks(
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
    Ok(Some(
        PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
            version,
            current.entity_path().to_string(),
            entity.name().as_str().to_string(),
            current.primary_key_field_ids().to_vec(),
            current.row_layout().clone(),
            provisional.fields().to_vec(),
            indexes,
        )
        .with_constraint_catalog(constraint_catalog)
        .with_relations(current.relations().to_vec())
        .with_constraint_candidates(
            current.candidate_indexes().to_vec(),
            current.candidate_relations().to_vec(),
        ),
    ))
}

/// Re-declared generated fields plus the metadata changes relevant to indexes.
struct ExistingFieldCandidate {
    fields: Vec<PersistedFieldSnapshot>,
    changed: bool,
    field_names_changed: bool,
}

fn lower_existing_fields(
    bundle: &AcceptedSchemaRevisionBundle,
    catalogs: &ExistingCatalogCandidate,
    entity: &EntityFragment,
    entity_tag: EntityTag,
    current: &PersistedSchemaSnapshot,
) -> Result<ExistingFieldCandidate, InternalError> {
    let generated_field_count = current
        .fields()
        .iter()
        .filter(|field| field.generated())
        .count();
    if generated_field_count != entity.fields().len() {
        return Err(InternalError::store_unsupported());
    }
    let bindings = bundle.source_bindings();
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
        let kind = lower_field_type(proposed.field_type(), |source| bindings.named_type(source))?;
        let storage_decode = match proposed.field_type() {
            FieldType::Scalar(_) => FieldStorageDecode::ByKind,
            FieldType::Named(_) => FieldStorageDecode::CatalogValue,
        };
        let leaf_codec = match proposed.field_type() {
            FieldType::Scalar(_) => scalar_leaf_codec(&kind),
            FieldType::Named(_) => LeafCodec::Structural,
        };
        let nested_leaves = lower_nested_leaves(&kind, &catalogs.composite_catalog)?;
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
        let candidate = PersistedRelationEdgeSnapshot::new(
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

fn lower_existing_checks(
    bundle: &AcceptedSchemaRevisionBundle,
    catalogs: &ExistingCatalogCandidate,
    entity: &EntityFragment,
    entity_tag: EntityTag,
    candidate: &PersistedSchemaSnapshot,
    accepted_snapshot: &PersistedSchemaSnapshot,
    bindings: &mut AcceptedSourceBindingCatalog,
) -> Result<AcceptedConstraintCatalog, InternalError> {
    let mut catalog = accepted_snapshot.constraint_catalog().clone();
    let mut declared = BTreeSet::new();
    for proposed in entity.constraints() {
        let expression = bind_source_check_expr(
            proposed.expression(),
            entity_tag,
            bindings,
            candidate,
            &catalogs.enum_catalog,
            &catalogs.composite_catalog,
        )
        .map_err(|_| InternalError::store_unsupported())?;

        if let Some(constraint_id) = bindings.constraint(entity_tag, proposed.source_key()) {
            let accepted = accepted_snapshot
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
            declared.insert(constraint_id);
            continue;
        }

        let activation_epoch = bundle
            .revision()
            .checked_next()
            .ok_or_else(InternalError::store_unsupported)?
            .get();
        catalog = catalog
            .with_added_check_activation(
                proposed.name().as_str().to_string(),
                ConstraintOrigin::Generated,
                expression,
                bundle.semantic_fingerprint()?,
                activation_epoch,
            )
            .map_err(|_| InternalError::store_unsupported())?;
        let constraint_id = ConstraintId::new(catalog.allocator().high_water())
            .ok_or_else(InternalError::store_invariant)?;
        bindings.insert_constraint(entity_tag, proposed.source_key().clone(), constraint_id)?;
        declared.insert(constraint_id);
    }

    let all_existing_generated_checks_are_declared = accepted_snapshot
        .constraints()
        .iter()
        .filter(|constraint| {
            constraint.origin() == ConstraintOrigin::Generated
                && matches!(constraint.kind(), AcceptedConstraintKind::Check { .. })
        })
        .all(|constraint| declared.contains(&constraint.id()));
    if !all_existing_generated_checks_are_declared {
        return Err(InternalError::store_unsupported());
    }
    Ok(catalog)
}

fn allocate_entity_identities(
    entities_by_store: &BTreeMap<&'static str, Vec<&EntityFragment>>,
) -> Result<BTreeMap<EntitySourceKey, EntityTag>, InternalError> {
    let mut accepted = BTreeMap::new();
    for entities in entities_by_store.values() {
        for (offset, entity) in entities.iter().enumerate() {
            let raw = u64::try_from(offset)
                .ok()
                .and_then(|value| value.checked_add(1))
                .ok_or_else(InternalError::store_unsupported)?;
            accepted.insert(entity.source_key().clone(), EntityTag::new(raw));
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
        let expression = bind_source_check_expr(
            constraint.expression(),
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
        let id = catalog
            .constraints()
            .iter()
            .find_map(|accepted| {
                (accepted.name() == constraint.name().as_str()
                    && matches!(accepted.kind(), AcceptedConstraintKind::Check { .. }))
                .then_some(accepted.id())
            })
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
        let kind = lower_field_type(field.field_type(), |source| {
            context.named_type_bindings.named_type(source)
        })?;
        let storage_decode = match field.field_type() {
            FieldType::Scalar(_) => FieldStorageDecode::ByKind,
            FieldType::Named(_) => FieldStorageDecode::CatalogValue,
        };
        let leaf_codec = match field.field_type() {
            FieldType::Scalar(_) => scalar_leaf_codec(&kind),
            FieldType::Named(_) => LeafCodec::Structural,
        };
        let nested_leaves = lower_nested_leaves(&kind, &context.composite_catalog)?;
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

fn lower_nested_leaves(
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
    for field in fields {
        push_nested_leaves(
            field.name(),
            field.contract(),
            catalog,
            &mut Vec::new(),
            &mut leaves,
            0,
        )?;
    }
    leaves.sort_by(|left, right| left.path().cmp(right.path()));
    Ok(leaves)
}

fn push_nested_leaves(
    name: &str,
    contract: &AcceptedCompositeElement,
    catalog: &AcceptedCompositeCatalog,
    path: &mut Vec<String>,
    leaves: &mut Vec<PersistedNestedLeafSnapshot>,
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
        if let AcceptedCompositeShape::Record(fields) = definition.shape() {
            for field in fields {
                push_nested_leaves(
                    field.name(),
                    field.contract(),
                    catalog,
                    path,
                    leaves,
                    depth.saturating_add(1),
                )?;
            }
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
                if local.field_type() != target_field.field_type() {
                    return Err(InternalError::store_unsupported());
                }
            }
            accepted_bindings.insert((entity_tag, relation.source_key().clone()), id);
            Ok(PersistedRelationEdgeSnapshot::new(
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

fn lower_field_type(
    field_type: &FieldType,
    resolve_named: impl FnOnce(&TypeSourceKey) -> Option<AcceptedNamedTypeIdentity>,
) -> Result<AcceptedFieldKind, InternalError> {
    let scalar = match field_type {
        FieldType::Scalar(scalar) => scalar,
        FieldType::Named(source) => {
            return resolve_named(source)
                .map(|identity| match identity {
                    AcceptedNamedTypeIdentity::Enum(type_id) => AcceptedFieldKind::Enum { type_id },
                    AcceptedNamedTypeIdentity::Composite(type_id) => {
                        AcceptedFieldKind::Composite { type_id }
                    }
                })
                .ok_or_else(InternalError::store_unsupported);
        }
    };
    Ok(match scalar {
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
    })
}

const fn scalar_leaf_codec(kind: &AcceptedFieldKind) -> LeafCodec {
    match kind {
        AcceptedFieldKind::Blob { .. } => LeafCodec::Scalar(ScalarCodec::Blob),
        AcceptedFieldKind::Bool => LeafCodec::Scalar(ScalarCodec::Bool),
        AcceptedFieldKind::Date => LeafCodec::Scalar(ScalarCodec::Date),
        AcceptedFieldKind::Duration => LeafCodec::Scalar(ScalarCodec::Duration),
        AcceptedFieldKind::Float32 => LeafCodec::Scalar(ScalarCodec::Float32),
        AcceptedFieldKind::Float64 => LeafCodec::Scalar(ScalarCodec::Float64),
        AcceptedFieldKind::Int8
        | AcceptedFieldKind::Int16
        | AcceptedFieldKind::Int32
        | AcceptedFieldKind::Int64 => LeafCodec::Scalar(ScalarCodec::Int64),
        AcceptedFieldKind::Principal => LeafCodec::Scalar(ScalarCodec::Principal),
        AcceptedFieldKind::Subaccount => LeafCodec::Scalar(ScalarCodec::Subaccount),
        AcceptedFieldKind::Text { .. } => LeafCodec::Scalar(ScalarCodec::Text),
        AcceptedFieldKind::Timestamp => LeafCodec::Scalar(ScalarCodec::Timestamp),
        AcceptedFieldKind::Nat8
        | AcceptedFieldKind::Nat16
        | AcceptedFieldKind::Nat32
        | AcceptedFieldKind::Nat64 => LeafCodec::Scalar(ScalarCodec::Nat64),
        AcceptedFieldKind::Ulid => LeafCodec::Scalar(ScalarCodec::Ulid),
        AcceptedFieldKind::Unit => LeafCodec::Scalar(ScalarCodec::Unit),
        _ => LeafCodec::Structural,
    }
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
    use super::{
        ExistingProposalStore, ProposalStoreTarget, lower_existing_schema_proposal,
        lower_initial_schema_proposal,
    };
    use crate::db::schema::{
        AcceptedConstraintKind, AcceptedNamedTypeIdentity, AcceptedSchemaRevision,
        ConstraintActivationKind, ConstraintOrigin, PersistedFieldSnapshot, SchemaInsertDefault,
        composite_catalog::AcceptedCompositeShape,
    };
    use icydb_schema::{
        ConstraintFragment, ConstraintSourceKey, EntityFragment, EntitySourceKey,
        EntityStoreAssignment, EnumTypeFragment, EnumVariantFragment, ExpectedAcceptedHead,
        ExpectedSchemaFingerprint, FieldFragment, FieldInsertPolicy, FieldSourceKey, FieldType,
        IndexFragment, IndexKeyFragment, IndexSourceKey, NamedTypeFragment, RecordFieldFragment,
        RecordTypeFragment, ScalarLiteral, ScalarType, SchemaCapability, SchemaFragment,
        SchemaName, SchemaProposal, SchemaRemoval, SchemaSubmissionKey, SourceCheckExpr,
        SourceCheckInstruction, TargetDatabaseIdentity, TargetStoreIdentity, TypeSourceKey,
    };

    fn name(value: &str) -> SchemaName {
        SchemaName::try_new(value).expect("test schema name should admit")
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
            EntitySourceKey::try_new("test:entity:item").expect("test entity source should admit");
        let id_source =
            FieldSourceKey::try_new("test:field:id").expect("test field source should admit");
        let score_source =
            FieldSourceKey::try_new("test:field:score").expect("test field source should admit");
        let check = SourceCheckExpr::try_new(vec![
            SourceCheckInstruction::Field(score_source.clone()),
            SourceCheckInstruction::Literal(ScalarLiteral::Int(0)),
            SourceCheckInstruction::GreaterThanOrEqual,
        ])
        .expect("test check should admit");
        let entity = EntityFragment::try_new(
            entity_source.clone(),
            name(entity_name),
            vec![
                FieldFragment::new(
                    id_source.clone(),
                    name("id"),
                    FieldType::Scalar(ScalarType::Nat64),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
                FieldFragment::new(
                    score_source.clone(),
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
                    IndexSourceKey::try_new("test:index:score")
                        .expect("test index source should admit"),
                    name("score_idx"),
                    vec![IndexKeyFragment::Field(score_source)],
                    false,
                    None,
                )
                .expect("test index should admit"),
            ],
            Vec::new(),
            include_check
                .then(|| {
                    ConstraintFragment::new(
                        ConstraintSourceKey::try_new("test:check:score")
                            .expect("test check source should admit"),
                        name("score_non_negative"),
                        check,
                    )
                })
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

        let candidates = lower_existing_schema_proposal(
            &addition,
            &[ExistingProposalStore {
                path: "test::Store",
                identity: store,
                bundle: initial_bundle,
            }],
        )
        .expect("generated check addition should lower");
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
        let source = ConstraintSourceKey::try_new("test:check:score")
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
                constraint: ConstraintSourceKey::try_new("test:check:score")
                    .expect("test constraint source should admit"),
            }],
        )
        .expect("exact removal proposal should compose");

        let candidates = lower_existing_schema_proposal(
            &removal,
            &[ExistingProposalStore {
                path: "test::Store",
                identity: store,
                bundle: initial_bundle,
            }],
        )
        .expect("generated check removal should lower");

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
                &ConstraintSourceKey::try_new("test:check:score")
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
    fn existing_physical_removal_still_rejects_before_candidate_construction() {
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
                field: FieldSourceKey::try_new("test:field:score")
                    .expect("test field source should admit"),
            }],
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
            "physical removal must reject until its migration owner participates",
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
                entity: EntitySourceKey::try_new("test:entity:item")
                    .expect("test entity source should admit"),
                constraint: ConstraintSourceKey::try_new("test:check:score")
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

    #[test]
    fn existing_entity_and_indexed_field_rename_relabels_generated_metadata_only() {
        let (initial, entity_source, store) =
            scalar_proposal_fixture(ExpectedAcceptedHead::Empty, "rename-initial", 5);
        let initial_candidates = lower_initial_schema_proposal(
            &initial,
            &[ProposalStoreTarget {
                path: "test::Store",
                identity: store,
            }],
        )
        .expect("initial scalar proposal should lower");
        let initial_bundle = initial_candidates[0].bundle();
        let (renamed, _, _) = scalar_proposal_fixture_with_names(
            ExpectedAcceptedHead::Exact {
                revision: 1,
                fingerprint: ExpectedSchemaFingerprint::from_bytes([0x44; 32]),
            },
            "rename-existing",
            5,
            "RenamedItem",
            "points",
            true,
            Vec::new(),
        );

        let renamed_candidates = lower_existing_schema_proposal(
            &renamed,
            &[ExistingProposalStore {
                path: "test::Store",
                identity: store,
                bundle: initial_bundle,
            }],
        )
        .expect("source-keyed entity and field renames should lower");
        let renamed_bundle = renamed_candidates[0].bundle();
        assert_eq!(
            renamed_bundle.source_bindings_for_tests(),
            initial_bundle.source_bindings_for_tests(),
        );
        let entity_tag = renamed_bundle
            .source_bindings_for_tests()
            .entity(&entity_source)
            .expect("entity source should remain bound");
        let before = initial_bundle
            .entity_snapshots()
            .get(&entity_tag)
            .expect("initial entity should exist");
        let after = renamed_bundle
            .entity_snapshots()
            .get(&entity_tag)
            .expect("renamed entity should exist");

        assert_eq!(after.entity_name(), "RenamedItem");
        assert_eq!(after.row_layout(), before.row_layout());
        assert_eq!(
            after.primary_key_field_ids(),
            before.primary_key_field_ids()
        );
        assert_eq!(
            after.indexes()[0].schema_id(),
            before.indexes()[0].schema_id()
        );
        assert_eq!(after.indexes()[0].ordinal(), before.indexes()[0].ordinal());
        assert_eq!(
            after.indexes()[0].physical_generation(),
            before.indexes()[0].physical_generation(),
        );
        assert_eq!(after.indexes()[0].key().field_paths()[0].path(), ["points"],);
        assert_eq!(after.constraint_catalog(), before.constraint_catalog());
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
        named_type_fragments_with_names("Status", "Active", "Profile", "label")
    }

    fn named_type_fragments_with_names(
        status_name: &str,
        active_name: &str,
        profile_name: &str,
        label_name: &str,
    ) -> (NamedTypeKeys, TypeSourceKey, Vec<NamedTypeFragment>) {
        let keys = NamedTypeKeys {
            status: TypeSourceKey::try_new("test:type:status").expect("type key should admit"),
            profile: TypeSourceKey::try_new("test:type:profile").expect("type key should admit"),
            score: TypeSourceKey::try_new("test:type:score").expect("type key should admit"),
            tags: TypeSourceKey::try_new("test:type:tags").expect("type key should admit"),
            roles: TypeSourceKey::try_new("test:type:roles").expect("type key should admit"),
            counters: TypeSourceKey::try_new("test:type:counters").expect("type key should admit"),
            pair: TypeSourceKey::try_new("test:type:pair").expect("type key should admit"),
        };
        let active =
            TypeSourceKey::try_new("test:variant:active").expect("variant key should admit");
        let variants = vec![
            EnumVariantFragment::new(active.clone(), name(active_name)),
            EnumVariantFragment::new(
                TypeSourceKey::try_new("test:variant:disabled").expect("variant key should admit"),
                name("Disabled"),
            ),
        ];
        let record_fields = vec![
            RecordFieldFragment::new(
                FieldSourceKey::try_new("test:record:label").expect("field key should admit"),
                name(label_name),
                FieldType::Scalar(ScalarType::Text { max_len: Some(64) }),
                false,
            ),
            RecordFieldFragment::new(
                FieldSourceKey::try_new("test:record:status").expect("field key should admit"),
                name("status"),
                FieldType::Named(keys.status.clone()),
                false,
            ),
        ];
        let types = vec![
            NamedTypeFragment::Enum(
                EnumTypeFragment::try_new(keys.status.clone(), name(status_name), variants)
                    .expect("enum should admit"),
            ),
            NamedTypeFragment::Record(
                RecordTypeFragment::try_new(
                    keys.profile.clone(),
                    name(profile_name),
                    record_fields,
                )
                .expect("record should admit"),
            ),
            NamedTypeFragment::Newtype {
                source_key: keys.score.clone(),
                name: name("Score"),
                inner: FieldType::Scalar(ScalarType::Int64),
            },
            NamedTypeFragment::List {
                source_key: keys.tags.clone(),
                name: name("Tags"),
                item: FieldType::Scalar(ScalarType::Text { max_len: Some(32) }),
            },
            NamedTypeFragment::Set {
                source_key: keys.roles.clone(),
                name: name("Roles"),
                item: FieldType::Scalar(ScalarType::Text { max_len: Some(32) }),
            },
            NamedTypeFragment::Map {
                source_key: keys.counters.clone(),
                name: name("Counters"),
                key: FieldType::Scalar(ScalarType::Text { max_len: Some(32) }),
                value: FieldType::Scalar(ScalarType::Nat64),
            },
            NamedTypeFragment::Tuple {
                source_key: keys.pair.clone(),
                name: name("Pair"),
                members: vec![
                    FieldType::Scalar(ScalarType::Text { max_len: Some(32) }),
                    FieldType::Scalar(ScalarType::Nat64),
                ],
            },
        ];
        (keys, active, types)
    }

    fn named_holder_entity(
        keys: &NamedTypeKeys,
        active: TypeSourceKey,
    ) -> (EntitySourceKey, EntityFragment) {
        let entity_source =
            EntitySourceKey::try_new("test:entity:holder").expect("entity key should admit");
        let id_source = FieldSourceKey::try_new("test:field:id").expect("field key should admit");
        let mut fields = vec![FieldFragment::new(
            id_source.clone(),
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
                FieldSourceKey::try_new(format!("test:field:{suffix}"))
                    .expect("field key should admit"),
                name(suffix),
                FieldType::Named(field_type),
                false,
                FieldInsertPolicy::Required,
                None,
            ));
        }
        fields.push(FieldFragment::new(
            FieldSourceKey::try_new("test:field:status").expect("field key should admit"),
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
            entity_source.clone(),
            name("Holder"),
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
        let source =
            EntitySourceKey::try_new("test:entity:other").expect("entity key should admit");
        let id = FieldSourceKey::try_new("test:field:other-id").expect("field key should admit");
        let entity = EntityFragment::try_new(
            source.clone(),
            name("Other"),
            vec![
                FieldFragment::new(
                    id.clone(),
                    name("id"),
                    FieldType::Scalar(ScalarType::Nat64),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
                FieldFragment::new(
                    FieldSourceKey::try_new("test:field:other-status")
                        .expect("field key should admit"),
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
        let label_source =
            FieldSourceKey::try_new("test:record:label").expect("field source should admit");
        let status_source =
            FieldSourceKey::try_new("test:record:status").expect("field source should admit");
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

    fn named_metadata_proposal(
        expected_head: ExpectedAcceptedHead,
        submission_key: &str,
        status_name: &str,
        active_name: &str,
        profile_name: &str,
        label_name: &str,
    ) -> (
        SchemaProposal,
        NamedTypeKeys,
        EntitySourceKey,
        TargetStoreIdentity,
    ) {
        let (keys, active, types) =
            named_type_fragments_with_names(status_name, active_name, profile_name, label_name);
        let (entity_source, entity) = named_holder_entity(&keys, active);
        let store = TargetStoreIdentity::from_bytes([0x22; 32]);
        let fragment =
            SchemaFragment::try_new(vec![entity], types).expect("named fragment should admit");
        let proposal = SchemaProposal::try_compose(
            vec![
                SchemaCapability::EXACT_COMPOSITE_TYPES,
                SchemaCapability::INSERT_DEFAULTS,
            ],
            TargetDatabaseIdentity::from_bytes([0x11; 32]),
            SchemaSubmissionKey::try_new(submission_key).expect("test key should admit"),
            expected_head,
            vec![fragment],
            vec![EntityStoreAssignment::new(entity_source.clone(), store)],
            Vec::new(),
        )
        .expect("named proposal should compose");
        (proposal, keys, entity_source, store)
    }

    fn initial_named_metadata_candidate() -> (
        super::CandidateSchemaRevision,
        NamedTypeKeys,
        EntitySourceKey,
        TargetStoreIdentity,
    ) {
        let (initial, keys, entity_source, store) = named_metadata_proposal(
            ExpectedAcceptedHead::Empty,
            "initial-named-metadata",
            "Status",
            "Active",
            "Profile",
            "label",
        );
        let initial_candidates = lower_initial_schema_proposal(
            &initial,
            &[ProposalStoreTarget {
                path: "test::Store",
                identity: store,
            }],
        )
        .expect("initial proposal should lower");
        (
            initial_candidates
                .into_iter()
                .next()
                .expect("initial candidate should exist"),
            keys,
            entity_source,
            store,
        )
    }

    fn assert_renamed_named_metadata(
        initial_bundle: &super::AcceptedSchemaRevisionBundle,
        renamed_bundle: &super::AcceptedSchemaRevisionBundle,
        initial_keys: &NamedTypeKeys,
        entity_source: &EntitySourceKey,
    ) {
        assert_eq!(
            renamed_bundle.source_bindings_for_tests(),
            initial_bundle.source_bindings_for_tests(),
        );
        let bindings = renamed_bundle.source_bindings_for_tests();
        let AcceptedNamedTypeIdentity::Enum(status_id) = bindings
            .named_type(&initial_keys.status)
            .expect("status source should remain bound")
        else {
            panic!("status source should remain enum-owned")
        };
        assert_eq!(
            renamed_bundle.enum_catalog().type_id("LifecycleStatus"),
            Some(status_id),
        );
        assert!(
            renamed_bundle
                .enum_catalog()
                .enum_type(status_id)
                .expect("status enum should exist")
                .variant_id("Enabled")
                .is_some(),
        );
        let AcceptedNamedTypeIdentity::Composite(profile_id) = bindings
            .named_type(&initial_keys.profile)
            .expect("profile source should remain bound")
        else {
            panic!("profile source should remain composite-owned")
        };
        assert_eq!(
            renamed_bundle.composite_catalog().type_id("UserProfile"),
            Some(profile_id),
        );
        let label_source =
            FieldSourceKey::try_new("test:record:label").expect("label source should admit");
        let label_id = bindings
            .composite_field(profile_id, &label_source)
            .expect("label source should remain bound");
        let AcceptedCompositeShape::Record(profile_fields) = renamed_bundle
            .composite_catalog()
            .composite_type(profile_id)
            .expect("profile type should exist")
            .shape()
        else {
            panic!("profile should remain record-shaped")
        };
        assert!(
            profile_fields
                .iter()
                .any(|field| field.id() == label_id && field.name() == "label"),
        );

        let entity_tag = bindings
            .entity(entity_source)
            .expect("entity source should remain bound");
        let before = initial_bundle
            .entity_snapshots()
            .get(&entity_tag)
            .expect("initial entity should exist");
        let after = renamed_bundle
            .entity_snapshots()
            .get(&entity_tag)
            .expect("renamed entity should exist");
        assert_eq!(after.row_layout(), before.row_layout());
        assert_eq!(
            after
                .fields()
                .iter()
                .map(PersistedFieldSnapshot::id)
                .collect::<Vec<_>>(),
            before
                .fields()
                .iter()
                .map(PersistedFieldSnapshot::id)
                .collect::<Vec<_>>(),
        );
        let profile = after
            .fields()
            .iter()
            .find(|field| field.name() == "profile")
            .expect("profile field should exist");
        assert!(
            profile
                .nested_leaves()
                .iter()
                .any(|leaf| leaf.path() == ["label"]),
        );
    }

    #[test]
    fn existing_named_metadata_reconciliation_preserves_type_shape_and_layout_identity() {
        let (initial_candidate, initial_keys, entity_source, store) =
            initial_named_metadata_candidate();
        let initial_bundle = initial_candidate.bundle();
        let (renamed, _, _, _) = named_metadata_proposal(
            ExpectedAcceptedHead::Exact {
                revision: 1,
                fingerprint: ExpectedSchemaFingerprint::from_bytes([0x44; 32]),
            },
            "renamed-named-metadata",
            "LifecycleStatus",
            "Enabled",
            "UserProfile",
            "label",
        );
        let renamed_candidates = lower_existing_schema_proposal(
            &renamed,
            &[ExistingProposalStore {
                path: "test::Store",
                identity: store,
                bundle: initial_bundle,
            }],
        )
        .expect("renamed named metadata should lower");
        assert_renamed_named_metadata(
            initial_bundle,
            renamed_candidates[0].bundle(),
            &initial_keys,
            &entity_source,
        );
    }

    #[test]
    fn existing_record_member_rename_rejects_without_rewriting_canonical_values() {
        let (initial_candidate, _, _, store) = initial_named_metadata_candidate();
        let (invalid, _, _, _) = named_metadata_proposal(
            ExpectedAcceptedHead::Exact {
                revision: 1,
                fingerprint: ExpectedSchemaFingerprint::from_bytes([0x44; 32]),
            },
            "invalid-record-member-rename",
            "Status",
            "Active",
            "Profile",
            "display_label",
        );
        assert!(
            lower_existing_schema_proposal(
                &invalid,
                &[ExistingProposalStore {
                    path: "test::Store",
                    identity: store,
                    bundle: initial_candidate.bundle(),
                }],
            )
            .is_err(),
            "record member names remain part of canonical value shape",
        );
    }
}
