//! Module: db::schema::application_lowering
//! Responsibility: lower source-keyed initial proposals into accepted catalog candidates.
//! Does not own: optimistic admission, durable receipts, publication, or activation progress.
//! Boundary: validated public proposal plus target-store routing -> catalog-native candidates.

use std::collections::BTreeMap;

use icydb_schema::{
    EntityFragment, EntitySourceKey, FieldInsertPolicy, FieldManagementPolicy, FieldSourceKey,
    FieldType, IndexKeyFragment, ScalarType, SchemaProposal, TargetStoreIdentity,
};

use crate::{
    db::{
        data::encode_input_value_for_candidate_field_contract,
        schema::{
            AcceptedCompositeCatalog, AcceptedConstraintCatalog, AcceptedConstraintKind,
            AcceptedEnumCatalog, AcceptedFieldDecodeContract, AcceptedFieldKind,
            AcceptedSchemaFingerprint, AcceptedSchemaRevision, AcceptedSchemaRevisionBundle,
            AcceptedSourceBindingCatalog, AcceptedStoreCatalogScope, AcceptedValueCatalogHandle,
            CandidateSchemaRevision, ConstraintId, ConstraintOrigin, FieldId, PersistedFieldOrigin,
            PersistedFieldSnapshot, PersistedIndexExpressionOp, PersistedIndexExpressionSnapshot,
            PersistedIndexFieldPathSnapshot, PersistedIndexKeyItemSnapshot,
            PersistedIndexKeySnapshot, PersistedIndexSnapshot, PersistedRelationEdgeSnapshot,
            PersistedSchemaSnapshot, RelationId, RowLayoutVersion, SchemaFieldSlot,
            SchemaFieldWritePolicy, SchemaHistoricalFill, SchemaIndexId, SchemaInsertDefault,
            SchemaRowLayout, SchemaVersion, ValueAdmissionBudget, bind_source_check_expr,
            render_accepted_check_expr_sql, source_literal_input,
        },
    },
    error::InternalError,
    model::field::{
        FieldInsertGeneration, FieldStorageDecode, FieldWriteManagement, LeafCodec, ScalarCodec,
    },
    types::EntityTag,
};

/// One registered store routing fact admitted by the application boundary.
#[derive(Clone, Copy)]
pub(in crate::db::schema) struct ProposalStoreTarget {
    pub(in crate::db::schema) path: &'static str,
    pub(in crate::db::schema) identity: TargetStoreIdentity,
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
    value_catalog: AcceptedValueCatalogHandle,
}

impl<'a> InitialStoreContext<'a> {
    fn new(
        store_path: &'static str,
        assignments: &'a BTreeMap<EntitySourceKey, &'static str>,
        all_entities: &'a BTreeMap<EntitySourceKey, &'a EntityFragment>,
        accepted_entities: &'a BTreeMap<EntitySourceKey, EntityTag>,
    ) -> Self {
        let enum_catalog = AcceptedEnumCatalog::empty();
        let composite_catalog = AcceptedCompositeCatalog::empty();
        // Rendering scalar-only index predicates consumes only the value
        // catalogs. The unpublished authority identity cannot enter the
        // candidate and is replaced by the bundle's computed fingerprint.
        let value_catalog = AcceptedValueCatalogHandle::new(
            enum_catalog.clone(),
            composite_catalog.clone(),
            AcceptedStoreCatalogScope::new(),
            AcceptedSchemaRevision::INITIAL,
            AcceptedSchemaFingerprint::new([1; 32]),
        );
        Self {
            store_path,
            assignments,
            all_entities,
            accepted_entities,
            enum_catalog,
            composite_catalog,
            value_catalog,
        }
    }
}

/// Mutable accepted identities allocated while completing one store-local
/// initial candidate.
#[derive(Default)]
struct InitialObjectBindings {
    indexes: BTreeMap<(EntityTag, icydb_schema::IndexSourceKey), SchemaIndexId>,
    relations: BTreeMap<(EntityTag, icydb_schema::RelationSourceKey), RelationId>,
    constraints: BTreeMap<(EntityTag, icydb_schema::ConstraintSourceKey), ConstraintId>,
}

/// Lower a scalar source-keyed proposal against an empty accepted database.
///
/// This is the sole proposal-to-accepted candidate path. Named types and
/// mutations of an existing accepted head remain rejected until their
/// identity-preserving catalog transitions are connected here; callers never
/// substitute generated model authority or partially publish a proposal.
pub(in crate::db::schema) fn lower_initial_schema_proposal(
    proposal: &SchemaProposal,
    stores: &[ProposalStoreTarget],
) -> Result<Vec<CandidateSchemaRevision>, InternalError> {
    if !proposal.removals().is_empty()
        || proposal
            .fragments()
            .iter()
            .any(|fragment| !fragment.types().is_empty())
    {
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
        )?);
    }
    candidates.sort_by(|left, right| left.store_path().cmp(right.store_path()));
    Ok(candidates)
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
) -> Result<CandidateSchemaRevision, InternalError> {
    let context =
        InitialStoreContext::new(store_path, assignments, all_entities, accepted_entities);
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
    );
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
    );
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
        let kind = lower_scalar_field_type(field.field_type())?;
        let storage_decode = FieldStorageDecode::ByKind;
        let leaf_codec = scalar_leaf_codec(&kind);
        let write_policy = lower_write_policy(field.insert_policy(), field.management(), &kind)?;
        let insert_default = lower_insert_default(
            context,
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
            Vec::new(),
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

fn lower_insert_default(
    context: &InitialStoreContext<'_>,
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
    let bindings = AcceptedSourceBindingCatalog::default();
    let input = source_literal_input(literal, kind, &bindings, &context.enum_catalog)
        .map_err(|_| InternalError::store_unsupported())?;
    let field =
        AcceptedFieldDecodeContract::new(field_name, kind, nullable, storage_decode, leaf_codec);
    let mut budget = ValueAdmissionBudget::standard();
    let payload = encode_input_value_for_candidate_field_contract(
        &context.enum_catalog,
        &context.composite_catalog,
        field,
        input,
        &mut budget,
    )?;
    Ok(SchemaInsertDefault::SlotPayload(payload))
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

fn lower_scalar_field_type(field_type: &FieldType) -> Result<AcceptedFieldKind, InternalError> {
    let FieldType::Scalar(scalar) = field_type else {
        return Err(InternalError::store_unsupported());
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
    use super::{ProposalStoreTarget, lower_initial_schema_proposal};
    use crate::db::schema::{AcceptedConstraintKind, AcceptedSchemaRevision, SchemaInsertDefault};
    use icydb_schema::{
        ConstraintFragment, ConstraintSourceKey, EntityFragment, EntitySourceKey,
        EntityStoreAssignment, ExpectedAcceptedHead, FieldFragment, FieldInsertPolicy,
        FieldSourceKey, FieldType, IndexFragment, IndexKeyFragment, IndexSourceKey,
        NamedTypeFragment, ScalarLiteral, ScalarType, SchemaCapability, SchemaFragment, SchemaName,
        SchemaProposal, SchemaSubmissionKey, SourceCheckExpr, SourceCheckInstruction,
        TargetDatabaseIdentity, TargetStoreIdentity, TypeSourceKey,
    };

    fn name(value: &str) -> SchemaName {
        SchemaName::try_new(value).expect("test schema name should admit")
    }

    fn initial_scalar_proposal_fixture() -> (SchemaProposal, EntitySourceKey, TargetStoreIdentity) {
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
            name("Item"),
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
                    name("score"),
                    FieldType::Scalar(ScalarType::Int64),
                    false,
                    FieldInsertPolicy::Default(ScalarLiteral::Int(5)),
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
            vec![ConstraintFragment::new(
                ConstraintSourceKey::try_new("test:check:score")
                    .expect("test check source should admit"),
                name("score_non_negative"),
                check,
            )],
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
            SchemaSubmissionKey::try_new("initial-scalar")
                .expect("test submission key should admit"),
            ExpectedAcceptedHead::Empty,
            vec![fragment],
            vec![EntityStoreAssignment::new(entity_source.clone(), store)],
            Vec::new(),
        )
        .expect("test proposal should compose");
        (proposal, entity_source, store)
    }

    #[test]
    fn initial_scalar_proposal_lowers_source_identity_defaults_indexes_and_checks() {
        let (proposal, entity_source, store) = initial_scalar_proposal_fixture();

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
    fn initial_lowering_rejects_named_types_before_constructing_candidates() {
        let fragment = SchemaFragment::try_new(
            Vec::new(),
            vec![NamedTypeFragment::Newtype {
                source_key: TypeSourceKey::try_new("test:type:score")
                    .expect("test type source should admit"),
                name: name("Score"),
                inner: FieldType::Scalar(ScalarType::Int64),
            }],
        )
        .expect("test fragment should admit");
        let proposal = SchemaProposal::try_compose(
            Vec::new(),
            TargetDatabaseIdentity::from_bytes([0x11; 32]),
            SchemaSubmissionKey::try_new("named-type-rejection")
                .expect("test submission key should admit"),
            ExpectedAcceptedHead::Empty,
            vec![fragment],
            Vec::new(),
            Vec::new(),
        )
        .expect("test proposal should compose");

        assert!(
            lower_initial_schema_proposal(&proposal, &[]).is_err(),
            "named types must fail closed until their catalog-native lowering is connected"
        );
    }
}
