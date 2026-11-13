//! Module: relation
//! Responsibility: relation-domain validation and reverse-index mutation helpers.
//! Does not own: query planning, executor routing, or storage codec policy.
//! Boundary: executor/commit paths delegate relation semantics to this module.

mod reverse_index;
mod validate;

use crate::{
    db::{
        Db,
        identity::EntityName,
        schema::{
            AcceptedCatalogSnapshotSelection, AcceptedFieldKind, classify_accepted_field_kind,
        },
    },
    error::InternalError,
    traits::CanisterKind,
    types::EntityTag,
};

pub(crate) use reverse_index::ReverseRelationSourceInfo;
#[cfg(feature = "migration")]
pub(in crate::db) use reverse_index::migration_relation_domain;
pub(in crate::db) use reverse_index::{
    RelationCommitBudget, RelationConstraintProjection, RelationProjectionBudget,
    prove_empty_reverse_relation_domain,
};
pub(in crate::db) use validate::{
    validate_candidate_relation_target_delete_barrier,
    validate_delete_relations_for_accepted_source,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AcceptedRelationCardinality {
    Single,
    List,
    Set,
}

///
/// AcceptedRelationTargetMetadata
///
/// Accepted-schema relation target metadata projected from a relation field
/// or a supported collection wrapper. This is intentionally field-shape
/// metadata only; save validation and reverse-index preparation add their
/// own execution-specific source slot context.
///

#[derive(Clone, Copy)]
struct AcceptedRelationTargetMetadata<'a> {
    target_path: &'a str,
    target_entity_name: &'a str,
    target_entity_tag: EntityTag,
    target_store_path: &'a str,
    scalar_target_key_kind: &'a AcceptedFieldKind,
    cardinality: AcceptedRelationCardinality,
}

#[derive(Clone, Debug)]
pub(in crate::db) struct AcceptedRelationTargetContract {
    target: AcceptedRelationTargetAuthority,
    primary_key_kinds: Vec<AcceptedFieldKind>,
}

impl AcceptedRelationTargetContract {
    /// Bind target identity and key kinds from one explicitly selected catalog.
    pub(in crate::db) fn from_catalog_selection(
        selection: &AcceptedCatalogSnapshotSelection,
    ) -> Result<Self, InternalError> {
        let identity = selection.identity();
        let accepted = selection.snapshot();
        Ok(Self {
            target: AcceptedRelationTargetAuthority::try_new(
                identity.entity_path(),
                accepted.entity_name(),
                identity.entity_tag(),
                identity.store_path(),
            )?,
            primary_key_kinds: accepted
                .primary_key_field_kinds()
                .into_iter()
                .cloned()
                .collect(),
        })
    }

    #[must_use]
    const fn primary_key_kinds(&self) -> &[AcceptedFieldKind] {
        self.primary_key_kinds.as_slice()
    }

    fn into_target(self) -> AcceptedRelationTargetAuthority {
        self.target
    }
}

#[derive(Clone, Copy)]
struct AcceptedRelationTupleEdgeLocalComponent<'a> {
    kind: &'a AcceptedFieldKind,
}

impl<'a> AcceptedRelationTupleEdgeLocalComponent<'a> {
    const fn new(kind: &'a AcceptedFieldKind) -> Self {
        Self { kind }
    }
}

fn validate_accepted_relation_tuple_components(
    target_contract: &AcceptedRelationTargetContract,
    local_components: &[AcceptedRelationTupleEdgeLocalComponent<'_>],
) -> Result<(), InternalError> {
    let target_kinds = target_contract.primary_key_kinds();
    if local_components.len() != target_kinds.len() {
        return Err(InternalError::relation_target_primary_key_arity_mismatch(
            target_kinds.len(),
            local_components.len(),
        ));
    }

    for (local, target_kind) in local_components.iter().zip(target_kinds) {
        let local_kind = relation_local_component_key_kind(local.kind);
        if local_kind != target_kind {
            return Err(InternalError::executor_internal());
        }
        validate_relation_primary_key_component_kind(local_kind)?;
    }

    Ok(())
}

fn accepted_scalar_relation_cardinality(
    kind: &AcceptedFieldKind,
    target_contract: &AcceptedRelationTargetContract,
) -> Result<Option<AcceptedRelationCardinality>, InternalError> {
    let Some(target) = accepted_relation_target_metadata_from_kind(kind) else {
        return Ok(None);
    };
    let accepted = &target_contract.target;
    if target.target_path != accepted.path() {
        return Err(InternalError::store_invariant());
    }
    if target.target_entity_name != accepted.entity_name.as_str()
        || target.target_entity_tag != accepted.entity_tag()
        || target.target_store_path != accepted.store_path()
    {
        return Err(InternalError::executor_internal());
    }
    validate_relation_primary_key_component_kind(target.scalar_target_key_kind)?;
    validate_accepted_relation_primary_key_kinds(
        std::slice::from_ref(target.scalar_target_key_kind),
        target_contract.primary_key_kinds(),
    )?;

    Ok(Some(target.cardinality))
}

/// Resolve the live target contract for ordinary writes and schema work.
pub(in crate::db) fn accepted_relation_target_contract<C>(
    db: &Db<C>,
    target_path: &str,
) -> Result<AcceptedRelationTargetContract, InternalError>
where
    C: CanisterKind,
{
    let target = db.accepted_runtime_entity_for_path(target_path)?;
    let target_store = db.store_handle(target.store_path())?;
    let selection = target_store
        .with_schema(|schema_store| {
            schema_store.current_accepted_catalog_selection(
                target.entity_tag(),
                target.entity_path(),
                target.store_path(),
            )
        })?
        .ok_or_else(InternalError::store_corruption)?;
    AcceptedRelationTargetContract::from_catalog_selection(&selection)
}

fn validate_accepted_relation_primary_key_kinds(
    relation_key_kinds: &[AcceptedFieldKind],
    accepted_key_kinds: &[AcceptedFieldKind],
) -> Result<(), InternalError> {
    if accepted_key_kinds.len() != relation_key_kinds.len() {
        return Err(InternalError::executor_internal());
    }

    for (accepted_key_kind, relation_key_kind) in accepted_key_kinds.iter().zip(relation_key_kinds)
    {
        if accepted_key_kind != relation_key_kind {
            return Err(InternalError::executor_internal());
        }
    }

    Ok(())
}

fn accepted_relation_target_metadata_from_kind(
    kind: &AcceptedFieldKind,
) -> Option<AcceptedRelationTargetMetadata<'_>> {
    fn relation_target(
        kind: &AcceptedFieldKind,
        cardinality: AcceptedRelationCardinality,
    ) -> Option<AcceptedRelationTargetMetadata<'_>> {
        let AcceptedFieldKind::Relation {
            target_path,
            target_entity_name,
            target_entity_tag,
            target_store_path,
            key_kind,
        } = kind
        else {
            return None;
        };

        Some(AcceptedRelationTargetMetadata {
            target_path,
            target_entity_name,
            target_entity_tag: *target_entity_tag,
            target_store_path,
            scalar_target_key_kind: key_kind.as_ref(),
            cardinality,
        })
    }

    match kind {
        AcceptedFieldKind::Relation { .. } => {
            relation_target(kind, AcceptedRelationCardinality::Single)
        }
        AcceptedFieldKind::List(inner) => {
            relation_target(inner.as_ref(), AcceptedRelationCardinality::List)
        }
        AcceptedFieldKind::Set(inner) => {
            relation_target(inner.as_ref(), AcceptedRelationCardinality::Set)
        }
        _ => None,
    }
}

fn validate_relation_primary_key_component_kind(
    key_kind: &AcceptedFieldKind,
) -> Result<(), InternalError> {
    if let AcceptedFieldKind::Relation { key_kind, .. } = key_kind {
        return validate_relation_primary_key_component_kind(key_kind);
    }

    if classify_accepted_field_kind(key_kind).is_relation_key_eligible() {
        Ok(())
    } else {
        Err(InternalError::persisted_row_decode_corruption())
    }
}

fn relation_local_component_key_kind(kind: &AcceptedFieldKind) -> &AcceptedFieldKind {
    match kind {
        AcceptedFieldKind::Relation { key_kind, .. } => key_kind,
        other => other,
    }
}

#[derive(Clone, Debug)]
struct AcceptedRelationTargetAuthority {
    path: String,
    entity_name: EntityName,
    entity_tag: EntityTag,
    store_path: String,
}

impl AcceptedRelationTargetAuthority {
    fn try_new(
        target_path: &str,
        target_entity_name: &str,
        target_entity_tag: EntityTag,
        target_store_path: &str,
    ) -> Result<Self, InternalError> {
        let entity_name = EntityName::try_from_str(target_entity_name)
            .map_err(|_| InternalError::executor_internal())?;

        Ok(Self {
            path: target_path.to_string(),
            entity_name,
            entity_tag: target_entity_tag,
            store_path: target_store_path.to_string(),
        })
    }

    #[must_use]
    const fn path(&self) -> &str {
        self.path.as_str()
    }

    #[must_use]
    const fn entity_tag(&self) -> EntityTag {
        self.entity_tag
    }

    #[must_use]
    const fn store_path(&self) -> &str {
        self.store_path.as_str()
    }
}

#[cfg(test)]
mod tests;
