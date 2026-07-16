//! Module: relation::metadata
//! Responsibility: extract and expose canonical relation field metadata from entity models.
//! Does not own: relation validation execution or reverse-index mutation application.
//! Boundary: defines lightweight relation metadata contracts consumed by schema
//! describe, relation validators, and reverse-index maintenance.

use crate::{
    model::entity::EntityModel,
    model::field::{FieldKind, RelationEnforcement},
};

///
/// RelationFieldCardinality
///
/// Canonical cardinality for a relation field declared directly or through a
/// supported collection wrapper. Relation-owned metadata extraction uses this
/// enum so schema describe and relation validation do not separately classify
/// single/list/set relation shapes.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum RelationFieldCardinality {
    Single,
    List,
    Set,
}

///
/// RelationFieldMetadata
///
/// Canonical relation metadata shape extracted from an entity model field.
/// The metadata is intentionally semantic: it describes relation target
/// identity, enforcement, and cardinality without performing save,
/// delete, reverse-index, or storage execution behavior.
///

#[derive(Clone, Copy)]
pub(in crate::db) struct RelationFieldMetadata {
    field_name: &'static str,
    target_path: &'static str,
    target_entity_name: &'static str,
    target_store_path: &'static str,
    enforcement: RelationEnforcement,
    cardinality: RelationFieldCardinality,
}

impl RelationFieldMetadata {
    // Build one canonical relation metadata value from already-classified target
    // metadata and the source field metadata that owns it.
    const fn new(field_name: &'static str, target: RelationTargetMetadata) -> Self {
        Self {
            field_name,
            target_path: target.path,
            target_entity_name: target.entity_name,
            target_store_path: target.store_path,
            enforcement: target.enforcement,
            cardinality: target.cardinality,
        }
    }

    /// Return the stable source field name for this relation.
    #[must_use]
    pub(in crate::db) const fn field_name(self) -> &'static str {
        self.field_name
    }

    /// Return the declared target entity path.
    #[must_use]
    pub(in crate::db) const fn target_path(self) -> &'static str {
        self.target_path
    }

    /// Return the declared target entity name.
    #[must_use]
    pub(in crate::db) const fn target_entity_name(self) -> &'static str {
        self.target_entity_name
    }

    /// Return the declared target store path.
    #[must_use]
    pub(in crate::db) const fn target_store_path(self) -> &'static str {
        self.target_store_path
    }

    /// Return the declared relation enforcement.
    #[must_use]
    pub(in crate::db) const fn enforcement(self) -> RelationEnforcement {
        self.enforcement
    }

    /// Return the declared relation cardinality.
    #[must_use]
    pub(in crate::db) const fn cardinality(self) -> RelationFieldCardinality {
        self.cardinality
    }
}

/// StrongRelationTargetMetadata
///
/// Raw target metadata for relation fields before canonical metadata
/// construction. It exists only to keep the `FieldKind` pattern match small
/// and lets the canonical public-in-db metadata value remain the only exported
/// relation metadata shape.
///

#[derive(Clone, Copy)]
struct RelationTargetMetadata {
    path: &'static str,
    entity_name: &'static str,
    store_path: &'static str,
    enforcement: RelationEnforcement,
    cardinality: RelationFieldCardinality,
}

/// Resolve a model field-kind into strong relation target metadata (if applicable).
const fn relation_target_from_kind(kind: &FieldKind) -> Option<RelationTargetMetadata> {
    match kind {
        FieldKind::Relation {
            target_path,
            target_entity_name,
            target_store_path,
            enforcement,
            ..
        } => Some(RelationTargetMetadata {
            path: target_path,
            entity_name: target_entity_name,
            store_path: target_store_path,
            enforcement: *enforcement,
            cardinality: RelationFieldCardinality::Single,
        }),
        FieldKind::List(FieldKind::Relation {
            target_path,
            target_entity_name,
            target_store_path,
            enforcement,
            ..
        }) => Some(RelationTargetMetadata {
            path: target_path,
            entity_name: target_entity_name,
            store_path: target_store_path,
            enforcement: *enforcement,
            cardinality: RelationFieldCardinality::List,
        }),
        FieldKind::Set(FieldKind::Relation {
            target_path,
            target_entity_name,
            target_store_path,
            enforcement,
            ..
        }) => Some(RelationTargetMetadata {
            path: target_path,
            entity_name: target_entity_name,
            store_path: target_store_path,
            enforcement: *enforcement,
            cardinality: RelationFieldCardinality::Set,
        }),
        _ => None,
    }
}

/// Resolve a model field into canonical relation metadata (if applicable).
const fn relation_field_metadata_from_field(
    field_name: &'static str,
    kind: &FieldKind,
) -> Option<RelationFieldMetadata> {
    let Some(target) = relation_target_from_kind(kind) else {
        return None;
    };

    Some(RelationFieldMetadata::new(field_name, target))
}

/// Resolve canonical relation metadata for one source model.
pub(in crate::db) fn relation_field_metadata_for_model_iter(
    model: &EntityModel,
) -> impl Iterator<Item = RelationFieldMetadata> + '_ {
    model
        .fields
        .iter()
        .filter_map(|field| relation_field_metadata_from_field(field.name, &field.kind))
}

impl EntityModel {
    /// Return `true` when this model declares any strong relation field.
    #[must_use]
    pub(in crate::db) fn has_any_strong_relations(&self) -> bool {
        relation_field_metadata_for_model_iter(self)
            .any(|metadata| metadata.enforcement() == RelationEnforcement::Enforced)
    }
}
