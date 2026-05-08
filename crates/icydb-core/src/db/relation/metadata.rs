//! Module: relation::metadata
//! Responsibility: extract and expose canonical relation metadata descriptors from entity models.
//! Does not own: relation validation execution or reverse-index mutation application.
//! Boundary: defines lightweight relation metadata contracts consumed by schema
//! describe, relation validators, and reverse-index maintenance.

use crate::{
    model::entity::EntityModel,
    model::field::{FieldKind, RelationStrength},
};

///
/// RelationDescriptorCardinality
///
/// Canonical cardinality for a relation field declared directly or through a
/// supported collection wrapper. Relation-owned descriptor extraction uses this
/// enum so schema describe and relation validation do not separately classify
/// single/list/set relation shapes.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum RelationDescriptorCardinality {
    Single,
    List,
    Set,
}

///
/// RelationDescriptor
///
/// Canonical relation metadata shape extracted from an entity model field.
/// The descriptor is intentionally semantic: it describes relation target
/// identity, strength, and cardinality without performing save,
/// delete, reverse-index, or storage execution behavior.
///

#[derive(Clone, Copy)]
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::db) struct RelationDescriptor {
    field_name: &'static str,
    target_path: &'static str,
    target_entity_name: &'static str,
    target_store_path: &'static str,
    strength: RelationStrength,
    cardinality: RelationDescriptorCardinality,
}

#[cfg_attr(not(test), allow(dead_code))]
impl RelationDescriptor {
    // Build one canonical relation descriptor from already-classified target
    // metadata and the source field metadata that owns it.
    const fn new(field_name: &'static str, target: RelationTargetInfo) -> Self {
        Self {
            field_name,
            target_path: target.path,
            target_entity_name: target.entity_name,
            target_store_path: target.store_path,
            strength: target.strength,
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

    /// Return the declared relation strength.
    #[must_use]
    pub(in crate::db) const fn strength(self) -> RelationStrength {
        self.strength
    }

    /// Return the declared relation cardinality.
    #[must_use]
    pub(in crate::db) const fn cardinality(self) -> RelationDescriptorCardinality {
        self.cardinality
    }
}

/// StrongRelationTargetInfo
///
/// Raw target descriptor for relation fields before canonical descriptor
/// construction. It exists only to keep the `FieldKind` pattern match small
/// and lets the canonical public-in-db descriptor remain the only exported
/// relation metadata shape.
///

#[derive(Clone, Copy)]
struct RelationTargetInfo {
    path: &'static str,
    entity_name: &'static str,
    store_path: &'static str,
    strength: RelationStrength,
    cardinality: RelationDescriptorCardinality,
}

/// Resolve a model field-kind into strong relation target metadata (if applicable).
const fn relation_target_from_kind(kind: &FieldKind) -> Option<RelationTargetInfo> {
    match kind {
        FieldKind::Relation {
            target_path,
            target_entity_name,
            target_store_path,
            strength,
            ..
        } => Some(RelationTargetInfo {
            path: target_path,
            entity_name: target_entity_name,
            store_path: target_store_path,
            strength: *strength,
            cardinality: RelationDescriptorCardinality::Single,
        }),
        FieldKind::List(FieldKind::Relation {
            target_path,
            target_entity_name,
            target_store_path,
            strength,
            ..
        }) => Some(RelationTargetInfo {
            path: target_path,
            entity_name: target_entity_name,
            store_path: target_store_path,
            strength: *strength,
            cardinality: RelationDescriptorCardinality::List,
        }),
        FieldKind::Set(FieldKind::Relation {
            target_path,
            target_entity_name,
            target_store_path,
            strength,
            ..
        }) => Some(RelationTargetInfo {
            path: target_path,
            entity_name: target_entity_name,
            store_path: target_store_path,
            strength: *strength,
            cardinality: RelationDescriptorCardinality::Set,
        }),
        _ => None,
    }
}

/// Resolve a model field into canonical relation metadata (if applicable).
const fn relation_descriptor_from_field(
    field_name: &'static str,
    kind: &FieldKind,
) -> Option<RelationDescriptor> {
    let Some(target) = relation_target_from_kind(kind) else {
        return None;
    };

    Some(RelationDescriptor::new(field_name, target))
}

/// Resolve canonical relation descriptors for one source model.
pub(in crate::db) fn relation_descriptors_for_model_iter(
    model: &EntityModel,
) -> impl Iterator<Item = RelationDescriptor> + '_ {
    model
        .fields
        .iter()
        .filter_map(|field| relation_descriptor_from_field(field.name, &field.kind))
}

impl EntityModel {
    /// Return `true` when this model declares any strong relation field.
    #[must_use]
    pub(in crate::db) fn has_any_strong_relations(&self) -> bool {
        relation_descriptors_for_model_iter(self)
            .any(|descriptor| descriptor.strength() == RelationStrength::Strong)
    }
}
