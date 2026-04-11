//! Module: relation::metadata
//! Responsibility: extract and expose strong-relation metadata descriptors from entity models.
//! Does not own: relation validation execution or reverse-index mutation application.
//! Boundary: defines lightweight relation metadata contracts consumed by relation validators.

use crate::{
    model::entity::EntityModel,
    model::field::{FieldKind, RelationStrength},
    types::EntityTag,
};

///
/// StrongRelationInfo
///
/// Lightweight relation descriptor extracted from runtime field metadata.
///

#[derive(Clone, Copy)]
pub(super) struct StrongRelationInfo {
    pub(super) field_index: usize,
    pub(super) field_name: &'static str,
    pub(super) field_kind: FieldKind,
    pub(super) target_path: &'static str,
    pub(super) target_entity_name: &'static str,
    pub(super) target_entity_tag: EntityTag,
    pub(super) target_store_path: &'static str,
}

///
/// StrongRelationTargetInfo
///
/// Shared target descriptor for strong relation fields.
///

#[derive(Clone, Copy)]
struct StrongRelationTargetInfo {
    path: &'static str,
    entity_name: &'static str,
    entity_tag: EntityTag,
    store_path: &'static str,
}

/// Resolve a model field-kind into strong relation target metadata (if applicable).
const fn strong_relation_target_from_kind(kind: &FieldKind) -> Option<StrongRelationTargetInfo> {
    match kind {
        FieldKind::Relation {
            target_path,
            target_entity_name,
            target_entity_tag,
            target_store_path,
            strength: RelationStrength::Strong,
            ..
        }
        | FieldKind::List(FieldKind::Relation {
            target_path,
            target_entity_name,
            target_entity_tag,
            target_store_path,
            strength: RelationStrength::Strong,
            ..
        })
        | FieldKind::Set(FieldKind::Relation {
            target_path,
            target_entity_name,
            target_entity_tag,
            target_store_path,
            strength: RelationStrength::Strong,
            ..
        }) => Some(StrongRelationTargetInfo {
            path: target_path,
            entity_name: target_entity_name,
            entity_tag: *target_entity_tag,
            store_path: target_store_path,
        }),
        _ => None,
    }
}

/// Resolve a model field into strong relation metadata (if applicable).
const fn strong_relation_from_field(
    field_index: usize,
    field_name: &'static str,
    kind: &FieldKind,
) -> Option<StrongRelationInfo> {
    let Some(target) = strong_relation_target_from_kind(kind) else {
        return None;
    };

    Some(StrongRelationInfo {
        field_index,
        field_name,
        field_kind: *kind,
        target_path: target.path,
        target_entity_name: target.entity_name,
        target_entity_tag: target.entity_tag,
        target_store_path: target.store_path,
    })
}

/// Resolve strong relation descriptors for one source model, optionally filtered by target path.
pub(super) fn strong_relations_for_model_iter<'a>(
    model: &'static EntityModel,
    target_path_filter: Option<&'a str>,
) -> impl Iterator<Item = StrongRelationInfo> + 'a {
    model
        .fields
        .iter()
        .enumerate()
        .filter_map(|(field_index, field)| {
            strong_relation_from_field(field_index, field.name, &field.kind)
        })
        .filter(move |relation| {
            target_path_filter.is_none_or(|target_path| relation.target_path == target_path)
        })
}

/// Return `true` when one model declares any strong relation field.
#[must_use]
pub(in crate::db) fn model_has_any_strong_relations(model: &'static EntityModel) -> bool {
    strong_relations_for_model_iter(model, None)
        .next()
        .is_some()
}

/// Return `true` when one source model declares any strong relation to the
/// target entity path under delete-side validation.
pub(in crate::db) fn model_has_strong_relations_to_target(
    model: &'static EntityModel,
    target_path: &str,
) -> bool {
    strong_relations_for_model_iter(model, Some(target_path))
        .next()
        .is_some()
}
