use crate::{
    model::field::{EntityFieldKind, RelationStrength},
    traits::EntityKind,
};

///
/// StrongRelationInfo
///
/// Lightweight relation descriptor extracted from runtime field metadata.
///

#[derive(Clone, Copy)]
pub(super) struct StrongRelationInfo {
    pub(super) field_name: &'static str,
    pub(super) target_path: &'static str,
    pub(super) target_entity_name: &'static str,
    pub(super) target_store_path: &'static str,
}

///
/// StrongRelationTargetInfo
///
/// Shared target descriptor for strong relation fields.
///

#[expect(clippy::struct_field_names)]
#[derive(Clone, Copy)]
pub struct StrongRelationTargetInfo {
    pub target_path: &'static str,
    pub target_entity_name: &'static str,
    pub target_store_path: &'static str,
}

// Resolve a model field-kind into strong relation target metadata (if applicable).
pub const fn strong_relation_target_from_kind(
    kind: &EntityFieldKind,
) -> Option<StrongRelationTargetInfo> {
    match kind {
        EntityFieldKind::Relation {
            target_path,
            target_entity_name,
            target_store_path,
            strength: RelationStrength::Strong,
            ..
        }
        | EntityFieldKind::List(EntityFieldKind::Relation {
            target_path,
            target_entity_name,
            target_store_path,
            strength: RelationStrength::Strong,
            ..
        })
        | EntityFieldKind::Set(EntityFieldKind::Relation {
            target_path,
            target_entity_name,
            target_store_path,
            strength: RelationStrength::Strong,
            ..
        }) => Some(StrongRelationTargetInfo {
            target_path,
            target_entity_name,
            target_store_path,
        }),
        _ => None,
    }
}

// Resolve a model field into strong relation metadata (if applicable).
const fn strong_relation_from_field(
    field_name: &'static str,
    kind: &EntityFieldKind,
) -> Option<StrongRelationInfo> {
    let Some(target) = strong_relation_target_from_kind(kind) else {
        return None;
    };

    Some(StrongRelationInfo {
        field_name,
        target_path: target.target_path,
        target_entity_name: target.target_entity_name,
        target_store_path: target.target_store_path,
    })
}

// Resolve strong relation descriptors for a source entity, optionally filtered by target path.
pub(super) fn strong_relations_for_source<S>(
    target_path_filter: Option<&str>,
) -> Vec<StrongRelationInfo>
where
    S: EntityKind,
{
    S::MODEL
        .fields
        .iter()
        .filter_map(|field| strong_relation_from_field(field.name, &field.kind))
        .filter(|relation| {
            target_path_filter.is_none_or(|target_path| relation.target_path == target_path)
        })
        .collect()
}
