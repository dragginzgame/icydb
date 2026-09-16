//! Query-value projection from accepted schema kinds.
//! One traversal owns newtype resolution, depth and whole-tree fallback;
//! output constructors select the existing owned representation, not semantics.

use crate::{
    db::schema::{
        AcceptedFieldKind, FieldType, MAX_ACCEPTED_RECURSIVE_DEPTH,
        composite_catalog::AcceptedCompositeCatalog, field_type_from_persisted_kind,
    },
    types::EntityTag,
};

/// Project the recursively unwrapped query kind, retaining relation metadata.
/// Records, tuples, missing definitions and wrapper cycles leave the original
/// whole kind unchanged, as does exceeding the shared projection depth.
#[must_use]
pub(in crate::db) fn query_field_kind_from_persisted_kind(
    kind: &AcceptedFieldKind,
    catalog: &AcceptedCompositeCatalog,
) -> AcceptedFieldKind {
    project_query_kind(kind, catalog)
}

/// Construct the query type directly, without an intermediate owned query kind.
/// Uses exactly the same newtype resolution and whole-tree fallback as kind projection.
#[must_use]
pub(in crate::db) fn query_field_type_from_persisted_kind(
    kind: &AcceptedFieldKind,
    catalog: &AcceptedCompositeCatalog,
) -> FieldType {
    project_query_kind(kind, catalog)
}

// Output-only constructors: no catalog access or resolution policy belongs here.
trait QueryProjection: Sized {
    fn unprojected(kind: &AcceptedFieldKind) -> Self;
    fn list(inner: Self) -> Self;
    fn set(inner: Self) -> Self;
    fn map(key: Self, value: Self) -> Self;
    fn relation(
        target_path: &str,
        target_entity_name: &str,
        target_entity_tag: EntityTag,
        target_store_path: &str,
        key: Self,
    ) -> Self;
}

impl QueryProjection for AcceptedFieldKind {
    fn unprojected(kind: &Self) -> Self {
        kind.clone()
    }

    fn list(inner: Self) -> Self {
        Self::List(Box::new(inner))
    }

    fn set(inner: Self) -> Self {
        Self::Set(Box::new(inner))
    }

    fn map(key: Self, value: Self) -> Self {
        Self::Map {
            key: Box::new(key),
            value: Box::new(value),
        }
    }

    fn relation(
        target_path: &str,
        target_entity_name: &str,
        target_entity_tag: EntityTag,
        target_store_path: &str,
        key: Self,
    ) -> Self {
        Self::Relation {
            target_path: target_path.to_owned(),
            target_entity_name: target_entity_name.to_owned(),
            target_entity_tag,
            target_store_path: target_store_path.to_owned(),
            key_kind: Box::new(key),
        }
    }
}

impl QueryProjection for FieldType {
    fn unprojected(kind: &AcceptedFieldKind) -> Self {
        field_type_from_persisted_kind(kind)
    }

    fn list(inner: Self) -> Self {
        Self::List(Box::new(inner))
    }

    fn set(inner: Self) -> Self {
        Self::Set(Box::new(inner))
    }

    fn map(key: Self, value: Self) -> Self {
        Self::Map {
            key: Box::new(key),
            value: Box::new(value),
        }
    }

    fn relation(_: &str, _: &str, _: EntityTag, _: &str, key: Self) -> Self {
        key
    }
}

// Failure at any descendant discards the partial output and projects the
// original root without unwrapping. Never preserve a partially unwrapped tree.
fn project_query_kind<P: QueryProjection>(
    kind: &AcceptedFieldKind,
    catalog: &AcceptedCompositeCatalog,
) -> P {
    project_at_depth(kind, catalog, 0).unwrap_or_else(|| P::unprojected(kind))
}

fn project_at_depth<P: QueryProjection>(
    kind: &AcceptedFieldKind,
    catalog: &AcceptedCompositeCatalog,
    depth: usize,
) -> Option<P> {
    if depth >= MAX_ACCEPTED_RECURSIVE_DEPTH {
        return None;
    }
    let next_depth = depth.saturating_add(1);
    match kind {
        AcceptedFieldKind::Composite { .. } => {
            let resolved = catalog.resolve_newtype_value_kind(kind)?;
            project_at_depth(resolved, catalog, next_depth)
        }
        AcceptedFieldKind::Relation {
            target_path,
            target_entity_name,
            target_entity_tag,
            target_store_path,
            key_kind,
        } => Some(P::relation(
            target_path,
            target_entity_name,
            *target_entity_tag,
            target_store_path,
            project_at_depth(key_kind, catalog, next_depth)?,
        )),
        AcceptedFieldKind::List(inner) => {
            Some(P::list(project_at_depth(inner, catalog, next_depth)?))
        }
        AcceptedFieldKind::Set(inner) => {
            Some(P::set(project_at_depth(inner, catalog, next_depth)?))
        }
        AcceptedFieldKind::Map { key, value } => Some(P::map(
            project_at_depth(key, catalog, next_depth)?,
            project_at_depth(value, catalog, next_depth)?,
        )),
        _ => Some(P::unprojected(kind)),
    }
}
