//! Query-value projection from accepted schema kinds.
//! One traversal owns newtype resolution, depth and whole-tree fallback;
//! Output constructors select owned metadata or a queryability flag, not semantics.

use crate::{
    db::schema::{
        AcceptedFieldKind, AcceptedFieldKindCategory, FieldType, MAX_ACCEPTED_RECURSIVE_DEPTH,
        MAX_SCHEMA_SNAPSHOT_BYTES, classify_accepted_field_kind,
        composite_catalog::AcceptedCompositeCatalog, field_type_from_persisted_kind,
    },
    error::InternalError,
    types::EntityTag,
};
use std::convert::Infallible;

// A stable cross-target allowance for one expanded metadata node, including its
// inline representation. Text is charged separately. Do not use host size_of:
// schema admission must agree between native tooling and Wasm execution.
const QUERY_PROJECTION_NODE_BYTES: usize = 128;

/// Prove an entity's direct and nested query kinds share the schema-size ceiling.
/// Accepted bundle admission supplies one entity at a time before publishing
/// query authority. Output projection can then stay infallible.
pub(in crate::db::schema) fn validate_query_projections<'a>(
    kinds: impl IntoIterator<Item = &'a AcceptedFieldKind>,
    catalog: &AcceptedCompositeCatalog,
) -> Result<(), InternalError> {
    let mut remaining = MAX_SCHEMA_SNAPSHOT_BYTES as usize;
    let mut charge = |kind: &AcceptedFieldKind| {
        remaining = remaining
            .checked_sub(QUERY_PROJECTION_NODE_BYTES)
            .ok_or_else(InternalError::store_unsupported)?;
        if let AcceptedFieldKind::Relation {
            target_path,
            target_entity_name,
            target_store_path,
            ..
        } = kind
        {
            for text in [target_path, target_entity_name, target_store_path] {
                remaining = remaining
                    .checked_sub(text.len())
                    .ok_or_else(InternalError::store_unsupported)?;
            }
        }
        Ok(())
    };
    for kind in kinds {
        match project_at_depth::<(), _>(kind, catalog, 0, &mut charge) {
            Ok(()) | Err(ProjectionFailure::Unprojected) => {}
            Err(ProjectionFailure::Admission(error)) => return Err(error),
        }
    }

    Ok(())
}

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

/// Inspect queryability without constructing a temporary query type tree.
/// The shared traversal retains newtype, depth and whole-tree fallback semantics.
#[must_use]
pub(in crate::db) fn query_field_is_queryable(
    kind: &AcceptedFieldKind,
    catalog: &AcceptedCompositeCatalog,
) -> bool {
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

// Admission uses the same traversal but constructs no recursive output.
impl QueryProjection for () {
    fn unprojected(_: &AcceptedFieldKind) {}
    fn list((): Self) {}
    fn set((): Self) {}
    fn map((): Self, (): Self) {}
    fn relation(_: &str, _: &str, _: EntityTag, _: &str, (): Self) {}
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

impl QueryProjection for bool {
    fn unprojected(mut kind: &AcceptedFieldKind) -> Self {
        // Fallback ignores newtype definitions, just like the owned type output.
        // Relations expose their physical key; no descendant tree is needed to
        // classify the outer scalar/list/set versus map/composite shape.
        while let AcceptedFieldKind::Relation { key_kind, .. } = kind {
            kind = key_kind;
        }
        match classify_accepted_field_kind(kind).category() {
            AcceptedFieldKindCategory::Scalar(_) => true,
            AcceptedFieldKindCategory::Collection => {
                matches!(kind, AcceptedFieldKind::List(_) | AcceptedFieldKind::Set(_))
            }
            AcceptedFieldKindCategory::Composite | AcceptedFieldKindCategory::Relation(_) => false,
        }
    }

    fn list(_: Self) -> Self {
        true
    }

    fn set(_: Self) -> Self {
        true
    }

    fn map(_: Self, _: Self) -> Self {
        false
    }

    fn relation(_: &str, _: &str, _: EntityTag, _: &str, key: Self) -> Self {
        key
    }
}

// Semantic fallback and resource rejection are deliberately distinct. Admission
// exhaustion must never be converted into an opaque but accepted query type.
enum ProjectionFailure<E> {
    Unprojected,
    Admission(E),
}

// Accepted authority has already passed expansion admission. This no-op visitor
// is erased for ordinary projection; no new per-query budget lifecycle exists.
fn project_query_kind<P: QueryProjection>(
    kind: &AcceptedFieldKind,
    catalog: &AcceptedCompositeCatalog,
) -> P {
    match project_at_depth(kind, catalog, 0, &mut |_| Ok::<(), Infallible>(())) {
        Ok(output) => output,
        Err(ProjectionFailure::Unprojected) => P::unprojected(kind),
        Err(ProjectionFailure::Admission(never)) => match never {},
    }
}

fn project_at_depth<P: QueryProjection, E>(
    kind: &AcceptedFieldKind,
    catalog: &AcceptedCompositeCatalog,
    depth: usize,
    visit: &mut impl FnMut(&AcceptedFieldKind) -> Result<(), E>,
) -> Result<P, ProjectionFailure<E>> {
    visit(kind).map_err(ProjectionFailure::Admission)?;
    if depth >= MAX_ACCEPTED_RECURSIVE_DEPTH {
        return Err(ProjectionFailure::Unprojected);
    }
    let next_depth = depth.saturating_add(1);
    match kind {
        AcceptedFieldKind::Composite { .. } => {
            let resolved = catalog
                .resolve_newtype_value_kind(kind)
                .ok_or(ProjectionFailure::Unprojected)?;
            project_at_depth(resolved, catalog, next_depth, visit)
        }
        AcceptedFieldKind::Relation {
            target_path,
            target_entity_name,
            target_entity_tag,
            target_store_path,
            key_kind,
        } => Ok(P::relation(
            target_path,
            target_entity_name,
            *target_entity_tag,
            target_store_path,
            project_at_depth(key_kind, catalog, next_depth, visit)?,
        )),
        AcceptedFieldKind::List(inner) => Ok(P::list(project_at_depth(
            inner, catalog, next_depth, visit,
        )?)),
        AcceptedFieldKind::Set(inner) => {
            Ok(P::set(project_at_depth(inner, catalog, next_depth, visit)?))
        }
        AcceptedFieldKind::Map { key, value } => Ok(P::map(
            project_at_depth(key, catalog, next_depth, visit)?,
            project_at_depth(value, catalog, next_depth, visit)?,
        )),
        _ => Ok(P::unprojected(kind)),
    }
}
