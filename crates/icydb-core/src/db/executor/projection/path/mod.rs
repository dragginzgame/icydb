//! Module: db::executor::projection::path
//! Responsibility: projection-local nested value-storage path resolution.
//! Does not own: planner path lowering, predicate evaluation, or index access.
//! Boundary: hides `ValueStorageView` behind an executor projection helper.

mod cache;

use crate::{
    db::{
        data::{
            CanonicalSlotReader, FieldDecodeError, ValueStorageView,
            decode_structural_value_storage_bytes,
        },
        query::plan::expr::ProjectionEvalError,
    },
    error::InternalError,
    value::Value,
};

pub(in crate::db::executor::projection) use cache::{PathCache, PathError, PathSource, PathValue};

/// Select sibling leaves into caller-admitted backing, retaining row borrows.
/// First matches, including explicit NULL, win; absent ancestors stay absent.
pub(in crate::db::executor) fn select_value_sibling_paths<'value>(
    root: &'value Value,
    prefix: &[impl AsRef<[u8]>],
    members: &[&[u8]],
    selected: &mut [Option<&'value Value>],
) -> Result<(), ProjectionEvalError> {
    selected.fill(None);
    let parent = resolve_value_field_path(root, prefix)?;
    if let Some(parent) = parent.filter(|value| !matches!(value, Value::Null)) {
        select_value_map_members(parent, members, selected, |_, value| value)?;
    }
    Ok(())
}

// Both borrowed selection and owned-root caches retain the same first matching
// member. The projection cache stores positions rather than self-references.
fn select_value_map_members<'value, T>(
    parent: &'value Value,
    members: &[&[u8]],
    selected: &mut [Option<T>],
    mut select: impl FnMut(usize, &'value Value) -> T,
) -> Result<(), ProjectionEvalError> {
    let entries = parent.as_map().ok_or_else(|| {
        let error = InternalError::persisted_row_decode_corruption();
        ProjectionEvalError::FieldPathEvaluationFailed {
            class: error.class(),
            origin: error.origin(),
        }
    })?;
    for (index, (key, value)) in entries.iter().enumerate() {
        if let Value::Text(key) = key {
            for (member, result) in members.iter().zip(selected.iter_mut()) {
                if result.is_none() && key.as_bytes() == *member {
                    *result = Some(select(index, value));
                }
            }
        }
    }
    Ok(())
}

/// Walk one already-materialized record path without cloning nested maps.
/// Missing members and null ancestors both project as an absent scalar leaf;
/// a non-map ancestor is persisted-row corruption.
pub(in crate::db::executor) fn resolve_value_field_path<'value>(
    root: &'value Value,
    segments: &[impl AsRef<[u8]>],
) -> Result<Option<&'value Value>, ProjectionEvalError> {
    let mut current = root;
    for segment in segments {
        if matches!(current, Value::Null) {
            return Ok(None);
        }
        let entries = current.as_map().ok_or_else(|| {
            let err = InternalError::persisted_row_decode_corruption();
            ProjectionEvalError::FieldPathEvaluationFailed {
                class: err.class(),
                origin: err.origin(),
            }
        })?;
        let Some((_, value)) = entries.iter().find(
            |(key, _)| matches!(key, Value::Text(text) if text.as_bytes() == segment.as_ref()),
        ) else {
            return Ok(None);
        };
        current = value;
    }

    Ok(Some(current))
}

/// Resolve an accepted slot path, preserving borrowed traversal for physical
/// payloads and delegating absent historical slots to the logical-slot owner.
pub(in crate::db::executor) fn resolve_slot_field_path(
    slots: &dyn CanonicalSlotReader,
    root_slot: usize,
    segment_bytes: &[Box<[u8]>],
) -> Result<Option<Value>, InternalError> {
    if slots.get_bytes(root_slot).is_some() {
        let raw_bytes = slots.required_bytes(root_slot)?;
        let leaf = resolve_path_segments(raw_bytes, segment_bytes)
            .map_err(|_| InternalError::persisted_row_decode_corruption())?;
        return leaf
            .map(decode_structural_value_storage_bytes)
            .transpose()
            .map_err(|_| InternalError::persisted_row_decode_corruption());
    }

    // Absence alone does not authorize a fill: the owning contract checks the
    // declared slot, row stamp and frozen fill, and validates its bounded payload.
    let root = slots.required_value_by_contract_cow(root_slot)?;
    resolve_value_field_path(root.as_ref(), segment_bytes)
        .map(Option::<&Value>::cloned)
        .map_err(|_| InternalError::persisted_row_decode_corruption())
}

/// Resolve one nested map path using already-encoded segment bytes.
fn resolve_path_segments<'a>(
    raw_bytes: &'a [u8],
    segment_bytes: &[Box<[u8]>],
) -> Result<Option<&'a [u8]>, FieldDecodeError> {
    ValueStorageView::from_raw_map_text_path(raw_bytes, segment_bytes)
        .map(|view| view.map(|view| view.as_bytes()))
}

#[cfg(test)]
mod tests;
