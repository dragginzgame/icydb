//! Module: executor::aggregate::runtime::grouped_fold::utils::path_key
//! Responsibility: scalar-path group-key probes over resolved value borrows.
//! Does not own: path admission, grouped strategy selection, or key canonicalization.
//! Boundary: evaluates planner-bound direct/path sources against one row view.

use icydb_diagnostic_code::DiagnosticExecutionBudgetResource;
use std::cmp::Ordering;

use crate::{
    db::{
        executor::{
            group::{GroupKey, StableHash, stable_hash_from_digest},
            pipeline::runtime::RowView,
            projection::{resolve_value_field_path, select_value_sibling_paths},
        },
        numeric::canonical_value_compare,
        query::plan::{GroupField, expr::ProjectionEvalError},
        resource::budget::charge_current_execution_budget,
    },
    error::InternalError,
    value::{Value, ValueHashWriter},
};

pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn resolve_group_field_value<'row>(
    row_view: &'row RowView,
    field: &GroupField,
) -> Result<&'row Value, InternalError> {
    if let Some(direct) = field.as_direct() {
        return row_view
            .slot_value_ref(direct.index())
            .ok_or_else(InternalError::query_executor_invariant);
    }
    let Some(path) = field.as_scalar_path() else {
        return Err(InternalError::query_executor_invariant());
    };
    if let Some(value) = row_view.predecoded_single_group_path_value() {
        return Ok(value);
    }
    let root = row_view
        .slot_value_ref(path.root_slot())
        .ok_or_else(InternalError::query_executor_invariant)?;

    Ok(resolve_value_field_path(root, path.path().segments())
        .map_err(ProjectionEvalError::into_internal_error)?
        .unwrap_or(&Value::Null))
}

/// Resolve a path tuple once so hashing, collision checks and insertion share it.
pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn resolve_path_group_values<'row>(
    row_view: &'row RowView,
    group_fields: &[GroupField],
) -> Result<Vec<&'row Value>, InternalError> {
    let mut values = Vec::with_capacity(group_fields.len());
    let mut remaining = group_fields;
    while let Some((field, tail)) = remaining.split_first() {
        let count = sibling_group_path_prefix_len(remaining);
        if count < 2 || row_view.predecoded_single_group_path_value().is_some() {
            values.push(resolve_group_field_value(row_view, field)?);
            remaining = tail;
            continue;
        }
        let path = field
            .as_scalar_path()
            .ok_or_else(InternalError::query_executor_invariant)?;
        let root = row_view
            .slot_value_ref(path.root_slot())
            .ok_or_else(InternalError::query_executor_invariant)?;
        let (_, prefix) = path
            .path()
            .segments()
            .split_last()
            .ok_or_else(InternalError::query_executor_invariant)?;
        // Charge borrowed names and selection slots before allocating. Selected
        // values stay borrowed through hashing and collision checks.
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::TemporaryBytes,
            (count as u64)
                .saturating_mul((size_of::<&[u8]>() + size_of::<Option<&Value>>()) as u64),
        )?;
        let mut members = Vec::with_capacity(count);
        for sibling in &remaining[..count] {
            let member = sibling
                .as_scalar_path()
                .and_then(|path| path.path().segments().last())
                .ok_or_else(InternalError::query_executor_invariant)?;
            members.push(member.as_bytes());
        }
        let mut selected = vec![None; count];
        select_value_sibling_paths(root, prefix, &members, &mut selected)
            .map_err(ProjectionEvalError::into_internal_error)?;
        values.extend(
            selected
                .into_iter()
                .map(|value| value.unwrap_or(&Value::Null)),
        );
        remaining = &remaining[count..];
    }
    Ok(values)
}

// Direct fields and distinct parents end a run, preserving tuple/error order.
fn sibling_group_path_prefix_len(fields: &[GroupField]) -> usize {
    let Some(path) = fields.first().and_then(GroupField::as_scalar_path) else {
        return 1;
    };
    let Some((_, prefix)) = path.path().segments().split_last() else {
        return 1;
    };
    fields
        .iter()
        .take_while(|field| {
            field.as_scalar_path().is_some_and(|candidate| {
                candidate.root_slot() == path.root_slot()
                    && candidate
                        .path()
                        .segments()
                        .split_last()
                        .is_some_and(|(_, parent)| parent == prefix)
            })
        })
        .count()
}

/// Hash a resolved tuple without cloning its values.
pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn stable_hash_path_group_values(
    values: &[&Value],
) -> Result<StableHash, InternalError> {
    let mut hash_writer = ValueHashWriter::new();
    hash_writer.write_list_prefix(values.len());
    for value in values {
        hash_writer.write_list_value(value)?;
    }

    Ok(stable_hash_from_digest(hash_writer.finish()))
}

/// Compare an owned canonical key with the already-resolved tuple.
pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn group_key_matches_path_group_values(
    group_key: &GroupKey,
    values: &[&Value],
) -> Result<bool, InternalError> {
    let Value::List(canonical_values) = group_key.canonical_value() else {
        return Err(InternalError::query_executor_invariant());
    };
    if canonical_values.len() != values.len() {
        return Err(InternalError::query_executor_invariant());
    }

    for (value, canonical_value) in values.iter().zip(canonical_values) {
        if canonical_value_compare(value, canonical_value) != Ordering::Equal {
            return Ok(false);
        }
    }

    Ok(true)
}

/// Clone the resolved tuple only after a group-table miss.
pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn materialize_path_group_key(
    values: &[&Value],
    hash: StableHash,
) -> Result<GroupKey, InternalError> {
    GroupKey::from_group_values_with_hash(
        values.iter().map(|value| (*value).clone()).collect(),
        hash,
    )
}

#[cfg(test)]
mod tests;
