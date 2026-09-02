//! Module: executor::aggregate::runtime::grouped_fold::utils::path_key
//! Responsibility: allocation-free scalar-path group-key probes.
//! Does not own: path admission, grouped strategy selection, or key canonicalization.
//! Boundary: evaluates planner-bound direct/path sources against one row view.

use std::cmp::Ordering;

use crate::{
    db::{
        executor::{
            group::{GroupKey, KeyCanonicalError, StableHash, stable_hash_from_digest},
            pipeline::runtime::RowView,
            projection::resolve_value_field_path,
        },
        numeric::canonical_value_compare,
        query::plan::{GroupField, expr::ProjectionEvalError},
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

    Ok(
        resolve_value_field_path(root, path.label(), path.path().segments())
            .map_err(ProjectionEvalError::into_invalid_logical_plan_internal_error)?
            .unwrap_or(&Value::Null),
    )
}

/// Hash one virtual path-aware group tuple without materializing a value vector.
pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn stable_hash_path_group_values(
    row_view: &RowView,
    group_fields: &[GroupField],
) -> Result<StableHash, InternalError> {
    let mut hash_writer = ValueHashWriter::new();
    hash_writer.write_list_prefix(group_fields.len());
    for field in group_fields {
        hash_writer.write_list_value(resolve_group_field_value(row_view, field)?)?;
    }

    Ok(stable_hash_from_digest(hash_writer.finish()))
}

/// Compare one owned canonical key with the row's borrowed path-aware tuple.
pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn group_key_matches_path_group_values(
    group_key: &GroupKey,
    row_view: &RowView,
    group_fields: &[GroupField],
) -> Result<bool, InternalError> {
    let Value::List(canonical_values) = group_key.canonical_value() else {
        return Err(InternalError::query_executor_invariant());
    };
    if canonical_values.len() != group_fields.len() {
        return Err(InternalError::query_executor_invariant());
    }

    for (field, canonical_value) in group_fields.iter().zip(canonical_values) {
        let value = resolve_group_field_value(row_view, field)?;
        if canonical_value_compare(value, canonical_value) != Ordering::Equal {
            return Ok(false);
        }
    }

    Ok(true)
}

/// Materialize one owned path-aware tuple only after a group-table miss.
pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn materialize_path_group_key(
    row_view: &RowView,
    group_fields: &[GroupField],
    hash: StableHash,
) -> Result<GroupKey, InternalError> {
    let mut values = Vec::with_capacity(group_fields.len());
    for field in group_fields {
        values.push(resolve_group_field_value(row_view, field)?.clone());
    }

    GroupKey::from_group_values_with_hash(values, hash)
        .map_err(KeyCanonicalError::into_internal_error)
}

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            executor::{group::GroupKey, pipeline::runtime::RowView},
            query::plan::{FieldSlot, GroupField},
            schema::AcceptedFieldKind,
        },
        value::Value,
    };

    use super::{
        group_key_matches_path_group_values, materialize_path_group_key,
        stable_hash_path_group_values,
    };

    fn rank_path() -> GroupField {
        GroupField::scalar_path_for_test(
            "profile.rank",
            "profile",
            vec!["rank".to_string()],
            1,
            AcceptedFieldKind::Int32,
        )
    }

    fn profile(rank: Value) -> Value {
        Value::Map(vec![(Value::Text("rank".to_string()), rank)])
    }

    #[test]
    fn path_probe_hashes_matches_and_materializes_one_canonical_tuple() {
        let row = RowView::new(vec![Some(Value::Int64(7)), Some(profile(Value::Int64(7)))]);
        let fields = [
            GroupField::Direct(FieldSlot::from_test_accepted_kind(
                0,
                "direct_rank",
                AcceptedFieldKind::Int32,
            )),
            rank_path(),
        ];

        let hash = stable_hash_path_group_values(&row, &fields).expect("path tuple hash");
        let key = materialize_path_group_key(&row, &fields, hash).expect("path tuple key");

        assert_eq!(
            key,
            GroupKey::from_group_values(vec![Value::Int64(7), Value::Int64(7)])
                .expect("expected canonical key"),
        );
        assert!(
            group_key_matches_path_group_values(&key, &row, &fields).expect("path tuple equality")
        );
    }

    #[test]
    fn missing_member_and_null_ancestor_share_the_null_group() {
        let fields = [rank_path()];
        let missing = RowView::new(vec![
            None,
            Some(Value::Map(vec![(
                Value::Text("other".to_string()),
                Value::Int64(7),
            )])),
        ]);
        let null = RowView::new(vec![None, Some(Value::Null)]);

        let missing_hash =
            stable_hash_path_group_values(&missing, &fields).expect("missing path hash");
        let missing_key =
            materialize_path_group_key(&missing, &fields, missing_hash).expect("missing path key");
        let null_hash = stable_hash_path_group_values(&null, &fields).expect("null path hash");

        assert_eq!(missing_hash, null_hash);
        assert!(
            group_key_matches_path_group_values(&missing_key, &null, &fields)
                .expect("null path equality")
        );
        assert_eq!(
            missing_key,
            GroupKey::from_group_values(vec![Value::Null]).expect("expected null key"),
        );
    }

    #[test]
    fn malformed_non_record_ancestor_fails_closed() {
        let row = RowView::new(vec![None, Some(Value::Int64(7))]);
        let _error = stable_hash_path_group_values(&row, &[rank_path()])
            .expect_err("non-record ancestor must fail");
    }
}
