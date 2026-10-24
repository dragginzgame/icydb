//! Module: executor::aggregate::runtime::grouped_fold::utils::path_key
//! Responsibility: scalar-path group-key probes over resolved value borrows.
//! Does not own: path admission, grouped strategy selection, or key canonicalization.
//! Boundary: evaluates planner-bound direct/path sources against one row view.

use std::cmp::Ordering;

use crate::{
    db::{
        executor::{
            group::{GroupKey, StableHash, stable_hash_from_digest},
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

    Ok(resolve_value_field_path(root, path.path().segments())
        .map_err(ProjectionEvalError::into_internal_error)?
        .unwrap_or(&Value::Null))
}

/// Resolve a path tuple once so hashing, collision checks and insertion share it.
pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn resolve_path_group_values<'row>(
    row_view: &'row RowView,
    group_fields: &[GroupField],
) -> Result<Vec<&'row Value>, InternalError> {
    group_fields
        .iter()
        .map(|field| resolve_group_field_value(row_view, field))
        .collect()
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
        group_key_matches_path_group_values, materialize_path_group_key, resolve_path_group_values,
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

        let values = resolve_path_group_values(&row, &fields).expect("resolved tuple");
        let hash = stable_hash_path_group_values(&values).expect("path tuple hash");
        let key = materialize_path_group_key(&values, hash).expect("path tuple key");

        assert_eq!(
            key,
            GroupKey::from_group_values(vec![Value::Int64(7), Value::Int64(7)])
                .expect("expected canonical key"),
        );
        assert!(group_key_matches_path_group_values(&key, &values).expect("path tuple equality"));
        let collision =
            GroupKey::from_group_values_with_hash(vec![Value::Int64(8), Value::Int64(7)], hash)
                .expect("same hash, distinct tuple");
        assert!(!group_key_matches_path_group_values(&collision, &values).unwrap());
        assert!(group_key_matches_path_group_values(&key, &values[..1]).is_err());
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

        let missing_values = resolve_path_group_values(&missing, &fields).expect("missing tuple");
        let null_values = resolve_path_group_values(&null, &fields).expect("null tuple");
        let missing_hash =
            stable_hash_path_group_values(&missing_values).expect("missing path hash");
        let missing_key =
            materialize_path_group_key(&missing_values, missing_hash).expect("missing path key");
        let null_hash = stable_hash_path_group_values(&null_values).expect("null path hash");

        assert_eq!(missing_hash, null_hash);
        assert!(
            group_key_matches_path_group_values(&missing_key, &null_values)
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
        let _error = resolve_path_group_values(&row, &[rank_path()])
            .expect_err("non-record ancestor must fail");
    }
}
