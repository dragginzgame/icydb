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

    #[test]
    fn sibling_group_paths_preserve_borrows_tuple_order_and_diagnostics() {
        use super::resolve_group_field_value;
        let missing = GroupField::scalar_path_for_test(
            "profile.missing",
            "profile",
            vec!["missing".into()],
            1,
            AcceptedFieldKind::Int32,
        );
        let direct = GroupField::Direct(FieldSlot::from_test_accepted_kind(
            0,
            "id",
            AcceptedFieldKind::Int32,
        ));
        let fields = [direct.clone(), rank_path(), missing, rank_path(), direct];
        for root in [
            Some(profile(Value::Int64(7))),
            Some(Value::Null),
            Some(Value::Map(vec![])),
            Some(Value::Map(vec![
                (Value::Text("rank".into()), Value::Null),
                (Value::Text("rank".into()), Value::Int64(9)),
            ])),
        ] {
            let row = RowView::new(vec![Some(Value::Int64(3)), root]);
            let expected: Vec<_> = fields
                .iter()
                .map(|field| resolve_group_field_value(&row, field).unwrap())
                .collect();
            let values = resolve_path_group_values(&row, &fields).unwrap();
            assert_eq!(values, expected);
            for (selected, ordinary) in values.iter().zip(&expected) {
                if !matches!(selected, Value::Null) {
                    assert!(std::ptr::eq(*selected, *ordinary));
                }
            }
            let hash = stable_hash_path_group_values(&values).unwrap();
            let key = materialize_path_group_key(
                &expected,
                stable_hash_path_group_values(&expected).unwrap(),
            )
            .unwrap();
            assert!(group_key_matches_path_group_values(&key, &values).unwrap());
            assert_eq!(materialize_path_group_key(&values, hash).unwrap(), key);
        }
        for root in [None, Some(Value::Int64(9))] {
            let row = RowView::new(vec![Some(Value::Int64(3)), root]);
            let expected = resolve_group_field_value(&row, &fields[1]).unwrap_err();
            let error = resolve_path_group_values(&row, &fields).unwrap_err();
            assert_eq!(error.class(), expected.class());
            assert_eq!(error.origin(), expected.origin());
            assert_eq!(error.diagnostic(), expected.diagnostic());
            assert_eq!(error.diagnostic_facts(), expected.diagnostic_facts());
        }
    }

    #[test]
    fn nested_sibling_group_paths_preserve_parent_boundaries() {
        use super::resolve_group_field_value;
        let fields: Vec<_> = [
            ("left", "rank"),
            ("left", "missing"),
            ("right", "rank"),
            ("right", "missing"),
        ]
        .into_iter()
        .map(|(parent, leaf)| {
            GroupField::scalar_path_for_test(
                format!("profile.{parent}.{leaf}"),
                "profile",
                vec![parent.into(), leaf.into()],
                1,
                AcceptedFieldKind::Int32,
            )
        })
        .collect();
        for parent in [
            profile(Value::Int64(7)),
            Value::Null,
            Value::Map(vec![]),
            Value::Int64(9),
        ] {
            let row = RowView::new(vec![
                None,
                Some(Value::Map(vec![
                    (Value::Text("left".into()), parent),
                    (Value::Text("left".into()), profile(Value::Int64(99))),
                    (Value::Text("right".into()), profile(Value::Int64(8))),
                ])),
            ]);
            let expected: Result<Vec<_>, _> = fields
                .iter()
                .map(|field| resolve_group_field_value(&row, field))
                .collect();
            match expected {
                Ok(expected) => {
                    assert_eq!(resolve_path_group_values(&row, &fields).unwrap(), expected);
                }
                Err(expected) => {
                    let error = resolve_path_group_values(&row, &fields).unwrap_err();
                    assert_eq!(error.class(), expected.class());
                    assert_eq!(error.origin(), expected.origin());
                    assert_eq!(error.diagnostic(), expected.diagnostic());
                    assert_eq!(error.diagnostic_facts(), expected.diagnostic_facts());
                }
            }
        }
    }

    #[test]
    fn sibling_group_path_selection_admits_exact_borrowed_scratch() {
        use crate::db::{
            RequestExecutionRoot,
            resource::budget::{
                HardExecutionBudget, HardExecutionContext, HardExecutionFailureHeadroom,
                with_read_execution_budget,
            },
        };
        use icydb_diagnostic_code::{
            DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionBudgetScope,
            DiagnosticExecutionLane, DiagnosticFactTag,
        };
        let row = RowView::new(vec![None, Some(profile(Value::Int64(7)))]);
        let fields = [rank_path(), rank_path()];
        let needed = 2 * (size_of::<&[u8]>() + size_of::<Option<&Value>>()) as u64;
        for allowance in [needed - 1, needed] {
            let root = RequestExecutionRoot::new_for_tests(
                HardExecutionBudget::uniform_for_tests(
                    u64::MAX,
                    HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                )
                .with_limit_for_tests(Resource::TemporaryBytes, allowance),
            );
            let result = with_read_execution_budget(
                &root.scope(),
                HardExecutionContext::new(
                    DiagnosticExecutionBudgetScope::Execution,
                    DiagnosticExecutionLane::PublicRead,
                    1,
                ),
                || resolve_path_group_values(&row, &fields),
            );
            if allowance == needed {
                assert_eq!(result.unwrap(), vec![&Value::Int64(7); 2]);
            } else {
                assert!(result.unwrap_err().diagnostic_facts().contains(&(
                    DiagnosticFactTag::BudgetResource,
                    Resource::TemporaryBytes.raw(),
                )));
            }
        }
    }

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
