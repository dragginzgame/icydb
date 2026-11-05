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
    let missing_hash = stable_hash_path_group_values(&missing_values).expect("missing path hash");
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
    let _error =
        resolve_path_group_values(&row, &[rank_path()]).expect_err("non-record ancestor must fail");
}
