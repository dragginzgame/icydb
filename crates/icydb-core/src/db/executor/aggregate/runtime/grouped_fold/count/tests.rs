use crate::{
    db::{
        executor::{
            aggregate::{
                ExecutionConfig, ExecutionContext,
                runtime::grouped_fold::{
                    count::{GroupedCountState, window::GroupedCountWindowSelection},
                    utils::stable_hash_group_values_from_row_view,
                },
            },
            pipeline::runtime::RowView,
        },
        query::plan::FieldSlot,
    },
    error::InternalError,
    types::Decimal,
    value::{Value, with_test_hash_override},
};

fn group_fields(indices: &[usize]) -> Vec<FieldSlot> {
    indices
        .iter()
        .map(|index| FieldSlot::from_parts_for_test(*index, format!("field_{index}")))
        .collect()
}

#[test]
fn grouped_count_fast_path_hash_matches_owned_group_key_hash() {
    fn supports_group_probe(
        row_view: &RowView,
        group_fields: &[FieldSlot],
    ) -> Result<bool, InternalError> {
        fn group_value_supports_group_probe(value: &Value) -> bool {
            match value {
                Value::List(_) | Value::Map(_) | Value::Unit => false,
                Value::Enum(value_enum) => value_enum
                    .payload()
                    .is_none_or(group_value_supports_group_probe),
                _ => true,
            }
        }

        for field in group_fields {
            let supports = row_view.with_required_slot(field.index(), |value| {
                Ok(group_value_supports_group_probe(value))
            })?;
            if !supports {
                return Ok(false);
            }
        }

        Ok(true)
    }

    let row_view = RowView::new(vec![
        Some(Value::Decimal(Decimal::new(100, 2))),
        Some(Value::Text("alpha".to_string())),
    ]);
    let group_fields = group_fields(&[0, 1]);

    assert!(
        supports_group_probe(&row_view, &group_fields).expect("borrowed probe"),
        "scalar grouped values should stay on the borrowed grouped-count fast path",
    );

    let borrowed_hash =
        stable_hash_group_values_from_row_view(&row_view, &group_fields).expect("hash");
    let owned_group_key = crate::db::executor::group::GroupKey::from_group_values(
        row_view.group_values(&group_fields).expect("group values"),
    )
    .expect("owned group key");

    assert_eq!(
        borrowed_hash,
        owned_group_key.hash(),
        "borrowed grouped-count hashing must stay aligned with owned canonical group-key hashing",
    );
}

#[test]
fn grouped_count_fast_path_rejects_structured_group_values() {
    fn supports_group_probe(
        row_view: &RowView,
        group_fields: &[FieldSlot],
    ) -> Result<bool, InternalError> {
        fn group_value_supports_group_probe(value: &Value) -> bool {
            match value {
                Value::List(_) | Value::Map(_) | Value::Unit => false,
                Value::Enum(value_enum) => value_enum
                    .payload()
                    .is_none_or(group_value_supports_group_probe),
                _ => true,
            }
        }

        for field in group_fields {
            let supports = row_view.with_required_slot(field.index(), |value| {
                Ok(group_value_supports_group_probe(value))
            })?;
            if !supports {
                return Ok(false);
            }
        }

        Ok(true)
    }

    let row_view = RowView::new(vec![Some(Value::List(vec![Value::Uint(7)]))]);
    let group_fields = group_fields(&[0]);

    assert!(
        !supports_group_probe(&row_view, &group_fields).expect("borrowed probe"),
        "structured grouped values must fall back to owned canonical key materialization",
    );
}

#[test]
fn grouped_count_fast_path_handles_hash_collisions_without_merging_groups() {
    with_test_hash_override([0xAB; 16], || {
        let mut grouped_execution_context = ExecutionContext::new(ExecutionConfig::unbounded());
        let group_fields = group_fields(&[0]);
        let alpha = RowView::new(vec![Some(Value::Text("alpha".to_string()))]);
        let beta = RowView::new(vec![Some(Value::Text("beta".to_string()))]);
        let mut grouped_counts = GroupedCountState::new();

        grouped_counts
            .increment_row_borrowed_group_probe(
                &alpha,
                &group_fields,
                &mut grouped_execution_context,
            )
            .expect("alpha insert");
        grouped_counts
            .increment_row_borrowed_group_probe(
                &beta,
                &group_fields,
                &mut grouped_execution_context,
            )
            .expect("beta insert");
        grouped_counts
            .increment_row_borrowed_group_probe(
                &alpha,
                &group_fields,
                &mut grouped_execution_context,
            )
            .expect("alpha increment");

        let mut rows = grouped_counts.into_groups();
        rows.sort_by(|(left_key, _), (right_key, _)| {
            crate::db::numeric::canonical_value_compare(
                left_key.canonical_value(),
                right_key.canonical_value(),
            )
        });
        assert_eq!(
            rows,
            vec![
                (
                    crate::db::executor::group::GroupKey::from_group_values(vec![Value::Text(
                        "alpha".to_string(),
                    )])
                    .expect("alpha key"),
                    2,
                ),
                (
                    crate::db::executor::group::GroupKey::from_group_values(vec![Value::Text(
                        "beta".to_string(),
                    )])
                    .expect("beta key"),
                    1,
                ),
            ],
            "same-hash grouped count rows must remain distinct under canonical grouped equality",
        );
    });
}

#[test]
fn grouped_count_bounded_candidate_selection_keeps_smallest_canonical_window() {
    let rows = vec![
        (
            crate::db::executor::group::GroupKey::from_group_values(vec![Value::Uint(9)])
                .expect("group key"),
            9,
        ),
        (
            crate::db::executor::group::GroupKey::from_group_values(vec![Value::Uint(2)])
                .expect("group key"),
            2,
        ),
        (
            crate::db::executor::group::GroupKey::from_group_values(vec![Value::Uint(5)])
                .expect("group key"),
            5,
        ),
        (
            crate::db::executor::group::GroupKey::from_group_values(vec![Value::Uint(1)])
                .expect("group key"),
            1,
        ),
    ];

    let route = crate::db::executor::pipeline::contracts::GroupedRouteStage::new_for_test(
        crate::db::direction::Direction::Asc,
        Some(3),
    );
    let selected = GroupedCountWindowSelection::new(&route)
        .expect("grouped count window selection should compile")
        .retain_smallest_candidates(rows, 3);

    assert_eq!(
        selected
            .into_iter()
            .map(|(group_key, count)| (group_key.into_canonical_value(), count))
            .collect::<Vec<_>>(),
        vec![
            (Value::List(vec![Value::Uint(1)]), 1),
            (Value::List(vec![Value::Uint(2)]), 2),
            (Value::List(vec![Value::Uint(5)]), 5),
        ],
        "bounded grouped count selection should retain the smallest canonical grouped-key window only",
    );
}
