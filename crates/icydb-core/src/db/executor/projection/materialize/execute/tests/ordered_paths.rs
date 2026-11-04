//! Scalar sibling selection with retained ordering values.

use super::*;
use crate::db::{
    executor::{DataRowOrderWindow, terminal::RetainedSlotLayout},
    query::plan::{OrderDirection, ResolvedOrder, ResolvedOrderField, ResolvedOrderValueSource},
};

#[test]
fn ordered_sibling_paths_batch_around_cached_leaves() {
    let layout = RowLayout::from_structural_row_contract(projection_contract(
        AcceptedFieldKind::Map {
            key: Box::new(AcceptedFieldKind::Text { max_len: None }),
            value: Box::new(AcceptedFieldKind::Text { max_len: None }),
        },
        LeafCodec::Structural,
        FieldStorageDecode::CatalogValue,
    ));
    let expressions: Vec<_> = ["city", "name", "missing", "city", "name", "missing"]
        .into_iter()
        .map(|name| CompiledExpr::FieldPath {
            root_slot: 1,
            segments: vec![name.into()].into_boxed_slice(),
            segment_bytes: vec![name.as_bytes().to_vec().into_boxed_slice()].into_boxed_slice(),
        })
        .collect();
    for payload in [
        Value::Map(vec![
            (Value::Text("city".into()), Value::Text("Paris".into())),
            (Value::Text("name".into()), Value::Text("Ada".into())),
        ]),
        Value::Null,
    ] {
        for direction in [OrderDirection::Asc, OrderDirection::Desc] {
            let order = ResolvedOrder::new(vec![ResolvedOrderField::new(
                ResolvedOrderValueSource::expression(expressions[0].clone()),
                direction,
            )]);
            let mut window = DataRowOrderWindow::new(layout.clone(), &order, Some(1));
            window
                .push(projection_row(layout.contract(), &payload), None)
                .unwrap();
            let ordered = window
                .into_sorted_rows()
                .unwrap()
                .into_rows()
                .next()
                .unwrap();
            let (raw, cached) = ordered.into_parts();
            let cached = cached.expect("expression ordering retains its value");
            let expected =
                project_scalar_data_row(&expressions, raw.data_row(), &layout, None).unwrap();
            let expected_cached = (!matches!(payload, Value::Null)).then_some(&expected[0]);
            assert_eq!(cached.projection_value(&expressions[0]), expected_cached);
            assert_eq!(
                project_scalar_data_row(&expressions, raw.data_row(), &layout, Some(&cached))
                    .unwrap(),
                expected
            );
            // Repeated projection borrows the same ordering result; it does not
            // consume it or hand distinct output columns shared ownership.
            assert_eq!(
                project_scalar_data_row(&expressions, raw.data_row(), &layout, Some(&cached))
                    .unwrap(),
                expected
            );
            assert_eq!(cached.projection_value(&expressions[0]), expected_cached);
            // A missing expression result is deliberately not reusable. It takes
            // ordinary projection handling, including the full sibling batch.
            if expected_cached.is_none() {
                continue;
            }
            let slots = RetainedSlotLayout::compile(2, vec![1]);
            let retained =
                RetainedSlotRow::from_indexed_values(&slots, vec![Some(payload.clone())]);
            assert_mixed_projection_scratch_refusal(
                &expressions,
                raw.data_row(),
                &layout,
                &retained,
                &expected,
                Some(&cached),
            );
        }
    }
}
