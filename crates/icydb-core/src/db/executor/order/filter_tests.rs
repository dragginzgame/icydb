//! Filtered raw-row ordering preserves lazy admission and bounded selection.

use super::*;
use crate::{
    db::{
        data::{
            DecodedDataStoreKey, StructuralRowContract,
            canonical_row_from_runtime_value_source_with_accepted_contract,
        },
        key_taxonomy::PrimaryKeyComponent,
        predicate::{Predicate, PredicateProgram},
        query::plan::{ResolvedOrderField, expr::CompiledExpr},
        resource::budget::{
            HardExecutionBudget, HardExecutionContext, HardExecutionFailureHeadroom,
            current_execution_budget_usage, with_execution_budget_for_tests,
        },
        schema::{
            AcceptedCompositeCatalog, AcceptedFieldKind, AcceptedRowLayoutRuntimeContract,
            AcceptedSchemaRevision, AcceptedSchemaSnapshot, AcceptedValueCatalogHandle, FieldId,
            FieldStorageDecode, LeafCodec, PersistedFieldSnapshot, PersistedSchemaSnapshot,
            ScalarCodec, SchemaFieldSlot, SchemaInfo, SchemaInsertDefault, SchemaRowLayout,
            SchemaVersion, empty_accepted_enum_catalog_for_tests,
        },
    },
    types::EntityTag,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetScope, DiagnosticExecutionLane, DiagnosticFactTag,
};

fn layout_and_schema(max_len: Option<u32>) -> (RowLayout, SchemaInfo) {
    let fields = [
        ("id", AcceptedFieldKind::Nat64, ScalarCodec::Nat64, false),
        ("keep", AcceptedFieldKind::Bool, ScalarCodec::Bool, false),
        (
            "rank",
            AcceptedFieldKind::Text { max_len },
            ScalarCodec::Text,
            true,
        ),
    ]
    .into_iter()
    .enumerate()
    .map(|(slot, (name, kind, codec, nullable))| {
        PersistedFieldSnapshot::new_initial(
            FieldId::new(u32::try_from(slot + 1).unwrap()),
            name.into(),
            SchemaFieldSlot::new(u16::try_from(slot).unwrap()),
            kind,
            Vec::new(),
            nullable,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(codec),
        )
    })
    .collect::<Vec<_>>();
    let accepted = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::FilteredOrder".into(),
        "FilteredOrder".into(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    ));
    let catalog = AcceptedValueCatalogHandle::new_for_tests(
        empty_accepted_enum_catalog_for_tests(),
        AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    );
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted).unwrap();
    let layout = RowLayout::from_structural_row_contract(
        StructuralRowContract::from_accepted_decode_contract(
            accepted.entity_path(),
            descriptor.row_decode_contract(catalog.clone()),
        ),
    );
    (
        layout,
        SchemaInfo::from_accepted_snapshot_and_catalog(&accepted, catalog),
    )
}

fn row(layout: &RowLayout, id: u64, keep: bool, rank: Option<&str>) -> DataRow {
    let values = [
        Value::Nat64(id),
        Value::Bool(keep),
        rank.map_or(Value::Null, |v| Value::Text(v.into())),
    ];
    let raw =
        canonical_row_from_runtime_value_source_with_accepted_contract(layout.contract(), |slot| {
            Ok(Cow::Borrowed(&values[slot]))
        })
        .unwrap()
        .into_raw_row();
    (
        DecodedDataStoreKey::new(EntityTag::new(1), &PrimaryKeyComponent::Nat64(id).into()),
        raw,
    )
}

fn order(expression: bool, direction: OrderDirection) -> ResolvedOrder {
    ResolvedOrder::new(vec![
        ResolvedOrderField::new(
            if expression {
                ResolvedOrderValueSource::expression(CompiledExpr::Slot { slot: 2 })
            } else {
                ResolvedOrderValueSource::direct_field(2)
            },
            direction,
        ),
        ResolvedOrderField::new(ResolvedOrderValueSource::direct_field(0), direction),
    ])
}

#[test]
fn filtered_raw_order_preserves_nulls_ties_and_bounded_winners() {
    let (layout, schema) = layout_and_schema(None);
    let predicate = PredicateProgram::compile_with_schema_info(
        &schema,
        &Predicate::eq("keep".into(), Value::Bool(true)),
    );
    let filters = [
        EffectiveRuntimeFilterProgram::predicate(predicate.clone()),
        EffectiveRuntimeFilterProgram::expression(CompiledExpr::Slot { slot: 1 }, None),
        EffectiveRuntimeFilterProgram::expression(CompiledExpr::Slot { slot: 1 }, Some(predicate)),
    ];
    for filter in filters {
        for expression in [false, true] {
            for direction in [OrderDirection::Asc, OrderDirection::Desc] {
                let order = order(expression, direction);
                let mut expected = [1, 2, 4, 5];
                if direction == OrderDirection::Desc {
                    expected.reverse();
                }
                for keep_count in [None, Some(1), Some(3), Some(8)] {
                    let mut window = DataRowOrderWindow::new(layout.clone(), &order, keep_count);
                    for (id, keep, rank) in [
                        (1, true, None),
                        (2, true, Some("a")),
                        (3, false, Some("a")),
                        (4, true, Some("a")),
                        (5, true, Some("z")),
                    ] {
                        assert_eq!(
                            window
                                .push(row(&layout, id, keep, rank), Some(&filter))
                                .unwrap(),
                            keep
                        );
                    }
                    let actual = window
                        .into_sorted_rows()
                        .unwrap()
                        .into_rows()
                        .map(|row| row.data_row().0.primary_key_value())
                        .collect::<Vec<_>>();
                    let expected = expected
                        .iter()
                        .take(keep_count.unwrap_or(usize::MAX))
                        .map(|&id| PrimaryKeyComponent::Nat64(id).into())
                        .collect::<Vec<_>>();
                    assert_eq!(actual, expected);
                }
            }
        }
    }
}

#[test]
fn filtered_raw_order_does_not_admit_rejected_order_slots_or_keys() {
    let (writer, _) = layout_and_schema(None);
    let (reader, _) = layout_and_schema(Some(1));
    let filter = EffectiveRuntimeFilterProgram::expression(CompiledExpr::Slot { slot: 1 }, None);
    let order = order(false, OrderDirection::Asc);
    let mut window = DataRowOrderWindow::new(reader, &order, Some(1));
    assert!(
        !window
            .push(row(&writer, 1, false, Some("too long")), Some(&filter))
            .unwrap()
    );
    let mut rejected = row(&writer, 1, false, Some("a"));
    rejected.0 = DecodedDataStoreKey::new(EntityTag::new(1), &PrimaryKeyComponent::Nat64(2).into());
    assert!(!window.push(rejected, Some(&filter)).unwrap());
    assert!(
        window
            .push(row(&writer, 1, true, Some("too long")), Some(&filter))
            .is_err()
    );
    let mut mismatched = row(&writer, 1, true, Some("a"));
    mismatched.0 =
        DecodedDataStoreKey::new(EntityTag::new(1), &PrimaryKeyComponent::Nat64(2).into());
    assert!(window.push(mismatched, Some(&filter)).is_err());
    assert_eq!(window.into_sorted_rows().unwrap().row_count(), 0);
}

#[test]
fn filtered_raw_order_charges_decode_before_filter_and_sort_only_after_admission() {
    use DiagnosticExecutionBudgetResource::{DecodedBytes, SortEntries};
    let (layout, _) = layout_and_schema(None);
    let order = order(false, OrderDirection::Asc);
    let filter = EffectiveRuntimeFilterProgram::expression(CompiledExpr::Slot { slot: 1 }, None);
    for keep in [false, true] {
        let candidate = row(&layout, 1, keep, Some("a"));
        let bytes = u64::try_from(candidate.1.len()).unwrap();
        for limit in [bytes - 1, bytes] {
            let budget = HardExecutionBudget::uniform_for_tests(
                16_000_000,
                HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
            )
            .with_limit_for_tests(DecodedBytes, limit);
            let context = HardExecutionContext::new(
                DiagnosticExecutionBudgetScope::Execution,
                DiagnosticExecutionLane::TrustedRead,
                0,
            );
            with_execution_budget_for_tests(
                budget,
                context,
                || {
                    let mut window = DataRowOrderWindow::new(layout.clone(), &order, Some(1));
                    let result = window.push(candidate.clone(), Some(&filter));
                    let usage = current_execution_budget_usage()?;
                    // Exhausted charges retain attempted work in the counters.
                    assert_eq!(usage.observed(DecodedBytes), bytes);
                    if limit < bytes {
                        assert!(
                            result
                                .unwrap_err()
                                .diagnostic_facts()
                                .contains(
                                    &(DiagnosticFactTag::BudgetResource, DecodedBytes.raw(),)
                                )
                        );
                        assert_eq!(usage.observed(SortEntries), 0);
                    } else {
                        assert_eq!(result.unwrap(), keep);
                        assert_eq!(usage.observed(SortEntries), u64::from(keep));
                    }
                    Ok(())
                },
                |error| error,
            )
            .unwrap();
        }
    }
}

#[test]
fn selected_raw_order_values_move_without_decoding_again() {
    let (layout, _) = layout_and_schema(None);
    let order = order(false, OrderDirection::Asc);
    let mut window = DataRowOrderWindow::new(layout.clone(), &order, Some(1));
    window
        .push(row(&layout, 1, true, Some("retained")), None)
        .unwrap();
    let rows = window.into_sorted_rows().unwrap();
    let ProjectionDataRowStorage::Ordered { rows: pairs, .. } = &rows.storage else {
        panic!("cached rows expected")
    };
    let Some(Some(Value::Text(text))) = pairs[0].1.value_at(0) else {
        panic!("cached text expected")
    };
    let pointer = text.as_ptr();
    assert_eq!(
        rows.cursor_boundary_at(0, &layout, &order)
            .unwrap()
            .unwrap()
            .slots,
        vec![
            CursorBoundarySlot::Present(Value::Text("retained".into())),
            CursorBoundarySlot::Present(Value::Nat64(1))
        ]
    );
    let wrong = super::filter_tests::order(false, OrderDirection::Desc);
    assert_eq!(
        rows.cursor_boundary_at(0, &layout, &wrong)
            .unwrap_err()
            .diagnostic_code(),
        InternalError::query_executor_invariant().diagnostic_code()
    );
    let (_, Some(mut cached)) = rows.into_rows().next().unwrap().into_parts() else {
        panic!("cached values expected")
    };
    let Some(Value::Text(text)) = cached.take_slot(2) else {
        panic!("cached text expected")
    };
    assert_eq!(text.as_ptr(), pointer);
}

#[test]
fn kernel_order_slots_survive_raw_page_finalization() {
    use crate::db::executor::terminal::{RetainedSlotLayout, RetainedSlotRow};
    let (layout, _) = layout_and_schema(None);
    let order = order(false, OrderDirection::Asc);
    let text = String::from("kept");
    let pointer = text.as_ptr();
    let retained = RetainedSlotLayout::compile(3, vec![2, 0]);
    let kernel = KernelRow::new_with_retained_slots(
        row(&layout, 7, true, Some("kept")),
        RetainedSlotRow::from_indexed_values(
            &retained,
            vec![Some(Value::Text(text)), Some(Value::Nat64(7))],
        ),
    );
    let rows = ProjectionDataRows::from_kernel_rows(vec![kernel], Some(&order)).unwrap();
    let (_, Some(mut cached)) = rows.into_rows().next().unwrap().into_parts() else {
        panic!("retained order values expected")
    };
    let Some(Value::Text(text)) = cached.take_slot(2) else {
        panic!("retained text expected")
    };
    assert_eq!(text.as_ptr(), pointer);
    assert_eq!(cached.take_slot(0), Some(Value::Nat64(7)));

    let kernel = KernelRow::new_with_retained_slots(
        row(&layout, 7, true, Some("kept")),
        RetainedSlotRow::from_indexed_values(
            &retained,
            vec![Some(Value::Text("kept".into())), Some(Value::Nat64(7))],
        ),
    );
    let resource = DiagnosticExecutionBudgetResource::TemporaryBytes;
    let budget = HardExecutionBudget::uniform_for_tests(
        16_000_000,
        HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
    )
    .with_limit_for_tests(resource, 0);
    let context = HardExecutionContext::new(
        DiagnosticExecutionBudgetScope::Execution,
        DiagnosticExecutionLane::TrustedRead,
        0,
    );
    with_execution_budget_for_tests(
        budget,
        context,
        || {
            let Err(error) = ProjectionDataRows::from_kernel_rows(vec![kernel], Some(&order))
            else {
                panic!("wider tuple storage must be admitted before allocation");
            };
            assert!(
                error
                    .diagnostic_facts()
                    .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
            );
            assert!(current_execution_budget_usage()?.observed(resource) > 0);
            Ok(())
        },
        std::convert::identity,
    )
    .unwrap();
}

#[test]
fn missing_order_expression_results_keep_projection_error_ownership() {
    use crate::db::query::plan::expr::BinaryOp;
    let (layout, _) = layout_and_schema(None);
    let expr = CompiledExpr::Binary {
        op: BinaryOp::Add,
        left: Box::new(CompiledExpr::Slot { slot: 2 }),
        right: Box::new(CompiledExpr::Literal(Value::Nat64(1))),
    };
    let order = ResolvedOrder::new(vec![ResolvedOrderField::new(
        ResolvedOrderValueSource::expression(expr.clone()),
        OrderDirection::Asc,
    )]);
    let mut window = DataRowOrderWindow::new(layout.clone(), &order, Some(1));
    window
        .push(row(&layout, 1, true, Some("text")), None)
        .unwrap();
    let rows = window.into_sorted_rows().unwrap();
    assert_eq!(
        rows.cursor_boundary_at(0, &layout, &order)
            .unwrap()
            .unwrap()
            .slots,
        vec![CursorBoundarySlot::Missing]
    );
    let (_, Some(cached)) = rows.into_rows().next().unwrap().into_parts() else {
        panic!("expression order expected")
    };
    assert_eq!(cached.projection_value(&expr), None);
}
