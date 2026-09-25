//! Prefix candidates share admitted bounds without changing selection semantics.

use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        access::AccessPath,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        index::{TextPrefixBoundMode, starts_with_component_bounds},
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        query::{
            plan::{
                VisibleIndexes,
                planner::{PlannerError, plan_access_selection_with_order_and_semantic_indexes},
            },
            preparation::PreparationWork,
        },
        schema::{
            AcceptedCompositeCatalog, AcceptedFieldKind, AcceptedSchemaRevision,
            AcceptedSchemaSnapshot, AcceptedValueCatalogHandle, FieldId, FieldStorageDecode,
            PersistedFieldSnapshot, PersistedIndexExpressionOp, PersistedIndexExpressionSnapshot,
            PersistedIndexFieldPathSnapshot, PersistedIndexKeyItemSnapshot,
            PersistedIndexKeySnapshot, PersistedIndexSnapshot, PersistedSchemaSnapshot,
            SchemaFieldSlot, SchemaIndexId, SchemaInfo, SchemaInsertDefault, SchemaRowLayout,
            SchemaVersion, empty_accepted_enum_catalog_for_tests,
        },
    },
    value::{Value, lower_text_construction_allowance},
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

pub(in crate::db::query::plan) fn schema() -> SchemaInfo {
    let text = AcceptedFieldKind::Text { max_len: None };
    let fields: Vec<_> = [("id", AcceptedFieldKind::Nat64), ("name", text.clone())]
        .into_iter()
        .enumerate()
        .map(|(slot, (name, kind))| {
            let slot = u16::try_from(slot).unwrap();
            let decode = FieldStorageDecode::ByKind;
            let codec = kind.leaf_codec_for_storage(decode);
            PersistedFieldSnapshot::new_initial(
                FieldId::new(u32::from(slot) + 1),
                name.into(),
                SchemaFieldSlot::new(slot),
                kind,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                decode,
                codec,
            )
        })
        .collect();
    let indexes = ["z_raw", "a_raw", "z_lower", "a_lower"]
        .into_iter()
        .enumerate()
        .map(|(slot, name)| {
            let slot = u16::try_from(slot).unwrap();
            let source = PersistedIndexFieldPathSnapshot::new(
                FieldId::new(2),
                SchemaFieldSlot::new(1),
                vec!["name".into()],
                text.clone(),
                false,
            );
            let key = if name.ends_with("lower") {
                PersistedIndexKeySnapshot::Items(vec![PersistedIndexKeyItemSnapshot::Expression(
                    Box::new(PersistedIndexExpressionSnapshot::new(
                        PersistedIndexExpressionOp::Lower,
                        source,
                        text.clone(),
                        text.clone(),
                        "expr:v1:LOWER(name)".into(),
                    )),
                )])
            } else {
                PersistedIndexKeySnapshot::FieldPath(vec![source])
            };
            PersistedIndexSnapshot::new(
                SchemaIndexId::new(u32::from(slot) + 1).unwrap(),
                slot + 1,
                name.into(),
                "prefix_tests".into(),
                false,
                key,
                None,
            )
        })
        .collect();
    let layout = SchemaRowLayout::initial(
        fields
            .iter()
            .map(|field| (field.id(), field.slot()))
            .collect(),
    );
    let snapshot = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new_with_indexes(
        SchemaVersion::initial(),
        "prefix_tests::Entity".into(),
        "Entity".into(),
        FieldId::new(1),
        layout,
        fields,
        indexes,
    ));
    let catalog = AcceptedValueCatalogHandle::new_for_tests(
        empty_accepted_enum_catalog_for_tests(),
        AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    );
    SchemaInfo::from_accepted_snapshot_and_catalog(&snapshot, catalog)
}

fn request(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "Keep bound parity and cumulative admission in one read-lane matrix."
)]
fn prefix_selection_shares_bounds_and_propagates_cumulative_exhaustion() {
    let schema = schema();
    let visible =
        VisibleIndexes::accepted_schema_visible(&schema).expect("valid accepted index fixture");
    let indexes = visible.accepted_semantic_index_contracts();
    // Every candidate checks its non-optional key slots for stream completeness,
    // including candidates later rejected by the lookup coercion gate.
    let completeness_steps: u64 = indexes.iter().map(|index| index.key_arity() as u64).sum();
    for prefix in [
        "abc",
        "İΣ",
        "a\0b",
        "\u{7f}",
        "é\u{10ffff}",
        "\u{10ffff}",
        &"İΣ".repeat(1024),
    ] {
        for (coercion, mode, winner) in [
            (CoercionId::Strict, TextPrefixBoundMode::Strict, "a_raw"),
            (
                CoercionId::TextCasefold,
                TextPrefixBoundMode::LowerOnly,
                "a_lower",
            ),
        ] {
            let predicate = Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::StartsWith,
                Value::Text(prefix.into()),
                coercion,
            ));
            let before = predicate.clone();
            let (lowered, copy_bytes, copy_steps, visits) = if coercion == CoercionId::Strict {
                (
                    prefix.to_string(),
                    prefix.len() as u64,
                    prefix.len() as u64,
                    1,
                )
            } else {
                let (bytes, steps) = lower_text_construction_allowance(prefix.len());
                (prefix.to_lowercase(), bytes, steps, 0)
            };
            let (lower, upper) = starts_with_component_bounds(&lowered, mode).unwrap();
            let len = lowered.len() as u64;
            let (bound_bytes, bound_steps) = if mode == TextPrefixBoundMode::Strict {
                (2 * len + 1, 3 * len + 1)
            } else {
                (len, len)
            };
            let bytes = (std::mem::size_of_val(indexes)
                + size_of::<usize>()
                + size_of::<AccessPath<Value>>()) as u64
                + copy_bytes
                + bound_bytes;
            let steps =
                1 + 2 * indexes.len() as u64 + completeness_steps + copy_steps + bound_steps;
            for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
                for (resource, exact) in [
                    (Resource::TemporaryBytes, bytes),
                    (Resource::PredicateExpressionSteps, steps),
                    (Resource::NestedValueSteps, visits),
                ] {
                    for limit in [0, exact.saturating_sub(1), exact * 2] {
                        let root = request(resource, limit);
                        PreparationWork::run(&root.scope(), lane, |work| {
                            for attempt in 0..3 {
                                let result = plan_access_selection_with_order_and_semantic_indexes(
                                    indexes,
                                    &schema,
                                    Some(&predicate),
                                    None,
                                    false,
                                    work,
                                );
                                if exact == 0 || limit >= exact && attempt < 2 {
                                    let (access, _) =
                                        result.unwrap().into_access_and_non_index_reason();
                                    let spec = access.as_index_range_path().unwrap();
                                    assert_eq!(spec.index_ref().name(), winner);
                                    assert_eq!(spec.lower(), &lower);
                                    assert_eq!(spec.upper(), &upper);
                                    assert_eq!(spec.field_slots(), &[0]);
                                    assert!(spec.prefix_values().is_empty());
                                    assert_eq!(root.observed(resource), exact * (attempt + 1));
                                } else {
                                    let PlannerError::Internal(error) = result.unwrap_err() else {
                                        panic!("exhaustion must remain typed")
                                    };
                                    assert!(
                                        QueryError::execute(*error).diagnostic_facts().contains(&(
                                            DiagnosticFactTag::BudgetResource,
                                            resource.raw()
                                        ))
                                    );
                                    break;
                                }
                            }
                            Ok(())
                        })
                        .unwrap();
                        assert_eq!(predicate, before);
                        assert_eq!(root.observed(Resource::RowsVisited), 0);
                    }
                }
            }
        }
    }
}

#[test]
fn unsupported_prefixes_do_not_construct_operands() {
    let schema = schema();
    let visible =
        VisibleIndexes::accepted_schema_visible(&schema).expect("valid accepted index fixture");
    let indexes = visible.accepted_semantic_index_contracts();
    for (field, value, coercion) in [
        ("name", Value::Text(String::new()), CoercionId::Strict),
        ("name", Value::Text(String::new()), CoercionId::TextCasefold),
        ("name", Value::Nat64(1), CoercionId::Strict),
        ("absent", Value::Text("abc".into()), CoercionId::Strict),
        ("name", Value::Text("abc".into()), CoercionId::NumericWiden),
    ] {
        let predicate = Predicate::Compare(ComparePredicate::with_coercion(
            field,
            CompareOp::StartsWith,
            value,
            coercion,
        ));
        let root = request(
            Resource::TemporaryBytes,
            std::mem::size_of_val(indexes) as u64,
        );
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            let (access, _) = plan_access_selection_with_order_and_semantic_indexes(
                indexes,
                &schema,
                Some(&predicate),
                None,
                false,
                work,
            )
            .unwrap()
            .into_access_and_non_index_reason();
            assert!(access.is_single_full_scan());
            Ok(())
        })
        .unwrap();
        assert_eq!(root.observed(Resource::NestedValueSteps), 0);
    }
}

#[test]
fn and_prefix_ranges_merge_unicode_bounds_with_cumulative_admission() {
    use crate::db::query::plan::planner::range::index_range_from_and;
    use std::ops::Bound;

    let schema = schema();
    let visible =
        VisibleIndexes::accepted_schema_visible(&schema).expect("valid accepted index fixture");
    let indexes = visible.accepted_semantic_index_contracts();
    for coercion in [CoercionId::Strict, CoercionId::TextCasefold] {
        let children: Vec<_> = [
            (CompareOp::Gte, "İ"),
            (CompareOp::Lt, "\u{10ffff}"),
            (CompareOp::StartsWith, "İ"),
        ]
        .into_iter()
        .map(|(op, text)| {
            Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                op,
                Value::Text(text.into()),
                coercion,
            ))
        })
        .collect();
        let baseline = request(Resource::TemporaryBytes, 16_000_000);
        let expected = PreparationWork::run(&baseline.scope(), Lane::Diagnostic, |work| {
            Ok(
                index_range_from_and(indexes, &schema, &children, None, false, work)
                    .unwrap()
                    .unwrap(),
            )
        })
        .unwrap();
        let (name, lower, upper, copies, comparisons_per_index) = if coercion == CoercionId::Strict
        {
            ("a_raw", "İ", "ı", 4, 4)
        } else {
            ("a_lower", "i\u{307}", "\u{10ffff}", 0, 3)
        };
        assert_eq!(expected.index_ref().name(), name);
        assert_eq!(
            expected.lower(),
            &Bound::Included(Value::Text(lower.into()))
        );
        assert_eq!(
            expected.upper(),
            &Bound::Excluded(Value::Text(upper.into()))
        );
        // Two eligible indexes each merge the same bounds; each comparison
        // charges the admission walk and the scalar operand visit.
        assert_eq!(
            baseline.observed(Resource::NestedValueSteps),
            copies + 2 * comparisons_per_index * 2
        );
        for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
            for resource in [
                Resource::TemporaryBytes,
                Resource::PredicateExpressionSteps,
                Resource::NestedValueSteps,
            ] {
                let exact = baseline.observed(resource);
                for limit in [0, exact.saturating_sub(1), exact * 2] {
                    let root = request(resource, limit);
                    PreparationWork::run(&root.scope(), lane, |work| {
                        for attempt in 1..=3 {
                            let result = index_range_from_and(
                                indexes, &schema, &children, None, false, work,
                            );
                            if exact == 0 || attempt * exact <= limit {
                                assert_eq!(result.unwrap().unwrap(), expected);
                                assert_eq!(root.observed(resource), attempt * exact);
                            } else {
                                assert!(
                                    crate::db::QueryError::execute(result.unwrap_err())
                                        .diagnostic_facts()
                                        .contains(&(
                                            DiagnosticFactTag::BudgetResource,
                                            resource.raw()
                                        ))
                                );
                                break;
                            }
                        }
                        Ok(())
                    })
                    .unwrap();
                    assert_eq!(root.observed(Resource::RowsVisited), 0);
                }
            }
        }
    }
}

#[test]
fn and_primary_range_copies_valid_interval_endpoints() {
    use crate::db::query::plan::planner::range::primary_key_range_from_and;

    let schema = schema();
    let children = [
        Predicate::gte("id".into(), Value::Nat64(2)),
        Predicate::lt("id".into(), Value::Nat64(5)),
    ];
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        for (resource, exact) in [
            (
                Resource::TemporaryBytes,
                size_of::<AccessPath<Value>>() as u64,
            ),
            (Resource::PredicateExpressionSteps, 2),
            (Resource::NestedValueSteps, 4),
        ] {
            for limit in [0, exact - 1, exact * 2] {
                let root = request(resource, limit);
                PreparationWork::run(&root.scope(), lane, |work| {
                    for attempt in 1..=3 {
                        let result = primary_key_range_from_and(&schema, &children, work);
                        if attempt * exact <= limit {
                            assert_eq!(
                                result.unwrap().unwrap(),
                                crate::db::access::AccessPlan::key_range(
                                    Value::Nat64(2),
                                    Value::Nat64(5)
                                )
                            );
                            assert_eq!(root.observed(resource), attempt * exact);
                        } else {
                            assert!(
                                crate::db::QueryError::execute(result.unwrap_err())
                                    .diagnostic_facts()
                                    .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                            );
                            break;
                        }
                    }
                    Ok(())
                })
                .unwrap();
                assert_eq!(root.observed(Resource::RowsVisited), 0);
            }
        }
    }
    for (invalid, compared_nodes) in [
        (
            vec![
                children[0].clone(),
                children[0].clone(),
                children[1].clone(),
            ],
            0,
        ),
        (
            vec![
                children[0].clone(),
                Predicate::lt("id".into(), Value::Nat64(2)),
            ],
            2,
        ),
        (
            vec![
                children[0].clone(),
                Predicate::lt("id".into(), Value::Text("wrong type".into())),
            ],
            0,
        ),
    ] {
        let root = request(Resource::TemporaryBytes, 0);
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            assert!(
                primary_key_range_from_and(&schema, &invalid, work)
                    .unwrap()
                    .is_none()
            );
            Ok(())
        })
        .unwrap();
        assert_eq!(root.observed(Resource::NestedValueSteps), compared_nodes);
    }
}

#[test]
fn scalar_primary_key_one_sided_comparisons_select_bounded_access() {
    use crate::db::{
        access::AccessPlan,
        query::plan::planner::{compare::plan_compare, range::primary_key_range_from_and},
    };

    let schema = schema();
    let visible = VisibleIndexes::accepted_schema_visible(&schema).unwrap();
    let indexes = visible.accepted_semantic_index_contracts();
    for (op, expected) in [
        (
            CompareOp::Gt,
            AccessPlan::key_range_bounds(Some(Value::Nat64(40)), None),
        ),
        (
            CompareOp::Gte,
            AccessPlan::key_range_bounds(Some(Value::Nat64(40)), None),
        ),
        (
            CompareOp::Lt,
            AccessPlan::key_range_bounds(None, Some(Value::Nat64(40))),
        ),
        (
            CompareOp::Lte,
            AccessPlan::key_range_bounds(None, Some(Value::Nat64(40))),
        ),
    ] {
        let cmp = ComparePredicate::with_coercion("id", op, Value::Nat64(40), CoercionId::Strict);
        let root = request(Resource::TemporaryBytes, 16_000_000);
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            assert_eq!(
                plan_compare(indexes, &schema, &cmp, None, false, work).unwrap(),
                expected
            );
            assert_eq!(
                primary_key_range_from_and(&schema, &[Predicate::Compare(cmp.clone())], work)
                    .unwrap(),
                Some(expected.clone())
            );
            Ok(())
        })
        .unwrap();
    }
}
