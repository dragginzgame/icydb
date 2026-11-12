use super::{
    CollectionOperator, CompareOperator, FieldCompareOperator, FilterExpr, InputValue,
    JunctionOperator, SetOperator, StateOperator, field_compare_expr, membership_expr,
};
use crate::{
    db::{
        predicate::PredicateProgram,
        query::plan::expr::{
            BinaryOp, Expr, Function, UnaryOp, derive_normalized_bool_expr_predicate_subset,
            eval_builder_expr_for_value_preview, normalize_bool_expr,
        },
        schema::{
            AcceptedCompositeCatalog, AcceptedFieldKind, AcceptedSchemaRevision,
            AcceptedSchemaSnapshot, AcceptedValueCatalogHandle, FieldId, FieldStorageDecode,
            LeafCodec, PersistedFieldSnapshot, PersistedSchemaSnapshot, ScalarCodec,
            SchemaFieldSlot, SchemaInfo, SchemaInsertDefault, SchemaRowLayout, SchemaVersion,
            empty_accepted_enum_catalog_for_tests,
        },
    },
    types::{Date, Duration, Subaccount, Timestamp},
    value::{PublicValue, Value},
};

fn lower_filter(filter: &FilterExpr, schema: &SchemaInfo) -> Expr {
    let root = crate::db::RequestExecutionRoot::__new_runtime_root();
    crate::db::query::preparation::PreparationWork::run(
        &root.scope(),
        icydb_diagnostic_code::DiagnosticExecutionLane::PublicRead,
        |work| filter.lower_bool_expr_for_schema(schema, work),
    )
    .expect("fixture lowering fits the production request budget")
}

#[test]
fn nested_filter_value_lowering_charges_before_descent_and_retains_failed_work() {
    use crate::db::{
        RequestExecutionRoot,
        query::preparation::PreparationWork,
        resource::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    };
    use icydb_diagnostic_code::{
        DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane,
    };

    let schema = membership_schema();
    let filter = FilterExpr::in_list(
        "id",
        [
            InputValue::text("1".to_string()),
            InputValue::list(vec![InputValue::boolean(true)]),
        ],
    );
    let root = RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::NestedValueSteps, 2),
    );
    for expected in [3, 4] {
        let error =
            PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
                filter.lower_bool_expr_for_schema(&schema, work)
            })
            .expect_err("reject the child visit before materializing it");
        assert_eq!(root.observed(Resource::NestedValueSteps), expected);
        assert!(error.diagnostic_facts().contains(&(
            icydb_diagnostic_code::DiagnosticFactTag::BudgetResource,
            Resource::NestedValueSteps.raw()
        )));
    }
}

fn filter_construction_cases() -> Vec<(FilterExpr, u64)> {
    let slot = size_of::<Expr>() as u64;
    // NULL avoids variable raw-literal storage here. This tests construction;
    // operation/type admissibility still belongs to downstream validation.
    let mut cases = vec![
        (FilterExpr::Constant(true), 0),
        (FilterExpr::and(vec![]), 0),
        (FilterExpr::or(vec![]), 0),
        (FilterExpr::not(FilterExpr::Constant(true)), slot),
        (
            FilterExpr::and(vec![
                FilterExpr::Constant(true),
                FilterExpr::Constant(false),
            ]),
            2 * slot,
        ),
        (
            FilterExpr::or(vec![
                FilterExpr::Constant(true),
                FilterExpr::Constant(false),
            ]),
            2 * slot,
        ),
        (FilterExpr::in_list("id", Vec::<InputValue>::new()), 0),
        (FilterExpr::not_in("id", Vec::<InputValue>::new()), 0),
        (FilterExpr::eq("id", InputValue::null()), 2 + 2 * slot),
        (FilterExpr::eq_ci("id", InputValue::null()), 2 + 3 * slot),
        (FilterExpr::eq_field("id", "id"), 4 + 2 * slot),
        (
            FilterExpr::in_list("id", [InputValue::null()]),
            2 + size_of::<Value>() as u64 + 2 * slot,
        ),
        (
            FilterExpr::not_in("id", [InputValue::null()]),
            2 + size_of::<Value>() as u64 + 3 * slot,
        ),
    ];
    for (operator, slots) in [
        (CollectionOperator::Contains, 2),
        (CollectionOperator::TextContains, 2),
        (CollectionOperator::TextContainsCi, 3),
        (CollectionOperator::StartsWith, 2),
        (CollectionOperator::StartsWithCi, 3),
        (CollectionOperator::EndsWith, 2),
        (CollectionOperator::EndsWithCi, 3),
    ] {
        cases.push((
            FilterExpr::Collection {
                operator,
                field: "id".into(),
                value: InputValue::null(),
            },
            2 + slots * slot,
        ));
    }
    for operator in [
        StateOperator::IsNull,
        StateOperator::IsNotNull,
        StateOperator::IsMissing,
        StateOperator::IsEmpty,
        StateOperator::IsNotEmpty,
    ] {
        cases.push((
            FilterExpr::State {
                operator,
                field: "id".into(),
            },
            2 + slot,
        ));
    }
    cases
}

#[test]
fn typed_filter_constructors_charge_names_and_introduced_storage() {
    use crate::db::{
        RequestExecutionRoot,
        query::preparation::PreparationWork,
        resource::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    };
    use icydb_diagnostic_code::{
        DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane, DiagnosticFactTag,
    };

    let schema = membership_schema();
    for (filter, expected) in filter_construction_cases() {
        for reject in [false, true] {
            if reject && expected == 0 {
                continue;
            }
            let limit = expected - u64::from(reject);
            let root = RequestExecutionRoot::new_for_tests(
                HardExecutionBudget::uniform_for_tests(
                    16_000_000,
                    HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                )
                .with_limit_for_tests(Resource::TemporaryBytes, limit),
            );
            let result =
                PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
                    filter.lower_bool_expr_for_schema(&schema, work)
                });
            if reject {
                let error = result.expect_err("one byte short rejects construction");
                assert!(error.diagnostic_facts().contains(&(
                    DiagnosticFactTag::BudgetResource,
                    Resource::TemporaryBytes.raw()
                )));
            } else {
                result.expect("exact construction allowance fits");
            }
            assert_eq!(
                root.observed(Resource::TemporaryBytes),
                expected,
                "{filter:?}"
            );
        }
    }
}

fn membership_schema() -> SchemaInfo {
    let id = FieldId::new(1);
    let slot = SchemaFieldSlot::new(0);
    let snapshot = AcceptedSchemaSnapshot::try_new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "filter::tests::Entity".to_string(),
        "Entity".to_string(),
        id,
        SchemaRowLayout::initial(vec![(id, slot)]),
        vec![PersistedFieldSnapshot::new_initial(
            id,
            "id".to_string(),
            slot,
            AcceptedFieldKind::Int32,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Int32),
        )],
    ))
    .expect("test schema should be accepted");
    let catalog = AcceptedValueCatalogHandle::new_for_tests(
        empty_accepted_enum_catalog_for_tests(),
        AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    );

    SchemaInfo::from_accepted_snapshot_and_catalog(&snapshot, catalog)
}

// Inspect junction depth and leaf order without imposing a recursive walk
// on the test itself. Comparisons are leaves for this measurement.
fn bool_chain_leaves(expr: &Expr, join: BinaryOp) -> (usize, Vec<&Expr>) {
    let mut pending = vec![(expr, 0)];
    let mut leaves = Vec::new();
    let mut depth = 0;
    while let Some((expr, level)) = pending.pop() {
        depth = depth.max(level);
        if let Expr::Binary { op, left, right } = expr
            && *op == join
        {
            pending.push((right, level + 1));
            pending.push((left, level + 1));
        } else {
            leaves.push(expr);
        }
    }

    (depth, leaves)
}

#[test]
fn flat_junctions_lower_with_logarithmic_depth_in_source_order() {
    crate::db::query::preparation::with_preparation_work(|work| {
        let schema = membership_schema();
        for count in [0_usize, 1, 2, 3, 127, 128, 129, 1024, 4096] {
            let values = (0..count)
                .map(|value| InputValue::text(value.to_string()))
                .collect::<Vec<_>>();
            for negated in [false, true] {
                let compare = if negated { BinaryOp::Ne } else { BinaryOp::Eq };
                let join = if negated { BinaryOp::And } else { BinaryOp::Or };
                let filters = values
                    .iter()
                    .map(|value| FilterExpr::Compare {
                        operator: if negated {
                            CompareOperator::Ne
                        } else {
                            CompareOperator::Eq
                        },
                        field: "id".to_string(),
                        value: value.clone(),
                    })
                    .collect();
                let junction = FilterExpr::Junction {
                    operator: if negated {
                        JunctionOperator::And
                    } else {
                        JunctionOperator::Or
                    },
                    filters,
                };
                let junction = lower_filter(&junction, &schema);
                if count == 0 {
                    assert_eq!(junction, Expr::Literal(Value::Bool(negated)));
                    continue;
                }

                let (depth, leaves) = bool_chain_leaves(&junction, join);
                assert_eq!(depth, count.next_power_of_two().ilog2() as usize);
                assert_eq!(leaves.len(), count);
                for (index, leaf) in leaves.into_iter().enumerate() {
                    let value = i64::try_from(index).expect("test integer fits");
                    assert_eq!(
                        *leaf,
                        field_compare_expr(compare, "id", Value::Int64(value), work)
                            .expect("bounded fixture comparison")
                    );
                }
                // Canonical identity must not depend on associative grouping.
                // Keep this control below the separate wide-lowering stress sizes.
                if count <= 129 {
                    let associated = bool_chain_leaves(&junction, join)
                        .1
                        .into_iter()
                        .cloned()
                        .reduce(|left, right| Expr::Binary {
                            op: join,
                            left: Box::new(left),
                            right: Box::new(right),
                        })
                        .expect("nonempty fixture");
                    assert_eq!(
                        normalize_bool_expr(junction, work).expect("canonical preparation"),
                        normalize_bool_expr(associated, work).expect("canonical preparation")
                    );
                }
            }
        }
    });
}

#[test]
fn compact_membership_preserves_admitted_values_without_comparison_expansion() {
    crate::db::query::preparation::with_preparation_work(|work| {
        let schema = membership_schema();
        let values = vec![
            InputValue::text("3".to_string()),
            InputValue::null(),
            InputValue::text("1".to_string()),
            InputValue::text("3".to_string()),
        ];
        for negated in [false, true] {
            let lowered = FilterExpr::Set {
                operator: if negated {
                    SetOperator::NotIn
                } else {
                    SetOperator::In
                },
                field: "id".to_string(),
                values: values.clone(),
            };
            let lowered = lower_filter(&lowered, &schema);
            let expected = vec![
                Value::Int64(3),
                Value::Null,
                Value::Int64(1),
                Value::Int64(3),
            ];
            assert_eq!(
                lowered,
                membership_expr("id", expected, negated, work).expect("bounded fixture membership")
            );
            assert_eq!(
                normalize_bool_expr(lowered.clone(), work).expect("canonical preparation"),
                lowered
            );
        }
    });
}

#[test]
fn compact_membership_moves_the_list_and_keeps_constant_depth() {
    crate::db::query::preparation::with_preparation_work(|work| {
        for count in [1, 2, 128, 4096] {
            for negated in [false, true] {
                let values = (0..count).map(Value::Int64).collect::<Vec<_>>();
                let allocation = values.as_ptr();
                let expr = membership_expr("id", values, negated, work)
                    .expect("bounded fixture membership");
                let membership = if negated {
                    let Expr::Unary {
                        op: UnaryOp::Not,
                        expr,
                    } = &expr
                    else {
                        panic!("negated membership wraps one compact operation");
                    };
                    expr.as_ref()
                } else {
                    &expr
                };
                let Expr::FunctionCall {
                    function: Function::InList,
                    args,
                } = membership
                else {
                    panic!("membership remains compact at every width");
                };
                let [Expr::Field(field), Expr::Literal(Value::List(values))] = args.as_slice()
                else {
                    panic!("membership owns a target and a value list");
                };
                assert_eq!(field.as_str(), "id");
                assert_eq!(values.as_ptr(), allocation);
                assert_eq!(
                    values.len(),
                    usize::try_from(count).expect("fixture count fits")
                );
            }
        }
    });
}

#[test]
fn compact_membership_matches_explicit_comparisons_and_empty_identities() {
    crate::db::query::preparation::with_preparation_work(|work| {
        let fixtures = [
            vec![],
            vec![Value::Null],
            vec![Value::Int64(1)],
            vec![Value::Int64(1), Value::Null, Value::Int64(1)],
            vec![Value::Int64(2), Value::Int64(3)],
            vec![Value::Int64(1), Value::Nat64(1)],
            vec![Value::Text("one".into()), Value::Text("two".into())],
            vec![Value::Bool(true), Value::Bool(false)],
            vec![Value::Blob(vec![1]), Value::Blob(vec![2])],
            vec![Value::List(vec![Value::Int64(1)])],
            vec![Value::Unit],
        ];
        let targets = [
            Value::Null,
            Value::Int64(1),
            Value::Int64(4),
            Value::Nat64(1),
            Value::Text("one".into()),
            Value::Bool(false),
            Value::Blob(vec![1]),
            Value::List(vec![Value::Int64(1)]),
            Value::Unit,
        ];
        for values in fixtures {
            for negated in [false, true] {
                let compare = if negated { BinaryOp::Ne } else { BinaryOp::Eq };
                let join = if negated { BinaryOp::And } else { BinaryOp::Or };
                let explicit = values
                    .iter()
                    .cloned()
                    .map(|value| {
                        field_compare_expr(compare, "id", value, work)
                            .expect("bounded fixture comparison")
                    })
                    .reduce(|left, right| Expr::Binary {
                        op: join,
                        left: Box::new(left),
                        right: Box::new(right),
                    })
                    .unwrap_or(Expr::Literal(Value::Bool(negated)));
                let compact = membership_expr("id", values.clone(), negated, work)
                    .expect("bounded fixture membership");
                if values.is_empty() {
                    assert_eq!(compact, Expr::Literal(Value::Bool(negated)));
                }
                for target in &targets {
                    let actual = eval_builder_expr_for_value_preview(&compact, "id", target);
                    let expected = eval_builder_expr_for_value_preview(&explicit, "id", target);
                    match (actual, expected) {
                        (Ok(actual), Ok(expected)) => assert_eq!(actual, expected),
                        (Err(_), Err(_)) => {}
                        other => panic!("membership evaluation parity failed: {other:?}"),
                    }
                }
                let compact_predicate = derive_normalized_bool_expr_predicate_subset(
                    &normalize_bool_expr(compact, work).expect("canonical preparation"),
                    work,
                )
                .expect("fixture predicate construction fits");
                let explicit_predicate = derive_normalized_bool_expr_predicate_subset(
                    &normalize_bool_expr(explicit, work).expect("canonical preparation"),
                    work,
                )
                .expect("fixture predicate construction fits");
                assert_eq!(compact_predicate.is_some(), explicit_predicate.is_some());
                if let (Some(compact), Some(explicit)) = (compact_predicate, explicit_predicate) {
                    let schema = membership_schema();
                    let compact = PredicateProgram::compile_with_schema_info(&schema, &compact);
                    let explicit = PredicateProgram::compile_with_schema_info(&schema, &explicit);
                    for target in &targets {
                        assert_eq!(
                            compact.eval_with_slot_value_cow_reader(&mut |_| Some(
                                std::borrow::Cow::Borrowed(target)
                            )),
                            explicit.eval_with_slot_value_cow_reader(&mut |_| Some(
                                std::borrow::Cow::Borrowed(target)
                            )),
                        );
                    }
                }
            }
        }
    });
}

#[test]
fn typed_filter_atoms_preserve_public_value_kinds() {
    assert_eq!(
        InputValue::from(vec![0x00, 0x0a, 0xff]),
        InputValue::from_public(PublicValue::Blob(vec![0x00, 0x0a, 0xff])),
    );
    assert_eq!(
        InputValue::from(Date::try_new(2026, 8, 5).expect("test date should be valid")),
        InputValue::from_public(PublicValue::Date(Date::try_new(2026, 8, 5).unwrap())),
    );
    assert_eq!(
        InputValue::from(Duration::from_millis(12_345)),
        InputValue::from_public(PublicValue::Duration(Duration::from_millis(12_345))),
    );
    assert_eq!(
        InputValue::from(Subaccount::from_array([0xab; 32])),
        InputValue::from_public(PublicValue::Subaccount(Subaccount::from_array([0xab; 32]))),
    );
    assert_eq!(
        InputValue::from(Timestamp::from_millis(-42)),
        InputValue::from_public(PublicValue::Timestamp(Timestamp::from_millis(-42))),
    );
}

#[test]
fn grouped_filter_wire_round_trips_every_supported_operation() {
    let value = InputValue::text("value".to_string());
    let comparisons = [
        CompareOperator::Eq,
        CompareOperator::EqCi,
        CompareOperator::Ne,
        CompareOperator::Lt,
        CompareOperator::Lte,
        CompareOperator::Gt,
        CompareOperator::Gte,
    ]
    .map(|operator| FilterExpr::Compare {
        operator,
        field: "left".to_string(),
        value: value.clone(),
    });
    let field_comparisons = [
        FieldCompareOperator::Eq,
        FieldCompareOperator::Ne,
        FieldCompareOperator::Lt,
        FieldCompareOperator::Lte,
        FieldCompareOperator::Gt,
        FieldCompareOperator::Gte,
    ]
    .map(|operator| FilterExpr::CompareFields {
        operator,
        left_field: "left".to_string(),
        right_field: "right".to_string(),
    });
    let sets = [SetOperator::In, SetOperator::NotIn].map(|operator| FilterExpr::Set {
        operator,
        field: "set".to_string(),
        values: vec![value.clone()],
    });
    let collections = [
        CollectionOperator::Contains,
        CollectionOperator::TextContains,
        CollectionOperator::TextContainsCi,
        CollectionOperator::StartsWith,
        CollectionOperator::StartsWithCi,
        CollectionOperator::EndsWith,
        CollectionOperator::EndsWithCi,
    ]
    .map(|operator| FilterExpr::Collection {
        operator,
        field: "collection".to_string(),
        value: value.clone(),
    });
    let states = [
        StateOperator::IsNull,
        StateOperator::IsNotNull,
        StateOperator::IsMissing,
        StateOperator::IsEmpty,
        StateOperator::IsNotEmpty,
    ]
    .map(|operator| FilterExpr::State {
        operator,
        field: "state".to_string(),
    });
    let mut filters = vec![
        FilterExpr::Constant(true),
        FilterExpr::Constant(false),
        FilterExpr::Junction {
            operator: JunctionOperator::And,
            filters: Vec::new(),
        },
        FilterExpr::Junction {
            operator: JunctionOperator::Or,
            filters: Vec::new(),
        },
        FilterExpr::Not(Box::new(FilterExpr::Constant(true))),
    ];
    filters.extend(comparisons);
    filters.extend(field_comparisons);
    filters.extend(sets);
    filters.extend(collections);
    filters.extend(states);
    filters.push(FilterExpr::not(FilterExpr::and(vec![
        FilterExpr::eq("collection_id", "collection"),
        FilterExpr::eq("stage", "Draft"),
    ])));

    for filter in filters {
        let encoded = icydb_schema::wire::encode_one(&filter).expect("filter should encode");
        let decoded =
            icydb_schema::wire::decode_one::<FilterExpr>(&encoded).expect("filter should decode");
        assert_eq!(decoded, filter);

        let optional = Some(filter);
        let encoded =
            icydb_schema::wire::encode_one(&optional).expect("optional filter should encode");
        let decoded = icydb_schema::wire::decode_one::<Option<FilterExpr>>(&encoded)
            .expect("supplied filter should decode");
        assert_eq!(decoded, optional);
    }
}

#[test]
fn optional_filter_wire_preserves_explicit_absence() {
    let encoded = icydb_schema::wire::encode_one(Option::<FilterExpr>::None)
        .expect("absent filter should encode");
    assert_eq!(
        icydb_schema::wire::decode_one::<Option<FilterExpr>>(&encoded)
            .expect("absent filter should decode"),
        None,
    );
}

#[test]
fn recursive_filter_wire_refuses_depth_before_materializing_the_input() {
    // The native format carries Not as tag 2 and Constant(true) as [0, 1].
    // Build hostile bytes directly so encoder admission cannot hide a decode gap.
    let shallow = FilterExpr::Not(Box::new(FilterExpr::Constant(true)));
    assert_eq!(icydb_schema::wire::encode_one(&shallow).unwrap(), [2, 0, 1]);
    let mut nested = vec![2; icydb_schema::wire::MAX_DEPTH + 1];
    nested.extend_from_slice(&[0, 1]);
    assert!(matches!(
        icydb_schema::wire::decode_one::<FilterExpr>(&nested),
        Err(icydb_schema::wire::Error::Limit),
    ));
    nested.insert(0, 1); // Some must not recover the rejected predicate as None.
    assert!(matches!(
        icydb_schema::wire::decode_one::<Option<FilterExpr>>(&nested),
        Err(icydb_schema::wire::Error::Limit),
    ));

    assert_eq!(
        icydb_schema::wire::encode_one(InputValue::list(vec![InputValue::null()])).unwrap(),
        [11, 1, 13],
    );
    let mut nested = [11, 1].repeat(icydb_schema::wire::MAX_DEPTH + 1);
    nested.push(13);
    assert!(matches!(
        icydb_schema::wire::decode_one::<InputValue>(&nested),
        Err(icydb_schema::wire::Error::Limit),
    ));

    assert_eq!(
        icydb_schema::wire::encode_one(PublicValue::List(vec![PublicValue::Null])).unwrap(),
        [11, 1, 13],
    );
    let mut nested = [11, 1].repeat(icydb_schema::wire::MAX_DEPTH + 1);
    nested.push(13);
    assert!(matches!(
        icydb_schema::wire::decode_one::<PublicValue>(&nested),
        Err(icydb_schema::wire::Error::Limit),
    ));

    let mut admitted = FilterExpr::Constant(true);
    for _ in 0..64 {
        admitted = FilterExpr::Not(Box::new(admitted));
    }
    let bytes = icydb_schema::wire::encode_one(&admitted).unwrap();
    assert_eq!(
        icydb_schema::wire::decode_one::<FilterExpr>(&bytes).unwrap(),
        admitted,
    );
}

#[test]
fn optional_filter_wire_rejects_unknown_expression_family() {
    #[derive(serde::Serialize)]
    enum InvalidFilter {
        Unsupported,
    }

    let encoded = icydb_schema::wire::encode_one(Some(InvalidFilter::Unsupported))
        .expect("invalid fixture should encode");
    assert!(icydb_schema::wire::decode_one::<Option<FilterExpr>>(&encoded).is_err());
}

#[test]
fn optional_filter_wire_rejects_malformed_current_payloads() {
    #[derive(serde::Serialize)]
    enum InvalidFilter {
        Constant(String),
        Compare {
            operator: String,
            field: String,
            value: InputValue,
        },
        Not(Box<Self>),
        Junction {
            operator: JunctionOperator,
            filters: Vec<Self>,
        },
    }

    let invalid = [
        InvalidFilter::Constant("true".to_string()),
        InvalidFilter::Compare {
            operator: "Eq".to_string(),
            field: "stage".to_string(),
            value: InputValue::text("Draft".to_string()),
        },
        InvalidFilter::Not(Box::new(InvalidFilter::Constant("false".to_string()))),
        InvalidFilter::Junction {
            operator: JunctionOperator::And,
            filters: vec![InvalidFilter::Constant("true".to_string())],
        },
    ];
    for filter in invalid {
        let encoded =
            icydb_schema::wire::encode_one(Some(filter)).expect("invalid fixture should encode");
        assert!(icydb_schema::wire::decode_one::<Option<FilterExpr>>(&encoded).is_err());
    }
}

#[test]
fn constructors_build_the_grouped_filter_families_directly() {
    assert!(matches!(
        FilterExpr::and(vec![FilterExpr::Constant(true)]),
        FilterExpr::Junction {
            operator: JunctionOperator::And,
            ..
        }
    ));
    assert!(matches!(
        FilterExpr::eq("field", 1_u64),
        FilterExpr::Compare {
            operator: CompareOperator::Eq,
            ..
        }
    ));
    assert!(matches!(
        FilterExpr::eq_field("left", "right"),
        FilterExpr::CompareFields {
            operator: FieldCompareOperator::Eq,
            ..
        }
    ));
    assert!(matches!(
        FilterExpr::not_in("field", [1_u64]),
        FilterExpr::Set {
            operator: SetOperator::NotIn,
            ..
        }
    ));
    assert!(matches!(
        FilterExpr::contains("field", 1_u64),
        FilterExpr::Collection {
            operator: CollectionOperator::Contains,
            ..
        }
    ));
    assert!(matches!(
        FilterExpr::is_missing("field"),
        FilterExpr::State {
            operator: StateOperator::IsMissing,
            ..
        }
    ));
}

#[test]
fn grouped_operator_lowering_retains_the_existing_planner_operations() {
    assert_eq!(JunctionOperator::And.binary_op(), BinaryOp::And);
    assert_eq!(JunctionOperator::Or.binary_op(), BinaryOp::Or);
    assert_eq!(CompareOperator::Eq.binary_op(), BinaryOp::Eq);
    assert_eq!(CompareOperator::EqCi.binary_op(), BinaryOp::Eq);
    assert_eq!(CompareOperator::Ne.binary_op(), BinaryOp::Ne);
    assert_eq!(CompareOperator::Lt.binary_op(), BinaryOp::Lt);
    assert_eq!(CompareOperator::Lte.binary_op(), BinaryOp::Lte);
    assert_eq!(CompareOperator::Gt.binary_op(), BinaryOp::Gt);
    assert_eq!(CompareOperator::Gte.binary_op(), BinaryOp::Gte);
    assert_eq!(FieldCompareOperator::Eq.binary_op(), BinaryOp::Eq);
    assert_eq!(FieldCompareOperator::Ne.binary_op(), BinaryOp::Ne);
    assert_eq!(FieldCompareOperator::Lt.binary_op(), BinaryOp::Lt);
    assert_eq!(FieldCompareOperator::Lte.binary_op(), BinaryOp::Lte);
    assert_eq!(FieldCompareOperator::Gt.binary_op(), BinaryOp::Gt);
    assert_eq!(FieldCompareOperator::Gte.binary_op(), BinaryOp::Gte);
    assert!(!SetOperator::In.is_negated());
    assert!(SetOperator::NotIn.is_negated());
    assert_eq!(StateOperator::IsNull.function(), Function::IsNull);
    assert_eq!(StateOperator::IsNotNull.function(), Function::IsNotNull);
    assert_eq!(StateOperator::IsMissing.function(), Function::IsMissing);
    assert_eq!(StateOperator::IsEmpty.function(), Function::IsEmpty);
    assert_eq!(StateOperator::IsNotEmpty.function(), Function::IsNotEmpty);
}

#[test]
fn typed_literals_keep_identity_structure_and_unit_through_wire_and_lowering() {
    let values = [
        InputValue::nat64(u64::MAX),
        InputValue::unit(),
        InputValue::null(),
        InputValue::map(vec![(
            InputValue::text("key".to_string()),
            InputValue::nat64(7),
        )]),
        InputValue::enum_value("Variant", Some("test::Enum"))
            .with_enum_payload(InputValue::list(vec![InputValue::nat64(9)]))
            .unwrap(),
    ];
    for value in values {
        let filter = FilterExpr::eq("id", value.clone());
        let bytes = icydb_schema::wire::encode_one(&filter).unwrap();
        assert_eq!(
            icydb_schema::wire::decode_one::<FilterExpr>(&bytes).unwrap(),
            filter
        );
        if !matches!(value.as_public(), PublicValue::Enum(_)) {
            crate::db::query::preparation::with_preparation_work(|work| {
                let lowered = super::lower_input_value(&value, None, work).unwrap();
                assert_eq!(lowered, value.clone().try_into_runtime_non_enum().unwrap());
            });
        }
    }
}
