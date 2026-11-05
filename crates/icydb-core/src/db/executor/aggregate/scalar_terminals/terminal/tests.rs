use super::{
    StructuralAggregateTerminal, StructuralAggregateTerminalKind,
    compile_structural_scalar_aggregate_terminal,
};
use crate::{
    db::executor::aggregate::{
        AggregateKind, BinaryOp, CompiledExpr,
        scalar_terminals::terminal::{
            InternedScalarAggregateInput, PreparedScalarAggregateTerminal,
            PreparedScalarAggregateTerminalSet, ScalarAggregateInput, ScalarAggregateTerminalKind,
        },
    },
    value::Value,
};

fn scalar_schema(name: &str) -> crate::db::schema::SchemaInfo {
    use crate::db::schema::{
        AcceptedCompositeCatalog, AcceptedFieldKind, AcceptedSchemaRevision,
        AcceptedSchemaSnapshot, AcceptedValueCatalogHandle, FieldId, FieldStorageDecode, LeafCodec,
        PersistedFieldSnapshot, PersistedSchemaSnapshot, ScalarCodec, SchemaFieldSlot, SchemaInfo,
        SchemaInsertDefault, SchemaRowLayout, SchemaVersion, empty_accepted_enum_catalog_for_tests,
    };
    let field = PersistedFieldSnapshot::new_initial(
        FieldId::new(1),
        name.into(),
        SchemaFieldSlot::new(0),
        AcceptedFieldKind::Nat64,
        Vec::new(),
        false,
        SchemaInsertDefault::None,
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Nat64),
    );
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::ScalarTerminal".into(),
        "ScalarTerminal".into(),
        field.id(),
        SchemaRowLayout::initial(vec![(field.id(), field.slot())]),
        vec![field],
    );
    SchemaInfo::from_accepted_snapshot_and_catalog(
        &AcceptedSchemaSnapshot::new(snapshot),
        AcceptedValueCatalogHandle::new_for_tests(
            empty_accepted_enum_catalog_for_tests(),
            AcceptedCompositeCatalog::empty(),
            AcceptedSchemaRevision::INITIAL,
        ),
    )
}

#[test]
fn structural_input_and_filter_compilation_share_current_scalar_admission() {
    use crate::{
        db::query::{
            builder::aggregate::count,
            plan::expr::{
                Expr, FieldId, FieldPath, Function, compile_scalar_projection_expr_with_schema,
            },
        },
        error::InternalError,
    };
    let expressions = [
        Expr::Field(FieldId::new("id")),
        Expr::Field(FieldId::new("id_now")),
        Expr::Literal(Value::Nat64(42)),
        Expr::FunctionCall {
            function: Function::Coalesce,
            args: vec![
                Expr::Field(FieldId::new("id")),
                Expr::Literal(Value::Nat64(0)),
            ],
        },
        Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Literal(Value::Text("private-value".repeat(1024)))),
            right: Box::new(Expr::Field(FieldId::new("missing"))),
        },
        Expr::FieldPath(FieldPath::new("missing", vec!["leaf".into()])),
        Expr::Aggregate(count()),
    ];
    for name in ["id", "id_now"] {
        let schema = scalar_schema(name);
        for expr in &expressions {
            let expected = crate::db::query::preparation::with_preparation_work(|work| {
                compile_scalar_projection_expr_with_schema(&schema, expr, work)
            })
            .unwrap();
            for input in [true, false] {
                let terminal = StructuralAggregateTerminal::new(
                    if input {
                        StructuralAggregateTerminalKind::Sum
                    } else {
                        StructuralAggregateTerminalKind::CountRows
                    },
                    None,
                    input.then(|| expr.clone()),
                    (!input).then(|| expr.clone()),
                    false,
                );
                match (
                    crate::db::query::preparation::with_preparation_work(|work| {
                        compile_structural_scalar_aggregate_terminal(&schema, &terminal, work)
                    }),
                    &expected,
                ) {
                    (Ok(prepared), Some(expected)) => {
                        if input {
                            assert_eq!(
                                prepared.input,
                                ScalarAggregateInput::Expr(expected.clone())
                            );
                        } else {
                            assert_eq!(prepared.filter.as_ref(), Some(expected));
                        }
                    }
                    (Err(error), None) => {
                        let expected = InternalError::query_executor_invariant();
                        assert_eq!(error.class(), expected.class());
                        assert_eq!(error.origin(), expected.origin());
                        assert_eq!(error.diagnostic_code(), expected.diagnostic_code());
                        assert_eq!(error.diagnostic_facts(), expected.diagnostic_facts());
                    }
                    other => panic!("compiler/terminal admission disagreement: {other:?}"),
                }
            }
        }
    }
}

fn literal_nat(value: u64) -> CompiledExpr {
    CompiledExpr::Literal(Value::Nat64(value))
}

fn repeated_input_expr() -> CompiledExpr {
    CompiledExpr::Binary {
        op: BinaryOp::Add,
        left: Box::new(literal_nat(41)),
        right: Box::new(literal_nat(1)),
    }
}

fn repeated_filter_expr() -> CompiledExpr {
    CompiledExpr::Binary {
        op: BinaryOp::Gte,
        left: Box::new(literal_nat(42)),
        right: Box::new(literal_nat(1)),
    }
}

fn field_terminal(
    kind: ScalarAggregateTerminalKind,
    filter: Option<CompiledExpr>,
    distinct: bool,
) -> PreparedScalarAggregateTerminal {
    PreparedScalarAggregateTerminal::from_validated_inputs(
        kind,
        ScalarAggregateInput::Field {
            slot: 2,
            field: "age".to_string(),
        },
        filter,
        distinct,
    )
}

#[test]
fn scalar_aggregate_terminal_set_exposes_only_single_direct_extrema_route_candidates() {
    let min = PreparedScalarAggregateTerminalSet::new(vec![field_terminal(
        ScalarAggregateTerminalKind::Min,
        None,
        false,
    )]);
    let max = PreparedScalarAggregateTerminalSet::new(vec![field_terminal(
        ScalarAggregateTerminalKind::Max,
        None,
        false,
    )]);

    assert_eq!(
        min.single_field_extrema_route_candidate(),
        Some((AggregateKind::Min, "age")),
    );
    assert_eq!(
        max.single_field_extrema_route_candidate(),
        Some((AggregateKind::Max, "age")),
    );
}

#[test]
fn scalar_aggregate_terminal_set_keeps_unsafe_extrema_shapes_off_routed_probes() {
    let filtered = PreparedScalarAggregateTerminalSet::new(vec![field_terminal(
        ScalarAggregateTerminalKind::Min,
        Some(repeated_filter_expr()),
        false,
    )]);
    let distinct = PreparedScalarAggregateTerminalSet::new(vec![field_terminal(
        ScalarAggregateTerminalKind::Min,
        None,
        true,
    )]);
    let expression = PreparedScalarAggregateTerminalSet::new(vec![
        PreparedScalarAggregateTerminal::from_validated_inputs(
            ScalarAggregateTerminalKind::Min,
            ScalarAggregateInput::Expr(repeated_input_expr()),
            None,
            false,
        ),
    ]);
    let multiple = PreparedScalarAggregateTerminalSet::new(vec![
        field_terminal(ScalarAggregateTerminalKind::Min, None, false),
        field_terminal(ScalarAggregateTerminalKind::Max, None, false),
    ]);

    assert_eq!(filtered.single_field_extrema_route_candidate(), None);
    assert_eq!(distinct.single_field_extrema_route_candidate(), None);
    assert_eq!(expression.single_field_extrema_route_candidate(), None);
    assert_eq!(multiple.single_field_extrema_route_candidate(), None);
}

#[test]
fn scalar_aggregate_terminal_set_interns_duplicate_input_and_filter_exprs() {
    let input = repeated_input_expr();
    let filter = repeated_filter_expr();
    let terminals = PreparedScalarAggregateTerminalSet::new(vec![
        PreparedScalarAggregateTerminal::from_validated_inputs(
            ScalarAggregateTerminalKind::Sum,
            ScalarAggregateInput::Expr(input.clone()),
            Some(filter.clone()),
            false,
        ),
        PreparedScalarAggregateTerminal::from_validated_inputs(
            ScalarAggregateTerminalKind::Avg,
            ScalarAggregateInput::Expr(input),
            Some(filter),
            false,
        ),
    ]);

    assert_eq!(
        terminals.input_exprs.len(),
        1,
        "duplicate SUM/AVG input expressions should share one interned input expression",
    );
    assert_eq!(
        terminals.filter_exprs.len(),
        1,
        "duplicate aggregate FILTER expressions should share one interned filter expression",
    );
    assert!(
        terminals
            .terminals
            .iter()
            .all(|terminal| matches!(terminal.input, InternedScalarAggregateInput::Expr(0))),
        "every expression-backed terminal should point at the shared input expression",
    );
    assert!(
        terminals
            .terminals
            .iter()
            .all(|terminal| terminal.filter == Some(0)),
        "every filtered terminal should point at the shared filter expression",
    );
}

#[test]
fn scalar_aggregate_terminal_set_keeps_field_inputs_out_of_expr_table() {
    let terminals = PreparedScalarAggregateTerminalSet::new(vec![
        PreparedScalarAggregateTerminal::from_validated_inputs(
            ScalarAggregateTerminalKind::CountValues,
            ScalarAggregateInput::Field {
                slot: 2,
                field: "age".to_string(),
            },
            None,
            false,
        ),
        PreparedScalarAggregateTerminal::from_validated_inputs(
            ScalarAggregateTerminalKind::Sum,
            ScalarAggregateInput::Expr(repeated_input_expr()),
            None,
            false,
        ),
    ]);

    assert_eq!(
        terminals.input_exprs.len(),
        1,
        "only expression-backed aggregate inputs should enter the input expression table",
    );
    assert!(
        matches!(
            terminals.terminals[0].input,
            InternedScalarAggregateInput::Field { slot: 2, .. }
        ),
        "field-backed aggregate inputs should remain direct retained-slot reads",
    );
    assert!(
        matches!(
            terminals.terminals[1].input,
            InternedScalarAggregateInput::Expr(0)
        ),
        "the expression-backed aggregate input should point at its interned expression",
    );
}
