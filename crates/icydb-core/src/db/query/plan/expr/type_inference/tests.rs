use super::{ExprType, infer_expr_type};
use crate::db::query::preparation::with_preparation_work;
use crate::{
    db::{
        query::{
            builder::aggregate::{avg, sum},
            plan::{
                AggregateKind,
                expr::{BinaryOp, Expr, FieldId, Function},
            },
        },
        schema::{
            AcceptedCompositeCatalog, AcceptedFieldKind, AcceptedSchemaRevision,
            AcceptedSchemaSnapshot, AcceptedValueCatalogHandle, FieldId as SchemaFieldId,
            FieldStorageDecode, LeafCodec, PersistedFieldSnapshot, PersistedSchemaSnapshot,
            ScalarCodec, SchemaFieldSlot, SchemaInfo, SchemaInsertDefault, SchemaRowLayout,
            SchemaVersion, empty_accepted_enum_catalog_for_tests,
        },
    },
    types::U256,
    value::Value,
};

fn u256_schema() -> SchemaInfo {
    let field_id = SchemaFieldId::new(1);
    let slot = SchemaFieldSlot::new(0);
    let snapshot = AcceptedSchemaSnapshot::try_new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "type_inference::U256Entity".to_string(),
        "U256Entity".to_string(),
        field_id,
        SchemaRowLayout::initial(vec![(field_id, slot)]),
        vec![PersistedFieldSnapshot::new_initial(
            field_id,
            "balance".to_string(),
            slot,
            AcceptedFieldKind::U256,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::U256),
        )],
    ))
    .expect("U256 planner fixture should validate");
    let catalog = AcceptedValueCatalogHandle::new_for_tests(
        empty_accepted_enum_catalog_for_tests(),
        AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    );

    SchemaInfo::from_accepted_snapshot_and_catalog(&snapshot, catalog)
}

#[test]
fn null_arithmetic_validates_both_operands_before_propagating_null() {
    let schema = u256_schema();
    for op in [BinaryOp::Add, BinaryOp::Sub, BinaryOp::Mul, BinaryOp::Div] {
        for value in [Value::Null, Value::Nat64(1), Value::U256(U256::MAX)] {
            for (left, right) in [(Value::Null, value.clone()), (value, Value::Null)] {
                let expr = Expr::Binary {
                    op,
                    left: Box::new(Expr::Literal(left)),
                    right: Box::new(Expr::Literal(right)),
                };
                assert_eq!(
                    with_preparation_work(|work| infer_expr_type(&expr, &schema, work))
                        .expect("NULL arithmetic"),
                    ExprType::Null
                );
            }
        }
        for other in [
            Expr::Literal(Value::Text("invalid".into())),
            Expr::Field(FieldId::new("missing")),
        ] {
            let expr = Expr::Binary {
                op,
                left: Box::new(Expr::Literal(Value::Null)),
                right: Box::new(other),
            };
            assert!(with_preparation_work(|work| infer_expr_type(&expr, &schema, work)).is_err());
        }
    }
}

#[test]
fn null_else_does_not_erase_accumulated_case_result_type() {
    use crate::db::query::plan::expr::CaseWhenArm;

    let expression = Expr::Case {
        when_then_arms: vec![
            CaseWhenArm::new(
                Expr::Literal(Value::Bool(false)),
                Expr::Literal(Value::Text("incompatible".into())),
            ),
            CaseWhenArm::new(
                Expr::Literal(Value::Bool(true)),
                Expr::Literal(Value::Nat64(1)),
            ),
        ],
        else_expr: Box::new(Expr::Literal(Value::Null)),
    };
    assert!(
        with_preparation_work(|work| infer_expr_type(&expression, &u256_schema(), work)).is_err()
    );
}

#[test]
fn planner_admits_strict_u256_arithmetic_and_sum_but_not_average_or_mixed_width() {
    let schema = u256_schema();
    let balance = || Expr::Field(FieldId::new("balance"));
    let add = Expr::Binary {
        op: BinaryOp::Add,
        left: Box::new(balance()),
        right: Box::new(balance()),
    };
    let modulo = Expr::FunctionCall {
        function: Function::Mod,
        args: vec![balance(), Expr::Literal(Value::U256(U256::from(3_u64)))],
    };
    let mixed = Expr::Binary {
        op: BinaryOp::Add,
        left: Box::new(balance()),
        right: Box::new(Expr::Literal(Value::Nat64(1))),
    };
    let sum = Expr::Aggregate(sum("balance"));
    let average = Expr::Aggregate(avg("balance"));

    assert_eq!(
        with_preparation_work(|work| infer_expr_type(&add, &schema, work))
            .expect("U256 addition should plan"),
        ExprType::U256,
    );
    assert_eq!(
        with_preparation_work(|work| infer_expr_type(&modulo, &schema, work))
            .expect("U256 MOD should plan"),
        ExprType::U256,
    );
    assert_eq!(
        with_preparation_work(|work| infer_expr_type(&sum, &schema, work))
            .expect("U256 SUM should plan"),
        ExprType::U256,
    );
    assert!(with_preparation_work(|work| infer_expr_type(&average, &schema, work)).is_err());
    assert!(with_preparation_work(|work| infer_expr_type(&mixed, &schema, work)).is_err());

    let expression_sum = crate::db::query::builder::aggregate::AggregateExpr::from_expression_input(
        AggregateKind::Sum,
        add,
    );
    assert_eq!(
        with_preparation_work(|work| infer_expr_type(
            &Expr::Aggregate(expression_sum),
            &schema,
            work
        ))
        .expect("U256 expression SUM should plan"),
        ExprType::U256,
    );
}
