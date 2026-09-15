//! Module: query::plan::expr::type_inference
//! Responsibility: infer deterministic planner expression type classes from schema and AST.
//! Does not own: runtime projection evaluation or expression execution behavior.
//! Boundary: returns planner-domain type information and typed plan errors
//! without compiling predicates or rewriting canonical expression shape.

mod aggregate;
mod binary;
mod case;
mod function;
mod source;
mod unify;

#[cfg(test)]
mod admission_tests;

use crate::db::{
    QueryError,
    query::plan::{
        PlanError,
        expr::{
            NumericSubtype,
            ast::{Expr, UnaryOp},
        },
        validate::ExprPlanError,
    },
    query::preparation::PreparationWork,
    schema::SchemaInfo,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

pub(in crate::db::query::plan::expr) use function::function_is_compare_operand_coarse_family;

///
/// ExprType
///
/// Minimal deterministic expression type classification for planner inference.
/// This intentionally remains coarse in the bootstrap phase.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum ExprType {
    Blob,
    Bool,
    Numeric(NumericSubtype),
    Text,
    // Known SQL NULL is distinct from unresolved type information in every build.
    Null,
    Collection,
    Structured,
    Opaque,
    U256,
    Unknown,
}

///
/// FunctionArgumentFamily
///
/// Closed scalar-function argument family used by planner type validation.
/// This projects only the numeric/text distinctions required by fixed
/// signatures and does not form a second expression type lattice.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FunctionArgumentFamily {
    Numeric,
    Text,
}

impl ExprType {
    // Eligibility answers "can this participate in numeric-only operators?".
    // Subtype answers "which numeric family?" and may remain unresolved.
    const fn is_numeric_eligible(&self) -> bool {
        matches!(self, Self::Numeric(_))
    }

    const fn numeric_subtype(&self) -> Option<NumericSubtype> {
        match self {
            Self::Numeric(subtype) => Some(*subtype),
            _ => None,
        }
    }
}

/// Infer expression type under current preparation authority without rewriting
/// syntax. Admit each visited node before inspecting it; child order is semantic.
pub(in crate::db) fn infer_expr_type(
    expr: &Expr,
    schema: &SchemaInfo,
    work: &PreparationWork<'_>,
) -> Result<ExprType, QueryError> {
    work.charge(Resource::PredicateExpressionSteps, 1)?;
    match expr {
        Expr::Field(field) => source::infer_field_expr_type(field, schema, work),
        Expr::FieldPath(path) => source::infer_field_path_expr_type(path, schema, work),
        Expr::Literal(value) => Ok(source::infer_literal_type(value)),
        Expr::FunctionCall { function, args } => {
            function::infer_function_expr_type(*function, args.as_slice(), schema, work)
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            case::infer_case_expr_type(when_then_arms.as_slice(), else_expr.as_ref(), schema, work)
        }
        Expr::Aggregate(aggregate) => aggregate::infer_aggregate_expr_type(aggregate, schema, work),
        #[cfg(test)]
        Expr::Alias { expr, .. } => infer_expr_type(expr.as_ref(), schema, work),
        Expr::Unary { op, expr } => {
            let inner = infer_expr_type(expr.as_ref(), schema, work)?;

            match op {
                UnaryOp::Not => {
                    if !matches!(inner, ExprType::Bool | ExprType::Null) {
                        return Err(PlanError::from(ExprPlanError::invalid_unary_operand(
                            *op, &inner,
                        ))
                        .into());
                    }

                    Ok(ExprType::Bool)
                }
            }
        }
        Expr::Binary { op, left, right } => {
            binary::infer_binary_expr_type(*op, left.as_ref(), right.as_ref(), schema, work)
        }
    }
}

#[cfg(test)]
mod tests {
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

        SchemaInfo::from_accepted_snapshot_and_catalog(&snapshot, catalog, true)
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
                assert!(
                    with_preparation_work(|work| infer_expr_type(&expr, &schema, work)).is_err()
                );
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
            with_preparation_work(|work| infer_expr_type(&expression, &u256_schema(), work))
                .is_err()
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

        let expression_sum =
            crate::db::query::builder::aggregate::AggregateExpr::from_expression_input(
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
}
