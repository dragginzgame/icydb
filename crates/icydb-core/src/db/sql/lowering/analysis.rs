use crate::{db::query::plan::expr::Expr, model::entity::EntityModel};

///
/// LoweredExprAnalysis
///
/// LoweredExprAnalysis keeps the compile-time facts SQL lowering repeatedly
/// asks of one already-lowered planner expression.
/// This exists so grouped and global aggregate lowering stop rescanning the
/// same `Expr` tree for aggregate presence, direct-field leakage, and unknown
/// field diagnostics on separate helper seams.
/// The result is intentionally immutable and short-lived: callers analyze one
/// lowered expression, consume the facts immediately, and do not cache or
/// reuse the summary across unrelated lowering contexts.
///

#[derive(Debug, Default)]
pub(in crate::db::sql::lowering) struct LoweredExprAnalysis {
    contains_aggregate: bool,
    references_direct_fields: bool,
    first_unknown_field: Option<String>,
}

impl LoweredExprAnalysis {
    /// Return whether the analyzed expression contains at least one aggregate leaf.
    #[must_use]
    pub(in crate::db::sql::lowering) const fn contains_aggregate(&self) -> bool {
        self.contains_aggregate
    }

    /// Return whether the analyzed expression references direct field leaves
    /// outside aggregate-owned subtrees.
    #[must_use]
    pub(in crate::db::sql::lowering) const fn references_direct_fields(&self) -> bool {
        self.references_direct_fields
    }

    /// Borrow the first unknown field discovered during left-to-right tree walk.
    #[must_use]
    pub(in crate::db::sql::lowering) fn first_unknown_field(&self) -> Option<&str> {
        self.first_unknown_field.as_deref()
    }

    /// Record one field leaf while preserving the first unknown-field diagnostic.
    fn visit_field(&mut self, field: &str, model: Option<&EntityModel>) {
        self.references_direct_fields = true;
        if self.first_unknown_field.is_none()
            && model.is_some_and(|model| model.resolve_field_slot(field).is_none())
        {
            self.first_unknown_field = Some(field.to_string());
        }
    }
}

/// Analyze one already-lowered planner expression once for the shared SQL
/// lowering questions about aggregate presence, direct field references, and
/// unknown field diagnostics.
#[must_use]
pub(in crate::db::sql::lowering) fn analyze_lowered_expr(
    expr: &Expr,
    model: Option<&EntityModel>,
) -> LoweredExprAnalysis {
    let mut analysis = LoweredExprAnalysis {
        contains_aggregate: expr.contains_aggregate(),
        references_direct_fields: false,
        first_unknown_field: None,
    };

    expr.try_for_each_tree_expr(&mut |node| match node {
        Expr::Field(field) => {
            analysis.visit_field(field.as_str(), model);
            Ok::<(), ()>(())
        }
        _ => Ok(()),
    })
    .expect("field-only lowered-expression analysis visitor cannot fail");

    analysis
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            query::{
                builder::aggregate::AggregateExpr,
                plan::{
                    AggregateKind,
                    expr::{BinaryOp, Expr, Function},
                },
            },
            sql::lowering::analysis::analyze_lowered_expr,
        },
        model::field::FieldKind,
        traits::EntitySchema,
        types::Ulid,
        value::Value,
    };
    use serde::Deserialize;

    #[derive(Clone, Debug, Default, Deserialize, PartialEq)]
    struct LoweredExprAnalysisEntity {
        id: Ulid,
        age: u64,
    }

    crate::test_canister! {
        ident = LoweredExprAnalysisCanister,
        commit_memory_id = crate::testing::test_commit_memory_id(),
    }

    crate::test_store! {
        ident = LoweredExprAnalysisStore,
        canister = LoweredExprAnalysisCanister,
    }

    crate::test_entity_schema! {
        ident = LoweredExprAnalysisEntity,
        id = Ulid,
        entity_name = "LoweredExprAnalysisEntity",
        entity_tag = crate::types::EntityTag::new(0x1040),
        pk_index = 0,
        fields = [
            ("id", FieldKind::Ulid),
            ("age", FieldKind::Uint),
        ],
        indexes = [],
        store = LoweredExprAnalysisStore,
        canister = LoweredExprAnalysisCanister,
    }

    #[test]
    fn lowered_expr_analysis_matches_grouped_and_global_post_aggregate_shapes() {
        let grouped_shape = Expr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(Expr::FunctionCall {
                function: Function::Round,
                args: vec![
                    Expr::Aggregate(AggregateExpr::terminal_for_kind(AggregateKind::Count)),
                    Expr::Literal(Value::Uint(0)),
                ],
            }),
            right: Box::new(Expr::Literal(Value::Uint(1))),
        };
        let global_shape = Expr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(Expr::FunctionCall {
                function: Function::Round,
                args: vec![
                    Expr::Aggregate(AggregateExpr::terminal_for_kind(AggregateKind::Count)),
                    Expr::Literal(Value::Uint(0)),
                ],
            }),
            right: Box::new(Expr::Literal(Value::Uint(1))),
        };

        let grouped = analyze_lowered_expr(&grouped_shape, Some(LoweredExprAnalysisEntity::MODEL));
        let global = analyze_lowered_expr(&global_shape, Some(LoweredExprAnalysisEntity::MODEL));

        assert_eq!(
            grouped.contains_aggregate(),
            global.contains_aggregate(),
            "equivalent grouped/global post-aggregate shapes must agree on aggregate presence",
        );
        assert_eq!(
            grouped.references_direct_fields(),
            global.references_direct_fields(),
            "equivalent grouped/global post-aggregate shapes must agree on direct-field leakage",
        );
        assert_eq!(
            grouped.first_unknown_field(),
            global.first_unknown_field(),
            "equivalent grouped/global post-aggregate shapes must agree on unknown-field diagnostics",
        );
    }
}
