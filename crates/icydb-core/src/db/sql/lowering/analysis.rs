use crate::{
    db::query::plan::expr::Expr,
    model::entity::{EntityModel, resolve_field_slot},
};

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
            && model.is_some_and(|model| resolve_field_slot(model, field).is_none())
        {
            self.first_unknown_field = Some(field.to_string());
        }
    }

    /// Merge one child analysis while preserving the first discovered field error.
    fn absorb(&mut self, child: Self) {
        self.contains_aggregate |= child.contains_aggregate;
        self.references_direct_fields |= child.references_direct_fields;
        if self.first_unknown_field.is_none() {
            self.first_unknown_field = child.first_unknown_field;
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
    match expr {
        Expr::Field(field) => {
            let mut analysis = LoweredExprAnalysis::default();
            analysis.visit_field(field.as_str(), model);
            analysis
        }
        Expr::Aggregate(_) => LoweredExprAnalysis {
            contains_aggregate: true,
            references_direct_fields: false,
            first_unknown_field: None,
        },
        Expr::Literal(_) => LoweredExprAnalysis::default(),
        Expr::FunctionCall { args, .. } => {
            let mut analysis = LoweredExprAnalysis::default();
            for arg in args {
                analysis.absorb(analyze_lowered_expr(arg, model));
            }
            analysis
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            let mut analysis = LoweredExprAnalysis::default();
            for arm in when_then_arms {
                analysis.absorb(analyze_lowered_expr(arm.condition(), model));
                analysis.absorb(analyze_lowered_expr(arm.result(), model));
            }
            analysis.absorb(analyze_lowered_expr(else_expr.as_ref(), model));
            analysis
        }
        Expr::Binary { left, right, .. } => {
            let mut analysis = analyze_lowered_expr(left.as_ref(), model);
            analysis.absorb(analyze_lowered_expr(right.as_ref(), model));
            analysis
        }
        Expr::Unary { expr, .. } => analyze_lowered_expr(expr.as_ref(), model),
        #[cfg(test)]
        Expr::Alias { expr, .. } => analyze_lowered_expr(expr.as_ref(), model),
    }
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
