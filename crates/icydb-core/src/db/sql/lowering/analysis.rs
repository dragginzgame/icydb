use crate::{
    db::query::{builder::AggregateExpr, plan::expr::Expr},
    model::entity::EntityModel,
};

///
/// AnalyzedLoweredExpr
///
/// One lowered planner expression plus the compile-time facts derived from the
/// same tree walk. SQL lowering code should pass this around when the lowered
/// expression and its aggregate/field proof are consumed together, instead of
/// keeping the proof as an adjacent loose value.
///

#[derive(Debug)]
pub(in crate::db::sql::lowering) struct AnalyzedLoweredExpr {
    expr: Expr,
    analysis: LoweredExprAnalysis,
}

impl AnalyzedLoweredExpr {
    /// Analyze one owned lowered planner expression.
    #[must_use]
    pub(in crate::db::sql::lowering) fn new(expr: Expr, model: Option<&EntityModel>) -> Self {
        let analysis = analyze_lowered_expr(&expr, model);

        Self { expr, analysis }
    }

    /// Borrow the lowered expression.
    #[must_use]
    pub(in crate::db::sql::lowering) const fn expr(&self) -> &Expr {
        &self.expr
    }

    /// Borrow the analysis proof derived from the lowered expression.
    #[must_use]
    pub(in crate::db::sql::lowering) const fn analysis(&self) -> &LoweredExprAnalysis {
        &self.analysis
    }

    /// Consume the artifact and return the lowered expression.
    #[must_use]
    pub(in crate::db::sql::lowering) fn into_expr(self) -> Expr {
        self.expr
    }
}

///
/// LoweredExprAnalysis
///
/// LoweredExprAnalysis keeps the compile-time facts SQL lowering repeatedly
/// asks of one already-lowered planner expression.
/// This exists so grouped and global aggregate lowering stop rescanning the
/// same `Expr` tree for aggregate leaves, direct-field leakage, and unknown
/// field diagnostics on separate helper seams.
/// The result is intentionally immutable and short-lived: callers analyze one
/// lowered expression, consume the facts immediately, and do not cache or
/// reuse the summary across unrelated lowering contexts.
///

#[derive(Clone, Debug, Default)]
pub(in crate::db::sql::lowering) struct LoweredExprAnalysis {
    aggregate_refs: Vec<AggregateExpr>,
    direct_field_roots: Vec<String>,
    contains_field_path: bool,
    first_unknown_field: Option<String>,
}

impl LoweredExprAnalysis {
    /// Return whether the analyzed expression contains at least one aggregate leaf.
    #[must_use]
    pub(in crate::db::sql::lowering) const fn contains_aggregate(&self) -> bool {
        !self.aggregate_refs.is_empty()
    }

    /// Borrow aggregate leaves in the same left-to-right order as the analyzed
    /// expression traversal. Aggregate input/filter expressions remain owned by
    /// the aggregate leaf and are not analyzed as outer direct-field leakage.
    #[must_use]
    pub(in crate::db::sql::lowering) const fn aggregate_refs(&self) -> &[AggregateExpr] {
        self.aggregate_refs.as_slice()
    }

    /// Return whether the analyzed expression references direct field leaves
    /// outside aggregate-owned subtrees.
    #[must_use]
    pub(in crate::db::sql::lowering) const fn references_direct_fields(&self) -> bool {
        !self.direct_field_roots.is_empty()
    }

    /// Return whether all analyzed field references are direct leaves from the
    /// supplied allowlist. Field paths fail this direct-field proof, matching
    /// grouped projection admission.
    #[must_use]
    pub(in crate::db::sql::lowering) fn references_only_direct_fields(
        &self,
        allowed: &[&str],
    ) -> bool {
        !self.contains_field_path
            && self
                .direct_field_roots
                .iter()
                .all(|field| allowed.contains(&field.as_str()))
    }

    /// Borrow the first unknown field discovered during left-to-right tree walk.
    #[must_use]
    pub(in crate::db::sql::lowering) fn first_unknown_field(&self) -> Option<&str> {
        self.first_unknown_field.as_deref()
    }

    /// Return the first unknown direct field for one model without rewalking
    /// the expression tree. This lets callers that analyzed without model
    /// context reuse the recorded field-root order later at a model-bound seam.
    #[must_use]
    pub(in crate::db::sql::lowering) fn first_unknown_field_for_model(
        &self,
        model: &EntityModel,
    ) -> Option<&str> {
        self.first_unknown_field().or_else(|| {
            self.direct_field_roots
                .iter()
                .find(|field| model.resolve_field_slot(field.as_str()).is_none())
                .map(String::as_str)
        })
    }

    /// Record one field leaf while preserving the first unknown-field diagnostic.
    fn visit_field(&mut self, field: &str, model: Option<&EntityModel>) {
        self.direct_field_roots.push(field.to_string());
        if self.first_unknown_field.is_none()
            && model.is_some_and(|model| model.resolve_field_slot(field).is_none())
        {
            self.first_unknown_field = Some(field.to_string());
        }
    }

    fn visit_field_path(&mut self, field: &str, model: Option<&EntityModel>) {
        self.contains_field_path = true;
        self.visit_field(field, model);
    }

    fn visit_aggregate(&mut self, aggregate: &AggregateExpr) {
        self.aggregate_refs.push(aggregate.clone());
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
        aggregate_refs: Vec::new(),
        direct_field_roots: Vec::new(),
        contains_field_path: false,
        first_unknown_field: None,
    };

    expr.try_for_each_tree_expr(&mut |node| match node {
        Expr::Field(field) => {
            analysis.visit_field(field.as_str(), model);
            Ok::<(), ()>(())
        }
        Expr::FieldPath(path) => {
            analysis.visit_field_path(path.root().as_str(), model);
            Ok::<(), ()>(())
        }
        Expr::Aggregate(aggregate) => {
            analysis.visit_aggregate(aggregate);
            Ok::<(), ()>(())
        }
        _ => Ok(()),
    })
    .expect("sql lowering invariant");

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
                    expr::{BinaryOp, Expr, FieldId, FieldPath, Function},
                },
            },
            sql::lowering::analysis::{AnalyzedLoweredExpr, analyze_lowered_expr},
        },
        model::field::FieldKind,
        traits::EntitySchema,
        types::Ulid,
        value::Value,
    };
    use serde::Deserialize;

    #[derive(Clone, Debug, Deserialize, PartialEq)]
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

    crate::test_entity! {
        ident = LoweredExprAnalysisEntity,
        entity_name = "LoweredExprAnalysisEntity",
        tag = crate::types::EntityTag::new(0x1040),
        store = LoweredExprAnalysisStore,
        canister = LoweredExprAnalysisCanister,
    key_type = Ulid,
        primary_key = [id],
        fields = [
            crate::test_field! { id: Ulid => FieldKind::Ulid },
            crate::test_field! { age: u64 => FieldKind::Nat64 },
        ],
        indexes = [],
        relations = [],
        entity_value = none,
    }

    #[test]
    fn lowered_expr_analysis_matches_grouped_and_global_post_aggregate_shapes() {
        let grouped_shape = Expr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(Expr::FunctionCall {
                function: Function::Round,
                args: vec![
                    Expr::Aggregate(AggregateExpr::terminal_for_kind(AggregateKind::Count)),
                    Expr::Literal(Value::Nat64(0)),
                ],
            }),
            right: Box::new(Expr::Literal(Value::Nat64(1))),
        };
        let global_shape = Expr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(Expr::FunctionCall {
                function: Function::Round,
                args: vec![
                    Expr::Aggregate(AggregateExpr::terminal_for_kind(AggregateKind::Count)),
                    Expr::Literal(Value::Nat64(0)),
                ],
            }),
            right: Box::new(Expr::Literal(Value::Nat64(1))),
        };

        let grouped = analyze_lowered_expr(&grouped_shape, Some(LoweredExprAnalysisEntity::MODEL));
        let global = analyze_lowered_expr(&global_shape, Some(LoweredExprAnalysisEntity::MODEL));

        assert_eq!(
            grouped.contains_aggregate(),
            global.contains_aggregate(),
            "equivalent grouped/global post-aggregate shapes must agree on aggregate presence",
        );
        assert_eq!(
            grouped.aggregate_refs(),
            global.aggregate_refs(),
            "equivalent grouped/global post-aggregate shapes must agree on aggregate leaves",
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

    #[test]
    fn lowered_expr_analysis_proves_direct_group_fields_without_admitting_field_paths() {
        let direct = analyze_lowered_expr(&Expr::Field(FieldId::new("age")), None);
        let path = analyze_lowered_expr(
            &Expr::FieldPath(FieldPath::new("age", vec!["rank".to_string()])),
            None,
        );

        assert!(direct.references_direct_fields());
        assert!(direct.references_only_direct_fields(&["age"]));
        assert!(path.references_direct_fields());
        assert!(
            !path.references_only_direct_fields(&["age"]),
            "field paths must not satisfy grouped direct-field authority just because their root is grouped",
        );
    }

    #[test]
    fn analyzed_lowered_expr_keeps_expr_and_analysis_coupled() {
        let expr = Expr::Field(FieldId::new("age"));
        let analyzed =
            AnalyzedLoweredExpr::new(expr.clone(), Some(LoweredExprAnalysisEntity::MODEL));

        assert_eq!(analyzed.expr(), &expr);
        assert!(analyzed.analysis().references_direct_fields());
        assert_eq!(analyzed.analysis().first_unknown_field(), None);
        assert_eq!(analyzed.into_expr(), expr);
    }

    #[test]
    fn lowered_expr_analysis_collects_aggregate_leaves_without_field_leakage() {
        let avg_age = AggregateExpr::from_expression_input(
            AggregateKind::Avg,
            Expr::Field(FieldId::new("age")),
        );
        let count_all = AggregateExpr::terminal_for_kind(AggregateKind::Count);
        let expr = Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Aggregate(avg_age.clone())),
            right: Box::new(Expr::Aggregate(count_all.clone())),
        };

        let analysis = analyze_lowered_expr(&expr, Some(LoweredExprAnalysisEntity::MODEL));

        assert!(analysis.contains_aggregate());
        assert_eq!(
            analysis.aggregate_refs(),
            &[avg_age, count_all],
            "aggregate refs should preserve left-to-right lowered expression order",
        );
        assert!(
            !analysis.references_direct_fields(),
            "aggregate input fields are aggregate-owned and must not count as outer direct-field leakage",
        );
    }
}
