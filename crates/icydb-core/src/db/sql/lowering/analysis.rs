use crate::db::{
    query::{
        builder::AggregateExpr,
        plan::{
            GroupFieldSet,
            expr::{Expr, FieldPath},
        },
    },
    schema::SchemaInfo,
};

///
/// AnalyzedLoweredExpr
///
/// One lowered planner expression plus the compile-time facts derived from the
/// same tree walk. SQL lowering code should pass this around when the lowered
/// expression and its aggregate/field proof are consumed together, instead of
/// keeping the proof as an adjacent loose value.
///

#[derive(Clone, Debug)]
pub(in crate::db::sql::lowering) struct AnalyzedLoweredExpr {
    expr: Expr,
    analysis: LoweredExprAnalysis,
}

impl AnalyzedLoweredExpr {
    /// Analyze one owned lowered planner expression.
    #[must_use]
    pub(in crate::db::sql::lowering) fn new(expr: Expr) -> Self {
        let analysis = analyze_lowered_expr(&expr);

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

    /// Consume the artifact and return the lowered expression with the
    /// analysis derived from that exact expression.
    #[must_use]
    pub(in crate::db::sql::lowering) fn into_parts(self) -> (Expr, LoweredExprAnalysis) {
        (self.expr, self.analysis)
    }
}

///
/// LoweredExprSourceRef
///
/// One field source reference discovered while analyzing a lowered SQL
/// expression. This lets schema-bound SQL validation consume the same analysis
/// proof instead of walking projection expressions again.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::sql::lowering) enum LoweredExprSourceRef {
    Direct(String),
    Path(FieldPath),
}

impl LoweredExprSourceRef {
    const fn root(&self) -> &str {
        match self {
            Self::Direct(field) => field.as_str(),
            Self::Path(path) => path.root().as_str(),
        }
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
    source_refs: Vec<LoweredExprSourceRef>,
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
        !self.source_refs.is_empty()
    }

    /// Return whether all analyzed field references are direct leaves from the
    /// supplied allowlist. Field paths fail this direct-field proof, matching
    /// grouped projection admission.
    #[must_use]
    pub(in crate::db::sql::lowering) fn references_only_direct_fields(
        &self,
        allowed: &[&str],
    ) -> bool {
        self.source_refs.iter().all(|source_ref| match source_ref {
            LoweredExprSourceRef::Direct(field) => allowed.contains(&field.as_str()),
            LoweredExprSourceRef::Path(_) => false,
        })
    }

    /// Return whether every outer field/path leaf is one declared group key.
    #[must_use]
    pub(in crate::db::sql::lowering) fn references_only_group_fields(
        &self,
        group_fields: &GroupFieldSet,
    ) -> bool {
        self.source_refs.iter().all(|source_ref| {
            group_fields.iter().any(|group_field| match source_ref {
                LoweredExprSourceRef::Direct(field) => group_field
                    .as_direct()
                    .is_some_and(|group_field| group_field.field() == field),
                LoweredExprSourceRef::Path(path) => group_field
                    .as_scalar_path()
                    .is_some_and(|group_path| group_path.path() == path.path_spec()),
            })
        })
    }

    /// Borrow the first unknown field discovered during left-to-right tree walk.
    #[must_use]
    pub(in crate::db::sql::lowering) fn first_unknown_field(&self) -> Option<&str> {
        self.first_unknown_field.as_deref()
    }

    /// Return the first unknown field root for accepted schema authority.
    #[must_use]
    pub(in crate::db::sql::lowering) fn first_unknown_field_for_schema(
        &self,
        schema: &SchemaInfo,
    ) -> Option<&str> {
        self.first_unknown_field().or_else(|| {
            self.source_refs.iter().find_map(|source_ref| {
                let root = source_ref.root();
                schema.field_slot_index(root).is_none().then_some(root)
            })
        })
    }

    /// Borrow field source references in traversal order.
    #[must_use]
    pub(in crate::db::sql::lowering) const fn source_refs(&self) -> &[LoweredExprSourceRef] {
        self.source_refs.as_slice()
    }

    /// Record one field leaf while preserving the first unknown-field diagnostic.
    fn visit_field(&mut self, field: &str) {
        self.source_refs
            .push(LoweredExprSourceRef::Direct(field.to_string()));
    }

    fn visit_field_path(&mut self, path: &FieldPath) {
        self.source_refs
            .push(LoweredExprSourceRef::Path(path.clone()));
    }

    fn visit_aggregate(&mut self, aggregate: &AggregateExpr) {
        self.aggregate_refs.push(aggregate.clone());
    }
}

/// Analyze one already-lowered planner expression once for the shared SQL
/// lowering questions about aggregate presence, direct field references, and
/// unknown field diagnostics.
#[must_use]
pub(in crate::db::sql::lowering) fn analyze_lowered_expr(expr: &Expr) -> LoweredExprAnalysis {
    let mut analysis = LoweredExprAnalysis {
        aggregate_refs: Vec::new(),
        source_refs: Vec::new(),
        first_unknown_field: None,
    };

    expr.for_each_tree_expr(&mut |node| match node {
        Expr::Field(field) => {
            analysis.visit_field(field.as_str());
        }
        Expr::FieldPath(path) => {
            analysis.visit_field_path(path);
        }
        Expr::Aggregate(aggregate) => {
            analysis.visit_aggregate(aggregate);
        }
        _ => {}
    });

    analysis
}
