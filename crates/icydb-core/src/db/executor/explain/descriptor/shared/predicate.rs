use crate::{
    db::{
        QueryError,
        executor::ExecutionPreparation,
        predicate::{IndexPredicateCapability, PredicateCapabilityProfile},
        query::{
            explain::{
                ExplainAccessPath as ExplainAccessRoute, ExplainExecutionMode,
                ExplainExecutionNodeDescriptor, ExplainExecutionNodeType, ExplainPredicate,
                property_keys, property_values,
            },
            plan::{
                AccessPlanProjection, AccessPlannedQuery, AggregateKind, ResidualFilterShape,
                index_covering_existing_rows_terminal_eligible, project_explain_access_path,
                render_scalar_filter_expr_plan_label,
            },
            preparation::PreparationWork,
        },
    },
    value::Value,
};
use std::{fmt::Write, ops::Bound};

///
/// PredicateStageObservability
///
/// Route-owned predicate-stage observability for scalar load execution.
/// Planner-owned predicate-pushdown diagnostics describe logical predicate
/// coverage; this enum describes the executor route stage that will run, if
/// any, after route preparation and strict index-prefilter compatibility.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor::explain::descriptor) enum PredicateStageObservability {
    None,
    IndexPrefilterStrictAllOrNone,
    ResidualRuntime,
}

impl PredicateStageObservability {
    #[must_use]
    pub(in crate::db::executor::explain::descriptor) const fn from_parts(
        strict_prefilter_compiled: bool,
        residual_filter_shape: ResidualFilterShape,
    ) -> Self {
        if strict_prefilter_compiled {
            return Self::IndexPrefilterStrictAllOrNone;
        }
        if !residual_filter_shape.is_absent() {
            return Self::ResidualRuntime;
        }

        Self::None
    }

    #[must_use]
    pub(in crate::db::executor::explain::descriptor) const fn label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::IndexPrefilterStrictAllOrNone => "index_prefilter(strict_all_or_none)",
            Self::ResidualRuntime => "residual_runtime",
        }
    }

    #[must_use]
    const fn strict_prefilter_compiled(self) -> bool {
        matches!(self, Self::IndexPrefilterStrictAllOrNone)
    }

    #[must_use]
    const fn is_absent(self) -> bool {
        matches!(self, Self::None)
    }
}

pub(in crate::db::executor::explain::descriptor) fn predicate_stage_descriptors(
    filter_expr: Option<String>,
    residual_filter_expr: Option<String>,
    explain_predicate: Option<ExplainPredicate>,
    residual_filter_shape: ResidualFilterShape,
    access_strategy: Option<&ExplainAccessRoute>,
    stage: PredicateStageObservability,
    execution_mode: ExplainExecutionMode,
) -> Vec<ExplainExecutionNodeDescriptor> {
    if stage.is_absent() {
        return Vec::new();
    }

    // Strict prefilters still describe one pushdown-only predicate stage. The
    // semantic filter expression is carried through for wording parity, but
    // there is no residual execution-stage predicate node in this case.
    if stage.strict_prefilter_compiled() {
        let mut node =
            crate::db::executor::explain::descriptor::shared::empty_execution_node_descriptor(
                ExplainExecutionNodeType::IndexPredicatePrefilter,
                execution_mode,
            );
        node.predicate_pushdown = Some(property_values::STRICT_ALL_OR_NONE.to_string());
        node.filter_expr = filter_expr;
        let pushdown_predicate = access_strategy
            .and_then(pushdown_predicate_from_access_strategy)
            .unwrap_or_else(|| format!("{explain_predicate:?}"));
        node.node_properties
            .insert(property_keys::PUSHDOWN, Value::from(pushdown_predicate));
        return vec![node];
    }

    // Residual execution keeps both labels when they diverge:
    // `filter_expr` remains the planner-owned semantic WHERE expression,
    // while `residual_filter_expr` and `residual_filter_predicate` describe the
    // explicit runtime residual state that still survives access planning.
    let mut node =
        crate::db::executor::explain::descriptor::shared::empty_execution_node_descriptor(
            ExplainExecutionNodeType::ResidualFilter,
            execution_mode,
        );
    node.predicate_pushdown = access_strategy.and_then(pushdown_predicate_from_access_strategy);
    node.filter_expr = filter_expr;
    node.residual_filter_expr = residual_filter_expr;
    node.residual_filter_predicate = explain_predicate;
    node.node_properties.insert(
        property_keys::RESIDUAL_FILTER_SHAPE,
        Value::from(residual_filter_shape.label()),
    );

    vec![node]
}

pub(in crate::db::executor::explain::descriptor) fn explain_filter_expr_for_plan(
    plan: &AccessPlannedQuery,
) -> Option<String> {
    plan.scalar_plan()
        .filter_expr
        .as_ref()
        .map(render_scalar_filter_expr_plan_label)
}

pub(in crate::db::executor::explain::descriptor) fn explain_residual_filter_expr_for_plan(
    plan: &AccessPlannedQuery,
) -> Option<String> {
    plan.residual_filter_expr()
        .map(render_scalar_filter_expr_plan_label)
}

pub(in crate::db::executor::explain::descriptor) fn execution_preparation_predicate_index_capability(
    execution_preparation: &ExecutionPreparation,
) -> Option<IndexPredicateCapability> {
    execution_preparation
        .predicate_capability_profile()
        .map(PredicateCapabilityProfile::index)
}

pub(in crate::db::executor::explain::descriptor) const fn predicate_index_capability_label(
    capability: IndexPredicateCapability,
) -> &'static str {
    match capability {
        IndexPredicateCapability::FullyIndexable => "fully_indexable",
        IndexPredicateCapability::PartiallyIndexable => "partially_indexable",
        IndexPredicateCapability::RequiresFullScan => "requires_full_scan",
    }
}

fn pushdown_predicate_from_access_strategy(access: &ExplainAccessRoute) -> Option<String> {
    project_explain_access_path(access, &mut ExplainAccessPushdownPredicateProjection)
}

///
/// ExplainAccessPushdownPredicateProjection
///
/// Shared EXPLAIN-side pushdown text projection over canonical explain-access
/// DTOs. This keeps executor explain predicate wording on the same access walk
/// contract instead of rebuilding another local `ExplainAccessPath` ladder.
///

struct ExplainAccessPushdownPredicateProjection;

impl AccessPlanProjection<Value> for ExplainAccessPushdownPredicateProjection {
    type Output = Option<String>;

    fn by_key(&mut self, _key: &Value) -> Self::Output {
        None
    }

    fn by_keys(&mut self, _keys: &[Value]) -> Self::Output {
        None
    }

    fn key_range(&mut self, _start: &Value, _end: &Value) -> Self::Output {
        None
    }

    fn index_prefix<'a>(
        &mut self,
        _index_name: &str,
        index_fields: impl ExactSizeIterator<Item = &'a str> + Clone,
        prefix_len: usize,
        values: &[Value],
    ) -> Self::Output {
        prefix_predicate_text(index_fields, values, prefix_len)
    }

    fn index_multi_lookup<'a>(
        &mut self,
        _index_name: &str,
        mut index_fields: impl ExactSizeIterator<Item = &'a str> + Clone,
        values: &[Value],
    ) -> Self::Output {
        let field = index_fields.next()?;
        if values.is_empty() {
            None
        } else {
            Some(format!("{field} IN {values:?}"))
        }
    }

    fn index_branch_set<'a>(
        &mut self,
        _index_name: &str,
        mut index_fields: impl ExactSizeIterator<Item = &'a str> + Clone,
        fixed_values: &[Value],
        branch_values: &[Value],
    ) -> Self::Output {
        let mut parts = Vec::new();
        if let Some(prefix) =
            prefix_predicate_text(index_fields.clone(), fixed_values, fixed_values.len())
        {
            parts.push(prefix);
        }
        if let Some(field) = index_fields.nth(fixed_values.len())
            && !branch_values.is_empty()
        {
            parts.push(format!("{field} IN {branch_values:?}"));
        }

        (!parts.is_empty()).then(|| parts.join(" AND "))
    }

    fn index_range<'a>(
        &mut self,
        _index_name: &str,
        index_fields: impl ExactSizeIterator<Item = &'a str> + Clone,
        prefix_len: usize,
        prefix: &[Value],
        lower: &Bound<Value>,
        upper: &Bound<Value>,
    ) -> Self::Output {
        index_range_pushdown_predicate_text(index_fields, prefix_len, prefix, lower, upper)
    }

    fn full_scan(&mut self) -> Self::Output {
        None
    }

    fn union<T>(
        &mut self,
        _children: &[T],
        _project: impl Fn(&T, &mut Self) -> Self::Output,
    ) -> Self::Output {
        None
    }

    fn intersection<T>(
        &mut self,
        _children: &[T],
        _project: impl Fn(&T, &mut Self) -> Self::Output,
    ) -> Self::Output {
        None
    }
}

fn prefix_predicate_text<'a>(
    fields: impl ExactSizeIterator<Item = &'a str> + Clone,
    values: &[Value],
    prefix_len: usize,
) -> Option<String> {
    let applied_len = prefix_len.min(fields.len()).min(values.len());
    if applied_len == 0 {
        return None;
    }

    let mut out = String::new();
    for (idx, (field, value)) in fields.zip(values).take(applied_len).enumerate() {
        if idx > 0 {
            out.push_str(" AND ");
        }
        let _ = write!(out, "{field}={value:?}");
    }

    Some(out)
}

fn index_range_pushdown_predicate_text<'a>(
    mut fields: impl ExactSizeIterator<Item = &'a str> + Clone,
    prefix_len: usize,
    prefix: &[Value],
    lower: &Bound<Value>,
    upper: &Bound<Value>,
) -> Option<String> {
    let mut out = String::new();
    if let Some(prefix_text) = prefix_predicate_text(fields.clone(), prefix, prefix_len) {
        out.push_str(&prefix_text);
    }

    let range_field = fields.nth(prefix_len).unwrap_or("index_range");
    match lower {
        Bound::Included(value) => {
            if !out.is_empty() {
                out.push_str(" AND ");
            }
            let _ = write!(out, "{range_field}>={value:?}");
        }
        Bound::Excluded(value) => {
            if !out.is_empty() {
                out.push_str(" AND ");
            }
            let _ = write!(out, "{range_field}>{value:?}");
        }
        Bound::Unbounded => {}
    }
    match upper {
        Bound::Included(value) => {
            if !out.is_empty() {
                out.push_str(" AND ");
            }
            let _ = write!(out, "{range_field}<={value:?}");
        }
        Bound::Excluded(value) => {
            if !out.is_empty() {
                out.push_str(" AND ");
            }
            let _ = write!(out, "{range_field}<{value:?}");
        }
        Bound::Unbounded => {}
    }

    if out.is_empty() { None } else { Some(out) }
}

pub(in crate::db::executor::explain::descriptor) fn explain_predicate_for_plan(
    plan: &AccessPlannedQuery,
    work: &PreparationWork<'_>,
) -> Result<Option<ExplainPredicate>, QueryError> {
    plan.effective_execution_predicate()
        .as_deref()
        .map(|predicate| ExplainPredicate::from_predicate(predicate, work))
        .transpose()
}

// Return whether one scalar aggregate terminal can remain index-only under the
// current plan and executor preparation contracts.
pub(in crate::db::executor::explain::descriptor) fn aggregate_covering_projection_for_terminal(
    plan: &AccessPlannedQuery,
    aggregation: AggregateKind,
    execution_preparation: &ExecutionPreparation,
) -> bool {
    let strict_predicate_compatible = crate::db::query::plan::covering_strict_predicate_compatible(
        plan,
        execution_preparation_predicate_index_capability(execution_preparation),
    );

    if aggregation.supports_covering_existing_rows_terminal() {
        index_covering_existing_rows_terminal_eligible(plan, strict_predicate_compatible)
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::explain_predicate_for_plan;
    use crate::db::{
        Predicate, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        executor::explain::descriptor::shared::PredicateStageObservability,
        predicate::MissingRowPolicy,
        query::{
            plan::{AccessPlannedQuery, LogicalPlan, ResidualFilterShape},
            preparation::PreparationWork,
        },
    };
    use crate::value::Value;
    use icydb_diagnostic_code::{
        DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    };

    #[test]
    fn execution_predicate_projection_uses_request_budget_without_identity_copy() {
        let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
        let LogicalPlan::Scalar(scalar) = &mut plan.logical else {
            unreachable!()
        };
        scalar.predicate = Some(Predicate::eq("abc".into(), Value::Text("payload".into())));
        let root = RequestExecutionRoot::new_for_tests(
            HardExecutionBudget::uniform_for_tests(
                16_000_000,
                HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
            )
            .with_limit_for_tests(Resource::TemporaryBytes, 10),
        );
        let run = || {
            PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
                explain_predicate_for_plan(&plan, work)
            })
        };
        assert!(run().unwrap().is_some());
        assert_eq!(root.observed(Resource::TemporaryBytes), 10);
        assert_eq!(root.observed(Resource::NestedValueSteps), 1);
        assert!(run().is_err());
        assert_eq!(root.observed(Resource::RowsVisited), 0);
    }

    #[test]
    fn predicate_stage_observability_prefers_strict_prefilter() {
        assert_eq!(
            PredicateStageObservability::from_parts(
                true,
                ResidualFilterShape::ExpressionAndPredicate,
            ),
            PredicateStageObservability::IndexPrefilterStrictAllOrNone,
            "route-owned strict prefilter stage should win over residual labels",
        );
    }

    #[test]
    fn predicate_stage_observability_reports_residual_and_absent_shapes() {
        assert_eq!(
            PredicateStageObservability::from_parts(false, ResidualFilterShape::Expression).label(),
            "residual_runtime",
        );
        assert_eq!(
            PredicateStageObservability::from_parts(false, ResidualFilterShape::Absent).label(),
            "none",
        );
    }
}
