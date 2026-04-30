use crate::{
    db::{
        executor::ExecutionPreparation,
        predicate::{IndexPredicateCapability, PredicateCapabilityProfile},
        query::{
            explain::{
                ExplainAccessPath as ExplainAccessRoute, ExplainExecutionMode,
                ExplainExecutionNodeDescriptor, ExplainExecutionNodeType, ExplainPredicate,
                explain_predicate_from_expr,
            },
            plan::{
                AccessPlanProjection, AccessPlannedQuery, AggregateKind,
                index_covering_existing_rows_terminal_eligible, project_explain_access_path,
                render_scalar_filter_expr_plan_label,
            },
        },
    },
    value::Value,
};
use std::{fmt::Write, ops::Bound};

pub(in crate::db::executor::explain::descriptor) fn predicate_stage_descriptors(
    filter_expr: Option<String>,
    residual_filter_expr: Option<String>,
    explain_predicate: Option<ExplainPredicate>,
    access_strategy: Option<&ExplainAccessRoute>,
    strict_prefilter_compiled: bool,
    execution_mode: ExplainExecutionMode,
) -> Vec<ExplainExecutionNodeDescriptor> {
    if !strict_prefilter_compiled && residual_filter_expr.is_none() && explain_predicate.is_none() {
        return Vec::new();
    }

    // Strict prefilters still describe one pushdown-only predicate stage. The
    // semantic filter expression is carried through for wording parity, but
    // there is no residual execution-stage predicate node in this case.
    if strict_prefilter_compiled {
        let mut node =
            crate::db::executor::explain::descriptor::shared::empty_execution_node_descriptor(
                ExplainExecutionNodeType::IndexPredicatePrefilter,
                execution_mode,
            );
        node.predicate_pushdown = Some("strict_all_or_none".to_string());
        node.filter_expr = filter_expr;
        let pushdown_predicate = access_strategy
            .and_then(pushdown_predicate_from_access_strategy)
            .unwrap_or_else(|| format!("{explain_predicate:?}"));
        node.node_properties
            .insert("pushdown", Value::from(pushdown_predicate));
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
    // Prefer the canonical residual predicate surface whenever the surviving
    // semantic filter still lowers onto the shared boolean predicate family.
    // This keeps searched CASE and its expanded boolean equivalent on the same
    // residual explain contract while still preserving `filter_expr` as the
    // semantic query-owned surface.
    plan.residual_filter_expr()
        .filter(|expr| explain_predicate_from_expr(expr).is_none())
        .map(render_scalar_filter_expr_plan_label)
}

pub(in crate::db::executor::explain::descriptor) fn execution_preparation_predicate_index_capability(
    execution_preparation: &ExecutionPreparation,
) -> Option<IndexPredicateCapability> {
    execution_preparation
        .predicate_capability_profile()
        .map(PredicateCapabilityProfile::index)
}

// Return a conservative explain-only predicate capability when the planner did
// not retain an execution-preparation predicate, but explain can still derive a
// canonical residual predicate from the surviving boolean expression tree.
pub(in crate::db::executor::explain::descriptor) fn fallback_explain_predicate_index_capability_for_plan(
    plan: &AccessPlannedQuery,
) -> Option<IndexPredicateCapability> {
    explain_predicate_for_plan(plan)
        .is_some()
        .then_some(IndexPredicateCapability::RequiresFullScan)
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

    fn index_prefix(
        &mut self,
        _index_name: &'static str,
        index_fields: &[&'static str],
        prefix_len: usize,
        values: &[Value],
    ) -> Self::Output {
        prefix_predicate_text(index_fields, values, prefix_len)
    }

    fn index_multi_lookup(
        &mut self,
        _index_name: &'static str,
        index_fields: &[&'static str],
        values: &[Value],
    ) -> Self::Output {
        let field = index_fields.first()?;
        if values.is_empty() {
            None
        } else {
            Some(format!("{field} IN {values:?}"))
        }
    }

    fn index_range(
        &mut self,
        _index_name: &'static str,
        index_fields: &[&'static str],
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

    fn union(&mut self, _children: Vec<Self::Output>) -> Self::Output {
        None
    }

    fn intersection(&mut self, _children: Vec<Self::Output>) -> Self::Output {
        None
    }
}

fn prefix_predicate_text(fields: &[&str], values: &[Value], prefix_len: usize) -> Option<String> {
    let applied_len = prefix_len.min(fields.len()).min(values.len());
    if applied_len == 0 {
        return None;
    }

    let mut out = String::new();
    for idx in 0..applied_len {
        if idx > 0 {
            out.push_str(" AND ");
        }
        let _ = write!(out, "{}={:?}", fields[idx], values[idx]);
    }

    Some(out)
}

fn index_range_pushdown_predicate_text(
    fields: &[&str],
    prefix_len: usize,
    prefix: &[Value],
    lower: &Bound<Value>,
    upper: &Bound<Value>,
) -> Option<String> {
    let mut out = String::new();
    if let Some(prefix_text) = prefix_predicate_text(fields, prefix, prefix_len) {
        out.push_str(&prefix_text);
    }

    let range_field = fields.get(prefix_len).copied().unwrap_or("index_range");
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
) -> Option<ExplainPredicate> {
    plan.effective_execution_predicate()
        .as_ref()
        .map(ExplainPredicate::from_predicate)
        .or_else(|| {
            plan.residual_filter_expr()
                .and_then(explain_predicate_from_expr)
        })
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
