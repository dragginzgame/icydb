use crate::{
    db::{
        executor::ExecutionPreparation,
        predicate::{IndexPredicateCapability, PredicateCapabilityProfile},
        query::{
            explain::{
                ExplainAccessPath as ExplainAccessRoute, ExplainExecutionMode,
                ExplainExecutionNodeDescriptor, ExplainExecutionNodeType, ExplainPredicate,
            },
            plan::{
                AccessPlannedQuery, AggregateKind, index_covering_existing_rows_terminal_eligible,
            },
        },
    },
    value::Value,
};
use std::{fmt::Write, ops::Bound};

pub(in crate::db::executor::explain::descriptor) fn predicate_stage_descriptors(
    explain_predicate: Option<ExplainPredicate>,
    access_strategy: Option<&ExplainAccessRoute>,
    strict_prefilter_compiled: bool,
    execution_mode: ExplainExecutionMode,
) -> Vec<ExplainExecutionNodeDescriptor> {
    let Some(explain_predicate) = explain_predicate else {
        return Vec::new();
    };

    if strict_prefilter_compiled {
        let mut node =
            crate::db::executor::explain::descriptor::shared::empty_execution_node_descriptor(
                ExplainExecutionNodeType::IndexPredicatePrefilter,
                execution_mode,
            );
        node.predicate_pushdown = Some("strict_all_or_none".to_string());
        let pushdown_predicate = access_strategy
            .and_then(pushdown_predicate_from_access_strategy)
            .unwrap_or_else(|| format!("{explain_predicate:?}"));
        node.node_properties
            .insert("pushdown", Value::from(pushdown_predicate));
        return vec![node];
    }

    let mut node =
        crate::db::executor::explain::descriptor::shared::empty_execution_node_descriptor(
            ExplainExecutionNodeType::ResidualPredicateFilter,
            execution_mode,
        );
    node.predicate_pushdown = access_strategy.and_then(pushdown_predicate_from_access_strategy);
    node.residual_predicate = Some(explain_predicate);

    vec![node]
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
    match access {
        ExplainAccessRoute::IndexPrefix {
            fields,
            prefix_len,
            values,
            ..
        } => prefix_predicate_text(fields, values, *prefix_len),
        ExplainAccessRoute::IndexRange {
            fields,
            prefix_len,
            prefix,
            lower,
            upper,
            ..
        } => index_range_pushdown_predicate_text(fields, *prefix_len, prefix, lower, upper),
        ExplainAccessRoute::IndexMultiLookup { fields, values, .. } => {
            let field = fields.first()?;
            if values.is_empty() {
                None
            } else {
                Some(format!("{field} IN {values:?}"))
            }
        }
        _ => None,
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
