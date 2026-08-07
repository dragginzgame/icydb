//! Module: db::executor::diagnostics
//! Responsibility: executor-scoped execution-trace projection.
//! Does not own: explain rendering, metrics sink persistence, or route behavior.
//! Boundary: projects executor access and direction into diagnostics-owned trace types.

#[cfg(all(test, feature = "sql"))]
mod node;

use crate::db::{
    access::{AccessPathKind, AccessPlan},
    diagnostics::ExecutionAccessPathVariant,
    direction::Direction,
    query::plan::OrderDirection,
};
#[cfg(feature = "diagnostics")]
use crate::{
    db::{
        diagnostics::{RequestDiagnosticAccessPath, RequestQueryPlanEvidence},
        predicate::{CompareOp, Predicate},
    },
    value::hash_value,
};

pub(in crate::db) use crate::db::diagnostics::ExecutionOptimization;
pub(in crate::db::executor) use crate::db::diagnostics::ExecutionTrace;

/// Build one execution trace from executor-owned access and route state.
#[must_use]
pub(in crate::db::executor) fn execution_trace_for_access<K>(
    access: &AccessPlan<K>,
    direction: Direction,
    continuation_applied: bool,
) -> ExecutionTrace {
    ExecutionTrace::new_from_variant(
        execution_access_path_variant(access),
        execution_order_direction(direction),
        continuation_applied,
    )
}

// Keep planner/executor access-shape interpretation on the executor side of
// the diagnostics boundary; diagnostics only stores the projected variant.
fn execution_access_path_variant<K>(access: &AccessPlan<K>) -> ExecutionAccessPathVariant {
    match access {
        AccessPlan::Path(path) => match path.kind() {
            AccessPathKind::ByKey => ExecutionAccessPathVariant::ByKey,
            AccessPathKind::ByKeys => ExecutionAccessPathVariant::ByKeys,
            AccessPathKind::KeyRange => ExecutionAccessPathVariant::KeyRange,
            AccessPathKind::IndexPrefix => ExecutionAccessPathVariant::IndexPrefix,
            AccessPathKind::IndexMultiLookup => ExecutionAccessPathVariant::IndexMultiLookup,
            AccessPathKind::IndexBranchSet => ExecutionAccessPathVariant::IndexBranchSet,
            AccessPathKind::IndexRange => ExecutionAccessPathVariant::IndexRange,
            AccessPathKind::FullScan => ExecutionAccessPathVariant::FullScan,
        },
        AccessPlan::Union(_) => ExecutionAccessPathVariant::Union,
        AccessPlan::Intersection(_) => ExecutionAccessPathVariant::Intersection,
    }
}

/// Project one prepared plan into bounded request-diagnostic evidence.
#[cfg(feature = "diagnostics")]
pub(in crate::db) fn request_query_plan_evidence(
    plan: &crate::db::executor::SharedPreparedExecutionPlan,
) -> RequestQueryPlanEvidence {
    let logical = plan.logical_plan();
    let access = &logical.access;
    let selected_index = access.selected_index_contract();
    let residual = logical.effective_execution_predicate();
    let mut residual_fields = Vec::new();
    if let Some(predicate) = residual.as_ref() {
        collect_predicate_fields(predicate, &mut residual_fields);
    }

    let mut equality_fields = Vec::new();
    if let Some(predicate) = logical.scalar_plan().predicate.as_ref() {
        collect_conjunctive_equality_fields(predicate, &mut equality_fields);
    }
    let selected_index_fields = selected_index
        .as_ref()
        .map(|index| {
            (0..index.key_arity())
                .filter_map(|slot| index.key_field_at(slot))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let compound_index_candidate = useful_compound_index_candidate(
        selected_index_fields,
        equality_fields.as_slice(),
        residual_fields.as_slice(),
    );

    let exact_key_hashes = access
        .as_by_key_path()
        .into_iter()
        .chain(access.as_by_keys_path().into_iter().flatten())
        .filter_map(|value| hash_value(value).ok())
        .collect::<Vec<_>>();

    RequestQueryPlanEvidence::bounded(
        plan.execution_shape_fingerprint_prefix(),
        plan.authority_ref().entity_path(),
        request_access_path_variant(access),
        selected_index
            .as_ref()
            .map(crate::db::access::SemanticIndexAccessContract::name),
        residual_fields,
        compound_index_candidate,
        exact_key_hashes,
    )
}

#[cfg(feature = "diagnostics")]
fn useful_compound_index_candidate<'a>(
    mut selected_index_fields: Vec<&'a str>,
    equality_fields: &[&'a str],
    residual_fields: &[&'a str],
) -> Vec<&'a str> {
    let selected_field_count = selected_index_fields.len();
    for field in equality_fields {
        if residual_fields.contains(field) && !selected_index_fields.contains(field) {
            selected_index_fields.push(field);
        }
    }
    if selected_index_fields.len() < 2 || selected_index_fields.len() == selected_field_count {
        selected_index_fields.clear();
    }
    selected_index_fields
}

#[cfg(feature = "diagnostics")]
fn request_access_path_variant<K>(access: &AccessPlan<K>) -> RequestDiagnosticAccessPath {
    match access {
        AccessPlan::Path(path) => match path.kind() {
            AccessPathKind::ByKey => RequestDiagnosticAccessPath::ByKey,
            AccessPathKind::ByKeys => RequestDiagnosticAccessPath::ByKeys,
            AccessPathKind::KeyRange => RequestDiagnosticAccessPath::KeyRange,
            AccessPathKind::IndexPrefix => RequestDiagnosticAccessPath::IndexPrefix,
            AccessPathKind::IndexMultiLookup => RequestDiagnosticAccessPath::IndexMultiLookup,
            AccessPathKind::IndexBranchSet => RequestDiagnosticAccessPath::IndexBranchSet,
            AccessPathKind::IndexRange => RequestDiagnosticAccessPath::IndexRange,
            AccessPathKind::FullScan => RequestDiagnosticAccessPath::FullScan,
        },
        AccessPlan::Union(_) => RequestDiagnosticAccessPath::Union,
        AccessPlan::Intersection(_) => RequestDiagnosticAccessPath::Intersection,
    }
}

#[cfg(feature = "diagnostics")]
fn collect_conjunctive_equality_fields<'a>(predicate: &'a Predicate, fields: &mut Vec<&'a str>) {
    match predicate {
        Predicate::And(children) => {
            for child in children {
                collect_conjunctive_equality_fields(child, fields);
            }
        }
        Predicate::Compare(compare) if compare.op() == CompareOp::Eq => {
            push_unique_field(fields, compare.field());
        }
        Predicate::True
        | Predicate::False
        | Predicate::Or(_)
        | Predicate::Not(_)
        | Predicate::Compare(_)
        | Predicate::CompareFields(_)
        | Predicate::IsNull { .. }
        | Predicate::IsNotNull { .. }
        | Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. }
        | Predicate::TextContains { .. }
        | Predicate::TextContainsCi { .. } => {}
    }
}

#[cfg(feature = "diagnostics")]
fn collect_predicate_fields<'a>(predicate: &'a Predicate, fields: &mut Vec<&'a str>) {
    match predicate {
        Predicate::And(children) | Predicate::Or(children) => {
            for child in children {
                collect_predicate_fields(child, fields);
            }
        }
        Predicate::Not(child) => collect_predicate_fields(child, fields),
        Predicate::Compare(compare) => push_unique_field(fields, compare.field()),
        Predicate::CompareFields(compare) => {
            push_unique_field(fields, compare.left_field());
            push_unique_field(fields, compare.right_field());
        }
        Predicate::IsNull { field }
        | Predicate::IsNotNull { field }
        | Predicate::IsMissing { field }
        | Predicate::IsEmpty { field }
        | Predicate::IsNotEmpty { field }
        | Predicate::TextContains { field, .. }
        | Predicate::TextContainsCi { field, .. } => push_unique_field(fields, field),
        Predicate::True | Predicate::False => {}
    }
}

#[cfg(feature = "diagnostics")]
fn push_unique_field<'a>(fields: &mut Vec<&'a str>, field: &'a str) {
    if !fields.contains(&field) {
        fields.push(field);
    }
}

// Runtime scan direction and diagnostic order direction are distinct enum
// surfaces, so the executor performs the mechanical projection before tracing.
const fn execution_order_direction(direction: Direction) -> OrderDirection {
    match direction {
        Direction::Asc => OrderDirection::Asc,
        Direction::Desc => OrderDirection::Desc,
    }
}

#[cfg(all(test, feature = "diagnostics"))]
mod request_tests {
    use super::*;

    #[test]
    fn residual_equality_extends_selected_index_prefix() {
        assert_eq!(
            useful_compound_index_candidate(
                vec!["collection_id"],
                &["collection_id", "stage"],
                &["stage"],
            ),
            vec!["collection_id", "stage"],
        );
    }

    #[test]
    fn unrelated_or_duplicate_residuals_do_not_invent_compound_candidates() {
        assert!(
            useful_compound_index_candidate(vec!["collection_id"], &["collection_id"], &["stage"])
                .is_empty(),
        );
        assert!(
            useful_compound_index_candidate(
                vec!["collection_id"],
                &["collection_id"],
                &["collection_id"],
            )
            .is_empty(),
        );
        assert!(
            useful_compound_index_candidate(
                vec!["collection_id", "stage"],
                &["collection_id", "stage"],
                &["stage"],
            )
            .is_empty(),
        );
    }
}
