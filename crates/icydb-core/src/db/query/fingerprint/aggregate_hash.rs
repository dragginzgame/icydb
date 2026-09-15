//! Module: query::fingerprint::aggregate_hash
//! Responsibility: grouped aggregate structural hash encoding.
//! Does not own: explain projection assembly or plan profile ordering.
//! Boundary: semantic grouped aggregate hash bytes independent from explain-only metadata.

#[cfg(test)]
mod tests;

use crate::{
    db::query::{
        construction::ConstructionBudget,
        fingerprint::hash_sections::{write_expr_label, write_str, write_tag},
        plan::GroupAggregateSpec,
    },
    error::InternalError,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
use sha2::Sha256;

const GROUP_AGGREGATE_STRUCTURAL_FINGERPRINT_TAG: u8 = 0x01;
const AGGREGATE_TARGET_ABSENT_TAG: u8 = 0x00;
const AGGREGATE_TARGET_PRESENT_TAG: u8 = 0x01;
const AGGREGATE_DISTINCT_TAG: u8 = 0x02;
const AGGREGATE_NON_DISTINCT_TAG: u8 = 0x03;
const AGGREGATE_INPUT_EXPR_PRESENT_TAG: u8 = 0x04;
const AGGREGATE_FILTER_EXPR_PRESENT_TAG: u8 = 0x05;
const AGGREGATE_FILTER_EXPR_ABSENT_TAG: u8 = 0x06;

/// Hash the planned aggregate directly, with at most one rendered operand alive.
/// Preserve the current kind/target/distinct/input/filter framing and use the
/// planner's semantic DISTINCT rule rather than normalizing a second hash DTO.
pub(in crate::db::query::fingerprint) fn hash_group_aggregate_structural_fingerprint(
    hasher: &mut Sha256,
    aggregate: &GroupAggregateSpec,
    budget: &dyn ConstructionBudget,
) -> Result<(), InternalError> {
    budget.charge(Resource::PredicateExpressionSteps, 1)?;
    write_tag(hasher, GROUP_AGGREGATE_STRUCTURAL_FINGERPRINT_TAG);
    write_tag(hasher, aggregate.kind().fingerprint_tag());
    match aggregate.target_field() {
        Some(field) => {
            write_tag(hasher, AGGREGATE_TARGET_PRESENT_TAG);
            budget.charge(Resource::PredicateExpressionSteps, field.len() as u64)?;
            write_str(hasher, field);
        }
        None => write_tag(hasher, AGGREGATE_TARGET_ABSENT_TAG),
    }
    write_tag(
        hasher,
        if aggregate.semantic_distinct() {
            AGGREGATE_DISTINCT_TAG
        } else {
            AGGREGATE_NON_DISTINCT_TAG
        },
    );
    if let Some(input_expr) = aggregate.input_expr() {
        write_tag(hasher, AGGREGATE_INPUT_EXPR_PRESENT_TAG);
        write_expr_label(hasher, input_expr, budget)?;
    }
    if let Some(filter_expr) = aggregate.filter_expr() {
        write_tag(hasher, AGGREGATE_FILTER_EXPR_PRESENT_TAG);
        write_expr_label(hasher, filter_expr, budget)?;
    } else {
        write_tag(hasher, AGGREGATE_FILTER_EXPR_ABSENT_TAG);
    }
    Ok(())
}
