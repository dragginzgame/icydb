//! Module: diagnostics::sql_structural
//! Responsibility: query-scoped structural work counters for SQL performance attribution.
//! Does not own: query planning, predicate normalization, or runtime policy.
//! Boundary: diagnostics-enabled SQL entrypoints reset and consume one thread-local sample.

use std::cell::Cell;

use candid::CandidType;
use serde::Deserialize;

///
/// SqlStructuralWorkAttribution
///
/// Structural work facts needed to distinguish membership/prefix planning
/// costs from physical compound-range work. These counters are diagnostics
/// only and describe work performed by one attributed SQL query.
///

#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SqlStructuralWorkAttribution {
    /// Compatible range conjunctions selected by the planner.
    pub range_conjunctions_examined: u64,
    /// Selected conjunctions that contributed a lower bound.
    pub range_lower_bounds_extracted: u64,
    /// Selected conjunctions that contributed an upper bound.
    pub range_upper_bounds_extracted: u64,
    /// Physical index-range children emitted while lowering access.
    pub range_physical_children_emitted: u64,
    /// Effective runtime predicate evaluations after access planning.
    pub residual_predicate_evaluations: u64,
    /// Membership values authored in the SQL surface.
    pub membership_authored_members: u64,
    /// Membership values retained after accepted normalization.
    pub membership_normalized_members: u64,
    /// Distinct values retained by canonical membership sets.
    pub membership_distinct_members: u64,
    /// Null members authored in SQL membership lists.
    pub membership_null_members: u64,
    /// Canonical membership-set passes performed.
    pub membership_canonicalization_passes: u64,
    /// Membership values revisited across canonicalization passes.
    pub membership_members_revisited: u64,
    /// Candidate prefix branches before canonical deduplication.
    pub prefix_branches_before_deduplication: u64,
    /// Candidate prefix branches after canonical deduplication.
    pub prefix_branches_after_deduplication: u64,
    /// Exclusion comparisons performed while pruning branches.
    pub prefix_exclusions_tested: u64,
    /// Candidate branches removed by explicit exclusions.
    pub prefix_exclusions_pruned: u64,
    /// Candidate branch sets admitted by the maintained cap.
    pub prefix_branch_cap_admissions: u64,
    /// Candidate branch sets rejected by the maintained cap.
    pub prefix_branch_cap_rejections: u64,
}

thread_local! {
    static SQL_STRUCTURAL_WORK: Cell<SqlStructuralWorkAttribution> =
        Cell::new(SqlStructuralWorkAttribution::default());
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn update(run: impl FnOnce(&mut SqlStructuralWorkAttribution)) {
    SQL_STRUCTURAL_WORK.with(|work| {
        let mut current = work.get();
        run(&mut current);
        work.set(current);
    });
}

pub(in crate::db) fn begin_sql_structural_work_attribution() {
    SQL_STRUCTURAL_WORK.with(|work| work.set(SqlStructuralWorkAttribution::default()));
}

pub(in crate::db) fn finish_sql_structural_work_attribution() -> SqlStructuralWorkAttribution {
    SQL_STRUCTURAL_WORK.with(|work| {
        let current = work.get();
        work.set(SqlStructuralWorkAttribution::default());
        current
    })
}

pub(in crate::db) fn record_sql_range_conjunction(
    lower_bound_extracted: bool,
    upper_bound_extracted: bool,
) {
    update(|work| {
        work.range_conjunctions_examined = work.range_conjunctions_examined.saturating_add(1);
        work.range_lower_bounds_extracted = work
            .range_lower_bounds_extracted
            .saturating_add(u64::from(lower_bound_extracted));
        work.range_upper_bounds_extracted = work
            .range_upper_bounds_extracted
            .saturating_add(u64::from(upper_bound_extracted));
    });
}

pub(in crate::db) fn record_sql_range_physical_child() {
    update(|work| {
        work.range_physical_children_emitted =
            work.range_physical_children_emitted.saturating_add(1);
    });
}

pub(in crate::db) fn record_sql_residual_predicate_evaluation() {
    update(|work| {
        work.residual_predicate_evaluations = work.residual_predicate_evaluations.saturating_add(1);
    });
}

pub(in crate::db) fn record_sql_membership_authored(values: &[crate::value::Value]) {
    update(|work| {
        work.membership_authored_members = work
            .membership_authored_members
            .saturating_add(usize_to_u64(values.len()));
        work.membership_null_members = work.membership_null_members.saturating_add(usize_to_u64(
            values
                .iter()
                .filter(|value| matches!(value, crate::value::Value::Null))
                .count(),
        ));
    });
}

pub(in crate::db) fn record_sql_membership_normalized(values: &[crate::value::Value]) {
    update(|work| {
        let count = usize_to_u64(values.len());
        work.membership_normalized_members =
            work.membership_normalized_members.saturating_add(count);
        work.membership_distinct_members = work.membership_distinct_members.saturating_add(count);
    });
}

pub(in crate::db) fn record_sql_membership_canonicalization(member_count: usize) {
    update(|work| {
        work.membership_canonicalization_passes =
            work.membership_canonicalization_passes.saturating_add(1);
        work.membership_members_revisited = work
            .membership_members_revisited
            .saturating_add(usize_to_u64(member_count));
    });
}

pub(in crate::db) fn record_sql_prefix_branch_deduplication(
    branches_before: usize,
    branches_after: usize,
) {
    update(|work| {
        work.prefix_branches_before_deduplication = work
            .prefix_branches_before_deduplication
            .saturating_add(usize_to_u64(branches_before));
        work.prefix_branches_after_deduplication = work
            .prefix_branches_after_deduplication
            .saturating_add(usize_to_u64(branches_after));
    });
}

pub(in crate::db) fn record_sql_prefix_exclusion_pruning(tested: usize, pruned: usize) {
    update(|work| {
        work.prefix_exclusions_tested = work
            .prefix_exclusions_tested
            .saturating_add(usize_to_u64(tested));
        work.prefix_exclusions_pruned = work
            .prefix_exclusions_pruned
            .saturating_add(usize_to_u64(pruned));
    });
}

pub(in crate::db) fn record_sql_prefix_branch_cap(admitted: bool) {
    update(|work| {
        if admitted {
            work.prefix_branch_cap_admissions = work.prefix_branch_cap_admissions.saturating_add(1);
        } else {
            work.prefix_branch_cap_rejections = work.prefix_branch_cap_rejections.saturating_add(1);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;

    #[test]
    fn structural_work_scope_is_exact_and_consuming() {
        begin_sql_structural_work_attribution();
        record_sql_membership_authored(&[Value::Nat64(1), Value::Null, Value::Nat64(1)]);
        record_sql_membership_canonicalization(3);
        record_sql_membership_normalized(&[Value::Null, Value::Nat64(1)]);
        record_sql_prefix_branch_deduplication(3, 2);
        record_sql_prefix_exclusion_pruning(4, 1);
        record_sql_prefix_branch_cap(true);
        record_sql_range_conjunction(true, true);
        record_sql_range_physical_child();
        record_sql_residual_predicate_evaluation();

        let work = finish_sql_structural_work_attribution();
        assert_eq!(work.membership_authored_members, 3);
        assert_eq!(work.membership_normalized_members, 2);
        assert_eq!(work.membership_distinct_members, 2);
        assert_eq!(work.membership_null_members, 1);
        assert_eq!(work.membership_canonicalization_passes, 1);
        assert_eq!(work.membership_members_revisited, 3);
        assert_eq!(work.prefix_branches_before_deduplication, 3);
        assert_eq!(work.prefix_branches_after_deduplication, 2);
        assert_eq!(work.prefix_exclusions_tested, 4);
        assert_eq!(work.prefix_exclusions_pruned, 1);
        assert_eq!(work.prefix_branch_cap_admissions, 1);
        assert_eq!(work.prefix_branch_cap_rejections, 0);
        assert_eq!(work.range_conjunctions_examined, 1);
        assert_eq!(work.range_lower_bounds_extracted, 1);
        assert_eq!(work.range_upper_bounds_extracted, 1);
        assert_eq!(work.range_physical_children_emitted, 1);
        assert_eq!(work.residual_predicate_evaluations, 1);
        assert_eq!(
            finish_sql_structural_work_attribution(),
            SqlStructuralWorkAttribution::default(),
        );
    }
}
