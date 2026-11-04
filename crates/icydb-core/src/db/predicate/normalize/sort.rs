//! Admit canonical ordering before the infallible sort can allocate or encode.
//! Encoding and successful ordering remain with their existing owners.

use crate::{
    db::{
        predicate::{
            Predicate, encoding::raw_predicate_key_capacity,
            normalize::canonicalize_predicate_children_with_scratch,
        },
        resource::{budget::sort_comparison_allowance, construction::ConstructionBudget},
    },
    error::InternalError,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
use std::cell::Cell;

const RESOURCE_COUNT: usize = Resource::ALL.len();

// Admit each operand once and retain only its resource extents. Repeated
// comparator encoding then uses the largest extent in each resource, rather
// than charging every operand for every possible comparator invocation.
struct EncodingAttempt<'a> {
    work: &'a dyn ConstructionBudget,
    usage: Cell<[u64; RESOURCE_COUNT]>,
}

impl ConstructionBudget for EncodingAttempt<'_> {
    fn charge(&self, resource: Resource, amount: u64) -> Result<(), InternalError> {
        self.work.charge(resource, amount)?;
        let index = Resource::ALL
            .iter()
            .position(|candidate| *candidate == resource)
            .ok_or_else(InternalError::query_executor_invariant)?;
        let mut usage = self.usage.get();
        usage[index] = usage[index].saturating_add(amount);
        self.usage.set(usage);
        Ok(())
    }
}

pub(in crate::db::predicate::normalize) fn canonicalize_with_budget(
    children: &mut Vec<Predicate>,
    work: &dyn ConstructionBudget,
) -> Result<(), InternalError> {
    if children.len() < 2 {
        return Ok(());
    }
    let entries = children.len() as u64;
    let comparisons = sort_comparison_allowance(children.len());
    work.charge(Resource::SortEntries, entries)?;
    work.charge(Resource::SortComparisons, comparisons)?;
    work.charge(
        Resource::SortTemporaryBytes,
        entries.saturating_mul(size_of::<Predicate>() as u64),
    )?;
    let mut capacity = 0;
    let mut maximum = [0; RESOURCE_COUNT];
    for child in children.iter() {
        let encoding = EncodingAttempt {
            work,
            usage: Cell::new([0; RESOURCE_COUNT]),
        };
        capacity = capacity.max(raw_predicate_key_capacity(child, &encoding)?);
        for (maximum, used) in maximum.iter_mut().zip(encoding.usage.get()) {
            *maximum = (*maximum).max(used);
        }
    }
    // Two keys per comparison, plus the per-child admission above for adjacent
    // structural deduplication. No fallible charge runs inside the comparator.
    for (resource, amount) in Resource::ALL.iter().zip(maximum) {
        if amount != 0 {
            work.charge(
                *resource,
                amount.saturating_mul(comparisons.saturating_mul(2)),
            )?;
        }
    }
    // Preallocation prevents geometric key-buffer growth inside the comparator.
    // Raw-key admission also covers repeated coercion and map/member scratch.
    let mut left = work.vec_with_capacity(capacity)?;
    let mut right = work.vec_with_capacity(capacity)?;
    canonicalize_predicate_children_with_scratch(children, &mut left, &mut right);
    debug_assert!(left.len() <= capacity && right.len() <= capacity);
    Ok(())
}
