//! Local insertion checks distinguish budget rejection from duplicate slots.

use super::*;
use crate::db::{
    RequestExecutionRoot,
    executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
};
use icydb_diagnostic_code::{DiagnosticExecutionLane, DiagnosticFactTag};

fn request(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

#[test]
fn rejected_slot_insertion_preserves_existing_entries_and_capacity() {
    for (resource, limit) in [
        (Resource::PredicateExpressionSteps, 2),
        (Resource::TemporaryBytes, 0),
    ] {
        let root = request(resource, limit);
        let mut slots = vec![2, 4];
        let (pointer, capacity) = (slots.as_ptr(), slots.capacity());
        let error =
            PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
                insert_projection_slot(&mut slots, 3, work)
            })
            .unwrap_err();
        assert!(
            error
                .diagnostic_facts()
                .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
        );
        assert_eq!(slots, [2, 4]);
        assert_eq!((slots.as_ptr(), slots.capacity()), (pointer, capacity));
    }
}

#[test]
fn duplicate_slots_need_comparisons_but_no_new_backing() {
    let root = request(Resource::TemporaryBytes, 0);
    let mut slots = vec![2, 4];
    PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
        insert_projection_slot(&mut slots, 2, work)
    })
    .unwrap();
    assert_eq!(slots, [2, 4]);
    assert_eq!(root.observed(Resource::PredicateExpressionSteps), 2);
    assert_eq!(root.observed(Resource::TemporaryBytes), 0);
}
