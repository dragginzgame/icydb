//! Family selection consumes routes; eligibility and operand construction are separate.

use super::{choose_best_and_family_access, primary_key_child_access_candidate};
use crate::{
    db::{access::AccessPlan, query::plan::PlannedNonIndexAccessReason as Reason},
    value::Value,
};

#[test]
fn family_selection_preserves_priority_and_moves_the_winning_route() {
    // The selector treats family routes as opaque inputs. Distinct key routes
    // identify each input without duplicating accepted-index fixture machinery.
    for mask in 0u8..32 {
        for required_order in [false, true] {
            let routes: [Option<AccessPlan<Value>>; 5] = std::array::from_fn(|slot| {
                (mask & (1 << slot) != 0).then(|| AccessPlan::by_key(Value::Nat64(slot as u64 + 1)))
            });
            let expected_slot = if required_order && routes[0].is_some() {
                Some(0)
            } else {
                routes.iter().rposition(Option::is_some)
            };
            let expected_address = expected_slot
                .map(|slot| std::ptr::from_ref(routes[slot].as_ref().unwrap().as_path().unwrap()));
            let [primary, prefix, range, branch, intersection] = routes;
            let selected = choose_best_and_family_access(
                None,
                intersection,
                primary,
                range,
                branch,
                prefix,
                required_order,
            );
            let Some(slot) = expected_slot else {
                assert!(selected.is_none());
                continue;
            };
            let (access, reason) = selected.unwrap().into_access_and_non_index_reason();
            assert_eq!(
                access.as_by_key_path(),
                Some(&Value::Nat64(slot as u64 + 1))
            );
            assert_eq!(
                Some(std::ptr::from_ref(access.as_path().unwrap())),
                expected_address
            );
            assert_eq!(
                reason,
                match slot {
                    0 if required_order => Some(Reason::RequiredOrderPrimaryKeyRangePreferred),
                    0 => Some(Reason::PlannerPrimaryKeyRange),
                    4 => Some(Reason::PlannerExactIndexIntersection),
                    _ => None,
                }
            );
        }
    }
}

#[test]
fn child_priority_overrides_owned_family_routes() {
    for (children, expected_key, reason) in [
        (
            vec![AccessPlan::by_keys(Vec::new())],
            None,
            Reason::EmptyChildAccessPreferred,
        ),
        (
            vec![
                AccessPlan::by_key(Value::Nat64(1)),
                AccessPlan::by_key(Value::Nat64(2)),
            ],
            None,
            Reason::ConflictingPrimaryKeyChildrenAccessPreferred,
        ),
        (
            vec![AccessPlan::by_key(Value::Nat64(1))],
            Some(Value::Nat64(1)),
            Reason::SingletonPrimaryKeyChildAccessPreferred,
        ),
    ] {
        let family = || Some(AccessPlan::by_key(Value::Nat64(99)));
        let child_candidate = crate::db::query::preparation::with_preparation_work(|work| {
            primary_key_child_access_candidate(&children, work)
        })
        .unwrap();
        let (access, actual_reason) = choose_best_and_family_access(
            child_candidate,
            family(),
            family(),
            family(),
            family(),
            family(),
            true,
        )
        .unwrap()
        .into_access_and_non_index_reason();
        assert_eq!(actual_reason, Some(reason));
        if let Some(key) = expected_key {
            assert_eq!(access.as_by_key_path(), Some(&key));
        } else {
            assert!(access.is_explicit_empty());
        }
    }
}
