use super::{find_matching_group_index_in_bucket, group_key_matches_single_group_value};
use crate::{db::executor::group::GroupKey, error::InternalError, value::Value};

#[test]
fn bucket_lookup_preserves_matching_absence_and_typed_failure() {
    let keys = [1, 2]
        .map(|value| GroupKey::from_single_canonical_group_value(Value::Nat64(value)).unwrap());
    for (bucket, target, expected) in [
        (&[0, 1][..], 2, Some(1)),
        (&[1, 0][..], 1, Some(0)),
        (&[0, 1][..], 3, None),
        (&[][..], 1, None),
    ] {
        let found = find_matching_group_index_in_bucket(
            bucket,
            |index| keys.get(index),
            |key| group_key_matches_single_group_value(key, &Value::Nat64(target)),
        )
        .unwrap();
        assert_eq!(found, expected);
    }

    let missing = find_matching_group_index_in_bucket(
        &[2],
        |index| keys.get(index),
        |_| unreachable!("missing keys must reject before comparison"),
    )
    .unwrap_err();
    assert_eq!(
        missing.diagnostic(),
        InternalError::query_executor_invariant().diagnostic()
    );
    let comparison_error = InternalError::planner_executor_invariant();
    let expected = comparison_error.diagnostic();
    let error = find_matching_group_index_in_bucket(
        &[0],
        |index| keys.get(index),
        |_| Err(InternalError::planner_executor_invariant()),
    )
    .unwrap_err();
    assert_eq!(error.diagnostic(), expected);
}
