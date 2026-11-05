use super::batch_override_releases_unique_membership;

#[test]
fn complete_batch_override_releases_moved_or_deleted_unique_membership() {
    assert!(batch_override_releases_unique_membership(true, false));
}

#[test]
fn complete_batch_override_preserves_final_unique_collision() {
    assert!(!batch_override_releases_unique_membership(true, true));
}

#[test]
fn committed_unique_membership_mismatch_is_not_silently_released() {
    assert!(!batch_override_releases_unique_membership(false, false));
}
