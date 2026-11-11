use super::*;

fn position(allocation: u8, sequence: u64) -> JournalOverlayPosition {
    JournalOverlayPosition::new(
        StoreAllocationIdentity::new(allocation.into(), "test::allocation"),
        JournalSequence::new(sequence),
    )
}

#[test]
fn positioned_metadata_preserves_newer_same_target_effects() {
    let mut metadata = PositionedOverlayMetadata::new();
    metadata
        .preflight_publish(&7, position(100, 1))
        .expect("first position should publish");
    metadata.publish_preflighted(7, position(100, 1));
    metadata
        .preflight_publish(&7, position(100, 2))
        .expect("later position should supersede");
    metadata.publish_preflighted(7, position(100, 2));

    let retirement = metadata
        .preflight_retirement(&7, position(100, 1))
        .expect("older retirement should preserve the newer effect");
    assert_eq!(retirement, PositionedOverlayRetirement::Superseded);
    metadata.retire_preflighted(&7, retirement);
    assert_eq!(metadata.len(), 1);

    let retirement = metadata
        .preflight_retirement(&7, position(100, 2))
        .expect("newest position should retire exactly");
    assert_eq!(retirement, PositionedOverlayRetirement::Exact);
    metadata.retire_preflighted(&7, retirement);
    assert_eq!(metadata.len(), 0);
}

#[test]
fn positioned_metadata_rejects_older_or_cross_allocation_publication() {
    let mut metadata = PositionedOverlayMetadata::new();
    metadata.publish_preflighted(7, position(100, 2));

    assert!(metadata.preflight_publish(&7, position(100, 1)).is_err());
    assert!(metadata.preflight_publish(&7, position(101, 3)).is_err());
}

#[test]
fn bulk_publication_preflights_every_distinct_target_before_effects() {
    let mut metadata = PositionedOverlayMetadata::new();
    metadata.publish_preflighted(7, position(100, 2));
    let before = metadata.positions.clone();

    assert_eq!(
        metadata
            .preflight_publication_keys([9, 7, 3, 9], position(100, 3))
            .unwrap(),
        vec![3, 7, 9]
    );
    // The first new target is valid, but a later retained target must refuse
    // stale or cross-allocation publication without publishing the prefix.
    for invalid in [position(100, 1), position(101, 3)] {
        assert!(
            metadata
                .preflight_publication_keys([3, 7, 9], invalid)
                .is_err()
        );
        assert_eq!(metadata.positions, before);
    }
}

#[test]
fn bulk_retirement_preserves_exact_superseded_and_missing_target_authority() {
    let mut metadata = PositionedOverlayMetadata::new();
    metadata.publish_preflighted(1, position(100, 1));
    metadata.publish_preflighted(3, position(100, 2));
    let before = metadata.positions.clone();

    assert_eq!(
        metadata
            .preflight_retirements([3, 1, 3], position(100, 1))
            .unwrap(),
        vec![
            (1, PositionedOverlayRetirement::Exact),
            (3, PositionedOverlayRetirement::Superseded),
        ]
    );
    // A missing later key must not retire the earlier admitted target.
    assert!(
        metadata
            .preflight_retirements([1, 2], position(100, 1))
            .is_err()
    );
    for invalid in [position(100, 3), position(101, 1)] {
        assert!(metadata.preflight_retirements([1, 3], invalid).is_err());
    }
    assert_eq!(metadata.positions, before);
}

#[test]
fn derived_index_classification_uses_value_presence_as_semantics() {
    assert_eq!(
        classify_derived_index_overlay(Some(&IndexEntryValue::presence())),
        OnlineOverlayDecision::IndexPositive,
    );
    assert_eq!(
        classify_derived_index_overlay(None),
        OnlineOverlayDecision::IndexTombstone,
    );
}
