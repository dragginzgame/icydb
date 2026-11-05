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
