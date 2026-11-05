use super::*;
use crate::{
    db::{
        index::{IndexId, IndexKeyKind},
        key_taxonomy::PrimaryKeyComponent,
    },
    types::EntityTag,
};

#[test]
fn entry_admission_reserves_source_row_and_stops_without_consuming() {
    let key = IndexKey::new_from_components_with_primary_key_value(
        &IndexId::new(EntityTag::new(7), 0),
        IndexKeyKind::User,
        &[vec![1]],
        &PrimaryKeyComponent::Nat64(7).into(),
    )
    .unwrap()
    .to_raw()
    .unwrap();
    let value = IndexEntryValue::presence_only();
    let entry_bytes = key.as_bytes().len() + value.len();
    let reserve = crate::db::codec::MAX_ROW_BYTES as usize;
    // Exercise the exact byte boundary and an independent entry-count
    // boundary. Neither refusal may advance the checkpoint or charge bytes.
    for limits in [
        DerivedInspectionLimits {
            decoded_bytes: entry_bytes + reserve,
            ..DerivedInspectionLimits::standard()
        },
        DerivedInspectionLimits {
            entries: 1,
            decoded_bytes: entry_bytes * 2 + reserve,
            ..DerivedInspectionLimits::standard()
        },
    ] {
        let checkpoint = PhysicalUnitCheckpoint::BeforeFirst;
        let mut page = DerivedPageAccumulator::new(checkpoint.clone());
        assert!(page.start_entry(&key, &value, limits).unwrap());
        assert_eq!(page.decoded_bytes, entry_bytes);
        assert!(!page.stopped);
        assert!(!page.start_entry(&key, &value, limits).unwrap());
        assert!(page.stopped);
        let result = page.finish(false).unwrap();
        assert_eq!(result.checkpoint, checkpoint);
        assert_eq!(result.entries_started, 1);
        assert_eq!(result.entries_completed, 0);
        assert_eq!(result.decoded_bytes, entry_bytes as u64);
        assert!(!result.exhausted);
    }
}

#[test]
fn source_budget_classifies_first_row_and_defers_later_overflow() {
    let limits = DerivedInspectionLimits::standard();
    let oversized = limits.decoded_bytes + 1;
    let mut first = DerivedPageAccumulator::new(PhysicalUnitCheckpoint::BeforeFirst);
    first.entries_started = 1;
    assert!(first.consume_source_row(oversized, limits).unwrap());
    assert_eq!(first.decoded_bytes, oversized);
    assert!(!first.stopped);

    let mut later = DerivedPageAccumulator::new(PhysicalUnitCheckpoint::BeforeFirst);
    later.entries_started = 2;
    later.decoded_bytes = 1;
    assert!(
        later
            .consume_source_row(limits.decoded_bytes - 1, limits)
            .unwrap()
    );
    assert_eq!(later.decoded_bytes, limits.decoded_bytes);
    assert!(!later.consume_source_row(1, limits).unwrap());
    assert_eq!(later.decoded_bytes, limits.decoded_bytes);
    assert!(later.stopped);
}
