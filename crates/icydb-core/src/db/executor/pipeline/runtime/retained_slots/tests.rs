use super::{RetainedSlotRequirements, RetainedSlotValueMode};

#[test]
fn full_value_requirements_win_in_either_marking_order() {
    for length_first in [false, true] {
        let mut requirements = RetainedSlotRequirements::new(4);
        if length_first {
            requirements.mark_slot_octet_length(2);
        }
        requirements.mark_slots([2, 0]);
        requirements.mark_slot_octet_length(2);
        requirements.mark_slot(99);
        requirements.mark_slot_octet_length(99);
        let (slots, modes) = requirements.into_slots_and_value_modes();
        assert_eq!(slots, [0, 2]);
        assert!(modes.is_empty());
        assert_eq!(modes.capacity(), 0);
    }
}

#[test]
fn length_override_modes_align_with_sparse_sorted_slots() {
    use RetainedSlotValueMode::{Normal, ScalarOctetLength};

    for length_slot in [0, 2, 4] {
        let mut requirements = RetainedSlotRequirements::new(6);
        for slot in [4, 0, 2] {
            if slot == length_slot {
                requirements.mark_slot_octet_length(slot);
                requirements.mark_slot_octet_length(slot);
            } else {
                requirements.mark_slot(slot);
            }
        }
        let (slots, modes) = requirements.into_slots_and_value_modes();
        assert_eq!(slots, [0, 2, 4]);
        assert_eq!(modes.len(), slots.len());
        for (slot, mode) in slots.into_iter().zip(modes) {
            assert_eq!(
                mode,
                if slot == length_slot {
                    ScalarOctetLength
                } else {
                    Normal
                }
            );
        }
    }
    let (slots, modes) = RetainedSlotRequirements::new(0).into_slots_and_value_modes();
    assert!(slots.is_empty());
    assert_eq!(modes.capacity(), 0);
}
