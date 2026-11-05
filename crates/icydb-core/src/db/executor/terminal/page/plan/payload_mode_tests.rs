use super::*;

#[test]
fn retained_projection_uses_compact_slots_independently_of_cursor_emission() {
    let layout = RetainedSlotLayout::compile(8, vec![1, 4]);

    assert_eq!(
        select_kernel_row_payload_mode(true, Some(&layout)),
        KernelRowPayloadMode::SlotsOnly,
    );
    assert_eq!(
        select_cursorless_short_path_payload_mode(true, Some(&layout)),
        KernelRowPayloadMode::SlotsOnly,
    );
}

#[test]
fn non_projection_consumers_keep_required_raw_row_modes() {
    let layout = RetainedSlotLayout::compile(8, vec![1, 4]);

    assert_eq!(
        select_kernel_row_payload_mode(false, Some(&layout)),
        KernelRowPayloadMode::FullRowRetained,
    );
    assert_eq!(
        select_kernel_row_payload_mode(false, None),
        KernelRowPayloadMode::DataRowOnly,
    );
}
