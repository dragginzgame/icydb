//! Test-only constructors and adapters for the parent module.

use super::*;

impl RowView {
    /// Build one retained row fixture from slot-indexed values.
    #[must_use]
    pub(in crate::db::executor) fn new(slots: Vec<Option<Value>>) -> Self {
        // Keep fixture field indices while storing only populated values under
        // the same layout used by grouped ingest.
        let required_slots = slots
            .iter()
            .enumerate()
            .filter_map(|(slot, value)| value.as_ref().map(|_| slot))
            .collect();
        let layout = RetainedSlotLayout::compile(slots.len(), required_slots);
        let values = slots.into_iter().flatten().map(Some).collect();

        Self::from_retained_slots(RetainedSlotRow::from_indexed_values(&layout, values))
    }
}
