use crate::{db::executor::projection::ProjectionValidationRow, value::Value};
use std::sync::Arc;

///
/// RetainedSlotLayout
///
/// RetainedSlotLayout is the executor-owned shared slot lookup compiled once
/// for one slot-only execution shape.
/// Retained rows clone this layout handle so each row can stay compact while
/// still resolving slot reads in O(1) time.
///

#[derive(Clone, Debug)]
pub(in crate::db::executor) struct RetainedSlotLayout {
    data: Arc<RetainedSlotLayoutData>,
}

///
/// RetainedSlotLayoutData
///
/// Shared retained-slot metadata carried by one retained-slot layout handle.
/// It preserves the retained slot order plus the reverse slot-to-value-index
/// lookup so row decode does not rebuild either structure per row.
///

#[derive(Debug)]
struct RetainedSlotLayoutData {
    required_slots: Box<[usize]>,
    value_modes: Box<[RetainedSlotValueMode]>,
    slot_to_value_index: Box<[Option<usize>]>,
}

///
/// RetainedSlotValueMode
///
/// RetainedSlotValueMode describes how one retained slot value should be
/// materialized from raw row storage.
/// It lets projection-owned retained rows keep byte-length-only blob/text
/// projections cheap without changing the logical projection expression or
/// leaking expression details into retained-row consumers.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum RetainedSlotValueMode {
    Normal,
    ScalarOctetLength,
}

impl RetainedSlotLayout {
    /// Compile one retained-slot layout from one stable retained-slot list.
    #[must_use]
    pub(in crate::db::executor) fn compile(slot_count: usize, required_slots: Vec<usize>) -> Self {
        let value_modes = vec![RetainedSlotValueMode::Normal; required_slots.len()];

        Self::compile_with_value_modes(slot_count, required_slots, value_modes)
    }

    /// Compile one retained-slot layout from slots plus per-slot decode modes.
    #[must_use]
    pub(in crate::db::executor) fn compile_with_value_modes(
        slot_count: usize,
        required_slots: Vec<usize>,
        value_modes: Vec<RetainedSlotValueMode>,
    ) -> Self {
        debug_assert_eq!(required_slots.len(), value_modes.len());

        let mut slot_to_value_index = vec![None; slot_count];
        for (value_index, &slot) in required_slots.iter().enumerate() {
            if let Some(entry) = slot_to_value_index.get_mut(slot) {
                *entry = Some(value_index);
            }
        }

        Self {
            data: Arc::new(RetainedSlotLayoutData {
                required_slots: required_slots.into_boxed_slice(),
                value_modes: value_modes.into_boxed_slice(),
                slot_to_value_index: slot_to_value_index.into_boxed_slice(),
            }),
        }
    }

    /// Borrow the retained slots in the same stable order used by retained-row value storage.
    #[must_use]
    pub(in crate::db::executor) fn required_slots(&self) -> &[usize] {
        self.data.required_slots.as_ref()
    }

    /// Borrow the per-retained-slot materialization modes in layout order.
    #[must_use]
    pub(in crate::db::executor) fn value_modes(&self) -> &[RetainedSlotValueMode] {
        self.data.value_modes.as_ref()
    }

    /// Return whether any retained slot uses a non-standard materialization mode.
    #[must_use]
    pub(in crate::db::executor) fn has_value_mode_overrides(&self) -> bool {
        self.data
            .value_modes
            .iter()
            .any(|mode| *mode != RetainedSlotValueMode::Normal)
    }

    /// Resolve one global slot index to one retained-row value index.
    #[must_use]
    pub(in crate::db::executor) fn value_index_for_slot(&self, slot: usize) -> Option<usize> {
        self.data.slot_to_value_index.get(slot).copied().flatten()
    }

    /// Return the full slot span covered by this retained-slot layout.
    #[must_use]
    pub(in crate::db::executor) fn slot_count(&self) -> usize {
        self.data.slot_to_value_index.len()
    }

    /// Return the number of retained values each indexed retained row stores.
    #[must_use]
    pub(in crate::db::executor) fn retained_value_count(&self) -> usize {
        self.data.required_slots.len()
    }
}

///
/// RetainedSlotRow
///
/// RetainedSlotRow keeps only the caller-declared decoded slot values for one
/// retained-slot structural row.
/// The slot-only execution path stores those retained values in one compact
/// slot-sorted entry list so sparse outer projections do not allocate a
/// field-count-sized `Vec<Option<Value>>` for every row.
///

pub(in crate::db) struct RetainedSlotRow {
    storage: RetainedSlotRowStorage,
}

///
/// RetainedSlotEntry
///
/// RetainedSlotEntry stores one retained slot index plus its optional value.
/// Entries stay sorted by slot so retained rows can binary-search sparse slot
/// lookups without rebuilding a dense per-row slot image.
///

struct RetainedSlotEntry {
    slot: usize,
    value: Option<Value>,
}

// Retained rows either reuse one shared indexed layout for O(1) slot access
// or keep one compact sparse fallback shape when no prepared retained-slot
// layout exists for the producer.
enum RetainedSlotRowStorage {
    Indexed {
        layout: RetainedSlotLayout,
        values: Vec<Option<Value>>,
    },
    Sparse {
        slot_count: usize,
        entries: Vec<RetainedSlotEntry>,
    },
}

impl RetainedSlotRow {
    /// Build one retained slot row from sparse decoded `(slot, value)` pairs.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn new(slot_count: usize, entries: Vec<(usize, Value)>) -> Self {
        let mut compact_entries = entries
            .into_iter()
            .filter(|(slot, _)| *slot < slot_count)
            .collect::<Vec<_>>();
        compact_entries.sort_by_key(|(slot, _)| *slot);

        let mut deduped_entries: Vec<RetainedSlotEntry> = Vec::with_capacity(compact_entries.len());
        for (slot, value) in compact_entries {
            if let Some(entry) = deduped_entries.last_mut()
                && entry.slot == slot
            {
                entry.value = Some(value);
            } else {
                deduped_entries.push(RetainedSlotEntry {
                    slot,
                    value: Some(value),
                });
            }
        }

        Self {
            storage: RetainedSlotRowStorage::Sparse {
                slot_count,
                entries: deduped_entries,
            },
        }
    }

    /// Build one retained slot row from one already-materialized dense slot image.
    #[must_use]
    pub(in crate::db::executor) fn from_dense_slots(slots: Vec<Option<Value>>) -> Self {
        let slot_count = slots.len();
        let mut entries = Vec::new();

        for (slot, value) in slots.into_iter().enumerate() {
            let Some(value) = value else {
                continue;
            };

            entries.push(RetainedSlotEntry {
                slot,
                value: Some(value),
            });
        }

        Self {
            storage: RetainedSlotRowStorage::Sparse {
                slot_count,
                entries,
            },
        }
    }

    /// Build one retained slot row from compact retained values under one
    /// shared retained-slot layout.
    #[must_use]
    pub(in crate::db::executor) fn from_indexed_values(
        layout: &RetainedSlotLayout,
        values: Vec<Option<Value>>,
    ) -> Self {
        debug_assert_eq!(values.len(), layout.retained_value_count());

        Self {
            storage: RetainedSlotRowStorage::Indexed {
                layout: layout.clone(),
                values,
            },
        }
    }

    /// Borrow one retained slot value without cloning it back out of the row.
    #[must_use]
    pub(in crate::db) fn slot_ref(&self, slot: usize) -> Option<&Value> {
        match &self.storage {
            RetainedSlotRowStorage::Indexed { layout, values } => {
                let index = layout.value_index_for_slot(slot)?;

                values.get(index).and_then(Option::as_ref)
            }
            RetainedSlotRowStorage::Sparse { entries, .. } => {
                Self::find_sparse_entry(entries.as_slice(), slot)
                    .and_then(|entry| entry.value.as_ref())
            }
        }
    }

    /// Remove one retained slot value by slot index while consuming the row in
    /// direct field-projection paths.
    pub(in crate::db) fn take_slot(&mut self, slot: usize) -> Option<Value> {
        match &mut self.storage {
            RetainedSlotRowStorage::Indexed { layout, values } => {
                let index = layout.value_index_for_slot(slot)?;

                values.get_mut(index)?.take()
            }
            RetainedSlotRowStorage::Sparse { entries, .. } => {
                let index = Self::find_sparse_entry_index(entries.as_slice(), slot)?;

                entries.get_mut(index)?.value.take()
            }
        }
    }

    /// Expand this retained row back into one dense slot vector for callers
    /// that still require slot-indexed access across the full row width.
    #[must_use]
    pub(in crate::db::executor) fn into_dense_slots(self) -> Vec<Option<Value>> {
        match self.storage {
            RetainedSlotRowStorage::Indexed { layout, values } => {
                let mut slots = vec![None; layout.slot_count()];

                for (&slot, value) in layout.required_slots().iter().zip(values) {
                    slots[slot] = value;
                }

                slots
            }
            RetainedSlotRowStorage::Sparse {
                slot_count,
                entries,
            } => {
                let mut slots = vec![None; slot_count];

                for entry in entries {
                    if let Some(value) = entry.value {
                        slots[entry.slot] = Some(value);
                    }
                }

                slots
            }
        }
    }

    // Resolve one retained sparse entry by slot index inside the slot-sorted compact row.
    fn find_sparse_entry(entries: &[RetainedSlotEntry], slot: usize) -> Option<&RetainedSlotEntry> {
        let index = Self::find_sparse_entry_index(entries, slot)?;

        entries.get(index)
    }

    // Binary-search one compact sparse retained-slot entry list by stable slot index.
    fn find_sparse_entry_index(entries: &[RetainedSlotEntry], slot: usize) -> Option<usize> {
        entries.binary_search_by_key(&slot, |entry| entry.slot).ok()
    }
}

impl ProjectionValidationRow for RetainedSlotRow {
    fn projection_validation_slot_value(&self, slot: usize) -> Option<&Value> {
        self.slot_ref(slot)
    }
}
