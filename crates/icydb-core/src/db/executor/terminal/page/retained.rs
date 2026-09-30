use crate::{
    db::executor::{OrderReadableRow, budget::runtime_value_work},
    value::Value,
};
use std::{borrow::Cow, rc::Rc};

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
    data: Rc<RetainedSlotLayoutData>,
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
        Self::compile_with_value_modes(slot_count, required_slots, Vec::new())
    }

    /// Compile one retained-slot layout from slots plus per-slot decode modes.
    #[must_use]
    pub(in crate::db::executor) fn compile_with_value_modes(
        slot_count: usize,
        required_slots: Vec<usize>,
        value_modes: Vec<RetainedSlotValueMode>,
    ) -> Self {
        debug_assert!(value_modes.is_empty() || required_slots.len() == value_modes.len());

        let mut has_value_mode_overrides = false;
        for mode in &value_modes {
            has_value_mode_overrides |= *mode != RetainedSlotValueMode::Normal;
        }
        let mut slot_to_value_index = vec![None; slot_count];
        for (value_index, &slot) in required_slots.iter().enumerate() {
            if let Some(entry) = slot_to_value_index.get_mut(slot) {
                *entry = Some(value_index);
            }
        }

        Self {
            data: Rc::new(RetainedSlotLayoutData {
                required_slots: required_slots.into_boxed_slice(),
                value_modes: if has_value_mode_overrides {
                    value_modes.into_boxed_slice()
                } else {
                    Vec::new().into_boxed_slice()
                },
                slot_to_value_index: slot_to_value_index.into_boxed_slice(),
            }),
        }
    }

    /// Borrow the retained slots in the same stable order used by retained-row value storage.
    #[must_use]
    pub(in crate::db::executor) fn required_slots(&self) -> &[usize] {
        self.data.required_slots.as_ref()
    }

    /// Borrow the per-retained-slot override materialization modes in layout order.
    #[must_use]
    pub(in crate::db::executor) fn override_value_modes(&self) -> Option<&[RetainedSlotValueMode]> {
        self.has_value_mode_overrides()
            .then_some(self.data.value_modes.as_ref())
    }

    /// Return whether any retained slot uses a non-standard materialization mode.
    #[must_use]
    pub(in crate::db::executor) fn has_value_mode_overrides(&self) -> bool {
        !self.data.value_modes.is_empty()
    }

    /// Resolve one global slot index to one retained-row value index.
    #[must_use]
    pub(in crate::db::executor) fn value_index_for_slot(&self, slot: usize) -> Option<usize> {
        self.data.slot_to_value_index.get(slot).copied().flatten()
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
/// Each row stores compact values in shared layout order. The layout resolves
/// global slot indices without allocating a field-count-sized value vector
/// for every row.
///

pub(in crate::db) struct RetainedSlotRow {
    layout: RetainedSlotLayout,
    values: Vec<Option<Value>>,
}

impl RetainedSlotRow {
    /// Build one retained slot row from compact retained values under one
    /// shared retained-slot layout.
    #[must_use]
    pub(in crate::db::executor) fn from_indexed_values(
        layout: &RetainedSlotLayout,
        values: Vec<Option<Value>>,
    ) -> Self {
        debug_assert_eq!(values.len(), layout.retained_value_count());

        Self {
            layout: layout.clone(),
            values,
        }
    }

    /// Borrow one retained slot value without cloning it back out of the row.
    #[must_use]
    pub(in crate::db) fn slot_ref(&self, slot: usize) -> Option<&Value> {
        let index = self.layout.value_index_for_slot(slot)?;

        self.values.get(index).and_then(Option::as_ref)
    }

    /// Remove one retained slot value by slot index while consuming the row in
    /// direct field-projection paths.
    pub(in crate::db) fn take_slot(&mut self, slot: usize) -> Option<Value> {
        let index = self.layout.value_index_for_slot(slot)?;

        self.values.get_mut(index)?.take()
    }

    /// Estimate complete value backing retained by this compact slot row.
    #[must_use]
    pub(in crate::db::executor) fn estimated_backing_bytes(&self) -> u64 {
        self.values.iter().flatten().fold(0_u64, |total, value| {
            total.saturating_add(runtime_value_work(value).0)
        })
    }
}

impl OrderReadableRow for RetainedSlotRow {
    fn read_order_slot_ref(&self, slot: usize) -> Option<&Value> {
        self.slot_ref(slot)
    }

    fn read_order_slot_cow(&self, slot: usize) -> Option<Cow<'_, Value>> {
        self.slot_ref(slot).map(Cow::Borrowed)
    }

    fn order_slots_are_borrowed(&self) -> bool {
        true
    }
}

// Exhaustive cache-retention coverage; new owned fields require accounting.
crate::retained::retained_fields!(RetainedSlotLayout {
Self{data} => [data],
});
crate::retained::retained_fields!(RetainedSlotLayoutData {
Self{required_slots,value_modes,slot_to_value_index} => [required_slots,value_modes,slot_to_value_index],
});
crate::retained::retained_copy!(RetainedSlotValueMode);
