#[cfg(any(test, feature = "diagnostics"))]
use crate::db::data::persisted_row::reader::StructuralSlotReader;
#[cfg(any(test, feature = "diagnostics"))]
use std::cell::{Cell, RefCell};

///
/// StructuralReadMetrics
///
/// StructuralReadMetrics aggregates one test-scoped view of structural row
/// validation and lazy non-scalar materialization activity.
/// It lets row-backed benchmarks prove the new boundary validates all declared
/// slots while only materializing the non-scalar slots a caller actually
/// touches.
///

#[cfg(any(test, feature = "diagnostics"))]
#[cfg_attr(all(test, not(feature = "diagnostics")), allow(unreachable_pub))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StructuralReadMetrics {
    pub rows_opened: u64,
    pub declared_slots_validated: u64,
    pub validated_non_scalar_slots: u64,
    pub materialized_non_scalar_slots: u64,
    pub rows_without_lazy_non_scalar_materializations: u64,
}

#[cfg(any(test, feature = "diagnostics"))]
std::thread_local! {
    static STRUCTURAL_READ_METRICS: RefCell<Option<StructuralReadMetrics>> = const {
        RefCell::new(None)
    };
}

///
/// StructuralReadProbe
///
/// StructuralReadProbe tracks one reader instance's structural validation and
/// deferred non-scalar materialization counts while a test-scoped metrics
/// capture is active.
///

#[cfg(any(test, feature = "diagnostics"))]
#[derive(Debug)]
pub(super) struct StructuralReadProbe {
    pub(super) collect: bool,
    declared_slots_validated: Cell<u64>,
    validated_non_scalar_slots: Cell<u64>,
    materialized_non_scalar_slots: Cell<u64>,
}

#[cfg(not(any(test, feature = "diagnostics")))]
#[derive(Debug)]
pub(super) struct StructuralReadProbe;

#[cfg(any(test, feature = "diagnostics"))]
impl StructuralReadProbe {
    // Begin one optional per-reader metrics probe when a test-scoped capture
    // is active on the current thread.
    pub(super) fn begin(_field_count: usize) -> Self {
        let collect = STRUCTURAL_READ_METRICS.with(|metrics| metrics.borrow().is_some());

        Self {
            collect,
            declared_slots_validated: Cell::new(0),
            validated_non_scalar_slots: Cell::new(0),
            materialized_non_scalar_slots: Cell::new(0),
        }
    }

    // Record one distinct slot validated on first access.
    pub(super) fn record_validated_slot(&self) {
        if !self.collect {
            return;
        }

        self.declared_slots_validated
            .set(self.declared_slots_validated.get().saturating_add(1));
    }

    // Record one non-scalar slot validated at row-open.
    pub(super) fn record_validated_non_scalar(&self) {
        if !self.collect {
            return;
        }

        self.validated_non_scalar_slots
            .set(self.validated_non_scalar_slots.get().saturating_add(1));
    }

    // Record one distinct non-scalar slot materialized after row-open.
    pub(super) fn record_materialized_non_scalar(&self) {
        if !self.collect {
            return;
        }

        self.materialized_non_scalar_slots
            .set(self.materialized_non_scalar_slots.get().saturating_add(1));
    }
}

#[cfg(not(any(test, feature = "diagnostics")))]
impl StructuralReadProbe {
    // Build one no-op probe when structural read metrics are not compiled in.
    pub(super) const fn begin(_field_count: usize) -> Self {
        Self
    }

    // Record one distinct slot validated on first access.
    pub(super) const fn record_validated_slot(&self) {
        let _ = self;
    }

    // Record one non-scalar slot validated at row-open.
    pub(super) const fn record_validated_non_scalar(&self) {
        let _ = self;
    }

    // Record one distinct non-scalar slot materialized after row-open.
    pub(super) const fn record_materialized_non_scalar(&self) {
        let _ = self;
    }
}

// Flush one direct sparse-read probe into the thread-local structural metrics
// aggregator so executor sparse decode paths preserve the same observability
// contract as reader-backed lazy decode paths.
#[cfg(any(test, feature = "diagnostics"))]
pub(super) fn finish_direct_probe(probe: &StructuralReadProbe) {
    if !probe.collect {
        return;
    }

    let validated_non_scalar_slots = probe.validated_non_scalar_slots.get();
    let materialized_non_scalar_slots = probe.materialized_non_scalar_slots.get();
    let declared_slots_validated = probe.declared_slots_validated.get();

    STRUCTURAL_READ_METRICS.with(|metrics| {
        if let Some(aggregate) = metrics.borrow_mut().as_mut() {
            aggregate.rows_opened = aggregate.rows_opened.saturating_add(1);
            aggregate.declared_slots_validated = aggregate
                .declared_slots_validated
                .saturating_add(declared_slots_validated);
            aggregate.validated_non_scalar_slots = aggregate
                .validated_non_scalar_slots
                .saturating_add(validated_non_scalar_slots);
            aggregate.materialized_non_scalar_slots = aggregate
                .materialized_non_scalar_slots
                .saturating_add(materialized_non_scalar_slots);
            if materialized_non_scalar_slots == 0 {
                aggregate.rows_without_lazy_non_scalar_materializations = aggregate
                    .rows_without_lazy_non_scalar_materializations
                    .saturating_add(1);
            }
        }
    });
}

#[cfg(not(any(test, feature = "diagnostics")))]
pub(super) const fn finish_direct_probe(_probe: &StructuralReadProbe) {}

#[cfg(any(test, feature = "diagnostics"))]
impl Drop for StructuralSlotReader<'_> {
    fn drop(&mut self) {
        if !self.metrics.collect {
            return;
        }

        let validated_non_scalar_slots = self.metrics.validated_non_scalar_slots.get();
        let materialized_non_scalar_slots = self.metrics.materialized_non_scalar_slots.get();
        let declared_slots_validated = self.metrics.declared_slots_validated.get();

        STRUCTURAL_READ_METRICS.with(|metrics| {
            if let Some(aggregate) = metrics.borrow_mut().as_mut() {
                aggregate.rows_opened = aggregate.rows_opened.saturating_add(1);
                aggregate.declared_slots_validated = aggregate
                    .declared_slots_validated
                    .saturating_add(declared_slots_validated);
                aggregate.validated_non_scalar_slots = aggregate
                    .validated_non_scalar_slots
                    .saturating_add(validated_non_scalar_slots);
                aggregate.materialized_non_scalar_slots = aggregate
                    .materialized_non_scalar_slots
                    .saturating_add(materialized_non_scalar_slots);
                if materialized_non_scalar_slots == 0 {
                    aggregate.rows_without_lazy_non_scalar_materializations = aggregate
                        .rows_without_lazy_non_scalar_materializations
                        .saturating_add(1);
                }
            }
        });
    }
}

///
/// with_structural_read_metrics
///
/// Run one closure while collecting structural-read metrics on the current
/// thread, then return the closure result plus the aggregated snapshot.
///

#[cfg(any(test, feature = "diagnostics"))]
#[cfg_attr(all(test, not(feature = "diagnostics")), allow(unreachable_pub))]
pub fn with_structural_read_metrics<T>(f: impl FnOnce() -> T) -> (T, StructuralReadMetrics) {
    STRUCTURAL_READ_METRICS.with(|metrics| {
        debug_assert!(
            metrics.borrow().is_none(),
            "structural read metrics captures should not nest"
        );
        *metrics.borrow_mut() = Some(StructuralReadMetrics::default());
    });

    let result = f();
    let metrics =
        STRUCTURAL_READ_METRICS.with(|metrics| metrics.borrow_mut().take().unwrap_or_default());

    (result, metrics)
}
