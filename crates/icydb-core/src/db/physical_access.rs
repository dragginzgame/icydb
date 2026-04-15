//! Module: db::physical_access
//! Responsibility: scoped instruction attribution for physical store/index access.
//! Does not own: planner/executor phase classification or store operation semantics.
//! Boundary: query/session perf surfaces opt in to this meter when they want one
//! separate physical-access bucket without double-counting nested store calls.

#[cfg(target_arch = "wasm32")]
use canic_cdk::api::performance_counter;
use std::cell::{Cell, RefCell};

#[cfg(feature = "perf-attribution")]
std::thread_local! {
    static PHYSICAL_ACCESS_ATTRIBUTION_STACK: RefCell<Vec<u64>> = const {
        RefCell::new(Vec::new())
    };
    static PHYSICAL_ACCESS_MEASURE_DEPTH: Cell<u32> = const { Cell::new(0) };
}

#[cfg(feature = "perf-attribution")]
#[expect(
    clippy::missing_const_for_fn,
    reason = "the wasm32 branch reads the runtime performance counter and cannot be const"
)]
fn read_local_instruction_counter() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        performance_counter(1)
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        0
    }
}

#[cfg(feature = "perf-attribution")]
fn record_physical_access_local_instructions(delta: u64) {
    if delta == 0 {
        return;
    }

    PHYSICAL_ACCESS_ATTRIBUTION_STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        let Some(active) = stack.last_mut() else {
            return;
        };

        *active = active.saturating_add(delta);
    });
}

/// Run one query/session phase while collecting nested physical store/index
/// access instructions separately from the surrounding phase total.
#[cfg(feature = "perf-attribution")]
pub(in crate::db) fn with_physical_access_attribution<T>(run: impl FnOnce() -> T) -> (u64, T) {
    PHYSICAL_ACCESS_ATTRIBUTION_STACK.with(|stack| {
        stack.borrow_mut().push(0);
    });

    let result = run();
    let total = PHYSICAL_ACCESS_ATTRIBUTION_STACK.with(|stack| {
        stack
            .borrow_mut()
            .pop()
            .unwrap_or_else(|| unreachable!("physical-access attribution stack must be balanced"))
    });

    PHYSICAL_ACCESS_ATTRIBUTION_STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        if let Some(parent) = stack.last_mut() {
            *parent = parent.saturating_add(total);
        }
    });

    (total, result)
}

/// Measure one physical store/index operation when the surrounding query phase
/// opted into physical-access attribution.
#[cfg(feature = "perf-attribution")]
pub(in crate::db) fn measure_physical_access_operation<T>(run: impl FnOnce() -> T) -> T {
    let has_active_attribution =
        PHYSICAL_ACCESS_ATTRIBUTION_STACK.with(|stack| !stack.borrow().is_empty());
    if !has_active_attribution {
        return run();
    }

    let nested_depth = PHYSICAL_ACCESS_MEASURE_DEPTH.with(Cell::get);
    if nested_depth > 0 {
        return run();
    }

    PHYSICAL_ACCESS_MEASURE_DEPTH.with(|depth| depth.set(depth.get().saturating_add(1)));
    let start = read_local_instruction_counter();
    let result = run();
    let delta = read_local_instruction_counter().saturating_sub(start);
    PHYSICAL_ACCESS_MEASURE_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    record_physical_access_local_instructions(delta);

    result
}
