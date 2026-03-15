use crate::{
    db::data::DataKey,
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

///
/// StreamInputMode
///
/// Declares what item shape one kernel reducer consumes from execution.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum StreamInputMode {
    KeyOnly,
    RowOnly,
}

///
/// StreamItem
///
/// Item payload delivered by the kernel reducer runner.
/// Items are borrowed from kernel-local staging for one `on_item` call.
/// Reducers must treat these references as ephemeral and must not retain them.
///

pub(in crate::db::executor) enum StreamItem<'a, E: EntityKind + EntityValue> {
    Key(&'a DataKey),
    Row(&'a E),
}

///
/// ReducerControl
///
/// Reducer step-control contract returned by one `on_item` call.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum ReducerControl {
    Continue,
    StopEarly,
}

///
/// KernelReducer
///
/// KernelReducer is the canonical reducer contract for kernel-owned runner
/// orchestration. Reducers must be deterministic and restart-safe, and must
/// not retain `StreamItem` references after `on_item` returns.
///

pub(in crate::db::executor) trait KernelReducer<E: EntityKind + EntityValue> {
    type Output;
    const INPUT_MODE: StreamInputMode;

    /// Consume one stream item and return reducer control state.
    fn on_item(&mut self, item: StreamItem<'_, E>) -> Result<ReducerControl, InternalError>;
    /// Finalize reducer output after stream consumption completes.
    fn finish(self) -> Result<Self::Output, InternalError>;
}
