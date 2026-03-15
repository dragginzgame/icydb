use crate::{
    db::executor::pipeline::operators::reducer::{
        KernelReducer, ReducerControl, StreamInputMode, StreamItem,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

///
/// RowCollectorReducer
///
/// RowCollectorReducer accepts ephemeral row items and keeps canonical load
/// row-collection behavior in the kernel-owned runner boundary.
///

pub(in crate::db::executor::pipeline::operators::terminal) struct RowCollectorReducer;

impl<E> KernelReducer<E> for RowCollectorReducer
where
    E: EntityKind + EntityValue,
{
    type Output = ();
    const INPUT_MODE: StreamInputMode = StreamInputMode::RowOnly;

    fn on_item(&mut self, item: StreamItem<'_, E>) -> Result<ReducerControl, InternalError> {
        match item {
            StreamItem::Row(_row) => Ok(ReducerControl::Continue),
            StreamItem::Key(_key) => Err(crate::db::error::query_executor_invariant(
                "row collector reducer received key item for row-only input mode",
            )),
        }
    }

    fn finish(self) -> Result<Self::Output, InternalError> {
        Ok(())
    }
}
