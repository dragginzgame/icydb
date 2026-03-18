use crate::{
    db::executor::{
        ExecutionTrace,
        pipeline::contracts::{CursorPage, GroupedCursorPage},
    },
    error::InternalError,
    traits::EntityKind,
};
use std::any::Any;

///
/// ErasedLoadPayload
///
/// ErasedLoadPayload is the generic-free scalar payload container used by the
/// monomorphic load entrypoint path.
/// Typed scalar pages or row payloads are boxed once at the runtime boundary
/// and downcast only at the final entrypoint surface adapter.
///

pub(in crate::db::executor) struct ErasedLoadPayload {
    payload: Box<dyn Any>,
}

impl ErasedLoadPayload {
    /// Erase one typed scalar payload behind a safe trait-object boundary.
    #[must_use]
    pub(in crate::db::executor) fn new<T>(value: T) -> Self
    where
        T: 'static,
    {
        Self {
            payload: Box::new(value),
        }
    }

    /// Recover one typed scalar payload at the final entrypoint boundary.
    pub(in crate::db::executor) fn into_typed<T>(
        self,
        mismatch_message: &'static str,
    ) -> Result<T, InternalError>
    where
        T: 'static,
    {
        self.payload
            .downcast::<T>()
            .map(|value| *value)
            .map_err(|_| crate::db::error::query_executor_invariant(mismatch_message))
    }
}

///
/// ErasedLoadExecutionSurface
///
/// Finalized generic-free load output surface for entrypoint wrappers.
/// This preserves one monomorphic load-orchestrator return contract while
/// allowing typed scalar payload recovery only at the last wrapper boundary.
///

pub(in crate::db::executor) enum ErasedLoadExecutionSurface {
    ScalarPage(ErasedLoadPayload),
    ScalarPageWithTrace(ErasedLoadPayload, Option<ExecutionTrace>),
    GroupedPageWithTrace(GroupedCursorPage, Option<ExecutionTrace>),
}

///
/// LoadExecutionSurface
///
/// Finalized typed load output surface for legacy typed entrypoint wrappers and
/// stage-loop backstop execution.
///

#[allow(dead_code)]
pub(in crate::db::executor) enum LoadExecutionSurface<E: EntityKind> {
    ScalarRows(crate::db::response::EntityResponse<E>),
    ScalarPage(CursorPage<E>),
    ScalarPageWithTrace(CursorPage<E>, Option<ExecutionTrace>),
    GroupedPageWithTrace(GroupedCursorPage, Option<ExecutionTrace>),
}
