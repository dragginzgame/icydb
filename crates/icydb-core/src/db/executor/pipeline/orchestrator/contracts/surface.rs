use crate::{
    db::{
        executor::{
            ExecutionTrace,
            pipeline::contracts::{CursorPage, GroupedCursorPage},
        },
        response::EntityResponse,
    },
    traits::EntityKind,
};

///
/// LoadExecutionSurface
///
/// Finalized load output surface for entrypoint wrappers.
/// Encodes one terminal response shape so wrapper adapters do not carry
/// payload/trace pairing branches.
///

pub(in crate::db::executor) enum LoadExecutionSurface<E: EntityKind> {
    ScalarRows(EntityResponse<E>),
    ScalarPage(CursorPage<E>),
    ScalarPageWithTrace(CursorPage<E>, Option<ExecutionTrace>),
    GroupedPageWithTrace(GroupedCursorPage, Option<ExecutionTrace>),
}
