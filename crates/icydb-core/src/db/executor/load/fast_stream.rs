use crate::{
    db::{
        Context,
        executor::load::{ExecutionOptimization, FastPathKeyResult, LoadExecutor},
        executor::{AccessPlanStreamRequest, VecOrderedKeyStream, route::RoutedKeyStreamRequest},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Resolve one fast-path access stream through the canonical resolver boundary,
    // then materialize keys once so fast-path accounting can expose rows_scanned.
    pub(super) fn execute_fast_stream_request(
        ctx: &Context<'_, E>,
        stream_request: AccessPlanStreamRequest<'_, E::Key>,
        optimization: ExecutionOptimization,
    ) -> Result<FastPathKeyResult, InternalError> {
        let mut key_stream = Self::resolve_routed_key_stream(
            ctx,
            RoutedKeyStreamRequest::AccessPlan(stream_request),
        )?;
        let mut ordered_keys = Vec::new();
        while let Some(key) = key_stream.next_key()? {
            ordered_keys.push(key);
        }
        let rows_scanned = ordered_keys.len();

        Ok(FastPathKeyResult {
            ordered_key_stream: Box::new(VecOrderedKeyStream::new(ordered_keys)),
            rows_scanned,
            optimization,
        })
    }
}
