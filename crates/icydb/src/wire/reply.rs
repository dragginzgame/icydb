//! Product query-reply admission over IC Wire's complete-envelope sizing.
//!
//! Adapters retain exports and transport; this owner selects existing IC byte
//! caps and preserves the small typed result when success cannot be delivered.

use crate::{
    Error, ErrorKind, ErrorOrigin, RuntimeErrorKind, db::EntitySchemaDescription,
    diagnostic::RuntimeBoundaryCode,
};
use serde::Serialize;

pub(crate) const MAX_PUBLIC_QUERY_REPLY_BYTES: usize = icydb_schema::wire::MAX_REPLY_BYTES;
#[cfg(any(target_arch = "wasm32", test))]
pub(crate) const MAX_REPLICATED_QUERY_REPLY_BYTES: usize = 2 * 1024 * 1024;

/// Admit one generated accepted-schema success under the actual message cap.
#[doc(hidden)]
pub fn into_deliverable_schema_reply(
    report: Vec<EntitySchemaDescription>,
) -> Result<Vec<EntitySchemaDescription>, Error> {
    into_deliverable_query_reply(report, RuntimeBoundaryCode::SchemaReplyBytesExceeded)
}

pub(crate) fn into_deliverable_query_reply<T: Serialize>(
    value: T,
    exceeded: RuntimeBoundaryCode,
) -> Result<T, Error> {
    #[cfg(target_arch = "wasm32")]
    let maximum = if ic_platform::ic0::in_replicated_execution() != 0 {
        MAX_REPLICATED_QUERY_REPLY_BYTES
    } else {
        MAX_PUBLIC_QUERY_REPLY_BYTES
    };
    #[cfg(not(target_arch = "wasm32"))]
    let maximum = MAX_PUBLIC_QUERY_REPLY_BYTES;
    into_deliverable_query_reply_with_limit(value, maximum, exceeded)
}

// One exact success-envelope check serves both handlers and both IC contexts.
// Codec admission failures retain the existing SQL Internal/Response contract.
pub(crate) fn into_deliverable_query_reply_with_limit<T: Serialize>(
    value: T,
    maximum: usize,
    exceeded: RuntimeBoundaryCode,
) -> Result<T, Error> {
    let response: Result<&T, Error> = Ok(&value);
    let encoded =
        ic_wire::native::encoded_frame_len(&response, icydb_schema::wire::sizing_limits())
            .map_err(|_| {
                Error::from_kind(
                    ErrorKind::Runtime(RuntimeErrorKind::Internal),
                    ErrorOrigin::Response,
                )
            })?;
    if encoded > maximum {
        return Err(Error::from_runtime_boundary(
            exceeded,
            ErrorOrigin::Response,
        ));
    }
    Ok(value)
}

#[cfg(test)]
mod tests;
