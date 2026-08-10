//! Module: cursor
//! Responsibility: continuation cursor decode/revalidation boundaries for executor routes.
//! Does not own: query planning policy, index lowering, or storage mutation semantics.
//! Boundary: accepts planner/runtime cursor context and produces validated cursor state.

#[cfg(test)]
mod tests;

mod boundary;
mod continuation;
mod error;
mod runtime;
mod signature;
mod spine;
mod string;
mod validated;

mod token;

use crate::db::{
    direction::Direction,
    query::plan::{
        OrderSpec, validate::CursorOrderPlanShapeError, validate_cursor_order_plan_shape,
    },
};

pub(in crate::db) use boundary::apply_order_direction;
pub(in crate::db::cursor) use boundary::validate_cursor_window_offset;
pub(in crate::db) use boundary::{CursorBoundary, CursorBoundarySlot};
pub(in crate::db) use continuation::{
    IndexScanContinuationInput, effective_keep_count_for_limit, effective_page_offset_for_window,
};
pub(crate) use error::CursorPlanError;
#[cfg(test)]
pub(crate) use error::CursorSignaturePrefix;
pub(in crate::db) use runtime::{ContinuationKeyRef, ContinuationRuntime};
pub(in crate::db) use signature::ContinuationSignature;
pub(crate) use string::CursorDecodeError;
use string::decode_cursor;
pub(in crate::db) use string::encode_cursor;
#[cfg(test)]
pub(in crate::db) use string::encode_grouped_cursor_token;
pub(in crate::db) use token::{
    GroupedContinuationToken, ScalarOrderTermContract, ScalarPageMode, ScalarPageToken,
    ScalarPageTokenAuthority, ScalarPageTokenProgress, ScalarPageTokenWindow, TokenWireError,
    decode_current_value_payload, encode_current_value_payload,
};
pub(in crate::db) use validated::ValidatedGroupedCursor;

/// Decode one optional external continuation token through cursor-runtime authority.
pub(in crate::db) fn decode_optional_cursor_token(
    cursor_token: Option<&str>,
) -> Result<Option<Vec<u8>>, CursorPlanError> {
    cursor_token
        .map(|token| decode_cursor(token).map_err(CursorPlanError::invalid_continuation_cursor))
        .transpose()
}

/// Decode one optional grouped cursor token through the existing external
/// hex-token boundary while preserving grouped-token ownership for downstream
/// validation.
pub(in crate::db) fn decode_optional_grouped_cursor_token(
    cursor_token: Option<&str>,
) -> Result<Option<GroupedContinuationToken>, CursorPlanError> {
    decode_optional_cursor_token(cursor_token)?
        .map(|bytes| {
            GroupedContinuationToken::decode(bytes.as_slice())
                .map_err(CursorPlanError::from_token_wire_error)
        })
        .transpose()
}

/// Validate and decode a grouped continuation cursor into grouped cursor state.
#[cfg(test)]
pub(in crate::db) fn prepare_grouped_cursor(
    entity_path: &str,
    order: Option<&OrderSpec>,
    direction: Direction,
    continuation_signature: ContinuationSignature,
    initial_offset: u32,
    cursor: Option<&[u8]>,
) -> Result<ValidatedGroupedCursor, CursorPlanError> {
    validate_grouped_cursor_order_plan(order)?;

    spine::validate_grouped_cursor(
        cursor,
        entity_path,
        continuation_signature,
        direction,
        initial_offset,
    )
}

/// Validate one already-decoded grouped continuation token into grouped
/// executor cursor state.
pub(in crate::db) fn prepare_grouped_cursor_token(
    entity_path: &str,
    order: Option<&OrderSpec>,
    direction: Direction,
    continuation_signature: ContinuationSignature,
    initial_offset: u32,
    cursor: Option<GroupedContinuationToken>,
) -> Result<ValidatedGroupedCursor, CursorPlanError> {
    validate_grouped_cursor_order_plan(order)?;

    spine::validate_grouped_cursor_token(
        cursor,
        entity_path,
        continuation_signature,
        direction,
        initial_offset,
    )
}

/// Validate grouped cursor ordering plan shape.
///
/// Grouped pagination uses canonical lexicographic group-key order by default,
/// so explicit ordering is optional, but empty order specs remain invalid.
pub(in crate::db) fn validate_grouped_cursor_order_plan(
    order: Option<&OrderSpec>,
) -> Result<(), CursorPlanError> {
    validated_cursor_order_internal(order, false).map(|_| ())
}

fn validated_cursor_order_internal(
    order: Option<&OrderSpec>,
    require_explicit_order: bool,
) -> Result<Option<&OrderSpec>, CursorPlanError> {
    validate_cursor_order_plan_shape(order, require_explicit_order)
        .map_err(CursorOrderPlanShapeError::to_cursor_plan_error)
}
