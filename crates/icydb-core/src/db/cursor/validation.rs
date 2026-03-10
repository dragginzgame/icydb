//! Module: db::cursor::validation
//! Responsibility: module-local ownership and contracts for db::cursor::validation.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        cursor::{
            ContinuationSignature, CursorPlanError, GroupedPlannedCursor, PlannedCursor,
            prepare_cursor, prepare_grouped_cursor,
        },
        executor::ExecutableAccessPath,
        query::plan::{ExecutionOrderContract, ExecutionOrdering},
    },
    traits::{EntityKind, FieldValue},
};

///
/// CursorValidationOutcome
///
/// Cursor compatibility validation result for one planned query mode.
/// Encodes validated scalar or grouped cursor state without exposing token internals.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum CursorValidationOutcome {
    Scalar(Box<PlannedCursor>),
    Grouped(GroupedPlannedCursor),
}

/// Validate optional cursor bytes for one planned query mode and return typed
/// cursor state without leaking token payload details across boundaries.
pub(in crate::db) fn validate_cursor_compatibility<E: EntityKind>(
    contract: &ExecutionOrderContract,
    access: Option<ExecutableAccessPath<'_, E::Key>>,
    continuation_signature: ContinuationSignature,
    initial_offset: u32,
    cursor: Option<&[u8]>,
) -> Result<CursorValidationOutcome, CursorPlanError>
where
    E::Key: FieldValue,
{
    match contract.ordering() {
        ExecutionOrdering::PrimaryKey => {
            if cursor.is_some() || contract.supports_cursor() {
                return Err(CursorPlanError::continuation_cursor_invariant(
                    "cursor compatibility requires explicit or grouped ordering contract",
                ));
            }

            Ok(CursorValidationOutcome::Scalar(Box::new(
                PlannedCursor::none(),
            )))
        }
        ExecutionOrdering::Explicit(order) => {
            let scalar = prepare_cursor::<E>(
                access,
                Some(order),
                contract.direction(),
                continuation_signature,
                initial_offset,
                cursor,
            )?;

            Ok(CursorValidationOutcome::Scalar(Box::new(scalar)))
        }
        ExecutionOrdering::Grouped(order) => {
            let grouped = prepare_grouped_cursor(
                E::PATH,
                order.as_ref(),
                continuation_signature,
                initial_offset,
                cursor,
            )?;

            Ok(CursorValidationOutcome::Grouped(grouped))
        }
    }
}
