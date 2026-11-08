//! Explicit multi-commit composition of the canonical atomic typed writer.
//! Owns bounded orchestration and confirmed progress, never encoding or recovery.

#[cfg(test)]
mod tests;

use crate::{
    db::{
        DynamicMutationResult, TypedAdapterError, TypedOperationError, TypedWrite,
        session::DbSession,
    },
    error::Error,
    traits::CanisterKind,
};

use icydb_core::db::DynamicTypedBatchAttempt;
use std::{error::Error as StdError, fmt};

/// Caller-selected bounds for one synchronous multi-commit operation.
/// All bounds must be nonzero. Canonical engine ceilings also apply.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TypedBulkWriteLimits {
    /// Maximum inputs per atomic attempt, capped by the engine's row ceiling.
    pub max_batch_rows: usize,
    /// Canonical staged key/before/after bytes per atomic attempt.
    pub max_batch_staged_bytes: usize,
    /// Canonical encoded result-vector bytes per atomic attempt.
    pub max_batch_result_bytes: usize,
    /// Total attempted inputs, including work in refused attempts.
    /// This is a structural work bound, not an IC instruction allowance.
    pub max_attempted_rows: usize,
    /// Maximum successful atomic batches, including successful no-op batches.
    pub max_commits: usize,
}

impl TypedBulkWriteLimits {
    const fn is_valid(self) -> bool {
        self.max_batch_rows != 0
            && self.max_batch_staged_bytes != 0
            && self.max_batch_result_bytes != 0
            && self.max_attempted_rows != 0
            && self.max_commits != 0
    }
}

impl Default for TypedBulkWriteLimits {
    fn default() -> Self {
        Self {
            max_batch_rows: 128,
            max_batch_staged_bytes: 512 * 1024,
            max_batch_result_bytes: 512 * 1024,
            max_attempted_rows: 4_096,
            max_commits: 32,
        }
    }
}

/// One confirmed atomic batch, including assigned keys and affected-row counts.
#[derive(Debug)]
pub struct TypedBulkWriteBatch {
    /// Zero-based position in the original supplied input slice.
    pub first_input: usize,
    /// One canonical result per input, in input order.
    pub results: Vec<DynamicMutationResult>,
}

/// Confirmed progress only; an ordinary error does not certify the failed batch.
#[derive(Debug, Default)]
pub struct TypedBulkWriteProgress {
    /// Number of confirmed inputs, also the first unconfirmed input position.
    pub next_input: usize,
    /// Inputs charged to attempted work, including byte-refused attempts.
    pub attempted_rows: usize,
    /// Successful batches in commit order. Use each batch's affected-row counts
    /// for application-owned mutation observations, including on failure.
    pub batches: Vec<TypedBulkWriteBatch>,
}

/// Why a bulk operation stopped before confirming every input.
#[derive(Debug)]
pub enum TypedBulkWriteError {
    /// At least one requested limit was zero; no operation was attempted.
    InvalidLimits,
    /// The attempted-input or successful-batch allowance was exhausted.
    /// The unattempted suffix has no effects from this operation.
    WorkLimit,
    /// One input cannot fit canonical or caller byte bounds. This attempt
    /// published no effect, and no smaller batch can contain that input.
    InputTooLarge(Error),
    /// Adapter or database failure. No automatic retry or replay is authorized;
    /// in particular, confirmed prefix progress does not classify this failure.
    Operation(TypedOperationError),
}

impl fmt::Display for TypedBulkWriteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLimits => formatter.write_str("bulk write limits must be nonzero"),
            Self::WorkLimit => formatter.write_str("bulk write work allowance exhausted"),
            Self::InputTooLarge(error) => error.fmt(formatter),
            Self::Operation(error) => error.fmt(formatter),
        }
    }
}

impl StdError for TypedBulkWriteError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::InputTooLarge(error) => Some(error),
            Self::Operation(error) => Some(error),
            Self::InvalidLimits | Self::WorkLimit => None,
        }
    }
}

/// A stopped multi-commit operation with its successful prefix preserved.
#[derive(Debug)]
pub struct TypedBulkWriteFailure {
    /// Successful prefix and work already consumed; never replay this prefix.
    pub progress: TypedBulkWriteProgress,
    /// Typed stop reason, retaining adapter/database causes.
    pub error: TypedBulkWriteError,
}

impl fmt::Display for TypedBulkWriteFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl StdError for TypedBulkWriteFailure {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.error)
    }
}

impl<C: CanisterKind> DbSession<C> {
    /// Execute an ordered typed input slice as explicitly separate atomic batches.
    ///
    /// Earlier batches may have committed when a later input fails. Results and
    /// assigned keys retain original input order; failure carries that successful
    /// prefix. Empty input succeeds without attempting a batch. The existing
    /// atomic batch API never splits and is unchanged.
    ///
    /// Only a canonical byte refusal proven to precede the commit handoff permits
    /// halving the current attempt. Other failures stop immediately. Refused work
    /// counts against `max_attempted_rows`; a successful prefix is never replayed.
    /// Journal backlog and request execution budgets remain independently enforced.
    /// Returned result memory is bounded by the successful-batch and byte limits.
    ///
    /// Authorization, input normalization, application revisions, public error
    /// mapping and cross-message resume remain caller-owned. Input positions are
    /// relative to this slice. Ordinary database failures give no retry authority
    /// for the unconfirmed batch. On IC, a later trap
    /// can roll back the whole message; returning partial progress is not a durable
    /// cross-message checkpoint or a claim that an atomic call was split.
    pub fn execute_trusted_typed_write_bulk(
        &self,
        writes: &[TypedWrite],
        limits: TypedBulkWriteLimits,
    ) -> Result<TypedBulkWriteProgress, TypedBulkWriteFailure> {
        execute_bulk(writes.len(), limits, |start, count| {
            let requests = writes[start..start + count]
                .iter()
                .map(|write| (write.binding.inner.clone(), write.mutation.clone()))
                .collect();
            self.inner
                .attempt_trusted_typed_mutation_batch_shared(
                    requests,
                    limits.max_batch_staged_bytes,
                    limits.max_batch_result_bytes,
                )
                .map_err(|error| TypedOperationError::Database(Error::from(error)))?
                .ok_or(TypedOperationError::Adapter(
                    TypedAdapterError::StaleBinding,
                ))
        })
    }
}

fn execute_bulk(
    input_count: usize,
    limits: TypedBulkWriteLimits,
    mut attempt: impl FnMut(usize, usize) -> Result<DynamicTypedBatchAttempt, TypedOperationError>,
) -> Result<TypedBulkWriteProgress, TypedBulkWriteFailure> {
    let mut progress = TypedBulkWriteProgress::default();
    if !limits.is_valid() {
        return Err(TypedBulkWriteFailure {
            progress,
            error: TypedBulkWriteError::InvalidLimits,
        });
    }
    // Keep a successful reduced size for the remainder; do not repeatedly probe
    // the original oversized batch or re-encode every growing input prefix.
    let mut batch_rows = limits
        .max_batch_rows
        .min(DynamicTypedBatchAttempt::MAX_ROWS);
    while progress.next_input < input_count {
        let work_left = limits
            .max_attempted_rows
            .saturating_sub(progress.attempted_rows);
        if work_left == 0 || progress.batches.len() >= limits.max_commits {
            return Err(TypedBulkWriteFailure {
                progress,
                error: TypedBulkWriteError::WorkLimit,
            });
        }
        let count = batch_rows
            .min(work_left)
            .min(input_count - progress.next_input);
        progress.attempted_rows += count;
        match attempt(progress.next_input, count) {
            Ok(DynamicTypedBatchAttempt::Committed(results)) => {
                progress.batches.push(TypedBulkWriteBatch {
                    first_input: progress.next_input,
                    results,
                });
                progress.next_input += count;
            }
            Ok(DynamicTypedBatchAttempt::PrecommitByteRefusal(error)) if count == 1 => {
                return Err(TypedBulkWriteFailure {
                    progress,
                    error: TypedBulkWriteError::InputTooLarge(error.into()),
                });
            }
            Ok(DynamicTypedBatchAttempt::PrecommitByteRefusal(_)) => {
                batch_rows = count / 2;
            }
            Err(error) => {
                return Err(TypedBulkWriteFailure {
                    progress,
                    error: TypedBulkWriteError::Operation(error),
                });
            }
        }
    }
    Ok(progress)
}
