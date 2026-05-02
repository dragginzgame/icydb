//! Module: query::builder::aggregate::boundary
//! Responsibility: aggregate terminal request and output DTOs shared by fluent
//! builders, session routing, and executor aggregate adapters.
//! Does not own: executor fold semantics, fast-path selection, or storage scans.
//! Boundary: query-facing aggregate contracts consumed by executor internals.

use crate::{
    db::{
        data::DataKey,
        query::plan::{AggregateKind, FieldSlot},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue, KeyValueCodec},
    types::Id,
    value::{StorageKey, Value, storage_key_as_runtime_value},
};

///
/// ScalarTerminalBoundaryRequest
///
/// Query-owned request contract for scalar aggregate terminal calls.
/// Fluent aggregate descriptors produce this shape before session routing
/// hands it to executor aggregate adapters.
///

pub(in crate::db) enum ScalarTerminalBoundaryRequest {
    Count,
    Exists,
    IdTerminal {
        kind: AggregateKind,
    },
    IdBySlot {
        kind: AggregateKind,
        target_field: FieldSlot,
    },
    NthBySlot {
        target_field: FieldSlot,
        nth: usize,
    },
    MedianBySlot {
        target_field: FieldSlot,
    },
    MinMaxBySlot {
        target_field: FieldSlot,
    },
}

///
/// ScalarTerminalBoundaryOutput
///
/// Query-owned output contract for scalar aggregate terminal calls.
/// Executor adapters return this shape so typed session/fluent APIs can decode
/// concrete IDs and counters without depending on executor internals.
///

pub(in crate::db) enum ScalarTerminalBoundaryOutput {
    Count(u32),
    Exists(bool),
    Id(Option<StorageKey>),
    IdPair(Option<(StorageKey, StorageKey)>),
}

impl ScalarTerminalBoundaryOutput {
    // Build one canonical scalar terminal boundary mismatch on the owner type.
    fn output_kind_mismatch(message: &'static str) -> InternalError {
        InternalError::query_executor_invariant(message)
    }

    // Decode COUNT boundary output while preserving request/output mismatch context.
    pub(in crate::db) fn into_count(self) -> Result<u32, InternalError> {
        match self {
            Self::Count(value) => Ok(value),
            _ => Err(Self::output_kind_mismatch(
                "scalar terminal boundary COUNT output kind mismatch",
            )),
        }
    }

    // Decode EXISTS boundary output while preserving request/output mismatch context.
    pub(in crate::db) fn into_exists(self) -> Result<bool, InternalError> {
        match self {
            Self::Exists(value) => Ok(value),
            _ => Err(Self::output_kind_mismatch(
                "scalar terminal boundary EXISTS output kind mismatch",
            )),
        }
    }

    // Decode id-returning boundary output while preserving request/output mismatch context.
    pub(in crate::db) fn into_id<E>(self) -> Result<Option<Id<E>>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        match self {
            Self::Id(value) => value.map(decode_storage_key_to_id::<E>).transpose(),
            _ => Err(Self::output_kind_mismatch(
                "scalar terminal boundary id output kind mismatch",
            )),
        }
    }

    // Decode paired-id boundary output while preserving request/output mismatch context.
    pub(in crate::db) fn into_id_pair<E>(self) -> Result<Option<(Id<E>, Id<E>)>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        match self {
            Self::IdPair(value) => value
                .map(|(left, right)| {
                    Ok((
                        decode_storage_key_to_id::<E>(left)?,
                        decode_storage_key_to_id::<E>(right)?,
                    ))
                })
                .transpose(),
            _ => Err(Self::output_kind_mismatch(
                "scalar terminal boundary id-pair output kind mismatch",
            )),
        }
    }
}

// Re-enter typed identity only at the terminal API boundary.
fn decode_storage_key_to_id<E>(key: StorageKey) -> Result<Id<E>, InternalError>
where
    E: EntityKind + EntityValue,
{
    let value = storage_key_as_runtime_value(&key);
    let decoded = <E::Key as KeyValueCodec>::from_key_value(&value).ok_or_else(|| {
        InternalError::store_corruption(format!(
            "scalar aggregate output primary key decode failed: {value:?}"
        ))
    })?;

    Ok(Id::from_key(decoded))
}

///
/// ScalarNumericFieldBoundaryRequest
///
/// Query-owned request contract for numeric field aggregate terminal calls.
/// Executor aggregate code derives fold operations from this semantic request
/// without exposing executor-prepared operation types to query builders.
///

#[derive(Clone, Copy)]
pub(in crate::db) enum ScalarNumericFieldBoundaryRequest {
    Sum,
    SumDistinct,
    Avg,
    AvgDistinct,
}

///
/// ScalarProjectionBoundaryRequest
///
/// Query-owned request contract for scalar field projection aggregate calls.
/// Builder descriptors use this shape for value, distinct-value, and
/// terminal-value aggregate projection requests.
///

pub(in crate::db) enum ScalarProjectionBoundaryRequest {
    Values,
    DistinctValues,
    CountDistinct,
    ValuesWithIds,
    TerminalValue { terminal_kind: AggregateKind },
}

///
/// ScalarProjectionBoundaryOutput
///
/// Query-owned output contract for scalar projection aggregate calls.
/// Executor projection adapters return this shape so session/fluent layers can
/// decode IDs and values without importing executor projection internals.
///

pub(in crate::db) enum ScalarProjectionBoundaryOutput {
    Count(u32),
    Values(Vec<Value>),
    ValuesWithDataKeys(Vec<(DataKey, Value)>),
    TerminalValue(Option<Value>),
}

impl ScalarProjectionBoundaryOutput {
    // Build the canonical boundary mismatch for projection output decoding.
    fn output_kind_mismatch(message: &'static str) -> InternalError {
        InternalError::query_executor_invariant(message)
    }

    // Decode one plain-value projection boundary output.
    pub(in crate::db) fn into_values(self) -> Result<Vec<Value>, InternalError> {
        match self {
            Self::Values(values) => Ok(values),
            _ => Err(Self::output_kind_mismatch(
                "scalar projection boundary values output kind mismatch",
            )),
        }
    }

    // Decode one count-distinct projection boundary output.
    pub(in crate::db) fn into_count(self) -> Result<u32, InternalError> {
        match self {
            Self::Count(value) => Ok(value),
            _ => Err(Self::output_kind_mismatch(
                "scalar projection boundary count output kind mismatch",
            )),
        }
    }

    // Decode one `(id, value)` projection boundary output.
    pub(in crate::db) fn into_values_with_ids<E>(self) -> Result<Vec<(Id<E>, Value)>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        match self {
            Self::ValuesWithDataKeys(values) => values
                .into_iter()
                .map(|(data_key, value)| Ok((Id::from_key(data_key.try_key::<E>()?), value)))
                .collect(),
            _ => Err(Self::output_kind_mismatch(
                "scalar projection boundary values-with-ids output kind mismatch",
            )),
        }
    }

    // Decode one terminal-value projection boundary output.
    pub(in crate::db) fn into_terminal_value(self) -> Result<Option<Value>, InternalError> {
        match self {
            Self::TerminalValue(value) => Ok(value),
            _ => Err(Self::output_kind_mismatch(
                "scalar projection boundary terminal-value output kind mismatch",
            )),
        }
    }
}
