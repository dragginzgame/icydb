//! Module: query::fluent::load::runtime
//! Responsibility: neutral fluent terminal runtime outputs shared by fluent
//! terminal APIs and the session execution adapter.
//! Does not own: executor request types or physical execution routing.
//! Boundary: carries query-facing terminal results without depending on executor internals.

use crate::{error::InternalError, traits::EntityKey, types::Id, value::Value};

/// Optional pair of typed entity ids returned by paired scalar terminals.
pub(in crate::db) type FluentScalarTerminalIdPair<E> = Option<(Id<E>, Id<E>)>;

///
/// FluentScalarTerminalOutput
///
/// FluentScalarTerminalOutput is the query-owned scalar terminal result
/// boundary used by fluent terminals after session execution has decoded any
/// executor-specific storage identity.
/// It keeps fluent query code independent from executor aggregate output
/// enums while preserving the same mismatch checks at terminal call sites.
///

pub(in crate::db) enum FluentScalarTerminalOutput<E: EntityKey> {
    Count(u32),
    Exists(bool),
    Id(Option<Id<E>>),
    IdPair(FluentScalarTerminalIdPair<E>),
}

impl<E> FluentScalarTerminalOutput<E>
where
    E: EntityKey,
{
    // Build one canonical scalar terminal boundary mismatch on the query-owned
    // output type while preserving the previous executor-facing messages.
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
    pub(in crate::db) fn into_id(self) -> Result<Option<Id<E>>, InternalError> {
        match self {
            Self::Id(value) => Ok(value),
            _ => Err(Self::output_kind_mismatch(
                "scalar terminal boundary id output kind mismatch",
            )),
        }
    }

    // Decode paired-id boundary output while preserving request/output mismatch context.
    pub(in crate::db) fn into_id_pair(
        self,
    ) -> Result<FluentScalarTerminalIdPair<E>, InternalError> {
        match self {
            Self::IdPair(value) => Ok(value),
            _ => Err(Self::output_kind_mismatch(
                "scalar terminal boundary id-pair output kind mismatch",
            )),
        }
    }
}

///
/// FluentProjectionTerminalOutput
///
/// FluentProjectionTerminalOutput is the query-owned field projection terminal
/// result boundary used after session execution has decoded storage keys into
/// typed ids.
/// It keeps fluent projection terminals free from executor aggregate output
/// enums while preserving the existing projection mismatch behavior.
///

pub(in crate::db) enum FluentProjectionTerminalOutput<E: EntityKey> {
    Count(u32),
    Values(Vec<Value>),
    ValuesWithIds(Vec<(Id<E>, Value)>),
    TerminalValue(Option<Value>),
}

impl<E> FluentProjectionTerminalOutput<E>
where
    E: EntityKey,
{
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
    pub(in crate::db) fn into_values_with_ids(self) -> Result<Vec<(Id<E>, Value)>, InternalError> {
        match self {
            Self::ValuesWithIds(values) => Ok(values),
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
