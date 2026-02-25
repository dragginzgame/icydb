mod access_stream;
pub(super) mod aggregate;
mod context;
mod cursor;
mod delete;
mod direction;
mod executable_plan;
mod execution_plan;
mod fold;
mod kernel;
pub(super) mod load;
mod mutation;
mod ordered_key_stream;
mod physical_path;
mod plan;
mod query_bridge;
pub(super) mod route;
#[cfg(test)]
mod tests;
mod window;

pub(in crate::db) use crate::db::lowering::{LoweredIndexPrefixSpec, LoweredIndexRangeSpec};
pub(super) use access_stream::*;
pub(super) use context::*;
pub(in crate::db) use cursor::{
    PlannedCursor, decode_pk_cursor_boundary, decode_typed_primary_key_cursor_slot, plan_cursor,
    revalidate_planned_cursor, validate_index_range_anchor,
    validate_index_range_boundary_anchor_consistency,
};
pub(super) use delete::DeleteExecutor;
pub(crate) use direction::normalize_ordered_keys;
pub(in crate::db) use executable_plan::ExecutablePlan;
pub(in crate::db::executor) use execution_plan::ExecutionPlan;
pub(in crate::db::executor) use kernel::{ExecutionKernel, IndexPredicateCompileMode};
pub(super) use load::LoadExecutor;
pub use load::{ExecutionAccessPathVariant, ExecutionOptimization, ExecutionTrace};
pub(super) use mutation::save::SaveExecutor;
pub(super) use ordered_key_stream::{
    BudgetedOrderedKeyStream, IntersectOrderedKeyStream, KeyOrderComparator, MergeOrderedKeyStream,
    OrderedKeyStream, OrderedKeyStreamBox, VecOrderedKeyStream,
};
pub(in crate::db) use window::compute_page_window;

// Design notes:
// - SchemaInfo is the planner-visible schema (relational attributes). Executors may see
//   additional tuple payload not represented in SchemaInfo.
// - Unsupported or opaque values are treated as incomparable; executor validation may
//   skip type checks for these values.
// - ORDER BY is stable; incomparable values preserve input order.
// - Corruption indicates invalid persisted bytes or store mismatches; invariant violations
//   indicate executor/planner contract breaches.

use crate::{
    db::{
        data::DataKey,
        query::{plan::AccessPlannedQuery, predicate::PredicateFieldSlots},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    traits::EntityKind,
};
use thiserror::Error as ThisError;

pub(super) fn compile_predicate_slots<E: EntityKind>(
    plan: &AccessPlannedQuery<E::Key>,
) -> Option<PredicateFieldSlots> {
    plan.predicate
        .as_ref()
        .map(PredicateFieldSlots::resolve::<E>)
}

///
/// ExecutorError
///

#[derive(Debug, ThisError)]
pub(crate) enum ExecutorError {
    #[error("corruption detected ({origin}): {message}")]
    Corruption {
        origin: ErrorOrigin,
        message: String,
    },

    #[error("data key exists: {0}")]
    KeyExists(DataKey),
}

impl ExecutorError {
    pub(crate) const fn class(&self) -> ErrorClass {
        match self {
            Self::KeyExists(_) => ErrorClass::Conflict,
            Self::Corruption { .. } => ErrorClass::Corruption,
        }
    }

    pub(crate) const fn origin(&self) -> ErrorOrigin {
        match self {
            Self::KeyExists(_) => ErrorOrigin::Store,
            Self::Corruption { origin, .. } => *origin,
        }
    }

    pub(crate) fn corruption(origin: ErrorOrigin, message: impl Into<String>) -> Self {
        Self::Corruption {
            origin,
            message: message.into(),
        }
    }

    // Construct a store-origin corruption error with canonical taxonomy.
    pub(crate) fn store_corruption(message: impl Into<String>) -> Self {
        Self::corruption(ErrorOrigin::Store, message)
    }

    // Construct a serialize-origin corruption error with canonical taxonomy.
    pub(crate) fn serialize_corruption(message: impl Into<String>) -> Self {
        Self::corruption(ErrorOrigin::Serialize, message)
    }

    // Construct a store-origin corruption error from displayable source context.
    pub(crate) fn store_corruption_from(source: impl std::fmt::Display) -> Self {
        Self::store_corruption(source.to_string())
    }
}

impl From<ExecutorError> for InternalError {
    fn from(err: ExecutorError) -> Self {
        Self::classified(err.class(), err.origin(), err.to_string())
    }
}
