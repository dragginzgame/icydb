//! Module: db
//!
//! Responsibility: root subsystem wiring, façade re-exports, and runtime hook contracts.
//! Does not own: feature semantics delegated to child modules (`query`, `executor`, etc.).
//! Boundary: top-level db API and internal orchestration entrypoints.

pub(crate) mod access;
pub(crate) mod contracts;
pub(crate) mod cursor;
pub(crate) mod diagnostics;
pub(crate) mod identity;
pub(crate) mod predicate;
pub(crate) mod query;
pub(crate) mod registry;
pub(crate) mod response;
pub(crate) mod session;

pub(in crate::db) mod codec;
pub(in crate::db) mod commit;
pub(in crate::db) mod data;
pub(in crate::db) mod direction;
pub(in crate::db) mod executor;
pub(in crate::db) mod index;
pub(in crate::db) mod numeric;
pub(in crate::db) mod relation;

use crate::{
    db::{
        commit::{
            CommitRowOp, PreparedRowCommitOp, ensure_recovered,
            rebuild_secondary_indexes_from_rows, replay_commit_marker_row_ops,
        },
        data::RawDataKey,
        executor::Context,
    },
    error::InternalError,
    traits::{CanisterKind, EntityKind, EntityValue},
};
use std::{collections::BTreeSet, marker::PhantomData, thread::LocalKey};

pub use codec::cursor::{decode_cursor, encode_cursor};
pub use commit::EntityRuntimeHooks;
pub use data::DataStore;
pub(crate) use data::StorageKey;
pub use diagnostics::StorageReport;
pub use executor::{ExecutionAccessPathVariant, ExecutionOptimization, ExecutionTrace};
pub use identity::{EntityName, IndexName};
pub use index::IndexStore;
pub use predicate::{
    CoercionId, CompareOp, ComparePredicate, MissingRowPolicy, Predicate, UnsupportedQueryFeature,
    ValidateError,
};
pub use query::{
    builder::{
        AggregateExpr, FieldRef, count, count_by, exists, first, last, max, max_by, min, min_by,
        sum,
    },
    expr::{FilterExpr, SortExpr},
    fluent::{
        delete::FluentDeleteQuery,
        load::{FluentLoadQuery, PagedLoadQuery},
    },
    intent::{
        CompiledQuery, DeleteSpec, IntentError, LoadSpec, Query, QueryError, QueryExecuteError,
        QueryMode,
    },
    plan::{OrderDirection, PlanError},
};
pub use registry::StoreRegistry;
pub use response::{
    GroupedRow, PagedGroupedExecution, PagedGroupedExecutionWithTrace, PagedLoadExecution,
    PagedLoadExecutionWithTrace, ProjectedRow, Response, ResponseError, Row, WriteBatchResponse,
    WriteResponse,
};
pub use session::DbSession;

///
/// Db
/// A handle to the set of stores registered for a specific canister domain.
///

pub(crate) struct Db<C: CanisterKind> {
    store: &'static LocalKey<StoreRegistry>,
    entity_runtime_hooks: &'static [EntityRuntimeHooks<C>],
    _marker: PhantomData<C>,
}

impl<C: CanisterKind> Db<C> {
    /// Construct a db handle without per-entity runtime hooks.
    #[must_use]
    #[cfg(test)]
    pub(crate) const fn new(store: &'static LocalKey<StoreRegistry>) -> Self {
        Self::new_with_hooks(store, &[])
    }

    /// Construct a db handle with explicit per-entity runtime hook wiring.
    #[must_use]
    pub(crate) const fn new_with_hooks(
        store: &'static LocalKey<StoreRegistry>,
        entity_runtime_hooks: &'static [EntityRuntimeHooks<C>],
    ) -> Self {
        Self {
            store,
            entity_runtime_hooks,
            _marker: PhantomData,
        }
    }

    #[must_use]
    pub(crate) const fn context<E>(&self) -> Context<'_, E>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Context::new(self)
    }

    /// Return a recovery-guarded context for read paths.
    ///
    /// This enforces startup recovery and a fast persisted-marker check so reads
    /// do not proceed while an incomplete commit is pending replay.
    pub(crate) fn recovered_context<E>(&self) -> Result<Context<'_, E>, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        ensure_recovered(self)?;

        Ok(Context::new(self))
    }

    /// Ensure startup/in-progress commit recovery has been applied.
    pub(crate) fn ensure_recovered_state(&self) -> Result<(), InternalError> {
        ensure_recovered(self)
    }

    /// Execute one closure against the registered store set.
    pub(crate) fn with_store_registry<R>(&self, f: impl FnOnce(&StoreRegistry) -> R) -> R {
        self.store.with(|reg| f(reg))
    }

    /// Build one storage diagnostics report for registered stores/entities.
    pub(crate) fn storage_report(
        &self,
        name_to_path: &[(&'static str, &'static str)],
    ) -> Result<StorageReport, InternalError> {
        diagnostics::storage_report(self, name_to_path)
    }

    pub(in crate::db) fn prepare_row_commit_op(
        &self,
        op: &CommitRowOp,
    ) -> Result<PreparedRowCommitOp, InternalError> {
        let hooks = self.runtime_hook_for_entity_path(op.entity_path.as_str())?;

        (hooks.prepare_row_commit)(self, op)
    }

    pub(in crate::db) fn replay_commit_marker_row_ops(
        &self,
        row_ops: &[CommitRowOp],
    ) -> Result<(), InternalError> {
        replay_commit_marker_row_ops(self, row_ops)
    }

    pub(in crate::db) fn rebuild_secondary_indexes_from_rows(&self) -> Result<(), InternalError> {
        rebuild_secondary_indexes_from_rows(self)
    }

    // Validate strong relation constraints for delete-selected target keys.
    pub(crate) fn validate_delete_strong_relations(
        &self,
        target_path: &str,
        deleted_target_keys: &BTreeSet<RawDataKey>,
    ) -> Result<(), InternalError> {
        // Skip hook traversal when no target keys were deleted.
        if deleted_target_keys.is_empty() {
            return Ok(());
        }

        // Delegate delete-side relation validation to each entity runtime hook.
        for hooks in self.entity_runtime_hooks {
            (hooks.validate_delete_strong_relations)(self, target_path, deleted_target_keys)?;
        }

        Ok(())
    }
}

impl<C: CanisterKind> Db<C> {
    #[must_use]
    /// Return whether this db has any registered runtime hook callbacks.
    pub(crate) const fn has_runtime_hooks(&self) -> bool {
        commit::has_runtime_hooks(self.entity_runtime_hooks)
    }

    // Resolve exactly one runtime hook for a persisted entity name.
    // Duplicate matches are treated as store invariants.
    pub(crate) fn runtime_hook_for_entity_name(
        &self,
        entity_name: &str,
    ) -> Result<&EntityRuntimeHooks<C>, InternalError> {
        commit::resolve_runtime_hook_by_name(self.entity_runtime_hooks, entity_name)
    }

    // Resolve exactly one runtime hook for a persisted entity path.
    // Duplicate matches are treated as store invariants.
    pub(crate) fn runtime_hook_for_entity_path(
        &self,
        entity_path: &str,
    ) -> Result<&EntityRuntimeHooks<C>, InternalError> {
        commit::resolve_runtime_hook_by_path(self.entity_runtime_hooks, entity_path)
    }
}

impl<C: CanisterKind> Copy for Db<C> {}

impl<C: CanisterKind> Clone for Db<C> {
    fn clone(&self) -> Self {
        *self
    }
}
