// 1️⃣ Module declarations
pub(crate) mod access;
pub(crate) mod contracts;
pub(crate) mod cursor;
pub(crate) mod diagnostics;
pub(crate) mod identity;
pub(crate) mod policy;
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
pub(in crate::db) mod relation;

// 2️⃣ Public re-exports (Tier-2 API surface)
pub use codec::cursor::{decode_cursor, encode_cursor};
pub use contracts::ReadConsistency;
pub use contracts::ValidateError;
pub use contracts::{CoercionId, CompareOp, ComparePredicate, Predicate, UnsupportedQueryFeature};
pub use data::DataStore;
pub(crate) use data::StorageKey;
pub use diagnostics::StorageReport;
pub use executor::{ExecutionAccessPathVariant, ExecutionOptimization, ExecutionTrace};
pub use identity::{EntityName, IndexName};
pub use index::IndexStore;
pub use query::{
    builder::field::FieldRef,
    expr::{FilterExpr, SortExpr},
    fluent::{
        delete::FluentDeleteQuery,
        load::{FluentLoadQuery, PagedLoadQuery},
    },
    intent::{DeleteSpec, IntentError, LoadSpec, Query, QueryError, QueryMode},
    plan::{OrderDirection, PlanError},
};
pub use registry::StoreRegistry;
pub use relation::validate_delete_strong_relations_for_source;
pub use response::{Response, ResponseError, Row, WriteBatchResponse, WriteResponse};
pub use session::DbSession;

// 3️⃣ Internal imports (implementation wiring)
use crate::{
    db::{
        commit::{
            CommitRowOp, PreparedRowCommitOp, ensure_recovered, prepare_row_commit_for_entity,
            rebuild_secondary_indexes_from_rows, replay_commit_marker_row_ops,
        },
        data::RawDataKey,
        executor::Context,
        relation::StrongRelationDeleteValidateFn,
    },
    error::InternalError,
    traits::{CanisterKind, EntityIdentity, EntityKind, EntityValue},
    value::Value,
};
use std::{collections::BTreeSet, marker::PhantomData, thread::LocalKey};

///
/// PagedLoadExecution
///
/// Cursor-paged load response with optional continuation cursor bytes.
///

#[derive(Debug)]
pub struct PagedLoadExecution<E: EntityKind> {
    response: Response<E>,
    continuation_cursor: Option<Vec<u8>>,
}

impl<E: EntityKind> PagedLoadExecution<E> {
    /// Create a paged load execution payload.
    #[must_use]
    pub const fn new(response: Response<E>, continuation_cursor: Option<Vec<u8>>) -> Self {
        Self {
            response,
            continuation_cursor,
        }
    }

    /// Borrow the paged response rows.
    #[must_use]
    pub const fn response(&self) -> &Response<E> {
        &self.response
    }

    /// Borrow the optional continuation cursor bytes.
    #[must_use]
    pub fn continuation_cursor(&self) -> Option<&[u8]> {
        self.continuation_cursor.as_deref()
    }

    /// Consume this payload and return `(response, continuation_cursor)`.
    #[must_use]
    pub fn into_parts(self) -> (Response<E>, Option<Vec<u8>>) {
        (self.response, self.continuation_cursor)
    }
}

impl<E: EntityKind> From<(Response<E>, Option<Vec<u8>>)> for PagedLoadExecution<E> {
    fn from(value: (Response<E>, Option<Vec<u8>>)) -> Self {
        let (response, continuation_cursor) = value;

        Self::new(response, continuation_cursor)
    }
}

impl<E: EntityKind> From<PagedLoadExecution<E>> for (Response<E>, Option<Vec<u8>>) {
    fn from(value: PagedLoadExecution<E>) -> Self {
        value.into_parts()
    }
}

///
/// PagedLoadExecutionWithTrace
///
/// Cursor-paged load response plus optional execution trace details.
///

#[derive(Debug)]
pub struct PagedLoadExecutionWithTrace<E: EntityKind> {
    execution: PagedLoadExecution<E>,
    execution_trace: Option<ExecutionTrace>,
}

impl<E: EntityKind> PagedLoadExecutionWithTrace<E> {
    /// Create a traced paged load execution payload.
    #[must_use]
    pub const fn new(
        response: Response<E>,
        continuation_cursor: Option<Vec<u8>>,
        execution_trace: Option<ExecutionTrace>,
    ) -> Self {
        Self {
            execution: PagedLoadExecution::new(response, continuation_cursor),
            execution_trace,
        }
    }

    /// Borrow the paged execution payload.
    #[must_use]
    pub const fn execution(&self) -> &PagedLoadExecution<E> {
        &self.execution
    }

    /// Borrow the paged response rows.
    #[must_use]
    pub const fn response(&self) -> &Response<E> {
        self.execution.response()
    }

    /// Borrow the optional continuation cursor bytes.
    #[must_use]
    pub fn continuation_cursor(&self) -> Option<&[u8]> {
        self.execution.continuation_cursor()
    }

    /// Borrow optional execution trace details.
    #[must_use]
    pub const fn execution_trace(&self) -> Option<&ExecutionTrace> {
        self.execution_trace.as_ref()
    }

    /// Consume this payload and drop trace details.
    #[must_use]
    pub fn into_execution(self) -> PagedLoadExecution<E> {
        self.execution
    }

    /// Consume this payload and return `(response, continuation_cursor, trace)`.
    #[must_use]
    pub fn into_parts(self) -> (Response<E>, Option<Vec<u8>>, Option<ExecutionTrace>) {
        let (response, continuation_cursor) = self.execution.into_parts();

        (response, continuation_cursor, self.execution_trace)
    }
}

impl<E: EntityKind> From<(Response<E>, Option<Vec<u8>>, Option<ExecutionTrace>)>
    for PagedLoadExecutionWithTrace<E>
{
    fn from(value: (Response<E>, Option<Vec<u8>>, Option<ExecutionTrace>)) -> Self {
        let (response, continuation_cursor, execution_trace) = value;

        Self::new(response, continuation_cursor, execution_trace)
    }
}

impl<E: EntityKind> From<PagedLoadExecutionWithTrace<E>>
    for (Response<E>, Option<Vec<u8>>, Option<ExecutionTrace>)
{
    fn from(value: PagedLoadExecutionWithTrace<E>) -> Self {
        value.into_parts()
    }
}

///
/// GroupedRow
///
/// One grouped result row: ordered grouping key values plus ordered aggregate outputs.
/// Group/aggregate vectors preserve query declaration order.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupedRow {
    group_key: Vec<Value>,
    aggregate_values: Vec<Value>,
}

impl GroupedRow {
    /// Construct one grouped row payload.
    #[must_use]
    pub const fn new(group_key: Vec<Value>, aggregate_values: Vec<Value>) -> Self {
        Self {
            group_key,
            aggregate_values,
        }
    }

    /// Borrow grouped key values.
    #[must_use]
    pub const fn group_key(&self) -> &[Value] {
        self.group_key.as_slice()
    }

    /// Borrow aggregate output values.
    #[must_use]
    pub const fn aggregate_values(&self) -> &[Value] {
        self.aggregate_values.as_slice()
    }
}

///
/// PagedGroupedExecution
///
/// Cursor-paged grouped execution payload with optional grouped continuation cursor bytes.
///
#[derive(Debug)]
pub struct PagedGroupedExecution {
    rows: Vec<GroupedRow>,
    continuation_cursor: Option<Vec<u8>>,
}

impl PagedGroupedExecution {
    /// Construct one grouped paged execution payload.
    #[must_use]
    pub const fn new(rows: Vec<GroupedRow>, continuation_cursor: Option<Vec<u8>>) -> Self {
        Self {
            rows,
            continuation_cursor,
        }
    }

    /// Borrow grouped rows.
    #[must_use]
    pub const fn rows(&self) -> &[GroupedRow] {
        self.rows.as_slice()
    }

    /// Borrow optional continuation cursor bytes.
    #[must_use]
    pub fn continuation_cursor(&self) -> Option<&[u8]> {
        self.continuation_cursor.as_deref()
    }

    /// Consume into grouped rows and continuation cursor bytes.
    #[must_use]
    pub fn into_parts(self) -> (Vec<GroupedRow>, Option<Vec<u8>>) {
        (self.rows, self.continuation_cursor)
    }
}

///
/// PagedGroupedExecutionWithTrace
///
/// Cursor-paged grouped execution payload plus optional route/execution trace.
///
#[derive(Debug)]
pub struct PagedGroupedExecutionWithTrace {
    execution: PagedGroupedExecution,
    execution_trace: Option<ExecutionTrace>,
}

impl PagedGroupedExecutionWithTrace {
    /// Construct one traced grouped paged execution payload.
    #[must_use]
    pub const fn new(
        rows: Vec<GroupedRow>,
        continuation_cursor: Option<Vec<u8>>,
        execution_trace: Option<ExecutionTrace>,
    ) -> Self {
        Self {
            execution: PagedGroupedExecution::new(rows, continuation_cursor),
            execution_trace,
        }
    }

    /// Borrow grouped execution payload.
    #[must_use]
    pub const fn execution(&self) -> &PagedGroupedExecution {
        &self.execution
    }

    /// Borrow grouped rows.
    #[must_use]
    pub const fn rows(&self) -> &[GroupedRow] {
        self.execution.rows()
    }

    /// Borrow optional continuation cursor bytes.
    #[must_use]
    pub fn continuation_cursor(&self) -> Option<&[u8]> {
        self.execution.continuation_cursor()
    }

    /// Borrow optional execution trace details.
    #[must_use]
    pub const fn execution_trace(&self) -> Option<&ExecutionTrace> {
        self.execution_trace.as_ref()
    }

    /// Consume payload and drop trace details.
    #[must_use]
    pub fn into_execution(self) -> PagedGroupedExecution {
        self.execution
    }

    /// Consume into grouped rows, continuation cursor bytes, and optional trace.
    #[must_use]
    pub fn into_parts(self) -> (Vec<GroupedRow>, Option<Vec<u8>>, Option<ExecutionTrace>) {
        let (rows, continuation_cursor) = self.execution.into_parts();

        (rows, continuation_cursor, self.execution_trace)
    }
}

///
/// Db
/// A handle to the set of stores registered for a specific canister domain.
///

pub struct Db<C: CanisterKind> {
    store: &'static LocalKey<StoreRegistry>,
    entity_runtime_hooks: &'static [EntityRuntimeHooks<C>],
    _marker: PhantomData<C>,
}

impl<C: CanisterKind> Db<C> {
    #[must_use]
    pub const fn new(store: &'static LocalKey<StoreRegistry>) -> Self {
        Self::new_with_hooks(store, &[])
    }

    #[must_use]
    pub const fn new_with_hooks(
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

    pub(crate) fn with_store_registry<R>(&self, f: impl FnOnce(&StoreRegistry) -> R) -> R {
        self.store.with(|reg| f(reg))
    }

    pub fn storage_report(
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
        if deleted_target_keys.is_empty() {
            return Ok(());
        }

        for hooks in self.entity_runtime_hooks {
            (hooks.validate_delete_strong_relations)(self, target_path, deleted_target_keys)?;
        }

        Ok(())
    }
}

///
/// EntityRuntimeHooks
///
/// Per-entity runtime callbacks used for commit preparation and delete-side
/// strong relation validation.
///

pub struct EntityRuntimeHooks<C: CanisterKind> {
    pub(crate) entity_name: &'static str,
    pub(crate) entity_path: &'static str,
    pub(in crate::db) prepare_row_commit:
        fn(&Db<C>, &CommitRowOp) -> Result<PreparedRowCommitOp, InternalError>,
    pub(crate) validate_delete_strong_relations: StrongRelationDeleteValidateFn<C>,
}

impl<C: CanisterKind> EntityRuntimeHooks<C> {
    #[must_use]
    pub(in crate::db) const fn new(
        entity_name: &'static str,
        entity_path: &'static str,
        prepare_row_commit: fn(&Db<C>, &CommitRowOp) -> Result<PreparedRowCommitOp, InternalError>,
        validate_delete_strong_relations: StrongRelationDeleteValidateFn<C>,
    ) -> Self {
        Self {
            entity_name,
            entity_path,
            prepare_row_commit,
            validate_delete_strong_relations,
        }
    }

    #[must_use]
    pub const fn for_entity<E>(
        validate_delete_strong_relations: StrongRelationDeleteValidateFn<C>,
    ) -> Self
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Self::new(
            <E as EntityIdentity>::ENTITY_NAME,
            E::PATH,
            prepare_row_commit_for_entity::<E>,
            validate_delete_strong_relations,
        )
    }
}

impl<C: CanisterKind> Db<C> {
    #[must_use]
    pub(crate) const fn has_runtime_hooks(&self) -> bool {
        !self.entity_runtime_hooks.is_empty()
    }

    // Resolve exactly one runtime hook for a persisted entity name.
    // Duplicate matches are treated as store invariants.
    pub(crate) fn runtime_hook_for_entity_name(
        &self,
        entity_name: &str,
    ) -> Result<&EntityRuntimeHooks<C>, InternalError> {
        let mut matched = None;
        for hooks in self.entity_runtime_hooks {
            if hooks.entity_name != entity_name {
                continue;
            }

            if matched.is_some() {
                return Err(InternalError::store_invariant(format!(
                    "duplicate runtime hooks for entity name '{entity_name}'"
                )));
            }

            matched = Some(hooks);
        }

        matched.ok_or_else(|| {
            InternalError::store_unsupported(format!(
                "unsupported entity name in data store: '{entity_name}'"
            ))
        })
    }

    // Resolve exactly one runtime hook for a persisted entity path.
    // Duplicate matches are treated as store invariants.
    pub(crate) fn runtime_hook_for_entity_path(
        &self,
        entity_path: &str,
    ) -> Result<&EntityRuntimeHooks<C>, InternalError> {
        let mut matched = None;
        for hooks in self.entity_runtime_hooks {
            if hooks.entity_path != entity_path {
                continue;
            }

            if matched.is_some() {
                return Err(InternalError::store_invariant(format!(
                    "duplicate runtime hooks for entity path '{entity_path}'"
                )));
            }

            matched = Some(hooks);
        }

        matched.ok_or_else(|| InternalError::unsupported_entity_path(entity_path))
    }
}

impl<C: CanisterKind> Copy for Db<C> {}

impl<C: CanisterKind> Clone for Db<C> {
    fn clone(&self) -> Self {
        *self
    }
}
