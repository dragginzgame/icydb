pub(crate) mod commit;
pub(crate) mod cursor;
pub(crate) mod decode;
pub(crate) mod executor;
pub(crate) mod identity;
pub(crate) mod index;
pub(crate) mod query;
pub(crate) mod relation;
pub(crate) mod response;
pub(crate) mod store;

use crate::{
    db::{
        commit::{CommitRowOp, PreparedRowCommitOp, ensure_recovered},
        executor::{Context, DeleteExecutor, LoadExecutor, SaveExecutor},
        query::{
            ReadConsistency,
            intent::{Query, QueryError, QueryMode},
            plan::PlanError,
            session::{delete::SessionDeleteQuery, load::SessionLoadQuery},
        },
        relation::StrongRelationDeleteValidateFn,
        response::{Response, WriteBatchResponse, WriteResponse},
        store::{RawDataKey, StoreRegistry},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    obs::sink::{MetricsSink, with_metrics_sink},
    traits::{CanisterKind, EntityKind, EntityValue},
};
use std::{collections::BTreeSet, marker::PhantomData, thread::LocalKey};

/// re-exports
pub use identity::{EntityName, IndexName};

///
/// Db
///
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

    pub(crate) fn with_store_registry<R>(&self, f: impl FnOnce(&StoreRegistry) -> R) -> R {
        self.store.with(|reg| f(reg))
    }

    pub(crate) fn prepare_row_commit_op(
        &self,
        op: &CommitRowOp,
    ) -> Result<PreparedRowCommitOp, InternalError> {
        let hooks = self
            .entity_runtime_hooks
            .iter()
            .find(|hooks| hooks.entity_path == op.entity_path.as_str())
            .ok_or_else(|| InternalError::unsupported_entity_path(op.entity_path.as_str()))?;

        (hooks.prepare_row_commit)(self, op)
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
    pub(crate) prepare_row_commit:
        fn(&Db<C>, &CommitRowOp) -> Result<PreparedRowCommitOp, InternalError>,
    pub(crate) validate_delete_strong_relations: StrongRelationDeleteValidateFn<C>,
}

impl<C: CanisterKind> EntityRuntimeHooks<C> {
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn new(
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
}

impl<C: CanisterKind> Db<C> {
    #[must_use]
    pub(crate) const fn has_runtime_hooks(&self) -> bool {
        !self.entity_runtime_hooks.is_empty()
    }

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
                return Err(InternalError::new(
                    ErrorClass::InvariantViolation,
                    ErrorOrigin::Store,
                    format!("duplicate runtime hooks for entity name '{entity_name}'"),
                ));
            }

            matched = Some(hooks);
        }

        matched.ok_or_else(|| {
            InternalError::new(
                ErrorClass::Unsupported,
                ErrorOrigin::Store,
                format!("unsupported entity name in data store: '{entity_name}'"),
            )
        })
    }
}

impl<C: CanisterKind> Copy for Db<C> {}

impl<C: CanisterKind> Clone for Db<C> {
    fn clone(&self) -> Self {
        *self
    }
}

///
/// DbSession
///
/// Session-scoped database handle with policy (debug, metrics) and execution routing.
///
pub struct DbSession<C: CanisterKind> {
    db: Db<C>,
    debug: bool,
    metrics: Option<&'static dyn MetricsSink>,
}

impl<C: CanisterKind> DbSession<C> {
    #[must_use]
    pub const fn new(db: Db<C>) -> Self {
        Self {
            db,
            debug: false,
            metrics: None,
        }
    }

    #[must_use]
    pub const fn debug(mut self) -> Self {
        self.debug = true;
        self
    }

    #[must_use]
    pub const fn metrics_sink(mut self, sink: &'static dyn MetricsSink) -> Self {
        self.metrics = Some(sink);
        self
    }

    fn with_metrics<T>(&self, f: impl FnOnce() -> T) -> T {
        if let Some(sink) = self.metrics {
            with_metrics_sink(sink, f)
        } else {
            f()
        }
    }

    // Shared save-facade wrapper keeps metrics wiring and response shaping uniform.
    fn execute_save_with<E, T, R>(
        &self,
        op: impl FnOnce(SaveExecutor<E>) -> Result<T, InternalError>,
        map: impl FnOnce(T) -> R,
    ) -> Result<R, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        let value = self.with_metrics(|| op(self.save_executor::<E>()))?;

        Ok(map(value))
    }

    // Shared save-facade wrappers keep response shape explicit at call sites.
    fn execute_save_entity<E>(
        &self,
        op: impl FnOnce(SaveExecutor<E>) -> Result<E, InternalError>,
    ) -> Result<WriteResponse<E>, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_with(op, WriteResponse::new)
    }

    fn execute_save_batch<E>(
        &self,
        op: impl FnOnce(SaveExecutor<E>) -> Result<Vec<E>, InternalError>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_with(op, WriteBatchResponse::new)
    }

    fn execute_save_view<E>(
        &self,
        op: impl FnOnce(SaveExecutor<E>) -> Result<E::ViewType, InternalError>,
    ) -> Result<E::ViewType, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_with(op, std::convert::identity)
    }

    // ---------------------------------------------------------------------
    // Query entry points (public, fluent)
    // ---------------------------------------------------------------------

    #[must_use]
    pub const fn load<E>(&self) -> SessionLoadQuery<'_, C, E>
    where
        E: EntityKind<Canister = C>,
    {
        SessionLoadQuery::new(self, Query::new(ReadConsistency::MissingOk))
    }

    #[must_use]
    pub const fn load_with_consistency<E>(
        &self,
        consistency: ReadConsistency,
    ) -> SessionLoadQuery<'_, C, E>
    where
        E: EntityKind<Canister = C>,
    {
        SessionLoadQuery::new(self, Query::new(consistency))
    }

    #[must_use]
    pub fn delete<E>(&self) -> SessionDeleteQuery<'_, C, E>
    where
        E: EntityKind<Canister = C>,
    {
        SessionDeleteQuery::new(self, Query::new(ReadConsistency::MissingOk).delete())
    }

    #[must_use]
    pub fn delete_with_consistency<E>(
        &self,
        consistency: ReadConsistency,
    ) -> SessionDeleteQuery<'_, C, E>
    where
        E: EntityKind<Canister = C>,
    {
        SessionDeleteQuery::new(self, Query::new(consistency).delete())
    }

    // ---------------------------------------------------------------------
    // Low-level executors (crate-internal; execution primitives)
    // ---------------------------------------------------------------------

    #[must_use]
    pub(crate) const fn load_executor<E>(&self) -> LoadExecutor<E>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        LoadExecutor::new(self.db, self.debug)
    }

    #[must_use]
    pub(crate) const fn delete_executor<E>(&self) -> DeleteExecutor<E>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        DeleteExecutor::new(self.db, self.debug)
    }

    #[must_use]
    pub(crate) const fn save_executor<E>(&self) -> SaveExecutor<E>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        SaveExecutor::new(self.db, self.debug)
    }

    // ---------------------------------------------------------------------
    // Query diagnostics / execution (internal routing)
    // ---------------------------------------------------------------------

    pub fn execute_query<E>(&self, query: &Query<E>) -> Result<Response<E>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        let plan = query.plan()?;

        let result = match query.mode() {
            QueryMode::Load(_) => self.with_metrics(|| self.load_executor::<E>().execute(plan)),
            QueryMode::Delete(_) => self.with_metrics(|| self.delete_executor::<E>().execute(plan)),
        };

        result.map_err(QueryError::Execute)
    }

    pub(crate) fn execute_load_query_paged<E>(
        &self,
        query: &Query<E>,
        cursor_token: Option<&str>,
    ) -> Result<(Response<E>, Option<Vec<u8>>), QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        let plan = query.plan()?;
        let cursor_bytes = match cursor_token {
            Some(token) => Some(cursor::decode_cursor(token).map_err(|reason| {
                QueryError::from(PlanError::InvalidContinuationCursor { reason })
            })?),
            None => None,
        };
        let boundary = plan.plan_cursor_boundary(cursor_bytes.as_deref())?;

        let page = self
            .with_metrics(|| self.load_executor::<E>().execute_paged(plan, boundary))
            .map_err(QueryError::Execute)?;

        Ok((page.items, page.next_cursor))
    }

    // ---------------------------------------------------------------------
    // High-level write API (public, intent-level)
    // ---------------------------------------------------------------------

    pub fn insert<E>(&self, entity: E) -> Result<WriteResponse<E>, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_entity(|save| save.insert(entity))
    }

    /// Insert a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn insert_many_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_batch(|save| save.insert_many_atomic(entities))
    }

    /// Insert a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier inserts may commit before an error.
    pub fn insert_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_batch(|save| save.insert_many_non_atomic(entities))
    }

    pub fn replace<E>(&self, entity: E) -> Result<WriteResponse<E>, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_entity(|save| save.replace(entity))
    }

    /// Replace a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn replace_many_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_batch(|save| save.replace_many_atomic(entities))
    }

    /// Replace a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier replaces may commit before an error.
    pub fn replace_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_batch(|save| save.replace_many_non_atomic(entities))
    }

    pub fn update<E>(&self, entity: E) -> Result<WriteResponse<E>, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_entity(|save| save.update(entity))
    }

    /// Update a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn update_many_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_batch(|save| save.update_many_atomic(entities))
    }

    /// Update a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier updates may commit before an error.
    pub fn update_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_batch(|save| save.update_many_non_atomic(entities))
    }

    pub fn insert_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_view::<E>(|save| save.insert_view(view))
    }

    pub fn replace_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_view::<E>(|save| save.replace_view(view))
    }

    pub fn update_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.execute_save_view::<E>(|save| save.update_view(view))
    }

    /// TEST ONLY: clear all registered data and index stores for this database.
    #[cfg(test)]
    #[doc(hidden)]
    pub fn clear_stores_for_tests(&self) {
        self.db.with_store_registry(|reg| {
            for (_, store) in reg.iter() {
                store.with_data_mut(crate::db::store::DataStore::clear);
                store.with_index_mut(crate::db::index::IndexStore::clear);
            }
        });
    }
}
