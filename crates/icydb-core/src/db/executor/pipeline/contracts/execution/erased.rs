//! Module: db::executor::pipeline::contracts::execution::erased
//! Responsibility: erased execution-runtime pointer bindings and typed recovery helpers.
//! Does not own: structural page materialization or execution-attempt orchestration.
//! Boundary: contains the execution-runtime unsafe pointer recovery seam in one place.

use crate::{
    db::{
        access::AccessPlan,
        direction::Direction,
        executor::{
            AccessStreamBindings, Context, ExecutableAccess, LoadExecutor, OrderedKeyStreamBox,
            pipeline::contracts::FastPathKeyResult, route::RoutedKeyStreamRequest,
        },
        index::predicate::IndexPredicateExecution,
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};
use std::ptr;

type PkOrderRuntimeFn = unsafe fn(
    ErasedContext,
    &AccessPlannedQuery,
    Direction,
    Option<usize>,
) -> Result<Option<FastPathKeyResult>, InternalError>;
type SecondaryIndexOrderRuntimeFn = unsafe fn(
    ErasedContext,
    &AccessPlannedQuery,
    Option<&crate::db::executor::LoweredIndexPrefixSpec>,
    Direction,
    Option<usize>,
    Option<IndexPredicateExecution<'_>>,
) -> Result<Option<FastPathKeyResult>, InternalError>;
type IndexRangeLimitPushdownRuntimeFn =
    unsafe fn(
        ErasedContext,
        &AccessPlannedQuery,
        Option<&crate::db::executor::LoweredIndexRangeSpec>,
        crate::db::executor::AccessScanContinuationInput<'_>,
        usize,
        Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError>;
type FallbackExecutionKeysRuntimeFn = unsafe fn(
    ErasedContext,
    ErasedAccessPlan,
    AccessStreamBindings<'_>,
    Option<usize>,
    Option<IndexPredicateExecution<'_>>,
) -> Result<OrderedKeyStreamBox, InternalError>;

// SAFETY INVARIANTS:
// - `ErasedContext` must only be constructed from `&Context<'_, E>`.
// - `ErasedAccessPlan` must only be constructed from `&AccessPlan<E::Key>`.
// - `ExecutionRuntimeCoreVTable` must be instantiated with the same `E` used
//   to create the erased pointers.
// - `ErasedRuntimeBindings` keeps the erased pointers and matching vtable
//   together so cross-entity mixing cannot happen through separate fields.
// - Lifetimes remain bounded by the executor scope through the borrowed
//   `ExecutionRuntimeAdapter<'ctx, 'a>` wrapper that owns these bindings.

///
/// ErasedContext
///
/// ErasedContext is the type-erased execution context pointer carried through
/// the structural runtime adapter.
/// It documents that the pointer originated from one typed `Context<'_, E>`
/// and centralizes typed recovery behind one audited unsafe boundary.
///

#[repr(transparent)]
#[derive(Clone, Copy)]
pub(in crate::db::executor::pipeline::contracts::execution) struct ErasedContext(*const ());

impl ErasedContext {
    /// Construct one erased execution context from one typed context handle.
    #[must_use]
    pub(in crate::db::executor::pipeline::contracts::execution) const fn new<E>(
        ctx: &Context<'_, E>,
    ) -> Self
    where
        E: EntityKind + EntityValue,
    {
        Self(ptr::from_ref(ctx).cast())
    }

    // Recover one typed execution context behind the runtime vtable contract.
    const unsafe fn as_typed<E>(self) -> &'static Context<'static, E>
    where
        E: EntityKind + EntityValue,
    {
        // SAFETY:
        // - the pointer was created from `&Context<'_, E>` in `ErasedContext::new`
        // - the paired runtime vtable was instantiated with the same `E`
        // - the outer adapter lifetime bounds ensure the reference does not
        //   outlive the original executor-scoped borrow
        unsafe { &*self.0.cast::<Context<'_, E>>() }
    }
}

///
/// ErasedAccessPlan
///
/// ErasedAccessPlan is the type-erased access-plan pointer carried with the
/// structural runtime adapter.
/// It documents that the pointer originated from one typed `AccessPlan<E::Key>`
/// and centralizes typed recovery behind one audited unsafe boundary.
///

#[repr(transparent)]
#[derive(Clone, Copy)]
pub(in crate::db::executor::pipeline::contracts::execution) struct ErasedAccessPlan(*const ());

impl ErasedAccessPlan {
    /// Construct one erased typed access plan from one borrowed typed access plan.
    #[must_use]
    pub(in crate::db::executor::pipeline::contracts::execution) const fn new<E>(
        access: &AccessPlan<E::Key>,
    ) -> Self
    where
        E: EntityKind + EntityValue,
    {
        Self(ptr::from_ref(access).cast())
    }

    // Recover one typed access plan behind the runtime vtable contract.
    const unsafe fn as_typed<E>(self) -> &'static AccessPlan<E::Key>
    where
        E: EntityKind + EntityValue,
    {
        // SAFETY:
        // - the pointer was created from `&AccessPlan<E::Key>` in `ErasedAccessPlan::new`
        // - the paired runtime vtable was instantiated with the same `E`
        // - the outer adapter lifetime bounds ensure the reference does not
        //   outlive the original executor-scoped borrow
        unsafe { &*self.0.cast::<AccessPlan<E::Key>>() }
    }
}

///
/// ErasedRuntimeBindings
///
/// ErasedRuntimeBindings keeps the erased runtime pointers and matching typed
/// leaf vtable together as one invariant-carrying bundle.
/// The structural runtime adapter uses this as pure delegation state so pointer
/// creation and unsafe typed recovery remain isolated to this module only.
///

pub(in crate::db::executor::pipeline::contracts::execution) struct ErasedRuntimeBindings {
    ctx: ErasedContext,
    access: ErasedAccessPlan,
    vtable: ExecutionRuntimeCoreVTable,
}

impl ErasedRuntimeBindings {
    /// Construct one erased runtime binding bundle from one typed runtime boundary.
    #[must_use]
    pub(in crate::db::executor::pipeline::contracts::execution) const fn new<E>(
        ctx: &Context<'_, E>,
        access: &AccessPlan<E::Key>,
    ) -> Self
    where
        E: EntityKind + EntityValue,
    {
        Self {
            ctx: ErasedContext::new::<E>(ctx),
            access: ErasedAccessPlan::new::<E>(access),
            vtable: execution_runtime_core_vtable::<E>(),
        }
    }

    /// Delegate primary-key fast-path execution through the typed runtime leaf.
    pub(in crate::db::executor::pipeline::contracts::execution) fn pk_order(
        &self,
        plan: &AccessPlannedQuery,
        direction: Direction,
        physical_fetch_hint: Option<usize>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        // SAFETY:
        // - this bundle was constructed from matching typed pointers and vtable
        // - the called leaf is selected by the same `E` that created the bundle
        unsafe { (self.vtable.pk_order)(self.ctx, plan, direction, physical_fetch_hint) }
    }

    /// Delegate secondary-index fast-path execution through the typed runtime leaf.
    pub(in crate::db::executor::pipeline::contracts::execution) fn secondary_index_order(
        &self,
        plan: &AccessPlannedQuery,
        index_prefix_spec: Option<&crate::db::executor::LoweredIndexPrefixSpec>,
        direction: Direction,
        physical_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        // SAFETY:
        // - this bundle was constructed from matching typed pointers and vtable
        // - the called leaf is selected by the same `E` that created the bundle
        unsafe {
            (self.vtable.secondary_index_order)(
                self.ctx,
                plan,
                index_prefix_spec,
                direction,
                physical_fetch_hint,
                index_predicate_execution,
            )
        }
    }

    /// Delegate index-range limit-pushdown execution through the typed runtime leaf.
    pub(in crate::db::executor::pipeline::contracts::execution) fn index_range_limit_pushdown(
        &self,
        plan: &AccessPlannedQuery,
        index_range_spec: Option<&crate::db::executor::LoweredIndexRangeSpec>,
        continuation: crate::db::executor::AccessScanContinuationInput<'_>,
        fetch: usize,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        // SAFETY:
        // - this bundle was constructed from matching typed pointers and vtable
        // - the called leaf is selected by the same `E` that created the bundle
        unsafe {
            (self.vtable.index_range_limit_pushdown)(
                self.ctx,
                plan,
                index_range_spec,
                continuation,
                fetch,
                index_predicate_execution,
            )
        }
    }

    /// Delegate fallback key-stream resolution through the typed runtime leaf.
    pub(in crate::db::executor::pipeline::contracts::execution) fn fallback_execution_keys(
        &self,
        bindings: AccessStreamBindings<'_>,
        physical_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        // SAFETY:
        // - this bundle was constructed from matching typed pointers and vtable
        // - the called leaf is selected by the same `E` that created the bundle
        unsafe {
            (self.vtable.fallback_execution_keys)(
                self.ctx,
                self.access,
                bindings,
                physical_fetch_hint,
                index_predicate_execution,
            )
        }
    }
}

struct ExecutionRuntimeCoreVTable {
    pk_order: PkOrderRuntimeFn,
    secondary_index_order: SecondaryIndexOrderRuntimeFn,
    index_range_limit_pushdown: IndexRangeLimitPushdownRuntimeFn,
    fallback_execution_keys: FallbackExecutionKeysRuntimeFn,
}

const fn execution_runtime_core_vtable<E>() -> ExecutionRuntimeCoreVTable
where
    E: EntityKind + EntityValue,
{
    ExecutionRuntimeCoreVTable {
        pk_order: runtime_try_execute_pk_order_stream::<E>,
        secondary_index_order: runtime_try_execute_secondary_index_order_stream::<E>,
        index_range_limit_pushdown: runtime_try_execute_index_range_limit_pushdown_stream::<E>,
        fallback_execution_keys: runtime_resolve_fallback_execution_key_stream::<E>,
    }
}

unsafe fn runtime_try_execute_pk_order_stream<E>(
    ctx: ErasedContext,
    plan: &AccessPlannedQuery,
    direction: Direction,
    physical_fetch_hint: Option<usize>,
) -> Result<Option<FastPathKeyResult>, InternalError>
where
    E: EntityKind + EntityValue,
{
    let ctx = unsafe { ctx.as_typed::<E>() };

    LoadExecutor::<E>::try_execute_pk_order_stream(ctx, plan, direction, physical_fetch_hint)
}

unsafe fn runtime_try_execute_secondary_index_order_stream<E>(
    ctx: ErasedContext,
    plan: &AccessPlannedQuery,
    index_prefix_spec: Option<&crate::db::executor::LoweredIndexPrefixSpec>,
    direction: Direction,
    physical_fetch_hint: Option<usize>,
    index_predicate_execution: Option<IndexPredicateExecution<'_>>,
) -> Result<Option<FastPathKeyResult>, InternalError>
where
    E: EntityKind + EntityValue,
{
    let ctx = unsafe { ctx.as_typed::<E>() };

    LoadExecutor::<E>::try_execute_secondary_index_order_stream(
        ctx,
        plan,
        index_prefix_spec,
        direction,
        physical_fetch_hint,
        index_predicate_execution,
    )
}

unsafe fn runtime_try_execute_index_range_limit_pushdown_stream<E>(
    ctx: ErasedContext,
    plan: &AccessPlannedQuery,
    index_range_spec: Option<&crate::db::executor::LoweredIndexRangeSpec>,
    continuation: crate::db::executor::AccessScanContinuationInput<'_>,
    fetch: usize,
    index_predicate_execution: Option<IndexPredicateExecution<'_>>,
) -> Result<Option<FastPathKeyResult>, InternalError>
where
    E: EntityKind + EntityValue,
{
    let ctx = unsafe { ctx.as_typed::<E>() };

    LoadExecutor::<E>::try_execute_index_range_limit_pushdown_stream(
        ctx,
        plan,
        index_range_spec,
        continuation,
        fetch,
        index_predicate_execution,
    )
}

unsafe fn runtime_resolve_fallback_execution_key_stream<E>(
    ctx: ErasedContext,
    access: ErasedAccessPlan,
    bindings: AccessStreamBindings<'_>,
    physical_fetch_hint: Option<usize>,
    index_predicate_execution: Option<IndexPredicateExecution<'_>>,
) -> Result<OrderedKeyStreamBox, InternalError>
where
    E: EntityKind + EntityValue,
{
    let ctx = unsafe { ctx.as_typed::<E>() };
    let access = unsafe { access.as_typed::<E>() };
    let access = ExecutableAccess::new(
        access,
        bindings,
        physical_fetch_hint,
        index_predicate_execution,
    );

    LoadExecutor::<E>::resolve_routed_key_stream(
        ctx,
        RoutedKeyStreamRequest::ExecutableAccess(access),
    )
}
